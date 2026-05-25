"""Live Pulsar producer for crawler records.

Publishing semantics:

- Async send via Pulsar's ``send_async`` for throughput; the publisher waits
  for every logical record to either be acked or exhaust retries before the
  run completes.
- Transient send failures are retried up to ``--max-retries`` times with
  exponential backoff. Records that exhaust retries are recorded as
  ``permanent_failures`` and logged.
- Optional NDJSON output (``--output``) writes successfully acknowledged
  published records to disk so the validator can inspect what this run sent.
  The file is truncated on each run and does not include records skipped via
  the publish checkpoint — treat it as a publication log for THIS run, not as
  a snapshot of the topic.
- Optional publish checkpoint (``--checkpoint-path``) records every
  successfully published ``repo_id`` to a JSON file and skips ``repo_id``s
  already in that file on restart, providing idempotent recovery without
  consumer-side dedup.

Delivery model: at-least-once. Consumers MUST be idempotent on ``repo_id``
because retries can produce duplicate broker writes for the same record.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
import requests
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

from crawler.cli_args import (
    add_crawl_args,
    add_rate_limit_args,
    build_crawl_config,
)
from crawler.crawl import CrawlStats, crawl_window
from crawler.github_client import GitHubClient
from crawler.models import RepoRecord
from streaming.pulsar_connection import get_pulsar_client

DEFAULT_BROKER_URL = "pulsar://localhost:6650"
DEFAULT_TOPIC = "repos.raw"

log = logging.getLogger(__name__)


class AsyncProducer(Protocol):
    """Subset of the Pulsar Producer API the publisher needs."""

    def send_async(
        self,
        content: bytes,
        callback: Callable[[Any, Any], None],
        partition_key: str | None = None,
    ) -> None: ...

    def flush(self) -> None: ...


class PublishCheckpoint:
    """JSON-backed set of successfully published ``repo_id``s.

    Threadsafe: the publisher's async callbacks run from Pulsar's I/O thread
    pool, so ``mark_published`` may be invoked concurrently.
    """

    def __init__(self, path: Path | None, save_every: int = 1000) -> None:
        self.path = path
        self.save_every = save_every
        self._published_ids: set[int] = set()
        self._dirty = 0
        self._lock = threading.Lock()
        if path is not None and path.exists():
            self._load()

    @property
    def published_count(self) -> int:
        with self._lock:
            return len(self._published_ids)

    def already_published(self, repo_id: int) -> bool:
        with self._lock:
            return repo_id in self._published_ids

    def mark_published(self, repo_id: int) -> None:
        if self.path is None:
            return
        should_save = False
        with self._lock:
            self._published_ids.add(repo_id)
            self._dirty += 1
            if self._dirty >= self.save_every:
                should_save = True
        if should_save:
            self.save()

    def save(self) -> None:
        if self.path is None:
            return
        with self._lock:
            payload = {"published_repo_ids": sorted(self._published_ids)}
            self._dirty = 0
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", encoding="utf-8") as file:
            json.dump(payload, file)
        tmp.replace(self.path)

    def _load(self) -> None:
        assert self.path is not None
        with self.path.open(encoding="utf-8") as file:
            data = json.load(file)
        loaded = data.get("published_repo_ids", [])
        self._published_ids = {int(repo_id) for repo_id in loaded}


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = build_crawl_config(args)
    client = GitHubClient(
        max_wait_seconds=args.max_wait_seconds,
        max_total_wait_seconds=args.max_total_wait_seconds,
    )

    pulsar_client, producer = create_pulsar_producer(args.broker, args.topic)
    records, stats = crawl_window(client, config)

    checkpoint = PublishCheckpoint(args.checkpoint_path) if args.checkpoint_path else None
    if checkpoint is not None and checkpoint.published_count:
        log.info(
            "loaded publish checkpoint %s with %d previously published repo_ids",
            args.checkpoint_path,
            checkpoint.published_count,
        )

    output_file = None
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output_file = args.output.open("w", encoding="utf-8", newline="\n")

    # fixed by TJ: pass counters in by reference so that if the crawl iterator
    # raises partway through, the partial counts are still here for the final
    # stats line. Before this, counters was only the return value, so a crash
    # left it empty and the stats line logged all zeros.
    counters: dict[str, int] = {}
    try:
        admin_url = os.environ.get("PULSAR_ADMIN_URL")
        max_backlog = int(os.environ.get("CRAWLER_MAX_BACKLOG", "1000"))
        backlog_sleep_seconds = int(os.environ.get("CRAWLER_BACKLOG_SLEEP_SECONDS", "30"))
        runner_subscription = os.environ.get("ANALYTICS_SUBSCRIPTION", "analytics-q1-q4")
        backlog_enabled = os.environ.get("CRAWLER_BACKLOG_ENABLED",
                                         "false").lower() == "true"

        publish_records(
            records,
            producer,
            checkpoint=checkpoint,
            max_in_flight=args.max_in_flight,
            max_retries=args.max_retries,
            on_publish=_combine_publish_callbacks(
                _make_output_writer(output_file),
                _make_publish_logger(args.publish_log_every, args.topic),
            ),
            counters=counters,
            admin_url=admin_url,
            raw_topic=args.topic,
            max_backlog=max_backlog,
            backlog_sleep_seconds=backlog_sleep_seconds,
            runner_subscription=runner_subscription,
            backlog_enabled=backlog_enabled,
        )
    finally:
        flush = getattr(producer, "flush", None)
        if flush is not None:
            flush()
        if output_file is not None:
            output_file.close()
        close = getattr(producer, "close", None)
        if close is not None:
            close()
        pulsar_client.close()
        # fixed by TJ: this is inside finally so partial crawl/publish stats
        # still get logged when the records iterator raises and the exception
        # is re-raised after cleanup.
        log_publish_stats(counters, stats, client, args.topic, args.output)

    if counters.get("permanent_failures", 0):
        raise SystemExit(1)


def publish_records(
    records: Iterable[RepoRecord],
    producer: AsyncProducer,
    on_publish: Callable[[RepoRecord], None] | None = None,
    on_failure: Callable[[RepoRecord], None] | None = None,
    checkpoint: PublishCheckpoint | None = None,
    max_in_flight: int = 1000,
    max_retries: int = 3,
    retry_initial_seconds: float = 0.1,
    counters: dict[str, int] | None = None,
    admin_url: str | None = None,
    raw_topic: str = "repos.raw",
    runner_subscription: str = "analytics-q1-q4",
    max_backlog: int = 1000,
    backlog_sleep_seconds: int = 30,
    backlog_enabled: bool = False,
) -> dict[str, int]:
    """Publish crawler records via ``send_async`` with bounded retry.

    Returns counters: ``published``, ``publish_failures`` (transient send
    errors retried), ``permanent_failures`` (records that exhausted retries),
    ``skipped_via_checkpoint``.

    ``max_in_flight`` bounds outstanding records submitted to the broker.
    The producer must support ``send_async(content, callback, partition_key=...)``
    and ``flush()``. The real Pulsar client does; the test fake does too.

    ``counters`` may be supplied by the caller so partial counts survive an
    exception raised by the ``records`` iterator; if omitted a fresh dict is
    used. The same dict is returned either way.
    """
    if max_in_flight < 1:
        raise ValueError("max_in_flight must be at least 1")
    # fixed by TJ: counters used to be a local dict, so if the crawl iterator
    # raised midway the caller got nothing back and logged all zeros. Now the
    # caller can pass its own dict in and still see the partial counts.
    if counters is None:
        counters = {}
    # setdefault so a caller-supplied dict with partial counts from a previous
    # run is not zeroed — preserving the crash-recovery contract documented above.
    for key in ("published", "publish_failures", "permanent_failures", "skipped_via_checkpoint"):
        counters.setdefault(key, 0)
    lock = threading.Lock()
    completion = threading.Condition(lock)
    in_flight = threading.BoundedSemaphore(max_in_flight)
    logical_in_flight = 0

    def mark_record_started() -> None:
        nonlocal logical_in_flight
        with completion:
            logical_in_flight += 1

    def mark_record_done(release_slot: bool) -> None:
        nonlocal logical_in_flight
        if release_slot:
            in_flight.release()
        with completion:
            logical_in_flight -= 1
            completion.notify_all()

    def wait_for_all_records() -> None:
        with completion:
            while logical_in_flight:
                completion.wait()

    def send_with_retry(record: RepoRecord, attempt: int, release_slot: bool) -> None:
        content = record_to_message(record)
        partition_key = str(record.repo_id)

        def callback(result: Any, _msg_id: Any) -> None:
            if _is_send_ok(result):
                try:
                    with lock:
                        counters["published"] += 1
                    if checkpoint is not None:
                        try:
                            checkpoint.mark_published(record.repo_id)
                        except Exception:
                            log.exception(
                                "checkpoint.mark_published failed for repo_id=%s; continuing",
                                record.repo_id,
                            )
                    if on_publish is not None:
                        try:
                            on_publish(record)
                        except Exception:
                            log.exception(
                                "on_publish callback raised for repo_id=%s; continuing",
                                record.repo_id,
                            )
                finally:
                    mark_record_done(release_slot)
                return
            with lock:
                counters["publish_failures"] += 1
            if attempt < max_retries:
                delay = retry_initial_seconds * (2**attempt)
                # Dispatch retry to a new thread so Pulsar's I/O callback
                # thread is released immediately. Sleeping on the callback
                # thread blocks the entire async I/O loop, stalling ACKs and
                # heartbeats for all other in-flight records.
                def _retry(r=record, a=attempt, rs=release_slot, d=delay) -> None:
                    if d > 0:
                        time.sleep(d)
                    send_with_retry(r, a + 1, release_slot=rs)
                threading.Thread(target=_retry, daemon=True).start()
            else:
                try:
                    with lock:
                        counters["permanent_failures"] += 1
                    if on_failure is not None:
                        try:
                            on_failure(record)
                        except Exception:
                            log.exception(
                                "on_failure callback raised for repo_id=%s; continuing",
                                record.repo_id,
                            )
                    log.error(
                        "permanent publish failure for repo_id=%s after %d attempts",
                        record.repo_id,
                        max_retries + 1,
                    )
                finally:
                    mark_record_done(release_slot)

        try:
            producer.send_async(content, callback, partition_key=partition_key)
        except Exception:
            mark_record_done(release_slot)
            raise

    for record in records:
        if backlog_enabled and admin_url:
            while True:
                backlog = _raw_topic_backlog(admin_url, raw_topic, runner_subscription)

                if backlog is None:
                    log.info(
                        "Backlog unavailable for topic=%s subscription=%s; waiting %ds",
                        raw_topic, runner_subscription, backlog_sleep_seconds,
                    )
                    time.sleep(backlog_sleep_seconds)
                    continue

                if backlog < max_backlog:
                    break

                log.info(
                    "backlog=%d >= max_backlog=%d; pausing crawler %ds",
                    backlog, max_backlog, backlog_sleep_seconds,
                )
                time.sleep(backlog_sleep_seconds)

        if checkpoint is not None and checkpoint.already_published(record.repo_id):
            with lock:
                counters["skipped_via_checkpoint"] += 1
            continue
        in_flight.acquire()
        mark_record_started()
        send_with_retry(record, attempt=0, release_slot=True)

    wait_for_all_records()
    producer.flush()
    if checkpoint is not None:
        checkpoint.save()

    return counters


def record_to_message(record: RepoRecord) -> bytes:
    """Serialize one crawler record as raw JSON for Pulsar."""
    return record.to_json_line().rstrip("\n").encode("utf-8")


def create_pulsar_producer(broker_url: str, topic: str):
    """Create a Pulsar client and producer, waiting for the broker if needed.

    fixed by TJ: the old local get_pulsar_client only retried pulsar.Client(),
    which is lazy and never actually connects, so the retry did nothing. Now
    we use the shared helper that probes the broker for real by creating and
    closing a producer on the target topic. By the time get_pulsar_client
    returns, the broker is confirmed up, so the create_producer call below
    will not fail on a slow-to-start broker.
    """
    client = get_pulsar_client(broker_url, probe_topic=topic)
    return client, client.create_producer(topic)


def log_publish_stats(
    counters: dict[str, int],
    stats: CrawlStats,
    client: GitHubClient,
    topic: str,
    output_path: Path | None,
) -> None:
    log.info(
        "published %d records to Pulsar topic %s (publish_failures=%d "
        "permanent_failures=%d skipped_via_checkpoint=%d)",
        counters.get("published", 0),
        topic,
        counters.get("publish_failures", 0),
        counters.get("permanent_failures", 0),
        counters.get("skipped_via_checkpoint", 0),
    )
    if output_path is not None:
        log.info("wrote NDJSON mirror of crawler output to %s", output_path)
    log.info(
        "crawl stats: emitted=%d fetched=%d cache_written=%d loaded_from_cache=%d "
        "slice_duplicates=%d global_duplicates=%d api_slices=%d cache_slices=%d "
        "memory_samples=%d peak_python_memory_kb=%d "
        "search_splits=%d search_cap_warnings=%d incomplete_search_warnings=%d "
        "rate_limit_waits=%d rate_limit_wait_seconds=%d",
        stats.emitted,
        stats.fetched,
        stats.written_to_cache,
        stats.loaded_from_cache,
        stats.duplicate_in_slice,
        stats.duplicate_global,
        stats.slices_from_api,
        stats.slices_from_cache,
        stats.memory_samples,
        stats.peak_python_memory_kb,
        stats.search_splits,
        stats.search_cap_warnings,
        stats.incomplete_search_warnings,
        client.rate_limit_waits,
        client.rate_limit_wait_seconds,
    )


def _is_send_ok(result: Any) -> bool:
    """Detect a successful Pulsar send result without importing pulsar.

    Pulsar's ``Result.Ok`` enum value is 0 and its ``name`` attribute is
    ``"Ok"``. Fake producers used in tests can return ``None`` or 0.
    """
    if result is None:
        return True
    name = getattr(result, "name", None)
    if isinstance(name, str):
        return name == "Ok"
    return result == 0


def _make_publish_logger(
    publish_log_every: int,
    topic: str,
) -> Callable[[RepoRecord], None] | None:
    if not publish_log_every:
        return None
    state = {"count": 0}

    def on_publish(_record: RepoRecord) -> None:
        state["count"] += 1
        if state["count"] % publish_log_every == 0:
            log.info("published %d crawler records to %s", state["count"], topic)

    return on_publish


def _make_output_writer(output_file) -> Callable[[RepoRecord], None] | None:
    if output_file is None:
        return None
    lock = threading.Lock()

    def on_publish(record: RepoRecord) -> None:
        with lock:
            output_file.write(record.to_json_line())

    return on_publish


def _combine_publish_callbacks(
    *callbacks: Callable[[RepoRecord], None] | None,
) -> Callable[[RepoRecord], None] | None:
    active_callbacks = [callback for callback in callbacks if callback is not None]
    if not active_callbacks:
        return None

    def on_publish(record: RepoRecord) -> None:
        for callback in active_callbacks:
            try:
                callback(record)
            except Exception:
                log.exception(
                    "combined publish callback %r raised for repo_id=%s; continuing",
                    getattr(callback, "__name__", callback),
                    record.repo_id,
                )

    return on_publish


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Live-publish crawler records to an Apache Pulsar topic."
    )
    add_crawl_args(parser)
    add_rate_limit_args(parser)
    parser.add_argument(
        "--broker",
        default=os.environ.get("PULSAR_SERVICE_URL", DEFAULT_BROKER_URL),
        help="Pulsar service URL; default comes from PULSAR_SERVICE_URL or localhost",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("PULSAR_TOPIC", DEFAULT_TOPIC),
        help="Pulsar topic for raw crawler records; default: repos.raw",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "optional NDJSON publication log for THIS run: every record that "
            "is successfully sent to Pulsar is also written here. The file is "
            "truncated on each run and does NOT include records skipped via "
            "--checkpoint-path. Treat as 'what we sent this run', not 'what's "
            "on the topic'."
        ),
    )
    parser.add_argument(
        "--checkpoint-path",
        type=Path,
        default=None,
        help=(
            "optional publish checkpoint file; previously published repo_ids "
            "are skipped on restart for idempotent recovery"
        ),
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="bounded retry attempts per record on transient producer.send failure",
    )
    parser.add_argument(
        "--max-in-flight",
        type=int,
        default=1000,
        help="maximum records with outstanding async sends before crawling blocks",
    )
    parser.add_argument(
        "--publish-log-every",
        type=int,
        default=1000,
        help="log every N successfully sent Pulsar messages; set 0 to disable",
    )
    return parser.parse_args()

def _topic_admin_path(topic: str) -> str:
    """Convert a Pulsar topic name to the path expected by the admin API."""
    if topic.startswith("persistent://"):
        return topic.removeprefix("persistent://")
    return f"public/default/{topic}"

def _raw_topic_backlog(admin_url: str, topic: str, subscription: str) -> int | None:
    """Return the backlog size for a subscription on a topic via Pulsar admin API.
    Returns None if the check fails (network error, topic not yet created, etc.)
    """
    try:
        # topic format: persistent/public/default/<topic-name>
        topic_path = _topic_admin_path(topic)
        url = f"{admin_url.rstrip('/')}/admin/v2/persistent/{topic_path}/stats"

        response = requests.get(url, timeout=5)
        if response.status_code != 200:
            log.warning(
                "backlog check failed for topic=%s subscription=%s: status=%s body=%s",
                topic,
                subscription,
                response.status_code,
                response.text[:200],
                )
            return None

        stats = response.json()
        subscriptions = stats.get("subscriptions", {})

        if subscription not in subscriptions:
            log.info(
                "subscription %s not found yet on topic %s",
                subscription,
                topic,
                )
            return None

        return int(subscriptions[subscription].get("msgBacklog", 0))

    except Exception:
        log.exception(
            "backlog check failed for topic=%s subscription=%s",
            topic,
            subscription,
        )
        return None


if __name__ == "__main__":
    main()
