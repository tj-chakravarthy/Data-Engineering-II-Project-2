"""Single Pulsar aggregator for enriched repository analytics.

Run with:
    PYTHONPATH=src python3 -m analytics.aggregator
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from crawler.crawl import _emit_timestamp

from analytics.common import AnalyticsState, config, is_receive_timeout, should_idle_flush
from analytics.plot_results import plot_aggregate_payload
from streaming.pulsar_connection import get_pulsar_client

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = config()

    import pulsar

    client = get_pulsar_client(cfg["broker_url"], probe_topic=cfg["enriched_topic"])
    consumer = client.subscribe(
        cfg["enriched_topic"],
        cfg["aggregator_subscription"],
        initial_position=pulsar.InitialPosition.Earliest,
    )

    state = AnalyticsState.load(cfg["state_path"])
    total_received = 0
    pending: list = []
    saved_once = False
    last_flush_at = time.monotonic()
    last_message_at = last_flush_at
    receive_timeout_millis = 30000

    def flush() -> None:
        nonlocal last_flush_at, saved_once
        save_state(state, cfg)
        saved_once = True
        for processed in pending:
            consumer.acknowledge(processed)
        pending.clear()
        last_flush_at = time.monotonic()

    log.info(
        "analytics aggregator started enriched_topic=%s resumed_repos=%d",
        cfg["enriched_topic"],
        len(state.seen),
    )

    try:
        while True:
            try:
                message = consumer.receive(timeout_millis=receive_timeout_millis)
            except Exception as exc:
                if is_receive_timeout(exc):
                    idle_seconds = time.monotonic() - last_message_at
                    if should_idle_flush(len(pending), idle_seconds, cfg["flush_idle_seconds"]):
                        pending_count = len(pending)
                        try:
                            flush()
                            log.info(
                                "idle-flushed %d pending messages after %.1fs",
                                pending_count,
                                idle_seconds,
                            )
                        except Exception:
                            # Mirror the runner's idle-flush handling: a save/plot
                            # failure must not kill the consumer loop. Pending
                            # messages stay un-acked; Pulsar will redeliver on
                            # reconnect or after the negative-ack timeout.
                            log.exception(
                                "failed to flush %d pending messages on idle; "
                                "leaving un-acked for redelivery",
                                pending_count,
                            )
                    continue
                raise

            try:
                last_message_at = time.monotonic()
                payload = json.loads(message.data().decode("utf-8"))
                records = enriched_records(payload)
                total_received += len(records)
                is_new = process_enriched_records(records, state)
            except Exception:
                log.exception("failed to aggregate enriched message; negative acking")
                consumer.negative_acknowledge(message)
                continue

            if cfg["prof_mode"] == "true":
                log_timestamps(records, cfg)

            pending.append(message)

            if (is_new and not saved_once) or len(pending) >= cfg["flush_every"]:
                flush()
                log.info(
                    "aggregated %d enriched messages, %d unique repos",
                    total_received,
                    len(state.seen),
                )

            if cfg["max_repos"] and len(state.seen) >= cfg["max_repos"]:
                break
    finally:
        # The final flush is the only step that writes user-visible output
        # (the results JSONs). Losing it silently on a controlled shutdown
        # (MAX_REPOS, signal) would let the service exit 0 with no results
        # on disk. On crash, the original exception is more informative
        # than a chained flush error, so log-and-continue is correct there.
        # plot_results and close_* are always best-effort.
        crashed = sys.exc_info()[0] is not None
        flush_error: Exception | None = None
        try:
            flush()
        except Exception as exc:
            log.exception("aggregator final flush failed")
            flush_error = exc
        for step_name, step in (
            ("plot results", lambda: plot_results(state, cfg)),
            ("close consumer", consumer.close),
            ("close client", client.close),
        ):
            try:
                step()
            except Exception:
                log.exception("aggregator shutdown step %r failed; continuing", step_name)
        if flush_error is not None and not crashed:
            raise flush_error


def process_enriched_repo(repo: dict, state: AnalyticsState) -> bool:
    repo["aggregator_received_at"] = _emit_timestamp()
    return state.add_repo(repo, repo)


def process_enriched_records(records: list[dict], state: AnalyticsState) -> bool:
    added = False
    for repo in records:
        added = process_enriched_repo(repo, state) or added
    return added


def enriched_records(payload) -> list[dict]:
    if isinstance(payload, dict) and isinstance(payload.get("records"), list):
        return payload["records"]
    if isinstance(payload, dict):
        return [payload]
    raise ValueError("enriched payload must be a repo object or {'records': [...]}")


def save_state(state: AnalyticsState, cfg: dict) -> None:
    state.save(cfg["results_dir"], cfg["state_path"], cfg["top_n"])


def plot_results(state: AnalyticsState, cfg: dict) -> None:
    # state.results() is inside the try so any unexpected exception is caught
    # here and does not escape the finally block to suppress the original crash.
    try:
        results = state.results(cfg["top_n"])
        plot_aggregate_payload(results, cfg["figures_dir"])
    except Exception:
        log.exception("plot_aggregate_payload failed; results JSON is still saved")

def log_timestamps(records: list[dict], cfg: dict) -> None:
    results_dir = cfg["results_dir"]
    results_dir.mkdir(parents=True, exist_ok=True)
    log_path = results_dir / "timestamps_profiling.jsonl"

    with log_path.open("a", encoding="utf-8") as f:
        for repo in records:
            json.dump({
                "repo_name": repo.get("full_name"),
                "crawler_emitted_at": repo.get("emitted_at"),
                "runner_received_at": repo.get("runner_received_at"),
                "runner_enriched_at": repo.get("runner_enriched_at"),
                "aggregator_received_at": repo.get("aggregator_received_at"),
            }, f, sort_keys=True)
            f.write("\n")


if __name__ == "__main__":
    main()
