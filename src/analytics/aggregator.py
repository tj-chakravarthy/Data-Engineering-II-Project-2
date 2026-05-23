"""Single Pulsar aggregator for enriched repository analytics.

Run with:
    PYTHONPATH=src python3 -m analytics.aggregator
"""

from __future__ import annotations

import json
import logging
import time

from analytics.common import AnalyticsState, config, is_receive_timeout, should_idle_flush
from analytics.plot_results import plot_aggregate_payload
from streaming.pulsar_connection import get_pulsar_client

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = config()

    client = get_pulsar_client(cfg["broker_url"], probe_topic=cfg["enriched_topic"])
    consumer = client.subscribe(cfg["enriched_topic"], cfg["aggregator_subscription"])

    state = AnalyticsState.load(cfg["state_path"])
    total_received = 0
    pending: list = []
    saved_once = False
    last_flush_at = time.monotonic()
    last_message_at = last_flush_at
    receive_timeout_millis = 1000

    def flush() -> None:
        nonlocal last_flush_at, saved_once
        save_and_plot(state, cfg)
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
        flush()
        consumer.close()
        client.close()


def process_enriched_repo(repo: dict, state: AnalyticsState) -> bool:
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


def save_and_plot(state: AnalyticsState, cfg: dict) -> None:
    state.save(cfg["results_dir"], cfg["state_path"], cfg["top_n"])
    results = state.results(cfg["top_n"])
    # Plot failures (font missing, matplotlib backend issue) must not block
    # the ack path: results JSON is already on disk and is the source of truth.
    # A future flush will retry the figures.
    # TODO: remove plot geenration here. All we need is the JSON file, and we
    # can generate the plots later.
    try:
        plot_aggregate_payload(results, cfg["figures_dir"])
    except Exception:
        log.exception("plot_aggregate_payload failed; results JSON is still saved")


if __name__ == "__main__":
    main()
