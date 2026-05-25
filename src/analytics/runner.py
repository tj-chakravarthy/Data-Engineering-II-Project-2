"""Scalable Pulsar enrichment worker for repository analytics.

Run with:
    PYTHONPATH=src python3 -m analytics.runner
"""

from __future__ import annotations

import json
import logging
import time

from analytics.common import config, enrich_repo, is_receive_timeout, should_idle_flush
from crawler.github_client import GitHubClient
from streaming.pulsar_connection import get_pulsar_client

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = config()

    # Built only when enrichment is on: GitHubClient raises if no tokens exist.
    github_client = GitHubClient() if cfg["enrich_github"] else None

    import pulsar

    client = get_pulsar_client(cfg["broker_url"], probe_topic=cfg["enriched_topic"])
    consumer = client.subscribe(
        cfg["raw_topic"],
        cfg["analytics_subscription"],
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest,
        receiver_queue_size=max(1, cfg["runner_batch_size"] // 10),
    )
    enriched_producer = client.create_producer(cfg["enriched_topic"])

    total_received = 0
    pending_messages: list = []
    pending_records: list[dict] = []
    last_message_at = time.monotonic()
    receive_timeout_millis = 1000

    def flush_batch() -> None:
        if not pending_records:
            return
        send_enriched_batch(enriched_producer, pending_records)
        for processed in pending_messages:
            consumer.acknowledge(processed)
        pending_messages.clear()
        pending_records.clear()

    def negative_ack_pending() -> None:
        for processed in pending_messages:
            consumer.negative_acknowledge(processed)
        pending_messages.clear()
        pending_records.clear()

    log.info(
        "analytics runner started raw_topic=%s enriched_topic=%s batch_size=%d enrich_github=%s",
        cfg["raw_topic"],
        cfg["enriched_topic"],
        cfg["runner_batch_size"],
        cfg["enrich_github"],
    )

    try:
        while True:
            try:
                message = consumer.receive(timeout_millis=receive_timeout_millis)
            except Exception as exc:
                if is_receive_timeout(exc):
                    idle_seconds = time.monotonic() - last_message_at
                    if should_idle_flush(len(pending_records), idle_seconds, cfg["flush_idle_seconds"]):
                        try:
                            batch_size = len(pending_records)
                            flush_batch()
                            log.info(
                                "idle-sent %d enriched records after %.1fs",
                                batch_size,
                                idle_seconds,
                            )
                        except Exception:
                            log.exception("failed to publish idle enriched batch; negative acking pending")
                            negative_ack_pending()
                    continue
                raise

            try:
                last_message_at = time.monotonic()
                repo = json.loads(message.data().decode("utf-8"))
                total_received += 1

                pending_records.append(process_repo(repo, github_client))
                pending_messages.append(message)
            except Exception:
                log.exception("failed to process message; negative acking")
                consumer.negative_acknowledge(message)
                continue

            try:
                if len(pending_records) >= cfg["runner_batch_size"]:
                    flush_batch()
                    log.info("sent %d enriched raw repo messages", total_received)

                if cfg["max_repos"] and total_received >= cfg["max_repos"]:
                    flush_batch()
                    break
            except Exception:
                log.exception("failed to publish enriched batch; negative acking pending")
                negative_ack_pending()
    finally:
        try:
            flush_batch()
        except Exception:
            log.exception("failed to publish final enriched batch; negative acking pending")
            negative_ack_pending()
        consumer.close()
        enriched_producer.close()
        client.close()


def process_repo(
    repo: dict,
    github_client: GitHubClient | None,
) -> dict:
    enrichment = enrich_repo(github_client, repo) if github_client else {}
    return {
        **repo,
        "commit_count": enrichment.get("commit_count"),
        "has_tests": enrichment.get("has_tests", False),
        "has_ci": enrichment.get("has_ci", False),
    }


def send_enriched_batch(producer, records: list[dict]) -> None:
    send_json(producer, {"records": records})


def send_json(producer, payload: dict) -> None:
    producer.send(json.dumps(payload, sort_keys=True).encode("utf-8"))


if __name__ == "__main__":
    main()
