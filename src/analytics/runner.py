"""Live Pulsar consumer and aggregator for Q1-Q4.

Run with:
    PYTHONPATH=src python3 -m analytics.runner
"""

from __future__ import annotations

import json
import logging

from analytics.common import AnalyticsState, config, enrich_repo

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = config()

    try:
        import pulsar
    except ImportError as exc:
        raise RuntimeError("Install pulsar-client with `pip install -r requirements.txt`.") from exc

    client = pulsar.Client(cfg["broker_url"])
    consumer = client.subscribe(cfg["raw_topic"], cfg["subscription"])
    commits_producer = client.create_producer(cfg["commits_topic"])
    tests_producer = client.create_producer(cfg["tests_topic"])
    ci_producer = client.create_producer(cfg["ci_topic"])
    aggregate_producer = client.create_producer(cfg["aggregate_topic"])

    state = AnalyticsState()
    processed_since_flush = 0
    total_received = 0

    log.info(
        "analytics consumer started raw_topic=%s top_n=%d enrich_github=%s",
        cfg["raw_topic"],
        cfg["top_n"],
        cfg["enrich_github"],
    )

    try:
        while True:
            message = consumer.receive()
            try:
                repo = json.loads(message.data().decode("utf-8"))
                total_received += 1
                enrichment = enrich_repo(repo) if cfg["enrich_github"] else None

                is_new = state.add_repo(repo, enrichment)
                if is_new:
                    enrichment = enrichment or {}
                    send_json(commits_producer, {**repo, "commit_count": enrichment.get("commit_count")})
                    send_json(tests_producer, {**repo, "has_tests": enrichment.get("has_tests", False)})
                    send_json(ci_producer, {**repo, "has_ci": enrichment.get("has_ci", False)})
                    processed_since_flush += 1

                consumer.acknowledge(message)

                if processed_since_flush >= cfg["flush_every"]:
                    save_and_publish(state, cfg, aggregate_producer)
                    log.info(
                        "processed %d received messages, %d unique repos",
                        total_received,
                        len(state.seen),
                    )
                    processed_since_flush = 0

                if cfg["max_repos"] and len(state.seen) >= cfg["max_repos"]:
                    break
            except Exception:
                log.exception("failed to process message; negative acking")
                consumer.negative_acknowledge(message)
    finally:
        save_and_publish(state, cfg, aggregate_producer)
        consumer.close()
        commits_producer.close()
        tests_producer.close()
        ci_producer.close()
        aggregate_producer.close()
        client.close()


def save_and_publish(state: AnalyticsState, cfg: dict, aggregate_producer) -> None:
    state.save(cfg["results_dir"], cfg["state_path"], cfg["top_n"])
    send_json(aggregate_producer, state.results(cfg["top_n"]))


def send_json(producer, payload: dict) -> None:
    producer.send(json.dumps(payload, sort_keys=True).encode("utf-8"))


if __name__ == "__main__":
    main()
