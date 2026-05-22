"""Live Pulsar consumer and aggregator for Q1-Q4.

Run with:
    PYTHONPATH=src python3 -m analytics.runner
"""

from __future__ import annotations

import json
import logging

from analytics.common import AnalyticsState, config, enrich_repo
from crawler.crawl import load_dotenv
from crawler.github_client import GitHubClient
from streaming.pulsar_connection import get_pulsar_client

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    cfg = config()

    # Built only when enrichment is on: GitHubClient raises if no tokens exist.
    github_client = GitHubClient() if cfg["enrich_github"] else None

    client = get_pulsar_client(cfg["broker_url"], probe_topic=cfg["commits_topic"])
    consumer = client.subscribe(cfg["raw_topic"], cfg["subscription"])
    commits_producer = client.create_producer(cfg["commits_topic"])
    tests_producer = client.create_producer(cfg["tests_topic"])
    ci_producer = client.create_producer(cfg["ci_topic"])
    aggregate_producer = client.create_producer(cfg["aggregate_topic"])

    # Resume from the last saved state: Pulsar will not redeliver messages it
    # already acked, so a fresh empty state would silently drop them.
    state = AnalyticsState.load(cfg["state_path"])
    total_received = 0
    pending: list = []
    saved_once = False

    def flush() -> None:
        nonlocal saved_once
        # Persist state, then ack. A message is acked only once its work is
        # durable, so a crash redelivers it instead of dropping it; add_repo
        # dedupes any redelivered message via state.seen.
        save_and_publish(state, cfg, aggregate_producer)
        saved_once = True
        for processed in pending:
            consumer.acknowledge(processed)
        pending.clear()

    log.info(
        "analytics consumer started raw_topic=%s top_n=%d enrich_github=%s resumed_repos=%d",
        cfg["raw_topic"],
        cfg["top_n"],
        cfg["enrich_github"],
        len(state.seen),
    )

    try:
        while True:
            message = consumer.receive()
            try:
                repo = json.loads(message.data().decode("utf-8"))
                total_received += 1

                is_new = process_repo(
                    repo,
                    state,
                    github_client,
                    commits_producer,
                    tests_producer,
                    ci_producer,
                )
            except Exception:
                log.exception("failed to process message; negative acking")
                consumer.negative_acknowledge(message)
                continue

            pending.append(message)

            if (is_new and not saved_once) or len(pending) >= cfg["flush_every"]:
                flush()
                log.info(
                    "processed %d received messages, %d unique repos",
                    total_received,
                    len(state.seen),
                )

            if cfg["max_repos"] and len(state.seen) >= cfg["max_repos"]:
                break
    finally:
        flush()
        consumer.close()
        commits_producer.close()
        tests_producer.close()
        ci_producer.close()
        aggregate_producer.close()
        client.close()


def save_and_publish(state: AnalyticsState, cfg: dict, aggregate_producer) -> None:
    state.save(cfg["results_dir"], cfg["state_path"], cfg["top_n"])
    send_json(aggregate_producer, state.results(cfg["top_n"]))


def process_repo(
    repo: dict,
    state: AnalyticsState,
    github_client: GitHubClient | None,
    commits_producer,
    tests_producer,
    ci_producer,
) -> bool:
    # Skip already-counted repos before enriching: enrich_repo makes many
    # GitHub calls, and duplicates would otherwise burn rate-limit budget.
    if int(repo["repo_id"]) in state.seen:
        return False

    enrichment = enrich_repo(github_client, repo) if github_client else {}

    # Publish derived records before mutating state. If any send raises, the
    # raw message is negative-acked and redelivered; state.seen must not cause
    # the retry to skip derived topics that did not publish yet.
    send_json(commits_producer, {**repo, "commit_count": enrichment.get("commit_count")})
    send_json(tests_producer, {**repo, "has_tests": enrichment.get("has_tests", False)})
    send_json(ci_producer, {**repo, "has_ci": enrichment.get("has_ci", False)})

    return state.add_repo(repo, enrichment)


def send_json(producer, payload: dict) -> None:
    producer.send(json.dumps(payload, sort_keys=True).encode("utf-8"))


if __name__ == "__main__":
    main()
