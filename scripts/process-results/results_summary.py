from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from statistics import mean


def parse_timestamp(value: str | None) -> float | None:
    if not value:
        return None

    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--file",
        type=Path,
        default="data/results/timestamps_profiling.jsonl",
        help="Path to timestamps_profiling.jsonl",
    )

    parser.add_argument(
        "--deployed-at",
        type=float,
        default=None,
        help="Unix timestamp of when this experiment was deployed",
    )

    parser.add_argument(
        "--run-seconds",
        required=True,
        type=float,
        default=None,
        help="Number of seconds this experiment ran for",
    )

    args = parser.parse_args()
    path = args.file
    deployed_at = args.deployed_at
    configured_duration_seconds = args.run_seconds

    if not path.exists():
        print(f"File does not exist: {path}")
        return

    latencies = []
    first_start = None
    last_end = None
    stale_count = 0

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            row = json.loads(line)

            crawler_emitted_at = parse_timestamp(row.get("crawler_emitted_at"))
            aggregator_received_at = parse_timestamp(row.get("aggregator_received_at"))

            if crawler_emitted_at is None or aggregator_received_at is None:
                continue
            if deployed_at is not None and crawler_emitted_at < deployed_at:
                stale_count += 1
                continue

            latency = aggregator_received_at - crawler_emitted_at
            latencies.append(latency)

            if first_start is None or crawler_emitted_at < first_start:
                first_start = crawler_emitted_at

            if last_end is None or aggregator_received_at > last_end:
                last_end = aggregator_received_at

    total_processed_repositories = len(latencies)

    if total_processed_repositories == 0:
        print("No valid timestamp rows found.")
        return

    observed_duration_seconds = last_end - first_start
    throughput_repos_per_second = total_processed_repositories / configured_duration_seconds
    throughput_repos_per_minute = throughput_repos_per_second * 60
    average_latency_seconds = mean(latencies)

    summary = {
        "total_processed_repositories": total_processed_repositories,
        "throughput_repos_per_second": round(throughput_repos_per_second, 4),
        "throughput_repos_per_minute": round(throughput_repos_per_minute, 4),
        "average_latency_seconds": round(average_latency_seconds, 4),
        "observed_duration_seconds": round(observed_duration_seconds, 4),
    }
    if deployed_at is not None:
        summary["stale_discarded"] = stale_count
        summary["stale_fraction"] = round(stale_count / (total_processed_repositories + stale_count), 4)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
