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
        default="data/results/timestamps_profiling.jsonl",
        help="Path to timestamps_profiling.jsonl",
    )

    args = parser.parse_args()
    path = Path(args.file)

    if not path.exists():
        print(f"File does not exist: {path}")
        return

    latencies = []
    first_start = None
    last_end = None

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

    duration_seconds = last_end - first_start
    throughput_repos_per_second = total_processed_repositories / duration_seconds
    throughput_repos_per_minute = throughput_repos_per_second * 60
    average_latency_seconds = mean(latencies)

    summary = {
        "total_processed_repositories": total_processed_repositories,
        "throughput_repos_per_second": round(throughput_repos_per_second, 4),
        "throughput_repos_per_minute": round(throughput_repos_per_minute, 4),
        "average_latency_seconds": round(average_latency_seconds, 4),
    }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
