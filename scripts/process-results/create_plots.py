"""
    python3 scripts/create_plots.py \
        --input experiments-results.jsonl \
        --output-dir figures/experiments
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


def extract_json_objects(text: str) -> list[str]:
    objects: list[str] = []
    depth = 0
    start = None

    for i, char in enumerate(text):
        if char == "{":
            if depth == 0:
                start = i
            depth += 1

        elif char == "}":
            depth -= 1

            if depth == 0 and start is not None:
                objects.append(text[start : i + 1])
                start = None

    return objects


def load_results(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    raw_objects = extract_json_objects(text)

    rows: list[dict[str, Any]] = []

    for raw in raw_objects:
        rows.append(json.loads(raw))

    return rows


def write_clean_csv(rows: list[dict[str, Any]], output_dir: Path) -> None:
    csv_path = output_dir / "clean_experiment_results.csv"

    fieldnames = [
        "run_id",
        "total_processed_repositories",
        "throughput_repos_per_second",
        "throughput_repos_per_minute",
        "average_latency_seconds",
        "observed_duration_seconds",
        "stale_discarded",
        "stale_fraction",
        "run_seconds",
        "analytics_num_runners",
        "num_workers",
        "token_count",
        "flush_every",
        "partition_tokens",
        "pulsar_topic",
        "enriched_topic",
        "analytics_subscription",
        "aggregator_subscription",
        "deployed_at",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

    print(f"Wrote {csv_path}")


def sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: str(row.get("run_id", "")))


def save_all_experiments_bar_plot(
    rows: list[dict[str, Any]],
    y_key: str,
    ylabel: str,
    title: str,
    output_base: Path,
) -> None:
    valid_rows = [
        row for row in rows
        if row.get("run_id") is not None and row.get(y_key) is not None
    ]

    if not valid_rows:
        print(f"Skipping {output_base.name}: no valid rows")
        return

    run_ids = [str(row["run_id"]) for row in valid_rows]
    y_values = [float(row[y_key]) for row in valid_rows]

    plt.figure(figsize=(12, 5))
    plt.bar(run_ids, y_values)
    plt.xlabel("Experiment run")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", linestyle="--", linewidth=0.5)
    plt.tight_layout()

    png_path = output_base.with_suffix(".png")
    pdf_path = output_base.with_suffix(".pdf")

    plt.savefig(png_path, dpi=300)
    plt.savefig(pdf_path)
    plt.close()

    print(f"Wrote {png_path}")
    print(f"Wrote {pdf_path}")


def print_summary(rows: list[dict[str, Any]]) -> None:
    print()
    print("Loaded experiment results:")
    print()

    for row in rows:
        print(
            f"{row.get('run_id')}: "
            f"repos={row.get('total_processed_repositories')}, "
            f"throughput={row.get('throughput_repos_per_minute')} repos/min, "
            f"latency={row.get('average_latency_seconds')}s, "
            f"runners={row.get('analytics_num_runners')}, "
            f"workers={row.get('num_workers')}, "
            f"tokens={row.get('token_count')}, "
            f"batch={row.get('flush_every')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        default="experiments-results.jsonl",
        help="Path to experiments-results.jsonl",
    )

    parser.add_argument(
        "--output-dir",
        default="figures/experiments",
        help="Directory where plots and cleaned CSV will be saved",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = sort_rows(load_results(input_path))

    if not rows:
        raise RuntimeError("No experiment results were loaded.")

    print_summary(rows)
    write_clean_csv(rows, output_dir)

    save_all_experiments_bar_plot(
        rows=rows,
        y_key="throughput_repos_per_minute",
        ylabel="Throughput (repositories/minute)",
        title="Throughput Across All Experiments",
        output_base=output_dir / "all_experiments_throughput",
    )

    save_all_experiments_bar_plot(
        rows=rows,
        y_key="average_latency_seconds",
        ylabel="Average latency (seconds)",
        title="Average Latency Across All Experiments",
        output_base=output_dir / "all_experiments_average_latency",
    )

    save_all_experiments_bar_plot(
        rows=rows,
        y_key="total_processed_repositories",
        ylabel="Processed repositories",
        title="Processed Repositories Across All Experiments",
        output_base=output_dir / "all_experiments_processed_repositories",
    )


if __name__ == "__main__":
    main()
