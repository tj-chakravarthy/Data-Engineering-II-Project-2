"""Command-line entry point for the crawler-owned pipeline stage."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from crawler.cli_args import (
    add_crawl_args,
    add_rate_limit_args,
    build_crawl_config,
)
from crawler.crawl import crawl_window
from crawler.github_client import GitHubClient


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = build_crawl_config(args)
    client = GitHubClient(
        max_wait_seconds=args.max_wait_seconds,
        max_total_wait_seconds=args.max_total_wait_seconds,
    )
    records, stats = crawl_window(client, config)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as file:
        for record in records:
            file.write(record.to_json_line())

    logging.info("wrote deduplicated records to %s", args.output)
    logging.info(
        "stats: emitted=%d fetched=%d cache_written=%d loaded_from_cache=%d "
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl GitHub repository metadata with cache and deduplication."
    )
    add_crawl_args(parser)
    add_rate_limit_args(parser)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/output/repos.ndjson"),
        help="path to write the deduplicated NDJSON output; overwritten on each run",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
