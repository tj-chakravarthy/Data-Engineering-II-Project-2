"""Command-line entry point for the crawler-owned pipeline stage."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from crawler.crawl import CrawlConfig, crawl_window, load_dotenv
from crawler.github_client import GitHubClient


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv()

    end_date = (
        datetime.strptime(args.end, "%Y-%m-%d").date()
        if args.end
        else datetime.now(timezone.utc).date()
    )
    config = CrawlConfig(
        end_date=end_date,
        days=args.days,
        date_field=args.date_field,
        query_suffix=args.query,
        cache_dir=args.cache_dir,
        use_cache=not args.no_cache,
        refresh_cache=args.refresh_cache,
        limit_per_day=args.limit_per_day,
        global_limit=args.limit,
        log_every=args.log_every,
        memory_log_every=args.memory_log_every,
        max_memory_mb=args.max_memory_mb,
    )

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
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--end", help="end date as YYYY-MM-DD; default: today UTC")
    parser.add_argument(
        "--date-field",
        choices=["created", "pushed", "created-or-pushed"],
        default="created",
        help=(
            "GitHub search date qualifier to slice by. created-or-pushed runs both "
            "created and pushed date-sliced searches and globally deduplicates. "
            "GitHub does not support an updated: search qualifier; pushed: (last "
            "commit) is used to capture the PDF's 'updated' criterion."
        ),
    )
    parser.add_argument(
        "--query",
        default="",
        help='additional GitHub search qualifiers, e.g. "stars:>=10 archived:false"',
    )
    parser.add_argument("--cache-dir", type=Path, default=Path("data/cache"))
    parser.add_argument("--output", type=Path, default=Path("data/output/repos.ndjson"))
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--limit-per-day", type=int)
    parser.add_argument("--limit", type=int, help="global output limit for local tests")
    parser.add_argument("--log-every", type=int, default=100)
    parser.add_argument("--memory-log-every", type=int, default=1000)
    parser.add_argument(
        "--max-memory-mb",
        type=int,
        help="fail the crawl if tracked Python memory exceeds this limit",
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=60,
        help="cap on a single rate-limit sleep between token-pool retries",
    )
    parser.add_argument(
        "--max-total-wait-seconds",
        type=int,
        default=3600,
        help="total rate-limit wait budget per GET before raising RateLimitError",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
