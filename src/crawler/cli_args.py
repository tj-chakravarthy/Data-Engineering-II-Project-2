"""Shared argparse helpers for crawler-driven entry points.

Both ``crawler.cli`` (file output) and ``streaming.pulsar_producer`` (live
publish) need the same crawler-shaping flags. Defining them here keeps the
two entry points from drifting.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

from crawler.crawl import CrawlConfig


def add_crawl_args(parser: argparse.ArgumentParser) -> None:
    """Register the crawler-shaping flags common to all entry points."""
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--end", help="end date as YYYY-MM-DD; default: today UTC")
    parser.add_argument(
        "--date-field",
        choices=["created", "pushed", "created-or-pushed"],
        default="created",
        help=(
            "GitHub search date qualifier to slice by. created-or-pushed runs "
            "both created and pushed date-sliced searches and globally "
            "deduplicates. GitHub does not support an updated: search "
            "qualifier; pushed: (last commit) is used to capture the PDF's "
            "'updated' criterion."
        ),
    )
    parser.add_argument(
        "--query",
        default="",
        help='additional GitHub search qualifiers, e.g. "stars:>=10 archived:false"',
    )
    parser.add_argument("--cache-dir", type=Path, default=Path("data/cache"))
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--limit-per-day", type=int)
    parser.add_argument(
        "--limit",
        type=int,
        help="global emit limit; used for smoke tests, disables cache writes",
    )
    parser.add_argument("--log-every", type=int, default=100)
    parser.add_argument("--memory-log-every", type=int, default=1000)
    parser.add_argument(
        "--max-memory-mb",
        type=int,
        help="fail the crawl if tracked Python memory exceeds this limit",
    )


def add_rate_limit_args(parser: argparse.ArgumentParser) -> None:
    """Register the GitHubClient rate-limit flags."""
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


def resolve_end_date(end: str | None) -> date:
    """Return the configured end date, defaulting to today in UTC."""
    if end:
        return datetime.strptime(end, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date()


def build_crawl_config(args: argparse.Namespace) -> CrawlConfig:
    """Assemble a CrawlConfig from a Namespace produced by ``add_crawl_args``."""
    return CrawlConfig(
        end_date=resolve_end_date(args.end),
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
