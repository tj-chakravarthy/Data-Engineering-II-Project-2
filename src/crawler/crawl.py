"""Configurable, cache-backed GitHub repository crawler."""

from __future__ import annotations

import logging
import os
import tracemalloc
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from crawler.cache import RepoCache
from crawler.github_client import GitHubClient
from crawler.models import RepoRecord

log = logging.getLogger(__name__)

VALID_DATE_FIELDS = {"created", "updated", "pushed", "created-or-updated"}


class MemoryLimitExceeded(RuntimeError):
    """Raised when crawler memory tracking crosses the configured limit."""


@dataclass(frozen=True)
class CrawlConfig:
    """Crawler settings owned by the crawler role."""

    end_date: date
    days: int = 365
    date_field: str = "created"
    query_suffix: str = ""
    sort: str = "stars"
    order: str = "desc"
    cache_dir: Path = Path("data/cache")
    use_cache: bool = True
    refresh_cache: bool = False
    limit_per_day: int | None = None
    global_limit: int | None = None
    log_every: int = 100
    memory_log_every: int = 1000
    max_memory_mb: int | None = None

    @classmethod
    def for_utc_today(cls, **kwargs: object) -> "CrawlConfig":
        return cls(end_date=datetime.now(timezone.utc).date(), **kwargs)

    def __post_init__(self) -> None:
        if self.days < 1:
            raise ValueError("days must be at least 1")
        if self.date_field not in VALID_DATE_FIELDS:
            allowed = ", ".join(sorted(VALID_DATE_FIELDS))
            raise ValueError(f"date_field must be one of: {allowed}")
        if self.max_memory_mb is not None and self.max_memory_mb < 1:
            raise ValueError("max_memory_mb must be at least 1 when set")


@dataclass
class CrawlStats:
    """Operational counters for logs and report notes."""

    fetched: int = 0
    loaded_from_cache: int = 0
    written_to_cache: int = 0
    duplicate_in_slice: int = 0
    duplicate_global: int = 0
    emitted: int = 0
    slices_from_api: int = 0
    slices_from_cache: int = 0
    memory_samples: int = 0
    peak_python_memory_kb: int = 0


def crawl_window(
    client: GitHubClient,
    config: CrawlConfig,
) -> tuple[Iterator[RepoRecord], CrawlStats]:
    """Return a lazy iterator of deduplicated records plus mutable stats."""
    stats = CrawlStats()
    cache = RepoCache(config.cache_dir)
    seen_global: set[str] = set()
    if (config.memory_log_every or config.max_memory_mb) and not tracemalloc.is_tracing():
        tracemalloc.start()

    def iterator() -> Iterator[RepoRecord]:
        for day in date_slices(config.end_date, config.days):
            day_records = _records_for_day(client, cache, config, day, stats)
            for record in day_records:
                key = record.dedupe_key()
                if key in seen_global:
                    stats.duplicate_global += 1
                    continue
                seen_global.add(key)
                stats.emitted += 1
                if config.log_every and stats.emitted % config.log_every == 0:
                    log.info("emitted %d deduplicated repos", stats.emitted)
                should_log_memory = bool(
                    config.memory_log_every
                    and stats.emitted % config.memory_log_every == 0
                )
                should_check_memory = config.max_memory_mb is not None
                if should_log_memory or should_check_memory:
                    _sample_memory(stats, config, log_sample=should_log_memory)
                yield record
                if config.global_limit and stats.emitted >= config.global_limit:
                    return

    return iterator(), stats


def date_slices(end_date: date, days: int) -> Iterator[str]:
    """Yield YYYY-MM-DD strings from newest to oldest."""
    for offset in range(days):
        yield (end_date - timedelta(days=offset)).strftime("%Y-%m-%d")


def build_search_query(date_field: str, day: str, query_suffix: str = "") -> str:
    base = f"{date_field}:{day}"
    return f"{base} {query_suffix.strip()}" if query_suffix.strip() else base


def date_fields_for_mode(date_field: str) -> tuple[str, ...]:
    """Expand the public crawl mode into concrete GitHub date qualifiers."""
    if date_field == "created-or-updated":
        return ("created", "updated")
    return (date_field,)


def _records_for_day(
    client: GitHubClient,
    cache: RepoCache,
    config: CrawlConfig,
    day: str,
    stats: CrawlStats,
) -> Iterator[RepoRecord]:
    for date_field in date_fields_for_mode(config.date_field):
        if config.global_limit:
            remaining = config.global_limit - stats.emitted
            if remaining <= 0:
                return
        else:
            remaining = None

        yield from _records_for_slice(
            client,
            cache,
            config,
            date_field,
            day,
            stats,
            max_records=remaining,
        )


def _records_for_slice(
    client: GitHubClient,
    cache: RepoCache,
    config: CrawlConfig,
    date_field: str,
    day: str,
    stats: CrawlStats,
    max_records: int | None,
) -> Iterator[RepoRecord]:
    if (
        config.use_cache
        and not config.refresh_cache
        and cache.has_slice(date_field, day, config.query_suffix)
    ):
        stats.slices_from_cache += 1
        count = 0
        for record in cache.read_slice(date_field, day, config.query_suffix):
            stats.loaded_from_cache += 1
            yield record
            count += 1
            if max_records is not None and count >= max_records:
                return
        return

    stats.slices_from_api += 1
    query = build_search_query(date_field, day, config.query_suffix)
    records = _fetch_day_records(
        client,
        query,
        day,
        config,
        stats,
        max_records=max_records,
    )

    writes_complete_slice = (
        config.use_cache
        and max_records is None
        and config.limit_per_day is None
    )
    if writes_complete_slice:
        written, duplicates = cache.write_slice(
            date_field,
            day,
            records,
            config.query_suffix,
        )
        stats.written_to_cache += written
        stats.duplicate_in_slice += duplicates
        yield from cache.read_slice(date_field, day, config.query_suffix)
        return

    seen_slice: set[str] = set()
    for record in records:
        key = record.dedupe_key()
        if key in seen_slice:
            stats.duplicate_in_slice += 1
            continue
        seen_slice.add(key)
        yield record


def _fetch_day_records(
    client: GitHubClient,
    query: str,
    day: str,
    config: CrawlConfig,
    stats: CrawlStats,
    max_records: int | None = None,
) -> Iterator[RepoRecord]:
    count = 0
    effective_limit = _effective_fetch_limit(config.limit_per_day, max_records)
    for item in client.search_repositories(query, sort=config.sort, order=config.order):
        record = RepoRecord.from_github_item(item, crawl_day=day)
        stats.fetched += 1
        yield record
        count += 1
        if effective_limit is not None and count >= effective_limit:
            return


def _effective_fetch_limit(
    limit_per_day: int | None,
    max_records: int | None,
) -> int | None:
    limits = [limit for limit in (limit_per_day, max_records) if limit is not None]
    return min(limits) if limits else None


def _sample_memory(
    stats: CrawlStats,
    config: CrawlConfig,
    log_sample: bool,
) -> None:
    stats.memory_samples += 1
    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    current_kb = current_bytes // 1024
    peak_kb = peak_bytes // 1024
    stats.peak_python_memory_kb = max(stats.peak_python_memory_kb, peak_kb)
    if log_sample:
        log.info(
            "memory sample %d: python_current=%d KB python_peak=%d KB",
            stats.memory_samples,
            current_kb,
            peak_kb,
        )
    if config.max_memory_mb is None:
        return
    limit_kb = config.max_memory_mb * 1024
    if peak_kb > limit_kb:
        raise MemoryLimitExceeded(
            "Crawler exceeded configured Python memory limit: "
            f"peak={peak_kb} KB limit={limit_kb} KB. "
            "Use a smaller date window, lower limits, or rely on cache-backed runs."
        )


def load_dotenv(path: Path = Path(".env")) -> None:
    """Tiny `.env` loader so local runs do not need an extra dependency."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)
