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
from crawler.github_client import GitHubClient, SearchMetadata
from crawler.models import RepoRecord

log = logging.getLogger(__name__)

VALID_DATE_FIELDS = {"created", "pushed", "created-or-pushed"}
GITHUB_SEARCH_RESULT_CAP = 1000


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
    search_splits: int = 0
    search_cap_warnings: int = 0
    incomplete_search_warnings: int = 0


@dataclass(frozen=True)
class SearchRange:
    """Inclusive UTC time range used to keep GitHub search queries below the cap."""

    start: datetime
    end: datetime

    def expression(self) -> str:
        return f"{_github_datetime(self.start)}..{_github_datetime(self.end)}"

    def can_split(self) -> bool:
        return self.start < self.end

    def split(self) -> tuple["SearchRange", "SearchRange"]:
        midpoint = self.start + (self.end - self.start) / 2
        midpoint = midpoint.replace(microsecond=0)
        if midpoint >= self.end:
            midpoint = self.start
        return (
            SearchRange(self.start, midpoint),
            SearchRange(midpoint + timedelta(seconds=1), self.end),
        )


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


def build_search_query(
    date_field: str,
    date_expression: str,
    query_suffix: str = "",
) -> str:
    base = f"{date_field}:{date_expression}"
    return f"{base} {query_suffix.strip()}" if query_suffix.strip() else base


def date_fields_for_mode(date_field: str) -> tuple[str, ...]:
    """Expand the public crawl mode into concrete GitHub date qualifiers."""
    if date_field == "created-or-pushed":
        return ("created", "pushed")
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
    records = _fetch_day_records(
        client,
        date_field,
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
        def on_write() -> None:
            stats.written_to_cache += 1

        def on_duplicate() -> None:
            stats.duplicate_in_slice += 1

        yield from cache.write_slice_streaming(
            date_field,
            day,
            records,
            config.query_suffix,
            on_write=on_write,
            on_duplicate=on_duplicate,
        )
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
    date_field: str,
    day: str,
    config: CrawlConfig,
    stats: CrawlStats,
    max_records: int | None = None,
) -> Iterator[RepoRecord]:
    count = 0
    effective_limit = _effective_fetch_limit(config.limit_per_day, max_records)
    if effective_limit is None:
        queries = _complete_slice_queries(client, date_field, day, config, stats)
        on_search_metadata = None
    else:
        queries = [
            build_search_query(
                date_field,
                _full_day_range(day).expression(),
                config.query_suffix,
            )
        ]
        on_search_metadata = lambda metadata: _record_search_metadata(metadata, stats)

    for query in queries:
        for item in client.search_repositories(
            query,
            sort=config.sort,
            order=config.order,
            on_search_metadata=on_search_metadata,
        ):
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


def _complete_slice_queries(
    client: GitHubClient,
    date_field: str,
    day: str,
    config: CrawlConfig,
    stats: CrawlStats,
) -> Iterator[str]:
    return _split_range_queries(
        client,
        date_field,
        _full_day_range(day),
        config,
        stats,
    )


def _split_range_queries(
    client: GitHubClient,
    date_field: str,
    search_range: SearchRange,
    config: CrawlConfig,
    stats: CrawlStats,
) -> Iterator[str]:
    query = build_search_query(date_field, search_range.expression(), config.query_suffix)
    metadata = client.search_repositories_metadata(
        query,
        sort=config.sort,
        order=config.order,
    )

    # ``incomplete_results`` is a warning regardless of whether we can split.
    _record_incomplete_warning(metadata, stats)

    within_cap = (
        metadata.total_count is None
        or metadata.total_count <= GITHUB_SEARCH_RESULT_CAP
    )
    if within_cap:
        yield query
        return

    if not search_range.can_split():
        # Over the cap and the range cannot be subdivided further: this is a
        # real cap warning. ``_record_incomplete_warning`` above already
        # recorded any incomplete-results signal, so we only need the cap
        # warning here.
        _record_cap_warning(metadata, stats)
        yield query
        return

    stats.search_splits += 1
    log.info(
        "splitting GitHub search query with total_count=%d: %s",
        metadata.total_count,
        query,
    )
    left, right = search_range.split()
    yield from _split_range_queries(client, date_field, left, config, stats)
    yield from _split_range_queries(client, date_field, right, config, stats)


def _record_search_metadata(metadata: SearchMetadata, stats: CrawlStats) -> None:
    """Record both warnings from a metadata response.

    Used by callers that always want both signals (e.g. the limited / non-
    splitting path in ``_fetch_day_records``). The splitter calls the
    individual helpers below so that cap warnings are not double-counted
    when the splitter both detects and recovers from an over-cap range.
    """
    _record_cap_warning(metadata, stats)
    _record_incomplete_warning(metadata, stats)


def _record_cap_warning(metadata: SearchMetadata, stats: CrawlStats) -> None:
    if (
        metadata.total_count is not None
        and metadata.total_count > GITHUB_SEARCH_RESULT_CAP
    ):
        stats.search_cap_warnings += 1
        log.warning(
            "GitHub search cap warning: query=%r reports total_count=%d; "
            "GitHub exposes only the first 1000 search results for one query. "
            "Use a narrower query/date partition before treating this slice as complete.",
            metadata.query,
            metadata.total_count,
        )


def _record_incomplete_warning(metadata: SearchMetadata, stats: CrawlStats) -> None:
    if metadata.incomplete_results:
        stats.incomplete_search_warnings += 1
        log.warning(
            "GitHub marked search results incomplete for query=%r; rerun or narrow the query.",
            metadata.query,
        )


def _full_day_range(day: str) -> SearchRange:
    """Return the inclusive UTC range covering one calendar day.

    The end is `23:59:59`, not `23:59:59.999999`, because GitHub timestamps
    are second-resolution: the API stores `pushed_at` / `created_at` as
    `YYYY-MM-DDTHH:MM:SSZ` with no sub-second component, and the search
    qualifier `<field>:start..end` is inclusive on both ends. There is no
    sub-second gap to miss.
    """
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return SearchRange(start=start, end=end)


def _github_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
