from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from crawler.cache import RepoCache
from crawler.crawl import (
    CrawlConfig,
    build_search_query,
    crawl_window,
    date_fields_for_mode,
    date_slices,
)
from crawler.github_client import SearchMetadata
from crawler.models import RepoRecord


class FakeClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def search_repositories_metadata(self, query: str, sort: str, order: str):
        return SearchMetadata(
            query=query,
            total_count=3,
            incomplete_results=False,
        )

    def search_repositories(self, query: str, sort: str, order: str, on_search_metadata=None):
        self.queries.append(query)
        day = query.split(":", 1)[1][:10]
        if on_search_metadata:
            on_search_metadata(
                SearchMetadata(
                    query=query,
                    total_count=3,
                    incomplete_results=False,
                )
            )
        yield _github_item(1, "owner/one", day)
        yield _github_item(1, "owner/one", day)
        yield _github_item(2, "owner/two", day)


class CappedFakeClient:
    def search_repositories(self, query: str, sort: str, order: str, on_search_metadata=None):
        if on_search_metadata:
            on_search_metadata(
                SearchMetadata(
                    query=query,
                    total_count=1500,
                    incomplete_results=True,
                )
            )
        yield _github_item(1, "owner/one", query.split(":", 1)[1][:10])


class SplittingFakeClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def search_repositories_metadata(self, query: str, sort: str, order: str):
        is_full_day = (
            "T00:00:00Z..2026-05-19T23:59:59Z" in query
        )
        return SearchMetadata(
            query=query,
            total_count=1500 if is_full_day else 2,
            incomplete_results=False,
        )

    def search_repositories(self, query: str, sort: str, order: str, on_search_metadata=None):
        self.queries.append(query)
        day = query.split(":", 1)[1][:10]
        yield _github_item(len(self.queries), f"owner/{len(self.queries)}", day)


class CrawlerTests(unittest.TestCase):
    def test_date_slices_newest_to_oldest(self) -> None:
        self.assertEqual(
            list(date_slices(date(2026, 5, 19), 3)),
            ["2026-05-19", "2026-05-18", "2026-05-17"],
        )

    def test_build_search_query(self) -> None:
        self.assertEqual(build_search_query("created", "2026-05-19"), "created:2026-05-19")
        self.assertEqual(
            build_search_query("updated", "2026-05-19", "stars:>=10 archived:false"),
            "updated:2026-05-19 stars:>=10 archived:false",
        )

    def test_date_field_modes(self) -> None:
        self.assertEqual(date_fields_for_mode("created"), ("created",))
        self.assertEqual(
            date_fields_for_mode("created-or-updated"),
            ("created", "updated"),
        )

    def test_cache_deduplicates_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = RepoCache(Path(tmp))
            written, duplicates = cache.write_slice(
                "created",
                "2026-05-19",
                [
                    _record(1, "owner/one"),
                    _record(1, "owner/one"),
                    _record(2, "owner/two"),
                ],
            )
            self.assertEqual(written, 2)
            self.assertEqual(duplicates, 1)
            self.assertEqual(len(list(cache.read_slice("created", "2026-05-19"))), 2)

    def test_crawl_window_deduplicates_before_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = CrawlConfig(
                end_date=date(2026, 5, 19),
                days=1,
                cache_dir=Path(tmp),
                log_every=0,
                memory_log_every=0,
            )
            records, stats = crawl_window(FakeClient(), config)  # type: ignore[arg-type]
            self.assertEqual([record.repo_id for record in records], [1, 2])
            self.assertEqual(stats.fetched, 3)
            self.assertEqual(stats.duplicate_in_slice, 1)
            self.assertEqual(stats.emitted, 2)

    def test_global_limit_bounds_api_fetch_and_avoids_partial_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            config = CrawlConfig(
                end_date=date(2026, 5, 19),
                days=1,
                cache_dir=cache_dir,
                global_limit=1,
                log_every=0,
                memory_log_every=0,
            )
            records, stats = crawl_window(FakeClient(), config)  # type: ignore[arg-type]
            self.assertEqual([record.repo_id for record in records], [1])
            self.assertEqual(stats.fetched, 1)
            self.assertFalse((cache_dir / "repos_created_2026-05-19.ndjson").exists())

    def test_created_or_updated_mode_runs_both_queries_and_global_dedupes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = FakeClient()
            config = CrawlConfig(
                end_date=date(2026, 5, 19),
                days=1,
                date_field="created-or-updated",
                cache_dir=Path(tmp),
                log_every=0,
                memory_log_every=0,
            )
            records, stats = crawl_window(client, config)  # type: ignore[arg-type]
            self.assertEqual([record.repo_id for record in records], [1, 2])
            self.assertEqual(
                client.queries,
                [
                    "created:2026-05-19T00:00:00Z..2026-05-19T23:59:59Z",
                    "updated:2026-05-19T00:00:00Z..2026-05-19T23:59:59Z",
                ],
            )
            self.assertEqual(stats.fetched, 6)
            self.assertEqual(stats.duplicate_global, 2)

    def test_warns_when_github_search_slice_exceeds_cap_or_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = CrawlConfig(
                end_date=date(2026, 5, 19),
                days=1,
                cache_dir=Path(tmp),
                global_limit=1,
                log_every=0,
                memory_log_every=0,
            )
            records, stats = crawl_window(CappedFakeClient(), config)  # type: ignore[arg-type]
            self.assertEqual([record.repo_id for record in records], [1])
            self.assertEqual(stats.search_cap_warnings, 1)
            self.assertEqual(stats.incomplete_search_warnings, 1)

    def test_complete_crawl_splits_search_ranges_above_github_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = SplittingFakeClient()
            config = CrawlConfig(
                end_date=date(2026, 5, 19),
                days=1,
                cache_dir=Path(tmp),
                log_every=0,
                memory_log_every=0,
            )
            records, stats = crawl_window(client, config)  # type: ignore[arg-type]

            self.assertEqual([record.repo_id for record in records], [1, 2])
            self.assertEqual(len(client.queries), 2)
            self.assertEqual(stats.search_splits, 1)
            self.assertEqual(stats.search_cap_warnings, 0)


def _record(repo_id: int, full_name: str) -> RepoRecord:
    return RepoRecord(
        repo_id=repo_id,
        full_name=full_name,
        language="Python",
        stars=1,
        forks=0,
        created_at="2026-05-19T00:00:00Z",
        updated_at="2026-05-19T00:00:00Z",
        pushed_at="2026-05-19T00:00:00Z",
        size_kb=1,
        default_branch="main",
        crawl_day="2026-05-19",
    )


def _github_item(repo_id: int, full_name: str, day: str) -> dict[str, object]:
    return {
        "id": repo_id,
        "full_name": full_name,
        "language": "Python",
        "stargazers_count": 1,
        "forks_count": 0,
        "created_at": f"{day}T00:00:00Z",
        "updated_at": f"{day}T00:00:00Z",
        "pushed_at": f"{day}T00:00:00Z",
        "size": 1,
        "default_branch": "main",
        "archived": False,
        "topics": [],
        "open_issues_count": 0,
    }


if __name__ == "__main__":
    unittest.main()
