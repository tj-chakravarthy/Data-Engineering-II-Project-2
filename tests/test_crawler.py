from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from crawler.cache import RepoCache
from crawler.crawl import (
    CrawlConfig,
    CrawlStats,
    SearchRange,
    _split_range_queries,
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
            build_search_query("pushed", "2026-05-19", "stars:>=10 archived:false"),
            "pushed:2026-05-19 stars:>=10 archived:false",
        )

    def test_date_field_modes(self) -> None:
        self.assertEqual(date_fields_for_mode("created"), ("created",))
        self.assertEqual(
            date_fields_for_mode("created-or-pushed"),
            ("created", "pushed"),
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

    def test_cache_streams_records_before_promoting_complete_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = RepoCache(Path(tmp))
            final_path = cache.path_for_slice("created", "2026-05-19")
            tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
            writes = 0
            duplicates = 0

            def on_write() -> None:
                nonlocal writes
                writes += 1

            def on_duplicate() -> None:
                nonlocal duplicates
                duplicates += 1

            stream = cache.write_slice_streaming(
                "created",
                "2026-05-19",
                [
                    _record(1, "owner/one"),
                    _record(1, "owner/one"),
                    _record(2, "owner/two"),
                ],
                on_write=on_write,
                on_duplicate=on_duplicate,
            )

            first = next(stream)
            self.assertEqual(first.repo_id, 1)
            self.assertEqual(writes, 1)
            self.assertEqual(duplicates, 0)
            self.assertTrue(tmp_path.exists())
            self.assertFalse(final_path.exists())

            self.assertEqual([record.repo_id for record in stream], [2])
            self.assertEqual(writes, 2)
            self.assertEqual(duplicates, 1)
            self.assertTrue(final_path.exists())
            self.assertFalse(tmp_path.exists())

    def test_cache_removes_temp_file_when_stream_is_interrupted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = RepoCache(Path(tmp))
            final_path = cache.path_for_slice("created", "2026-05-19")
            tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")

            stream = cache.write_slice_streaming(
                "created",
                "2026-05-19",
                [_record(1, "owner/one"), _record(2, "owner/two")],
            )

            self.assertEqual(next(stream).repo_id, 1)
            self.assertTrue(tmp_path.exists())
            stream.close()
            self.assertFalse(tmp_path.exists())
            self.assertFalse(final_path.exists())

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

    def test_crawl_window_stamps_emitted_at(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = CrawlConfig(
                end_date=date(2026, 5, 19),
                days=1,
                cache_dir=Path(tmp),
                log_every=0,
                memory_log_every=0,
            )
            before = datetime.now(timezone.utc)
            records, _ = crawl_window(FakeClient(), config)  # type: ignore[arg-type]
            stamps = [record.emitted_at for record in records]
            after = datetime.now(timezone.utc)
            # Pin both format (microsecond UTC ISO) and recency: a regression
            # that emits stale, malformed, or "1970-01-01..." would slip past
            # a non-None + endswith("Z") check.
            self.assertEqual(len(stamps), 2)
            for stamp in stamps:
                self.assertIsNotNone(stamp)
                parsed = datetime.strptime(
                    stamp, "%Y-%m-%dT%H:%M:%S.%fZ"
                ).replace(tzinfo=timezone.utc)
                self.assertGreaterEqual(parsed, before)
                self.assertLessEqual(parsed, after)

    def test_emitted_at_round_trips_through_json(self) -> None:
        """emitted_at must survive serialization so cache reads and Pulsar
        messages preserve the emit timestamp the aggregator reads for
        latency math."""
        record = _record(1, "owner/repo")
        stamped = RepoRecord.from_dict(
            {**record.to_dict(), "emitted_at": "2026-05-23T14:30:00.123456Z"}
        )
        revived = RepoRecord.from_json_line(stamped.to_json_line())
        self.assertEqual(revived.emitted_at, "2026-05-23T14:30:00.123456Z")
        # A record constructed without emitted_at defaults to None and still
        # round-trips cleanly.
        plain = RepoRecord.from_json_line(_record(2, "owner/two").to_json_line())
        self.assertIsNone(plain.emitted_at)

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

    def test_created_or_pushed_mode_runs_both_queries_and_global_dedupes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = FakeClient()
            config = CrawlConfig(
                end_date=date(2026, 5, 19),
                days=1,
                date_field="created-or-pushed",
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
                    "pushed:2026-05-19T00:00:00Z..2026-05-19T23:59:59Z",
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

    def test_splitter_counts_each_warning_once_when_unsplittable(self) -> None:
        """Regression for double-counted warnings: when a leaf range is both
        over-cap AND incomplete AND can't split further, both counters must
        increment exactly once, not twice."""

        class OverCapIncompleteClient:
            def search_repositories_metadata(
                self, query: str, sort: str, order: str
            ) -> SearchMetadata:
                return SearchMetadata(
                    query=query,
                    total_count=1500,
                    incomplete_results=True,
                )

        moment = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
        unsplittable = SearchRange(start=moment, end=moment)
        self.assertFalse(unsplittable.can_split())

        config = CrawlConfig(
            end_date=date(2026, 5, 19),
            days=1,
            cache_dir=Path("unused-for-this-test"),
            log_every=0,
            memory_log_every=0,
        )
        stats = CrawlStats()

        queries = list(
            _split_range_queries(
                OverCapIncompleteClient(),  # type: ignore[arg-type]
                "created",
                unsplittable,
                config,
                stats,
            )
        )

        self.assertEqual(len(queries), 1)
        self.assertEqual(stats.search_cap_warnings, 1)
        self.assertEqual(stats.incomplete_search_warnings, 1)



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
