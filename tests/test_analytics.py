from __future__ import annotations

import tempfile
import unittest
from collections import Counter
from pathlib import Path

from analytics.common import AnalyticsState, last_page, top_counter, top_dict


def _repo(repo_id: int, language: str | None = "Python", full_name: str | None = None) -> dict:
    return {
        "repo_id": repo_id,
        "language": language,
        "full_name": full_name or f"owner/repo{repo_id}",
    }


class AnalyticsStateTests(unittest.TestCase):
    def test_add_repo_dedupes_by_repo_id(self) -> None:
        state = AnalyticsState()

        self.assertTrue(state.add_repo(_repo(1, "Python")))
        # The same repo_id a second time is ignored and must not double-count.
        self.assertFalse(state.add_repo(_repo(1, "Python")))

        self.assertEqual(state.seen, {1})
        self.assertEqual(state.languages["Python"], 1)

    def test_add_repo_counts_languages(self) -> None:
        state = AnalyticsState()
        for repo_id, language in [(1, "Python"), (2, "Python"), (3, "Rust")]:
            state.add_repo(_repo(repo_id, language))

        self.assertEqual(state.languages["Python"], 2)
        self.assertEqual(state.languages["Rust"], 1)

    def test_add_repo_treats_missing_language_as_unknown(self) -> None:
        state = AnalyticsState()
        state.add_repo(_repo(1, None))

        self.assertEqual(state.languages["Unknown"], 1)

    def test_add_repo_records_commit_count_including_zero(self) -> None:
        state = AnalyticsState()
        state.add_repo(_repo(1, full_name="owner/has-commits"), {"commit_count": 42})
        state.add_repo(_repo(2, full_name="owner/empty"), {"commit_count": 0})
        # No commit_count in the enrichment payload -> not recorded at all.
        state.add_repo(_repo(3, full_name="owner/unknown"), {})

        self.assertEqual(state.commits["owner/has-commits"], 42)
        self.assertEqual(state.commits["owner/empty"], 0)
        self.assertNotIn("owner/unknown", state.commits)

    def test_add_repo_counts_test_and_ci_languages(self) -> None:
        state = AnalyticsState()
        state.add_repo(_repo(1, "Python"), {"has_tests": True, "has_ci": False})
        state.add_repo(_repo(2, "Python"), {"has_tests": True, "has_ci": True})
        state.add_repo(_repo(3, "Python"), {"has_tests": False, "has_ci": True})

        # q3 counts repos with tests; q4 needs tests AND ci.
        self.assertEqual(state.test_languages["Python"], 2)
        self.assertEqual(state.test_ci_languages["Python"], 1)

    def test_results_reports_counts_and_top_n(self) -> None:
        state = AnalyticsState()
        state.add_repo(_repo(1, "Python"), {"has_tests": True, "has_ci": True})
        state.add_repo(_repo(2, "Python"))
        state.add_repo(_repo(3, "Rust"))

        results = state.results(top_n=10)

        self.assertEqual(results["processed_unique_repositories"], 3)
        self.assertEqual(results["top_n"], 10)
        self.assertEqual(
            results["q1_top_languages_by_projects"],
            [{"name": "Python", "count": 2}, {"name": "Rust", "count": 1}],
        )
        self.assertEqual(
            results["q4_top_languages_with_tests_and_ci"],
            [{"name": "Python", "count": 1}],
        )

    def test_save_then_load_round_trips_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp) / "results"
            state_path = results_dir / "analytics_state.json"

            original = AnalyticsState()
            original.add_repo(
                _repo(1, "Python"),
                {"commit_count": 12, "has_tests": True, "has_ci": True},
            )
            original.add_repo(_repo(2, "Rust"), {"has_tests": True})
            original.save(results_dir, state_path, top_n=10)

            restored = AnalyticsState.load(state_path)

        # A restart must rebuild every counter, not just start from empty.
        self.assertEqual(restored.seen, {1, 2})
        self.assertEqual(restored.languages, original.languages)
        self.assertEqual(restored.commits, original.commits)
        self.assertEqual(restored.test_languages, original.test_languages)
        self.assertEqual(restored.test_ci_languages, original.test_ci_languages)

    def test_load_missing_state_file_returns_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            restored = AnalyticsState.load(Path(tmp) / "absent.json")

        self.assertEqual(restored.seen, set())
        self.assertEqual(restored.results(10)["processed_unique_repositories"], 0)

    def test_load_corrupt_state_file_returns_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "analytics_state.json"
            state_path.write_text("{ not valid json", encoding="utf-8")

            restored = AnalyticsState.load(state_path)

        # A corrupt state file must degrade to a fresh start, not crash.
        self.assertEqual(restored.seen, set())
        self.assertEqual(restored.results(10)["processed_unique_repositories"], 0)


class HelperTests(unittest.TestCase):
    def test_top_counter_sorts_by_count_and_limits(self) -> None:
        counter = Counter({"Python": 5, "Rust": 9, "Go": 1})

        self.assertEqual(
            top_counter(counter, 2),
            [{"name": "Rust", "count": 9}, {"name": "Python", "count": 5}],
        )

    def test_top_dict_sorts_by_count_and_limits(self) -> None:
        values = {"owner/a": 3, "owner/b": 10, "owner/c": 7}

        self.assertEqual(
            top_dict(values, 2),
            [{"name": "owner/b", "count": 10}, {"name": "owner/c", "count": 7}],
        )

    def test_last_page_extracts_page_number(self) -> None:
        header = (
            '<https://api.github.com/repositories/1/commits?per_page=1&page=2>; rel="next", '
            '<https://api.github.com/repositories/1/commits?per_page=1&page=347>; rel="last"'
        )
        self.assertEqual(last_page(header), 347)

    def test_last_page_returns_none_without_last_rel(self) -> None:
        header = '<https://api.github.com/repositories/1/commits?per_page=1&page=2>; rel="next"'
        self.assertIsNone(last_page(header))
        self.assertIsNone(last_page(""))


if __name__ == "__main__":
    unittest.main()
