from __future__ import annotations

import tempfile
import unittest
from collections import Counter
import json
from pathlib import Path
from unittest.mock import patch

import requests

from analytics.aggregator import (
    enriched_records,
    process_enriched_records,
    process_enriched_repo,
    save_and_plot,
)
from analytics.common import (
    AnalyticsState,
    commit_count,
    config,
    last_page,
    path_matches,
    should_idle_flush,
    top_counter,
    top_dict,
)
from analytics.plot_results import plot_aggregate_payload
from analytics.runner import process_repo, send_enriched_batch
from crawler.github_client import RateLimitError


def _repo(repo_id: int, language: str | None = "Python", full_name: str | None = None) -> dict:
    return {
        "repo_id": repo_id,
        "language": language,
        "full_name": full_name or f"owner/repo{repo_id}",
    }


class FakeProducer:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[bytes] = []

    def send(self, payload: bytes) -> None:
        if self.fail:
            raise RuntimeError("simulated publish failure")
        self.messages.append(payload)


class FakeGitHubClient:
    def __init__(self, side_effects) -> None:
        self.side_effects = list(side_effects)
        self.calls: list[tuple[str, dict | None]] = []

    def get(self, path: str, params: dict | None = None):
        self.calls.append((path, params))
        result = self.side_effects.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class FakeResponse:
    def __init__(self, status_code: int = 200, payload=None, headers=None) -> None:
        self.status_code = status_code
        self._payload = [] if payload is None else payload
        self.headers = headers or {}
        self.text = ""

    def json(self):
        return self._payload


def _http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(response=response)


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

    def test_process_repo_returns_one_enriched_record(self) -> None:
        repo = _repo(1, "Python")

        enriched = process_repo(repo, None)

        self.assertEqual(enriched["repo_id"], 1)
        self.assertIsNone(enriched["commit_count"])
        self.assertFalse(enriched["has_tests"])
        self.assertFalse(enriched["has_ci"])

    def test_send_enriched_batch_publishes_records_envelope(self) -> None:
        producer = FakeProducer()
        records = [_repo(1, "Python"), _repo(2, "Rust")]

        send_enriched_batch(producer, records)

        self.assertEqual(len(producer.messages), 1)
        payload = json.loads(producer.messages[0].decode("utf-8"))
        self.assertEqual(payload, {"records": records})

    def test_send_enriched_batch_raises_when_publish_fails(self) -> None:
        producer = FakeProducer(fail=True)

        with self.assertRaises(RuntimeError):
            send_enriched_batch(producer, [_repo(1, "Python")])

    def test_process_enriched_repo_updates_single_aggregator_state(self) -> None:
        state = AnalyticsState()
        repo = {
            **_repo(1, "Python", "owner/one"),
            "commit_count": 42,
            "has_tests": True,
            "has_ci": True,
        }

        self.assertTrue(process_enriched_repo(repo, state))
        self.assertFalse(process_enriched_repo(repo, state))
        self.assertEqual(state.seen, {1})
        self.assertEqual(state.languages["Python"], 1)
        self.assertEqual(state.commits["owner/one"], 42)
        self.assertEqual(state.test_languages["Python"], 1)
        self.assertEqual(state.test_ci_languages["Python"], 1)

    def test_process_enriched_records_updates_state_from_batch(self) -> None:
        state = AnalyticsState()
        records = [
            {**_repo(1, "Python", "owner/one"), "commit_count": 42},
            {**_repo(2, "Rust", "owner/two"), "commit_count": 7},
        ]

        self.assertTrue(process_enriched_records(records, state))
        self.assertEqual(state.seen, {1, 2})
        self.assertEqual(state.languages["Python"], 1)
        self.assertEqual(state.languages["Rust"], 1)

    def test_enriched_records_accepts_batch_and_single_record(self) -> None:
        record = _repo(1, "Python")
        batch = {"records": [record]}

        self.assertEqual(enriched_records(batch), [record])
        self.assertEqual(enriched_records(record), [record])

    def test_enriched_records_rejects_invalid_payload(self) -> None:
        with self.assertRaises(ValueError):
            enriched_records(["not", "a", "dict"])

    def test_aggregator_save_and_plot_writes_results_and_plots(self) -> None:
        state = AnalyticsState()
        state.add_repo(
            {
                **_repo(1, "Python", "owner/one"),
                "commit_count": 42,
                "has_tests": True,
                "has_ci": True,
            },
            {
                "commit_count": 42,
                "has_tests": True,
                "has_ci": True,
            },
        )
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {
                "results_dir": Path(tmp) / "results",
                "figures_dir": Path(tmp) / "figures",
                "state_path": Path(tmp) / "results" / "analytics_state.json",
                "top_n": 10,
            }

            save_and_plot(state, cfg)

            self.assertTrue((cfg["results_dir"] / "all_results.json").exists())
            self.assertTrue((cfg["figures_dir"] / "q1_languages.png").exists())
            self.assertTrue((cfg["figures_dir"] / "q2_commits.png").exists())
            saved = json.loads((cfg["results_dir"] / "all_results.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["processed_unique_repositories"], 1)
            self.assertEqual(
                saved["q2_top_projects_by_commits"],
                [{"name": "owner/one", "count": 42}],
            )

    def test_save_and_plot_swallows_plot_failure_but_writes_results(self) -> None:
        # Plot failures must not block the aggregator's ack path: results JSON
        # is the source of truth and is already on disk by the time we plot.
        from unittest import mock

        state = AnalyticsState()
        state.add_repo(_repo(1, "Python"))
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {
                "results_dir": Path(tmp) / "results",
                "figures_dir": Path(tmp) / "figures",
                "state_path": Path(tmp) / "results" / "analytics_state.json",
                "top_n": 10,
            }
            with mock.patch(
                "analytics.aggregator.plot_aggregate_payload",
                side_effect=RuntimeError("matplotlib boom"),
            ):
                save_and_plot(state, cfg)

            self.assertTrue((cfg["results_dir"] / "all_results.json").exists())


class HelperTests(unittest.TestCase):
    def test_config_uses_flush_every_when_runner_batch_size_is_blank(self) -> None:
        with patch.dict(
            "os.environ",
            {"FLUSH_EVERY": "25", "RUNNER_BATCH_SIZE": ""},
            clear=True,
        ):
            cfg = config()

        self.assertEqual(cfg["flush_every"], 25)
        self.assertEqual(cfg["runner_batch_size"], 25)

    def test_config_rejects_non_integer_batch_values(self) -> None:
        with patch.dict("os.environ", {"RUNNER_BATCH_SIZE": "not-an-int"}, clear=True):
            with self.assertRaises(ValueError):
                config()

    def test_should_idle_flush_requires_pending_messages_and_elapsed_timeout(self) -> None:
        self.assertTrue(should_idle_flush(1, 30.0, 30))
        self.assertFalse(should_idle_flush(0, 30.0, 30))
        self.assertFalse(should_idle_flush(1, 29.9, 30))
        self.assertFalse(should_idle_flush(1, 30.0, 0))

    def test_plot_aggregate_payload_writes_q1_to_q4_figures(self) -> None:
        payload = {
            "q1_top_languages_by_projects": [{"name": "Python", "count": 2}],
            "q2_top_projects_by_commits": [{"name": "owner/repo", "count": 42}],
            "q3_top_languages_with_tests": [{"name": "Python", "count": 1}],
            "q4_top_languages_with_tests_and_ci": [{"name": "Python", "count": 1}],
        }

        with tempfile.TemporaryDirectory() as tmp:
            written = plot_aggregate_payload(payload, Path(tmp))

            self.assertEqual(
                sorted(path.name for path in written),
                [
                    "q1_languages.png",
                    "q2_commits.png",
                    "q3_tdd_languages.png",
                    "q4_tdd_ci_languages.png",
                ],
            )
            for path in written:
                self.assertTrue(path.exists())

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

    def test_commit_count_bubbles_rate_limit_for_redelivery(self) -> None:
        client = FakeGitHubClient([RateLimitError("wait budget exhausted")])

        with self.assertRaises(RateLimitError):
            commit_count(client, "owner/repo")  # type: ignore[arg-type]

    def test_commit_count_bubbles_network_errors_for_redelivery(self) -> None:
        client = FakeGitHubClient([requests.ConnectionError("reset")])

        with self.assertRaises(requests.ConnectionError):
            commit_count(client, "owner/repo")  # type: ignore[arg-type]

    def test_commit_count_treats_empty_repo_as_zero(self) -> None:
        client = FakeGitHubClient([_http_error(409)])

        self.assertEqual(commit_count(client, "owner/empty"), 0)  # type: ignore[arg-type]

    def test_path_matches_continues_on_absent_paths(self) -> None:
        client = FakeGitHubClient([_http_error(404), FakeResponse()])

        matched, evidence = path_matches(
            client,  # type: ignore[arg-type]
            "owner/repo",
            ["missing", "tests"],
            "main",
        )

        self.assertTrue(matched)
        self.assertEqual(evidence, "tests")

    def test_path_matches_bubbles_network_errors_for_redelivery(self) -> None:
        client = FakeGitHubClient([requests.ConnectionError("reset")])

        with self.assertRaises(requests.ConnectionError):
            path_matches(client, "owner/repo", ["tests"], "main")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
