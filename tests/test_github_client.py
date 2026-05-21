from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import requests

from crawler.github_client import GitHubClient, RateLimitError


def _response(status: int, *, rate_limited: bool = False, json_payload=None):
    response = MagicMock()
    response.status_code = status
    response.headers = {
        "X-RateLimit-Remaining": "0" if rate_limited else "5000",
        "X-RateLimit-Reset": "0",
    }
    response.text = "rate limit exceeded" if rate_limited else ""
    response.json.return_value = json_payload or {}
    response.raise_for_status = MagicMock()
    return response


class GitHubClientRateLimitTests(unittest.TestCase):
    def test_recovers_after_multiple_sleeps(self) -> None:
        client = GitHubClient(
            tokens=["t1", "t2"],
            max_wait_seconds=1,
            max_total_wait_seconds=10,
        )
        ok = _response(200, json_payload={"ok": True})
        rate_limited = _response(403, rate_limited=True)
        client.session = MagicMock()
        client.session.get.side_effect = [
            rate_limited,
            rate_limited,
            rate_limited,
            rate_limited,
            ok,
        ]

        with patch("crawler.github_client.time.sleep") as sleep_mock:
            result = client.get("/x")

        self.assertIs(result, ok)
        self.assertEqual(client.session.get.call_count, 5)
        self.assertEqual(sleep_mock.call_count, 2)
        self.assertGreaterEqual(client.rate_limit_waits, 2)

    def test_raises_when_total_budget_exhausted(self) -> None:
        client = GitHubClient(
            tokens=["t1"],
            max_wait_seconds=1,
            max_total_wait_seconds=2,
        )
        rate_limited = _response(403, rate_limited=True)
        client.session = MagicMock()
        client.session.get.return_value = rate_limited

        with patch("crawler.github_client.time.sleep"):
            with self.assertRaises(RateLimitError):
                client.get("/x")

    def test_non_rate_limit_error_is_raised_immediately(self) -> None:
        client = GitHubClient(tokens=["t1"])
        bad = _response(422)
        bad.raise_for_status.side_effect = RuntimeError("422 Unprocessable Entity")
        client.session = MagicMock()
        client.session.get.return_value = bad

        with self.assertRaises(RuntimeError):
            client.get("/x")

    def test_transient_server_error_is_retried(self) -> None:
        client = GitHubClient(
            tokens=["t1"],
            max_transient_retries=2,
            transient_retry_initial_seconds=0.0,
        )
        bad_gateway = _response(502)
        ok = _response(200, json_payload={"ok": True})
        client.session = MagicMock()
        client.session.get.side_effect = [bad_gateway, ok]

        result = client.get("/x")

        self.assertIs(result, ok)
        self.assertEqual(client.session.get.call_count, 2)
        self.assertEqual(client.transient_retries, 1)
        bad_gateway.raise_for_status.assert_not_called()

    def test_transient_server_error_raises_after_retry_budget(self) -> None:
        client = GitHubClient(
            tokens=["t1"],
            max_transient_retries=1,
            transient_retry_initial_seconds=0.0,
        )
        bad_gateway = _response(502)
        bad_gateway.raise_for_status.side_effect = RuntimeError("502 Bad Gateway")
        client.session = MagicMock()
        client.session.get.return_value = bad_gateway

        with self.assertRaises(RuntimeError):
            client.get("/x")

        self.assertEqual(client.session.get.call_count, 2)
        self.assertEqual(client.transient_retries, 1)

    def test_request_exception_is_retried(self) -> None:
        """Network-level errors (no HTTP response at all) follow the same
        transient-retry path as 5xx responses."""
        client = GitHubClient(
            tokens=["t1"],
            max_transient_retries=2,
            transient_retry_initial_seconds=0.0,
        )
        ok = _response(200, json_payload={"ok": True})
        client.session = MagicMock()
        client.session.get.side_effect = [
            requests.ConnectionError("connection reset"),
            ok,
        ]

        result = client.get("/x")

        self.assertIs(result, ok)
        self.assertEqual(client.session.get.call_count, 2)
        self.assertEqual(client.transient_retries, 1)

    def test_request_exception_raises_after_retry_budget(self) -> None:
        client = GitHubClient(
            tokens=["t1"],
            max_transient_retries=1,
            transient_retry_initial_seconds=0.0,
        )
        client.session = MagicMock()
        client.session.get.side_effect = requests.ConnectionError(
            "connection refused"
        )

        with self.assertRaises(requests.ConnectionError):
            client.get("/x")

        # one original attempt + one retry = 2 calls, then raise
        self.assertEqual(client.session.get.call_count, 2)
        self.assertEqual(client.transient_retries, 1)

    def test_search_repositories_exposes_first_page_metadata(self) -> None:
        client = GitHubClient(tokens=["t1"])
        ok = _response(
            200,
            json_payload={
                "total_count": 1200,
                "incomplete_results": True,
                "items": [{"id": 1}],
            },
        )
        client.session = MagicMock()
        client.session.get.return_value = ok
        metadata = []

        items = list(
            client.search_repositories(
                "pushed:2026-05-19",
                on_search_metadata=metadata.append,
            )
        )

        self.assertEqual(items, [{"id": 1}])
        self.assertEqual(metadata[0].query, "pushed:2026-05-19")
        self.assertEqual(metadata[0].total_count, 1200)
        self.assertTrue(metadata[0].incomplete_results)


if __name__ == "__main__":
    unittest.main()
