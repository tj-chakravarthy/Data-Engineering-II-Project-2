from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

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


if __name__ == "__main__":
    unittest.main()
