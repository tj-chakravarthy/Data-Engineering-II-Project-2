from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import requests

from crawler.github_client import (
    GitHubClient,
    INITIAL_TOKEN_QUOTA,
    RateLimitError,
    TokenPool,
)


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

    def test_predictive_rotation_routes_away_from_depleted_token(self) -> None:
        """A token that just returned remaining=0 must not be picked again
        while a sibling still has quota — the core optimization over reactive
        post-429 rotation."""
        client = GitHubClient(tokens=["t1", "t2"])

        depleted = _response(200, json_payload={"ok": True})
        # Reset far in the future so the pool's proactive-recovery path
        # doesn't reclaim this token mid-test (epoch 9999999999 ≈ year 2286).
        depleted.headers = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "9999999999"}
        healthy = _response(200, json_payload={"ok": True})
        healthy.headers = {"X-RateLimit-Remaining": "4500", "X-RateLimit-Reset": "0"}

        client.session = MagicMock()
        client.session.get.side_effect = [depleted, healthy, healthy, healthy]

        client.get("/x")
        first_token = client.session.get.call_args_list[0].kwargs["headers"]["Authorization"]
        depleted_token = first_token.removeprefix("Bearer ")
        other_token = "t2" if depleted_token == "t1" else "t1"

        for _ in range(3):
            client.get("/x")
        for call in client.session.get.call_args_list[1:]:
            token = call.kwargs["headers"]["Authorization"].removeprefix("Bearer ")
            self.assertEqual(token, other_token)

    def test_predictive_rotation_balances_equal_tokens(self) -> None:
        """When all tokens have equal remaining quota, ties break round-robin
        so usage stays balanced. Two tokens, four requests, identical 5000
        remaining: each token sees two requests."""
        client = GitHubClient(tokens=["t1", "t2"])
        ok = _response(200, json_payload={"ok": True})
        ok.headers = {"X-RateLimit-Remaining": "5000", "X-RateLimit-Reset": "0"}
        client.session = MagicMock()
        client.session.get.return_value = ok

        for _ in range(4):
            client.get("/x")

        tokens_used = [
            call.kwargs["headers"]["Authorization"].removeprefix("Bearer ")
            for call in client.session.get.call_args_list
        ]
        self.assertEqual(tokens_used.count("t1"), 2)
        self.assertEqual(tokens_used.count("t2"), 2)

    def test_secondary_rate_limit_routes_away_despite_full_remaining_header(self) -> None:
        """GitHub's secondary/abuse rate limit returns 403 with body text but
        the X-RateLimit-Remaining header may still show full quota. The client
        must mark the token depleted anyway so subsequent picks route to
        other tokens instead of looping on the throttled one."""
        client = GitHubClient(
            tokens=["t1", "t2"],
            max_wait_seconds=1,
            max_total_wait_seconds=10,
        )
        # 403 + "rate limit" in body, but the header lies about remaining
        # quota. This is the exact shape GitHub returns for secondary limits.
        secondary = MagicMock()
        secondary.status_code = 403
        secondary.headers = {"X-RateLimit-Remaining": "4500", "X-RateLimit-Reset": "0"}
        secondary.text = "You have exceeded a secondary rate limit"
        secondary.raise_for_status = MagicMock()
        ok = _response(200, json_payload={"ok": True})
        client.session = MagicMock()
        client.session.get.side_effect = [secondary, ok]

        client.get("/x")

        # Two calls. The first hits the secondary limit; the second must
        # route to the other token even though the secondary response's
        # header claimed plenty of quota on the first token.
        self.assertEqual(client.session.get.call_count, 2)
        first = client.session.get.call_args_list[0].kwargs["headers"]["Authorization"]
        second = client.session.get.call_args_list[1].kwargs["headers"]["Authorization"]
        self.assertNotEqual(first, second)

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


class TokenPoolTests(unittest.TestCase):
    def test_observe_records_remaining_and_reset(self) -> None:
        pool = TokenPool(["t1"])
        pool.observe("t1", {"X-RateLimit-Remaining": "42", "X-RateLimit-Reset": "1700000000"})
        snap = pool.snapshot()
        self.assertEqual(snap["t1"].remaining, 42)
        self.assertEqual(snap["t1"].reset_at, 1700000000.0)

    def test_observe_ignores_missing_headers(self) -> None:
        pool = TokenPool(["t1"])
        pool.observe("t1", {})  # non-GitHub error response
        snap = pool.snapshot()
        # Initial state preserved
        self.assertGreater(snap["t1"].remaining, 0)

    def test_next_token_prefers_highest_remaining(self) -> None:
        pool = TokenPool(["t1", "t2", "t3"])
        pool.observe("t1", {"X-RateLimit-Remaining": "100"})
        pool.observe("t2", {"X-RateLimit-Remaining": "4000"})
        pool.observe("t3", {"X-RateLimit-Remaining": "500"})
        # t2 has the most; every call until t2 drops should pick t2
        self.assertEqual(pool.next_token(), "t2")
        self.assertEqual(pool.next_token(), "t2")

    def test_empty_token_list_rejected(self) -> None:
        with self.assertRaises(ValueError):
            TokenPool([])

    def test_initial_state_uses_seed_quota(self) -> None:
        """Seed remaining must match INITIAL_TOKEN_QUOTA so the first request
        treats all tokens as full until the first response refines them."""
        pool = TokenPool(["t1", "t2"])
        snap = pool.snapshot()
        for state in snap.values():
            self.assertEqual(state.remaining, INITIAL_TOKEN_QUOTA)
            self.assertEqual(state.reset_at, 0.0)

    def test_next_token_proactively_restores_after_reset_window(self) -> None:
        """A token whose reset_at is already in the past must regain
        eligibility even before observe() sees a fresh response. Without
        this the pool sits on idle quota until something else triggers a
        retry on the depleted token."""
        pool = TokenPool(["t1", "t2"])
        # t1: observed as depleted but its reset window has already passed.
        pool.observe("t1", {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1"})
        # t2: has actual quota and a reset far in the future, so proactive
        # recovery does not apply — its effective remaining is just 100.
        pool.observe(
            "t2",
            {"X-RateLimit-Remaining": "100", "X-RateLimit-Reset": "9999999999"},
        )
        # Without the fix, t2 (Remaining=100) beats t1 (Remaining=0).
        # With it, t1 counts as full quota (INITIAL_TOKEN_QUOTA=5000) > 100,
        # so the selector routes back to t1.
        self.assertEqual(pool.next_token(), "t1")

    def test_mark_depleted_forces_remaining_to_zero(self) -> None:
        """mark_depleted is the escape hatch for rate-limit signals the
        headers don't carry (e.g. secondary/abuse limits)."""
        pool = TokenPool(["t1", "t2"])
        pool.observe("t1", {"X-RateLimit-Remaining": "4500"})
        pool.mark_depleted("t1")
        snap = pool.snapshot()
        self.assertEqual(snap["t1"].remaining, 0)
        # Other tokens untouched
        self.assertGreater(snap["t2"].remaining, 0)


if __name__ == "__main__":
    unittest.main()
