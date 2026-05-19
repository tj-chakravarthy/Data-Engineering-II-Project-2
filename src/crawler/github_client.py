"""GitHub REST API client with token pooling, pagination, and rate-limit handling."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from typing import Any

import requests

GITHUB_API = "https://api.github.com"

log = logging.getLogger(__name__)


class RateLimitError(RuntimeError):
    """Raised when all tokens are exhausted beyond the configured wait window."""


class GitHubClient:
    """Small GitHub REST client for the crawler.

    Any environment variable whose name starts with `GITHUB_TOKEN` is included
    in the token pool. This lets teammates run with multiple tokens locally
    without putting credentials in source control.
    """

    def __init__(
        self,
        tokens: list[str] | None = None,
        base_url: str = GITHUB_API,
        max_wait_seconds: int = 60,
        max_total_wait_seconds: int = 3600,
        timeout_seconds: int = 30,
    ) -> None:
        if tokens is None:
            tokens = [
                value
                for key, value in os.environ.items()
                if key.startswith("GITHUB_TOKEN") and value
            ]
        if not tokens:
            raise RuntimeError(
                "No GitHub tokens configured. Set GITHUB_TOKEN or pass tokens=[...]."
            )
        self.tokens = tokens
        self._token_idx = 0
        self.base_url = base_url.rstrip("/")
        self.max_wait_seconds = max_wait_seconds
        self.max_total_wait_seconds = max_total_wait_seconds
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.rate_limit_waits = 0
        self.rate_limit_wait_seconds = 0

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """GET with token rotation and looped reset waiting on rate limits.

        Tries each token in the pool. If all tokens are rate-limited, sleeps
        until the soonest reset (capped per sleep by ``max_wait_seconds``) and
        retries the full pool. Continues until a token returns 200 or the total
        accumulated wait for this call exceeds ``max_total_wait_seconds``.
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        total_waited = 0
        last_rate_limited: requests.Response | None = None

        while True:
            for _ in range(len(self.tokens)):
                response = self.session.get(
                    url,
                    headers=self._headers(),
                    params=params,
                    timeout=self.timeout_seconds,
                )
                if response.status_code == 200:
                    return response
                if _is_rate_limited(response):
                    last_rate_limited = response
                    log.info("Rate-limited; rotating GitHub token")
                    self._rotate_token()
                    continue
                response.raise_for_status()

            if total_waited >= self.max_total_wait_seconds:
                raise RateLimitError(
                    f"Exhausted rate-limit wait budget ({total_waited}s) for {url}"
                )
            assert last_rate_limited is not None
            total_waited += self._sleep_until_reset(last_rate_limited)

    def paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield all items across paginated GitHub responses."""
        params = dict(params or {})
        params.setdefault("per_page", 100)
        url: str | None = path if path.startswith("http") else f"{self.base_url}{path}"
        first = True

        while url:
            response = self.get(url, params=params if first else None)
            first = False
            yield from _extract_items(response.json())
            url = _next_link(response.headers.get("Link", ""))

    def search_repositories(
        self,
        query: str,
        sort: str | None = "stars",
        order: str | None = "desc",
    ) -> Iterator[dict[str, Any]]:
        """Yield `/search/repositories` items.

        GitHub caps each search query at 1000 results, so the crawler should
        call this with narrow date-slice queries.
        """
        params: dict[str, Any] = {"q": query}
        if sort:
            params["sort"] = sort
        if order:
            params["order"] = order
        yield from self.paginate("/search/repositories", params=params)

    def rate_limit_status(self) -> dict[str, Any]:
        return self.get("/rate_limit").json()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._current_token()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _current_token(self) -> str:
        return self.tokens[self._token_idx % len(self.tokens)]

    def _rotate_token(self) -> None:
        self._token_idx += 1

    def _sleep_until_reset(self, response: requests.Response) -> int:
        reset = int(response.headers.get("X-RateLimit-Reset", "0"))
        wait = max(reset - int(time.time()), 1)
        wait = min(wait, self.max_wait_seconds)
        self.rate_limit_waits += 1
        self.rate_limit_wait_seconds += wait
        log.warning("All GitHub tokens rate-limited; sleeping %ds", wait)
        time.sleep(wait)
        return wait


def _is_rate_limited(response: requests.Response) -> bool:
    if response.status_code not in (403, 429):
        return False
    if response.headers.get("X-RateLimit-Remaining") == "0":
        return True
    return "rate limit" in response.text.lower()


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and "items" in payload:
        return payload["items"]
    if isinstance(payload, list):
        return payload
    return [payload]


def _next_link(link_header: str) -> str | None:
    for part in link_header.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>")
    return None

