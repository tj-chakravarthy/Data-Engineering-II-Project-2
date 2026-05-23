"""GitHub REST API client with token pooling, pagination, and rate-limit handling."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, replace
from typing import Any

import requests

GITHUB_API = "https://api.github.com"
# Authenticated REST quota per token; corrected by the first response header.
INITIAL_TOKEN_QUOTA = 5000

log = logging.getLogger(__name__)


class RateLimitError(RuntimeError):
    """Raised when all tokens are exhausted beyond the configured wait window."""


@dataclass(frozen=True)
class SearchMetadata:
    """Metadata returned by GitHub's search endpoints before item pagination."""

    query: str
    total_count: int | None
    incomplete_results: bool


@dataclass(frozen=True)
class TokenState:
    """Pool's last-known view of one token's rate-limit budget.

    Frozen so ``snapshot()`` callers can't mutate the pool by accident — the
    pool itself replaces state via ``dataclasses.replace`` under its lock.
    """

    remaining: int = INITIAL_TOKEN_QUOTA
    reset_at: float = 0.0


class TokenPool:
    """Predictive token selector for the GitHub REST API.

    On every request the pool picks the token with the highest remaining quota
    as last reported by GitHub. The caller calls ``observe()`` with each
    response's headers so the pool's view of remaining quota stays current.

    This avoids the "hammer token 1 until it 429s, then rotate" pattern of
    naive round-robin: the moment one token drops below the others the pool
    routes away from it until either another token also drops or the
    rate-limit window resets and a fresh response refills it.

    Ties (equal remaining) are broken round-robin so equal-budget tokens see
    even usage. The pool is thread-safe for the future parallel intra-repo
    enrichment path; today's single-threaded client incurs only an
    uncontended lock acquire per call.
    """

    def __init__(self, tokens: list[str]) -> None:
        if not tokens:
            raise ValueError("TokenPool requires at least one token")
        # Tuple so the public attribute can't be mutated to desync `_states`.
        self.tokens: tuple[str, ...] = tuple(tokens)
        self._states: dict[str, TokenState] = {t: TokenState() for t in self.tokens}
        # Cursor that walks the token list to break ties round-robin. Always
        # advanced before reading, so a fresh pool's first pick is tokens[0].
        self._cursor = len(self.tokens) - 1
        self._lock = threading.Lock()

    def next_token(self) -> str:
        """Return the token with the most remaining quota.

        Always returns a token, even when every token is known-depleted: the
        caller's retry loop is responsible for sleeping when responses keep
        coming back rate-limited. Equal-remaining tokens are picked
        round-robin so usage stays balanced. Tokens past their reset window
        are treated as full so the pool doesn't sit on idle quota waiting
        to discover the refill by accident.
        """
        with self._lock:
            now = time.time()
            max_remaining = max(
                _effective_remaining(s, now) for s in self._states.values()
            )
            for _ in range(len(self.tokens)):
                self._cursor = (self._cursor + 1) % len(self.tokens)
                token = self.tokens[self._cursor]
                if _effective_remaining(self._states[token], now) == max_remaining:
                    return token
            # Unreachable: at least one token has remaining == max_remaining.
            return self.tokens[0]

    def observe(self, token: str, headers: Mapping[str, Any]) -> None:
        """Update the pool's view of ``token`` from a response's headers.

        Missing or unparsable values leave the existing state untouched so
        non-GitHub error responses that lack the headers do not clobber state.
        """
        with self._lock:
            current = self._states[token]
            remaining = _int_or_none(headers.get("X-RateLimit-Remaining"))
            reset = _int_or_none(headers.get("X-RateLimit-Reset"))
            self._states[token] = replace(
                current,
                remaining=current.remaining if remaining is None else remaining,
                reset_at=current.reset_at if reset is None else float(reset),
            )

    def mark_depleted(self, token: str) -> None:
        """Force the pool to treat ``token`` as having no remaining quota.

        Required because ``observe`` only sees what the headers report.
        GitHub's secondary/abuse rate limit returns 403 with body text but
        leaves ``X-RateLimit-Remaining`` unchanged, so without this override
        the pool would keep picking a token that every request is rejecting.
        The state self-corrects on the next successful response.
        """
        with self._lock:
            self._states[token] = replace(self._states[token], remaining=0)

    def snapshot(self) -> dict[str, TokenState]:
        """Read-only snapshot of per-token state for logging and experiments."""
        with self._lock:
            return dict(self._states)


class GitHubClient:
    """Small GitHub REST client for the crawler.

    Tokens can be supplied two ways:

    - Pass ``tokens=[...]`` explicitly. The caller is responsible for any
      partitioning across runners.
    - Omit ``tokens`` and any environment variable whose name starts with
      ``GITHUB_TOKEN`` is included automatically, then stride-sliced by
      ``RUNNER_ID`` / ``NUM_RUNNERS`` so multiple replicas of the same service
      own disjoint subsets of the global pool. Single-runner runs (defaults
      ``RUNNER_ID=0``, ``NUM_RUNNERS=1``) see every token.
    """

    def __init__(
        self,
        tokens: list[str] | None = None,
        base_url: str = GITHUB_API,
        max_wait_seconds: int = 60,
        max_total_wait_seconds: int = 3600,
        timeout_seconds: int = 30,
        max_transient_retries: int = 5,
        transient_retry_initial_seconds: float = 1.0,
    ) -> None:
        if tokens is None:
            tokens = tokens_from_env()
        if not tokens:
            raise RuntimeError(
                "No GitHub tokens configured. Set GITHUB_TOKEN or pass tokens=[...]. "
                "Multi-runner deployments must keep NUM_RUNNERS <= number of tokens "
                "so every runner gets at least one."
            )
        self.pool = TokenPool(tokens)
        self.base_url = base_url.rstrip("/")
        self.max_wait_seconds = max_wait_seconds
        self.max_total_wait_seconds = max_total_wait_seconds
        self.timeout_seconds = timeout_seconds
        self.max_transient_retries = max_transient_retries
        self.transient_retry_initial_seconds = transient_retry_initial_seconds
        self.session = requests.Session()
        self.rate_limit_waits = 0
        self.rate_limit_wait_seconds = 0
        self.transient_retries = 0

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """GET with predictive token rotation and looped reset waiting.

        The pool picks the highest-quota token for each attempt. Every response
        feeds the pool's view via ``observe``, so a 429 immediately drops that
        token to remaining=0 and the next pick goes elsewhere. When every
        token in one burst returns rate-limited, the client sleeps until the
        soonest reset (capped per sleep by ``max_wait_seconds``) and starts a
        new burst. Total accumulated sleep is bounded by
        ``max_total_wait_seconds``.
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        total_waited = 0
        last_rate_limited: requests.Response | None = None
        transient_attempts = 0

        while True:
            retry_transient = False
            for _ in range(len(self.pool.tokens)):
                token = self.pool.next_token()
                try:
                    response = self.session.get(
                        url,
                        headers=self._headers_for(token),
                        params=params,
                        timeout=self.timeout_seconds,
                    )
                except requests.RequestException as exc:
                    if transient_attempts < self.max_transient_retries:
                        transient_attempts += 1
                        self._sleep_before_transient_retry(transient_attempts, url)
                        retry_transient = True
                        break
                    raise exc
                self.pool.observe(token, response.headers)
                if response.status_code == 200:
                    return response
                if _is_rate_limited(response):
                    # observe() reflects headers; mark_depleted covers
                    # secondary limits the headers don't signal.
                    self.pool.mark_depleted(token)
                    last_rate_limited = response
                    log.info(
                        "Rate-limited on token %s; pool will route away",
                        _short_token_id(token),
                    )
                    continue
                if _is_transient_server_error(response):
                    if transient_attempts < self.max_transient_retries:
                        transient_attempts += 1
                        self._sleep_before_transient_retry(transient_attempts, url)
                        retry_transient = True
                        break
                    response.raise_for_status()
                response.raise_for_status()

            if retry_transient:
                continue
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
        for payload in self.paginate_json(path, params=params):
            yield from _extract_items(payload)

    def paginate_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Iterator[Any]:
        """Yield decoded JSON payloads across paginated GitHub responses."""
        params = dict(params or {})
        params.setdefault("per_page", 100)
        url: str | None = path if path.startswith("http") else f"{self.base_url}{path}"
        first = True

        while url:
            response = self.get(url, params=params if first else None)
            first = False
            yield response.json()
            url = _next_link(response.headers.get("Link", ""))

    def search_repositories(
        self,
        query: str,
        sort: str | None = "stars",
        order: str | None = "desc",
        on_search_metadata: Callable[[SearchMetadata], None] | None = None,
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
        first = True
        for payload in self.paginate_json("/search/repositories", params=params):
            if first and isinstance(payload, dict):
                first = False
                metadata = _search_metadata_from_payload(query, payload)
                if on_search_metadata:
                    on_search_metadata(metadata)
            yield from _extract_items(payload)

    def search_repositories_metadata(
        self,
        query: str,
        sort: str | None = "stars",
        order: str | None = "desc",
    ) -> SearchMetadata:
        """Fetch only search metadata for adaptive query partitioning."""
        params: dict[str, Any] = {"q": query, "per_page": 1}
        if sort:
            params["sort"] = sort
        if order:
            params["order"] = order
        payload = self.get("/search/repositories", params=params).json()
        if not isinstance(payload, dict):
            return SearchMetadata(query=query, total_count=None, incomplete_results=False)
        return _search_metadata_from_payload(query, payload)

    def rate_limit_status(self) -> dict[str, Any]:
        return self.get("/rate_limit").json()

    def _headers_for(self, token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _sleep_until_reset(self, response: requests.Response) -> int:
        reset = int(response.headers.get("X-RateLimit-Reset", "0"))
        wait = max(reset - int(time.time()), 1)
        wait = min(wait, self.max_wait_seconds)
        self.rate_limit_waits += 1
        self.rate_limit_wait_seconds += wait
        log.warning("All GitHub tokens rate-limited; sleeping %ds", wait)
        time.sleep(wait)
        return wait

    def _sleep_before_transient_retry(self, attempt: int, url: str) -> None:
        wait = self.transient_retry_initial_seconds * (2 ** (attempt - 1))
        self.transient_retries += 1
        log.warning(
            "Transient GitHub error for %s; retrying in %.1fs "
            "(attempt %d/%d)",
            url,
            wait,
            attempt,
            self.max_transient_retries,
        )
        if wait > 0:
            time.sleep(wait)


def tokens_from_env(environ: Mapping[str, str] | None = None) -> list[str]:
    """Return the GITHUB_TOKEN* slice owned by this runner.

    Two replicas of the same service must not share a token: they'd hammer the
    same per-token rate limit and the pool's predictive routing would thrash.
    The stride slice ``all_tokens[RUNNER_ID::NUM_RUNNERS]`` gives every runner
    a disjoint subset. Defaults (``RUNNER_ID=0``, ``NUM_RUNNERS=1``) keep
    single-replica deployments and tests unchanged.

    Tokens are ordered by env-var name for a stable, reproducible slice across
    processes that may have set their env in different orders.
    """
    env = environ if environ is not None else os.environ
    pairs = sorted(
        (key, value)
        for key, value in env.items()
        if key.startswith("GITHUB_TOKEN") and value
    )
    all_tokens = [value for _, value in pairs]
    num_runners = max(_int_or_none(env.get("NUM_RUNNERS")) or 1, 1)
    runner_id = _int_or_none(env.get("RUNNER_ID")) or 0
    return partition_tokens(all_tokens, runner_id, num_runners)


def partition_tokens(all_tokens: list[str], runner_id: int, num_runners: int) -> list[str]:
    """Return ``all_tokens[runner_id::num_runners]`` with input validation."""
    if num_runners < 1:
        raise ValueError(f"NUM_RUNNERS must be >= 1, got {num_runners}")
    if runner_id < 0 or runner_id >= num_runners:
        raise ValueError(
            f"RUNNER_ID={runner_id} out of range for NUM_RUNNERS={num_runners}"
        )
    return all_tokens[runner_id::num_runners]


def _is_rate_limited(response: requests.Response) -> bool:
    if response.status_code not in (403, 429):
        return False
    if response.headers.get("X-RateLimit-Remaining") == "0":
        return True
    return "rate limit" in response.text.lower()


def _is_transient_server_error(response: requests.Response) -> bool:
    return response.status_code in {500, 502, 503, 504}


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and "items" in payload:
        return payload["items"]
    if isinstance(payload, list):
        return payload
    return [payload]


def _effective_remaining(state: TokenState, now: float) -> int:
    """Treat tokens past their reset window as having full quota.

    The pool only learns of actual quota via response headers, so without
    this a token that completed its rate-limit window while idle would
    keep showing as depleted until some unrelated probe happened to land
    on it. Returning the seed quota lets the selector route requests to
    a token we have good reason to believe has refilled; the very next
    response from that token corrects the value via ``observe``.
    """
    if state.reset_at and state.reset_at < now:
        return INITIAL_TOKEN_QUOTA
    return state.remaining


def _short_token_id(token: str) -> str:
    """Last 4 chars of the token for log lines.

    Tokens are credentials so we never log the full value; the suffix is enough
    to correlate log lines across one runner's session without leaking secrets.
    """
    return f"...{token[-4:]}" if len(token) > 4 else "..."


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _search_metadata_from_payload(query: str, payload: dict[str, Any]) -> SearchMetadata:
    return SearchMetadata(
        query=query,
        total_count=_int_or_none(payload.get("total_count")),
        incomplete_results=bool(payload.get("incomplete_results", False)),
    )


def _next_link(link_header: str) -> str | None:
    for part in link_header.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>")
    return None
