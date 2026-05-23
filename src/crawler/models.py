"""Crawler-owned repository record model.

The crawler emits records that are already deduplicated. Downstream streaming
and analytics code should not need to correct duplicated input.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from typing import Any


@dataclass(frozen=True)
class RepoRecord:
    """Normalized repository metadata produced by the crawler."""

    repo_id: int
    full_name: str
    language: str | None
    stars: int
    forks: int
    created_at: str
    updated_at: str
    pushed_at: str
    size_kb: int
    default_branch: str
    crawl_day: str
    archived: bool | None = None
    topics: list[str] | None = None
    open_issues_count: int | None = None
    # Wall-clock UTC ISO-8601 timestamp set when the crawler emits the record
    # downstream. Carried through the streaming pipeline so any downstream
    # consumer can compute end-to-end latency by diffing against its own
    # processing wall clock. None means the record predates this field or was
    # constructed in a context that does not emit (tests, ad-hoc reads).
    emitted_at: str | None = None

    @classmethod
    def from_github_item(cls, item: dict[str, Any], crawl_day: str) -> "RepoRecord":
        """Map a GitHub `/search/repositories` item into a crawler record."""
        return cls(
            repo_id=int(item["id"]),
            full_name=item["full_name"],
            language=item.get("language"),
            stars=int(item["stargazers_count"]),
            forks=int(item["forks_count"]),
            created_at=item["created_at"],
            updated_at=item["updated_at"],
            pushed_at=item["pushed_at"],
            size_kb=int(item["size"]),
            default_branch=item["default_branch"],
            crawl_day=crawl_day,
            archived=item.get("archived"),
            topics=item.get("topics"),
            open_issues_count=item.get("open_issues_count"),
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RepoRecord":
        """Create a record while ignoring unknown additive fields."""
        known = {field.name for field in fields(cls)}
        return cls(**{key: value for key, value in data.items() if key in known})

    @classmethod
    def from_json_line(cls, line: str) -> "RepoRecord":
        return cls.from_dict(json.loads(line))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True) + "\n"

    def dedupe_key(self) -> str:
        """Stable key for crawler-owned duplicate detection."""
        return str(self.repo_id) if self.repo_id else self.full_name.lower()

