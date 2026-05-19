"""Validate crawler NDJSON before handing it to downstream streaming code."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = {
    "repo_id",
    "full_name",
    "language",
    "stars",
    "forks",
    "created_at",
    "updated_at",
    "pushed_at",
    "size_kb",
    "default_branch",
    "crawl_day",
}


def main() -> None:
    args = _parse_args()
    stats = validate(args.input)
    print(
        "validated crawler output: "
        f"records={stats.records} duplicates={stats.duplicates} "
        f"missing_required={stats.missing_required} invalid_json={stats.invalid_json}"
    )
    if stats.languages:
        preview = ", ".join(
            f"{language}:{count}" for language, count in stats.languages.most_common(10)
        )
        print(f"top languages in handoff file: {preview}")
    if stats.has_errors:
        raise SystemExit(1)


class ValidationStats:
    def __init__(self) -> None:
        self.records = 0
        self.duplicates = 0
        self.missing_required = 0
        self.invalid_json = 0
        self.languages: Counter[str] = Counter()
        self._seen: set[str] = set()

    @property
    def has_errors(self) -> bool:
        return bool(self.duplicates or self.missing_required or self.invalid_json)

    def record(self, payload: dict[str, Any], line_number: int) -> None:
        self.records += 1
        missing = sorted(REQUIRED_FIELDS - payload.keys())
        if missing:
            self.missing_required += 1
            print(f"line {line_number}: missing required fields: {', '.join(missing)}")

        key = _dedupe_key(payload)
        if key in self._seen:
            self.duplicates += 1
            print(f"line {line_number}: duplicate repository key: {key}")
        self._seen.add(key)

        language = payload.get("language") or "UNKNOWN"
        self.languages[str(language)] += 1


def validate(path: Path) -> ValidationStats:
    stats = ValidationStats()
    with path.open(encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                stats.invalid_json += 1
                print(f"line {line_number}: invalid JSON: {exc}")
                continue
            if not isinstance(payload, dict):
                stats.invalid_json += 1
                print(f"line {line_number}: expected JSON object")
                continue
            stats.record(payload, line_number)
    return stats


def _dedupe_key(payload: dict[str, Any]) -> str:
    repo_id = payload.get("repo_id")
    if repo_id:
        return str(repo_id)
    return str(payload.get("full_name", "")).lower()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate crawler-owned NDJSON before streaming ingestion."
    )
    parser.add_argument("input", type=Path, help="crawler NDJSON file to validate")
    return parser.parse_args()


if __name__ == "__main__":
    main()
