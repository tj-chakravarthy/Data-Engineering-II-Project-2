"""Disk-backed crawler cache using newline-delimited JSON."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path

from crawler.models import RepoRecord


class RepoCache:
    """One deduplicated NDJSON cache file per query/date slice."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for_slice(self, date_field: str, day: str, query_suffix: str = "") -> Path:
        suffix = _safe_suffix(query_suffix)
        name = f"repos_{date_field}_{day}{suffix}.ndjson"
        return self.root / name

    def has_slice(self, date_field: str, day: str, query_suffix: str = "") -> bool:
        return self.path_for_slice(date_field, day, query_suffix).exists()

    def read_slice(
        self,
        date_field: str,
        day: str,
        query_suffix: str = "",
    ) -> Iterator[RepoRecord]:
        path = self.path_for_slice(date_field, day, query_suffix)
        with path.open(encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    yield RepoRecord.from_json_line(line)

    def write_slice(
        self,
        date_field: str,
        day: str,
        records: Iterable[RepoRecord],
        query_suffix: str = "",
    ) -> tuple[int, int]:
        """Write a deduplicated slice. Returns `(written, duplicates_skipped)`."""
        path = self.path_for_slice(date_field, day, query_suffix)
        tmp = path.with_suffix(path.suffix + ".tmp")
        seen: set[str] = set()
        written = 0
        duplicates = 0

        with tmp.open("w", encoding="utf-8", newline="\n") as file:
            for record in records:
                key = record.dedupe_key()
                if key in seen:
                    duplicates += 1
                    continue
                seen.add(key)
                file.write(record.to_json_line())
                written += 1

        tmp.replace(path)
        return written, duplicates


def _safe_suffix(query_suffix: str) -> str:
    if not query_suffix:
        return ""
    cleaned = "".join(
        char if char.isalnum() else "_"
        for char in query_suffix.strip().lower()
    ).strip("_")
    return f"_{cleaned}" if cleaned else ""

