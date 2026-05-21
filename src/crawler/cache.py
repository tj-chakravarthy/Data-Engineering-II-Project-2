"""Disk-backed crawler cache using newline-delimited JSON."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
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
        written = 0
        duplicates = 0

        def on_write() -> None:
            nonlocal written
            written += 1

        def on_duplicate() -> None:
            nonlocal duplicates
            duplicates += 1

        for _ in self.write_slice_streaming(
            date_field,
            day,
            records,
            query_suffix,
            on_write=on_write,
            on_duplicate=on_duplicate,
        ):
            pass

        return written, duplicates

    def write_slice_streaming(
        self,
        date_field: str,
        day: str,
        records: Iterable[RepoRecord],
        query_suffix: str = "",
        on_write: Callable[[], None] | None = None,
        on_duplicate: Callable[[], None] | None = None,
    ) -> Iterator[RepoRecord]:
        """Write a deduplicated slice while yielding records immediately.

        The final cache file is replaced only after the full input iterable is
        consumed. If the run fails partway through, downstream consumers may
        have received live records, but an incomplete cache file is not promoted.

        We do not fsync per record: the temp file is unlinked on any failure
        path, so per-record flushes would only protect against another process
        tailing the temp file (no such consumer exists). Python's default I/O
        buffering plus the final close-on-promote is sufficient.
        """
        path = self.path_for_slice(date_field, day, query_suffix)
        tmp = path.with_suffix(path.suffix + ".tmp")
        seen: set[str] = set()
        promoted = False

        try:
            with tmp.open("w", encoding="utf-8", newline="\n") as file:
                for record in records:
                    key = record.dedupe_key()
                    if key in seen:
                        if on_duplicate:
                            on_duplicate()
                        continue
                    seen.add(key)
                    file.write(record.to_json_line())
                    if on_write:
                        on_write()
                    yield record

            tmp.replace(path)
            promoted = True
        finally:
            if not promoted and tmp.exists():
                tmp.unlink()


def _safe_suffix(query_suffix: str) -> str:
    if not query_suffix:
        return ""
    cleaned = "".join(
        char if char.isalnum() else "_"
        for char in query_suffix.strip().lower()
    ).strip("_")
    return f"_{cleaned}" if cleaned else ""
