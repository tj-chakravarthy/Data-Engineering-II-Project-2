from __future__ import annotations

import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from scripts.validate_crawler_output import validate


class ValidateCrawlerOutputTests(unittest.TestCase):
    def test_accepts_valid_unique_ndjson(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "repos.ndjson"
            path.write_text(
                '{"repo_id": 1, "full_name": "owner/one", "language": "Python", '
                '"stars": 1, "forks": 0, "created_at": "2026-05-19T00:00:00Z", '
                '"updated_at": "2026-05-19T00:00:00Z", '
                '"pushed_at": "2026-05-19T00:00:00Z", "size_kb": 1, '
                '"default_branch": "main", "crawl_day": "2026-05-19"}\n',
                encoding="utf-8",
            )

            with redirect_stdout(StringIO()):
                stats = validate(path)

            self.assertEqual(stats.records, 1)
            self.assertFalse(stats.has_errors)

    def test_rejects_duplicates_and_missing_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "repos.ndjson"
            path.write_text(
                '{"repo_id": 1, "full_name": "owner/one"}\n'
                '{"repo_id": 1, "full_name": "owner/one"}\n',
                encoding="utf-8",
            )

            with redirect_stdout(StringIO()):
                stats = validate(path)

            self.assertEqual(stats.records, 2)
            self.assertEqual(stats.duplicates, 1)
            self.assertEqual(stats.missing_required, 2)
            self.assertTrue(stats.has_errors)


if __name__ == "__main__":
    unittest.main()
