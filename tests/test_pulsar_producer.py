from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from crawler.models import RepoRecord
from streaming.pulsar_producer import (
    PublishCheckpoint,
    publish_records,
    record_to_message,
)


class _OkResult:
    name = "Ok"


class _FailResult:
    name = "Timeout"


class FakeProducer:
    """Synchronous stand-in for Pulsar's async producer."""

    def __init__(self, fail_first_n: int = 0) -> None:
        self.messages: list[tuple[bytes, str | None]] = []
        self.fail_first_n = fail_first_n
        self.send_attempts = 0
        self.flush_calls = 0

    def send_async(self, content, callback, partition_key=None):
        self.send_attempts += 1
        if self.send_attempts <= self.fail_first_n:
            callback(_FailResult(), None)
            return
        self.messages.append((content, partition_key))
        callback(_OkResult(), object())

    def flush(self):
        self.flush_calls += 1


class PulsarProducerTests(unittest.TestCase):
    def test_record_to_message_is_raw_crawler_json_without_newline(self) -> None:
        message = record_to_message(_record(123, "owner/repo"))

        self.assertFalse(message.endswith(b"\n"))
        payload = json.loads(message.decode("utf-8"))
        self.assertEqual(payload["repo_id"], 123)
        self.assertEqual(payload["full_name"], "owner/repo")

    def test_publish_records_uses_repo_id_as_partition_key(self) -> None:
        producer = FakeProducer()
        published_repo_ids: list[int] = []

        counters = publish_records(
            [_record(1, "owner/one"), _record(2, "owner/two")],
            producer,
            on_publish=lambda record: published_repo_ids.append(record.repo_id),
        )

        self.assertEqual(counters["published"], 2)
        self.assertEqual(counters["permanent_failures"], 0)
        self.assertEqual(published_repo_ids, [1, 2])
        self.assertEqual(
            [partition_key for _, partition_key in producer.messages],
            ["1", "2"],
        )
        self.assertEqual(producer.flush_calls, 1)

    def test_publish_records_retries_transient_failures(self) -> None:
        producer = FakeProducer(fail_first_n=2)

        counters = publish_records(
            [_record(1, "owner/one")],
            producer,
            max_retries=3,
            retry_initial_seconds=0.0,
        )

        self.assertEqual(counters["published"], 1)
        self.assertEqual(counters["publish_failures"], 2)
        self.assertEqual(counters["permanent_failures"], 0)
        self.assertEqual(producer.send_attempts, 3)
        self.assertEqual(len(producer.messages), 1)

    def test_publish_records_records_permanent_failure_after_max_retries(self) -> None:
        producer = FakeProducer(fail_first_n=10)
        failed_repo_ids: list[int] = []

        counters = publish_records(
            [_record(7, "owner/seven")],
            producer,
            on_failure=lambda record: failed_repo_ids.append(record.repo_id),
            max_retries=2,
            retry_initial_seconds=0.0,
        )

        self.assertEqual(counters["published"], 0)
        self.assertEqual(counters["permanent_failures"], 1)
        self.assertEqual(failed_repo_ids, [7])
        self.assertEqual(producer.send_attempts, 3)
        self.assertEqual(len(producer.messages), 0)

    def test_publish_records_skips_records_already_in_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint_path = Path(tmp) / "publish.json"
            checkpoint_path.write_text(
                json.dumps({"published_repo_ids": [1]}),
                encoding="utf-8",
            )
            checkpoint = PublishCheckpoint(checkpoint_path)
            producer = FakeProducer()

            counters = publish_records(
                [_record(1, "owner/one"), _record(2, "owner/two")],
                producer,
                checkpoint=checkpoint,
                retry_initial_seconds=0.0,
            )

            self.assertEqual(counters["skipped_via_checkpoint"], 1)
            self.assertEqual(counters["published"], 1)
            self.assertEqual(
                [partition_key for _, partition_key in producer.messages],
                ["2"],
            )

    def test_publish_records_persists_checkpoint_after_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint_path = Path(tmp) / "publish.json"
            checkpoint = PublishCheckpoint(checkpoint_path)
            producer = FakeProducer()

            publish_records(
                [_record(1, "owner/one"), _record(2, "owner/two")],
                producer,
                checkpoint=checkpoint,
                retry_initial_seconds=0.0,
            )

            saved = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(saved, {"published_repo_ids": [1, 2]})


def _record(repo_id: int, full_name: str) -> RepoRecord:
    return RepoRecord(
        repo_id=repo_id,
        full_name=full_name,
        language="Python",
        stars=1,
        forks=0,
        created_at="2026-05-19T00:00:00Z",
        updated_at="2026-05-19T00:00:00Z",
        pushed_at="2026-05-19T00:00:00Z",
        size_kb=1,
        default_branch="main",
        crawl_day="2026-05-19",
    )


if __name__ == "__main__":
    unittest.main()
