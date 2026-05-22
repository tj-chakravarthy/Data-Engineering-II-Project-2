from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch

from streaming.pulsar_connection import get_pulsar_client


class _FakeProducer:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeClient:
    """Fake Pulsar client.

    get_pulsar_client builds a fresh client object on every retry attempt, so
    the "fail the first N attempts" counter has to live in shared state, not
    on the client. Each create_producer call is recorded in shared_state so
    the test can fail the first N calls across all retries.
    """

    def __init__(self, url: str, shared_state: dict, fail_first_n: int) -> None:
        self.url = url
        self._state = shared_state
        self._fail_first_n = fail_first_n
        self.closed = False

    def create_producer(self, topic: str) -> _FakeProducer:
        self._state["calls"].append(topic)
        if len(self._state["calls"]) <= self._fail_first_n:
            raise RuntimeError("broker not ready")
        producer = _FakeProducer()
        self._state["producers"].append(producer)
        return producer

    def close(self) -> None:
        self.closed = True


def _fake_pulsar_module(fail_first_n: int = 0):
    """Return a fake `pulsar` module, the list of clients it creates, and the
    shared state tracking create_producer calls across retries."""
    created: list[_FakeClient] = []
    shared_state: dict = {"calls": [], "producers": []}

    def client_factory(url: str) -> _FakeClient:
        client = _FakeClient(url, shared_state, fail_first_n)
        created.append(client)
        return client

    return types.SimpleNamespace(Client=client_factory), created, shared_state


class PulsarConnectionTests(unittest.TestCase):
    def test_probes_broker_with_a_real_producer_round_trip(self) -> None:
        fake_pulsar, created, shared_state = _fake_pulsar_module()
        with patch.dict(sys.modules, {"pulsar": fake_pulsar}):
            client = get_pulsar_client(
                "pulsar://broker:6650",
                probe_topic="repos.raw",
                retries=1,
                delay=0,
            )

        # The helper must actually create a producer on the probe topic,
        # not just construct the lazy client object.
        self.assertEqual(shared_state["calls"], ["repos.raw"])
        self.assertTrue(shared_state["producers"][0].closed)
        self.assertEqual(client.url, "pulsar://broker:6650")

    def test_retries_until_broker_answers(self) -> None:
        fake_pulsar, created, shared_state = _fake_pulsar_module(fail_first_n=2)
        with patch.dict(sys.modules, {"pulsar": fake_pulsar}):
            client = get_pulsar_client(
                "pulsar://broker:6650",
                probe_topic="repos.raw",
                retries=5,
                delay=0,
            )

        # two failed probe attempts plus one that succeeded
        self.assertEqual(len(created), 3)
        self.assertIs(client, created[-1])
        # a client whose probe failed must be closed before the next retry
        self.assertTrue(created[0].closed)

    def test_retries_client_constructor_failure(self) -> None:
        calls = {"count": 0}

        def client_factory(url: str) -> _FakeClient:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("client constructor failed")
            return _FakeClient(url, {"calls": [], "producers": []}, fail_first_n=0)

        fake_pulsar = types.SimpleNamespace(Client=client_factory)
        with patch.dict(sys.modules, {"pulsar": fake_pulsar}):
            client = get_pulsar_client(
                "pulsar://broker:6650",
                probe_topic="repos.raw",
                retries=2,
                delay=0,
            )

        self.assertEqual(calls["count"], 2)
        self.assertEqual(client.url, "pulsar://broker:6650")

    def test_raises_after_exhausting_retries(self) -> None:
        fake_pulsar, _, _ = _fake_pulsar_module(fail_first_n=100)
        with patch.dict(sys.modules, {"pulsar": fake_pulsar}):
            with self.assertRaises(RuntimeError):
                get_pulsar_client(
                    "pulsar://broker:6650",
                    probe_topic="repos.raw",
                    retries=3,
                    delay=0,
                )


if __name__ == "__main__":
    unittest.main()
