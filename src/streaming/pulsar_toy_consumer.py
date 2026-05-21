"""Simple Pulsar consumer for checking crawler-published repository records."""

from __future__ import annotations

import json
import logging
import os

from streaming.pulsar_connection import get_pulsar_client

log = logging.getLogger(__name__)

DEFAULT_BROKER_URL = "pulsar://pulsar:6650"
# fixed by TJ: toy consumer defaults to the same raw topic as the producer.
DEFAULT_TOPIC = "repos.raw"
DEFAULT_SUBSCRIPTION = "debug-consumer"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    broker_url = os.environ.get("PULSAR_SERVICE_URL", DEFAULT_BROKER_URL)
    topic = os.environ.get("PULSAR_TOPIC", DEFAULT_TOPIC)
    subscription = os.environ.get("PULSAR_SUBSCRIPTION", DEFAULT_SUBSCRIPTION)

    # fixed by TJ: PULSAR_MAX_MESSAGES lets us run a bounded smoke test
    # ("consume N messages then exit"). Before this the consumer could only
    # block forever, so there was no way to verify the pipeline and stop.
    # Unset = run forever, which is what the Swarm service still wants.
    max_messages_env = os.environ.get("PULSAR_MAX_MESSAGES")
    max_messages = int(max_messages_env) if max_messages_env else None

    # fixed by TJ: import pulsar lazily, like the producer does, so importing
    # this module (e.g. in a test) does not require the pulsar-client package.
    import pulsar

    client = get_pulsar_client(broker_url, probe_topic=topic)

    consumer = client.subscribe(
        topic,
        subscription_name=subscription,
        consumer_type=pulsar.ConsumerType.Shared,
    )

    log.info(
        "Consumer connected: broker=%s topic=%s subscription=%s",
        broker_url,
        topic,
        subscription,
    )

    consumed = 0
    try:
        while True:
            msg = consumer.receive()

            try:
                data = msg.data().decode("utf-8")
                record = json.loads(data)

                log.info(
                    "received repo_id=%s full_name=%s stars=%s language=%s",
                    record.get("repo_id"),
                    record.get("full_name"),
                    record.get("stars"),
                    record.get("language"),
                )

                consumer.acknowledge(msg)

            except Exception:
                log.exception("failed to process message")
                consumer.negative_acknowledge(msg)

            consumed += 1
            if max_messages is not None and consumed >= max_messages:
                log.info("reached PULSAR_MAX_MESSAGES=%d; exiting", max_messages)
                break

    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
