"""Simple Pulsar consumer for checking crawler-published repository records."""

from __future__ import annotations

import json
import logging
import os
import time

import pulsar

log = logging.getLogger(__name__)

DEFAULT_BROKER_URL = "pulsar://pulsar:6650"
# fixed by TJ: toy consumer defaults to the same raw topic as the producer.
DEFAULT_TOPIC = "repos.raw"
DEFAULT_SUBSCRIPTION = "debug-consumer"


def get_pulsar_client(
    broker_url: str,
    retries: int = 20,
    delay: int = 15,
) -> pulsar.Client:
    for attempt in range(1, retries + 1):
        try:
            client = pulsar.Client(broker_url)
            # fixed by TJ: force a broker round trip without leaking the temp producer.
            healthcheck = client.create_producer("persistent://public/default/__healthcheck__")
            healthcheck.close()
            return client
        except Exception as exc:
            log.warning(
                "Attempt %d/%d: Pulsar not ready (%s), retrying in %ds...",
                attempt,
                retries,
                exc,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Pulsar after {retries} attempts.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    broker_url = os.environ.get("PULSAR_SERVICE_URL", DEFAULT_BROKER_URL)
    topic = os.environ.get("PULSAR_TOPIC", DEFAULT_TOPIC)
    subscription = os.environ.get("PULSAR_SUBSCRIPTION", DEFAULT_SUBSCRIPTION)

    client = get_pulsar_client(broker_url)

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

    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
