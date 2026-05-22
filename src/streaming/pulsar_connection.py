"""Shared Pulsar connection helper used by the producer and the consumer."""

from __future__ import annotations

import logging
import time

log = logging.getLogger(__name__)


def get_pulsar_client(
    url: str,
    probe_topic: str,
    retries: int = 20,
    delay: int = 15,
):
    """Return a Pulsar client, but only once the broker actually answers.

    fixed by TJ: the problem was that pulsar.Client(url) is lazy -- it builds
    the client object but does not open any connection, so it succeeds even
    when the broker is down. The old producer retry loop only wrapped that
    call, so it never really waited for Pulsar; the run then died later on
    create_producer instead. The fix: actually create and close a producer on
    probe_topic, which forces a real round trip to the broker, and retry that.
    probe_topic is the topic the caller is going to use anyway, so this also
    avoids leaving a stray __healthcheck__ topic behind like the old consumer
    code did.
    """
    try:
        import pulsar
    except ImportError as exc:
        raise RuntimeError(
            "Missing Pulsar Python client. Install it with "
            "`pip install pulsar-client`."
        ) from exc

    for attempt in range(1, retries + 1):
        client = None
        try:
            # fixed by TJ: construct the client inside the retry block too,
            # because some Pulsar client failures can happen before probing.
            client = pulsar.Client(url)
            probe = client.create_producer(probe_topic)
            probe.close()
            log.info("Connected to Pulsar at %s", url)
            return client
        except Exception as exc:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            log.warning(
                "Attempt %d/%d: Pulsar not ready (%s), retrying in %ds...",
                attempt,
                retries,
                exc,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Pulsar after {retries} attempts.")
