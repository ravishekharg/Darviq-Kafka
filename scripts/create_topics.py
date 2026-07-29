"""Explicitly create the raw telemetry topic with a deliberate partition
count and retention, instead of relying on Kafka's auto-create-topics
default (which the local docker-compose.yml enables for demo
convenience, but which a real cluster should NOT rely on -- auto-created
topics get whatever partition count/replication the broker default
happens to be, not what your throughput/ordering requirements need).

Run after `docker compose up -d`:

    python scripts/create_topics.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [create_topics] %(message)s")
log = logging.getLogger(__name__)


def wait_for_broker(
    bootstrap_servers: str, retries: int = 15, delay_seconds: float = 2.0
) -> KafkaAdminClient:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="create-topics-script")
        except Exception as exc:  # noqa: BLE001 -- broker not ready yet, keep retrying
            last_error = exc
            log.info("Broker not ready yet (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers}") from last_error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "raw.iot-telemetry"))
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--retention-ms", type=int, default=7 * 24 * 60 * 60 * 1000)  # 7 days
    args = parser.parse_args()

    admin = wait_for_broker(args.bootstrap_servers)
    topic = NewTopic(
        name=args.topic,
        num_partitions=args.partitions,
        replication_factor=args.replication_factor,
        topic_configs={"retention.ms": str(args.retention_ms)},
    )
    try:
        admin.create_topics([topic])
        log.info(
            "Created topic '%s' (partitions=%d, replication_factor=%d)",
            args.topic,
            args.partitions,
            args.replication_factor,
        )
    except TopicAlreadyExistsError:
        log.info("Topic '%s' already exists -- nothing to do.", args.topic)
    finally:
        admin.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
