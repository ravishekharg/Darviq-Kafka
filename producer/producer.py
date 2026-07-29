"""Kafka producer for synthetic IoT device telemetry.

Run:
    python -m producer.producer

All behavior is controlled via environment variables -- see
producer/config.py and .env.example. No credentials are required for the
local Docker Compose broker (PLAINTEXT listener), so there is nothing to
put in a secret here; .env is still gitignored as a matter of habit for
anything that later carries SASL credentials against a real cluster.
"""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from producer.config import ProducerConfig
from producer.device_simulator import (
    MALFORMED_KINDS,
    corrupt_event,
    make_fleet,
    random_late_timestamp,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [producer] %(message)s",
)
log = logging.getLogger(__name__)


def build_producer(cfg: ProducerConfig) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=cfg.bootstrap_servers,
        value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=50,
    )


def _serialize(payload) -> bytes:
    if isinstance(payload, str):
        # Already-corrupted raw string (e.g. truncated JSON) -- send as-is.
        return payload.encode("utf-8")
    return json.dumps(payload).encode("utf-8")


def run(cfg: ProducerConfig | None = None) -> None:
    cfg = cfg or ProducerConfig()
    rng = random.Random(cfg.random_seed)
    fleet = make_fleet(cfg.num_devices, rng)
    producer = build_producer(cfg)

    log.info(
        "Starting producer: %s devices -> topic '%s' @ %s (%.1f events/sec)",
        cfg.num_devices,
        cfg.topic,
        cfg.bootstrap_servers,
        cfg.events_per_second,
    )

    sent = 0
    malformed_sent = 0
    late_sent = 0
    start = time.monotonic()
    period = 1.0 / cfg.events_per_second if cfg.events_per_second > 0 else 0.1

    try:
        while True:
            if cfg.run_seconds and (time.monotonic() - start) >= cfg.run_seconds:
                break

            for device in fleet:
                device.maybe_toggle_offline(cfg.offline_flip_probability, cfg.max_offline_ticks)

            online_devices = [d for d in fleet if d.online]
            if not online_devices:
                time.sleep(period)
                continue

            device = rng.choice(online_devices)
            now = datetime.now(timezone.utc)
            event_time = now

            is_late = rng.random() < cfg.late_event_rate
            if is_late:
                event_time = random_late_timestamp(now, cfg.max_late_seconds, rng)
                late_sent += 1

            event = device.build_event(event_time)

            is_malformed = rng.random() < cfg.malformed_rate
            payload = event
            if is_malformed:
                kind = rng.choice(MALFORMED_KINDS)
                payload = corrupt_event(event, kind, rng)
                malformed_sent += 1

            try:
                producer.send(
                    cfg.topic,
                    key=device.device_id,
                    value=_serialize(payload),
                )
                sent += 1

                if rng.random() < cfg.duplicate_rate:
                    # Simulate at-least-once upstream redelivery.
                    producer.send(
                        cfg.topic,
                        key=device.device_id,
                        value=_serialize(payload),
                    )
                    sent += 1
            except KafkaError:
                log.exception("Failed to send event for %s", device.device_id)

            if sent % 200 == 0:
                log.info(
                    "sent=%d malformed=%d late=%d online=%d/%d",
                    sent,
                    malformed_sent,
                    late_sent,
                    len(online_devices),
                    len(fleet),
                )

            time.sleep(period)
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
        log.info("Stopped. total_sent=%d malformed=%d late=%d", sent, malformed_sent, late_sent)


if __name__ == "__main__":
    run()
