"""Producer configuration, sourced entirely from environment variables.

Nothing here is a secret -- bootstrap servers and topic names are not
credentials -- but keeping every tunable in env vars (with sane local
defaults) means the same image/script can point at a different cluster
in CI or elsewhere without touching code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load a local .env file if present (gitignored). Safe no-op if absent.
load_dotenv()


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic: str = os.getenv("KAFKA_TOPIC", "raw.iot-telemetry")

    num_devices: int = _int("NUM_DEVICES", 25)
    events_per_second: float = _float("EVENTS_PER_SECOND", 10.0)

    # Data-quality knobs -- these are what force the Spark job to do real
    # work instead of a trivial pass-through.
    malformed_rate: float = _float("MALFORMED_RATE", 0.03)
    late_event_rate: float = _float("LATE_EVENT_RATE", 0.06)
    duplicate_rate: float = _float("DUPLICATE_RATE", 0.02)
    offline_flip_probability: float = _float("OFFLINE_FLIP_PROBABILITY", 0.002)
    max_offline_ticks: int = _int("MAX_OFFLINE_TICKS", 40)

    # Late events are delayed by up to this many seconds of "device clock"
    # relative to wall-clock produce time.
    max_late_seconds: int = _int("MAX_LATE_SECONDS", 240)

    # 0 means run until interrupted (Ctrl+C).
    run_seconds: int = _int("RUN_SECONDS", 0)

    random_seed: int | None = int(os.getenv("RANDOM_SEED")) if os.getenv("RANDOM_SEED") else None
