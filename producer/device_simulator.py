"""Synthetic IoT device fleet simulation.

Each simulated device is a small state machine that:
  - drifts its metric values with a bounded random walk (not just noise --
    a real sensor's next reading is correlated with its last one),
  - occasionally goes "offline" for a random number of ticks (no events
    produced, then it resumes -- and its next event may carry a
    stale/past timestamp as if buffered readings are catching up),
  - occasionally emits a malformed payload (missing field, wrong type,
    invalid enum value, or truncated JSON) to simulate a buggy firmware
    version or a flaky radio link corrupting the payload,
  - occasionally emits an out-of-order / late timestamp even while online,
    simulating clock skew or store-and-forward buffering upstream of Kafka.

This module has no Kafka dependency -- it is pure Python so it can be
unit-tested (see tests/) independently of any broker.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

METRIC_RANGES: dict[str, tuple[float, float, float]] = {
    # metric_type: (min, max, typical_step_per_tick)
    "temperature": (15.0, 40.0, 0.5),
    "humidity": (10.0, 95.0, 2.0),
    "battery": (0.0, 100.0, 0.3),
    "signal_strength": (-110.0, -40.0, 3.0),
}

MALFORMED_KINDS = (
    "missing_required_field",
    "invalid_metric_type",
    "non_numeric_value",
    "truncated_json",
    "bad_timestamp",
)


def new_device_id(rng: random.Random) -> str:
    return f"dev-{uuid.UUID(int=rng.getrandbits(128)).hex[:8]}"


@dataclass
class DeviceState:
    device_id: str
    site_id: str
    firmware_version: str
    metric_types: list[str]
    rng: random.Random
    values: dict[str, float] = field(default_factory=dict)
    online: bool = True
    offline_ticks_remaining: int = 0

    def __post_init__(self) -> None:
        if not self.values:
            for metric in self.metric_types:
                lo, hi, _ = METRIC_RANGES[metric]
                self.values[metric] = self.rng.uniform(lo, hi)

    def _walk(self, metric: str) -> float:
        lo, hi, step = METRIC_RANGES[metric]
        current = self.values[metric]
        if metric == "battery":
            # Batteries mostly drain, with occasional recharge events.
            delta = -abs(self.rng.gauss(step, step / 3))
            if self.rng.random() < 0.01:
                delta = self.rng.uniform(20, 60)  # recharge
        else:
            delta = self.rng.gauss(0, step)
        new_value = min(hi, max(lo, current + delta))
        self.values[metric] = new_value
        return round(new_value, 2)

    def maybe_toggle_offline(self, flip_probability: float, max_offline_ticks: int) -> None:
        if self.online:
            if self.rng.random() < flip_probability:
                self.online = False
                self.offline_ticks_remaining = self.rng.randint(3, max_offline_ticks)
        else:
            self.offline_ticks_remaining -= 1
            if self.offline_ticks_remaining <= 0:
                self.online = True

    def build_event(self, event_time: datetime) -> dict[str, Any]:
        metric = self.rng.choice(self.metric_types)
        value = self._walk(metric)
        return {
            "device_id": self.device_id,
            "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "metric_type": metric,
            "value": value,
            "site_id": self.site_id,
            "firmware_version": self.firmware_version,
        }


def make_fleet(num_devices: int, rng: random.Random) -> list[DeviceState]:
    sites = [f"site-{i:02d}" for i in range(1, max(2, num_devices // 5))]
    firmwares = ["1.4.2", "1.4.3", "1.5.0-rc1", "1.3.9"]
    fleet = []
    for _ in range(num_devices):
        num_metrics = rng.choice([1, 1, 2, 2, 3])
        metrics = rng.sample(list(METRIC_RANGES), k=num_metrics)
        fleet.append(
            DeviceState(
                device_id=new_device_id(rng),
                site_id=rng.choice(sites),
                firmware_version=rng.choice(firmwares),
                metric_types=metrics,
                rng=rng,
            )
        )
    return fleet


def corrupt_event(event: dict[str, Any], kind: str, rng: random.Random) -> Any:
    """Return a corrupted version of `event`. May return a dict (still valid
    JSON but semantically bad) or a raw string (not even valid JSON), matching
    the different failure modes the Spark job has to detect and quarantine.
    """
    event = dict(event)
    if kind == "missing_required_field":
        field_to_drop = rng.choice(["device_id", "timestamp", "metric_type", "value"])
        del event[field_to_drop]
        return event
    if kind == "invalid_metric_type":
        event["metric_type"] = rng.choice(["temp", "unknown", "", "TEMPERATURE"])
        return event
    if kind == "non_numeric_value":
        event["value"] = rng.choice(["N/A", "ERR", "", "null"])
        return event
    if kind == "bad_timestamp":
        event["timestamp"] = rng.choice(["not-a-timestamp", "2026-13-40T99:99:99Z", "", "1753776000"])
        return event
    if kind == "truncated_json":
        raw = str(event)
        cut = rng.randint(5, max(6, len(raw) - 5))
        return raw[:cut]
    return event


def random_late_timestamp(now: datetime, max_late_seconds: int, rng: random.Random) -> datetime:
    delay = rng.randint(5, max_late_seconds)
    return now - timedelta(seconds=delay)
