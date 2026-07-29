"""Spark-side schema definitions for the raw telemetry payload.

Kept separate from producer/ deliberately: in a real system the producer
and the Spark job are different deployables (different teams, different
release cadence, possibly different languages) and should not import each
other's code. The JSON Schema in schemas/telemetry.schema.json is the
one human-readable contract both sides are written against; this module
is just its Spark StructType equivalent, with one addition
(_corrupt_record) that from_json needs to report unparseable JSON.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

CORRUPT_RECORD_COLUMN = "_corrupt_record"

ALLOWED_METRIC_TYPES = ("temperature", "humidity", "battery", "signal_strength")

# (min, max) inclusive plausible physical range per metric_type. Used to
# route out-of-range readings (e.g. a signal_strength of +9000) to the
# dead-letter sink instead of silently averaging them into the aggregates.
METRIC_VALUE_RANGES: dict[str, tuple[float, float]] = {
    "temperature": (-40.0, 85.0),  # industrial sensor operating range
    "humidity": (0.0, 100.0),
    "battery": (0.0, 100.0),
    "signal_strength": (-130.0, -20.0),  # dBm
}


def get_event_schema() -> StructType:
    """Schema used with from_json(..., mode='PERMISSIVE') over the raw
    Kafka `value` column.

    Every field is StringType here, including `value` and `timestamp` --
    deliberately, even though `value` is logically numeric. from_json's
    PERMISSIVE mode does not politely null out one bad scalar field and
    keep going the way missing-object-field handling does: if a field
    fails to coerce to its declared type (e.g. the string "N/A" against a
    DoubleType field), Jackson raises a parse error for the WHOLE record,
    which surfaces as `_corrupt_record` rather than a per-field null. That
    would make "device sent a non-numeric value" indistinguishable from
    "device sent broken JSON" -- two different failure modes we want
    reported with two different dead-letter reasons. Parsing everything
    as a string and doing our own `.cast("double")` afterwards (see
    parse_kafka_value in transformations.py) gives us that distinction
    back, since a SQL cast just returns null on failure instead of
    aborting the whole row.
    """
    return StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("metric_type", StringType(), True),
            StructField("value", StringType(), True),
            StructField("site_id", StringType(), True),
            StructField("firmware_version", StringType(), True),
            StructField(CORRUPT_RECORD_COLUMN, StringType(), True),
        ]
    )
