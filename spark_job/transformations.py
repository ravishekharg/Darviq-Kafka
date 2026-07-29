"""Pure(-ish) DataFrame transformations for the telemetry pipeline.

Every function here takes a DataFrame in and returns a DataFrame out, with
no dependency on a specific source (Kafka) or sink (Parquet). That is
what makes them unit-testable with a local SparkSession and a small
in-memory batch DataFrame (see tests/) without needing Kafka, Docker, or
a streaming query at all -- the same groupBy/window/watermark logic runs
identically whether the input DataFrame is a finite batch or a micro-batch
slice of an infinite stream, which is the core trick that makes Structured
Streaming transformation logic testable in the first place.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_job.schemas import (
    ALLOWED_METRIC_TYPES,
    CORRUPT_RECORD_COLUMN,
    METRIC_VALUE_RANGES,
    get_event_schema,
)

TIMESTAMP_FORMATS = ["yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss'Z'"]


def parse_kafka_value(raw_df: DataFrame, key_col: str = "key", value_col: str = "value") -> DataFrame:
    """Turn the raw Kafka (key, value) bytes columns into a parsed struct.

    Input: any DataFrame with at least a string/binary `value_col` (and
    optionally `key_col`), such as what `spark.readStream.format("kafka")`
    produces.
    Output: same rows, with the raw string preserved as `raw_value` plus
    all fields from the telemetry schema parsed out as top-level columns,
    still UNVALIDATED (that is `classify_events`'s job).
    """
    schema = get_event_schema()
    value_str = F.col(value_col).cast("string")

    parsed = raw_df.withColumn(
        "_parsed",
        F.from_json(
            value_str, schema, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN}
        ),
    )

    out = parsed.select(
        (
            F.col(key_col).cast("string").alias("device_key")
            if key_col in raw_df.columns
            else F.lit(None).alias("device_key")
        ),
        value_str.alias("raw_value"),
        F.col("_parsed.device_id").alias("device_id"),
        F.col("_parsed.timestamp").alias("raw_timestamp"),
        F.col("_parsed.metric_type").alias("metric_type"),
        # Safe cast: returns null for non-numeric strings ("N/A", "", ...)
        # instead of failing the whole record the way a DoubleType field
        # in the from_json schema would have (see schemas.py docstring).
        F.col("_parsed.value").cast("double").alias("value"),
        F.col("_parsed.site_id").alias("site_id"),
        F.col("_parsed.firmware_version").alias("firmware_version"),
        F.col(f"_parsed.{CORRUPT_RECORD_COLUMN}").alias("corrupt_record"),
    )

    # Try each accepted timestamp format in turn; first non-null wins.
    event_time = F.coalesce(*[F.to_timestamp(F.col("raw_timestamp"), fmt) for fmt in TIMESTAMP_FORMATS])
    return out.withColumn("event_time", event_time)


def _value_range_condition() -> Column:
    """Column expression: True when `value` is within the physically
    plausible range for its `metric_type`. Unknown metric_types are
    handled by the enum check upstream, so this only needs to cover the
    known ones; anything else is left False (caught as invalid_metric_type
    first, since reason ordering short-circuits).
    """
    cond = F.lit(False)
    for metric, (lo, hi) in METRIC_VALUE_RANGES.items():
        cond = F.when(
            F.col("metric_type") == metric,
            F.col("value").between(lo, hi),
        ).otherwise(cond)
    return cond


def classify_events(parsed_df: DataFrame) -> DataFrame:
    """Add `is_valid` (bool) and `error_reason` (string, null when valid)
    columns to a DataFrame produced by `parse_kafka_value`.

    Reasons are checked in order of "how broken is this record" -- a
    record failing multiple checks gets the earliest, most fundamental
    reason (e.g. invalid JSON wins over a coincidentally-also-bad
    timestamp) so the dead-letter sink groups records sensibly.
    """
    metric_ok = F.col("metric_type").isin(list(ALLOWED_METRIC_TYPES))

    reason = (
        F.when(F.col("corrupt_record").isNotNull(), F.lit("invalid_json"))
        .when(F.col("device_id").isNull(), F.lit("missing_device_id"))
        .when(F.col("raw_timestamp").isNull() | F.col("event_time").isNull(), F.lit("invalid_timestamp"))
        .when(~metric_ok, F.lit("invalid_metric_type"))
        .when(F.col("value").isNull(), F.lit("missing_or_non_numeric_value"))
        .when(~_value_range_condition(), F.lit("value_out_of_range"))
        .otherwise(F.lit(None).cast("string"))
    )

    return parsed_df.withColumn("error_reason", reason).withColumn("is_valid", F.col("error_reason").isNull())


def split_valid_and_malformed(classified_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a classified DataFrame into (valid_df, dead_letter_df).

    valid_df keeps only the clean, typed business columns needed for
    aggregation. dead_letter_df keeps the raw payload plus the reason,
    so it is actually useful for someone debugging a bad device/firmware
    later -- not just a black hole of dropped records.
    """
    valid_cols = ["device_id", "event_time", "metric_type", "value", "site_id", "firmware_version"]
    valid_df = classified_df.filter(F.col("is_valid")).select(*valid_cols)

    dead_letter_df = classified_df.filter(~F.col("is_valid")).select(
        "raw_value",
        "device_id",
        "error_reason",
        F.current_timestamp().alias("quarantined_at"),
    )
    return valid_df, dead_letter_df


def compute_windowed_aggregates(
    valid_df: DataFrame,
    window_duration: str = "1 minute",
    watermark_delay: str = "2 minutes",
    slide_duration: str | None = None,
) -> DataFrame:
    """Tumbling (or sliding, if `slide_duration` is given) window
    aggregation per (device_id, metric_type), with a watermark on
    `event_time` so state for windows older than `watermark_delay` past
    the max seen event_time is dropped instead of being held forever.

    Works identically against a static batch DataFrame (used in unit
    tests -- the watermark clause is accepted but has no eviction effect
    outside a streaming query) and a streaming DataFrame (used by the
    real job, where it bounds state size and enables `outputMode
    ("append")` to emit only finalized windows).
    """
    watermarked = valid_df.withWatermark("event_time", watermark_delay)

    if slide_duration:
        window_col = F.window(F.col("event_time"), window_duration, slide_duration)
    else:
        window_col = F.window(F.col("event_time"), window_duration)

    aggregated = (
        watermarked.groupBy(window_col, F.col("device_id"), F.col("metric_type"))
        .agg(
            F.avg("value").alias("avg_value"),
            F.count(F.lit(1)).alias("event_count"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("device_id"),
            F.col("metric_type"),
            F.round("avg_value", 3).alias("avg_value"),
            F.col("event_count"),
            F.col("min_value"),
            F.col("max_value"),
        )
    )
    return aggregated


def build_bronze_and_aggregates(
    raw_df: DataFrame,
    window_duration: str = "1 minute",
    watermark_delay: str = "2 minutes",
) -> dict[str, DataFrame]:
    """Convenience wrapper chaining the full pipeline from raw Kafka rows
    to (aggregates, dead_letter). Used by both the real streaming job and
    integration-style tests that want the whole chain in one call.
    """
    parsed = parse_kafka_value(raw_df)
    classified = classify_events(parsed)
    valid_df, dead_letter_df = split_valid_and_malformed(classified)
    aggregates_df = compute_windowed_aggregates(valid_df, window_duration, watermark_delay)
    return {"valid": valid_df, "dead_letter": dead_letter_df, "aggregates": aggregates_df}


def get_or_create_spark(app_name: str = "iot-telemetry-pipeline") -> SparkSession:
    return SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()
