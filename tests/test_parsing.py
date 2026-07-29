"""Unit tests for parsing + validation (spark_job/transformations.py:
parse_kafka_value, classify_events, split_valid_and_malformed).

Every test builds a tiny static (batch) DataFrame shaped exactly like
what `spark.readStream.format("kafka")` would hand the job (a `key` and
`value` string column) and runs it through the same functions the real
streaming job uses. No Kafka broker, no Docker, no network -- just a
local SparkSession.
"""

from spark_job.transformations import (
    classify_events,
    parse_kafka_value,
    split_valid_and_malformed,
)

VALID_JSON = (
    '{"device_id": "dev-0000abcd", "timestamp": "2026-07-29T10:00:00.000Z", '
    '"metric_type": "temperature", "value": 21.5, "site_id": "site-01", '
    '"firmware_version": "1.4.2"}'
)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _rows_to_df(spark, rows):
    """Build a (key, value) DataFrame from literal SQL, not
    `spark.createDataFrame(rows, schema)`.

    This sidesteps two Windows/sandbox-specific local-mode quirks that
    have nothing to do with the transformation logic being tested:
    `createDataFrame` from a local Python list ships rows through a
    spawned PySpark worker process, and `spark.read`/file-source APIs on
    Windows need the winutils.exe/hadoop.dll native IO shim configured.
    A literal `SELECT ... UNION ALL ...` is parsed and evaluated entirely
    by Catalyst in the driver JVM, so it works everywhere Spark itself
    works, with no extra local setup. Equivalent to createDataFrame in
    every other respect for these tests.
    """
    selects = [f"SELECT '{_escape(key)}' AS key, '{_escape(value)}' AS value" for key, value in rows]
    return spark.sql(" UNION ALL ".join(selects))


def _classify(spark, rows):
    raw_df = _rows_to_df(spark, rows)
    return classify_events(parse_kafka_value(raw_df))


def test_valid_event_is_marked_valid(spark):
    df = _classify(spark, [("dev-0000abcd", VALID_JSON)])
    row = df.collect()[0]
    assert row.is_valid is True
    assert row.error_reason is None
    assert row.device_id == "dev-0000abcd"
    assert row.metric_type == "temperature"
    assert row.value == 21.5


def test_missing_device_id_is_quarantined(spark):
    bad_json = '{"timestamp": "2026-07-29T10:00:00.000Z", "metric_type": "temperature", "value": 21.5}'
    df = _classify(spark, [("k", bad_json)])
    row = df.collect()[0]
    assert row.is_valid is False
    assert row.error_reason == "missing_device_id"


def test_invalid_metric_type_is_quarantined(spark):
    bad_json = (
        '{"device_id": "dev-0000abcd", "timestamp": "2026-07-29T10:00:00.000Z", '
        '"metric_type": "warp_factor", "value": 21.5}'
    )
    df = _classify(spark, [("k", bad_json)])
    row = df.collect()[0]
    assert row.is_valid is False
    assert row.error_reason == "invalid_metric_type"


def test_non_numeric_value_is_quarantined(spark):
    bad_json = (
        '{"device_id": "dev-0000abcd", "timestamp": "2026-07-29T10:00:00.000Z", '
        '"metric_type": "temperature", "value": "N/A"}'
    )
    df = _classify(spark, [("k", bad_json)])
    row = df.collect()[0]
    assert row.is_valid is False
    assert row.error_reason == "missing_or_non_numeric_value"


def test_bad_timestamp_is_quarantined(spark):
    bad_json = (
        '{"device_id": "dev-0000abcd", "timestamp": "not-a-timestamp", '
        '"metric_type": "temperature", "value": 21.5}'
    )
    df = _classify(spark, [("k", bad_json)])
    row = df.collect()[0]
    assert row.is_valid is False
    assert row.error_reason == "invalid_timestamp"


def test_truncated_json_is_quarantined_as_invalid_json(spark):
    truncated = VALID_JSON[:20]  # cuts the JSON off mid-object
    df = _classify(spark, [("k", truncated)])
    row = df.collect()[0]
    assert row.is_valid is False
    assert row.error_reason == "invalid_json"


def test_out_of_range_value_is_quarantined(spark):
    bad_json = (
        '{"device_id": "dev-0000abcd", "timestamp": "2026-07-29T10:00:00.000Z", '
        '"metric_type": "temperature", "value": 999.9}'
    )
    df = _classify(spark, [("k", bad_json)])
    row = df.collect()[0]
    assert row.is_valid is False
    assert row.error_reason == "value_out_of_range"


def test_split_routes_rows_to_correct_dataframe(spark):
    bad_json = '{"device_id": "dev-0000abcd", "metric_type": "temperature", "value": 21.5}'  # no timestamp
    classified = _classify(spark, [("k1", VALID_JSON), ("k2", bad_json)])
    valid_df, dead_letter_df = split_valid_and_malformed(classified)

    assert valid_df.count() == 1
    assert dead_letter_df.count() == 1

    dl_row = dead_letter_df.collect()[0]
    assert dl_row.error_reason == "invalid_timestamp"
    assert dl_row.raw_value == bad_json  # original payload preserved for debugging

    valid_row = valid_df.collect()[0]
    assert set(valid_row.asDict().keys()) == {
        "device_id",
        "event_time",
        "metric_type",
        "value",
        "site_id",
        "firmware_version",
    }
