"""Unit tests for spark_job/transformations.py:compute_windowed_aggregates.

These exercise the windowing/grouping math itself (bucket boundaries,
avg/count/min/max correctness, and that bucketing is driven by
`event_time` -- not arrival order, which is what actually matters for
out-of-order data) using a static batch DataFrame. Batch mode is enough
to test this because `window()` assigns a row to a bucket purely from its
event_time value; it does not care whether the DataFrame is a finite
batch or one micro-batch of an infinite stream. The watermark's *state
eviction* behavior (dropping data that arrives too late) is a genuinely
streaming-only concern and is covered separately in test_watermark.py.
"""

from datetime import datetime

from spark_job.transformations import compute_windowed_aggregates


def _row(device_id, ts, metric, value):
    """One literal row as a SQL SELECT fragment (device_id, event_time,
    metric_type, value, site_id, firmware_version).

    Built via SQL text rather than `spark.createDataFrame(rows, schema)`
    so these tests run the same way in any environment -- see the
    docstring on `_rows_to_df` in test_parsing.py for why.
    """
    return (
        f"SELECT '{device_id}' AS device_id, TIMESTAMP('{ts}') AS event_time, "
        f"'{metric}' AS metric_type, CAST({value} AS DOUBLE) AS value, "
        f"'site-01' AS site_id, '1.4.2' AS firmware_version"
    )


def _df(spark, rows):
    return spark.sql(" UNION ALL ".join(rows))


def test_single_window_average_and_count(spark):
    rows = [
        _row("dev-aaaa1111", "2026-07-29T10:00:05", "temperature", 20.0),
        _row("dev-aaaa1111", "2026-07-29T10:00:35", "temperature", 22.0),
    ]
    df = _df(spark, rows)
    agg = compute_windowed_aggregates(df, window_duration="1 minute", watermark_delay="2 minutes")
    result = agg.collect()

    assert len(result) == 1
    row = result[0]
    assert row.device_id == "dev-aaaa1111"
    assert row.metric_type == "temperature"
    assert row.event_count == 2
    assert row.avg_value == 21.0
    assert row.min_value == 20.0
    assert row.max_value == 22.0
    assert row.window_start == datetime(2026, 7, 29, 10, 0, 0)
    assert row.window_end == datetime(2026, 7, 29, 10, 1, 0)


def test_events_in_different_windows_produce_separate_rows(spark):
    rows = [
        _row("dev-bbbb2222", "2026-07-29T10:00:10", "humidity", 40.0),
        _row("dev-bbbb2222", "2026-07-29T10:02:10", "humidity", 60.0),
    ]
    df = _df(spark, rows)
    agg = compute_windowed_aggregates(df, window_duration="1 minute", watermark_delay="2 minutes")
    result = sorted(agg.collect(), key=lambda r: r.window_start)

    assert len(result) == 2
    assert result[0].event_count == 1
    assert result[0].avg_value == 40.0
    assert result[1].event_count == 1
    assert result[1].avg_value == 60.0
    assert result[0].window_start < result[1].window_start


def test_out_of_order_arrival_still_buckets_by_event_time_not_arrival_order(spark):
    # Row order here is deliberately "late-arriving first": a reading whose
    # event_time is earlier appears AFTER a reading whose event_time is
    # later, in the list passed to createDataFrame. If bucketing were ever
    # (incorrectly) influenced by row/arrival order rather than the
    # event_time value, this would misbucket one of them.
    rows = [
        _row("dev-cccc3333", "2026-07-29T10:05:50", "battery", 80.0),  # "arrives" first
        _row("dev-cccc3333", "2026-07-29T10:05:05", "battery", 90.0),  # earlier event_time, "arrives" second
    ]
    df = _df(spark, rows)
    agg = compute_windowed_aggregates(df, window_duration="1 minute", watermark_delay="2 minutes")
    result = agg.collect()

    # Both events fall in the same [10:05:00, 10:06:00) window regardless
    # of the order they were listed/arrived in.
    assert len(result) == 1
    row = result[0]
    assert row.event_count == 2
    assert row.avg_value == 85.0
    assert row.min_value == 80.0
    assert row.max_value == 90.0


def test_separate_devices_and_metrics_do_not_mix(spark):
    rows = [
        _row("dev-dddd4444", "2026-07-29T10:00:10", "temperature", 20.0),
        _row("dev-eeee5555", "2026-07-29T10:00:10", "temperature", 100.0),
        _row("dev-dddd4444", "2026-07-29T10:00:20", "humidity", 50.0),
    ]
    df = _df(spark, rows)
    agg = compute_windowed_aggregates(df, window_duration="1 minute", watermark_delay="2 minutes")
    result = {(r.device_id, r.metric_type): r for r in agg.collect()}

    assert len(result) == 3
    assert result[("dev-dddd4444", "temperature")].avg_value == 20.0
    assert result[("dev-eeee5555", "temperature")].avg_value == 100.0
    assert result[("dev-dddd4444", "humidity")].avg_value == 50.0


def test_sliding_window_produces_overlapping_buckets(spark):
    rows = [_row("dev-ffff6666", "2026-07-29T10:00:30", "signal_strength", -70.0)]
    df = _df(spark, rows)
    agg = compute_windowed_aggregates(
        df, window_duration="1 minute", watermark_delay="2 minutes", slide_duration="30 seconds"
    )
    result = agg.collect()

    # A 1-minute window sliding every 30s means a single instant falls
    # into 2 overlapping windows.
    assert len(result) == 2
