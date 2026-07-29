"""Test that the watermark in compute_windowed_aggregates actually drops
state for windows that have already closed, instead of just being a
harmless annotation.

This is the one test in this suite that needs a REAL streaming query --
batch DataFrames never advance a watermark or evict state, so this
behavior can't be observed in batch mode (see test_aggregation.py for
that boundary explained). To test it without Kafka/Docker, we use:

  - a JSON file source instead of Kafka (same DataFrame shape once read),
  - `trigger(availableNow=True)`, which processes whatever files are
    currently in the input directory and then stops on its own, and
  - a shared checkpoint directory across three separate `.start()` calls,
    which is what carries the watermark/aggregation state between them
    (structurally the same thing that happens across micro-batches
    within one long-running query -- state lives in the checkpoint, not
    in the Python process).

Sequence: ingest an on-time window's worth of data, then a far-future
event that pushes the watermark well past that window's end (closing
it and causing it to be emitted in append mode), then a very late event
that lands back inside the now-closed window. The assertion is that the
late event in step 3 is dropped -- the emitted aggregate for that window
does not change.
"""

import json
from pathlib import Path

from pyspark.sql import functions as F

from spark_job.transformations import compute_windowed_aggregates

VALID_SCHEMA = "device_id string, event_time string, metric_type string, value double, site_id string, firmware_version string"


def _write_batch(input_dir: Path, name: str, records: list[dict]) -> None:
    path = input_dir / name
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _run_available_now(spark, input_dir, checkpoint_dir, output_dir):
    raw = spark.readStream.format("json").schema(VALID_SCHEMA).load(str(input_dir))
    valid_df = raw.withColumn("event_time", F.to_timestamp("event_time"))
    agg_df = compute_windowed_aggregates(valid_df, window_duration="1 minute", watermark_delay="1 minute")
    query = (
        agg_df.writeStream.format("parquet")
        .option("path", str(output_dir))
        .option("checkpointLocation", str(checkpoint_dir))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def test_late_event_past_watermark_is_dropped_from_output(spark, tmp_path):
    input_dir = tmp_path / "input"
    checkpoint_dir = tmp_path / "checkpoint"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    device = "dev-9999ffff"

    # Batch 1: two on-time readings both inside window [10:00:00, 10:01:00).
    _write_batch(
        input_dir,
        "batch1.json",
        [
            {
                "device_id": device,
                "event_time": "2026-07-29T10:00:10",
                "metric_type": "temperature",
                "value": 10.0,
                "site_id": "site-01",
                "firmware_version": "1.4.2",
            },
            {
                "device_id": device,
                "event_time": "2026-07-29T10:00:40",
                "metric_type": "temperature",
                "value": 20.0,
                "site_id": "site-01",
                "firmware_version": "1.4.2",
            },
        ],
    )
    _run_available_now(spark, input_dir, checkpoint_dir, output_dir)

    # Right after batch 1: max event_time seen is 10:00:40, watermark_delay
    # is 1 minute, so the watermark is 09:59:40 -- the [10:00,10:01) window
    # hasn't closed yet, so append-mode must not have emitted anything.
    if output_dir.exists() and any(output_dir.glob("*.parquet")):
        assert spark.read.parquet(str(output_dir)).count() == 0

    # Batch 2: one far-future reading. This advances the watermark to
    # 10:04:00, which is past the first window's end (10:01:00), closing
    # it and causing it to finally be emitted.
    _write_batch(
        input_dir,
        "batch2.json",
        [
            {
                "device_id": device,
                "event_time": "2026-07-29T10:05:00",
                "metric_type": "temperature",
                "value": 30.0,
                "site_id": "site-01",
                "firmware_version": "1.4.2",
            }
        ],
    )
    _run_available_now(spark, input_dir, checkpoint_dir, output_dir)

    result_after_batch2 = spark.read.parquet(str(output_dir)).collect()
    assert len(result_after_batch2) == 1
    first_window = result_after_batch2[0]
    assert first_window.event_count == 2
    assert first_window.avg_value == 15.0

    # Batch 3: a very late reading whose event_time (10:00:15) falls back
    # inside the now-closed [10:00,10:01) window, arriving well after the
    # watermark (10:04:00) has already passed it. Structured Streaming's
    # watermark contract says this row must be dropped, not merged into
    # a re-opened window.
    _write_batch(
        input_dir,
        "batch3.json",
        [
            {
                "device_id": device,
                "event_time": "2026-07-29T10:00:15",
                "metric_type": "temperature",
                "value": 999.0,  # if this leaked into the average, it would be very obvious
                "site_id": "site-01",
                "firmware_version": "1.4.2",
            }
        ],
    )
    _run_available_now(spark, input_dir, checkpoint_dir, output_dir)

    final_result = spark.read.parquet(str(output_dir)).collect()
    windows_for_first_bucket = [r for r in final_result if r.window_start.minute == 0]
    assert len(windows_for_first_bucket) == 1
    assert windows_for_first_bucket[0].event_count == 2  # unchanged -- late row was dropped
    assert windows_for_first_bucket[0].avg_value == 15.0  # unchanged, not polluted by 999.0
