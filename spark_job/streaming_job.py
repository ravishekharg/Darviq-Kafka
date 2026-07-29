"""Structured Streaming entrypoint: Kafka -> parse/validate -> windowed
aggregation -> Parquet, with a separate dead-letter Parquet sink for
records that fail validation.

Run locally (after `docker compose up -d` and the producer is running):

    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        spark_job/streaming_job.py

All paths and Kafka connection details come from environment variables
(see .env.example) so nothing about a specific machine is hardcoded.

Why two separate streaming queries instead of one? The aggregates query
needs `outputMode("append")` gated by the watermark (a window is only
final, and thus only written once, after the watermark passes its end).
The dead-letter query has no windowing at all -- it should write bad
records as soon as they arrive, with no such delay. Forcing both through
one query/output mode would mean either delaying dead-letter visibility
to match the aggregation watermark, or giving up append semantics for the
aggregates. Kafka is fine with two independent consumer groups reading
the same topic, so two queries is the simpler and more correct option
here; it costs an extra read of the topic, not much else at this rate.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql.streaming import StreamingQuery

from spark_job.transformations import (
    classify_events,
    compute_windowed_aggregates,
    get_or_create_spark,
    parse_kafka_value,
    split_valid_and_malformed,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [spark_job] %(message)s")
log = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def main() -> None:
    bootstrap_servers = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = _env("KAFKA_TOPIC", "raw.iot-telemetry")
    output_path = _env("OUTPUT_PATH", "./data/output/aggregates")
    dead_letter_path = _env("DEAD_LETTER_PATH", "./data/output/dead_letter")
    checkpoint_root = _env("CHECKPOINT_PATH", "./data/checkpoints")
    window_duration = _env("WINDOW_DURATION", "1 minute")
    watermark_delay = _env("WATERMARK_DELAY", "2 minutes")
    trigger_interval = _env("TRIGGER_INTERVAL", "30 seconds")
    starting_offsets = _env("STARTING_OFFSETS", "latest")

    spark = get_or_create_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading from Kafka topic '%s' @ %s", topic, bootstrap_servers)
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = parse_kafka_value(raw_df)
    classified_df = classify_events(parsed_df)
    valid_df, dead_letter_df = split_valid_and_malformed(classified_df)
    aggregates_df = compute_windowed_aggregates(valid_df, window_duration, watermark_delay)

    aggregates_query: StreamingQuery = (
        aggregates_df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", f"{checkpoint_root}/aggregates")
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )
    log.info(
        "Started aggregates query %s -> %s (checkpoint: %s/aggregates)",
        aggregates_query.id,
        output_path,
        checkpoint_root,
    )

    dead_letter_query: StreamingQuery = (
        dead_letter_df.writeStream.format("parquet")
        .option("path", dead_letter_path)
        .option("checkpointLocation", f"{checkpoint_root}/dead_letter")
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )
    log.info(
        "Started dead-letter query %s -> %s (checkpoint: %s/dead_letter)",
        dead_letter_query.id,
        dead_letter_path,
        checkpoint_root,
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
