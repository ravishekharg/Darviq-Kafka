"""Generates Darviq_Kafka_Low_Level_Design.docx from docx_builder.DesignDoc.

Run from the Docs/ directory (or anywhere, as long as docx_builder.py is
importable):

    python generate_lld.py
"""
from __future__ import annotations

from docx_builder import DesignDoc

VERSION = "1.0"
DATE = "July 31, 2026"


def build() -> DesignDoc:
    doc = DesignDoc(
        project_name="Darviq Kafka",
        subtitle="Streaming IoT Telemetry Pipeline -- Kafka (KRaft) + PySpark Structured Streaming",
        doc_kind="Low-Level Design (LLD)",
        version=VERSION,
        date=DATE,
    )
    doc.add_document_control()
    doc.add_toc_field()

    # 1. Introduction
    doc.add_heading1("1. Introduction")
    doc.add_heading2("1.1 Purpose")
    doc.add_paragraph(
        "This Low-Level Design (LLD) document provides the implementation-level detail behind the "
        "Darviq Kafka High-Level Design (HLD): concrete file paths, function names, schema field lists, "
        "Kafka topic/consumer configuration, and the exact windowing/watermark parameters actually used "
        "in the codebase. Where the HLD describes what each module is responsible for, this document "
        "describes how it does it, referencing the real source files rather than a generic pattern."
    )
    doc.add_heading2("1.2 Scope")
    doc.add_paragraph(
        "This document covers the detailed design of the producer, Kafka topic bootstrap, the "
        "Spark Structured Streaming job's parsing/classification/windowing/dead-letter logic, the input "
        "and output data schemas, sequence flows for both a normal and a malformed/late event, the "
        "concrete windowing and watermark algorithm, and validation/error-handling behavior including "
        "known gaps. It does not restate the architecture rationale already covered in the HLD (Section "
        "3 there); this document assumes that context."
    )
    doc.add_heading2("1.3 References")
    doc.add_bullets(
        [
            "Darviq Kafka High-Level Design document (Docs/Darviq_Kafka_High_Level_Design.docx) -- "
            "companion document; this LLD elaborates its Section 5 (module-wise design) and Section 6 "
            "(data design).",
            "README.md -- architecture diagram, design-decision rationale, and the \"what was actually "
            "verified\" section with real run output.",
            "producer/producer.py, producer/device_simulator.py, producer/config.py",
            "schemas/telemetry.schema.json",
            "scripts/create_topics.py, scripts/inspect_output.py",
            "spark_job/schemas.py, spark_job/transformations.py, spark_job/streaming_job.py",
            "tests/test_parsing.py, tests/test_aggregation.py, tests/test_watermark.py, tests/conftest.py",
            "docker-compose.yml, .env.example",
        ]
    )

    # 2. Detailed module design
    doc.add_heading1("2. Detailed module design")

    doc.add_heading2("2.1 Device producer and fleet simulation")
    doc.add_paragraph(
        "File: producer/device_simulator.py. DeviceState is a dataclass holding device_id, site_id, "
        "firmware_version, its list of metric_types, and a per-metric current value. METRIC_RANGES "
        "defines (min, max, typical_step_per_tick) per metric_type: temperature (15.0-40.0, step 0.5), "
        "humidity (10.0-95.0, step 2.0), battery (0.0-100.0, step 0.3), signal_strength (-110.0 to -40.0, "
        "step 3.0). _walk() applies a Gaussian-jittered delta per tick (battery instead mostly drains via "
        "-abs(gauss(step, step/3)), with a 1% chance per tick of a recharge event adding 20-60), clamped "
        "to the metric's [min, max] range. maybe_toggle_offline() flips a device offline with probability "
        "OFFLINE_FLIP_PROBABILITY (default 0.002 per tick) for a random number of ticks up to "
        "MAX_OFFLINE_TICKS (default 40)."
    )
    doc.add_paragraph(
        "make_fleet(num_devices, rng) builds the fleet, assigning each device 1-3 metric_types (weighted "
        "toward 1-2 via rng.choice([1,1,2,2,3])), a site_id from a generated pool of "
        "max(2, num_devices // 5) sites, and a firmware_version from a fixed pool "
        "[\"1.4.2\",\"1.4.3\",\"1.5.0-rc1\",\"1.3.9\"]. corrupt_event(event, kind, rng) implements the five "
        "MALFORMED_KINDS: missing_required_field (drops one of device_id/timestamp/metric_type/value), "
        "invalid_metric_type (sets metric_type to one of \"temp\"/\"unknown\"/\"\"/\"TEMPERATURE\"), "
        "non_numeric_value (sets value to \"N/A\"/\"ERR\"/\"\"/\"null\"), bad_timestamp (sets timestamp to "
        "\"not-a-timestamp\"/\"2026-13-40T99:99:99Z\"/\"\"/\"1753776000\"), and truncated_json (converts the "
        "event dict to its Python str() representation and slices it at a random cut point, producing "
        "genuinely unparseable JSON, not just a semantically-bad-but-valid one)."
    )
    doc.add_paragraph(
        "File: producer/producer.py. build_producer() constructs a KafkaProducer with acks=\"all\", "
        "retries=5, linger_ms=50, a value_serializer that UTF-8-encodes either a JSON string (already-"
        "corrupted truncated payloads) or a dict via json.dumps, and a key_serializer encoding device_id. "
        "run() drives the main loop: on each tick it toggles offline state for every device, picks a "
        "random online device, decides whether this event is late (probability LATE_EVENT_RATE, default "
        "0.06, via random_late_timestamp() which subtracts 5..MAX_LATE_SECONDS seconds from now), builds "
        "the event via device.build_event(event_time), decides whether to corrupt it (probability "
        "MALFORMED_RATE, default 0.03), sends it keyed by device_id, and with probability DUPLICATE_RATE "
        "(default 0.02) sends the identical payload again to simulate at-least-once redelivery. The loop "
        "sleeps 1/EVENTS_PER_SECOND seconds per iteration and logs a running total every 200 sends."
    )
    doc.add_paragraph(
        "File: producer/config.py. ProducerConfig is a frozen dataclass whose every field defaults from "
        "an environment variable (loaded via python-dotenv's load_dotenv()), including "
        "bootstrap_servers, topic, num_devices, events_per_second, malformed_rate, late_event_rate, "
        "duplicate_rate, offline_flip_probability, max_offline_ticks, max_late_seconds, run_seconds "
        "(0 = run until Ctrl+C), and an optional random_seed for reproducible runs."
    )

    doc.add_heading2("2.2 Kafka topic bootstrap")
    doc.add_paragraph(
        "File: scripts/create_topics.py. wait_for_broker() retries KafkaAdminClient construction up to "
        "15 times with a 2-second delay, since the broker container's healthcheck may report ready "
        "slightly before the admin API accepts connections. main() then creates a NewTopic named "
        "raw.iot-telemetry (overridable via --topic or KAFKA_TOPIC) with --partitions default 3, "
        "--replication-factor default 1, and a retention.ms topic config default of 7 * 24 * 60 * 60 * "
        "1000 (7 days), swallowing TopicAlreadyExistsError as a no-op re-run case."
    )

    doc.add_heading2("2.3 Kafka source, parsing, and validation")
    doc.add_paragraph(
        "File: spark_job/schemas.py. get_event_schema() returns a Spark StructType with fields "
        "device_id, timestamp, metric_type, value, site_id, firmware_version, and _corrupt_record "
        "(CORRUPT_RECORD_COLUMN), every one declared as StringType -- deliberately, including value and "
        "timestamp which are logically numeric/temporal, because from_json's PERMISSIVE mode fails an "
        "entire record if a single field cannot coerce to its declared type, which would make a "
        "non-numeric value indistinguishable from truncated garbage. ALLOWED_METRIC_TYPES is the tuple "
        "(\"temperature\", \"humidity\", \"battery\", \"signal_strength\"). METRIC_VALUE_RANGES gives the "
        "inclusive plausible physical range per metric_type used for out-of-range detection: temperature "
        "(-40.0, 85.0), humidity (0.0, 100.0), battery (0.0, 100.0), signal_strength (-130.0, -20.0) dBm."
    )
    doc.add_paragraph(
        "File: spark_job/transformations.py, function parse_kafka_value(raw_df, key_col=\"key\", "
        "value_col=\"value\"). Casts the Kafka value column to string, applies from_json with the schema "
        "above in PERMISSIVE mode reporting corruption via _corrupt_record, and selects out device_key "
        "(from the Kafka message key), raw_value (the original string, preserved for the dead-letter "
        "sink), device_id, raw_timestamp, metric_type, value (explicitly .cast(\"double\") -- returns null "
        "on failure instead of aborting the row), site_id, firmware_version, and corrupt_record. It then "
        "derives event_time via F.coalesce over F.to_timestamp attempts against TIMESTAMP_FORMATS = "
        "[\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"yyyy-MM-dd'T'HH:mm:ss'Z'\"], the first non-null match winning."
    )
    doc.add_paragraph(
        "Function classify_events(parsed_df) adds is_valid and error_reason columns using a "
        "F.when/.otherwise chain evaluated in this fixed priority order (first match wins): "
        "corrupt_record is not null -> \"invalid_json\"; device_id is null -> \"missing_device_id\"; "
        "raw_timestamp is null or event_time is null -> \"invalid_timestamp\"; metric_type not in "
        "ALLOWED_METRIC_TYPES -> \"invalid_metric_type\"; value is null -> \"missing_or_non_numeric_value\"; "
        "value outside its metric_type's range (via the private _value_range_condition() helper) -> "
        "\"value_out_of_range\"; otherwise null (valid). is_valid is simply error_reason.isNull()."
    )
    doc.add_paragraph(
        "Function split_valid_and_malformed(classified_df) returns (valid_df, dead_letter_df). valid_df "
        "keeps exactly [device_id, event_time, metric_type, value, site_id, firmware_version] for rows "
        "where is_valid is true. dead_letter_df keeps [raw_value, device_id, error_reason, "
        "F.current_timestamp().alias(\"quarantined_at\")] for rows where is_valid is false."
    )

    doc.add_heading2("2.4 Windowed aggregation and watermarking")
    doc.add_paragraph(
        "Function compute_windowed_aggregates(valid_df, window_duration=\"1 minute\", "
        "watermark_delay=\"2 minutes\", slide_duration=None) in spark_job/transformations.py. Applies "
        "valid_df.withWatermark(\"event_time\", watermark_delay), builds a window column via "
        "F.window(F.col(\"event_time\"), window_duration) for a tumbling window, or "
        "F.window(F.col(\"event_time\"), window_duration, slide_duration) for a sliding window when "
        "slide_duration is supplied (not used by the live job, but exercised by "
        "tests/test_aggregation.py::test_sliding_window_produces_overlapping_buckets). Groups by "
        "(window_col, device_id, metric_type) and aggregates F.avg(\"value\") as avg_value, "
        "F.count(F.lit(1)) as event_count, F.min(\"value\") as min_value, F.max(\"value\") as max_value, "
        "then projects window.start/window.end to window_start/window_end and rounds avg_value to 3 "
        "decimal places (F.round(\"avg_value\", 3))."
    )
    doc.add_paragraph(
        "Live-job parameter values (from .env.example / streaming_job.py env lookups): "
        "WINDOW_DURATION=\"1 minute\", WATERMARK_DELAY=\"2 minutes\", TRIGGER_INTERVAL=\"30 seconds\", "
        "STARTING_OFFSETS=\"latest\". These are read via a local _env(name, default) helper in "
        "spark_job/streaming_job.py, so the same job binary needs no code change to run with different "
        "window/watermark parameters."
    )
    doc.add_paragraph(
        "Function build_bronze_and_aggregates(raw_df, window_duration, watermark_delay) is a convenience "
        "wrapper chaining parse_kafka_value -> classify_events -> split_valid_and_malformed -> "
        "compute_windowed_aggregates in one call, returning a dict with keys \"valid\", \"dead_letter\", "
        "\"aggregates\"; used by integration-style tests that want the full chain without repeating the "
        "wiring. get_or_create_spark(app_name=\"iot-telemetry-pipeline\") builds/returns the SparkSession "
        "with spark.sql.session.timeZone pinned to UTC."
    )

    doc.add_heading2("2.5 Streaming job wiring (Kafka source + two Parquet sinks)")
    doc.add_paragraph(
        "File: spark_job/streaming_job.py, function main(). Reads KAFKA_BOOTSTRAP_SERVERS "
        "(default localhost:29092), KAFKA_TOPIC (default raw.iot-telemetry), OUTPUT_PATH (default "
        "./data/output/aggregates), DEAD_LETTER_PATH (default ./data/output/dead_letter), CHECKPOINT_PATH "
        "(default ./data/checkpoints), WINDOW_DURATION, WATERMARK_DELAY, TRIGGER_INTERVAL, and "
        "STARTING_OFFSETS from the environment. Builds a readStream against format(\"kafka\") with "
        ".option(\"subscribe\", topic), .option(\"startingOffsets\", starting_offsets), and "
        ".option(\"failOnDataLoss\", \"false\"). Pipes the result through parse_kafka_value -> "
        "classify_events -> split_valid_and_malformed -> compute_windowed_aggregates, then starts two "
        "independent writeStream queries: aggregates_query writes format(\"parquet\") to OUTPUT_PATH with "
        "checkpointLocation {CHECKPOINT_PATH}/aggregates, outputMode(\"append\"), "
        "trigger(processingTime=TRIGGER_INTERVAL); dead_letter_query writes to DEAD_LETTER_PATH with "
        "checkpointLocation {CHECKPOINT_PATH}/dead_letter, the same outputMode and trigger. The process "
        "blocks on spark.streams.awaitAnyTermination()."
    )
    doc.add_paragraph(
        "Neither query sets kafka.group.id explicitly, so Spark's Kafka source auto-assigns a unique "
        "internal consumer group id per query (Spark's own generated prefix, not a stable named group). "
        "This means the two queries are two independent consumer groups reading the same topic "
        "concurrently, each maintaining its own committed-offset state via its own checkpoint directory "
        "-- this is what the module docstring in streaming_job.py refers to when explaining why the job "
        "is two queries rather than one foreachBatch doing both jobs (the aggregates query needs "
        "watermark-gated append-mode output; the dead-letter query needs immediate, undelayed writes)."
    )

    doc.add_heading2("2.6 Output verification utility")
    doc.add_paragraph(
        "File: scripts/inspect_output.py. Connects an in-memory DuckDB database and runs "
        "SELECT ... FROM parquet_scan('<path>/**/*.parquet') directly against OUTPUT_PATH or "
        "DEAD_LETTER_PATH. show_aggregates() prints the most recent N windows (default 20, --top) "
        "ordered by window_end descending, plus per-metric_type totals (window count, summed "
        "event_count, overall average). show_dead_letter() prints a breakdown of record counts by "
        "error_reason, plus the most recent N quarantined records. Supports --watch to re-run every "
        "--interval seconds (default 10s). On Windows, stdout/stderr are reconfigured to UTF-8 before "
        "printing, since DuckDB's .show() uses Unicode box-drawing characters that a legacy cp1252 "
        "console cannot encode."
    )

    # 3. Message schema and topic/job configuration reference
    doc.add_heading1("3. Message schema and Kafka topic / Spark job configuration reference")
    doc.add_heading2("3.1 Input telemetry event schema")
    doc.add_paragraph("Defined in schemas/telemetry.schema.json (JSON Schema draft-07) and mirrored as a Spark StructType in spark_job/schemas.py:")
    doc.add_table(
        headers=["Field", "Type", "Description"],
        rows=[
            ["device_id", "string", "Required. Pattern ^dev-[0-9a-fA-F]{8}$."],
            ["timestamp", "string (date-time)", "Required. ISO-8601 UTC event time, e.g. "
             "2026-07-29T10:15:30.123Z; the time the reading was taken on-device, not produce time."],
            ["metric_type", "string (enum)", "Required. One of temperature, humidity, battery, "
             "signal_strength."],
            ["value", "number", "Required. Range validity enforced downstream in Spark "
             "(METRIC_VALUE_RANGES), not in the JSON Schema itself."],
            ["site_id", "string", "Optional. Logical grouping (gateway/site) a device belongs to."],
            ["firmware_version", "string", "Optional. Device firmware version string."],
        ],
    )
    doc.add_heading2("3.2 Output aggregate schema")
    doc.add_table(
        headers=["Field", "Type", "Description"],
        rows=[
            ["window_start", "timestamp", "Tumbling window start (1-minute buckets by default)."],
            ["window_end", "timestamp", "Tumbling window end."],
            ["device_id", "string", "Grouping key."],
            ["metric_type", "string", "Grouping key."],
            ["avg_value", "double", "Rounded to 3 decimals."],
            ["event_count", "long", "Count of valid events in this window/device/metric bucket."],
            ["min_value", "double", "Minimum observed value."],
            ["max_value", "double", "Maximum observed value."],
        ],
    )
    doc.add_heading2("3.3 Kafka topic configuration")
    doc.add_table(
        headers=["Setting", "Value", "Source"],
        rows=[
            ["Topic name", "raw.iot-telemetry", "KAFKA_TOPIC env var; scripts/create_topics.py default"],
            ["Partitions", "3 (default)", "scripts/create_topics.py --partitions argument"],
            ["Replication factor", "1", "scripts/create_topics.py --replication-factor argument "
             "(single-broker demo topology)"],
            ["Retention", "7 days (604,800,000 ms)", "scripts/create_topics.py --retention-ms argument"],
            ["Broker auto-create-topics", "true (demo convenience only)", "docker-compose.yml "
             "KAFKA_AUTO_CREATE_TOPICS_ENABLE"],
            ["Security protocol", "PLAINTEXT on all listeners", "docker-compose.yml "
             "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"],
            ["Broker mode", "KRaft, combined broker+controller, single node", "docker-compose.yml "
             "KAFKA_PROCESS_ROLES=broker,controller"],
        ],
    )
    doc.add_heading2("3.4 Spark job / consumer configuration")
    doc.add_table(
        headers=["Setting", "Value", "Source"],
        rows=[
            ["Kafka bootstrap servers", "localhost:29092 (default)", "KAFKA_BOOTSTRAP_SERVERS env var"],
            ["startingOffsets", "latest (default)", "STARTING_OFFSETS env var"],
            ["failOnDataLoss", "false", "hardcoded in streaming_job.py readStream options"],
            ["Consumer group id (aggregates query)", "not explicitly set -- Spark auto-assigns a unique "
             "internal group id", "spark_job/streaming_job.py (no kafka.group.id option)"],
            ["Consumer group id (dead-letter query)", "not explicitly set -- separate auto-assigned "
             "group id, independent from the aggregates query", "spark_job/streaming_job.py"],
            ["Window duration", "1 minute (default)", "WINDOW_DURATION env var"],
            ["Watermark delay", "2 minutes (default)", "WATERMARK_DELAY env var"],
            ["Trigger interval (both queries)", "30 seconds (default)", "TRIGGER_INTERVAL env var"],
            ["Output mode (both queries)", "append", "hardcoded in streaming_job.py .outputMode(\"append\")"],
            ["Kafka connector package", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "resolved via "
             "--packages / PYSPARK_SUBMIT_ARGS at job launch"],
        ],
    )

    # 4. Sequence flows
    doc.add_heading1("4. Sequence flows / process flows")

    doc.add_heading2("4.1 Normal telemetry event: producer to aggregated output")
    doc.add_table(
        headers=["Step", "Actor / Component", "Action"],
        rows=[
            ["1", "DeviceState (device_simulator.py)", "_walk() advances the device's metric value via "
             "bounded random walk; build_event() assembles the JSON-ready dict."],
            ["2", "producer.py:run()", "Determines the event is neither late nor malformed (probability "
             "checks fail); sends the JSON-serialized event to raw.iot-telemetry keyed by device_id."],
            ["3", "Kafka broker", "Appends the message to the topic partition selected by the device_id "
             "key's hash."],
            ["4", "streaming_job.py readStream", "The aggregates query's micro-batch reads the new "
             "record (subject to the 30-second trigger interval)."],
            ["5", "parse_kafka_value()", "Parses the JSON value with every field as string, casts value "
             "to double and timestamp to event_time -- both succeed since the record is well-formed."],
            ["6", "classify_events()", "All F.when() checks fall through; error_reason is null, "
             "is_valid is true."],
            ["7", "split_valid_and_malformed()", "Row is selected into valid_df with columns "
             "[device_id, event_time, metric_type, value, site_id, firmware_version]."],
            ["8", "compute_windowed_aggregates()", "Row is bucketed into its 1-minute tumbling window by "
             "event_time and grouped by (device_id, metric_type); contributes to avg/count/min/max."],
            ["9", "aggregates_query writeStream", "Once the watermark has advanced past the window's end "
             "(2 minutes past the max event_time seen), the window is finalized and written as a Parquet "
             "row under data/output/aggregates."],
            ["10", "scripts/inspect_output.py", "A DuckDB SELECT over parquet_scan(...) surfaces the new "
             "row for manual verification."],
        ],
    )

    doc.add_heading2("4.2 Malformed event: producer to dead-letter sink")
    doc.add_table(
        headers=["Step", "Actor / Component", "Action"],
        rows=[
            ["1", "producer.py:run()", "MALFORMED_RATE probability check succeeds; corrupt_event() is "
             "called with a randomly chosen kind from MALFORMED_KINDS."],
            ["2", "device_simulator.py:corrupt_event()", "Produces either a semantically-bad-but-valid-"
             "JSON dict (missing field / bad enum / non-numeric value / bad timestamp) or a truncated, "
             "unparseable JSON string."],
            ["3", "producer.py:_serialize()", "Serializes the corrupted payload as-is (raw string passed "
             "through directly if it is already a string, i.e. the truncated_json case)."],
            ["4", "Kafka broker", "Appends the malformed message to raw.iot-telemetry exactly as any "
             "other message -- Kafka itself has no awareness of payload validity."],
            ["5", "parse_kafka_value()", "from_json in PERMISSIVE mode either parses the JSON into "
             "fields with one or more nulls (semantically bad case) or fails entirely and populates "
             "corrupt_record (truncated JSON case)."],
            ["6", "classify_events()", "Evaluates the fixed-priority F.when() chain and assigns the "
             "first matching error_reason: invalid_json, missing_device_id, invalid_timestamp, "
             "invalid_metric_type, missing_or_non_numeric_value, or value_out_of_range."],
            ["7", "split_valid_and_malformed()", "Row is selected into dead_letter_df with columns "
             "[raw_value, device_id, error_reason, quarantined_at]."],
            ["8", "dead_letter_query writeStream", "Written immediately on the next 30-second trigger, "
             "with no windowing or watermark delay, to data/output/dead_letter."],
            ["9", "scripts/inspect_output.py --dead-letter", "A DuckDB SELECT groups by error_reason to "
             "show the breakdown, and lists the most recent quarantined records for debugging."],
        ],
    )

    doc.add_heading2("4.3 Late event within the watermark")
    doc.add_table(
        headers=["Step", "Actor / Component", "Action"],
        rows=[
            ["1", "producer.py:run()", "LATE_EVENT_RATE probability check succeeds; "
             "random_late_timestamp() sets event_time to now minus 5..MAX_LATE_SECONDS (default up to "
             "240) seconds."],
            ["2", "Kafka broker / Spark readStream", "The event is ingested normally; nothing in Kafka "
             "or the initial parse distinguishes a late event from an on-time one."],
            ["3", "compute_windowed_aggregates()", "event_time (not arrival/processing time) determines "
             "which window the event is bucketed into via F.window(); since the delay is within "
             "WATERMARK_DELAY (2 minutes) of the max event_time seen so far, the event's window has not "
             "yet been finalized and the event is correctly included in that window's aggregate."],
            ["4", "aggregates_query writeStream", "The window is eventually emitted once the watermark "
             "passes its end, now correctly reflecting the late-arriving event's contribution."],
        ],
    )

    doc.add_heading2("4.4 Late event past the watermark (dropped)")
    doc.add_table(
        headers=["Step", "Actor / Component", "Action"],
        rows=[
            ["1", "Upstream (device or simulated late event)", "An event's event_time falls inside a "
             "window whose end is already more than WATERMARK_DELAY behind the maximum event_time the "
             "job has observed."],
            ["2", "compute_windowed_aggregates()", "withWatermark(\"event_time\", watermark_delay) has "
             "already advanced past this window's end, so Structured Streaming's engine drops the row "
             "before it reaches the aggregation state for that (now-closed) window."],
            ["3", "aggregates_query writeStream", "No new row is written and no already-emitted "
             "aggregate for that window is altered -- the late row simply does not appear anywhere in "
             "output, verified explicitly by tests/test_watermark.py::"
             "test_late_event_past_watermark_is_dropped_from_output."],
            ["4", "(no dead-letter routing)", "This is a documented gap, not an error path: a "
             "watermark-dropped event is silently discarded by Spark's engine itself, before "
             "classify_events() or split_valid_and_malformed() ever run against it, so it does not "
             "appear in the dead-letter sink either."],
        ],
    )

    # 5. Key algorithms & business logic
    doc.add_heading1("5. Key algorithms & business logic")
    doc.add_heading2("5.1 Windowing and watermarking")
    doc.add_paragraph(
        "Window: tumbling, 1 minute by default (WINDOW_DURATION), computed via "
        "F.window(F.col(\"event_time\"), window_duration) with no slide_duration passed by the live job "
        "(spark_job/transformations.py supports an optional sliding-window mode via slide_duration, "
        "exercised only in tests/test_aggregation.py::test_sliding_window_produces_overlapping_buckets, "
        "not used in production configuration). Watermark: withWatermark(\"event_time\", "
        "watermark_delay), 2 minutes by default (WATERMARK_DELAY). The watermark tracks "
        "max(event_time seen so far) - watermark_delay; any window whose end falls before the current "
        "watermark is considered closed, is emitted in outputMode(\"append\"), and stops accepting new "
        "rows. This is verified concretely (not just asserted in prose) by "
        "tests/test_watermark.py, which drives a real streaming query with trigger(availableNow=True) "
        "across three micro-batches sharing one checkpoint: batch 1 seeds two on-time readings in "
        "[10:00, 10:01); batch 2 sends a far-future reading (10:05:00) which advances the watermark to "
        "10:04:00, past the first window's end, causing it to be emitted with event_count=2, "
        "avg_value=15.0; batch 3 sends a reading back inside the now-closed window (10:00:15, value "
        "999.0) which the test asserts is dropped -- the emitted aggregate for that window is unchanged "
        "(event_count still 2, avg_value still 15.0, not polluted by 999.0)."
    )
    doc.add_heading2("5.2 Dead-letter routing criteria")
    doc.add_paragraph(
        "Implemented as a single ordered F.when()/.otherwise() chain in classify_events() -- ordering "
        "matters because a record failing multiple checks is reported under the earliest, most "
        "fundamental reason, so the dead-letter sink groups records sensibly rather than by whichever "
        "check happened to run last. Order: (1) corrupt_record is not null -> invalid_json "
        "(from_json PERMISSIVE-mode failure, e.g. truncated JSON); (2) device_id is null -> "
        "missing_device_id; (3) raw_timestamp is null or event_time is null -> invalid_timestamp "
        "(covers both a missing timestamp field and one that failed to parse against either accepted "
        "format); (4) metric_type not in ALLOWED_METRIC_TYPES -> invalid_metric_type; (5) value is null "
        "-> missing_or_non_numeric_value (covers both a missing value field and a non-numeric string "
        "that failed the double cast); (6) value outside its metric_type's plausible range "
        "(_value_range_condition()) -> value_out_of_range; otherwise the record is valid."
    )
    doc.add_heading2("5.3 Physically-plausible value ranges")
    doc.add_paragraph(
        "METRIC_VALUE_RANGES in spark_job/schemas.py: temperature (-40.0, 85.0) modeling an industrial "
        "sensor operating range, humidity (0.0, 100.0), battery (0.0, 100.0), signal_strength (-130.0, "
        "-20.0) dBm. _value_range_condition() in transformations.py builds a nested F.when chain over "
        "this dict, defaulting to False for any metric_type not covered (which in practice never "
        "triggers for known-valid metric_types, since invalid_metric_type is checked earlier in the "
        "priority chain and short-circuits first)."
    )

    # 6. Validation & error handling
    doc.add_heading1("6. Validation & error handling")
    doc.add_paragraph(
        "Malformed JSON / partial payloads: handled by parsing every declared field as a string in "
        "get_event_schema() and then explicitly casting value to double and timestamp to Spark's "
        "timestamp type in parse_kafka_value(), rather than declaring value as DoubleType directly in "
        "the from_json schema. This distinction exists because from_json in PERMISSIVE mode fails the "
        "entire record (populating _corrupt_record) if any single declared-typed field fails to coerce -- "
        "which would make a non-numeric value indistinguishable from genuinely truncated JSON. Parsing "
        "as string first and casting afterward means a SQL cast failure just returns null for that one "
        "column, preserving the ability to report missing_or_non_numeric_value as a reason distinct from "
        "invalid_json. The README notes this was caught by a failing unit test during development "
        "(tests/test_parsing.py::test_non_numeric_value_is_quarantined), not discovered by code "
        "inspection."
    )
    doc.add_paragraph(
        "Late data beyond the watermark: dropped silently by Spark's Structured Streaming engine at the "
        "aggregation stage, before classify_events() runs -- this is a known, deliberate simplification, "
        "not a bug: a watermark-dropped row is not routed to the dead-letter sink, because the watermark "
        "eviction happens inside compute_windowed_aggregates() on the already-valid stream, after "
        "split_valid_and_malformed() has already separated valid from invalid records. A production "
        "version wanting visibility into watermark-dropped events would need additional instrumentation "
        "(e.g. a StreamingQueryListener tracking numLateInputs, or a parallel unwindowed audit stream) "
        "not implemented here."
    )
    doc.add_paragraph(
        "Duplicate delivery: the producer simulates at-least-once redelivery via DUPLICATE_RATE (default "
        "0.02), and this pipeline does not deduplicate anywhere -- neither at the Kafka producer level "
        "(no idempotent producer config) nor in the Spark aggregation (no dropDuplicatesWithinWatermark "
        "or similar). Duplicates simply inflate event_count and skew avg_value in whatever window they "
        "land in. This is documented as a known, explicit gap in the README's Production considerations "
        "section, not an oversight."
    )
    doc.add_paragraph(
        "Offline devices: handled implicitly rather than as a special case -- a device that stops "
        "sending simply contributes no rows to whatever windows fall during its offline period, since "
        "windows and groups are keyed per (device_id, metric_type); no explicit \"device went quiet\" "
        "detection or alerting exists in this repository."
    )
    doc.add_paragraph(
        "Dead-letter follow-up: quarantined records land in data/output/dead_letter/ and nothing further "
        "happens to them automatically -- there is no alerting on dead-letter volume/rate and no replay "
        "mechanism once a root cause (e.g. a firmware regression) is identified. This is called out "
        "explicitly as future work in both the README and Section 12 of the companion HLD."
    )
    doc.add_paragraph(
        "Broker data loss: the Kafka source is configured with failOnDataLoss=false, meaning the "
        "streaming queries will not fail outright if Spark detects that some offsets/partitions "
        "appear to have been lost (e.g. due to retention or an unclean topic recreation) -- appropriate "
        "for a single-broker demo where this can happen benignly, but a production deployment with "
        "properly replicated partitions would typically want this left at its default (true) so genuine "
        "data loss is surfaced rather than silently tolerated."
    )

    # 7. Non-functional implementation details
    doc.add_heading1("7. Non-functional implementation details")
    doc.add_heading2("7.1 Security implementation specifics")
    doc.add_paragraph(
        "docker-compose.yml configures KAFKA_LISTENER_SECURITY_PROTOCOL_MAP as "
        "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT -- every listener, including "
        "the inter-broker and controller listeners, is PLAINTEXT. No SASL mechanism, no TLS keystore/"
        "truststore, and no ACL authorizer are configured anywhere in the compose file or the Spark job's "
        "Kafka options. Kafka UI connects to the broker with no authentication "
        "(KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS pointed at the plain kafka:9092 listener). This is acceptable "
        "only because the entire stack is intended to run on localhost for development/demonstration."
    )
    doc.add_heading2("7.2 Performance and scaling considerations")
    doc.add_paragraph(
        "The one verified local run (documented in the README) used 15 simulated devices at 30 events/"
        "sec against a single-broker Kafka cluster and a single-JVM local-mode Spark driver, producing "
        "138 aggregate rows and 263 dead-letter rows over several minutes. Local micro-batches were "
        "observed taking roughly 20-45 seconds against a configured 30-second trigger interval, visible "
        "as ProcessingTimeExecutor \"Current batch is falling behind\" warnings in the job log -- "
        "attributed in the README to local-mode/shuffle-partition-count overhead on a laptop rather than "
        "a correctness problem, since it only delays when output appears, not whether it is correct. No "
        "throughput benchmark beyond this single run's observed counts is claimed anywhere in this "
        "repository. Horizontal scaling (multiple Kafka partitions consumed concurrently, multiple Spark "
        "executors) was not exercised in that run; the topic is created with 3 partitions "
        "(scripts/create_topics.py), which would allow up to 3 concurrent consumers per consumer group "
        "in a scaled-out deployment, but the demo job runs as a single local-mode process."
    )

    # 8. Appendix
    doc.add_heading1("8. Appendix")
    doc.add_heading2("8.1 Repository module / file map")
    doc.add_code_block(
        "docker-compose.yml          # Kafka (KRaft, single broker) + Kafka UI\n"
        ".env.example                 # all tunables; copy to .env (gitignored)\n"
        "schemas/\n"
        "  telemetry.schema.json       # JSON Schema contract for the wire format\n"
        "\n"
        "producer/\n"
        "  config.py                    # env-var-driven ProducerConfig\n"
        "  device_simulator.py           # pure-Python fleet simulation (no Kafka dep)\n"
        "  producer.py                    # Kafka producer entrypoint (python -m producer.producer)\n"
        "\n"
        "spark_job/\n"
        "  schemas.py                     # Spark StructType + validation constants\n"
        "  transformations.py              # parse/classify/split/window -- pure DataFrame functions\n"
        "  streaming_job.py                 # wires transformations.py to Kafka source + Parquet sinks\n"
        "\n"
        "scripts/\n"
        "  create_topics.py                 # explicit topic creation (partitions/retention)\n"
        "  inspect_output.py                 # DuckDB queries over the Parquet output\n"
        "\n"
        "tests/\n"
        "  conftest.py                       # shared local SparkSession fixture, forces UTC\n"
        "  test_parsing.py                    # dead-letter reason coverage (8 tests)\n"
        "  test_aggregation.py                 # windowing/grouping math (5 tests)\n"
        "  test_watermark.py                    # real streaming query, late-data eviction (1 test)\n"
        "\n"
        ".github/workflows/ci.yaml              # lint (ruff+black) / pytest / docker-compose validate"
    )
    doc.add_heading2("8.2 Environment variable / configuration reference")
    doc.add_table(
        headers=["Variable", "Default", "Consumed by"],
        rows=[
            ["KAFKA_BOOTSTRAP_SERVERS", "localhost:29092", "producer/config.py, spark_job/streaming_job.py, "
             "scripts/create_topics.py"],
            ["KAFKA_TOPIC", "raw.iot-telemetry", "producer/config.py, spark_job/streaming_job.py, "
             "scripts/create_topics.py"],
            ["NUM_DEVICES", "25", "producer/config.py"],
            ["EVENTS_PER_SECOND", "10", "producer/config.py"],
            ["MALFORMED_RATE", "0.03", "producer/config.py"],
            ["LATE_EVENT_RATE", "0.06", "producer/config.py"],
            ["DUPLICATE_RATE", "0.02", "producer/config.py"],
            ["OFFLINE_FLIP_PROBABILITY", "0.002", "producer/config.py"],
            ["MAX_OFFLINE_TICKS", "40", "producer/config.py"],
            ["MAX_LATE_SECONDS", "240", "producer/config.py"],
            ["RUN_SECONDS", "0 (run until Ctrl+C)", "producer/config.py"],
            ["RANDOM_SEED", "unset (nondeterministic)", "producer/config.py"],
            ["OUTPUT_PATH", "./data/output/aggregates", "spark_job/streaming_job.py, scripts/inspect_output.py"],
            ["DEAD_LETTER_PATH", "./data/output/dead_letter", "spark_job/streaming_job.py, "
             "scripts/inspect_output.py"],
            ["CHECKPOINT_PATH", "./data/checkpoints", "spark_job/streaming_job.py"],
            ["WINDOW_DURATION", "1 minute", "spark_job/streaming_job.py"],
            ["WATERMARK_DELAY", "2 minutes", "spark_job/streaming_job.py"],
            ["TRIGGER_INTERVAL", "30 seconds", "spark_job/streaming_job.py"],
            ["STARTING_OFFSETS", "latest", "spark_job/streaming_job.py"],
        ],
    )
    doc.add_heading2("8.3 Change history")
    doc.add_table(
        headers=["Version", "Date", "Description"],
        rows=[["1.0", DATE, "Initial low-level design document"]],
    )

    return doc


if __name__ == "__main__":
    doc = build()
    doc.save("Darviq_Kafka_Low_Level_Design.docx")
    print("Saved Darviq_Kafka_Low_Level_Design.docx")
