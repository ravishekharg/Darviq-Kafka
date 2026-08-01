"""Generates Darviq_Kafka_High_Level_Design.docx from docx_builder.DesignDoc.

Run from the Docs/ directory (or anywhere, as long as docx_builder.py is
importable):

    python generate_hld.py
"""
from __future__ import annotations

from docx_builder import DesignDoc

VERSION = "1.0"
DATE = "July 31, 2026"


def build() -> DesignDoc:
    doc = DesignDoc(
        project_name="Darviq Kafka",
        subtitle="Streaming IoT Telemetry Pipeline -- Kafka (KRaft) + PySpark Structured Streaming",
        doc_kind="High-Level Design (HLD)",
        version=VERSION,
        date=DATE,
    )
    doc.add_document_control()
    doc.add_toc_field()

    # 1. Introduction
    doc.add_heading1("1. Introduction")
    doc.add_heading2("1.1 Purpose")
    doc.add_paragraph(
        "This document describes the high-level design of Darviq Kafka, a reference streaming data "
        "pipeline that ingests synthetic IoT device telemetry through Apache Kafka and processes it with "
        "PySpark Structured Streaming to produce time-windowed aggregates, while routing malformed or "
        "invalid records to a dead-letter sink. It exists to give reviewers and future maintainers a "
        "single place to understand the pipeline's architecture, data flow, and design rationale without "
        "reading the source code line by line."
    )
    doc.add_heading2("1.2 Scope")
    doc.add_paragraph("In scope for this document:")
    doc.add_bullets(
        [
            "The synthetic device producer (producer/) and the events it emits, including deliberately "
            "injected data-quality problems (malformed payloads, late/out-of-order timestamps, duplicates, "
            "offline devices).",
            "Kafka topic configuration and the single-broker KRaft-mode deployment used for local "
            "development and demonstration (docker-compose.yml).",
            "The PySpark Structured Streaming job (spark_job/) that parses, validates, windows, and "
            "aggregates telemetry, and routes invalid records to a dead-letter Parquet sink.",
            "The Parquet-based output sinks and the DuckDB-based inspection utility used to verify "
            "pipeline output.",
            "Non-functional characteristics that are honestly attributable to this specific repository "
            "(semantics, watermark tolerance, and known gaps), as opposed to a production claim.",
        ]
    )
    doc.add_paragraph("Out of scope for this document:")
    doc.add_bullets(
        [
            "Multi-broker Kafka clusters, replication, or rack/AZ-aware deployment -- this repository "
            "runs a single KRaft broker with replication_factor=1 throughout, by design, as a laptop-"
            "sized reference architecture.",
            "Schema Registry / Avro or Protobuf wire formats -- the wire format here is plain JSON "
            "validated against a JSON Schema document, not a binary schema-governed format.",
            "Production security hardening (SASL/mTLS, ACLs) -- the local broker is PLAINTEXT with no "
            "authentication, which is acceptable for localhost use only.",
            "Any downstream dashboard, alerting system, or database consuming the aggregate output -- "
            "none exists in this repository; output is Parquet files inspected via SQL.",
        ]
    )
    doc.add_heading2("1.3 Intended audience")
    doc.add_bullets(
        [
            "Engineers evaluating or extending this repository as a reference for Kafka + Structured "
            "Streaming pipelines.",
            "Reviewers assessing the design decisions and honesty of documented limitations as part of "
            "a portfolio review.",
            "Future maintainers who need to understand the module boundaries before making a change.",
        ]
    )
    doc.add_heading2("1.4 Definitions & abbreviations")
    doc.add_table(
        headers=["Term", "Definition"],
        rows=[
            ["KRaft", "Kafka Raft metadata mode -- Kafka's own consensus protocol, replacing ZooKeeper "
             "for cluster metadata; used here for the single broker (no ZooKeeper container required)."],
            ["Structured Streaming", "Spark's high-level streaming API that expresses a stream as an "
             "unbounded table and lets the same DataFrame transformations run against batch or streaming "
             "input."],
            ["Watermark", "A Structured Streaming mechanism that tracks how far behind the maximum "
             "observed event time late data is tolerated, so state for closed windows can be safely "
             "evicted."],
            ["Tumbling window", "A fixed-size, non-overlapping time window (e.g. 1 minute) used here to "
             "bucket events by event_time for aggregation."],
            ["Dead-letter sink", "A separate output location (here, a Parquet directory) that receives "
             "records which fail validation, together with the reason, instead of being silently dropped."],
            ["Event time", "The timestamp a device recorded when the reading was taken, as opposed to "
             "processing time (when Spark actually handles the record) -- the two can differ for "
             "buffered/offline/late devices."],
            ["Checkpoint", "The directory Structured Streaming uses to persist offsets and aggregation "
             "state so a query can resume correctly after a restart."],
            ["PERMISSIVE mode", "A from_json parsing mode that attempts to parse a JSON string against a "
             "declared schema and reports failures via a corrupt-record column rather than throwing."],
            ["At-least-once delivery", "A delivery guarantee under which a message may be delivered more "
             "than once but never lost; simulated here by the producer's duplicate-send rate."],
        ],
    )

    # 2. System overview
    doc.add_heading1("2. System overview")
    doc.add_heading2("2.1 Problem statement")
    doc.add_paragraph(
        "IoT device telemetry (temperature, humidity, battery, signal strength) arrives continuously and "
        "unbounded, and the operationally useful questions about it are inherently time-windowed -- "
        "\"what was this device's average reading over the last minute?\" or \"has it gone quiet?\" A "
        "batch job that periodically re-scans accumulated files can answer these questions, but couples "
        "detection latency to batch cadence, makes reprocessing cost grow with retained history unless "
        "carefully bounded, and still has to invent its own ad hoc notion of \"how late is too late\" for "
        "out-of-order data. This repository's own README documents this reasoning directly and uses it "
        "as the rationale for choosing streaming over batch for this specific workload."
    )
    doc.add_heading2("2.2 Proposed solution summary")
    doc.add_paragraph(
        "A synthetic fleet of simulated IoT devices (producer/) publishes JSON telemetry events, keyed by "
        "device_id, to a single Kafka topic (raw.iot-telemetry) running on a single-broker KRaft cluster. "
        "A PySpark Structured Streaming job (spark_job/streaming_job.py) reads that topic, parses each "
        "record defensively (every field parsed as a string, then explicitly cast, so a single bad field "
        "does not fail the whole record), classifies each record as valid or invalid with a specific "
        "reason, computes 1-minute tumbling-window aggregates (avg/count/min/max) per device and metric "
        "type with a 2-minute watermark for late-data tolerance, and writes two independent output "
        "streams: aggregated results to a Parquet sink and invalid records to a separate dead-letter "
        "Parquet sink. A small DuckDB-based script (scripts/inspect_output.py) provides a concrete, "
        "runnable way to verify the pipeline's output without a dashboard."
    )

    # 3. Architecture overview
    doc.add_heading1("3. Architecture overview")
    doc.add_table(
        headers=["Component", "Responsibility", "Technology"],
        rows=[
            ["Device producer", "Simulates a fleet of IoT devices with random-walk metric values, "
             "injects malformed payloads, late/out-of-order timestamps, duplicates, and offline periods; "
             "publishes JSON events to Kafka.", "Python, kafka-python-ng"],
            ["Kafka broker", "Single-node message broker holding the raw.iot-telemetry topic; durable, "
             "ordered-per-partition event log that decouples producer and consumer.", "Apache Kafka 3.7.1, "
             "KRaft mode (no ZooKeeper), Docker"],
            ["Topic bootstrap script", "Explicitly creates the raw telemetry topic with a deliberate "
             "partition count and retention, rather than relying on broker auto-create.", "Python, "
             "kafka-python-ng admin client"],
            ["Spark Structured Streaming job", "Reads from Kafka, parses and validates records, computes "
             "windowed aggregates with watermarking, and writes both the aggregates and dead-letter "
             "streams as two independent streaming queries.", "PySpark 3.5.1, "
             "spark-sql-kafka-0-10 connector"],
            ["Aggregates sink", "Append-only Parquet output: one row per (window, device, metric_type) "
             "once its window has closed per the watermark.", "Parquet, local filesystem"],
            ["Dead-letter sink", "Append-only Parquet output holding the raw payload, device_id (when "
             "recoverable), a specific error_reason, and a quarantine timestamp for every record that "
             "fails validation.", "Parquet, local filesystem"],
            ["Output inspection utility", "Ad hoc SQL queries directly over the Parquet output "
             "directories, used as the concrete verification step instead of a dashboard.", "DuckDB"],
            ["Kafka UI", "Browser-based inspection of topics/partitions/messages during development.",
             "provectuslabs/kafka-ui (Docker)"],
        ],
    )
    doc.add_heading2("3.1 Component descriptions")
    doc.add_paragraph(
        "Device producer (producer/producer.py, device_simulator.py, config.py). Each simulated device "
        "is a small state machine that drifts its metric value with a bounded random walk, occasionally "
        "goes offline for a random number of ticks, occasionally emits a malformed payload (missing "
        "field, invalid enum, non-numeric value, truncated JSON, or bad timestamp), and occasionally "
        "emits a late/out-of-order timestamp even while online. This is deliberate: a pipeline that only "
        "ever sees clean, in-order, well-typed JSON would not exercise Structured Streaming's actual "
        "value proposition."
    )
    doc.add_paragraph(
        "Kafka broker (docker-compose.yml). A single apache/kafka:3.7.1 container running in combined "
        "broker+controller KRaft mode, exposing a container-internal listener (9092) and a host listener "
        "(29092) so the producer and Spark job, which run directly on the host machine rather than "
        "inside the compose network, can connect. Auto-create-topics is enabled for demo convenience but "
        "the intent is for the topic to be created explicitly via scripts/create_topics.py."
    )
    doc.add_paragraph(
        "Spark Structured Streaming job (spark_job/). Structured as a chain of pure DataFrame "
        "transformations (spark_job/transformations.py) that are unit-testable independent of Kafka, "
        "wired to a live Kafka source and two Parquet sinks in spark_job/streaming_job.py. Two "
        "independent streaming queries read the same topic (via two separate consumer groups) because "
        "the aggregates query needs append-mode output gated by the watermark, while the dead-letter "
        "query should write invalid records immediately with no such delay -- forcing both through one "
        "query/output mode would require compromising one or the other."
    )
    doc.add_paragraph(
        "Output sinks and inspection. Both sinks are plain Parquet directories with independent "
        "checkpoint locations under data/checkpoints/. scripts/inspect_output.py uses DuckDB's "
        "parquet_scan() to query them directly with SQL, deliberately avoiding a second Spark session or "
        "any dashboard dependency for what is meant to be a quick, concrete \"did this work\" check."
    )

    # 4. End-to-end functional workflow
    doc.add_heading1("4. End-to-end functional workflow")
    doc.add_figure_placeholder(
        "Figure 1: Device -> Kafka producer -> raw.iot-telemetry topic -> Spark parse/classify -> "
        "valid/dead-letter split -> windowed aggregation -> Parquet sinks"
    )
    doc.add_paragraph(
        "A simulated device builds an event (device_id, timestamp, metric_type, value, site_id, "
        "firmware_version) and the producer publishes it as JSON, keyed by device_id, to the "
        "raw.iot-telemetry topic. With configurable probability the producer instead sends a corrupted "
        "or late-timestamped version of the event, or sends the same event twice to simulate "
        "at-least-once redelivery."
    )
    doc.add_paragraph(
        "The Spark job's readStream subscribes to the topic (startingOffsets configurable, "
        "failOnDataLoss=false) and passes every micro-batch through parse_kafka_value(), which parses "
        "the JSON value with every field typed as a string (to avoid from_json's PERMISSIVE-mode "
        "behavior of failing an entire record over a single bad scalar field) and then explicitly casts "
        "value to double and timestamp to a Spark timestamp, so a bad value or a bad timestamp becomes a "
        "null in that column rather than aborting the row."
    )
    doc.add_paragraph(
        "classify_events() then assigns is_valid and, for invalid records, a specific error_reason, "
        "checked in a fixed order from most to least fundamental (invalid JSON, missing device_id, "
        "invalid timestamp, invalid metric type, missing/non-numeric value, out-of-range value). "
        "split_valid_and_malformed() splits the classified stream into a valid DataFrame (business "
        "columns only) and a dead-letter DataFrame (raw payload, device_id, reason, quarantine "
        "timestamp)."
    )
    doc.add_paragraph(
        "The valid stream is watermarked on event_time (2-minute delay) and grouped into 1-minute "
        "tumbling windows by (device_id, metric_type), computing avg/count/min/max per window. Both the "
        "aggregates DataFrame and the dead-letter DataFrame are written to independent Parquet sinks, "
        "each on its own 30-second micro-batch trigger and its own checkpoint directory, as two "
        "concurrently running streaming queries under one Spark driver process."
    )

    # 5. Module-wise design overview
    doc.add_heading1("5. Module-wise design overview")

    doc.add_heading2("5.1 Device producer and fleet simulation")
    doc.add_paragraph(
        "producer/device_simulator.py models each device as a DeviceState dataclass with a bounded "
        "random-walk value per metric (battery mostly drains with rare recharge events; other metrics "
        "use Gaussian jitter clamped to a physically plausible range). Devices randomly toggle offline "
        "for a configurable number of ticks. corrupt_event() implements five malformed-payload kinds "
        "(missing_required_field, invalid_metric_type, non_numeric_value, bad_timestamp, "
        "truncated_json). This module has no Kafka dependency, which is what makes it unit-testable in "
        "isolation. producer/producer.py wires this simulation to a real KafkaProducer (acks=\"all\", "
        "retries=5, linger_ms=50) and applies the late-event and duplicate-send probabilities configured "
        "in producer/config.py, which sources every tunable from environment variables via .env."
    )

    doc.add_heading2("5.2 Kafka topic setup")
    doc.add_paragraph(
        "scripts/create_topics.py explicitly creates the raw.iot-telemetry topic (default 3 partitions, "
        "replication factor 1, 7-day retention) via the Kafka admin client, rather than relying on the "
        "broker's auto-create-topics default, which is enabled in docker-compose.yml purely for local "
        "development convenience. The script waits for the broker to become reachable with a bounded "
        "retry loop before attempting topic creation, and treats TopicAlreadyExistsError as a no-op."
    )

    doc.add_heading2("5.3 Kafka source, parsing, and validation")
    doc.add_paragraph(
        "spark_job/schemas.py defines the Spark StructType used with from_json in PERMISSIVE mode, with "
        "every field (including value and timestamp) declared as StringType deliberately, plus the "
        "allowed metric_type enum and a per-metric-type physically plausible value range "
        "(METRIC_VALUE_RANGES). spark_job/transformations.py's parse_kafka_value() applies this schema "
        "to the Kafka value column, preserves the raw string as raw_value, and separately casts value to "
        "double and timestamp to Spark's timestamp type (trying two accepted ISO-8601 formats), which is "
        "what allows a non-numeric value or an unparseable timestamp to be reported as its own specific "
        "dead-letter reason instead of collapsing into a generic \"corrupt JSON\" bucket."
    )

    doc.add_heading2("5.4 Windowed aggregation and watermarking")
    doc.add_paragraph(
        "compute_windowed_aggregates() in spark_job/transformations.py applies withWatermark(\"event_time\", "
        "watermark_delay) followed by a groupBy on a tumbling (or optionally sliding, if a slide_duration "
        "is given) window over event_time plus device_id and metric_type, aggregating avg/count/min/max "
        "of value. The function is written to run identically against a static batch DataFrame (used in "
        "unit tests, where the watermark clause is accepted syntactically but has no eviction effect) and "
        "a real streaming DataFrame (used by the live job, where it bounds state size and enables "
        "outputMode(\"append\") to emit only windows that have actually closed)."
    )

    doc.add_heading2("5.5 Dead-letter routing")
    doc.add_paragraph(
        "split_valid_and_malformed() routes any record with is_valid == false to a dead-letter "
        "DataFrame carrying the original raw JSON string, the device_id when it could be recovered, the "
        "specific error_reason, and a quarantine timestamp -- so a dead-letter record is directly useful "
        "for debugging a bad device or firmware version later, not just a black hole of dropped data. "
        "The dead-letter streaming query has no windowing and writes on the same 30-second trigger as "
        "the aggregates query, but with no watermark-driven emission delay."
    )

    doc.add_heading2("5.6 Output verification")
    doc.add_paragraph(
        "scripts/inspect_output.py runs ad hoc DuckDB SQL directly over the Parquet output directories "
        "(parquet_scan over a glob) to show recent aggregate windows, per-metric-type totals, and "
        "dead-letter counts broken down by error_reason, with an optional --watch mode that re-runs on "
        "an interval. It is the tool actually used to confirm, in a real local run, that every injected "
        "failure mode was caught and routed and that aggregate output was produced correctly."
    )

    # 6. Data design
    doc.add_heading1("6. Data design")
    doc.add_paragraph(
        "The wire format is plain JSON validated against schemas/telemetry.schema.json (a standard JSON "
        "Schema document), chosen deliberately over Avro + Schema Registry for this demo since it needs "
        "no extra service and is human-readable/diffable in review, at the cost of enforcing schema "
        "compatibility only at the application layer (in Spark) rather than at write time."
    )
    doc.add_heading2("6.1 Input telemetry event schema (raw.iot-telemetry)")
    doc.add_table(
        headers=["Field", "Type", "Description"],
        rows=[
            ["device_id", "string", "Stable device identifier, pattern dev-[0-9a-fA-F]{8}. Required."],
            ["timestamp", "string (ISO-8601 UTC)", "Event time as recorded on the device, not produce "
             "time -- the two differ for offline/buffered/late devices. Required."],
            ["metric_type", "string (enum)", "One of temperature, humidity, battery, signal_strength. "
             "Required."],
            ["value", "number", "Reading value; valid range depends on metric_type and is enforced in "
             "Spark, not in the JSON Schema. Required."],
            ["site_id", "string", "Optional logical grouping (e.g. gateway/site) the device is attached to."],
            ["firmware_version", "string", "Optional device firmware version string."],
        ],
    )
    doc.add_heading2("6.2 Output aggregate schema (data/output/aggregates)")
    doc.add_table(
        headers=["Field", "Type", "Description"],
        rows=[
            ["window_start", "timestamp", "Start of the 1-minute tumbling window."],
            ["window_end", "timestamp", "End of the 1-minute tumbling window."],
            ["device_id", "string", "Device the aggregate applies to."],
            ["metric_type", "string", "Metric the aggregate applies to."],
            ["avg_value", "double", "Average of value across the window, rounded to 3 decimals."],
            ["event_count", "long", "Number of valid events contributing to this window."],
            ["min_value", "double", "Minimum value observed in this window."],
            ["max_value", "double", "Maximum value observed in this window."],
        ],
    )
    doc.add_heading2("6.3 Dead-letter record schema (data/output/dead_letter)")
    doc.add_table(
        headers=["Field", "Type", "Description"],
        rows=[
            ["raw_value", "string", "The original, unmodified Kafka message value as received."],
            ["device_id", "string (nullable)", "Recovered device_id when parseable; null when the "
             "record was too malformed to recover it."],
            ["error_reason", "string", "One of: invalid_json, missing_device_id, invalid_timestamp, "
             "invalid_metric_type, missing_or_non_numeric_value, value_out_of_range."],
            ["quarantined_at", "timestamp", "Processing-time timestamp when the record was classified "
             "invalid."],
        ],
    )

    # 7. Technology stack
    doc.add_heading1("7. Technology stack")
    doc.add_table(
        headers=["Layer", "Technology", "Notes"],
        rows=[
            ["Message broker", "Apache Kafka 3.7.1, KRaft mode", "Single broker+controller node, "
             "no ZooKeeper; replication_factor=1 throughout (demo-scale, documented as a known gap)."],
            ["Stream processing", "PySpark 3.5.1 (Structured Streaming)", "spark-sql-kafka-0-10_2.12:3.5.1 "
             "connector resolved via --packages; local[*] driver mode, no cluster manager."],
            ["Language", "Python 3.12", "Producer, Spark job, and scripts are all Python; PySpark/JVM "
             "interop needs Java 17 or 21 (developed against Java 21)."],
            ["Kafka client (producer)", "kafka-python-ng 2.2.3", "Maintained fork of kafka-python, "
             "chosen because upstream kafka-python 2.0.2 breaks on Python 3.12+."],
            ["Wire format", "JSON + JSON Schema", "schemas/telemetry.schema.json; Avro/Protobuf + "
             "Schema Registry explicitly called out as the production alternative, not implemented here."],
            ["Output sink", "Apache Parquet (local filesystem)", "Two independent sinks (aggregates, "
             "dead_letter), each with its own checkpoint directory."],
            ["Output inspection", "DuckDB", "SQL directly over Parquet directories via parquet_scan(); "
             "no dashboard or second Spark session."],
            ["Local orchestration", "Docker Compose", "Kafka broker + Kafka UI (provectuslabs/kafka-ui)."],
            ["Testing", "pytest, local Spark local[2] session", "14 tests covering parsing/validation, "
             "windowing math, and real streaming watermark eviction."],
            ["Lint/format", "ruff, black", "Enforced in CI (.github/workflows/ci.yaml)."],
        ],
    )

    # 8. Deployment architecture
    doc.add_heading1("8. Deployment architecture")
    doc.add_figure_placeholder(
        "Figure 2: Docker Compose network (kafka broker + kafka-ui) with the producer and Spark job "
        "running as separate host processes connecting via the host-mapped listener"
    )
    doc.add_paragraph(
        "The only containerized components are the Kafka broker and Kafka UI, brought up via `docker "
        "compose up -d` (docker-compose.yml). The Kafka broker exposes two listeners: a container-"
        "internal PLAINTEXT listener on 9092 (used if Spark itself ran inside the compose network, which "
        "it does not in the documented workflow) and a host-mapped PLAINTEXT_HOST listener on 29092, "
        "which is what the producer and Spark job -- both run directly on the host as ordinary Python "
        "processes -- actually connect to. Kafka UI is exposed on host port 8080 for browser-based topic "
        "inspection. Both the producer and the Spark job read all connection details and tunables from "
        "environment variables (via a local .env file, gitignored, copied from .env.example), so the "
        "same code can point at a different cluster without a code change. The Spark job itself runs in "
        "local driver mode (no YARN/Kubernetes) as `python -m spark_job.streaming_job`, requiring the "
        "Kafka connector JAR to be resolved via --packages the first time (cached under ~/.ivy2 "
        "afterward). On Windows specifically, Spark's local filesystem access for Parquet output and "
        "checkpoints additionally requires the Hadoop winutils.exe/hadoop.dll shim (not needed on Linux/"
        "macOS, including the ubuntu-latest CI runner)."
    )
    doc.add_table(
        headers=["Variable", "Default", "Purpose"],
        rows=[
            ["KAFKA_BOOTSTRAP_SERVERS", "localhost:29092", "Host-mapped Kafka listener used by both "
             "producer and Spark job."],
            ["KAFKA_TOPIC", "raw.iot-telemetry", "Topic name shared by producer, topic-creation script, "
             "and Spark job."],
            ["NUM_DEVICES", "25", "Size of the simulated device fleet."],
            ["EVENTS_PER_SECOND", "10", "Producer send rate."],
            ["MALFORMED_RATE", "0.03", "Probability an event is corrupted before sending."],
            ["LATE_EVENT_RATE", "0.06", "Probability an event carries a deliberately old timestamp."],
            ["DUPLICATE_RATE", "0.02", "Probability an event is sent twice (at-least-once simulation)."],
            ["OFFLINE_FLIP_PROBABILITY", "0.002", "Per-tick probability a device goes offline."],
            ["MAX_OFFLINE_TICKS", "40", "Upper bound on how long a device stays offline."],
            ["MAX_LATE_SECONDS", "240", "Upper bound on how far in the past a late event's timestamp is set."],
            ["RUN_SECONDS", "0", "Producer run duration; 0 means run until Ctrl+C."],
            ["OUTPUT_PATH", "./data/output/aggregates", "Aggregates Parquet sink path."],
            ["DEAD_LETTER_PATH", "./data/output/dead_letter", "Dead-letter Parquet sink path."],
            ["CHECKPOINT_PATH", "./data/checkpoints", "Root directory for both queries' checkpoints."],
            ["WINDOW_DURATION", "1 minute", "Tumbling window size for aggregation."],
            ["WATERMARK_DELAY", "2 minutes", "Late-data tolerance before a window is finalized."],
            ["TRIGGER_INTERVAL", "30 seconds", "Micro-batch trigger interval for both streaming queries."],
            ["STARTING_OFFSETS", "latest", "Kafka source starting offset policy."],
        ],
    )

    # 9. Security design
    doc.add_heading1("9. Security design")
    doc.add_paragraph(
        "The demo Kafka broker uses only the PLAINTEXT protocol on every listener, with no SASL "
        "authentication, no TLS, and no ACLs configured -- this is stated openly in the repository as "
        "acceptable for localhost development only, never for anything reachable over a real network. "
        "There are no credentials anywhere in this pipeline: environment variables carry only "
        "non-secret tunables (bootstrap servers, topic names, tunable rates), and .env is gitignored as "
        "a matter of habit for when a real cluster's SASL credentials would need to go there. Kafka UI "
        "is likewise exposed without authentication on port 8080. A production deployment of this "
        "architecture would require SASL/mTLS between producers, brokers, and the Spark job, plus "
        "per-topic ACLs restricting which principals may produce to raw.iot-telemetry or consume from "
        "it -- none of which is implemented in this repository, by design, since its purpose is "
        "demonstrating the streaming/windowing/dead-letter logic, not cluster security posture."
    )

    # 10. Non-functional requirements
    doc.add_heading1("10. Non-functional requirements")
    doc.add_table(
        headers=["Attribute", "Target / approach"],
        rows=[
            ["Delivery semantics (producer -> Kafka)", "At-least-once, by design: acks=\"all\" and "
             "retries=5 on the producer, plus a configurable duplicate-send rate that simulates upstream "
             "at-least-once redelivery."],
            ["Delivery semantics (Kafka -> Spark -> sinks)", "At-least-once / no deduplication. Duplicate "
             "events are not deduplicated anywhere in this pipeline; they currently inflate event_count "
             "and skew avg_value in whichever window they land in. This is a stated, deliberate gap, not "
             "an oversight -- see Future enhancements."],
            ["Windowing", "1-minute tumbling windows (configurable via WINDOW_DURATION), grouped by "
             "device_id and metric_type."],
            ["Late-data tolerance (watermark)", "2 minutes (configurable via WATERMARK_DELAY) past the "
             "maximum event_time seen; events arriving later than this relative to their window are "
             "dropped, verified by tests/test_watermark.py against a real streaming query."],
            ["Latency", "Micro-batch trigger every 30 seconds (TRIGGER_INTERVAL); end-to-end latency for "
             "a window's aggregate is therefore roughly window duration + watermark delay + trigger "
             "interval overhead, i.e. on the order of a few minutes for the default configuration."],
            ["Throughput", "Not benchmarked. The repository explicitly avoids quoting a throughput "
             "number it has not measured; the one verified local run used 15 devices at 30 events/sec "
             "with a single broker and local-mode Spark."],
            ["Fault tolerance / durability", "Single broker, replication_factor=1 -- a single point of "
             "failure by design for this laptop-scale reference; each streaming query has its own "
             "checkpoint directory so it can resume after a restart."],
            ["Data quality observability", "Every dead-letter record retains its raw payload, a specific "
             "error_reason, and a quarantine timestamp, so failure modes are diagnosable rather than "
             "silently dropped."],
            ["Horizontal scale", "Not exercised. The one verified local run was single-broker, "
             "single-machine, single-executor; multi-partition concurrent consumption and multi-executor "
             "Spark were not tested."],
        ],
    )

    # 11. Assumptions & constraints
    doc.add_heading1("11. Assumptions & constraints")
    doc.add_bullets(
        [
            "Runs on a single machine for local development and demonstration; it is not a claim of "
            "production/telecom-IoT scale (the README explicitly draws this distinction).",
            "Java 17 or 21 and Python 3.12 are assumed available; PySpark 3.5.1's local-mode compatibility "
            "is best on Python 3.11/3.12, and 3.13+ is not yet well supported by this pinned PySpark "
            "version.",
            "Windows execution additionally assumes the Hadoop winutils.exe/hadoop.dll shim is installed "
            "and HADOOP_HOME/PATH are set; this workaround is not needed on Linux/macOS or in CI.",
            "The Kafka broker is assumed reachable at localhost:29092 with no authentication; this is a "
            "development-only assumption, not a production one.",
            "Clock/timezone determinism for tests is assumed via explicit UTC pinning (both Spark SQL "
            "session timezone and the JVM's own default timezone), since window/watermark boundary "
            "assertions would otherwise vary by host locale.",
            "Auto-create-topics is enabled on the demo broker for convenience, but the intended operating "
            "assumption is that scripts/create_topics.py has been run first so partition count and "
            "retention are deliberate rather than accidental broker defaults.",
        ]
    )

    # 12. Future enhancements
    doc.add_heading1("12. Future enhancements")
    doc.add_paragraph(
        "The repository's own README documents a specific, honest list of what would change for a "
        "production version of this architecture; it is reproduced here as the future-enhancement list "
        "for this design document rather than invented separately:"
    )
    doc.add_bullets(
        [
            "Multi-broker Kafka with real replication -- 3+ brokers, replication_factor >= 3, tuned "
            "min.insync.replicas, and rack/AZ awareness, replacing the current single-broker topology.",
            "Schema Registry + Avro (or Protobuf) -- compatibility enforcement at write time and compact "
            "binary encoding at scale, replacing the current application-layer JSON Schema validation.",
            "Spark on YARN or Kubernetes (via the Spark Operator) instead of local mode, with multiple "
            "executors, driver HA, and dynamic allocation; the existing transformation code in "
            "spark_job/transformations.py would run unchanged since it never assumes local mode.",
            "Deduplication -- either an idempotent producer (enable.idempotence=true plus transactional "
            "writes) or dedup-by-key aggregation logic (e.g. dropDuplicatesWithinWatermark on an event ID "
            "field), addressing the currently undeduplicated duplicate-send simulation.",
            "Security hardening -- SASL/mTLS between producers, brokers, and Spark, plus per-topic ACLs, "
            "replacing the current PLAINTEXT-only, no-auth local broker.",
            "Monitoring/alerting -- exporting Spark's StreamingQueryListener metrics and Kafka JMX "
            "metrics to Prometheus/Grafana, with alerting on consumer lag and processing-time trend, "
            "beyond the current Kafka UI and streaming-query progress logs.",
            "Dead-letter follow-up -- alerting on dead-letter volume/rate as a firmware-regression signal, "
            "plus a replay path for quarantined records once a root cause is fixed; currently records "
            "land in data/output/dead_letter/ and nothing further happens to them.",
        ]
    )

    # 13. Appendix
    doc.add_heading1("13. Appendix")
    doc.add_heading2("13.1 References")
    doc.add_bullets(
        [
            "Repository README.md -- architecture diagram, design decisions, and \"what was actually "
            "verified\" section.",
            "schemas/telemetry.schema.json -- the JSON Schema contract for the wire format.",
            "spark_job/streaming_job.py, spark_job/transformations.py, spark_job/schemas.py -- the "
            "Structured Streaming job and its transformation/schema logic.",
            "producer/producer.py, producer/device_simulator.py, producer/config.py -- the synthetic "
            "device fleet and Kafka producer.",
            "docker-compose.yml, .env.example -- local deployment topology and configuration reference.",
            "tests/test_parsing.py, tests/test_aggregation.py, tests/test_watermark.py -- automated, "
            "repeatable verification of validation, windowing, and watermark behavior.",
            "Darviq Kafka Low-Level Design document (companion document to this HLD).",
        ]
    )
    doc.add_heading2("13.2 Change history")
    doc.add_table(
        headers=["Version", "Date", "Description"],
        rows=[["1.0", DATE, "Initial high-level design document"]],
    )

    return doc


if __name__ == "__main__":
    doc = build()
    doc.save("Darviq_Kafka_High_Level_Design.docx")
    print("Saved Darviq_Kafka_High_Level_Design.docx")
