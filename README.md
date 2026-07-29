# Kafka + Spark Structured Streaming: IoT Telemetry Pipeline

A reference streaming data pipeline: a synthetic IoT device fleet publishes
telemetry to Kafka, and a PySpark Structured Streaming job consumes it,
validates it, computes windowed aggregates with watermarking, and routes
anything malformed to a dead-letter sink instead of crashing or silently
corrupting results.

Themed around IoT device telemetry deliberately -- it mirrors the shape of
the large-scale connected-device platforms this pipeline's author has
worked on professionally (telecom IoT, 1M+ devices), scaled down to
something that runs on a laptop and is honest about the difference between
the two (see [Production considerations](#production-considerations)).

## Why streaming, not batch

Device telemetry is naturally an unbounded, continuously-arriving event
stream, and the questions worth asking of it are inherently time-windowed
("what was this device's average temperature over the last minute?",
"has it gone quiet?"). A batch job re-scanning accumulated files every N
minutes would work, but it means:

- **Latency is coupled to batch cadence.** A device going out of a safe
  operating range is discovered whenever the next batch happens to run,
  not close to when it happened.
- **Reprocessing cost grows with retained history**, unless the batch job
  is carefully written to only scan new files -- which is exactly the
  bookkeeping Structured Streaming's checkpointing already does for you.
- **Out-of-order and late-arriving data needs the same watermarking
  concept either way** -- a batch job still has to decide "how late is
  too late," Structured Streaming just makes that decision an explicit,
  first-class part of the API (`withWatermark`) instead of ad hoc
  filtering logic re-invented per job.

None of this means batch is wrong in general -- for this specific
workload (continuous device telemetry, time-windowed questions, a need
for bounded end-to-end latency) streaming is the better default, which is
why this repo demonstrates it.

## Architecture

```
                    ┌─────────────────────────┐
                    │   producer/producer.py   │
                    │  synthetic IoT fleet:     │
                    │  - N simulated devices     │
                    │  - random-walk metric      │
                    │    values (temp/humidity/  │
                    │    battery/signal)          │
                    │  - devices go offline       │
                    │  - malformed payloads       │
                    │  - late/out-of-order        │
                    │    timestamps               │
                    │  - duplicate sends           │
                    └───────────┬─────────────┘
                                │ JSON, keyed by device_id
                                ▼
                ┌───────────────────────────────┐
                │   Kafka (KRaft, single broker) │
                │   topic: raw.iot-telemetry      │
                │   docker-compose.yml            │
                └───────────────┬─────────────────┘
                                │ readStream (kafka)
                                ▼
        ┌───────────────────────────────────────────────┐
        │        spark_job/streaming_job.py               │
        │        (PySpark Structured Streaming)            │
        │                                                    │
        │  parse_kafka_value()  -- from_json + safe cast      │
        │           │                                           │
        │  classify_events()  -- is_valid / error_reason          │
        │           │                                              │
        │     ┌─────┴─────┐                                        │
        │     ▼           ▼                                        │
        │  valid_df    dead_letter_df                                │
        │     │           │                                           │
        │  withWatermark  │                                            │
        │  + window(1min) │                                             │
        │  groupBy(device, │                                             │
        │   metric_type)   │                                             │
        │  avg/count/min/  │                                             │
        │  max             │                                             │
        └──────┬───────────┴──────┐                                      │
               ▼                  ▼
    ┌────────────────────┐ ┌────────────────────────┐
    │ data/output/         │ │ data/output/              │
    │  aggregates/ (Parquet)│ │  dead_letter/ (Parquet)   │
    │  1 row per window/     │ │  raw payload + reason +    │
    │  device/metric          │ │  quarantine timestamp       │
    └──────────┬──────────┘ └────────────────────────┘
               │
               ▼
     scripts/inspect_output.py
     (DuckDB SQL over the Parquet directory --
      concrete, runnable verification)
```

Two independent streaming queries read from the same Kafka topic (one
per Kafka consumer group) and write to two separate Parquet sinks, each
with its own checkpoint directory. See the module docstring in
`spark_job/streaming_job.py` for why this is two queries instead of one
`foreachBatch` doing both jobs.

## Data quality is the point, not an afterthought

A pipeline that only ever sees clean, in-order, well-typed JSON doesn't
exercise Structured Streaming's actual value proposition. The producer
deliberately injects, at configurable rates:

| Failure mode | How it's produced | How it's handled |
|---|---|---|
| Missing required field | drops `device_id`/`timestamp`/`metric_type`/`value` | routed to dead-letter, reason `missing_device_id` etc. |
| Invalid enum value | `metric_type: "warp_factor"` | reason `invalid_metric_type` |
| Non-numeric value | `value: "N/A"` | reason `missing_or_non_numeric_value` |
| Unparseable timestamp | `timestamp: "not-a-timestamp"` | reason `invalid_timestamp` |
| Truncated / corrupt JSON | payload cut off mid-object | reason `invalid_json` |
| Out-of-range physical value | e.g. `temperature: 999.9` | reason `value_out_of_range` |
| Device offline | fleet member stops sending for N ticks | simply absent from that window (no special handling needed -- windows are per-device, so a quiet device just doesn't contribute a row) |
| Late / out-of-order event | device timestamp set minutes in the past | correctly bucketed into its real window if still within the watermark; **dropped** if older than the watermark (see `tests/test_watermark.py`) |
| Duplicate delivery | same event sent twice | **not** deduplicated in this demo -- see [Production considerations](#production-considerations) |

## Design decisions

**JSON Schema, not Avro.** The wire format is plain JSON validated
against `schemas/telemetry.schema.json` (a standard JSON Schema
document, human-readable and diffable in a PR). Avro plus a Schema
Registry is the more usual choice for a high-throughput production Kafka
pipeline (compact binary encoding, enforced compatibility checks on
schema evolution), but it adds a whole extra service to run and reason
about for a demo whose point is the Spark processing logic, not schema
governance. This is called out explicitly, not glossed over: **a real
production version of this pipeline should use Avro + Schema Registry**
(see below).

**Parquet, not Postgres, as the aggregate sink.** Parquet needs no
extra service, is trivial to inspect with DuckDB/Spark/pandas, and
partitions naturally by writing new files per micro-batch -- appropriate
for a demo and for genuinely append-only, rarely-updated aggregate
output. The real tradeoff: Postgres would give you indexed point lookups
("give me this device's last hour, right now"), concurrent readers
without file-listing overhead, and a natural place to enforce
upsert/dedup semantics if you later need `outputMode("update")` instead
of `append`. For a dashboard or alerting system consuming these
aggregates downstream, Postgres (or a proper OLAP store) is the better
choice; for "prove the pipeline computes the right numbers," Parquet is
simpler and sufficient.

**Two dead-letter reasons kept separate from JSON-level parse
failures.** `from_json` in PERMISSIVE mode does not gracefully null out
one bad field and continue -- if a single field fails to coerce to its
declared type, Jackson fails the *entire* record and reports it via
`_corrupt_record`. That would make "device sent `value: "N/A"`"
indistinguishable from "device sent truncated garbage," which are very
different operational problems. The fix (see `spark_job/schemas.py`):
parse every field as a string, then cast `value` to `double` explicitly
in `parse_kafka_value` -- a SQL cast returns null on failure instead of
aborting the row, which gives back the ability to report
`missing_or_non_numeric_value` as its own dead-letter reason distinct
from `invalid_json`. This was caught by a failing unit test during
development, not discovered by inspection -- see
`tests/test_parsing.py::test_non_numeric_value_is_quarantined`.

## Prerequisites

- Docker Desktop (or another Docker Engine + Compose v2)
- Python 3.12 (Spark/PySpark local-mode compatibility is best on 3.11/3.12;
  3.13+ is not yet well-supported by the PySpark version pinned here)
- Java 17 or 21 (PySpark needs a JVM; this was developed and tested
  against Java 21)
- **Windows only:** Spark's local filesystem access (used for the
  Parquet sinks, checkpoints, and the file-source watermark test) needs
  Hadoop's `winutils.exe`/`hadoop.dll` shim, which does not ship with
  PySpark. Download a matching `hadoop-3.3.x` build from
  [cdarlint/winutils](https://github.com/cdarlint/winutils) (this repo
  was built/tested against `hadoop-3.3.5`), put `winutils.exe` and
  `hadoop.dll` in some `<dir>\bin`, and set:
  ```
  set HADOOP_HOME=<dir>
  set PATH=%HADOOP_HOME%\bin;%PATH%
  set PYSPARK_SUBMIT_ARGS=--driver-java-options -Djava.library.path=<dir forward-slash form>/bin pyspark-shell
  ```
  Linux/macOS need none of this -- including CI, which runs on
  `ubuntu-latest` and never touches this workaround.

## Running it locally

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements-dev.txt

cp .env.example .env         # adjust if needed; defaults work as-is

# 1. Start Kafka (KRaft mode) + Kafka UI (http://localhost:8080)
docker compose up -d

# 2. Explicitly create the topic (3 partitions, 7-day retention) rather
#    than relying on the demo broker's auto-create-topics setting
python scripts/create_topics.py

# 3. Start the producer (runs until Ctrl+C, or set RUN_SECONDS to stop
#    automatically)
python -m producer.producer

# 4. In another terminal, start the Spark job. This needs the Kafka
#    connector, resolved by --packages the first time (downloads once,
#    then cached under ~/.ivy2):
PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell' \
    python -m spark_job.streaming_job

# 5. Give it a couple of minutes (1-minute windows + 2-minute watermark
#    delay before append-mode output is flushed), then, in a third
#    terminal:
python scripts/inspect_output.py            # aggregates
python scripts/inspect_output.py --dead-letter
python scripts/inspect_output.py --watch     # re-check every 10s
```

`scripts/inspect_output.py` uses DuckDB to run SQL directly over the
Parquet output directories -- no dashboard, no second Spark session,
just `SELECT ... FROM parquet_scan(...)`. Actual output from a local run
of this exact pipeline (see [What was actually verified](#what-was-actually-verified-and-how)
for the full numbers):

```
=== Aggregates: 138 window/device/metric rows written so far ===

-- Most recent windows across all devices --
┌─────────────────────┬─────────────────────┬──────────────┬─────────────────┬───┬─────────────┬───────────┬───────────┐
│    window_start     │     window_end      │  device_id   │   metric_type   │ … │ event_count │ min_value │ max_value │
├─────────────────────┼─────────────────────┼──────────────┼─────────────────┼───┼─────────────┼───────────┼───────────┤
│ 2026-07-29 09:28:00 │ 2026-07-29 09:29:00 │ dev-180cde3f │ temperature     │ … │          42 │      24.1 │     33.11 │
│ 2026-07-29 09:28:00 │ 2026-07-29 09:29:00 │ dev-180cde3f │ humidity        │ … │          30 │     54.89 │     75.31 │
│ 2026-07-29 09:28:00 │ 2026-07-29 09:29:00 │ dev-180cde3f │ signal_strength │ … │          40 │    -110.0 │    -69.73 │
└─────────────────────┴─────────────────────┴──────────────┴─────────────────┴───┴─────────────┴───────────┴───────────┘
```

To stop everything: `Ctrl+C` the producer and the Spark job, then
`docker compose down` (add `-v` to also drop the Kafka data volume).

## What was actually verified, and how

This pipeline was run end-to-end in the environment used to build it:
`docker compose up -d` brought up a healthy single-broker KRaft Kafka
plus Kafka UI; `scripts/create_topics.py` created the topic;
`producer/producer.py` ran for several minutes against it at 30
events/sec across 15 simulated devices; `spark_job/streaming_job.py`
started both streaming queries against the live topic and (after the
window + watermark delay elapsed) produced real output on both sinks,
confirmed with `scripts/inspect_output.py`:

- **Aggregates**: 138 window/device/metric rows, spanning all 4 metric
  types (e.g. 49 humidity windows totaling 1920 events, overall
  avg 63.19; 45 signal_strength windows, overall avg -82.01 dBm) --
  numbers from that one local run, not a benchmark claim.
- **Dead letter**: 263 quarantined records, broken down by every reason
  the classifier defines (71 `invalid_timestamp`, 63
  `missing_or_non_numeric_value`, 53 `invalid_metric_type`, 49
  `invalid_json`, 16 `value_out_of_range`, 11 `missing_device_id`) --
  i.e. every failure mode the producer injects was actually caught and
  routed, not just handled in theory.

The one thing not exercised in that run is horizontal scale (multiple
brokers, multiple partitions with concurrent consumers, multiple Spark
executors) -- everything above was verified on a single-broker,
single-machine setup, consistent with everything else in this README
about scope. Local Spark micro-batches on this machine were also slower
than the 15s-30s trigger interval configured (each batch took ~20-45s,
visible as `ProcessingTimeExecutor: Current batch is falling behind`
warnings in the job log) -- a local-mode/shuffle-partition-count
artifact of tiny data on a laptop, not a correctness issue; it only
means the demo's own output appears a little later than the configured
trigger interval would otherwise suggest.

**Automated, repeatable verification** is the unit test suite (14
tests, `pytest tests/ -v`, all passing), which is what CI actually runs
and what to trust over any one-off manual run:

- `tests/test_parsing.py` (8 tests) -- every dead-letter reason
  (missing field, invalid enum, non-numeric value, bad timestamp,
  truncated JSON, out-of-range value) against a local SparkSession, no
  Kafka/Docker involved.
- `tests/test_aggregation.py` (5 tests) -- window bucket boundaries,
  avg/count/min/max correctness, that out-of-order row *arrival* doesn't
  affect bucketing (only `event_time` does), multi-device/multi-metric
  isolation, and sliding-window overlap.
- `tests/test_watermark.py` (1 test) -- the one genuinely
  streaming-only behavior: feeds a real streaming query three file-source
  micro-batches across restarts sharing one checkpoint, and asserts that
  a reading arriving after the watermark has passed its window is
  **dropped** rather than corrupting an already-emitted aggregate. See
  the module docstring for why this needs an actual streaming query
  (batch DataFrames never evict state) and how the test drives it
  without Kafka.

## Running the tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
ruff check .
black --check .
```

No Docker, no Kafka broker, and no network access are required for the
test suite -- it spins up a local (non-cluster) SparkSession and feeds it
small static DataFrames plus one tiny file-based streaming source. This
is also exactly what `.github/workflows/ci.yaml` runs on every push/PR,
alongside `docker compose config` validation of `docker-compose.yml`.

## Repository structure

```
docker-compose.yml          # Kafka (KRaft, single broker) + Kafka UI
.env.example                 # all tunables; copy to .env (gitignored)
schemas/
  telemetry.schema.json       # JSON Schema contract for the wire format

producer/
  config.py                    # env-var-driven ProducerConfig
  device_simulator.py           # pure-Python fleet simulation (no Kafka dep -- easy to unit test)
  producer.py                    # Kafka producer entrypoint (python -m producer.producer)

spark_job/
  schemas.py                     # Spark StructType + validation constants
  transformations.py              # parse/classify/split/window -- pure DataFrame functions, unit tested
  streaming_job.py                 # wires transformations.py to Kafka source + Parquet sinks

scripts/
  create_topics.py                 # explicit topic creation (partitions/retention), not auto-create
  inspect_output.py                 # DuckDB queries over the Parquet output -- the verification step

tests/
  conftest.py                       # shared local SparkSession fixture, forces UTC for determinism
  test_parsing.py                    # dead-letter reason coverage
  test_aggregation.py                 # windowing/grouping math
  test_watermark.py                    # real streaming query, late-data eviction

.github/workflows/ci.yaml              # lint (ruff+black) / pytest / docker-compose config validate
```

## Production considerations

This is a reference architecture sized to run on one laptop, not a claim
of telecom-IoT scale. Concretely, what would change for that:

- **Multi-broker Kafka with real replication.** This repo runs a single
  KRaft broker (`replication_factor=1` everywhere) -- fine for a demo,
  a single point of failure in production. A real cluster needs 3+
  brokers, `replication_factor>=3`, `min.insync.replicas` tuned, and
  rack/AZ awareness.
- **Schema Registry + Avro (or Protobuf).** JSON Schema here is
  validated at the application layer (in Spark, after the fact); a
  Confluent/Redpanda Schema Registry with Avro enforces compatibility
  *at write time*, rejecting incompatible producer changes before they
  ever reach a topic, and gives you compact binary encoding at scale.
- **Spark on YARN/Kubernetes, not local mode.** `spark_job/streaming_job.py`
  runs as a single local-mode JVM here. Real throughput needs a proper
  cluster manager (Kubernetes via the Spark Operator, or YARN) with
  multiple executors, driver HA, and dynamic allocation -- and the same
  transformation code in `transformations.py` would run unchanged there,
  since it never assumes local mode.
- **Deduplication.** The producer simulates at-least-once delivery
  (duplicate sends); this pipeline does **not** deduplicate them --
  duplicates currently just slightly inflate `event_count`/skew
  `avg_value` in whatever window they land in. Production would add
  either an idempotent producer (`enable.idempotence=true` plus
  transactional writes) or dedup-by-key logic in the aggregation
  (e.g. `dropDuplicatesWithinWatermark` on an event ID field).
- **Security.** The demo broker has no authentication, no TLS, no ACLs
  (`PLAINTEXT` only) -- acceptable for `localhost`, not for anything
  reachable over a real network. Production needs SASL/mTLS between
  producers/brokers/Spark, and per-topic ACLs.
- **Monitoring/alerting.** There is no metrics/alerting layer here beyond
  what Kafka UI and the streaming query's own progress logs show.
  Production would export Spark's streaming query metrics (via the
  `StreamingQueryListener` API) and Kafka JMX metrics to
  Prometheus/Grafana, and alert on consumer lag and processing-time
  trend, not just on hard failures.
- **Dead-letter follow-up.** Records land in `data/output/dead_letter/`
  here and nothing further happens to them. Production would alert on
  dead-letter volume/rate (a spike usually means a firmware regression
  upstream) and have a replay path once the root cause is fixed.

No throughput numbers are quoted here beyond what's in "What was
actually verified" above (event counts observed in one local run) --
this repo has not been load-tested, and claiming a number without
measuring it would be exactly the kind of thing this section is trying
to be honest about *not* doing.
