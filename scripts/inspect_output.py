"""Concrete, runnable way to verify the pipeline is actually producing
aggregated output -- no dashboard required.

Uses DuckDB to query the Parquet output directories directly with SQL.
DuckDB (not Spark) is deliberate here: it's a single `pip install`, starts
in milliseconds, and can query a directory of Parquet files with zero
setup, which is exactly what "let me quickly check this worked" calls
for. Reaching for a second Spark session just to read back what the
first Spark session wrote would be slower and heavier for no benefit.

Usage (after the pipeline has been running for a couple of minutes so at
least one 1-minute window has closed and been written):

    python scripts/inspect_output.py
    python scripts/inspect_output.py --watch          # re-run every 10s
    python scripts/inspect_output.py --dead-letter     # inspect quarantined records instead
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import duckdb

# DuckDB's `.show()` pretty-prints tables using Unicode box-drawing
# characters. On Windows, a console/pipe using the legacy cp1252 codepage
# (still the default for a redirected stdout in many setups) can't encode
# those and raises UnicodeEncodeError -- a terminal quirk, not a data
# problem, but worth not crashing on since this script's whole job is
# being a reliable "did it work" check.
if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8")


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


def show_aggregates(con: duckdb.DuckDBPyConnection, path: str, top_n: int) -> None:
    glob = os.path.join(path, "**", "*.parquet")
    try:
        total = con.execute(f"SELECT count(*) FROM parquet_scan('{glob}')").fetchone()[0]
    except duckdb.IOException:
        print(f"No Parquet files found yet under {path!r} -- the aggregates query hasn't")
        print("flushed its first micro-batch. This is expected for the first ~30-90s")
        print("after starting the Spark job (it needs at least one full window plus the")
        print("watermark delay to elapse before append-mode output is emitted).")
        return

    print(f"=== Aggregates: {total} window/device/metric rows written so far ===\n")

    print("-- Most recent windows across all devices --")
    con.sql(
        f"""
        SELECT window_start, window_end, device_id, metric_type, avg_value, event_count, min_value, max_value
        FROM parquet_scan('{glob}')
        ORDER BY window_end DESC, device_id
        LIMIT {top_n}
        """
    ).show()

    print("-- Row/window counts per metric_type --")
    con.sql(
        f"""
        SELECT metric_type, count(*) AS windows, sum(event_count) AS total_events, avg(avg_value) AS overall_avg
        FROM parquet_scan('{glob}')
        GROUP BY metric_type
        ORDER BY metric_type
        """
    ).show()


def show_dead_letter(con: duckdb.DuckDBPyConnection, path: str, top_n: int) -> None:
    glob = os.path.join(path, "**", "*.parquet")
    try:
        total = con.execute(f"SELECT count(*) FROM parquet_scan('{glob}')").fetchone()[0]
    except duckdb.IOException:
        print(f"No Parquet files found yet under {path!r}.")
        return

    print(f"=== Dead letter: {total} quarantined records so far ===\n")

    print("-- Breakdown by reason --")
    con.sql(
        f"""
        SELECT error_reason, count(*) AS n
        FROM parquet_scan('{glob}')
        GROUP BY error_reason
        ORDER BY n DESC
        """
    ).show()

    print(f"-- Most recent {top_n} quarantined records --")
    con.sql(
        f"""
        SELECT quarantined_at, device_id, error_reason, raw_value
        FROM parquet_scan('{glob}')
        ORDER BY quarantined_at DESC
        LIMIT {top_n}
        """
    ).show(max_width=120)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregates-path", default=os.getenv("OUTPUT_PATH", "./data/output/aggregates"))
    parser.add_argument(
        "--dead-letter-path", default=os.getenv("DEAD_LETTER_PATH", "./data/output/dead_letter")
    )
    parser.add_argument(
        "--dead-letter", action="store_true", help="Inspect the dead-letter sink instead of aggregates."
    )
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--watch", action="store_true", help="Re-run every --interval seconds.")
    parser.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args()

    con = _connect()

    def run_once() -> None:
        if args.dead_letter:
            show_dead_letter(con, args.dead_letter_path, args.top)
        else:
            show_aggregates(con, args.aggregates_path, args.top)

    run_once()
    while args.watch:
        time.sleep(args.interval)
        print("\n" + "=" * 80 + "\n")
        run_once()


if __name__ == "__main__":
    main()
