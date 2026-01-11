from __future__ import annotations

"""
Utility script for recording pipeline run metadata.

This script inserts a record into the ``ops.pipeline_runs`` table after
an ingestion/transformation run has completed.  It captures the timestamp,
status, source file name and basic row counts.  If both a ``main_gold``
and ``gold`` schema are present, ``main_gold`` takes precedence for the
gold row count.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def main() -> None:
    """Parse arguments and log a pipeline run into DuckDB."""
    parser = argparse.ArgumentParser(
        description="Log pipeline run metadata to DuckDB."
    )
    parser.add_argument(
        "--db",
        default="warehouse/kfdp.duckdb",
        help="Path to the DuckDB database file",
    )
    parser.add_argument(
        "--status",
        required=True,
        choices=["success", "failed"],
        help="Execution status of the pipeline run",
    )
    parser.add_argument(
        "--source-file",
        dest="source_file",
        default="tfr_source.xlsx",
        help="Name of the input source file",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    con = duckdb.connect(str(db_path))

    # Ensure the ops schema and table exist
    con.execute("CREATE SCHEMA IF NOT EXISTS ops;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
          run_ts TIMESTAMP,
          status VARCHAR,
          source_file VARCHAR,
          silver_rows BIGINT,
          silver_min_year INTEGER,
          silver_max_year INTEGER,
          gold_rows BIGINT
        );
        """
    )

    # Compute silver table statistics
    silver_rows, min_year, max_year = con.execute(
        "SELECT COUNT(*), MIN(year), MAX(year) FROM silver.tfr_long;"
    ).fetchone()

    # Compute gold table row count.  Some dbt‑duckdb versions create a 'main_gold'
    # schema instead of 'gold', so we try both and use whichever exists.
    gold_rows = 0
    for schema in ("main_gold", "gold"):
        try:
            result = con.execute(
                f"SELECT COUNT(*) FROM {schema}.mart_tfr_metrics;"
            ).fetchone()
            gold_rows = result[0]
            break
        except Exception:
            continue

    # Insert a new run record
    con.execute(
        "INSERT INTO ops.pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?);",
        [
            datetime.now(timezone.utc).replace(tzinfo=None),
            args.status,
            args.source_file,
            silver_rows,
            min_year,
            max_year,
            gold_rows,
        ],
    )
    con.close()


if __name__ == "__main__":
    main()
