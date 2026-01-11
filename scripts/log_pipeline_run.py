from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def main() -> None:
    p = argparse.ArgumentParser(description="Log pipeline run metadata to DuckDB.")
    p.add_argument("--db", default="warehouse/kfdp.duckdb")
    p.add_argument("--status", required=True, choices=["success", "failed"])
    p.add_argument("--source_file", default="tfr_source.xlsx")
    args = p.parse_args()

    db_path = Path(args.db)
    con = duckdb.connect(str(db_path))

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

    # silver stats
    silver = con.execute(
        "SELECT COUNT(*), MIN(year), MAX(year) FROM silver.tfr_long;"
    ).fetchone()
    silver_rows, min_year, max_year = silver[0], silver[1], silver[2]

    # gold stats (환경에 따라 main_gold 또는 gold)
    # 먼저 main_gold 시도 -> 없으면 gold 시도
    gold_rows = None
    try:
        gold_rows = con.execute("SELECT COUNT(*) FROM main_gold.mart_tfr_metrics;").fetchone()[0]
    except Exception:
        try:
            gold_rows = con.execute("SELECT COUNT(*) FROM gold.mart_tfr_metrics;").fetchone()[0]
        except Exception:
            gold_rows = 0

    con.execute(
        """
        INSERT INTO ops.pipeline_runs
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
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
