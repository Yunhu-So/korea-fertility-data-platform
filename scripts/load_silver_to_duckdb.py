from __future__ import annotations

"""
Utility script for loading a silver parquet file into DuckDB.

The silver parquet is loaded into the ``silver`` schema of the local DuckDB
warehouse.  The script drops any existing ``silver.tfr_long`` table and
recreates it from the input parquet file.  Basic statistics are printed
after the load completes.
"""

import argparse
from pathlib import Path

import duckdb
from rich import print


def main() -> None:
    """Parse arguments and load the parquet file into DuckDB."""
    parser = argparse.ArgumentParser(
        description="Load a silver parquet file into DuckDB (silver schema)."
    )
    parser.add_argument(
        "--db",
        type=str,
        default="warehouse/kfdp.duckdb",
        help="Path to the DuckDB file",
    )
    parser.add_argument(
        "--parquet",
        type=str,
        default="data/silver/tfr_long.parquet",
        help="Path to the input parquet file",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    parquet_path = Path(args.parquet)

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found at {parquet_path}. Run the ingestion step first."
        )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # Recreate the silver table from the parquet file
    con.execute("DROP TABLE IF EXISTS silver.tfr_long;")
    con.execute(
        "CREATE TABLE silver.tfr_long AS SELECT * FROM read_parquet(?);",
        [str(parquet_path)],
    )

    # Log some basic stats
    row_count = con.execute("SELECT COUNT(*) FROM silver.tfr_long;").fetchone()[0]
    year_min, year_max = con.execute(
        "SELECT MIN(year), MAX(year) FROM silver.tfr_long;"
    ).fetchone()

    print(
        f"[green]OK[/green] Loaded {row_count} rows into silver.tfr_long ({year_min}..{year_max})"
    )
    con.close()


if __name__ == "__main__":
    main()
