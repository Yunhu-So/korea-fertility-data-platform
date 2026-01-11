from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
from rich import print


def main() -> None:
    p = argparse.ArgumentParser(description="Load silver parquet into DuckDB (silver schema).")
    p.add_argument("--db", type=str, default="warehouse/kfdp.duckdb", help="Path to DuckDB file")
    p.add_argument("--parquet", type=str, default="data/silver/tfr_long.parquet", help="Input parquet path")
    args = p.parse_args()

    db_path = Path(args.db)
    pq_path = Path(args.parquet)

    if not pq_path.exists():
        raise FileNotFoundError(f"Parquet not found: {pq_path}. Run the ingestion first.")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    # Create schemas
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # Recreate table (idempotent)
    con.execute("DROP TABLE IF EXISTS silver.tfr_long;")
    con.execute(
        """
        CREATE TABLE silver.tfr_long AS
        SELECT * FROM read_parquet(?);
        """,
        [str(pq_path)],
    )

    n = con.execute("SELECT COUNT(*) FROM silver.tfr_long;").fetchone()[0]
    yr = con.execute("SELECT MIN(year), MAX(year) FROM silver.tfr_long;").fetchone()

    print(f"[green]OK[/green] Loaded {n} rows into silver.tfr_long ({yr[0]}..{yr[1]})")
    con.close()


if __name__ == "__main__":
    main()
