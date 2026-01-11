from __future__ import annotations

import os
from typing import Any, Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query


DB_PATH_DEFAULT = "warehouse/kfdp.duckdb"


def _connect_readonly(db_path: str) -> duckdb.DuckDBPyConnection:
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"DuckDB file not found: {db_path}. "
            "Run the pipeline first (ingest -> load -> dbt)."
        )
    # DuckDB read-only
    return duckdb.connect(db_path, read_only=True)


def _detect_gold_schema(con: duckdb.DuckDBPyConnection) -> str:
    """
    dbt-duckdb 환경에 따라 gold schema가 'gold' 또는 'main_gold'로 생성될 수 있음.
    """
    schemas = con.execute(
        """
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY 1;
        """
    ).fetchall()
    schema_names = {s[0] for s in schemas}

    # Prefer main_gold if present (based on your current setup)
    for candidate in ("main_gold", "gold"):
        if candidate in schema_names:
            # Ensure mart exists
            exists = con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = 'mart_tfr_metrics';
                """,
                [candidate],
            ).fetchone()[0]
            if exists == 1:
                return candidate

    raise RuntimeError("Could not find mart_tfr_metrics in gold schemas (gold/main_gold).")


def _df_records(con: duckdb.DuckDBPyConnection, sql: str, params: Optional[list[Any]] = None) -> list[dict]:
    df = con.execute(sql, params or []).df()
    return df.to_dict(orient="records")


app = FastAPI(title="KFDP API", version="1.0.0")


@app.get("/health")
def health() -> dict:
    db_path = os.getenv("KFDP_DB_PATH", DB_PATH_DEFAULT)
    try:
        con = _connect_readonly(db_path)
        gold_schema = _detect_gold_schema(con)
        con.close()
        return {"status": "ok", "db_path": db_path, "gold_schema": gold_schema}
    except Exception as e:
        return {"status": "degraded", "db_path": db_path, "error": str(e)}


@app.get("/tfr/latest")
def tfr_latest() -> dict:
    db_path = os.getenv("KFDP_DB_PATH", DB_PATH_DEFAULT)
    try:
        con = _connect_readonly(db_path)
        gold_schema = _detect_gold_schema(con)
        rows = _df_records(
            con,
            f"""
            SELECT country, year, value, yoy_delta, ma_5y, is_below_replacement
            FROM {gold_schema}.mart_tfr_metrics
            ORDER BY year DESC
            LIMIT 1;
            """,
        )
        con.close()
        if not rows:
            raise HTTPException(status_code=404, detail="No rows found.")
        return rows[0]
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tfr")
def tfr_range(
    start: Optional[int] = Query(default=None, ge=1800, le=2200),
    end: Optional[int] = Query(default=None, ge=1800, le=2200),
    limit: int = Query(default=1000, ge=1, le=5000),
) -> list[dict]:
    db_path = os.getenv("KFDP_DB_PATH", DB_PATH_DEFAULT)
    try:
        con = _connect_readonly(db_path)
        gold_schema = _detect_gold_schema(con)

        where = []
        params: list[Any] = []

        if start is not None:
            where.append("year >= ?")
            params.append(start)
        if end is not None:
            where.append("year <= ?")
            params.append(end)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        sql = f"""
        SELECT country, year, value, yoy_delta, ma_5y, is_below_replacement
        FROM {gold_schema}.mart_tfr_metrics
        {where_sql}
        ORDER BY year ASC
        LIMIT ?;
        """
        params.append(limit)

        rows = _df_records(con, sql, params)
        con.close()
        return rows
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/runs")
def pipeline_runs(limit: int = Query(default=20, ge=1, le=500)) -> list[dict]:
    db_path = os.getenv("KFDP_DB_PATH", DB_PATH_DEFAULT)
    try:
        con = _connect_readonly(db_path)
        rows = _df_records(
            con,
            """
            SELECT run_ts, status, source_file, silver_rows, silver_min_year, silver_max_year, gold_rows
            FROM ops.pipeline_runs
            ORDER BY run_ts DESC
            LIMIT ?;
            """,
            [limit],
        )
        con.close()
        return rows
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
