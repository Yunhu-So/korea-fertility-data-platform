from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

BASE = "/opt/airflow"

default_args = {
    "owner": "kfdp",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="kfdp_bronze_to_gold",
    default_args=default_args,
    description="Bronze Excel -> Silver Parquet -> DuckDB -> dbt Gold marts",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * *",  # 매일 03:00 UTC
    catchup=False,
    tags=["kfdp", "duckdb", "dbt"],
) as dag:

    ingest_excel = BashOperator(
        task_id="ingest_excel_to_silver_parquet",
        bash_command=(
            f"python {BASE}/src/kfdp/cli/ingest_tfr.py "
            f"--input {BASE}/data/bronze/tfr_source.xlsx "
            f"--output {BASE}/data/silver/tfr_long.parquet"
        ),
        env={
            "PYTHONPATH": f"{BASE}/src",
        },
    )

    load_to_duckdb = BashOperator(
        task_id="load_silver_to_duckdb",
        bash_command=(
            f"python {BASE}/scripts/load_silver_to_duckdb.py "
            f"--db {BASE}/warehouse/kfdp.duckdb "
            f"--parquet {BASE}/data/silver/tfr_long.parquet"
        ),
        env={
            "PYTHONPATH": f"{BASE}/src",
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {BASE} && /home/airflow/.local/bin/dbt run --project-dir {BASE}/dbt/kfdp",
        env={
            "DBT_PROFILES_DIR": f"{BASE}/dbt",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {BASE} && /home/airflow/.local/bin/dbt test --project-dir {BASE}/dbt/kfdp",
        env={
            "DBT_PROFILES_DIR": f"{BASE}/dbt",
        },
    )

    log_success = BashOperator(
        task_id="log_run_success",
        bash_command=f"python {BASE}/scripts/log_pipeline_run.py --db {BASE}/warehouse/kfdp.duckdb --status success --source_file tfr_source.xlsx",
        env={"PYTHONPATH": f"{BASE}/src"},
    )        
    
    ingest_excel >> load_to_duckdb >> dbt_run >> dbt_test >> log_success
