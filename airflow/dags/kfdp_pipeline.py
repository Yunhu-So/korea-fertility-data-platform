from __future__ import annotations

"""
Airflow DAG definition for the Korea Fertility Data Platform pipeline.

This DAG orchestrates the ingestion of an Excel file into a long‑format
parquet file, loading it into DuckDB, running dbt models and tests, and
logging the run metadata.  It is scheduled to run once per day at 03:00 UTC.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Base directory inside the Airflow container
BASE_DIR = "/opt/airflow"

default_args = {
    "owner": "kfdp",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="kfdp_bronze_to_gold",
    default_args=default_args,
    description="Ingest Excel -> Silver Parquet -> DuckDB -> dbt Gold marts",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * *",  # run daily at 03:00 UTC
    catchup=False,
    tags=["kfdp", "duckdb", "dbt"],
) as dag:

    ingest_excel = BashOperator(
        task_id="ingest_excel_to_silver_parquet",
        bash_command=(
            f"python {BASE_DIR}/src/kfdp/cli/ingest_tfr.py "
            f"--input {BASE_DIR}/data/bronze/tfr_source.xlsx "
            f"--output {BASE_DIR}/data/silver/tfr_long.parquet"
        ),
        env={"PYTHONPATH": f"{BASE_DIR}/src"},
    )

    load_to_duckdb = BashOperator(
        task_id="load_silver_to_duckdb",
        bash_command=(
            f"python {BASE_DIR}/scripts/load_silver_to_duckdb.py "
            f"--db {BASE_DIR}/warehouse/kfdp.duckdb "
            f"--parquet {BASE_DIR}/data/silver/tfr_long.parquet"
        ),
        env={"PYTHONPATH": f"{BASE_DIR}/src"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {BASE_DIR} && "
            f"/home/airflow/.local/bin/dbt run --project-dir {BASE_DIR}/dbt/kfdp"
        ),
        env={"DBT_PROFILES_DIR": f"{BASE_DIR}/dbt"},
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {BASE_DIR} && "
            f"/home/airflow/.local/bin/dbt test --project-dir {BASE_DIR}/dbt/kfdp"
        ),
        env={"DBT_PROFILES_DIR": f"{BASE_DIR}/dbt"},
    )

    log_success = BashOperator(
        task_id="log_run_success",
        bash_command=(
            f"python {BASE_DIR}/scripts/log_pipeline_run.py "
            f"--db {BASE_DIR}/warehouse/kfdp.duckdb "
            f"--status success "
            f"--source-file tfr_source.xlsx"
        ),
        env={"PYTHONPATH": f"{BASE_DIR}/src"},
    )

    ingest_excel >> load_to_duckdb >> dbt_run >> dbt_test >> log_success
