# Korea Fertility Data Platform (KFDP)

This repository contains a reproducible data engineering project that takes raw fertility‑rate spreadsheets and turns them into a curated analytics mart with a read‑only API.  The goal of this project is to demonstrate how to build a small but complete data platform using **Python**, **DuckDB**, **dbt**, **Airflow**, **Docker** and **FastAPI**.

At a high level the pipeline looks like this:

1. **Ingestion** – an Excel file containing total fertility rate (TFR) by year is parsed into a tidy “long” parquet file.
2. **Warehouse loading** – the parquet file is loaded into a local DuckDB database with separate schemas for raw and curated data.
3. **Transformation** – [dbt](https://www.getdbt.com/) models transform the raw data into a gold‑level mart, computing useful metrics such as year‑over‑year deltas, five‑year moving averages and a below‑replacement flag.  dbt tests validate the results.
4. **Orchestration** – [Airflow](https://airflow.apache.org/) schedules and runs the ingestion, loading and transformation tasks on a daily cadence inside Docker.  After a run finishes, basic run metadata is logged to the warehouse.
5. **Serving** – a [FastAPI](https://fastapi.tiangolo.com/) application provides a simple REST interface to the curated data, including endpoints for the most recent metrics, a date‑range query and pipeline run history.

This implementation grew out of a personal research notebook.  By rebuilding it as a proper data platform you can reliably ingest new source files, run tests, view run history and serve data without touching a Jupyter notebook.

---

## Outputs

### Curated marts (DuckDB)

After running the pipeline there are two primary tables of interest in the DuckDB database:

- **`mart_tfr_metrics`** (gold schema) – an analysis‑ready table with derived metrics such as the year‑over‑year change, a five‑year moving average and a flag indicating whether the value is below the replacement fertility threshold.
- **`ops.pipeline_runs`** (ops schema) – metadata about each pipeline execution, including the timestamp, success/failure status, row counts and year ranges for the silver and gold layers.

### Read‑only API (FastAPI)

The API exposes a small data product on top of the mart.  It includes health information, the latest metrics and a query for historical ranges:

- `GET /health` – check the database path and gold schema used by the API.
- `GET /tfr/latest` – fetch the most recent row from the mart.
- `GET /tfr?start=YYYY&end=YYYY&limit=N` – get all rows between `start` and `end` (inclusive), limited to `N` results.
- `GET /runs` – list recent pipeline runs.

---

## Architecture

### Data layers

- **Bronze** – raw Excel input is preserved unchanged.
- **Silver** – the raw file is normalized into a long‑format parquet file with basic validations.
- **Warehouse** – a local DuckDB file (`warehouse/kfdp.duckdb`) stores the data; each layer has its own schema.
- **Gold** – dbt models transform the silver data into a curated mart and run tests against it.

### Orchestration & reproducibility

Airflow orchestrates the end‑to‑end pipeline inside Docker.  Docker Compose provides repeatable environments for both the Airflow stack and the API.  A Makefile offers convenience targets to run ingestion, loading, dbt and the entire pipeline locally.  Tests live in `tests/` to ensure ingestion produces clean data.

---

## Repository layout

```
api/                 # FastAPI application
airflow/dags/        # Airflow DAG definitions
data/                # bronze/silver artifacts (raw Excel and parquet)
dbt/kfdp/            # dbt project (models, tests, profiles)
scripts/             # utility scripts (loading, logging)
src/kfdp/            # ingestion and validation code
warehouse/           # DuckDB database file (generated)
docker-compose*.yml  # Docker Compose configurations
Makefile             # one‑command workflows
```

---

## Getting started

1. **Set up a virtual environment**

   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt -r requirements-dev.txt
   ```

2. **Ingest and transform locally**

   Run the entire pipeline outside of Docker:

   ```sh
   make pipeline
   ```

   This will ingest the Excel file, load it into DuckDB, run the dbt models and tests, and log a pipeline run.

3. **Inspect the warehouse**

   Connect to the DuckDB database to explore tables:

   ```python
   import duckdb

   con = duckdb.connect("warehouse/kfdp.duckdb")
   print(con.execute("SHOW ALL TABLES").df())
   print(con.execute("SELECT * FROM ops.pipeline_runs ORDER BY run_ts DESC LIMIT 5").df())
   con.close()
   ```

4. **Run with Airflow**

   Bring up the Airflow stack via Docker Compose:

   ```sh
   make airflow-up
   ```

   Open the Airflow UI at <http://localhost:8080>, trigger the `kfdp_bronze_to_gold` DAG, then monitor task logs.  When done, shut down with `make airflow-down`.

5. **Serve the API**

   You can run the API locally using uvicorn:

   ```sh
   source .venv/bin/activate
   uvicorn api.app:app --reload --port 8000
   ```

   Or spin it up via Docker Compose:

   ```sh
   docker compose -f docker-compose.api.yml up --build
   ```

   Once running, visit <http://127.0.0.1:8000/docs> for an interactive Swagger UI.

---

## Data quality & observability

Data quality is enforced through dbt tests on the gold mart.  Each pipeline run logs metadata—timestamp, status, input file, row counts and year ranges—into the `ops.pipeline_runs` table.  These logs make it easy to audit what happened, when it happened and how big the outputs were.

---

## Future improvements

Potential next steps include adding continuous integration for tests and linting, publishing dbt documentation to GitHub Pages, implementing incremental loading, defining explicit data contracts, adding pagination/caching/versioning in the API and migrating storage to a cloud data lake.
