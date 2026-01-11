# Korea Fertility Data Platform (KFDP)

A reproducible, end-to-end data platform that turns raw fertility-rate spreadsheets into curated analytics marts and a read-only API—built with **Python**, **DuckDB**, **dbt**, **Airflow**, **Docker**, and **FastAPI**.

> TL;DR: Excel → (Bronze/Silver) → DuckDB Warehouse → dbt Gold Mart + tests → Airflow orchestration + run metadata → FastAPI endpoints.

---

## Why this project exists

My original “Korea TFR” work started as a classic analysis notebook: load an Excel file, plot trends, compare variables, and try light forecasting.  
That was useful for exploration, but it didn’t demonstrate **Data Engineering** fundamentals:

- repeatable ingestion (not a one-off notebook)
- layered storage and modeling (bronze/silver/gold)
- automated orchestration (Airflow)
- reproducible environments (Docker)
- data quality checks (tests)
- observability and run tracking
- serving curated data as an interface (API)

This repository is a rebuild of the same topic as a **data platform**: something you can run, schedule, validate, and consume.

---

## What it delivers (outputs)

### Curated marts (DuckDB)
- **`mart_tfr_metrics`** (gold): a clean, analysis-ready table with derived metrics (e.g., YoY deltas, moving average, replacement-level flags).
- **`ops.pipeline_runs`** (ops): run metadata per pipeline execution (status, row counts, year ranges, timestamps).

### Read-only API (FastAPI)
Provides a small “data product” interface on top of curated tables:
- `GET /health`
- `GET /tfr/latest`
- `GET /tfr?start=YYYY&end=YYYY`
- `GET /runs`

---

## Architecture

### Data layers
- **Bronze**: raw input (spreadsheets) copied as-is
- **Silver**: standardized, tidy/parquet representation (schema + basic validations)
- **Warehouse**: DuckDB file (`warehouse/kfdp.duckdb`) used as a local analytics warehouse
- **Gold**: dbt models that produce curated marts + dbt tests

### Orchestration & reproducibility
- **Airflow** runs the pipeline end-to-end inside Docker
- **Docker Compose** brings up dependencies reliably on any machine
- **Makefile** offers one-command workflows for local runs

---

## Tech stack

- **Python**: ingestion, transformations, utilities
- **DuckDB**: local warehouse (portable single-file DB)
- **dbt**: transformations, modeling, testing, docs
- **Airflow**: orchestration (DAGs)
- **Docker**: reproducible development/runtime environment
- **FastAPI**: read-only serving layer

---

## Repository layout (high level)

api/ # FastAPI app (read-only endpoints)
airflow/dags/ # Airflow DAGs (end-to-end pipeline)
data/ # bronze/silver artifacts (raw + standardized)
dbt/kfdp/ # dbt project (models, tests)
scripts/ # warehouse loaders, run logger, utilities
src/ # ingestion + transformation modules (silver)
warehouse/ # DuckDB database file
docker-compose*.yml # compose files (airflow, api, etc.)
Makefile # one-command workflows

---

## Quickstart (local)

### 1) Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

### 2) Run the pipeline locally
`make pipeline`
### 3) Validate outputs (DuckDB)
```bash
python - << 'PY'
import duckdb
con = duckdb.connect("warehouse/kfdp.duckdb")
print(con.execute("SHOW ALL TABLES").df())
print(con.execute("SELECT * FROM ops.pipeline_runs ORDER BY run_ts DESC LIMIT 5").df())
con.close()
PY
```

### Run with Airflow (Docker)
Bring up the Airflow stack and run the DAG from the UI.
`make airflow-up`
Airflow UI: http://localhost:8080
Trigger the DAG (e.g., kfdp_pipeline) and monitor task logs.
Shut down:
`make airflow-down`

### API (FastAPI)
Run locally
```bash
source .venv/bin/activate
uvicorn api.app:app --reload --port 8000
```
Open:
http://127.0.0.1:8000/docs

### Run with Docker
`docker compose -f docker-compose.api.yml up --build`
Example calls:
GET /health
GET /tfr/latest
GET /tfr?start=1990&end=2024
GET /runs

### Data quality & observability
- dbt tests enforce expectations on curated tables (schema + constraints).
- ops.pipeline_runs records each end-to-end execution with:
    - run timestamp
    - success/failure
    - silver row counts + year range
    - gold row counts

This creates a minimal but real “production habit”: we can see what happened, when, and how big the output was.

### Future improvements (roadmap)
If I extend this beyond a local platform, the next steps would be:
- CI/CD: run dbt tests + linting on every PR (GitHub Actions)
- Docs hosting: publish dbt docs to GitHub Pages automatically
- Incremental loads: detect new/changed input and load incrementally
- Data contracts: explicit schema versions and compatibility checks
- Serving: pagination, caching, and versioned endpoints
- Cloud migration: S3 + Athena/Iceberg or BigQuery/Snowflake backend