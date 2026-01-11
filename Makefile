.PHONY: venv install ingest load dbt docs pipeline docker airflow-up airflow-down

venv:
	python3 -m venv .venv

install:
	. .venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt

ingest:
	export PYTHONPATH=src && python src/kfdp/cli/ingest_tfr.py --input data/bronze/tfr_source.xlsx --output data/silver/tfr_long.parquet

load:
	python scripts/load_silver_to_duckdb.py --db warehouse/kfdp.duckdb --parquet data/silver/tfr_long.parquet

dbt:
	export DBT_PROFILES_DIR="$(PWD)/dbt" && dbt run --project-dir dbt/kfdp && dbt test --project-dir dbt/kfdp

docs:
	export DBT_PROFILES_DIR="$(PWD)/dbt" && dbt docs generate --project-dir dbt/kfdp

pipeline: ingest load dbt

docker:
	docker compose up --build

airflow-up:
	docker compose -f docker-compose.yml -f docker-compose.airflow.yml up --build

airflow-down:
	docker compose -f docker-compose.yml -f docker-compose.airflow.yml down
