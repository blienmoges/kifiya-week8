#!/usr/bin/env bash
set -e

python -m src.scraper
python -m src.load_raw_to_postgres

dbt deps --project-dir medical_warehouse
dbt run --project-dir medical_warehouse
dbt test --project-dir medical_warehouse
