# scripts/run_all.ps1
$ErrorActionPreference = "Stop"

Write-Host "1) Load raw telegram JSON to Postgres..."
python -m src.load_raw_to_postgres

Write-Host "2) Run YOLO detection..."
python src/yolo_detect.py

Write-Host "3) Load YOLO results to Postgres..."
python src/load_yolo_to_postgres.py

Write-Host "4) dbt build (run + test)..."
dbt build --project-dir medical_warehouse --profiles-dir medical_warehouse

Write-Host "5) Start API..."
python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
