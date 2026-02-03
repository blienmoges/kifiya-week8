# medical-telegram-warehouse

End-to-end ELT pipeline:
- Extract from Telegram (Telethon)
- Raw Data Lake (partitioned JSON + downloaded images)
- Load to Postgres (raw schema)
- Transform in-warehouse with dbt (staging + star schema marts)

## 1. Setup

### 1) Create virtualenv (Windows Git Bash)
```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
# Medical Telegram Warehouse

## Overview
Pipeline:
1) Scrape Telegram messages + images (Task-1)
2) Load raw JSON into Postgres raw schema (Task-2)
3) Transform with dbt into analytics schema (Task-2)
4) Detect objects in images with YOLO and load detections (Task-3)
5) Serve analytics via FastAPI (Task-4)
6) End-to-end execution script + docs (Task-5)

## Requirements
- Python 3.11 recommended
- Postgres running locally
- dbt-postgres
- ultralytics (YOLO)
- FastAPI + Uvicorn

## Setup
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
## Orchestration (Dagster)

The entire data pipeline is orchestrated using Dagster.

Pipeline steps:
1. Load raw Telegram messages into Postgres
2. Run YOLO image detection
3. Load YOLO detections
4. Transform data using dbt (star schema)
5. Run dbt tests

The pipeline is scheduled to run daily at 06:00 (Africa/Addis_Ababa).

To run locally:
```bash
dagster dev -f orchestration/repository.py

---

# Final result if you do this

After these steps, your project will have:

✅ End-to-end pipeline  
✅ dbt transformations + tests  
✅ YOLO enrichment  
✅ FastAPI analytics API  
✅ **Dagster orchestration + scheduling**  
✅ Improved Git workflow  

➡️ That directly addresses **every line of the feedback**.

---

If you want, next I can:
- Review your **README.md wording**
- Help you choose **exact screenshots** for final submission
- Do a **final grading checklist** (what graders look for line-by-line)

Just tell me 👍
