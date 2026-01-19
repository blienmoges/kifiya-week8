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
