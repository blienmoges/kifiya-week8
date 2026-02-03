import os
from dotenv import load_dotenv

load_dotenv()

# -------------------
# Telegram / scraping config
# -------------------
CHANNELS = os.getenv("CHANNELS", "").split(",")
SCRAPE_DAYS_BACK = int(os.getenv("SCRAPE_DAYS_BACK", "30"))

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/raw")
LOG_DIR = os.getenv("LOG_DIR", "logs")

# -------------------
# PostgreSQL config
# -------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "medical_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
