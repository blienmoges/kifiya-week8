import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_API_ID = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_SESSION = os.getenv("TELEGRAM_SESSION", "medical_warehouse")

CHANNELS = [c.strip() for c in os.getenv("CHANNELS", "").split(",") if c.strip()]
SCRAPE_DAYS_BACK = int(os.getenv("SCRAPE_DAYS_BACK", "30"))

# Paths
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/raw")
LOG_DIR = os.getenv("LOG_DIR", "logs")

# Postgres
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "medical_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
