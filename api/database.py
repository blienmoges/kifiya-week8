# api/database.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Load .env from project root (works when you run uvicorn from repo root)
load_dotenv()

def get_engine() -> Engine:
    """
    Returns a SQLAlchemy Engine connected to Postgres using env vars.
    Uses 127.0.0.1 instead of localhost to avoid Windows socket issues.
    """
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "medical_dw")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd  = os.getenv("POSTGRES_PASSWORD", "")

    if not pwd:
        raise RuntimeError("POSTGRES_PASSWORD is missing in your environment/.env")

    # psycopg2 driver
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

    return create_engine(
        url,
        pool_pre_ping=True,   # avoids stale connections
        future=True,
    )
