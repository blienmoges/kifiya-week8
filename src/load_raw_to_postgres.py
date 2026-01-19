import json
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

from src.config import (
    RAW_DATA_DIR,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

INSERT_SQL = """
INSERT INTO raw.telegram_messages (
  message_id, channel_name, message_date, message_text, has_media,
  image_path, views, forwards, raw
) VALUES %s
"""

def parse_iso_dt(s):
    if not s:
        return None
    # Telethon gives ISO with timezone, keep it
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def collect_files():
    base = Path(RAW_DATA_DIR) / "telegram_messages"
    if not base.exists():
        raise FileNotFoundError(f"Missing raw lake folder: {base}")
    return list(base.rglob("*.json"))

def load_file(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    files = collect_files()
    if not files:
        print("No JSON files found in data lake. Run scraper first.")
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = False

    rows_total = 0

    try:
        with conn.cursor() as cur:
            for fp in files:
                data = load_file(fp)
                if not data:
                    continue

                values = []
                for r in data:
                    values.append(
                        (
                            r.get("message_id"),
                            r.get("channel_name"),
                            parse_iso_dt(r.get("message_date")),
                            r.get("message_text"),
                            r.get("has_media"),
                            r.get("image_path"),
                            r.get("views"),
                            r.get("forwards"),
                            json.dumps(r.get("raw", {})),
                        )
                    )

                execute_values(cur, INSERT_SQL, values, page_size=2000)
                rows_total += len(values)

        conn.commit()
        print(f"Loaded {rows_total} rows into raw.telegram_messages")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
