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


def parse_iso_dt(s: str | None):
    """Parse ISO datetime string to Python datetime (timezone-aware if present)."""
    if not s:
        return None
    # Handle common "Z" suffix for UTC
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def collect_files():
    """Collect all JSON files from the raw data lake telegram_messages folder."""
    base = Path(RAW_DATA_DIR) / "telegram_messages"
    if not base.exists():
        raise FileNotFoundError(f"Missing raw lake folder: {base}")
    return sorted(base.rglob("*.json"))


def load_file(path: Path):
    """
    Load one JSON file safely.
    If file is corrupted (invalid JSON), print and return None.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"\n[BAD JSON] Skipping file: {path}")
        print(f"Reason: {e}\n")
        return None


def connect():
    """Create a PostgreSQL connection."""
    if not POSTGRES_PASSWORD:
        raise ValueError("POSTGRES_PASSWORD is missing. Check your .env file.")

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def main():
    files = collect_files()
    if not files:
        print("No JSON files found in data lake. Run scraper first.")
        return

    print(f"Found {len(files)} JSON files. Loading into raw.telegram_messages...")

    conn = connect()
    conn.autocommit = False
    rows_total = 0
    files_loaded = 0
    files_skipped = 0

    try:
        with conn.cursor() as cur:
            for fp in files:
                data = load_file(fp)

                if data is None:
                    files_skipped += 1
                    continue

                # Some files might be empty lists
                if not data:
                    files_skipped += 1
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
                files_loaded += 1
                print(f"Loaded {len(values)} rows from {fp}")

        conn.commit()
        print("\n✅ LOAD COMPLETE")
        print(f"Files loaded: {files_loaded}")
        print(f"Files skipped: {files_skipped}")
        print(f"Total rows inserted: {rows_total}")
        print("Data inserted into: raw.telegram_messages")

    except Exception as e:
        conn.rollback()
        print("\n❌ LOAD FAILED — rolled back transaction.")
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    main()
