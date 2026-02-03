import os
import csv
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "medical_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

CSV_PATH = Path("data/processed/yolo/yolo_detections.csv")

INSERT_SQL = """
INSERT INTO raw.yolo_detections (
  run_ts, channel_name, message_id, image_path,
  detected_class, confidence_score, bbox_xyxy, image_category
) VALUES %s
"""

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing {CSV_PATH}. Run: python src/yolo_detect.py")

    if not POSTGRES_PASSWORD:
        raise ValueError("Missing POSTGRES_PASSWORD. Ensure .env exists and is correct.")

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = False

    rows = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                r["run_ts"] or None,
                (r["channel_name"] or "").strip().lower() or None,
                int(r["message_id"]) if r["message_id"] else None,
                r["image_path"] or None,
                r["detected_class"] or None,
                float(r["confidence_score"]) if r["confidence_score"] else None,
                r["bbox_xyxy"] or None,
                r["image_category"] or None
            ))

    try:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, rows, page_size=5000)
        conn.commit()
        print(f"✅ Loaded {len(rows)} rows into raw.yolo_detections")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
