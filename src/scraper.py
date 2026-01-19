import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

from telethon import TelegramClient
from telethon.tl.types import Message

from src.config import (
    TELEGRAM_API_ID,
    TELEGRAM_API_HASH,
    TELEGRAM_SESSION,
    CHANNELS,
    SCRAPE_DAYS_BACK,
    RAW_DATA_DIR,
    LOG_DIR,
)

def slugify(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")

def setup_logger(log_dir: str) -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(Path(log_dir) / "scraper.log", encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)

    if not logger.handlers:
        logger.addHandler(fh)
    return logger

def msg_to_dict(m: Message, channel_name: str, image_path: Optional[str]) -> Dict[str, Any]:
    return {
        "message_id": m.id,
        "channel_name": channel_name,
        "message_date": m.date.isoformat() if m.date else None,
        "message_text": m.message,
        "has_media": bool(m.media),
        "image_path": image_path,
        "views": getattr(m, "views", None),
        "forwards": getattr(m, "forwards", None),
        # preserve full API payload
        "raw": m.to_dict(),
    }

async def scrape_channel(
    client: TelegramClient,
    channel: str,
    start_dt: datetime,
    raw_dir: str,
    logger: logging.Logger,
) -> int:
    channel_slug = slugify(channel)
    messages_by_day: Dict[str, List[Dict[str, Any]]] = {}

    entity = await client.get_entity(channel)

    async for m in client.iter_messages(entity):
        if not m.date:
            continue
        if m.date < start_dt:
            break

        day = m.date.astimezone(timezone.utc).date().isoformat()
        image_path = None

        # Download image if the message has a photo
        if m.photo:
            img_dir = Path(raw_dir) / "images" / channel_slug
            img_dir.mkdir(parents=True, exist_ok=True)
            img_file = img_dir / f"{m.id}.jpg"
            try:
                await client.download_media(m, file=str(img_file))
                image_path = str(img_file)
            except Exception as e:
                logger.error(f"Image download failed | channel={channel_slug} msg={m.id} err={e}")

        rec = msg_to_dict(m, channel_slug, image_path)
        messages_by_day.setdefault(day, []).append(rec)

    # write partitioned JSON files per date for the channel
    written = 0
    for day, rows in messages_by_day.items():
        out_dir = Path(raw_dir) / "telegram_messages" / day
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{channel_slug}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        written += len(rows)
        logger.info(f"Wrote {len(rows)} rows -> {out_file}")

    return written

async def main():
    if not CHANNELS:
        raise ValueError("CHANNELS is empty. Set CHANNELS in .env (comma-separated).")

    raw_dir = RAW_DATA_DIR
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    logger = setup_logger(LOG_DIR)
    start_dt = datetime.now(timezone.utc) - timedelta(days=SCRAPE_DAYS_BACK)

    async with TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
        total = 0
        for ch in CHANNELS:
            try:
                logger.info(f"Scraping channel={ch} since {start_dt.isoformat()}")
                n = await scrape_channel(client, ch, start_dt, raw_dir, logger)
                total += n
            except Exception as e:
                logger.error(f"Channel scrape failed | channel={ch} err={e}")

        logger.info(f"Done. Total messages written: {total}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
