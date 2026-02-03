import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import Message
from dotenv import load_dotenv
import os
import tempfile

# Load environment variables
load_dotenv()

# Telegram credentials
API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("TELEGRAM_SESSION", "medical_warehouse")

# Channels to scrape
CHANNELS = [c.strip() for c in os.getenv("CHANNELS", "").split(",") if c.strip()]
SCRAPE_DAYS_BACK = int(os.getenv("SCRAPE_DAYS_BACK", "30"))

# Paths
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))


def slugify(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    return re.sub(r"_+", "_", name)


def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("telegram_scraper")
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(LOG_DIR / "scraper.log", encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)

    return logger


def message_to_dict(message, channel: str, image_path: str | None):
    return {
        "message_id": message.id,
        "channel_name": channel,
        "message_date": message.date.isoformat() if message.date else None,
        "message_text": message.message or "",
        "has_media": bool(message.media),
        "image_path": image_path,
        "views": message.views or 0,
        "forwards": message.forwards or 0,

        # Keep ONLY minimal raw metadata that is JSON-safe
        "raw_meta": {
            "grouped_id": getattr(message, "grouped_id", None),
            "reply_to_msg_id": getattr(getattr(message, "reply_to", None), "reply_to_msg_id", None),
            "edit_date": message.edit_date.isoformat() if getattr(message, "edit_date", None) else None,
            "media_type": type(message.media).__name__ if message.media else None,
        }
    }



async def scrape_channel(client, channel, start_date, logger):
    channel_slug = slugify(channel)
    messages_by_day = {}

    entity = await client.get_entity(channel)

    async for msg in client.iter_messages(entity):
        if not msg.date:
            continue

        if msg.date < start_date:
            break

        day = msg.date.astimezone(timezone.utc).date().isoformat()
        image_path = None

        if msg.photo:
            img_dir = RAW_DATA_DIR / "images" / channel_slug
            img_dir.mkdir(parents=True, exist_ok=True)
            img_file = img_dir / f"{msg.id}.jpg"
            await client.download_media(msg.photo, file=img_file)
            image_path = str(img_file)

        record = message_to_dict(msg, channel_slug, image_path)
        messages_by_day.setdefault(day, []).append(record)

    for day, records in messages_by_day.items():
        out_dir = RAW_DATA_DIR / "telegram_messages" / day
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{channel_slug}.json"

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {len(records)} messages to {out_file}")


async def main():
    logger = setup_logger()

    if not CHANNELS:
        raise ValueError("CHANNELS is empty. Check your .env file.")

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    start_date = datetime.now(timezone.utc) - timedelta(days=SCRAPE_DAYS_BACK)

    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        for channel in CHANNELS:
            try:
                logger.info(f"Scraping channel: {channel}")
                await scrape_channel(client, channel, start_date, logger)
            except Exception as e:
                logger.error(f"Error scraping {channel}: {e}")
def safe_write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=path.parent,
        suffix=".tmp"
    ) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)

    Path(tmp.name).replace(path)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
