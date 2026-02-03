import json
from pathlib import Path

def salvage_objects(text: str):
    """Extract complete JSON objects from possibly-corrupted JSON array text."""
    objs = []
    depth = 0
    start = None

    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
            if depth == 0 and start is not None:
                chunk = text[start:i+1]
                try:
                    objs.append(json.loads(chunk))
                except Exception:
                    pass
                start = None

    return objs

def repair_file(fp: Path):
    text = fp.read_text(encoding="utf-8", errors="ignore")
    objs = salvage_objects(text)

    out = fp.with_name(fp.stem + "_repaired.json")
    out.write_text(json.dumps(objs, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"{fp} -> {out} | salvaged objects: {len(objs)}")

def main():
    base = Path("data/raw/telegram_messages")
    if not base.exists():
        raise FileNotFoundError(f"Missing folder: {base}")

    files = sorted(base.rglob("*.json"))
    if not files:
        print("No JSON files found.")
        return

    for fp in files:
        # Skip already repaired files
        if fp.name.endswith("_repaired.json"):
            continue
        repair_file(fp)

if __name__ == "__main__":
    main()
