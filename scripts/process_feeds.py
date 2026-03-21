import json
import re
from pathlib import Path
from datetime import datetime, timezone
from html import unescape


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_FILE = PROCESSED_DIR / "daily_digest.json"

TARGET_SECTIONS = ["technology", "politics", "economy"]
MAX_TOPICS_PER_SECTION = 2


def load_raw_files():
    items = []
    raw_files = sorted(RAW_DIR.glob("*.json"))

    for file_path in raw_files:
        if file_path.name == "_manifest.json":
            continue

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        source_items = data.get("items", [])
        for item in source_items:
            items.append(item)

    return items


def clean_text(text):
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalise_title(title):
    title = clean_text(title).lower()
    title = re.sub(r"[^a-z0-9\u4e00-\u9fff\s]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def dedupe_items(items):
    seen = set()
    deduped = []

    for item in items:
        title = item.get("title", "")
        link = item.get("link", "")
        norm_title = normalise_title(title)

        dedupe_key = (item.get("category", ""), norm_title)

        if not norm_title:
            continue

        if dedupe_key in seen:
            continue

        seen.add(dedupe_key)

        cleaned_item = {
            "category": item.get("category", ""),
            "source_name": item.get("source_name", ""),
            "title": clean_text(title),
            "link": link,
            "summary": clean_text(item.get("summary", "")),
            "published": item.get("published", ""),
            "updated": item.get("updated", ""),
            "id": item.get("id", ""),
        }
        deduped.append(cleaned_item)

    return deduped


def sort_items(items):
    def sort_key(item):
        published = item.get("published", "")
        updated = item.get("updated", "")
        title = item.get("title", "")
        return (published or updated or "", title)

    return sorted(items, key=sort_key, reverse=True)


def select_top_items_by_section(items):
    sections = {section: [] for section in TARGET_SECTIONS}

    for section in TARGET_SECTIONS:
        section_items = [item for item in items if item.get("category") == section]
        section_items = sort_items(section_items)
        sections[section] = section_items[:MAX_TOPICS_PER_SECTION]

    return sections


def build_digest(raw_items, deduped_items, sections):
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_raw_items": len(raw_items),
            "total_deduped_items": len(deduped_items),
            "selected_total": sum(len(v) for v in sections.values()),
            "max_topics_per_section": MAX_TOPICS_PER_SECTION,
        },
        "sections": sections,
    }


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    raw_items = load_raw_files()
    deduped_items = dedupe_items(raw_items)
    sections = select_top_items_by_section(deduped_items)
    digest = build_digest(raw_items, deduped_items, sections)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)

    print("Daily digest generated.")
    print(json.dumps(digest["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
