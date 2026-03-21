import json
from pathlib import Path
from datetime import datetime, timezone
import feedparser


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "feeds.json"
OUTPUT_DIR = BASE_DIR / "data" / "raw"


def load_feeds_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_get(entry, field, default=""):
    value = entry.get(field, default)
    if value is None:
        return default
    return value


def parse_feed(category, source):
    feed_url = source["url"]
    feed_name = source["name"]

    parsed = feedparser.parse(feed_url)

    items = []
    for entry in parsed.entries:
        item = {
            "category": category,
            "source_name": feed_name,
            "feed_url": feed_url,
            "title": safe_get(entry, "title"),
            "link": safe_get(entry, "link"),
            "summary": safe_get(entry, "summary"),
            "published": safe_get(entry, "published"),
            "updated": safe_get(entry, "updated"),
            "id": safe_get(entry, "id"),
        }
        items.append(item)

    feed_meta = {
        "category": category,
        "source_name": feed_name,
        "feed_url": feed_url,
        "feed_title": safe_get(parsed.feed, "title"),
        "feed_link": safe_get(parsed.feed, "link"),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "entry_count": len(items),
        "bozo": getattr(parsed, "bozo", 0),
    }

    return {
        "meta": feed_meta,
        "items": items,
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    feeds_config = load_feeds_config()
    all_results = []

    for category, sources in feeds_config.items():
        for source in sources:
            result = parse_feed(category, source)
            all_results.append(result)

            safe_name = source["name"].lower().replace(" ", "-").replace("/", "-")
            output_file = OUTPUT_DIR / f"{category}__{safe_name}.json"

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_files": [f"{r['meta']['category']} | {r['meta']['source_name']}" for r in all_results],
        "total_sources": len(all_results),
        "total_items": sum(r["meta"]["entry_count"] for r in all_results),
    }

    with open(OUTPUT_DIR / "_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print("RSS fetch complete.")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
