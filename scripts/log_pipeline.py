import json
from pathlib import Path
from datetime import datetime, timezone, timedelta


BASE_DIR = Path(__file__).resolve().parent.parent
DIGEST_FILE = BASE_DIR / "data" / "processed" / "daily_digest.json"
LOGS_DIR = BASE_DIR / "data" / "logs"
ROLLING_WINDOW_DAYS = 30


def load_digest():
    with open(DIGEST_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_rolling_log(path):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"entries": []}


def save_log(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def prune_old_entries(entries, date_field="date"):
    cutoff = (
        datetime.now(timezone.utc).date() - timedelta(days=ROLLING_WINDOW_DAYS)
    ).isoformat()
    return [e for e in entries if e.get(date_field, "") >= cutoff]


def log_pipeline_run(digest, today):
    summary = digest.get("summary", {})
    enrichment = digest.get("enrichment_summary", {})
    sections = digest.get("sections", {})

    cross_section_count = sum(
        1
        for items in sections.values()
        for item in items
        if item.get("cross_section_refs")
    )

    run_log = {
        "date": today,
        "generated_at_utc": digest.get("generated_at_utc", ""),
        "total_raw_items": summary.get("total_raw_items", 0),
        "total_deduped_items": summary.get("total_deduped_items", 0),
        "selected_total": summary.get("selected_total", 0),
        "multi_source_selected_count": summary.get("multi_source_selected_count", 0),
        "cross_section_story_count": cross_section_count,
        "llm_enriched": enrichment.get("enriched", 0),
        "llm_fallback": enrichment.get("failed", 0),
        "theme_generated": bool(digest.get("theme_of_day", "")),
    }

    output_path = LOGS_DIR / f"pipeline_{today}.json"
    save_log(output_path, run_log)
    print(f"Run log saved: {output_path}")
    return run_log


def update_source_contribution(digest, today):
    path = LOGS_DIR / "source_contribution.json"
    log = load_rolling_log(path)
    entries = prune_old_entries(log.get("entries", []))

    sections = digest.get("sections", {})
    further = digest.get("further_reading", {})

    selected_sources = [
        item.get("source_name", "")
        for items in sections.values()
        for item in items
        if item.get("source_name")
    ]

    further_sources = [
        item.get("source_name", "")
        for items in further.values()
        for item in items
        if item.get("source_name")
    ]

    entries.append({
        "date": today,
        "selected": selected_sources,
        "further_reading": further_sources,
    })

    all_selected = [s for e in entries for s in e.get("selected", [])]
    source_counts = {}
    for s in all_selected:
        source_counts[s] = source_counts.get(s, 0) + 1

    log["entries"] = entries
    log["rolling_selected_totals"] = dict(
        sorted(source_counts.items(), key=lambda x: -x[1])
    )
    log["window_days"] = ROLLING_WINDOW_DAYS
    log["last_updated"] = today
    save_log(path, log)
    print(f"Source contribution log updated.")


def update_topic_distribution(digest, today):
    path = LOGS_DIR / "topic_distribution.json"
    log = load_rolling_log(path)
    entries = prune_old_entries(log.get("entries", []))

    sections = digest.get("sections", {})
    day_topics = {
        section_key: [item.get("topic_type", "general") for item in items]
        for section_key, items in sections.items()
    }

    entries.append({"date": today, "topics": day_topics})

    frequency = {}
    for e in entries:
        for section, topics in e.get("topics", {}).items():
            if section not in frequency:
                frequency[section] = {}
            for t in topics:
                frequency[section][t] = frequency[section].get(t, 0) + 1

    log["entries"] = entries
    log["rolling_frequency"] = frequency
    log["window_days"] = ROLLING_WINDOW_DAYS
    log["last_updated"] = today
    save_log(path, log)
    print(f"Topic distribution log updated.")


def update_multi_source_rate(digest, today):
    path = LOGS_DIR / "multi_source_rate.json"
    log = load_rolling_log(path)
    entries = prune_old_entries(log.get("entries", []))

    sections = digest.get("sections", {})
    all_items = [item for items in sections.values() for item in items]
    total = len(all_items)
    multi = sum(1 for item in all_items if item.get("source_count", 1) > 1)
    rate = round(multi / total, 3) if total else 0.0

    entries.append({
        "date": today,
        "total_selected": total,
        "multi_source_count": multi,
        "rate": rate,
    })

    rolling_rates = [e.get("rate", 0) for e in entries]
    avg_rate = round(sum(rolling_rates) / len(rolling_rates), 3) if rolling_rates else 0.0

    log["entries"] = entries
    log["rolling_avg_rate"] = avg_rate
    log["window_days"] = ROLLING_WINDOW_DAYS
    log["last_updated"] = today
    save_log(path, log)
    print(f"Multi-source rate log updated.")


def update_feed_health(today):
    raw_dir = BASE_DIR / "data" / "raw"
    path = LOGS_DIR / "feed_health.json"
    log = load_rolling_log(path)
    entries = prune_old_entries(log.get("entries", []))

    manifest_path = raw_dir / "_manifest.json"
    if not manifest_path.exists():
        print("Feed health: manifest not found, skipping.")
        return

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    failed_feeds = []
    healthy_feeds = []

    for raw_file in sorted(raw_dir.glob("*.json")):
        if raw_file.name == "_manifest.json":
            continue
        with open(raw_file, "r", encoding="utf-8") as f:
            feed_data = json.load(f)
        meta = feed_data.get("meta", {})
        if meta.get("failed"):
            failed_feeds.append({
                "source_name":    meta.get("source_name", ""),
                "category":       meta.get("category", ""),
                "feed_url":       meta.get("feed_url", ""),
                "failure_reason": meta.get("failure_reason", ""),
            })
        else:
            healthy_feeds.append(meta.get("source_name", ""))

    entries.append({
        "date":          today,
        "healthy_count": len(healthy_feeds),
        "failed_count":  len(failed_feeds),
        "failed_feeds":  failed_feeds,
    })

    persistent_failures = {}
    for entry in entries:
        for ff in entry.get("failed_feeds", []):
            name = ff.get("source_name", "")
            persistent_failures[name] = persistent_failures.get(name, 0) + 1

    log["entries"] = entries
    log["persistent_failures"] = dict(
        sorted(persistent_failures.items(), key=lambda x: -x[1])
    )
    log["window_days"] = ROLLING_WINDOW_DAYS
    log["last_updated"] = today

    save_log(path, log)

    if failed_feeds:
        print(f"Feed health: {len(failed_feeds)} failed feed(s) today:")
        for ff in failed_feeds:
            print(f"  - {ff['source_name']} ({ff['category']}): {ff['failure_reason']}")
    else:
        print("Feed health: all feeds returned entries.")
        

def main():
    today = datetime.now(timezone.utc).date().isoformat()

    if not DIGEST_FILE.exists():
        raise FileNotFoundError(f"Digest not found: {DIGEST_FILE}")

    digest = load_digest()

    log_pipeline_run(digest, today)
    update_source_contribution(digest, today)
    update_topic_distribution(digest, today)
    update_multi_source_rate(digest, today)

    print("All pipeline logs updated successfully.")


if __name__ == "__main__":
    main()
