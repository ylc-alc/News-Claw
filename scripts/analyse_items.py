"""
analyse_items.py

Enriches each selected news item in daily_digest.json with deep,
article-specific analysis generated via GitHub Models (GPT-4o-mini).

Runs AFTER process_feeds.py has selected the top stories, so LLM calls
are limited to exactly the items that appear on the published page.
Falls back silently to the existing template briefing on any failure.
"""

import json
import os
import time
from pathlib import Path

try:
    from openai import OpenAI
except ImportError:
    raise SystemExit("ERROR: openai package not installed. Add 'openai>=1.30.0' to requirements.txt.")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DIGEST_FILE = BASE_DIR / "data" / "processed" / "daily_digest.json"

# ---------------------------------------------------------------------------
# GitHub Models configuration
# ---------------------------------------------------------------------------
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

# gpt-4o-mini: free tier allows 15 req/min and 150 req/day for GitHub Free/Pro.
# With MAX_TOPICS_PER_SECTION=3 and 3 sections, we send at most 9 calls per run.
# Check https://github.com/marketplace/models for the current model catalogue.
MODEL = "gpt-4o-mini"
GITHUB_MODELS_BASE_URL = "https://models.inference.ai.azure.com"

# Seconds to sleep between API calls to stay well under the rate limit.
INTER_CALL_DELAY = 5

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """你是一位專業的國際新聞分析導師，專精於科技、政治與經濟議題。
你的任務是根據所提供的新聞標題與 RSS 摘要，撰寫結構清晰、冷靜專業的深度分析。

請以 JSON 格式回覆，格式如下（不要加任何額外說明）：
{
  "news_focus_zh": "（100字以內）以繁體中文精確摘述本則新聞的核心事件與關鍵訊息。",
  "background": "（150字以內）提供這則新聞的結構性背景，包括政策脈絡、產業歷史、地緣關係或市場趨勢。須針對本則具體事件，而非泛泛而論。",
  "analysis": "（150字以內）分析各方利益關係與角力動態，說明這則新聞對不同利益方的影響、意涵與潛在後續發展。",
  "stakeholders": ["利益方1", "利益方2", "利益方3"]
}

分析原則：
- 僅依據所提供的標題與摘要進行推論，不捏造事實
- 不使用陰謀論、煽情語言或模糊表述
- 保持專業、直接、客觀的繁體中文語氣
- stakeholders 為字串陣列，最多列出五個最關鍵的利益方
- 若此議題在不同政府、市場或觀察方之間存在重大爭議或相左詮釋，請在 analysis 欄位中明確指出各方立場差異，避免僅呈現單一視角"""


def build_user_prompt(item: dict) -> str:
    category_map = {
        "technology": "科技",
        "politics": "政治",
        "economy": "經濟",
    }
    category_zh = category_map.get(item.get("category", ""), item.get("category", ""))
    title = item.get("title", "").strip()
    summary = item.get("summary", "").strip()
    source = item.get("source_name", "").strip()

    # If the summary is identical to the title or empty, say so explicitly
    # so the model does not hallucinate extra detail.
    if not summary or summary.lower() == title.lower():
        summary_line = "（無獨立摘要，請僅依標題進行分析）"
    else:
        summary_line = summary

    return (
        f"類別：{category_zh}\n"
        f"來源：{source}\n"
        f"標題：{title}\n"
        f"摘要：{summary_line}\n\n"
        "請針對以上新聞提供深度分析，以 JSON 格式回覆。"
    )


# ---------------------------------------------------------------------------
# API call with retry
# ---------------------------------------------------------------------------
def call_github_models(client: "OpenAI", item: dict, max_retries: int = 2) -> dict | None:
    """
    Calls GitHub Models and returns a parsed dict with the four analysis keys.
    Returns None on permanent failure so the caller can fall back to templates.
    """
    prompt = build_user_prompt(item)

    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.3,       # Low temperature for factual, consistent output
                max_tokens=700,
                response_format={"type": "json_object"},
            )

            raw_text = response.choices[0].message.content or ""
            parsed = json.loads(raw_text)

            required_keys = {"news_focus_zh", "background", "analysis", "stakeholders"}
            missing = required_keys - parsed.keys()
            if missing:
                raise ValueError(f"LLM response missing keys: {missing}")

            # Sanitise: ensure stakeholders is always a list of strings
            stakeholders = parsed.get("stakeholders", [])
            if not isinstance(stakeholders, list):
                stakeholders = [str(stakeholders)]
            parsed["stakeholders"] = [str(s) for s in stakeholders][:5]

            return parsed

        except Exception as exc:
            if attempt < max_retries:
                wait = 4 * (attempt + 1)   # 4s, 8s
                print(f"    Attempt {attempt + 1} failed ({exc}). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    All {max_retries + 1} attempts failed: {exc}. Keeping template briefing.")
                return None


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------
def enrich_digest(digest: dict, client: "OpenAI") -> dict:
    sections = digest.get("sections", {})
    all_items = [
        (section_key, item)
        for section_key, items in sections.items()
        for item in items
    ]

    total = len(all_items)
    enriched = 0
    failed = 0

    print(f"\nStarting LLM enrichment: {total} item(s) via {MODEL}.")

    for idx, (section_key, item) in enumerate(all_items):
        title_preview = item.get("title", "(no title)")[:70]
        print(f"\n[{idx + 1}/{total}] [{section_key}] {title_preview}...")

        result = call_github_models(client, item)

        if result:
            briefing = item.setdefault("briefing", {})
            briefing["news_focus_zh"] = result["news_focus_zh"]
            briefing["background"]    = result["background"]
            briefing["analysis"]      = result["analysis"]
            briefing["stakeholders"]  = result["stakeholders"]
            item["briefing"] = briefing
            enriched += 1
            print(f"    Enriched.")
        else:
            failed += 1
            print(f"    Template fallback retained.")

        # Pause between calls; skip pause after the final item
        if idx < total - 1:
            time.sleep(INTER_CALL_DELAY)

    digest["sections"] = sections
    digest["enrichment_summary"] = {
        "model":    MODEL,
        "enriched": enriched,
        "failed":   failed,
        "total":    total,
    }
    return digest


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    if not GITHUB_TOKEN:
        print("WARNING: GITHUB_TOKEN is not set. Skipping LLM enrichment.")
        print("The site will still build using template-based analysis.")
        return

    if not DIGEST_FILE.exists():
        raise FileNotFoundError(
            f"Digest file not found: {DIGEST_FILE}\n"
            "Ensure process_feeds.py has run successfully before this script."
        )

    client = OpenAI(
        base_url=GITHUB_MODELS_BASE_URL,
        api_key=GITHUB_TOKEN,
    )

    with open(DIGEST_FILE, "r", encoding="utf-8") as f:
        digest = json.load(f)

    digest = enrich_digest(digest, client)

    with open(DIGEST_FILE, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)

    summary = digest.get("enrichment_summary", {})
    print(
        f"\nEnrichment complete: "
        f"{summary.get('enriched', 0)} enriched, "
        f"{summary.get('failed', 0)} fallback, "
        f"{summary.get('total', 0)} total."
    )


if __name__ == "__main__":
    main()
