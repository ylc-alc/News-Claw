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

你的任務是根據所提供的主新聞與參考報導，撰寫結構清晰、冷靜專業的深度分析。

請以 JSON 格式回覆，格式如下（不要加任何額外說明）：
{
  "news_focus_zh": "（120至160字）以繁體中文精確說明本則新聞實際發生了什麼，交代關鍵動作、回應或結果。",
  "background": "（150至220字）提供與本則新聞直接相關的背景脈絡，包括政策、產業、地緣或市場背景。必須緊扣本則事件，不可泛泛而論。",
  "analysis": "（180至260字）分析各方利益關係、角力動態與後續可能發展。若參考報導提供補充細節或不同重點，請明確指出。",
  "stakeholders": ["利益方1", "利益方2", "利益方3"]
}

分析原則：
- 僅依據所提供的標題與摘要進行推論，不捏造事實
- 若主新聞未交代某個細節，但參考報導有補充，可在分析中明確說明是參考報導提供的補充
- 若不同報導的重點不同，請指出差異，但不要誇大矛盾
- 不要引入輸入資料中沒有出現的國家、人物、機構、動機、數字或因果關係
- 不使用陰謀論、煽情語言或模糊表述
- 保持專業、直接、客觀的繁體中文語氣
- stakeholders 為字串陣列，最多列出五個最關鍵的利益方
- 若此議題在不同政府、市場或觀察方之間存在重大爭議或相左詮釋，請在 analysis 欄位中明確指出各方立場差異，避免僅呈現單一視角
- 禁止將美國、英國、歐盟籠統歸為「西方陣營」；若原始報導分別提及各方立場，須逐一明確說明"""

def build_user_prompt(item: dict) -> str:
    category_map = {
        "technology": "科技",
        "politics": "政治",
        "economy": "經濟",
    }

    def clean_summary(summary_text: str, title_text: str) -> str:
        summary_text = (summary_text or "").strip()
        title_text = (title_text or "").strip()

        if not summary_text or summary_text.lower() == title_text.lower():
            return "（無獨立摘要，請僅依標題進行分析）"
        return summary_text

    category_zh = category_map.get(item.get("category", ""), item.get("category", ""))
    title = item.get("title", "").strip()
    summary = item.get("summary", "").strip()
    source = item.get("source_name", "").strip()

    summary_line = clean_summary(summary, title)

    supporting_sources = item.get("supporting_sources", [])[:2]
    supporting_titles = item.get("supporting_titles", [])[:2]
    supporting_summaries = item.get("supporting_summaries", [])[:2]

    supporting_blocks = []
    for i in range(len(supporting_titles)):
        sup_source = supporting_sources[i] if i < len(supporting_sources) else ""
        sup_title = supporting_titles[i] if i < len(supporting_titles) else ""
        sup_summary = supporting_summaries[i] if i < len(supporting_summaries) else ""
        sup_summary_line = clean_summary(sup_summary, sup_title)

        supporting_blocks.append(
            f"參考報導{i + 1}來源：{sup_source}\n"
            f"參考報導{i + 1}標題：{sup_title}\n"
            f"參考報導{i + 1}摘要：{sup_summary_line}"
        )

    if supporting_blocks:
        supporting_text = "\n\n".join(supporting_blocks)
    else:
        supporting_text = "（無參考報導）"

    is_major_update = item.get("is_major_update", False)
    update_instruction = (
        "\n\n⚠️ 此新聞已被標記為重大進展。請在 analysis 欄位中明確說明："
        "與此議題的先前已知狀況相比，本次具體新增或升級了哪些內容。"
        "避免重複背景資訊，聚焦於本次變化本身。"
    ) if is_major_update else ""

    return (
        f"類別：{category_zh}\n\n"
        f"【主新聞】\n"
        f"來源：{source}\n"
        f"標題：{title}\n"
        f"摘要：{summary_line}\n\n"
        f"【參考報導】\n"
        f"{supporting_text}"
        f"{update_instruction}\n\n"
        "寫作要求：\n"
        "1. 先清楚說明主新聞實際發生了什麼。\n"
        "2. 若參考報導提供補充細節，請明確指出補充了什麼。\n"
        "3. 若不同報導的重點不同，請指出差異。\n"
        "4. 不要加入輸入中沒有出現的國家、人物、機構、動機或數字。\n"
        "5. 請以 JSON 格式回覆。"
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
def generate_theme_of_day(client: "OpenAI", digest: dict) -> str:
    section_labels = {"technology": "科技", "politics": "政治", "economy": "經濟"}
    lines = []

    for section_key, items in digest.get("sections", {}).items():
        label = section_labels.get(section_key, section_key)
        for item in items:
            title = item.get("title", "").strip()
            focus = item.get("briefing", {}).get("news_focus_zh", "").strip()
            if title:
                lines.append(f"【{label}】{title}：{focus}" if focus else f"【{label}】{title}")

    if not lines:
        return ""

    user_prompt = "以下是今日簡報選題摘要：\n\n" + "\n".join(lines) + "\n\n請撰寫今日編輯按語。"

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": THEME_SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.4,
            max_tokens=220,
        )
        theme = response.choices[0].message.content.strip()
        print(f"    Theme generated: {theme[:80]}...")
        return theme
    except Exception as exc:
        print(f"    Theme generation failed: {exc}. Skipping.")
        return ""
        

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

    print("\nGenerating theme of day...")
    theme = generate_theme_of_day(client, digest)
    if theme:
        digest["theme_of_day"] = theme

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
