import json
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parent.parent
DIGEST_FILE = BASE_DIR / "data" / "processed" / "daily_digest.json"
OUTPUT_FILE = BASE_DIR / "docs" / "index.html"


SECTION_LABELS = {
    "technology": "科技新聞",
    "politics": "政治新聞",
    "economy": "經濟新聞",
}


def load_digest():
    with open(DIGEST_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def format_generated_date(iso_string):
    if not iso_string:
        return "未知時間"
    try:
        dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso_string


def escape_html(text):
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def build_topic_card(item):
    title = escape_html(item.get("title", "未命名標題"))
    summary = escape_html(item.get("summary", ""))
    source_name = escape_html(item.get("source_name", "Unknown source"))
    link = escape_html(item.get("link", ""))
    published_at = escape_html(item.get("published_at_utc", ""))

    summary_html = f"<p>{summary}</p>" if summary else "<p>暫無摘要。</p>"
    time_html = f"<p><strong>時間：</strong>{published_at}</p>" if published_at else ""

    if link:
        title_html = f'<h3><a href="{link}" target="_blank" rel="noopener noreferrer">{title}</a></h3>'
    else:
        title_html = f"<h3>{title}</h3>"

    return f"""
    <article class="topic-card">
      {title_html}
      {summary_html}
      <p><strong>來源：</strong>{source_name}</p>
      {time_html}
    </article>
    """


def build_section(section_key, items):
    label = SECTION_LABELS.get(section_key, section_key)

    if not items:
        topics_html = "<p>今日暫無資料。</p>"
    else:
        topics_html = "\n".join(build_topic_card(item) for item in items)

    return f"""
    <section class="section-block">
      <h2>{label}</h2>
      {topics_html}
    </section>
    """


def build_page(digest):
    generated_at = format_generated_date(digest.get("generated_at_utc", ""))
    summary = digest.get("summary", {})
    sections = digest.get("sections", {})

    intro_html = f"""
    <section class="summary-box">
      <h2>今日總覽</h2>
      <p>本頁由 GitHub Actions 自動更新，整理科技、政治與經濟三大領域的代表性新聞。</p>
      <ul>
        <li>原始新聞總數：{summary.get("total_raw_items", 0)}</li>
        <li>去重後新聞數：{summary.get("total_deduped_items", 0)}</li>
        <li>最終入選主題數：{summary.get("selected_total", 0)}</li>
      </ul>
    </section>
    """

    sections_html = "\n".join(
        build_section(section, sections.get(section, []))
        for section in ["technology", "politics", "economy"]
    )

    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>News-Claw | 每日新聞簡報</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans TC", "PingFang TC", "Microsoft JhengHei", sans-serif;
      line-height: 1.75;
      max-width: 980px;
      margin: 0 auto;
      padding: 32px 20px 60px;
      color: #1f2937;
      background: #f8fafc;
    }}
    h1, h2, h3 {{
      color: #111827;
    }}
    h1 {{
      margin-bottom: 8px;
    }}
    .meta {{
      color: #6b7280;
      margin-bottom: 28px;
    }}
    .summary-box,
    .section-block {{
      background: #ffffff;
      border: 1px solid #e5e7eb;
      border-radius: 14px;
      padding: 22px;
      margin-bottom: 24px;
      box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
    }}
    .topic-card {{
      padding: 18px 0;
      border-top: 1px solid #e5e7eb;
    }}
    .topic-card:first-of-type {{
      border-top: none;
      padding-top: 6px;
    }}
    p {{
      margin: 10px 0;
    }}
    ul {{
      padding-left: 22px;
    }}
    a {{
      color: #2563eb;
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
    footer {{
      margin-top: 36px;
      color: #6b7280;
      font-size: 14px;
    }}
  </style>
</head>
<body>
  <header>
    <h1>News-Claw</h1>
    <p class="meta">每日新聞簡報｜科技、政治、經濟｜最後更新：{generated_at}</p>
    <p>此頁根據 RSS 來源自動整理每日代表性新聞，並由 GitHub Pages 發布。</p>
  </header>

  {intro_html}

  {sections_html}

  <footer>
    <p>News-Claw automated daily briefing prototype.</p>
  </footer>
</body>
</html>
"""


def main():
    digest = load_digest()
    html = build_page(digest)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Site generated: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
