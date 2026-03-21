import json
import re
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from email.utils import parsedate_to_datetime


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_FILE = PROCESSED_DIR / "daily_digest.json"

TARGET_SECTIONS = ["technology", "politics", "economy"]
MAX_TOPICS_PER_SECTION = 3

TECH_KEYWORDS = {
    "ai": ["ai", "artificial intelligence", "openai", "chatgpt", "model", "llm", "nvidia", "anthropic", "gemini"],
    "chips": ["chip", "chips", "semiconductor", "tsmc", "intel", "amd", "qualcomm", "gpu", "foundry"],
    "platforms": ["meta", "x", "tiktok", "google", "apple", "microsoft", "amazon", "platform", "social media"],
    "cybersecurity": ["cyber", "security", "hack", "breach", "malware", "ransomware"],
    "devices": ["iphone", "smartphone", "device", "laptop", "hardware", "wearable"],
    "regulation": ["antitrust", "regulator", "regulation", "eu", "commission", "fine", "lawsuit"],
}

POLITICS_KEYWORDS = {
    "us_china": ["china", "beijing", "washington", "u.s.", "us ", "tariff", "trade war"],
    "europe": ["eu", "european union", "brussels", "commission", "nato"],
    "uk": ["uk", "britain", "british", "london", "westminster"],
    "taiwan": ["taiwan", "taipei", "strait"],
    "election": ["election", "vote", "poll", "campaign", "parliament"],
    "security": ["war", "missile", "military", "defence", "defense", "security", "attack"],
    "diplomacy": ["sanction", "summit", "diplomatic", "talks", "ceasefire", "minister"],
}

ECONOMY_KEYWORDS = {
    "rates": ["interest rate", "rates", "federal reserve", "ecb", "boe", "central bank"],
    "inflation": ["inflation", "cpi", "prices", "consumer prices"],
    "jobs": ["jobs", "employment", "unemployment", "labour", "labor", "wages"],
    "energy": ["oil", "gas", "energy", "opec", "crude"],
    "supply_chain": ["supply chain", "shipping", "logistics", "factory", "manufacturing"],
    "markets": ["stocks", "market", "bonds", "investors", "shares"],
    "corporate": ["earnings", "revenue", "profit", "forecast", "sales"],
}

LOW_SIGNAL_PATTERNS = [
    "visits",
    "inside",
    "how ",
    "feature",
    "lifestyle",
    "animal",
    "food waste",
    "travel",
    "culture",
]


def load_raw_files():
    items = []
    raw_files = sorted(RAW_DIR.glob("*.json"))

    for file_path in raw_files:
        if file_path.name == "_manifest.json":
            continue

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data.get("items", []):
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


def tokenise_title(title):
    text = normalise_title(title)
    tokens = [t for t in text.split() if len(t) >= 3]
    return tokens
    

def title_overlap_score(title_a, title_b):
    tokens_a = set(tokenise_title(title_a))
    tokens_b = set(tokenise_title(title_b))

    if not tokens_a or not tokens_b:
        return 0.0

    overlap = tokens_a.intersection(tokens_b)
    smaller_size = min(len(tokens_a), len(tokens_b))

    if smaller_size == 0:
        return 0.0

    return len(overlap) / smaller_size


def parse_datetime(value):
    if not value:
        return None
    value = value.strip()

    try:
        dt = parsedate_to_datetime(value)
        if dt is not None:
            return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    return None


def best_datetime(item):
    return parse_datetime(item.get("published", "")) or parse_datetime(item.get("updated", ""))


def keyword_score(text, keyword_map, title_weight=3, summary_weight=1):
    scores = {topic_type: 0 for topic_type in keyword_map.keys()}
    title = text["title"]
    summary = text["summary"]

    for topic_type, keywords in keyword_map.items():
        for keyword in keywords:
            if keyword in title:
                scores[topic_type] += title_weight
            if keyword in summary:
                scores[topic_type] += summary_weight

    return scores


def detect_topic_type(category, title, summary):
    title_l = title.lower()
    summary_l = summary.lower()

    text = {"title": title_l, "summary": summary_l}

    if category == "technology":
        scores = keyword_score(text, TECH_KEYWORDS)
    elif category == "politics":
        scores = keyword_score(text, POLITICS_KEYWORDS)
    elif category == "economy":
        scores = keyword_score(text, ECONOMY_KEYWORDS)
    else:
        return "general"

    best_topic = "general"
    best_score = 0

    for topic_type, score in scores.items():
        if score > best_score:
            best_topic = topic_type
            best_score = score

    return best_topic if best_score > 0 else "general"


def contains_any(text, keywords):
    return any(keyword in text for keyword in keywords)


def detect_event_type(category, topic_type, title, summary):
    text = f"{title} {summary}".lower()

    launch_words = ["launch", "launches", "launched", "unveil", "unveils", "release", "releases", "rolls out", "introduces"]
    regulation_words = ["regulator", "regulation", "fine", "fines", "antitrust", "lawsuit", "investigation", "ban", "bans", "blocks", "approves"]
    partnership_words = ["partner", "partners", "partnership", "deal", "agreement", "ties up", "collaboration"]
    earnings_words = ["earnings", "revenue", "profit", "forecast", "sales", "quarter", "results"]
    rate_words = ["interest rate", "rates", "federal reserve", "ecb", "boe", "central bank", "rate decision", "holds rates", "cuts rates", "raises rates"]
    inflation_words = ["inflation", "cpi", "consumer prices", "price growth"]
    market_words = ["stocks", "shares", "markets", "bonds", "investors", "selloff", "rally"]
    energy_words = ["oil", "gas", "opec", "crude", "output", "energy"]
    election_words = ["election", "vote", "poll", "campaign", "parliament"]
    security_words = ["war", "missile", "military", "attack", "defence", "defense", "security", "troops"]
    diplomacy_words = ["summit", "talks", "ceasefire", "sanction", "minister", "diplomatic", "meeting"]

    if category == "technology":
        if contains_any(text, regulation_words):
            return "regulation_action"
        if contains_any(text, partnership_words):
            return "partnership_move"
        if contains_any(text, launch_words):
            return "product_launch"
        if contains_any(text, earnings_words):
            return "commercial_update"
        return "strategic_move"

    if category == "politics":
        if contains_any(text, election_words):
            return "election_move"
        if contains_any(text, security_words):
            return "security_event"
        if contains_any(text, diplomacy_words):
            return "diplomatic_move"
        if contains_any(text, regulation_words):
            return "policy_action"
        return "political_signal"

    if category == "economy":
        if contains_any(text, rate_words):
            return "rate_decision"
        if contains_any(text, inflation_words):
            return "inflation_data"
        if contains_any(text, energy_words):
            return "energy_supply_move"
        if contains_any(text, earnings_words):
            return "corporate_results"
        if contains_any(text, market_words):
            return "market_repricing"
        return "macro_signal"

    return "general_event"
    

def compute_relevance_score(category, topic_type, title, summary, source_name):
    title_l = title.lower()
    summary_l = summary.lower()
    blob = f"{title_l} {summary_l}"

    score = 0

    # Base topical relevance
    if topic_type != "general":
        score += 8
    else:
        score += 2

    # Stronger score for clearer hard-news style items
    strong_terms = [
        "announces", "launches", "cuts", "raises", "approves", "blocks",
        "ban", "tariff", "election", "interest rate", "inflation",
        "earnings", "profit", "security", "attack", "sanction"
    ]
    for term in strong_terms:
        if term in blob:
            score += 2

    # Down-rank soft / feature-like items
    for term in LOW_SIGNAL_PATTERNS:
        if term in blob:
            score -= 4

    # Slight boost for Reuters hard-news style
    if "reuters" in source_name.lower():
        score += 2

    # Section-specific relevance boosts
    if category == "technology" and topic_type in {"ai", "chips", "cybersecurity", "regulation", "platforms"}:
        score += 3

    if category == "politics" and topic_type in {"us_china", "security", "election", "diplomacy", "taiwan"}:
        score += 3

    if category == "economy" and topic_type in {"rates", "inflation", "energy", "markets", "corporate"}:
        score += 3

    return score


def get_topic_priority_score(category, topic_type):
    priority_map = {
        "technology": {
            "chips": 10,
            "ai": 9,
            "regulation": 8,
            "cybersecurity": 8,
            "platforms": 7,
            "devices": 5,
            "general": 3,
        },
        "politics": {
            "security": 10,
            "us_china": 9,
            "uk": 9,
            "election": 8,
            "europe": 8,
            "taiwan": 7,
            "diplomacy": 7,
            "general": 3,
        },
        "economy": {
            "rates": 10,
            "inflation": 9,
            "energy": 8,
            "markets": 7,
            "corporate": 6,
            "jobs": 5,
            "supply_chain": 5,
            "general": 3,
        },
    }

    return priority_map.get(category, {}).get(topic_type, 3)


def build_event_background(category, topic_type, event_type):
    backgrounds = {
        "technology": {
            "product_launch": "這則消息反映科技公司正持續透過新產品、新模型或新服務爭奪市場注意力與商業落地機會。近期競爭已不只是功能展示，更是平台整合、客戶導入與變現效率的比拼。",
            "regulation_action": "這則消息延續近年各國政府對大型科技公司、市場支配力、資料治理與平台責任的監管趨勢。對企業而言，監管風向正逐步成為與產品競爭同等重要的變數。",
            "partnership_move": "這則消息反映科技公司正透過合作、整合或生態系聯盟擴大市場位置。近來競爭已從單一產品能力，延伸到誰能串接更多企業客戶、資料資源與平台場景。",
            "commercial_update": "這則消息顯示科技競爭正進一步回到商業基本面，包括營收成長、成本控制與資本支出效率。市場不再只看技術敘事，也更重視獲利與執行能力。",
            "strategic_move": "這則消息反映科技產業仍處於快速重組階段，企業正透過策略調整來回應市場競爭、資本壓力與政策環境變化。",
        },
        "politics": {
            "election_move": "這則消息反映政治競爭正進入民意、政策與權力重組的關鍵階段。選舉相關動向通常不只影響國內政治，也會改變市場對後續政策方向的預期。",
            "security_event": "這則消息反映安全局勢仍是國際政治的主軸之一。這類事件通常不只涉及軍事風險本身，也牽動外交回應、能源價格與盟友協調。",
            "diplomatic_move": "這則消息顯示各方仍在透過外交互動、談判與政策訊號調整彼此立場。對外界而言，重點往往不只是會談本身，而是後續可執行的政策空間。",
            "policy_action": "這則消息反映政府正透過制度、政策或限制性措施回應政治壓力與外部局勢。這類行動通常同時具有內政管理與對外訊號的雙重意義。",
            "political_signal": "這則消息顯示政治環境正在釋出新的方向訊號，可能影響後續政策議程、盟友互動與市場判斷。",
        },
        "economy": {
            "rate_decision": "這則消息延續市場對央行政策路徑的高度敏感。利率相關訊號通常不只影響借貸成本，也會重塑投資人對成長、通膨與資產價格的預期。",
            "inflation_data": "這則消息反映價格壓力仍是市場與政策制定者關注的核心。通膨數據往往會直接影響央行政策預期、家庭購買力與企業成本結構。",
            "energy_supply_move": "這則消息顯示能源供應與價格仍深受地緣政治、產量決策與需求預期影響。能源變動往往進一步外溢至通膨與市場風險偏好。",
            "corporate_results": "這則消息反映企業端正透過財報與前景指引釋放對需求、成本與市場環境的判斷。對投資人而言，重點往往在於結果背後的趨勢訊號。",
            "market_repricing": "這則消息顯示市場正重新調整對利率、成長或風險事件的定價。資產價格波動往往反映的不只是單一事件，而是整體預期的再平衡。",
            "macro_signal": "這則消息反映總體經濟環境仍在調整中，市場與政策制定者正在重新評估成長、通膨與風險之間的平衡。",
        }
    }

    category_map = backgrounds.get(category, {})
    return category_map.get(event_type, "這則消息反映該領域近期正在出現新的結構性變化，值得放回更大的政策、競爭與市場脈絡中理解。")


def build_event_analysis(category, topic_type, event_type):
    analyses = {
        "technology": {
            "product_launch": "這次角力焦點不只在產品本身，而在誰能更快把新能力轉化為實際用戶成長、企業採用與商業收入，同時避免成本失控。",
            "regulation_action": "這次角力的核心在於企業能否維持成長與市場控制力，同時降低監管風險與合規成本。監管機構則希望重新界定技術平台的責任邊界。",
            "partnership_move": "這次角力不只是合作是否成立，而在誰能透過聯盟取得更大的客戶入口、資料優勢與生態系控制力。",
            "commercial_update": "這次市場真正關注的不是單一數字，而是企業能否同時維持成長敘事、改善獲利能力，並證明先前投入開始轉化為可持續回報。",
            "strategic_move": "這次角力重點在於企業如何在競爭升級、監管壓力與資本市場要求之間重新調整策略節奏。",
        },
        "politics": {
            "election_move": "這次角力的核心在於誰能把議題主導權轉化為實際政治動員，並在選後取得足夠的政策正當性與執行空間。",
            "security_event": "這次角力重點不只是事件本身，而在各方如何控制升高風險、釋放威懾訊號，並避免局勢失控帶來更高政治與經濟成本。",
            "diplomatic_move": "這次角力在於各方是否真願意交換籌碼，還是僅透過外交表態爭取時間、盟友支持或輿論優勢。",
            "policy_action": "這次角力重點在於政策工具是否足以改變對手行為，同時又不會對本國經濟、企業或政治支持造成過高反噬。",
            "political_signal": "這次真正需要觀察的，是這些政治訊號是否會進一步轉化為具體政策、制度變動或外交行動。",
        },
        "economy": {
            "rate_decision": "這次市場關注的核心不只是利率是否調整，而是央行如何描述後續路徑。真正的角力在於通膨風險與成長放緩風險，哪一方更值得被優先處理。",
            "inflation_data": "這次角力焦點在於價格壓力究竟是暫時波動，還是仍足以改變央行政策節奏。對市場而言，數據背後的政策含義比數字本身更重要。",
            "energy_supply_move": "這次角力重點在於供應控制、價格穩定與地緣政治目標是否能同時兼顧。能源市場的變化也會迅速傳導到通膨與風險資產定價。",
            "corporate_results": "這次角力不只是企業是否達標，而在管理層如何說明需求前景、成本壓力與未來投資節奏，這些訊號將直接影響市場信心。",
            "market_repricing": "這次市場波動反映的是預期重新定價。真正關鍵在於投資人是否相信原有的利率、成長或風險假設仍然成立。",
            "macro_signal": "這次角力核心在於政策制定者、企業與市場參與者如何重新校準對成長與風險的判斷。",
        }
    }

    category_map = analyses.get(category, {})
    return category_map.get(event_type, "這次角力的重點在於各方如何在政策、競爭與市場壓力之間重新分配風險與資源。")


def build_event_stakeholders(category, topic_type, event_type):
    stakeholder_map = {
        "technology": {
            "product_launch": ["科技公司", "企業客戶", "開發者", "投資人"],
            "regulation_action": ["科技公司", "監管機構", "競爭對手", "消費者"],
            "partnership_move": ["科技公司", "合作夥伴", "企業客戶", "投資人"],
            "commercial_update": ["企業管理層", "投資人", "客戶", "供應鏈"],
            "strategic_move": ["科技公司", "投資人", "監管機構", "使用者"],
        },
        "politics": {
            "election_move": ["執政黨", "反對黨", "選民", "市場"],
            "security_event": ["相關政府", "軍事同盟", "國際組織", "市場"],
            "diplomatic_move": ["相關國家政府", "盟友", "企業", "市場"],
            "policy_action": ["政府", "企業", "盟友", "公眾"],
            "political_signal": ["政府", "政黨", "盟友", "公眾"],
        },
        "economy": {
            "rate_decision": ["央行", "投資人", "借貸者", "政府"],
            "inflation_data": ["央行", "家庭", "企業", "市場"],
            "energy_supply_move": ["產油國", "能源企業", "進口國政府", "市場"],
            "corporate_results": ["企業管理層", "投資人", "員工", "客戶"],
            "market_repricing": ["投資人", "金融機構", "企業", "央行"],
            "macro_signal": ["政府", "央行", "企業", "消費者"],
        }
    }

    category_map = stakeholder_map.get(category, {})
    return category_map.get(event_type, ["政府", "企業", "市場", "公眾"])
    

def find_supporting_sources(primary_item, candidate_items):
    primary_category = primary_item.get("category", "")
    primary_topic = primary_item.get("topic_type", "general")
    primary_source = primary_item.get("source_name", "")
    primary_title = primary_item.get("title", "")

    primary_tokens = set(tokenise_title(primary_title))

    supporting_sources = []
    supporting_titles = []

    seen_sources = set()

    for item in candidate_items:
        if item is primary_item:
            continue

        if item.get("category", "") != primary_category:
            continue

        candidate_source = item.get("source_name", "")
        if candidate_source == primary_source:
            continue

        candidate_topic = item.get("topic_type", "general")
        candidate_title = item.get("title", "")
        candidate_tokens = set(tokenise_title(candidate_title))

        # topic_type 放寬：相同即可，或任一方為 general
        topic_compatible = (
            candidate_topic == primary_topic
            or candidate_topic == "general"
            or primary_topic == "general"
        )

        if not topic_compatible:
            continue

        overlap = title_overlap_score(primary_title, candidate_title)
        shared_tokens = primary_tokens.intersection(candidate_tokens)

        # 放寬規則：
        # 1) overlap >= 0.4
        # 或 2) 共享至少 2 個 token
        if overlap >= 0.4 or len(shared_tokens) >= 2:
            if candidate_source not in seen_sources:
                supporting_sources.append(candidate_source)
                supporting_titles.append(candidate_title)
                seen_sources.add(candidate_source)

    return supporting_sources, supporting_titles


def build_news_focus(title, summary):
    if summary:
        summary_clean = summary.strip()
        summary_clean = re.sub(r"\s+", " ", summary_clean)
        return summary_clean
    return title.strip()


def build_briefing(category, topic_type, event_type, title, summary):
    news_focus = build_news_focus(title, summary)
    background = build_event_background(category, topic_type, event_type)
    analysis = build_event_analysis(category, topic_type, event_type)
    stakeholders = build_event_stakeholders(category, topic_type, event_type)

    return {
        "news_focus": news_focus,
        "news_focus_zh": news_focus,
        "background": background,
        "stakeholders": stakeholders,
        "analysis": analysis,
    }


def dedupe_items(items):
    seen = set()
    deduped = []

    for item in items:
        title = item.get("title", "")
        norm_title = normalise_title(title)
        dedupe_key = (item.get("category", ""), norm_title)

        if not norm_title:
            continue
        if dedupe_key in seen:
            continue

        seen.add(dedupe_key)

        title_clean = clean_text(title)
        summary_clean = clean_text(item.get("summary", ""))
        category = item.get("category", "")
        source_name = item.get("source_name", "")
        topic_type = detect_topic_type(category, title_clean, summary_clean)
        event_type = detect_event_type(category, topic_type, title_clean, summary_clean)
        briefing = build_briefing(category, topic_type, event_type, title_clean, summary_clean)
        dt = best_datetime(item)
        relevance_score = compute_relevance_score(category, topic_type, title_clean, summary_clean, source_name)
        topic_priority_score = get_topic_priority_score(category, topic_type)

        cleaned_item = {
            "category": category,
            "source_name": source_name,
            "title": title_clean,
            "link": item.get("link", ""),
            "summary": summary_clean,
            "published": item.get("published", ""),
            "updated": item.get("updated", ""),
            "published_at_utc": dt.isoformat() if dt else "",
            "id": item.get("id", ""),
            "topic_type": topic_type,
            "relevance_score": relevance_score,
            "briefing": briefing,
            "topic_priority_score": topic_priority_score,
            "event_type": event_type,
        }
        deduped.append(cleaned_item)

    return deduped


def sort_items(items):
    def sort_key(item):
        topic_priority = item.get("topic_priority_score", 0)
        relevance = item.get("relevance_score", 0)
        parsed = item.get("published_at_utc", "")
        title = item.get("title", "")
        return (topic_priority, relevance, parsed, title)

    return sorted(items, key=sort_key, reverse=True)


def select_top_items_by_section(items):
    sections = {section: [] for section in TARGET_SECTIONS}
    further_reading = {section: [] for section in TARGET_SECTIONS}

    for section in TARGET_SECTIONS:
        section_items = [item for item in items if item.get("category") == section]
        section_items = sort_items(section_items)

        if not section_items:
            sections[section] = []
            further_reading[section] = []
            continue

        selected = []

        first_item = dict(section_items[0])
        selected.append(first_item)

        first_topic = first_item.get("topic_type", "general")
        second_item = None

        for item in section_items[1:]:
            if item.get("topic_type", "general") != first_topic:
                second_item = dict(item)
                break

        if second_item is None and len(section_items) > 1:
            second_item = dict(section_items[1])

        if second_item is not None:
            selected.append(second_item)

        # Third item: different topic_type from both prior selections
        selected_topics = {i.get("topic_type", "general") for i in selected}
        third_item = None

        for item in section_items:
            if item in selected:
                continue
            if item.get("topic_type", "general") not in selected_topics:
                third_item = dict(item)
                break

        if third_item is None:
            for item in section_items:
                if item not in selected and len(selected) < MAX_TOPICS_PER_SECTION:
                    third_item = dict(item)
                    break

        if third_item is not None:
            selected.append(third_item)

        # Enrich selected items with supporting source data
        enriched_selected = []
        for selected_item in selected:
            supporting_sources, supporting_titles = find_supporting_sources(selected_item, section_items)
            selected_item["supporting_sources"] = supporting_sources
            selected_item["supporting_titles"] = supporting_titles
            selected_item["source_count"] = 1 + len(supporting_sources)
            enriched_selected.append(selected_item)

        sections[section] = enriched_selected[:MAX_TOPICS_PER_SECTION]

        # Capture further reading: ranked items not already selected, up to 10
        selected_titles = {i.get("title", "") for i in enriched_selected}
        further = [
            {
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "source_name": item.get("source_name", ""),
                "published_at_utc": item.get("published_at_utc", ""),
                "relevance_score": item.get("relevance_score", 0),
            }
            for item in section_items
            if item.get("title", "") not in selected_titles
        ][:10]

        further_reading[section] = further

    return sections, further_reading
    

def build_digest(raw_items, deduped_items, sections, further_reading):
    topic_mix = {}
    multi_source_count = 0

    for section, items in sections.items():
        topic_mix[section] = [item.get("topic_type", "general") for item in items]
        multi_source_count += sum(1 for item in items if item.get("source_count", 1) > 1)

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_raw_items": len(raw_items),
            "total_deduped_items": len(deduped_items),
            "selected_total": sum(len(v) for v in sections.values()),
            "max_topics_per_section": MAX_TOPICS_PER_SECTION,
            "topic_mix": topic_mix,
            "multi_source_selected_count": multi_source_count,
        },
        "sections": sections,
        "further_reading": further_reading,
    }


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    raw_items = load_raw_files()
    deduped_items = dedupe_items(raw_items)
    sections, further_reading = select_top_items_by_section(deduped_items)
    digest = build_digest(raw_items, deduped_items, sections, further_reading)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)

    print("Daily digest generated.")
    print(json.dumps(digest["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
