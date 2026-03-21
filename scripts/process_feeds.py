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
MAX_TOPICS_PER_SECTION = 2


TECH_KEYWORDS = {
    "ai": ["ai", "artificial intelligence", "openai", "chatgpt", "model", "llm", "nvidia"],
    "chips": ["chip", "chips", "semiconductor", "tsmc", "intel", "amd", "qualcomm", "gpu"],
    "platforms": ["meta", "x", "tiktok", "google", "apple", "microsoft", "amazon", "platform"],
    "cybersecurity": ["cyber", "security", "hack", "breach", "malware", "ransomware"],
    "devices": ["iphone", "smartphone", "device", "laptop", "hardware"],
    "regulation": ["antitrust", "regulator", "regulation", "eu", "commission", "fine"],
}

POLITICS_KEYWORDS = {
    "us_china": ["china", "beijing", "washington", "us", "u.s.", "tariff", "trade war"],
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


def detect_topic_type(category, text):
    text = text.lower()

    keyword_map = {}
    if category == "technology":
        keyword_map = TECH_KEYWORDS
    elif category == "politics":
        keyword_map = POLITICS_KEYWORDS
    elif category == "economy":
        keyword_map = ECONOMY_KEYWORDS

    for topic_type, keywords in keyword_map.items():
        for keyword in keywords:
            if keyword in text:
                return topic_type

    return "general"


def build_briefing(category, topic_type, title, summary):
    news_focus = summary if summary else f"此則新聞聚焦於：{title}"

    templates = {
        "technology": {
            "ai": {
                "background": "此議題通常涉及生成式 AI、模型能力競爭、算力供應，以及商業化與監管之間的平衡。",
                "stakeholders": ["科技公司", "雲端平台", "企業客戶", "監管機構", "投資人"],
                "analysis": "核心角力在於誰能掌握模型能力、算力資源與商業應用場景，同時控制成本與政策風險。"
            },
            "chips": {
                "background": "半導體議題通常牽涉先進製程、供應鏈韌性、出口管制與國家產業政策。",
                "stakeholders": ["晶片設計公司", "晶圓代工廠", "設備供應商", "各國政府", "終端品牌商"],
                "analysis": "主要角力點在於技術領先、產能配置、地緣政治風險，以及供應鏈控制權。"
            },
            "platforms": {
                "background": "平台與大型科技公司的新聞，通常反映用戶流量、廣告收入、內容治理與生態系競爭。",
                "stakeholders": ["平台公司", "廣告主", "內容創作者", "監管機構", "使用者"],
                "analysis": "核心角力在於流量分配、商業模式穩定性，以及平台治理與市場支配力之間的平衡。"
            },
            "cybersecurity": {
                "background": "資安議題通常涉及企業防護能力、國家安全、供應鏈漏洞與合規壓力。",
                "stakeholders": ["企業", "政府機構", "資安供應商", "終端用戶", "攻擊者或威脅行為者"],
                "analysis": "角力重點在於防護成本、營運持續性、資料安全與責任歸屬。"
            },
            "devices": {
                "background": "裝置與硬體新聞通常牽涉消費需求、產品週期、零組件供應與品牌競爭。",
                "stakeholders": ["品牌商", "供應鏈廠商", "零售通路", "消費者", "投資人"],
                "analysis": "核心角力在於產品差異化、定價能力、供應穩定性與市場需求變化。"
            },
            "regulation": {
                "background": "科技監管議題通常圍繞反壟斷、資料治理、內容責任與跨境規則。",
                "stakeholders": ["科技公司", "監管機構", "消費者", "競爭對手", "政策制定者"],
                "analysis": "主要角力在於創新自由、市場支配力限制，以及公共風險治理。"
            },
            "general": {
                "background": "此議題反映科技產業近期的重要動向，通常涉及商業競爭、產品策略與政策環境。",
                "stakeholders": ["科技公司", "監管機構", "用戶", "投資人"],
                "analysis": "核心角力通常在於技術節奏、商業模式與市場預期的重新調整。"
            }
        },
        "politics": {
            "us_china": {
                "background": "美中相關議題通常同時涉及戰略競爭、貿易規則、科技限制與區域安全。",
                "stakeholders": ["美國政府", "中國政府", "盟友國家", "跨國企業", "金融市場"],
                "analysis": "核心角力在於經濟互賴與戰略脫鉤之間如何重新劃界。"
            },
            "europe": {
                "background": "歐洲政治議題通常涉及歐盟治理、成員國利益協調、安全政策與經濟穩定。",
                "stakeholders": ["歐盟機構", "成員國政府", "企業", "選民", "盟友"],
                "analysis": "主要角力在於共同政策目標與各國本身政治現實之間的平衡。"
            },
            "uk": {
                "background": "英國政治議題往往反映國內治理、脫歐後定位、財政壓力與對外政策調整。",
                "stakeholders": ["英國政府", "反對黨", "企業", "選民", "國際夥伴"],
                "analysis": "核心角力在於內政壓力、政策執行能力與國際定位之間的重新調整。"
            },
            "taiwan": {
                "background": "台灣相關政治議題通常與兩岸關係、區域安全、供應鏈地位及國際支持有關。",
                "stakeholders": ["台灣政府", "中國政府", "美國及盟友", "企業", "區域市場"],
                "analysis": "主要角力在於安全承諾、政治訊號與經濟穩定之間的平衡。"
            },
            "election": {
                "background": "選舉新聞通常反映政策方向、民意結構與權力重新分配的可能性。",
                "stakeholders": ["執政黨", "反對黨", "選民", "媒體", "市場"],
                "analysis": "核心角力在於議題設定能力、民意動員與選後政策可執行性。"
            },
            "security": {
                "background": "安全與衝突議題通常牽涉軍事風險、外交回應、能源與市場連動效應。",
                "stakeholders": ["衝突各方政府", "軍事同盟", "國際組織", "能源市場", "民間社會"],
                "analysis": "主要角力在於軍事升高風險、外交降溫空間與國際成本分攤。"
            },
            "diplomacy": {
                "background": "外交與制裁議題通常反映國家間談判、壓力施加與政策交換。",
                "stakeholders": ["相關國家政府", "國際組織", "企業", "盟友", "市場參與者"],
                "analysis": "核心角力在於談判籌碼、政策讓步空間與執行可信度。"
            },
            "general": {
                "background": "此議題屬於近期重要政治發展，通常涉及政策方向、國際關係與權力重組。",
                "stakeholders": ["政府", "政黨", "盟友", "企業", "公眾"],
                "analysis": "主要角力在於政策目標、政治成本與外部反應之間的調整。"
            }
        },
        "economy": {
            "rates": {
                "background": "利率相關議題通常反映央行對通膨、成長與金融穩定之間的取捨。",
                "stakeholders": ["央行", "政府", "借貸者", "銀行", "投資人"],
                "analysis": "核心角力在於壓抑通膨、維持經濟活動與控制金融市場波動。"
            },
            "inflation": {
                "background": "通膨新聞通常反映價格壓力、需求強弱、供應鏈變化與政策回應。",
                "stakeholders": ["央行", "家庭", "企業", "零售商", "投資人"],
                "analysis": "主要角力在於成本轉嫁能力、實質購買力與政策調整節奏。"
            },
            "jobs": {
                "background": "就業數據通常被視為經濟韌性與消費能力的重要觀察指標。",
                "stakeholders": ["勞工", "雇主", "政府", "央行", "市場"],
                "analysis": "核心角力在於工資壓力、企業用工策略與政策對景氣判斷的影響。"
            },
            "energy": {
                "background": "能源議題通常影響通膨、供應安全、地緣政治與企業成本。",
                "stakeholders": ["產油國", "能源企業", "進口國政府", "消費者", "市場"],
                "analysis": "主要角力在於供應控制、價格穩定與地緣政治風險管理。"
            },
            "supply_chain": {
                "background": "供應鏈議題通常涉及運輸瓶頸、製造能力、庫存調整與地區風險。",
                "stakeholders": ["製造商", "物流業者", "零售商", "政府", "消費者"],
                "analysis": "核心角力在於成本控制、交付穩定性與供應來源多元化。"
            },
            "markets": {
                "background": "市場新聞通常反映投資人風險偏好、政策預期與資產重新定價。",
                "stakeholders": ["投資人", "金融機構", "企業", "監管單位", "央行"],
                "analysis": "主要角力在於預期管理、流動性環境與風險資產估值調整。"
            },
            "corporate": {
                "background": "企業營運與財報新聞通常反映需求變化、成本結構與管理層對前景的判斷。",
                "stakeholders": ["企業管理層", "投資人", "員工", "供應商", "客戶"],
                "analysis": "核心角力在於成長預期、利潤壓力與資本市場信心。"
            },
            "general": {
                "background": "此議題反映總體經濟或市場的重要變化，通常與政策預期與風險偏好有關。",
                "stakeholders": ["政府", "央行", "企業", "投資人", "消費者"],
                "analysis": "主要角力在於成長、價格穩定與市場信心之間的平衡。"
            }
        }
    }

    section_templates = templates.get(category, {})
    template = section_templates.get(topic_type, section_templates.get("general"))

    return {
        "news_focus": news_focus,
        "background": template["background"],
        "stakeholders": template["stakeholders"],
        "analysis": template["analysis"],
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
        text_blob = f"{title_clean} {summary_clean}"

        topic_type = detect_topic_type(category, text_blob)
        briefing = build_briefing(category, topic_type, title_clean, summary_clean)
        dt = best_datetime(item)

        cleaned_item = {
            "category": category,
            "source_name": item.get("source_name", ""),
            "title": title_clean,
            "link": item.get("link", ""),
            "summary": summary_clean,
            "published": item.get("published", ""),
            "updated": item.get("updated", ""),
            "published_at_utc": dt.isoformat() if dt else "",
            "id": item.get("id", ""),
            "topic_type": topic_type,
            "briefing": briefing,
        }
        deduped.append(cleaned_item)

    return deduped


def sort_items(items):
    def sort_key(item):
        parsed = item.get("published_at_utc", "")
        title = item.get("title", "")
        return (parsed, title)

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
