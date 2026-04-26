import json
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from html import unescape
from email.utils import parsedate_to_datetime


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_FILE = PROCESSED_DIR / "daily_digest.json"

TARGET_SECTIONS = ["technology", "politics", "economy"]
MAX_TOPICS_PER_SECTION = 3

SEEN_TOPICS_FILE = BASE_DIR / "data" / "processed" / "seen_topics.json"
NOVELTY_LOOKBACK_DAYS = 3

MAJOR_UPDATE_SIGNALS = [
    "breaking", "breaks", "escalat", "confirms", "confirmed",
    "signs", "signed", "collapses", "collapse", "resigns", "resign",
    "fires", "invad", "ceasefire", "deal reached", "agreement reached",
    "verdict", "sentenced", "convicted", "overturned", "reversed",
    "record high", "record low", "historic", "unprecedented",
    "emergency", "default", "bankrupt", "arrested", "indicted",
    "new sanctions", "snap election", "declares",
]

TECH_KEYWORDS = {
    "ai": [
        "artificial intelligence", "openai", "chatgpt", "llm", "anthropic",
        "machine learning", "neural network", "deep learning", "gemini",
        "claude ai", "copilot", "grok", "large language model",
    ],
    "chips": [
        "semiconductor", "tsmc", "nvidia", "silicon", "chip fabrication",
        "export controls", "intel", "arm chips", "microchip", "gpu",
        "chip shortage", "foundry", "palantir", "defense tech",
        "pentagon tech", "military ai", "anduril",
    ],
    "quantum_computing": [
        "quantum computing", "qubit", "quantum supremacy",
        "quantum encryption", "ibm quantum", "google quantum",
    ],
    "platform_regulation": [
        "platform regulation", "digital markets act", "dma",
        "digital services act", "antitrust", "tech monopoly",
        "fcc", "ftc tech", "tech regulation", "content moderation",
    ],
    "cybersecurity": [
        "cybersecurity", "ransomware", "malware", "phishing",
        "data breach", "zero day", "cyberattack", "state sponsored cyber",
        "hacked", "vulnerability", "exploit",
    ],
    "space_tech": [
        "spacex", "nasa", "esa", "satellite", "orbital",
        "blue origin", "lunar mission", "rocket launch", "space station",
    ],
    "green_tech": [
        "cleantech", "ev battery", "solar tech", "carbon capture",
        "solid state battery", "renewable energy tech", "electric vehicle tech",
    ],
    "data_privacy": [
        "data privacy", "gdpr", "ccpa", "user tracking",
        "cookie", "privacy policy", "data surveillance", "facial recognition",
    ],
    "social_media": [
        "algorithm", "social media", "tiktok", "meta", "x twitter",
        "recommendation engine", "shadowban", "content moderation",
        "feed ranking", "youtube algorithm",
    ],
}

POLITICS_KEYWORDS = {
    "war_and_conflict": [
        "ukraine", "russia ukraine", "gaza", "israel hamas", "middle east escalation",
        "myanmar junta", "sudan conflict", "airstrike", "missile strike",
        "ground offensive", "ceasefire talks", "war crimes", "siege",
        "occupation", "iran attack", "hezbollah", "drone strike",
    ],
    "us_china_relations": [
        "us-china", "sino-us", "taiwan strait", "beijing washington",
        "south china sea", "chip export ban", "decoupling",
        "tech war", "us china trade", "prc",
    ],
    "eu_policy": [
        "european union", "european commission", "brussels",
        "ursula von der leyen", "european parliament",
        "eu regulation", "eu policy", "nato", "eu sanctions",
    ],
    "uk_politics": [
        "westminster", "downing street", "keir starmer",
        "house of commons", "tories", "labour party",
        "uk government", "uk policy", "parliament uk",
    ],
    "us_politics": [
        "white house", "congress", "senate", "trump",
        "us election", "presidential", "swing state",
        "electoral college", "house representatives",
    ],
    "diplomacy": [
        "summit", "bilateral talks", "un security council",
        "g7", "g20", "diplomatic envoy", "peace treaty",
        "brics", "state visit", "foreign minister",
    ],
    "democracy_autocracy": [
        "democracy", "autocracy", "human rights",
        "democratic backsliding", "authoritarian", "political prisoner",
        "press freedom", "election fraud",
    ],
    "social_protests": [
        "protest", "riot", "demonstration", "mass movement",
        "civil unrest", "police crackdown", "coup",
    ],
}

ECONOMY_KEYWORDS = {
    "interest_rates": [
        "interest rate", "fed hike", "rate cut", "federal reserve",
        "ecb rate", "boe rate", "bank of japan", "monetary policy",
        "rate decision", "hawkish", "dovish",
    ],
    "inflation": [
        "inflation", "cpi", "pce", "price index", "deflation",
        "cost of living", "core inflation", "consumer prices",
    ],
    "trade_wars": [
        "trade war", "tariffs", "protectionism", "wto",
        "export ban", "import duty", "trade deficit", "us-china trade",
        "trade deal", "trade barrier",
    ],
    "sanctions": [
        "sanctions", "ofac", "economic sanctions", "embargo",
        "asset freeze", "swift ban", "oligarch", "trade ban",
    ],
    "energy_markets": [
        "oil price", "brent crude", "wti", "natural gas",
        "opec", "energy market", "lng export", "energy crisis",
    ],
    "labour_markets": [
        "jobs report", "unemployment rate", "labour market",
        "wage growth", "jobless claims", "payroll", "layoffs",
        "hiring freeze", "workforce",
    ],
    "supply_chain": [
        "supply chain", "logistics", "freight", "shipping disruption",
        "port strike", "semiconductor shortage", "manufacturing bottleneck",
    ],
    "emerging_markets": [
        "emerging market", "sovereign default", "imf bailout",
        "world bank", "global south debt", "debt restructuring",
        "developing economy",
    ],
    "corporate_earnings": [
        "earnings", "revenue growth", "profit margin",
        "guidance", "wall street", "quarterly results",
        "beat estimates", "missed forecast",
    ],
    "markets": [
        "stocks", "bonds", "investors", "selloff", "rally",
        "market volatility", "equity", "yield curve",
    ],
}

LOW_SIGNAL_PATTERNS = [
    "visits", "inside look", "lifestyle", "food waste", "travel guide",
    "culture review", "film review", "movie review", "book review",
    "recipe", "fashion", "celebrity", "gossip", "entertainment",
    "wins wimbledon", "tennis match", "match result", "game result",
    "tournament result", "sporting victory", "beats in final",
    "scores goal", "nfl week", "nba scores", "premier league result",
    "cricket scores", "formula one race result",
]

EXPLAINER_TITLE_PATTERNS = [
    "what’s the latest",
    "what's the latest",
    "latest",
    "live",
    "live updates",
    "explainer",
    "analysis",
    "opinion",
    "how ",
    "why ",
]


TECH_GENERAL_EXCLUSION_PATTERNS = [
    "review", "music", "dance music", "album", "song", "concert", "festival",
    "coachella", "entertainment", "movie", "film", "tv show", "television",
    "celebrity", "fashion", "lifestyle", "recipe", "travel guide",
]

FIRST_PERSON_SOFT_PATTERNS = [
    "wasn't on my radar", "was not on my radar", "i stumbled upon",
    "i paused", "i opened the wrong stream", "my tv was lagging",
    "love letter to",
]

TECH_STRONG_SIGNAL_PATTERNS = [
    "startup", "software", "hardware", "device", "smartphone", "app", "apps",
    "platform", "developer", "developers", "cloud", "robot", "robotics",
    "autonomous", "semiconductor", "chip", "chips", "ai", "artificial intelligence",
    "cyber", "data center", "telecom", "satellite", "quantum", "battery",
    "ev", "electric vehicle", "privacy", "antitrust", "regulation",
]


REGIONAL_PRIORITY = {
    "high": [
        "united states", "u.s.", " us ", "american", "washington dc",
        "white house", "pentagon", "congress",
        "china", "chinese", "beijing", " prc ",
        "united kingdom", " uk ", "british", "britain", "london",
        "downing street", "westminster",
        "european union", "europe", "european", "brussels",
        "nato", "g7", "g20",
        "taiwan", "taipei", "taiwan strait",
    ],
    "medium": [
        "japan", "tokyo", "india", "new delhi", "germany", "berlin",
        "france", "paris", "russia", "moscow", "ukraine", "kyiv",
        "south korea", "seoul", "middle east", "israel", "gaza",
        "iran", "tehran", "saudi arabia",
    ],
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


STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "into", "over", "after",
    "before", "about", "says", "say", "said", "will", "would", "could", "should",
    "news", "report", "reports", "reporting", "update", "latest", "live",
    "amid", "as", "at", "by", "in", "of", "on", "to", "is", "are", "be",
    "its", "their", "his", "her", "them", "they", "it", "a", "an"
}

def tokenise_title(title):
    text = normalise_title(title)
    tokens = [t for t in text.split() if len(t) >= 3 and t not in STOPWORDS]
    return tokens

def tokenise_story(title, summary=""):
    text = normalise_title(f"{title} {summary}")
    tokens = [t for t in text.split() if len(t) >= 3 and t not in STOPWORDS]
    return tokens

def overlap_ratio(tokens_a, tokens_b):
    set_a = set(tokens_a)
    set_b = set(tokens_b)
    if not set_a or not set_b:
        return 0.0
    shared = set_a.intersection(set_b)
    smaller_size = min(len(set_a), len(set_b))
    if smaller_size == 0:
        return 0.0
    return len(shared) / smaller_size

def title_overlap_score(title_a, title_b):
    return overlap_ratio(tokenise_title(title_a), tokenise_title(title_b))

def story_overlap_score(item_a, item_b):
    tokens_a = tokenise_story(item_a.get("title", ""), item_a.get("summary", ""))
    tokens_b = tokenise_story(item_b.get("title", ""), item_b.get("summary", ""))
    return overlap_ratio(tokens_a, tokens_b)


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


def has_strong_tech_signal(text):
    for keywords in TECH_KEYWORDS.values():
        for keyword in keywords:
            if keyword in text:
                return True
    return any(pattern in text for pattern in TECH_STRONG_SIGNAL_PATTERNS)


def is_section_qualified(item, section):
    if item.get("category", "") != section:
        return False

    title_l = item.get("title", "").lower()
    summary_l = item.get("summary", "").lower()
    blob = f"{title_l} {summary_l}"
    topic_type = item.get("topic_type", "general")

    if section == "technology":
        if topic_type != "general":
            if contains_any(blob, TECH_GENERAL_EXCLUSION_PATTERNS) and not has_strong_tech_signal(blob):
                return False
            return True

        if contains_any(blob, TECH_GENERAL_EXCLUSION_PATTERNS):
            return False

        if contains_any(blob, FIRST_PERSON_SOFT_PATTERNS):
            return False

        if not has_strong_tech_signal(blob):
            return False

    return True


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
    

def compute_relevance_score(category, topic_type, title, summary, source_name, published_dt=None):
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

    if category == "economy" and topic_type in {
        "interest_rates", "inflation", "trade_wars", "sanctions", "energy_markets", "markets"
    }:
        score += 3

    blob_lower = f"{title_l} {summary_l}"
    for region in REGIONAL_PRIORITY["high"]:
        if region in blob_lower:
            score += 4
            break
    else:
        for region in REGIONAL_PRIORITY["medium"]:
            if region in blob_lower:
                score += 2
                break

    if published_dt is not None:
        age_hours = (datetime.now(timezone.utc) - published_dt).total_seconds() / 3600

        if age_hours <= 6:
            score += 6
        elif age_hours <= 12:
            score += 4
        elif age_hours <= 24:
            score += 2
        elif age_hours <= 48:
            score -= 2
        elif age_hours <= 72:
            score -= 5
        else:
            score -= 8

    return score


def get_topic_priority_score(category, topic_type):
    priority_map = {
        "technology": {
            "chips":             10,
            "ai":                 9,
            "cybersecurity":      9,
            "platform_regulation":8,
            "data_privacy":       7,
            "quantum_computing":  7,
            "social_media":       6,
            "green_tech":         5,
            "space_tech":         4,
            "general":            3,
        },
        "politics": {
            "war_and_conflict":  11,
            "us_china_relations":10,
            "eu_policy":          9,
            "uk_politics":        9,
            "us_politics":        8,
            "diplomacy":          7,
            "democracy_autocracy":6,
            "social_protests":    5,
            "general":            3,
        },
        "economy": {
            "interest_rates":    10,
            "inflation":          9,
            "trade_wars":         9,
            "sanctions":          8,
            "energy_markets":     8,
            "labour_markets":     7,
            "supply_chain":       6,
            "emerging_markets":   6,
            "corporate_earnings": 5,
            "markets":            4,
            "general":            3,
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
    

def is_explainer_style_title(title):
    title_l = clean_text(title).lower()
    for pattern in EXPLAINER_TITLE_PATTERNS:
        if pattern in title_l:
            return True
    return False


def infer_category(title, summary, fallback_category):
    text = {"title": title.lower(), "summary": summary.lower()}
    category_maps = {
        "technology": TECH_KEYWORDS,
        "politics": POLITICS_KEYWORDS,
        "economy": ECONOMY_KEYWORDS,
    }

    category_scores = {}
    for category, keyword_map in category_maps.items():
        score_map = keyword_score(text, keyword_map)
        category_scores[category] = sum(score_map.values())

    current_score = category_scores.get(fallback_category, 0)
    best_category = max(category_scores, key=lambda c: category_scores[c])
    best_score = category_scores.get(best_category, 0)

    if best_score >= 4 and best_score >= current_score + 3:
        return best_category
    return fallback_category


def is_hard_news_like(item):
    title_l = item.get("title", "").lower()
    summary_l = item.get("summary", "").lower()
    blob = f"{title_l} {summary_l}"

    if is_explainer_style_title(title_l):
        return False
    if contains_any(blob, LOW_SIGNAL_PATTERNS):
        return False
    return True


def is_same_event(item_a, item_b):
    if item_a.get("category") != item_b.get("category"):
        return False
    if story_overlap_score(item_a, item_b) >= 0.5:
        return True
    return title_overlap_score(item_a.get("title", ""), item_b.get("title", "")) >= 0.45


def supporting_match_score(primary_item, candidate_item):
    primary_category = primary_item.get("category", "")
    candidate_category = candidate_item.get("category", "")
    if candidate_category != primary_category:
        return None

    primary_source = primary_item.get("source_name", "")
    candidate_source = candidate_item.get("source_name", "")
    if not candidate_source or candidate_source == primary_source:
        return None

    primary_topic = primary_item.get("topic_type", "general")
    candidate_topic = candidate_item.get("topic_type", "general")
    topic_compatible = (
        candidate_topic == primary_topic
        or candidate_topic == "general"
        or primary_topic == "general"
    )
    if not topic_compatible:
        return None

    overlap = story_overlap_score(primary_item, candidate_item)
    title_overlap = title_overlap_score(
        primary_item.get("title", ""),
        candidate_item.get("title", "")
    )

    primary_story_tokens = set(
        tokenise_story(primary_item.get("title", ""), primary_item.get("summary", ""))
    )
    candidate_story_tokens = set(
        tokenise_story(candidate_item.get("title", ""), candidate_item.get("summary", ""))
    )
    shared_tokens = primary_story_tokens.intersection(candidate_story_tokens)
    shared_count = len(shared_tokens)

    primary_event = primary_item.get("event_type", "")
    candidate_event = candidate_item.get("event_type", "")
    same_event_type = primary_event and candidate_event and primary_event == candidate_event
    same_exact_topic = primary_topic != "general" and primary_topic == candidate_topic

    if overlap < 0.35 and title_overlap < 0.30:
        return None

    if shared_count < 2:
        return None

    score = 0
    score += overlap * 100
    score += title_overlap * 35
    score += min(shared_count, 5) * 5

    if same_event_type:
        score += 10
    if same_exact_topic:
        score += 8

    candidate_title = candidate_item.get("title", "")
    if is_explainer_style_title(candidate_title):
        score -= 15

    return round(score, 2)


def find_supporting_sources(primary_item, candidate_items):
    ranked_candidates = []
    seen_sources = set()

    for item in candidate_items:
        if item is primary_item:
            continue

        candidate_source = item.get("source_name", "")
        if candidate_source in seen_sources:
            continue

        match_score = supporting_match_score(primary_item, item)
        if match_score is None:
            continue

        ranked_candidates.append({
            "score": match_score,
            "relevance_score": item.get("relevance_score", 0),
            "published_at_utc": item.get("published_at_utc", ""),
            "source_name": candidate_source,
            "title": item.get("title", ""),
            "summary": item.get("summary", ""),
            "link": item.get("link", ""),
        })
        seen_sources.add(candidate_source)

    ranked_candidates = sorted(
        ranked_candidates,
        key=lambda x: (x["score"], x["relevance_score"], x["published_at_utc"]),
        reverse=True,
    )[:2]

    supporting_sources = [x["source_name"] for x in ranked_candidates]
    supporting_titles = [x["title"] for x in ranked_candidates]
    supporting_summaries = [x["summary"] for x in ranked_candidates]
    supporting_links = [x["link"] for x in ranked_candidates]

    return supporting_sources, supporting_titles, supporting_summaries, supporting_links


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


def load_seen_topics():
    if SEEN_TOPICS_FILE.exists():
        with open(SEEN_TOPICS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"stories": []}


def is_major_update(title, summary):
    text = f"{title} {summary}".lower()
    return contains_any(text, MAJOR_UPDATE_SIGNALS)


def apply_novelty_penalties(items, seen_topics):
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=NOVELTY_LOOKBACK_DAYS)

    recent = [
        s for s in seen_topics.get("stories", [])
        if s.get("last_seen_date", "") >= cutoff.isoformat()
    ]

    for item in items:
        title = item.get("title", "")
        category = item.get("category", "")
        summary = item.get("summary", "")
        item_tokens = set(tokenise_title(title))

        for seen in recent:
            if seen.get("category") != category:
                continue
            seen_tokens = set(seen.get("title_tokens", []))
            if not seen_tokens or not item_tokens:
                continue
            overlap = len(seen_tokens & item_tokens) / min(len(seen_tokens), len(item_tokens))
            if overlap >= 0.5:
                if is_major_update(title, summary):
                    item["relevance_score"] = item.get("relevance_score", 0) + 4
                    item["is_major_update"] = True
                else:
                    item["relevance_score"] = item.get("relevance_score", 0) - 12
                    item["is_repeat_story"] = True
                break

    return items


def resolve_cross_section_conflicts(sections, section_pools, further_reading):
    section_keys = list(sections.keys())
    processed_pairs = set()

    for i, sec_a in enumerate(section_keys):
        for sec_b in section_keys[i + 1:]:
            for item_a in list(sections.get(sec_a, [])):
                for item_b in list(sections.get(sec_b, [])):
                    title_a = item_a.get("title", "")
                    title_b = item_b.get("title", "")
                    pair_key = (min(title_a, title_b), max(title_a, title_b))
                    if pair_key in processed_pairs:
                        continue
                    if title_overlap_score(title_a, title_b) < 0.4:
                        continue

                    processed_pairs.add(pair_key)

                    selected_a = {x.get("title", "") for x in sections.get(sec_a, [])}
                    selected_b = {x.get("title", "") for x in sections.get(sec_b, [])}
                    further_a  = {x.get("title", "") for x in further_reading.get(sec_a, [])}
                    further_b  = {x.get("title", "") for x in further_reading.get(sec_b, [])}

                    next_a = next(
                        (x for x in section_pools.get(sec_a, [])
                         if x.get("title", "") not in selected_a | further_a
                         and x.get("title", "") != title_a),
                        None,
                    )
                    next_b = next(
                        (x for x in section_pools.get(sec_b, [])
                         if x.get("title", "") not in selected_b | further_b
                         and x.get("title", "") != title_b),
                        None,
                    )

                    score_next_a = next_a.get("relevance_score", 0) if next_a else -999
                    score_next_b = next_b.get("relevance_score", 0) if next_b else -999

                    if score_next_b >= score_next_a:
                        keeper_sec, keeper_item = sec_a, item_a
                        loser_sec,  loser_item  = sec_b, item_b
                        promotee = next_b
                    else:
                        keeper_sec, keeper_item = sec_b, item_b
                        loser_sec,  loser_item  = sec_a, item_a
                        promotee = next_a

                    sections[loser_sec] = [
                        x for x in sections[loser_sec]
                        if x.get("title", "") != loser_item.get("title", "")
                    ]

                    fr_entry = {
                        "title":          loser_item.get("title", ""),
                        "link":           loser_item.get("link", ""),
                        "source_name":    loser_item.get("source_name", ""),
                        "published_at_utc": loser_item.get("published_at_utc", ""),
                        "relevance_score": loser_item.get("relevance_score", 0),
                    }
                    fr_list = further_reading.setdefault(loser_sec, [])
                    fr_list.insert(0, fr_entry)
                    further_reading[loser_sec] = fr_list[:10]

                    if promotee:
                        p = dict(promotee)
                        sup, sup_titles, sup_summaries, sup_links = find_supporting_sources(
                            p, section_pools.get(loser_sec, [])
                        )
                        p["supporting_sources"] = sup
                        p["supporting_titles"] = sup_titles
                        p["supporting_summaries"] = sup_summaries
                        p["supporting_links"] = sup_links
                        p["source_count"] = 1 + len(sup)
                        sections[loser_sec].append(p)

                    refs = keeper_item.setdefault("cross_section_refs", [])
                    if loser_sec not in refs:
                        refs.append(loser_sec)

    return sections, further_reading

def save_seen_topics(seen_topics, sections):
    today = datetime.now(timezone.utc).date().isoformat()
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=7)).isoformat()

    existing = [
        s for s in seen_topics.get("stories", [])
        if s.get("last_seen_date", "") >= cutoff
    ]

    for section_key, items in sections.items():
        for item in items:
            title_tokens = list(set(tokenise_title(item.get("title", ""))))
            item_token_set = set(title_tokens)
            updated = False

            for seen in existing:
                if seen.get("category") != section_key:
                    continue
                seen_token_set = set(seen.get("title_tokens", []))
                if seen_token_set and item_token_set:
                    overlap = len(seen_token_set & item_token_set) / min(len(seen_token_set), len(item_token_set))
                    if overlap >= 0.5:
                        seen["last_seen_date"] = today
                        seen["times_seen"] = seen.get("times_seen", 1) + 1
                        updated = True
                        break

            if not updated:
                existing.append({
                    "title_tokens": title_tokens,
                    "category": section_key,
                    "topic_type": item.get("topic_type", "general"),
                    "last_seen_date": today,
                    "times_seen": 1,
                })

    seen_topics["stories"] = existing
    seen_topics["last_updated"] = today

    SEEN_TOPICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SEEN_TOPICS_FILE, "w", encoding="utf-8") as f:
        json.dump(seen_topics, f, ensure_ascii=False, indent=2)
        

def dedupe_items(items):
    deduped = []

    for item in items:
        title = item.get("title", "")
        title_clean = clean_text(title)
        summary_clean = clean_text(item.get("summary", ""))
        category = item.get("category", "")
        original_category = item.get("category", "")
        category = infer_category(title_clean, summary_clean, original_category)
        source_name = item.get("source_name", "")

        if not normalise_title(title_clean):
            continue

        topic_type = detect_topic_type(category, title_clean, summary_clean)
        event_type = detect_event_type(category, topic_type, title_clean, summary_clean)
        briefing = build_briefing(category, topic_type, event_type, title_clean, summary_clean)
        dt = best_datetime(item)
        relevance_score = compute_relevance_score(
            category,
            topic_type,
            title_clean,
            summary_clean,
            source_name,
            published_dt=dt
        )
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

        is_duplicate_story = False

        for existing in deduped:
            if existing.get("category") != category:
                continue

            existing_topic = existing.get("topic_type", "general")
            topic_compatible = (
                existing_topic == topic_type
                or existing_topic == "general"
                or topic_type == "general"
            )

            if not topic_compatible:
                continue

            overlap = story_overlap_score(cleaned_item, existing)

            if overlap >= 0.6:
                is_duplicate_story = True
                break

        if not is_duplicate_story:
            deduped.append(cleaned_item)

    return deduped

def sort_items(items):
    def sort_key(item):
        relevance = item.get("relevance_score", 0)
        parsed = item.get("published_at_utc", "")
        topic_priority = item.get("topic_priority_score", 0)
        title = item.get("title", "")
        return (relevance, parsed, topic_priority, title)

    return sorted(items, key=sort_key, reverse=True)


def select_top_items_by_section(items):
    sections = {section: [] for section in TARGET_SECTIONS}
    further_reading = {section: [] for section in TARGET_SECTIONS}
    section_pools = {section: [] for section in TARGET_SECTIONS}

    for section in TARGET_SECTIONS:
        section_items = [
            item for item in items
            if item.get("category") == section and is_section_qualified(item, section)
        ]
        section_items = sort_items(section_items)
        section_pools[section] = section_items

        if not section_items:
            continue
        selected = []
        soft_guard_slots = 2
        
            if len(selected) >= MAX_TOPICS_PER_SECTION:
                break
            if any(is_same_event(item, chosen) for chosen in selected):
                continue
            if len(selected) < soft_guard_slots and not is_hard_news_like(item):
                continue
            selected.append(dict(item))

        if len(selected) < MAX_TOPICS_PER_SECTION:
            for item in section_items:
                if len(selected) >= MAX_TOPICS_PER_SECTION:
                    break
                if any(chosen.get("title") == item.get("title") for chosen in selected):
                    continue
                if any(is_same_event(item, chosen) for chosen in selected):
                    continue
                selected.append(dict(item))


        enriched_selected = []
        for selected_item in selected[:MAX_TOPICS_PER_SECTION]:
            supporting_sources, supporting_titles, supporting_summaries, supporting_links = find_supporting_sources(
                selected_item, section_items
            )

            selected_item["supporting_sources"] = supporting_sources
            selected_item["supporting_titles"] = supporting_titles
            selected_item["supporting_summaries"] = supporting_summaries
            selected_item["supporting_links"] = supporting_links
            selected_item["source_count"] = 1 + len(supporting_sources)

            enriched_selected.append(selected_item)

        sections[section] = enriched_selected

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

    return sections, further_reading, section_pools
    

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

    seen_topics = load_seen_topics()
    raw_items = load_raw_files()
    deduped_items = dedupe_items(raw_items)
    deduped_items = apply_novelty_penalties(deduped_items, seen_topics)
    sections, further_reading, section_pools = select_top_items_by_section(deduped_items)
    sections, further_reading = resolve_cross_section_conflicts(
        sections, section_pools, further_reading
    )
    digest = build_digest(raw_items, deduped_items, sections, further_reading)
    save_seen_topics(seen_topics, sections)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)

    print("Daily digest generated.")
    print(json.dumps(digest["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
