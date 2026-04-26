#!/usr/bin/env python3
"""
process_feeds.py — News-Claw story selection engine (cluster-first redesign)

WHY THIS FILE WAS REWRITTEN
────────────────────────────
The previous version ranked individual articles and tried to fix duplication
problems afterwards with patches. That approach kept breaking because the
fundamental unit was wrong — it was article-first, not story-first.

This rewrite flips the order:
  1. Qualify  — decide if an article is hard news or soft content
  2. Score    — measure how well each article fits each section (by content, not feed label)
  3. Cluster  — group articles that are reporting on the same event
  4. Rank     — rank story clusters, not individual articles
  5. Select   — pick the best cluster per slot, enforcing hard editorial rules
  6. Package  — choose one representative article + supporting sources per cluster

EDITORIAL RULES ENFORCED
─────────────────────────
- Max 3 story clusters per section
- Each event appears in at most one section
- Each event occupies at most one slot within a section
- Soft content (reviews, guides, explainers, live blogs) cannot take a top slot
- Section assignment is based on article content, not feed/source category

OUTPUT FORMAT (daily_digest.json)
──────────────────────────────────
{
  "generated_at": "...",
  "technology": [ { title, summary, link, source, published,
                    supporting_sources, supporting_titles,
                    supporting_summaries, supporting_links }, ... ],
  "politics":   [ ... ],
  "economy":    [ ... ],
  "further_reading": { "technology": [...], "politics": [...], "economy": [...] }
}

This format is intentionally compatible with the existing analyse_items.py
and build_site.py so no changes are needed in those files.
"""

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ──────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DIGEST_PATH = PROCESSED_DIR / "daily_digest.json"
SEEN_TOPICS_PATH = PROCESSED_DIR / "seen_topics.json"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────
# Tunable constants
# ──────────────────────────────────────────────────────────────
SECTIONS = ["technology", "politics", "economy"]
MAX_ITEMS_PER_SECTION = 3       # hard cap: top story slots per section
MAX_SUPPORT_PER_ITEM = 2        # supporting sources per selected story
CLUSTER_OVERLAP_THRESHOLD = 0.14  # min token-overlap to merge into same cluster
SECTION_MIN_SCORE = 2           # an article must hit ≥ this many keywords to pass

# ──────────────────────────────────────────────────────────────
# Section keyword sets
# These are content signals, not feed labels.
# An article can hit keywords in more than one section; it goes to the highest scorer.
# ──────────────────────────────────────────────────────────────
SECTION_KEYWORDS = {
    "technology": {
        # English
        "ai", "artificial intelligence", "machine learning", "llm", "gpt", "claude",
        "chip", "semiconductor", "nvidia", "apple", "google", "microsoft", "meta",
        "amazon", "openai", "anthropic", "software", "hardware", "cybersecurity",
        "cyber", "hack", "data breach", "startup", "ipo", "robotics", "quantum",
        "cloud", "5g", "broadband", "smartphone", "app", "algorithm", "model",
        "tesla", "spacex", "satellite", "drone", "battery", "electric vehicle", "ev",
        "deepmind", "x.ai", "gemini", "internet", "social media", "platform",
        # Traditional Chinese
        "科技", "人工智能", "晶片", "半導體", "軟體", "硬體", "網路安全",
        "資料外洩", "新創", "無人機", "電動車", "演算法", "模型", "衛星",
    },
    "politics": {
        # English
        "president", "prime minister", "congress", "senate", "parliament",
        "election", "vote", "government", "military", "war", "attack", "troops",
        "sanctions", "diplomat", "treaty", "bilateral", "nato", "un",
        "united nations", "security council", "law", "legislation", "court",
        "supreme court", "indictment", "arrest", "protest", "coup",
        "assassination", "shooting", "bombing", "conflict", "ceasefire",
        "israel", "iran", "ukraine", "russia", "china", "taiwan", "north korea",
        "white house", "kremlin", "beijing", "pentagon", "state department",
        "foreign minister", "defense minister", "secretary of state",
        # Traditional Chinese
        "政治", "選舉", "政府", "軍事", "外交", "制裁", "議會", "國會",
        "總統", "總理", "戰爭", "攻擊", "衝突", "停火", "法院", "逮捕",
        "抗議", "政變", "刺殺", "炸彈", "以色列", "伊朗", "烏克蘭", "俄羅斯",
        "中國", "台灣", "北韓", "白宮", "克里姆林宮",
    },
    "economy": {
        # English
        "gdp", "inflation", "interest rate", "federal reserve", "central bank",
        "stock", "market", "trade", "tariff", "export", "import", "supply chain",
        "recession", "unemployment", "jobs", "imf", "world bank", "wto",
        "oil", "energy", "currency", "dollar", "yuan", "euro", "yen",
        "budget", "deficit", "debt", "bond", "treasury", "fiscal", "monetary",
        "bank", "finance", "investment", "merger", "acquisition", "earnings",
        "growth", "output", "sector", "commodity", "price", "cost", "wage",
        # Traditional Chinese
        "經濟", "通膨", "利率", "央行", "股市", "貿易", "關稅", "匯率",
        "財政", "貨幣", "通貨緊縮", "失業", "就業", "債券", "預算",
        "赤字", "債務", "投資", "併購", "收益", "能源", "油價",
    },
}

# ──────────────────────────────────────────────────────────────
# Soft content patterns
# Items matching any of these are excluded from top headline picks.
# They may still appear in further_reading.
# ──────────────────────────────────────────────────────────────
SOFT_PATTERNS = [
    r"\bbest\b.{0,30}\b(of|for|to)\b",
    r"\bhow to\b",
    r"\bguide\b",
    r"\b\d+ (ways|tips|things|reasons|steps)\b",
    r"\breview[:\s]",
    r"\bopinion[:\s]",
    r"\bexplained\b",
    r"\bwhat (is|are|was|were)\b",
    r"\blive (blog|stream|update|coverage)\b",
    r"\bwatch live\b",
    r"\bfeature[:\s]",
    r"\bdeals\b",
    r"\bbuy\b.{0,20}\b(now|here|today)\b",
    r"\bshop\b",
    r"\blifestyle\b",
    r"\bentertainment\b",
    r"\bplaylist\b",
    r"\balbum review\b",
    r"\bmovie review\b",
    r"\bfilm review\b",
    r"\bpodcast\b",
    r"\brecap\b",
    r"\bnewsletter\b",
    r"\bsponsored\b",
    r"\badvertisement\b",
]

# Hard news signals — action verbs and event markers that suggest real news
HARD_NEWS_PATTERNS = [
    r"\b(killed|died|dead|deaths?)\b",
    r"\b(arrested?|charged|indicted|convicted|sentenced)\b",
    r"\b(launched|deployed|signed|passed|vetoed|approved)\b",
    r"\b(attacked?|bombed?|shot|fired|expelled|assassinated?)\b",
    r"\b(sanctions|ban|embargo|blockade)\b",
    r"\b(elected|appointed|resigned|fired|ousted|sworn in)\b",
    r"\b(announced|confirmed|revealed|leaked|disclosed)\b",
    r"\b(crashed|collapsed|failed|shutdown|suspended)\b",
    r"\b(ceasefire|truce|agreement|deal signed)\b",
    r"\b(宣布|確認|逮捕|通過|簽署|攻擊|選舉|辭職|死亡|制裁|停火)\b",
]


# ──────────────────────────────────────────────────────────────
# I/O helpers
# ──────────────────────────────────────────────────────────────

def load_raw_items():
    """
    Load raw feed items from data/raw/*.json.
    Expects each file to be either:
      - a list of article dicts, or
      - a dict with an "items" key containing a list
    """
    items = []
    if not RAW_DIR.exists():
        print(f"[WARN] Raw data directory not found: {RAW_DIR}")
        return items
    for fpath in sorted(RAW_DIR.glob("*.json")):
        try:
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                items.extend(data)
            elif isinstance(data, dict) and "items" in data:
                items.extend(data["items"])
        except Exception as e:
            print(f"[WARN] Could not load {fpath.name}: {e}")
    return items


def load_seen_topics():
    try:
        with open(SEEN_TOPICS_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_seen_topics(seen: dict):
    with open(SEEN_TOPICS_PATH, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────────────────────────────
# Text helpers
# ──────────────────────────────────────────────────────────────

_STOPWORDS = {
    # English
    "the", "a", "an", "in", "on", "at", "to", "of", "and", "or", "but",
    "is", "are", "was", "were", "be", "been", "has", "have", "had",
    "it", "its", "this", "that", "with", "for", "from", "as", "by",
    "he", "she", "they", "we", "i", "my", "our", "his", "her",
    "will", "would", "could", "should", "may", "might", "can", "not",
    "over", "into", "after", "before", "about", "more", "than", "their",
    # Traditional Chinese
    "的", "了", "在", "是", "和", "與", "對", "為", "也", "都",
    "但", "而", "及", "由", "其", "已", "將", "被", "有", "一",
    "中", "上", "下", "後", "前", "內", "外", "說", "表示",
}


def normalise(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip().lower()


def tokenise(text: str) -> set:
    """Extract meaningful tokens from text for overlap comparison."""
    text = normalise(text)
    text = re.sub(r"[^\w\s]", " ", text)
    return {t for t in text.split() if len(t) > 2 and t not in _STOPWORDS}


def token_overlap(a: str, b: str) -> float:
    """
    Jaccard-like token overlap between two texts.
    Returns 0.0–1.0. Values above CLUSTER_OVERLAP_THRESHOLD suggest same event.
    """
    ta = tokenise(a)
    tb = tokenise(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def is_soft(title: str, summary: str) -> bool:
    combined = normalise(title + " " + summary)
    return any(re.search(p, combined) for p in SOFT_PATTERNS)


def count_hard_news_signals(title: str, summary: str) -> int:
    combined = normalise(title + " " + summary)
    return sum(1 for p in HARD_NEWS_PATTERNS if re.search(p, combined))


def score_for_section(title: str, summary: str, section: str) -> int:
    combined = normalise(title + " " + summary)
    return sum(1 for kw in SECTION_KEYWORDS[section] if kw in combined)


# ──────────────────────────────────────────────────────────────
# Date helpers
# ──────────────────────────────────────────────────────────────

_DATE_FORMATS = (
    "%a, %d %b %Y %H:%M:%S %z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)

_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def parse_date(raw) -> datetime:
    if not raw:
        return _EPOCH
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        except Exception:
            return _EPOCH
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(str(raw).strip()[:25], fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return _EPOCH


def freshness_score(dt: datetime) -> float:
    """
    Linear freshness decay:
      ≤12h  → 1.0
      ≤24h  → 0.8
      ≤48h  → 0.5
      ≤72h  → 0.2
      >72h  → 0.0  (item will be dropped early)
    """
    if dt == _EPOCH:
        return 0.3  # unknown date: give moderate freshness, don't fully discard
    age_h = max((datetime.now(tz=timezone.utc) - dt).total_seconds() / 3600, 0)
    if age_h <= 12:
        return 1.0
    if age_h <= 24:
        return 0.8
    if age_h <= 48:
        return 0.5
    if age_h <= 72:
        return 0.2
    return 0.0


# ──────────────────────────────────────────────────────────────
# PHASE 1 + 2: Qualify and score each article
# ──────────────────────────────────────────────────────────────

def qualify_and_score(raw_items: list) -> list:
    """
    For each raw article:
    - extract fields
    - compute freshness
    - detect soft vs hard-news content
    - score against all three sections (content-based, ignoring feed label)
    - drop items that are too stale or don't fit any section

    Returns enriched item dicts with private '_' fields for pipeline use.
    """
    enriched = []
    seen_links = set()

    for item in raw_items:
        title   = str(item.get("title") or "").strip()
        summary = str(item.get("summary") or item.get("description") or "").strip()
        link    = str(item.get("link") or item.get("url") or "").strip()
        source  = str(item.get("source") or item.get("feed_name") or "").strip()
        raw_date = item.get("published") or item.get("pub_date") or item.get("date")

        if not title or not link:
            continue
        if link in seen_links:
            continue
        seen_links.add(link)

        dt = parse_date(raw_date)
        fresh = freshness_score(dt)
        if fresh == 0.0:
            continue  # stale — skip entirely

        published_str = (
            dt.strftime("%Y-%m-%d %H:%M")
            if dt != _EPOCH
            else "日期不明"
        )

        soft   = is_soft(title, summary)
        hn_sig = count_hard_news_signals(title, summary)

        sec_scores = {s: score_for_section(title, summary, s) for s in SECTIONS}
        best_sec   = max(sec_scores, key=lambda s: sec_scores[s])
        best_score = sec_scores[best_sec]

        # Drop items that have no meaningful section signal at all
        if best_score < SECTION_MIN_SCORE:
            continue

        # Composite editorial score used to rank within clusters
        editorial = (hn_sig * 2.0) + (fresh * 1.5) + (best_score * 0.4) + (0 if soft else 1.0)

        enriched.append({
            # Fields that go into the digest
            "title":     title,
            "summary":   summary,
            "link":      link,
            "source":    source,
            "published": published_str,
            # Private pipeline fields (stripped before output)
            "_dt":             dt,
            "_fresh":          fresh,
            "_soft":           soft,
            "_hn_sig":         hn_sig,
            "_sec_scores":     sec_scores,
            "_best_sec":       best_sec,
            "_editorial":      editorial,
        })

    return enriched


# ──────────────────────────────────────────────────────────────
# PHASE 3: Global story clustering
# ──────────────────────────────────────────────────────────────

def cluster_items(items: list) -> list[list]:
    """
    Greedy single-linkage clustering by token overlap.

    Two articles belong to the same cluster if the overlap between their
    combined title+summary texts is ≥ CLUSTER_OVERLAP_THRESHOLD.

    This is intentionally global — the same event reported by tech sources
    AND political sources will correctly end up in one cluster, which
    is then assigned to exactly one section.
    """
    n = len(items)
    assigned = [False] * n
    clusters = []

    for i in range(n):
        if assigned[i]:
            continue
        cluster = [items[i]]
        assigned[i] = True
        text_i = items[i]["title"] + " " + items[i]["summary"]

        for j in range(i + 1, n):
            if assigned[j]:
                continue
            text_j = items[j]["title"] + " " + items[j]["summary"]
            if token_overlap(text_i, text_j) >= CLUSTER_OVERLAP_THRESHOLD:
                cluster.append(items[j])
                assigned[j] = True

        clusters.append(cluster)

    return clusters


# ──────────────────────────────────────────────────────────────
# PHASE 4: Rank clusters
# ──────────────────────────────────────────────────────────────

def cluster_importance(cluster: list) -> float:
    """
    Score a cluster for editorial importance.

    Higher corroboration (more sources on the same event) = bigger story.
    Fresh, hard-news clusters score highest.
    Clusters where every item is soft content are penalised.
    """
    corroboration = len(cluster)
    best_fresh    = max(it["_fresh"] for it in cluster)
    best_hn       = max(it["_hn_sig"] for it in cluster)
    all_soft      = all(it["_soft"] for it in cluster)

    score = (corroboration * 1.5) + (best_fresh * 2.0) + (best_hn * 1.5)
    if all_soft:
        score -= 5.0
    return score


def cluster_section(cluster: list) -> str:
    """
    Assign a cluster to the section with the highest total keyword score
    across all items in the cluster.
    This overrides per-article feed labels.
    """
    totals = defaultdict(float)
    for item in cluster:
        for sec, score in item["_sec_scores"].items():
            totals[sec] += score
    return max(totals, key=lambda s: totals[s])


# ──────────────────────────────────────────────────────────────
# PHASE 5: Select final items per section
# ──────────────────────────────────────────────────────────────

def pick_representative(cluster: list) -> dict:
    """
    Choose the best single article to represent a story cluster.
    Prefers non-soft, hard-news, fresh articles.
    """
    candidates = [it for it in cluster if not it["_soft"]] or cluster
    return max(candidates, key=lambda it: it["_editorial"])


def build_support(cluster: list, rep: dict) -> dict:
    """
    Build the support packet from the remaining articles in the cluster.
    """
    others  = [it for it in cluster if it["link"] != rep["link"]]
    support = sorted(others, key=lambda it: it["_editorial"], reverse=True)[:MAX_SUPPORT_PER_ITEM]
    return {
        "supporting_sources":   [it["source"]  for it in support],
        "supporting_titles":    [it["title"]   for it in support],
        "supporting_summaries": [it["summary"] for it in support],
        "supporting_links":     [it["link"]    for it in support],
    }


def select_items(clusters: list) -> tuple[dict, dict]:
    """
    Phase 5: Apply hard editorial rules to select final items.

    Rules:
    - Max MAX_ITEMS_PER_SECTION clusters per section
    - A cluster can only appear in one section (no cross-section duplication)
    - Soft-only clusters are skipped for top picks
    - The same event cannot fill more than one slot in a section

    Returns:
      selected       — { section: [ digest_item, ... ] }
      further_reading — { section: [ slim_item, ... ] }
    """
    # Sort clusters by importance (descending)
    ranked = sorted(clusters, key=cluster_importance, reverse=True)

    # Assign each cluster to a section
    section_queues = defaultdict(list)
    for cl in ranked:
        sec = cluster_section(cl)
        section_queues[sec].append(cl)

    used_cluster_ids = set()  # prevents a cluster appearing in multiple sections
    selected = {sec: [] for sec in SECTIONS}

    for sec in SECTIONS:
        for cluster in section_queues[sec]:
            if len(selected[sec]) >= MAX_ITEMS_PER_SECTION:
                break

            cl_id = frozenset(it["link"] for it in cluster)
            if cl_id in used_cluster_ids:
                continue  # already used in another section

            if all(it["_soft"] for it in cluster):
                continue  # soft-only cluster — skip for headline picks

            rep     = pick_representative(cluster)
            support = build_support(cluster, rep)

            selected[sec].append({
                "title":     rep["title"],
                "summary":   rep["summary"],
                "link":      rep["link"],
                "source":    rep["source"],
                "published": rep["published"],
                **support,
            })
            used_cluster_ids.add(cl_id)

    # Build further_reading from clusters not selected as top picks
    selected_links = {
        item["link"]
        for sec_items in selected.values()
        for item in sec_items
    }

    further = {sec: [] for sec in SECTIONS}
    for cluster in ranked:
        sec = cluster_section(cluster)
        if len(further[sec]) >= 5:
            continue
        rep = pick_representative(cluster)
        if rep["link"] in selected_links:
            continue
        further[sec].append({
            "title":     rep["title"],
            "link":      rep["link"],
            "source":    rep["source"],
            "published": rep["published"],
        })

    return selected, further


# ──────────────────────────────────────────────────────────────
# Seen-topics novelty tracking
# ──────────────────────────────────────────────────────────────

def update_seen_topics(selected: dict, seen: dict) -> dict:
    """
    Store a fingerprint of today's selected titles so future runs
    can detect if the same story is recurring.
    Fingerprint = sorted top-10 meaningful tokens from the title.
    Prune entries older than 7 days.
    """
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    for sec_items in selected.values():
        for item in sec_items:
            tokens = sorted(tokenise(item["title"]))[:10]
            seen[" ".join(tokens)] = today

    cutoff = datetime.now(tz=timezone.utc)
    return {
        fp: date_str
        for fp, date_str in seen.items()
        if (cutoff - datetime.strptime(date_str, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)).days <= 7
    }


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────

def main():
    print("[process_feeds] Loading raw items …")
    raw = load_raw_items()
    print(f"[process_feeds] {len(raw)} raw items loaded.")

    print("[process_feeds] Phase 1+2: Qualifying and scoring items …")
    items = qualify_and_score(raw)
    print(f"[process_feeds] {len(items)} items passed qualification.")

    if not items:
        print("[process_feeds] No qualifying items found. Exiting.")
        return

    print("[process_feeds] Phase 3: Clustering by story …")
    clusters = cluster_items(items)
    print(f"[process_feeds] {len(clusters)} story clusters formed.")

    print("[process_feeds] Phase 5: Selecting top stories per section …")
    selected, further_reading = select_items(clusters)
    for sec in SECTIONS:
        n = len(selected[sec])
        print(f"  {sec}: {n} item(s) selected.")

    print("[process_feeds] Updating seen-topics tracker …")
    seen = load_seen_topics()
    seen = update_seen_topics(selected, seen)
    save_seen_topics(seen)

    digest = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        **{sec: selected[sec] for sec in SECTIONS},
        "further_reading": further_reading,
    }

    with open(DIGEST_PATH, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)

    print(f"[process_feeds] Digest written → {DIGEST_PATH}")


if __name__ == "__main__":
    main()
