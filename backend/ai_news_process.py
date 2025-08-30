# src/ai_news_process.py
# Зависимости: feedparser, trafilatura, pyyaml
import os, re, json, time, html
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import yaml
import feedparser
import trafilatura

ROOT = Path(__file__).resolve().parents[1]
CFG = ROOT / "config" / "feed.yaml"

# --------- настройки ----------
WINDOW_HOURS_DEFAULT = 24
AI_KEYWORDS = [
    # ru
    "искусственный интеллект","нейросет","дз","машинное обучение","глубокое обучение",
    "большая языковая модель","llm","модель","генеративн","ai",
    # en
    "artificial intelligence","neural","machine learning","deep learning",
    "large language model","generative","foundation model","llm",
]
MAX_TEXT_PER_ITEM = 1500
SIMPLE_DUP_JACCARD = 0.7
HTTP_TIMEOUT = 15
SLEEP_BETWEEN = 0.2

# ---------- конфиг ----------
def load_feeds():
    if not CFG.exists():
        raise SystemExit(f"Нет файла {CFG}")
    cfg = yaml.safe_load(CFG.read_text(encoding="utf-8")) or {}
    feeds = cfg.get("feeds", [])
    window = int(cfg.get("window_hours", WINDOW_HOURS_DEFAULT))
    if not feeds:
        raise SystemExit("В config/feeds.yaml пустой список feeds")
    return feeds, window

def clean_text(t: str) -> str:
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def is_ai_related(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in AI_KEYWORDS)

# ---------- загрузка из RSS ----------
def fetch_entries(feeds, window_hours):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    items = []
    for url in feeds:
        try:
            d = feedparser.parse(url)
        except Exception:
            continue
        entries = getattr(d, "entries", []) or []
        for e in entries:
            link = getattr(e, "link", None)
            title = getattr(e, "title", "") or ""
            if not link or not title:
                continue
            # дата
            dt = None
            for attr in ("published_parsed", "updated_parsed"):
                if hasattr(e, attr) and getattr(e, attr):
                    try:
                        dt = datetime(*getattr(e, attr)[:6], tzinfo=timezone.utc)
                        break
                    except Exception:
                        pass
            if dt and dt < cutoff:
                continue
            summary = html.unescape(getattr(e, "summary", "") or "")
            items.append({
                "title": title.strip(),
                "link": link.strip(),
                "source": urlparse(link).netloc,
                "summary": clean_text(summary),
                "date": dt.isoformat() if dt else None,
            })
    return items

# ---------- вытаскивание текста ----------
def fetch_article_text(url: str) -> str:
    try:
        html_doc = trafilatura.fetch_url(url, timeout=HTTP_TIMEOUT, no_ssl=True)
        if not html_doc:
            return ""
        txt = trafilatura.extract(
            html_doc,
            include_comments=False,
            include_tables=False,
            favor_recall=True
        ) or ""
        return clean_text(txt)
    except Exception:
        return ""

# ---------- отбор AI-новостей ----------
def filter_ai(items):
    out = []
    for it in items:
        basis = (it["title"] + " " + it.get("summary","")).strip()
        if not is_ai_related(basis):
            full = fetch_article_text(it["link"])
            it["text"] = full
            if not full or not is_ai_related((it["title"] + " " + full)):
                continue
        else:
            it["text"] = fetch_article_text(it["link"])
        out.append(it)
        time.sleep(SLEEP_BETWEEN)
    return out

# ---------- дедуп ----------
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]{2,}", flags=re.UNICODE)

def normalize_words(s: str):
    return set(WORD_RE.findall(s.lower()))

def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

def simple_dedup(items):
    kept, sigs = [], []
    for it in items:
        text_basis = (it["title"] + " " + (it.get("text") or it.get("summary","")))
        sig = normalize_words(text_basis)
        if any(jaccard(sig, sj) >= SIMPLE_DUP_JACCARD for sj in sigs):
            continue
        kept.append(it)
        sigs.append(sig)
    return kept

# ---------- пары ----------
def to_triplets(items):
    """
    Возвращает список [текст, ссылка, дата].
    """
    triples = []
    for it in items:
        title = it.get("title", "").strip()
        summary = it.get("summary", "").strip()
        full = it.get("text", "").strip()
        url = it["link"]
        date = it.get("date")

        if title and summary:
            text = f"{title} — {summary}"
        elif title:
            text = title
        elif full:
            text = full[:MAX_TEXT_PER_ITEM]
        else:
            text = url

        if len(text) > MAX_TEXT_PER_ITEM:
            text = text[:MAX_TEXT_PER_ITEM].rsplit(" ", 1)[0] + "…"

        triples.append([text, url, date])
    return triples

# ---------- main ----------
def main():
    feeds, window = load_feeds()
    raw = fetch_entries(feeds, window)
    if not raw:
        print("[]")
        return
    ai_items = filter_ai(raw)
    if not ai_items:
        print("[]")
        return
    unique_items = simple_dedup(ai_items)
    triples = to_triplets(unique_items)
    print(json.dumps(triples, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
