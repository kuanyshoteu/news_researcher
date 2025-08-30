# src/discover.py
import sys
import re
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import requests
import yaml
import feedparser

ROOT = Path(__file__).resolve().parents[1]
CFG_FEEDS = ROOT / "config" / "feed.yaml"
CFG_DISC = ROOT / "config" / "discovery.yaml"

UA = "ai-news-discover/1.0 (+https://example.local)"
TIMEOUT = 10

LINK_RE = re.compile(
    r'<link[^>]+rel=["\']alternate["\'][^>]+type=["\'](application/(rss\+xml|atom\+xml))["\'][^>]*>',
    re.IGNORECASE,
)
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)

def load_yaml(path, required=True):
    if not path.exists():
        if required:
            sys.exit(f"Не найден файл {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def save_yaml(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

def get_html(url):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
    r.raise_for_status()
    # Только head и верх страницы нужны для discovery
    return r.text[:200_000]

def discover_in_head(url):
    """
    Возвращает список абсолютных URL фидов, найденных в <head> через rel=alternate.
    """
    try:
        html = get_html(url)
    except Exception:
        return []
    base = url
    found = []
    for tag in LINK_RE.findall(html):
        # LINK_RE матчится на тег, но нам нужен href из тега целиком.
        # Переберём все совпадения линков повторно по блоку head:
        pass  
    # Ищем все <link ...> и фильтруем по type
    tags = re.findall(r"<link[^>]+>", html, flags=re.IGNORECASE)
    for t in tags:
        if re.search(r'rel=["\']alternate["\']', t, flags=re.IGNORECASE) and \
           re.search(r'type=["\']application/(rss\+xml|atom\+xml)["\']', t, flags=re.IGNORECASE):
            m = HREF_RE.search(t)
            if m:
                href = m.group(1).strip()
                abs_url = urljoin(base, href)
                found.append(abs_url)
    return list(dict.fromkeys(found))

def try_common_paths(base_url, try_paths):
    """
    Пробует типовые пути вроде /feed, /rss.xml. Возвращает существующие.
    """
    out = []
    for p in try_paths:
        test_url = urljoin(base_url.rstrip("/") + "/", p.lstrip("/"))
        try:
            r = requests.get(test_url, headers={"User-Agent": UA}, timeout=TIMEOUT)
            if r.ok and r.headers.get("content-type", "").lower().startswith(("application/rss+xml", "application/atom+xml", "text/xml", "application/xml")):
                out.append(test_url)
                continue
            # даже если content-type общий, попробуем распарсить feedparser'ом
            if r.ok and looks_like_xml(r.text):
                out.append(test_url)
        except Exception:
            continue
    return list(dict.fromkeys(out))

def looks_like_xml(text):
    t = text.strip()[:200].lower()
    return t.startswith("<?xml") or "<rss" in t or "<feed" in t

def normalize_home(url):
    """
    Нормализуем домен к схеме+хосту. Если дана категория/раздел, оставим путь.
    """
    u = urlparse(url)
    scheme = u.scheme or "https"
    netloc = u.netloc or u.path  # если передали без схемы
    path = u.path if u.netloc else ""
    return f"{scheme}://{netloc}{path or ''}"

def validate_feed(url, min_recent_days=30):
    """
    Валидирует, что это действительно RSS/Atom и что есть свежая запись.
    Возвращает (ok:bool, title:str|None, reason:str|None).
    """
    try:
        d = feedparser.parse(url)
    except Exception as ex:
        return False, None, f"parse_error: {ex}"
    if not getattr(d, "entries", None):
        return False, None, "no_entries"
    # проверим свежесть
    cutoff = datetime.now(timezone.utc) - timedelta(days=min_recent_days)
    fresh = False
    for e in d.entries[:10]:
        dt = None
        for attr in ("published_parsed", "updated_parsed"):
            if hasattr(e, attr) and getattr(e, attr):
                try:
                    dt = datetime(*getattr(e, attr)[:6], tzinfo=timezone.utc)
                    break
                except Exception:
                    pass
        if dt is None:
            # если дат нет совсем — считаем валидным, но без проверки свежести
            fresh = True
            break
        if dt >= cutoff:
            fresh = True
            break
    if not fresh:
        return False, getattr(d.feed, "title", None), "stale"
    return True, getattr(d.feed, "title", None), None

def load_feeds_yaml():
    data = load_yaml(CFG_FEEDS, required=False)
    feeds = data.get("feeds", []) if isinstance(data, dict) else []
    win = (data.get("window_hours", 24) if isinstance(data, dict) else 24)
    return {"window_hours": win, "feeds": feeds}

def add_feeds_to_yaml(new_urls):
    y = load_feeds_yaml()
    existing = set(y["feeds"])
    added = [u for u in new_urls if u not in existing]
    if not added:
        return []
    y["feeds"] = list(existing.union(added))
    save_yaml(CFG_FEEDS, y)
    return added

def unique_urls(urls):
    # Уберём якоря и нормализуем
    out = []
    seen = set()
    for u in urls:
        p = urlparse(u)
        nu = f"{p.scheme or 'https'}://{p.netloc}{p.path or ''}"
        if nu not in seen:
            seen.add(nu)
            out.append(nu)
    return out

def main():
    ap = argparse.ArgumentParser(description="Поиск RSS/Atom у заданных доменов и добавление в feed.yaml")
    args = ap.parse_args()

    disc = load_yaml(CFG_DISC, required=False)
    domains = (disc.get("domains") or [])
    if not domains:
        sys.exit("В discovery.yaml нет списка domains и не переданы --domains")

    try_paths = (disc.get("rules", {}) or {}).get("try_paths", ["/feed", "/rss", "/rss.xml", "/atom.xml", "/blog/rss.xml"])
    min_recent_days = (disc.get("rules", {}) or {}).get("min_recent_days", 30)

    candidates = []
    for d in domains:
        base = normalize_home(d)
        print(f"\n[DISCOVER] {base}")

        # 1) discovery в <head>
        try:
            head_links = discover_in_head(base)
            if head_links:
                print(f"  найдено в <head>: {len(head_links)}")
                for u in head_links[:5]:
                    print(f"    - {u}")
                candidates.extend(head_links)
        except Exception as ex:
            print(f"  ошибка head discovery: {ex}")

        # 2) типовые пути
        try:
            path_links = try_common_paths(base, try_paths)
            if path_links:
                print(f"  найдено по типовым путям: {len(path_links)}")
                for u in path_links:
                    print(f"    - {u}")
                candidates.extend(path_links)
        except Exception as ex:
            print(f"  ошибка try_paths: {ex}")

    # нормализуем кандидатов и убираем дубликаты
    candidates = unique_urls(candidates)

    # валидация фидов
    valid_feeds = []
    for u in candidates:
        ok, title, reason = validate_feed(u, min_recent_days=min_recent_days)
        if ok:
            print(f"[OK] {u}  ({title or 'без названия'})")
            valid_feeds.append(u)
        else:
            print(f"[SKIP] {u}  причина: {reason}")

    if not valid_feeds:
        print("\nНовых валидных лент не найдено.")
        return

    added = add_feeds_to_yaml(valid_feeds)
    if added:
        print(f"\nДобавлено в config/feed.yaml: {len(added)}")
        for u in added:
            print(f"  + {u}")
    else:
        print("\nВсе найденные ленты уже были в feed.yaml.")

if __name__ == "__main__":
    main()
