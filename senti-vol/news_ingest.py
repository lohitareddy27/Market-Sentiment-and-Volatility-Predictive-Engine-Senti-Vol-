from dotenv import load_dotenv
load_dotenv()

import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import re

from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc

NEWS_API = "https://newsapi.org/v2/everything"


CORE_TERMS = [
    "wti", "brent", "crude oil", "petroleum", "opec", "opec+", "nymex", "ice brent", "eia"
]
CTX_TERMS = [
    "price", "prices", "futures", "spot", "inventory", "production", "demand", "supply",
    "rig count", "sanctions", "outage", "pipeline", "refinery"
]
PROXIMITY_CHARS = 80
PAGE_SIZE = 100
DAYS_BACK = 7

# Regex patterns for filtering

CORE_RE = re.compile(r"\b(" + "|".join(re.escape(t) for t in CORE_TERMS) + r")\b", re.I)
CTX_RE  = re.compile(r"\b(" + "|".join(re.escape(t) for t in CTX_TERMS) + r")\b", re.I)

def is_relevant_text(text: str) -> bool:
    if not text:
        return False
    if not CORE_RE.search(text) or not CTX_RE.search(text):
        return False
    t = text.lower()
    core_hits = [m.start() for m in CORE_RE.finditer(t)]
    ctx_hits  = [m.start() for m in CTX_RE.finditer(t)]
    return any(abs(i - j) <= PROXIMITY_CHARS for i in core_hits for j in ctx_hits)

def relevant(text: str) -> bool:
    if not text:
        return False
    return bool(CORE_RE.search(text))

def fetch_news():
    api_key = get_env("NEWSAPI_KEY", required=True)
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=DAYS_BACK)

   # Query construction
    q = "(" + " OR ".join(f'"{t}"' if " " in t else t for t in CORE_TERMS) + ")"
    q += " AND (" + " OR ".join(CTX_TERMS) + ")"

    params = {
        "q": q,
        "language": "en",
        "from": from_dt.isoformat(),
        "to": to_dt.isoformat(),
        "sortBy": "relevancy",
        "searchIn": "title,description",
        "pageSize": PAGE_SIZE,
        "apiKey": api_key,
    }

    resp = requests.get(NEWS_API, params=params, timeout=30)
    resp.raise_for_status()
    js = resp.json()

    rows = []
    for a in js.get("articles", []) or []:
        title = (a.get("title") or "").strip()
        desc  = (a.get("description") or "").strip()
        blob = f"{title} {desc}".strip()

        if not (is_relevant_text(title) or is_relevant_text(blob)):
            continue

        published_at = a.get("publishedAt") or ""
        url = a.get("url") or ""
        article_id = stable_id(url) if url else stable_id(title + "|" + (published_at or ""))

        rows.append({
            "source": "newsapi",
            "article_id": article_id,
            "published_at": published_at,
            "title": title,
            "description": desc,
            "url": url,
            "author": a.get("author"),
            "ingested_at": now_utc(),
        })

    if not rows:
        return pd.DataFrame(columns=[
            "source","article_id","published_at","title","description","url","author","ingested_at"
        ])
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["article_id"], keep="first")
    return df

def main():
    init_logging()
    df = fetch_news()
    if df.empty:
        print("No relevant news rows after filtering.")
        return

    # Normalize
    df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["article_id"])

    target_table = bq_table("news_articles")
    staging_table = bq_table("news_articles_staging")

    upsert_to_bq(
        target_table=target_table,
        df=df,
        schema=None,
        key_fields=["article_id"],
        staging_table=staging_table
    )
    print(f"Ingest attempted: {len(df)} news rows.")

if __name__ == "__main__":
    main()
