from dotenv import load_dotenv
load_dotenv()

import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import re

from google.cloud import bigquery
from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc

NEWS_API = "https://newsapi.org/v2/everything"

QUERY = '"WTI" OR "crude oil" OR "CME" OR "NYMEX" OR "oil futures" OR "OPEC" OR "OPEC+" OR "Middle East oil" OR "CL=F" OR "Brent" OR "pipeline" OR "refinery" OR "EIA"'

OIL_KEYWORDS = [
    "oil", "crude oil", "crude", "wti", "brent", "petroleum", "opec", r"opec\+", "barrel", "oil price", "oil prices",
    "nyme?x", "cme", "pipeline", "refinery", "eia"
]
KEYWORD_RE = re.compile(r"\b(" + "|".join(k for k in OIL_KEYWORDS) + r")\b", flags=re.I)

def is_relevant_text(text: str):
    return bool(KEYWORD_RE.search(text or ""))

def fetch_news_newsapi():
    api_key = get_env("NEWSAPI_KEY", required=True)
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=7)

    params = {
        "q": QUERY,
        "language": "en",
        "from": from_dt.isoformat(),
        "to": to_dt.isoformat(),
        "sortBy": "publishedAt",
        "pageSize": 100,
        "apiKey": api_key,
    }
    r = requests.get(NEWS_API, params=params, timeout=45)
    r.raise_for_status()
    js = r.json()
    print(f"NewsAPI status: {js.get('status')} totalResults: {js.get('totalResults')}")
    print("Effective URL:", r.url)

    rows = []
    for a in js.get("articles", []) or []:
        published_at = a.get("publishedAt") or ""
        title = a.get("title") or ""
        description = a.get("description") or ""
        content = a.get("content") or ""
        combined = " ".join(x for x in [title, description, content] if x)

        if not is_relevant_text(combined):
            continue

        url = a.get("url") or None
        # URL as stable unique ID if present, else hash of title+published_at
        if url:
            article_id = stable_id(url)
        else:
            article_id = stable_id(title + "|" + (published_at or ""))

        rows.append({
            "source": "newsapi",
            "article_id": article_id,
            "published_at": published_at,
            "title": title,
            "description": description,
            "url": url,
            "author": a.get("author"),
            "raw": json.dumps(a, ensure_ascii=False),
            "ingested_at": now_utc(),
        })
    return pd.DataFrame(rows)


def main():
    init_logging()
    df = fetch_news_newsapi()
    if df.empty:
        print("No relevant news rows fetched after relevance filtering.")
        return

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    
    df = df.drop(columns=["tickers"], errors="ignore")
    # Ensure non-null article_id must be present
    df = df.dropna(subset=["article_id"])

    news_schema = [
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("article_id", "STRING"),
        bigquery.SchemaField("published_at", "TIMESTAMP"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("raw", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    target_table = bq_table("news_articles")
    staging_table = bq_table("news_articles_staging")

    upsert_to_bq(target_table, df, schema=news_schema, key_fields=["article_id"], staging_table=staging_table)
    print(f"Ingest attempted: {len(df)} news rows (staged + MERGE).")

if __name__ == "__main__":
    main()

