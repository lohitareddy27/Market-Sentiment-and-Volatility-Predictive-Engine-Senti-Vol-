# yahoonews_ingest.py

from dotenv import load_dotenv
load_dotenv()

import feedparser
import json
import re
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc

YAHOO_FEED_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL=F&region=US&lang=en-US"

OIL_RE = re.compile(r"\b(oil|crude|wti|brent|petroleum|energy|opec|barrel|futures|refinery|gas|fuel)\b", re.I)


def fetch_yahoo_news():
    print(f"[YAHOO] Fetching RSS feed from {YAHOO_FEED_URL}")
    feed = feedparser.parse(YAHOO_FEED_URL)
    if not feed.entries:
        print("[YAHOO] No entries returned from RSS feed.")
        return pd.DataFrame()

    rows = []
    for entry in feed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        link = entry.get("link", "")
        published_parsed = entry.get("published_parsed")

        # Convert published date
        if published_parsed:
            published_at = datetime(*published_parsed[:6], tzinfo=timezone.utc)
        else:
            published_at = None

        # Only keep relevant news
        if not OIL_RE.search(title + " " + summary):
            continue

        article_id = stable_id(link or title)
        rows.append({
            "source": "yahoo_finance",
            "article_id": article_id,
            "published_at": published_at,
            "title": title,
            "description": summary,
            "url": link,
            "author": None,
            "raw": json.dumps(entry, ensure_ascii=False),
            "ingested_at": now_utc(),
        })

    return pd.DataFrame(rows)

def main():
    init_logging()
    df = fetch_yahoo_news()
    if df is None or df.empty:
        print("[YAHOO] no rows")
        return

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
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

    upsert_to_bq(
        target_table=target_table,
        df=df,
        schema=news_schema,
        key_fields=["article_id"],
        staging_table=staging_table
    )
    print(f"[YAHOO] upserted {len(df)} rows")

if __name__ == "__main__":
    main()
