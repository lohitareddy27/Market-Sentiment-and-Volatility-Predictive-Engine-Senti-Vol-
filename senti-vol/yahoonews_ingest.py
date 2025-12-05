from dotenv import load_dotenv
load_dotenv()

import feedparser
import pandas as pd
import re
from datetime import datetime, timezone
from google.cloud import bigquery

from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc


YAHOO_FEED_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL=F&region=US&lang=en-US"


def fetch_yahoo_news():
    print(f"[YAHOO] Fetching RSS feed from {YAHOO_FEED_URL}")
    feed = feedparser.parse(YAHOO_FEED_URL)
    if not getattr(feed, "entries", None):
        print("[YAHOO] No entries returned from RSS feed.")
        return pd.DataFrame()

    rows = []
    for entry in feed.entries:
        title = entry.get("title", "") or ""
        summary = entry.get("summary", "") or ""
        link = entry.get("link", "") or ""
        published_parsed = entry.get("published_parsed")

        published_at = datetime(*published_parsed[:6], tzinfo=timezone.utc) if published_parsed else None


        # if not (is_relevant(title) or is_relevant(f"{title} {summary}".strip())):
        #     continue

        article_id = stable_id(link or title)
        rows.append({
            "source": "yahoo_finance",
            "article_id": article_id,
            "published_at": published_at,
            "title": title,
            "description": summary,
            "url": link,
            "author": None,
            "ingested_at": now_utc(),
        })

    if not rows:
        return pd.DataFrame(columns=[
            "source", "article_id", "published_at", "title",
            "description", "url", "author", "ingested_at"
        ])

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["article_id"], keep="first")
    return df

def main():
    init_logging()
    df = fetch_yahoo_news()
    if df is None or df.empty:
        print("[YAHOO] no rows")
        return

   
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
    print(f"[YAHOO] upserted {len(df)} rows")

if __name__ == "__main__":
    main()
