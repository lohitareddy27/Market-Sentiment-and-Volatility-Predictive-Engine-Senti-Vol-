from dotenv import load_dotenv
load_dotenv()

import feedparser
import re
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc

YAHOO_FEED_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL=F&region=US&lang=en-US"


CORE_RE = re.compile(
    r"\b(wti|brent|crude oil|crude|petroleum|opec\+?|eia|nyme?x|ice(?: brent)?|barrel?s?|refiner(?:y|ies)|upstream|midstream|downstream)\b",
    re.I,
)
CTX_RE = re.compile(
    r"\b(price|prices|futures|spot|curve|spread|backwardation|contango|hedg(?:e|ing)|inventory|stocks?|output|production|exports?|imports?|demand|supply|rig count|shutdown|outage|sanctions?|disruption|capacity|maintenance|pipeline|refinery)\b",
    re.I,
)
PROXIMITY_CHARS = 80


def is_relevant(text: str) -> bool:
    if not text:
        return False
    if not CORE_RE.search(text) or not CTX_RE.search(text):
        return False

    t = text.lower()
    core_hits = [m.start() for m in CORE_RE.finditer(t)]
    ctx_hits = [m.start() for m in CTX_RE.finditer(t)]

    return any(abs(i - j) <= PROXIMITY_CHARS for i in core_hits for j in ctx_hits)


def fetch_yahoo_news():
    print(f"[YAHOO] Fetching RSS feed from {YAHOO_FEED_URL}")
    feed = feedparser.parse(YAHOO_FEED_URL)
    if not feed.entries:
        print("[YAHOO] No entries returned from RSS feed.")
        return pd.DataFrame()

    rows = []
    for entry in feed.entries:
        title = entry.get("title", "") or ""
        summary = entry.get("summary", "") or ""
        link = entry.get("link", "") or ""
        published_parsed = entry.get("published_parsed")

        published_at = (
            datetime(*published_parsed[:6], tzinfo=timezone.utc)
            if published_parsed
            else None
        )

        if not (is_relevant(title) or is_relevant(f"{title} {summary}".strip())):
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
            "ingested_at": now_utc(),
        })

    return pd.DataFrame(rows)


def main():
    init_logging()
    df = fetch_yahoo_news()

    if df.empty:
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
