from dotenv import load_dotenv
load_dotenv()

import json
import math
import re
import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc


FINNHUB_NEWS = "https://finnhub.io/api/v1/news"


PAGE_SIZE = 10         
MAX_PAGES = 10         
SLEEP_BETWEEN_PAGES = 0.25 


CORE_RE = re.compile(
    r"\b(wti|brent|crude oil|crude|petroleum|opec\+?|eia|nyme?x|ice(?: brent)?|barrel?s?|refiner(?:y|ies)|upstream|midstream|downstream)\b",
    re.I,
)
CTX_RE = re.compile(
    r"\b(price|prices|futures|spot|curve|spread|backwardation|contango|hedg(?:e|ing)|inventory|stocks?|output|production|exports?|imports?|demand|supply|rig count|shutdown|outage|sanctions?|disruption|capacity|maintenance|pipeline|refinery)\b",
    re.I,
)
PROXIMITY_CHARS = 40

def is_relevant(text: str) -> bool:
    if not text:
        return False
    if not CORE_RE.search(text) or not CTX_RE.search(text):
        return False
    t = text.lower()
    core_hits = [m.start() for m in CORE_RE.finditer(t)]
    ctx_hits  = [m.start() for m in CTX_RE.finditer(t)]
    return any(abs(i - j) <= PROXIMITY_CHARS for i in core_hits for j in ctx_hits)


def ingest_page(df_page):
    if df_page is None or df_page.empty:
        print("[FINNHUB] ingest_page called with empty df — skipping.")
        return 0

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

    try:
        upsert_to_bq(
            target_table=target_table,
            df=df_page,
            schema=news_schema,
            key_fields=["article_id"],
            staging_table=staging_table
        )
        print(f"[FINNHUB] Ingested {len(df_page)} rows from this page.")
        return len(df_page)
    except Exception as e:
        print(f"[FINNHUB] ERROR ingesting page: {e}")
        return 0


def main():
    init_logging()

    token = get_env("FINNHUB_API_KEY", required=True)
    days_back = int(get_env("FINNHUB_DAYS_BACK", default="7"))

    to_d = datetime.now(timezone.utc).date()
    from_d = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()

    params = {
        "category": "general",
        "from": str(from_d),
        "to": str(to_d),
        "token": token
    }

    print(f"[FINNHUB] Fetching news from {from_d} to {to_d} (category=general)")
    r = requests.get(FINNHUB_NEWS, params=params, timeout=45)
    r.raise_for_status()
    articles = r.json() or []


    rows = []
    for a in articles:
        title = (a.get("headline") or "").strip()
        summary = (a.get("summary") or "").strip()

        if not (is_relevant(title) or is_relevant(f"{title} {summary}".strip())):
            continue

        ts = a.get("datetime")
        published_at = pd.to_datetime(ts, unit="s", utc=True, errors="coerce") if ts else None
        url = a.get("url") or None
        aid = stable_id(url) if url else stable_id(title + "|" + str(published_at))

        rows.append({
            "source": "finnhub",
            "article_id": aid,
            "published_at": published_at,
            "title": title,
            "description": summary,
            "url": url,
            "author": a.get("source"),
            "raw": json.dumps(a, ensure_ascii=False),
            "ingested_at": now_utc(),
        })

    if not rows:
        print("[FINNHUB] no relevant rows after filtering; exiting.")
        return

    # Convert to DataFrame 
    df_all = pd.DataFrame(rows)
    df_all["published_at"] = pd.to_datetime(df_all["published_at"], utc=True, errors="coerce")
    df_all = df_all.dropna(subset=["article_id"])
    df_all = df_all.drop_duplicates(subset=["article_id"], keep="first")

 
    max_items = PAGE_SIZE * MAX_PAGES
    if len(df_all) > max_items:
        df_all = df_all.iloc[:max_items].reset_index(drop=True)

    total_candidates = len(df_all)
    print(f"[FINNHUB] {total_candidates} candidate articles after filtering (capped to {max_items}).")

    # Splitting
    pages = [df_all.iloc[i:i + PAGE_SIZE].copy().reset_index(drop=True)
             for i in range(0, total_candidates, PAGE_SIZE)]

    total_ingested = 0
    all_ingested_summaries = []

    for idx, df_page in enumerate(pages, start=1):
        print(f"[FINNHUB] Processing page {idx}/{len(pages)} with {len(df_page)} rows.")
        ingested_count = ingest_page(df_page)
        total_ingested += ingested_count

        if ingested_count > 0:
            for _, row in df_page.iterrows():
                summary = {
                    "article_id": row.get("article_id"),
                    "title": row.get("title"),
                    "published_at": None if pd.isnull(row.get("published_at")) else row.get("published_at").isoformat(),
                    "url": row.get("url"),
                    "source": row.get("source"),
                    "page_number": idx
                }
                all_ingested_summaries.append(summary)

        time.sleep(SLEEP_BETWEEN_PAGES)

    print(f"[FINNHUB] Finished run. Pages: {len(pages)}, Ingested rows total: {total_ingested}.")

    total_records = len(all_ingested_summaries)
    total_pages = math.ceil(total_records / PAGE_SIZE) if PAGE_SIZE > 0 else 0

    aggregated = {
        "data": all_ingested_summaries,
        "pagination": {
            "total_records": total_records,
            "current_page": 1,
            "total_pages": total_pages,
            "next_page": 2 if total_pages > 1 else None,
            "prev_page": None
        }
    }


    print("\nAggregated Pagination JSON")
    print(json.dumps(aggregated, indent=2, ensure_ascii=False))

    try:
        with open("finnhub_aggregated_pagination_summary.json", "w", encoding="utf-8") as f:
            json.dump(aggregated, f, indent=2, ensure_ascii=False)
        print("[FINNHUB] Wrote aggregated summary to finnhub_aggregated_pagination_summary.json")
    except Exception as e:
        print(f"[FINNHUB] Could not write aggregated summary to file: {e}")

    return aggregated

if __name__ == "__main__":
    main()
