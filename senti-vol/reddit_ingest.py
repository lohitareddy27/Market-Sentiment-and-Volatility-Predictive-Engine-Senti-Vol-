from dotenv import load_dotenv
load_dotenv()

import json
import math
import time
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import List, Tuple

import requests
import pandas as pd
from google.cloud import bigquery

from common import (
    get_env,
    bq_table,
    upsert_to_bq,
    init_logging,
    stable_id,
    now_utc
)


SUBREDDITS = [
    "wallstreetbets", "investing", "energy", "oilandgasworkers",
    "commodities", "economics", "financialmarkets", "news",
    "worldnews", "technology", "IndiaNews"
]

DAYS_BACK = int(get_env("REDDIT_DAYS_BACK", default="7"))
PAGE_SIZE = int(get_env("REDDIT_PAGE_SIZE", default="10"))    # rows per page to upsert
MAX_PAGES = int(get_env("REDDIT_MAX_PAGES", default="10"))
MAX_ITEMS = PAGE_SIZE * MAX_PAGES
SLEEP_BETWEEN_REQUESTS = float(get_env("REDDIT_SLEEP_BETWEEN_REQUESTS", default="0.25"))
SLEEP_BETWEEN_PAGES = float(get_env("REDDIT_SLEEP_BETWEEN_PAGES", default="0.25"))

TARGET_TABLE = bq_table("reddit_posts")
STAGING_TABLE = bq_table("reddit_posts_staging")
KEY_FIELDS = ["post_id"]


USER_AGENT = get_env("REDDIT_USER_AGENT", default="senti-vol-bot/0.1 (by /u/yourname)")


CORE_RE = re.compile(
    r"\b(wti|brent|crude oil|crude|petroleum|opec\+?|eia|nymex|ice(?: brent)?|barrel|refiner(?:y|ies)|upstream|downstream)\b",
    re.I
)
CTX_RE = re.compile(
    r"\b(price|prices|futures|spot|curve|spread|inventory|stocks?|output|production|exports?|imports?|demand|supply|rig count|shutdown|outage|sanctions?|disruption|capacity|maintenance|pipeline|refinery|quota|cuts?)\b",
    re.I
)
PROXIMITY_CHARS = int(get_env("REDDIT_PROXIMITY_CHARS", default="60"))

GENERIC_RE = re.compile(r"\b(please|upvote|downvote|crosspost|xpost|follow me|subscribe)\b", re.I)
MIN_LEN = int(get_env("REDDIT_MIN_LEN", default="10"))

def is_relevant_text(text: str) -> bool:
    if not text:
        return False
    if not CORE_RE.search(text) or not CTX_RE.search(text):
        return False
    t = text.lower()
    core_hits = [m.start() for m in CORE_RE.finditer(t)]
    ctx_hits = [m.start() for m in CTX_RE.finditer(t)]
    if not core_hits or not ctx_hits:
        return False
    return any(abs(i - j) <= PROXIMITY_CHARS for i in core_hits for j in ctx_hits)

def is_quality_text(text: str) -> bool:
    if not text:
        return False
    if len(text.strip()) < MIN_LEN:
        return False
    if GENERIC_RE.search(text):
        return False
    return True


def fetch_subreddit_public(subreddit: str, limit: int = 100) -> List[dict]:

    url = f"https://www.reddit.com/r/{subreddit}/new.json"
    headers = {"User-Agent": USER_AGENT}
    params = {"limit": min(100, limit)}  
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    except Exception as e:
        logging.warning("[PUBLIC] r/%s error fetching JSON: %s", subreddit, e)
        return []

    if resp.status_code == 401:
        logging.warning("[PUBLIC] r/%s error: 401 Unauthorized (possible auth required)", subreddit)
        return []
    if resp.status_code == 403:
        logging.warning("[PUBLIC] r/%s error: 403 Forbidden", subreddit)
        return []
    if resp.status_code == 429:
        logging.warning("[PUBLIC] r/%s error: 429 Too Many Requests — back off", subreddit)
        return []
    if not resp.ok:
        logging.warning("[PUBLIC] r/%s HTTP %d: %s", subreddit, resp.status_code, resp.text[:300])
        return []

    try:
        js = resp.json()
    except Exception as e:
        logging.warning("[PUBLIC] r/%s invalid JSON: %s", subreddit, e)
        return []

    children = js.get("data", {}).get("children", []) or []
    posts = []
    for ch in children:
        data = ch.get("data", {})
        posts.append(data)
    return posts

def extract_post_rows(posts: List[dict]) -> List[dict]:
    rows = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    for p in posts:
        created_utc = p.get("created_utc")
        try:
            published_at = datetime.fromtimestamp(float(created_utc), tz=timezone.utc) if created_utc else None
        except Exception:
            published_at = None

        if published_at and published_at < cutoff:
            continue

        title = (p.get("title") or "").strip()
        selftext = (p.get("selftext") or "").strip()
        body = (title + " " + selftext).strip()

        if not is_relevant_text(body) and not is_relevant_text(title):
            continue
        if not is_quality_text(body):
            continue

        url = p.get("url_overridden_by_dest") or p.get("url") or None
        post_id = p.get("id") or stable_id(url or title + "|" + (str(created_utc) if created_utc else ""))

        rows.append({
            "post_id": post_id,
            "subreddit": p.get("subreddit"),
            "title": title,
            "selftext": selftext,
            "author": p.get("author"),
            "score": int(p.get("score") or 0),
            "num_comments": int(p.get("num_comments") or 0),
            "url": url,
            "created_utc": published_at.isoformat() if published_at else None,
            "ingested_at": now_utc()
        })
    return rows


def ingest_pages_and_build_summary(full_df: pd.DataFrame) -> Tuple[List[dict], int]:
    if full_df is None or full_df.empty:
        return [], 0

    full_df["created_utc"] = pd.to_datetime(full_df["created_utc"], utc=True, errors="coerce")
    full_df = full_df.dropna(subset=["post_id"]).drop_duplicates(subset=["post_id"]).reset_index(drop=True)

    if len(full_df) > MAX_ITEMS:
        full_df = full_df.iloc[:MAX_ITEMS].reset_index(drop=True)

    pages = [full_df.iloc[i:i + PAGE_SIZE].copy().reset_index(drop=True) for i in range(0, len(full_df), PAGE_SIZE)]

    all_summaries = []
    total_ingested = 0

    for idx, df_page in enumerate(pages, start=1):
        logging.info("[REDDIT] Processing page %d/%d with %d rows.", idx, len(pages), len(df_page))

        try:
            upsert_to_bq(
                target_table=TARGET_TABLE,
                df=df_page,
                schema=None,   
                key_fields=KEY_FIELDS,
                staging_table=STAGING_TABLE
            )
            ingested_count = len(df_page)
            logging.info("[REDDIT] Ingested page %d (%d rows).", idx, ingested_count)
        except Exception as e:
            logging.error("[REDDIT] Failed upserting page %d: %s", idx, e)
            ingested_count = 0

        total_ingested += ingested_count

        for _, row in df_page.iterrows():
            all_summaries.append({
                "post_id": row.get("post_id"),
                "subreddit": row.get("subreddit"),
                "title": row.get("title"),
                "selftext": row.get("selftext"),
                "author": row.get("author"),
                "score": int(row.get("score") or 0),
                "num_comments": int(row.get("num_comments") or 0),
                "created_utc": None if pd.isnull(row.get("created_utc")) else row.get("created_utc").isoformat(),
                "page_number": idx
            })

        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_summaries, total_ingested


def main():
    init_logging()
    logging.info("[ENTRY] reddit_pagewise_ingest starting (public JSON only)")

    all_rows = []
    total_seen = 0
    total_kept = 0

    for sub in SUBREDDITS:
        logging.info("[PUBLIC] r/%s: fetching via public JSON", sub)
        posts = fetch_subreddit_public(sub, limit=100)
        seen = len(posts)
        total_seen += seen
        if not posts:
            logging.info("[PUBLIC] r/%s: seen=%d, kept=0", sub, seen)
            continue

        rows = extract_post_rows(posts)
        kept = len(rows)
        total_kept += kept
        logging.info("[PUBLIC] r/%s: seen=%d, kept=%d", sub, seen, kept)

        all_rows.extend(rows)
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    logging.info("[PUBLIC] total_seen=%d total_kept=%d (days_back=%d)", total_seen, total_kept, DAYS_BACK)

    if not all_rows:
        logging.info("[REDDIT] No candidate posts after filtering.")
        return


    df = pd.DataFrame(all_rows)


    reddit_schema = [
        bigquery.SchemaField("post_id", "STRING"),
        bigquery.SchemaField("subreddit", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("selftext", "STRING"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("score", "INT64"),
        bigquery.SchemaField("num_comments", "INT64"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("created_utc", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

   
    try:
        upsert_to_bq(
            target_table=TARGET_TABLE,
            df=df.head(0),
            schema=reddit_schema,
            key_fields=KEY_FIELDS,
            staging_table=STAGING_TABLE
        )
    except Exception as e:
        logging.warning("[REDDIT] Could not create/validate table schema via upsert_to_bq: %s", e)


    pages_summary, total_ingested = ingest_pages_and_build_summary(df)

    logging.info("[REDDIT] Finished run. Pages: %d, Attempted/ingested rows total: %d/%d.",
                 math.ceil(len(pages_summary) / PAGE_SIZE) if PAGE_SIZE > 0 else 0,
                 total_ingested, len(df))


    aggregated = {
        "data": pages_summary,
        "pagination": {
            "total_records": len(pages_summary),
            "current_page": 1,
            "total_pages": math.ceil(len(pages_summary) / PAGE_SIZE) if PAGE_SIZE > 0 else 0,
            "next_page": 2 if len(pages_summary) > PAGE_SIZE else None,
            "prev_page": None
        }
    }
    print(json.dumps(aggregated, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()


