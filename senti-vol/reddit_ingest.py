# reddit_ingest.py

from dotenv import load_dotenv
load_dotenv()

import os, time, json
import pandas as pd
import praw
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from common import get_env, bq_table, upsert_to_bq, append_to_bq, init_logging


SUBS = [
    "wallstreetbets","investing","energy","oilandgasworkers",
    "commodities","economics","financialmarkets","news","worldnews","technology","IndiaNews"
]

KEYWORDS = [
    "wti","crude","oil","opec","opec+","cl=f","brent",
    "pipeline","refinery","eia","saudi","iran","nigeria"
]

DAYS_BACK = int(get_env("REDDIT_DAYS_BACK", default="3"))
APPLY_KEYWORD_FILTER = get_env("REDDIT_KEYWORD_FILTER", default="1") != "0"
USE_PUBLIC = get_env("REDDIT_USE_PUBLIC", default="0") == "1"
VERBOSE = True



def reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=get_env("REDDIT_CLIENT_ID", required=True),
        client_secret=get_env("REDDIT_CLIENT_SECRET", required=True),
        user_agent=get_env("REDDIT_USER_AGENT", required=True),
        username=get_env("REDDIT_USERNAME", required=True),
        password=get_env("REDDIT_PASSWORD", required=True),
    )


def cutoff_ts():
    return (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).timestamp()


def keep_post(s) -> bool:
    if getattr(s, "created_utc", 0) < cutoff_ts():
        return False
    if not APPLY_KEYWORD_FILTER:
        return True
    title = (getattr(s, "title", "") or "")
    body = (getattr(s, "selftext", "") or "")
    blob = f"{title} {body}".lower()
    return any(k in blob for k in KEYWORDS)


def fmt_row(s, sub):
    title = getattr(s, "title", "") or ""
    body  = getattr(s, "selftext", "") or ""
    created_at = datetime.fromtimestamp(getattr(s, "created_utc", 0), tz=timezone.utc).isoformat()
    return {
        "source": "reddit",
        "post_id": getattr(s, "id", None),
        "created_at": created_at,
        "subreddit": sub,
        "author": str(getattr(s, "author", None)) if getattr(s, "author", None) else None,
        "title": title,
        "selftext": body,
        "url": f"https://reddit.com{getattr(s, 'permalink', '')}",
        "score": int(getattr(s, "score", 0)),
        "num_comments": int(getattr(s, "num_comments", 0)),
        "raw": {
            "permalink": getattr(s, "permalink", ""),
            "over_18": bool(getattr(s, "over_18", False)),
            "stickied": bool(getattr(s, "stickied", False)),
        },
    }


def fetch_praw() -> pd.DataFrame:
    r = reddit_client()
    rows = []
    total_seen = 0
    for sub in SUBS:
        seen = kept = 0
        for s in r.subreddit(sub).new(limit=200):
            seen += 1
            total_seen += 1
            if keep_post(s):
                kept += 1
                rows.append(fmt_row(s, sub))
            time.sleep(0.03)
        if VERBOSE:
            print(f"[PRAW] r/{sub}: seen={seen}, kept={kept}")
    if VERBOSE:
        print(f"[PRAW] total_seen={total_seen}, total_kept={len(rows)} (days_back={DAYS_BACK}, kw_filter={APPLY_KEYWORD_FILTER})")
        if len(rows) > 0:
            print("[PRAW] example:", rows[0]["title"][:120])
    return pd.DataFrame(rows)

# Public JSON (no OAuth) 
UA = {"User-Agent": "SentiVol/0.1 (contact: test@example.com)"}

def fetch_public_json() -> pd.DataFrame:
    rows = []
    cut = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    for sub in SUBS:
        try:
            url = f"https://www.reddit.com/r/{sub}/new.json"
            js = requests.get(url, params={"limit": 100}, headers=UA, timeout=30).json()
            children = js.get("data", {}).get("children", []) or []
        except Exception as e:
            print(f"[PUBLIC] r/{sub} error:", e)
            continue

        seen = kept = 0
        for ch in children:
            d = ch.get("data", {}) or {}
            seen += 1
            created = datetime.fromtimestamp(d.get("created_utc", 0), tz=timezone.utc)
            if created < cut:
                continue
            title = d.get("title", "") or ""
            body  = d.get("selftext", "") or ""
            blob  = (title + " " + body).lower()
            if APPLY_KEYWORD_FILTER and not any(k in blob for k in KEYWORDS):
                continue
            kept += 1
            rows.append({
                "source": "reddit_public",
                "post_id": d.get("id"),
                "created_at": created.isoformat(),
                "subreddit": sub,
                "author": d.get("author"),
                "title": title,
                "selftext": body,
                "url": "https://reddit.com" + (d.get("permalink") or ""),
                "score": int(d.get("score", 0)),
                "num_comments": int(d.get("num_comments", 0) or 0),
                "raw": {"permalink": d.get("permalink"), "over_18": bool(d.get("over_18", False)), "stickied": bool(d.get("stickied", False))}
            })
        if VERBOSE:
            print(f"[PUBLIC] r/{sub}: seen={seen}, kept={kept}")
        time.sleep(0.3)
    if VERBOSE:
        print(f"[PUBLIC] total_kept={len(rows)} (days_back={DAYS_BACK}, kw_filter={APPLY_KEYWORD_FILTER})")
        if len(rows) > 0:
            print("[PUBLIC] example:", rows[0]["title"][:120])
    return pd.DataFrame(rows)


def main():
    init_logging()
    print('[ENTRY] reddit_ingest starting')

    df = pd.DataFrame()

    if not USE_PUBLIC:
        try:
            print('[PRAW] Attempting OAuth fetch via PRAW')
            df = fetch_praw()
        except Exception as e:
            print('[PRAW] error:', e)
            df = pd.DataFrame()

    if df.empty:
        print('[INFO] PRAW returned 0 rows; trying public JSON fallback…')
        df = fetch_public_json()

    if df.empty:
        print('No reddit rows fetched (PRAW + public fallback).')
        return

    # Normalize created_at
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")

    
    # Ensure num_comments is integer
    if "num_comments" in df.columns:
        df["num_comments"] = pd.to_numeric(df["num_comments"], errors="coerce").fillna(0).astype('int64')

    # Ensure raw is serialized
    if "raw" in df.columns:
        df["raw"] = df["raw"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else (str(x) if x is not None else None))

    target_table = bq_table("reddit_posts")
    staging_table = bq_table("reddit_posts_staging")

    upsert_to_bq(target_table, df, schema=None, key_fields=["post_id"], staging_table=staging_table)
    print(f"[DONE] Ingest attempted: {len(df)} reddit rows (staged + MERGE).")

if __name__ == "__main__":
    main()

