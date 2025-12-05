from dotenv import load_dotenv
load_dotenv()

import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import re

from common import get_env, bq_table, upsert_to_bq, init_logging, now_utc


SUBS = [
    "wallstreetbets","investing","energy","oilandgasworkers",
    "commodities","economics","financialmarkets","news","worldnews"
]

DAYS_BACK = int(get_env("REDDIT_DAYS_BACK", default="7"))
VERBOSE = True

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
    ctx_hits  = [m.start() for m in CTX_RE.finditer(t)]
    return any(abs(i - j) <= PROXIMITY_CHARS for i in core_hits for j in ctx_hits)

# Public JSON fetcher
UA = {"User-Agent": get_env("REDDIT_USER_AGENT", default="SentiVol/0.1 (contact: test@example.com)")}

def fetch_public_json() -> pd.DataFrame:
    rows = []
    cut = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    for sub in SUBS:
        try:
            url = f"https://www.reddit.com/r/{sub}/new.json"
            res = requests.get(url, params={"limit": 100}, headers=UA, timeout=30)
            res.raise_for_status()
            js = res.json()
            children = js.get("data", {}).get("children", []) or []
        except Exception as e:
            if VERBOSE:
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
            blob  = f"{title} {body}".strip()
            if not (is_relevant(title) or is_relevant(blob)):
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
                "score": int(d.get("score", 0) or 0),
                "num_comments": int(d.get("num_comments", 0) or 0),
                "ingested_at": now_utc(),
            })
        if VERBOSE:
            print(f"[PUBLIC] r/{sub}: seen={seen}, kept={kept}")
        time.sleep(0.3)

    if VERBOSE:
        print(f"[PUBLIC] total_kept={len(rows)} (days_back={DAYS_BACK})")
    if not rows:
        return pd.DataFrame(columns=[
            "source","post_id","created_at","subreddit","author","title",
            "selftext","url","score","num_comments","ingested_at"
        ])
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["post_id"], keep="first")
    return df

def main():
    init_logging()

    df = fetch_public_json()

    if df.empty:
        print('No reddit rows fetched (public JSON).')
        return

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")

    if "num_comments" in df.columns:
        df["num_comments"] = pd.to_numeric(df["num_comments"], errors="coerce").fillna(0).astype('int64')
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype('int64')

    target_table = bq_table("reddit_posts")
    staging_table = bq_table("reddit_posts_staging")

  
    upsert_to_bq(target_table, df, schema=None, key_fields=["post_id"], staging_table=staging_table)
    print(f"[DONE] Ingest attempted: {len(df)} reddit rows (staged + MERGE).")

if __name__ == "__main__":
    main()
