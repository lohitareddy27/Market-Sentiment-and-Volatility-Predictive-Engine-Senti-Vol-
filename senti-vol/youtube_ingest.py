from dotenv import load_dotenv
load_dotenv()

import json
import math
import logging
import re
import time
from datetime import datetime, timezone
from typing import List

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from common import (
    get_env,
    bq_table,
    upsert_to_bq,
    init_logging,
    stable_id,
    now_utc
)


YOUTUBE_API_KEY = get_env("YOUTUBE_API_KEY", required=True)
GCP_PROJECT_ID = get_env("GCP_PROJECT_ID", required=True)
BQ_DATASET = get_env("BQ_DATASET", required=True)

YT_KEYWORDS = [
    k.strip() for k in get_env(
        "YT_KEYWORDS",
        default="crude oil,WTI oil price,oil futures,energy markets,brent crude"
    ).split(",")
]

YT_MAX_VIDEOS_PER_KEYWORD = int(get_env("YT_MAX_VIDEOS_PER_KEYWORD", default="5"))
YT_MAX_COMMENTS_PER_VIDEO = int(get_env("YT_MAX_COMMENTS_PER_VIDEO", default="10"))

TARGET_TABLE = bq_table("youtube_comments")
STAGING_TABLE = bq_table("youtube_comments_staging")
KEY_FIELDS = ["comment_id"]

PAGE_SIZE = int(get_env("YT_PAGE_SIZE", default="10"))     
MAX_PAGES = int(get_env("YT_MAX_PAGES", default="10"))     
MAX_ITEMS = PAGE_SIZE * MAX_PAGES
SLEEP_BETWEEN_PAGES = float(get_env("YT_SLEEP_BETWEEN_PAGES", default="0.25"))

init_logging()
logger = logging.getLogger("youtube_ingest")


CORE_RE = re.compile(
    r"\b(wti|brent|crude oil|crude|petroleum|opec\+?|eia|nyme?x|ice(?: brent)?|barrel|refiner(?:y|ies)|upstream|downstream)\b",
    re.I,
)
CTX_RE = re.compile(
    r"\b(price|prices|futures|market|spot|curve|spread|backwardation|contango|hedg(?:e|ing)|inventory|stocks?|output|production|exports?|imports?|demand|supply|rig count|shutdown|outage|sanctions?|disruption|capacity|maintenance|pipeline|refinery|quota|cuts?)\b",
    re.I,
)
PROXIMITY_CHARS = int(get_env("YT_PROXIMITY_CHARS", default="60"))

GENERIC_RE = re.compile(
    r"\b(thanks|thank you|nice|great|good (video|content)|awesome|amazing|love (it|this)|"
    r"subscribe|sub 4 sub|like (and|&) (share|subscribe)|follow me|check my channel)\b",
    re.I,
)
EMOJI_ONLY_RE = re.compile(r"^[\W_]+$")  
LINK_SPAM_RE = re.compile(r"(https?://|www\.)", re.I)
REPETITIVE_PUNCT_RE = re.compile(r"([!?.,*])\1{2,}")  
NON_ASCII_HEAVY_RE = re.compile(r"[^\x00-\x7F]")

MIN_LEN = int(get_env("YT_MIN_COMMENT_LEN", default="10"))
MAX_UPPER_RATIO = float(get_env("YT_MAX_UPPER_RATIO", default="0.7"))  

def mostly_english(text: str) -> bool:
    if not text:
        return False
    non_ascii = len(NON_ASCII_HEAVY_RE.findall(text))
    return non_ascii <= max(3, len(text) * 0.25)

def has_enough_alpha_words(text: str, n: int = 2) -> bool:
    return len(re.findall(r"[A-Za-z]{3,}", text)) >= n

def not_shouty(text: str) -> bool:
    letters = re.findall(r"[A-Za-z]", text)
    if not letters:
        return True
    uppers = sum(1 for c in letters if c.isupper())
    ratio = uppers / max(1, len(letters))
    return ratio <= MAX_UPPER_RATIO

def is_relevant_comment(text: str) -> bool:
    if not text:
        return False
    if not CORE_RE.search(text) or not CTX_RE.search(text):
        return False
    t = text.lower()
    core_hits = [m.start() for m in CORE_RE.finditer(t)]
    ctx_hits  = [m.start() for m in CTX_RE.finditer(t)]
    if not core_hits or not ctx_hits:
        return False
    return any(abs(i - j) <= PROXIMITY_CHARS for i in core_hits for j in ctx_hits)

def is_quality_comment(text: str) -> bool:
    if not text:
        return False
    text = text.strip()
    if len(text) < MIN_LEN:
        return False
    if EMOJI_ONLY_RE.match(text):
        return False
    if GENERIC_RE.search(text):
        return False
    if LINK_SPAM_RE.search(text):
        return False
    if REPETITIVE_PUNCT_RE.search(text):
        return False
    if not has_enough_alpha_words(text, n=2):
        return False
    if not mostly_english(text):
        return False
    if not not_shouty(text):
        return False
    return True


def build_youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)

def search_videos(youtube, query: str, max_results: int = 10) -> List[str]:
    try:
        resp = youtube.search().list(
            q=query,
            part="id",
            type="video",
            maxResults=max_results,
            order="relevance"
        ).execute()
    except HttpError as e:
        logger.error("Search failed for %s: %s", query, e)
        return []
    items = resp.get("items", []) or []
    return [it["id"]["videoId"] for it in items if it.get("id", {}).get("videoId")]

def get_video_meta(youtube, video_id: str):
    try:
        resp = youtube.videos().list(
            part="snippet,statistics",
            id=video_id,
            maxResults=1
        ).execute()
        items = resp.get("items") or []
        if not items:
            return ("","",0)
        snip = items[0].get("snippet", {})
        stats = items[0].get("statistics", {})
        title = snip.get("title") or ""
        desc  = snip.get("description") or ""
        c = int(stats.get("commentCount") or 0)
        return (title, desc, c)
    except Exception as e:
        logger.warning("get_video_meta failed for %s: %s", video_id, e)
        return ("","",0)

def fetch_comments_for_video(youtube, video_id: str, max_comments: int = 10):
    comments = []
    req = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        textFormat="plainText",
        maxResults=100
    )

    collected = 0
    while req and collected < max_comments:
        try:
            resp = req.execute()
        except HttpError as e:
            logger.warning("Comments fetch failed for %s: %s", video_id, e)
            time.sleep(3)
            break

        for item in resp.get("items", []):
            try:
                s = item["snippet"]["topLevelComment"]["snippet"]
            except Exception:
                continue

            text = (s.get("textOriginal") or s.get("textDisplay") or "").strip()

            if not is_relevant_comment(text):
                continue
            if not is_quality_comment(text):
                continue

            comment_id = item.get("id") or stable_id(
                f"{video_id}|{text}|{s.get('publishedAt')}"
            )

            comments.append({
                "comment_id": comment_id,
                "video_id": video_id,
                "author": s.get("authorDisplayName"),
                "author_channel_id": (s.get("authorChannelId") or {}).get("value"),
                "text": text,
                "like_count": int(s.get("likeCount") or 0),
                "published_at": s.get("publishedAt"),
                "ingested_at": now_utc(),
            })
            collected += 1
            if collected >= max_comments:
                break

        req = youtube.commentThreads().list_next(req, resp)
        time.sleep(0.15)

    return comments


def build_df(rows, keyword: str):
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["keyword"] = keyword

    cols = [
        "comment_id", "video_id", "keyword", "author", "author_channel_id",
        "text", "like_count", "published_at", "ingested_at"
    ]

    for c in cols:
        if c not in df.columns:
            df[c] = None

    return df[cols]


def ingest_pages_and_build_summary(full_df):
    if full_df is None or full_df.empty:
        return [], 0

    full_df["published_at"] = pd.to_datetime(full_df["published_at"], utc=True, errors="coerce")
    if "ingested_at" in full_df.columns:
        full_df["ingested_at"] = pd.to_datetime(full_df["ingested_at"], utc=True, errors="coerce")
    else:
        full_df["ingested_at"] = pd.to_datetime(now_utc())

   
    full_df = full_df.dropna(subset=["comment_id"]).drop_duplicates(subset=["comment_id"], keep="first").reset_index(drop=True)

   
    if len(full_df) > MAX_ITEMS:
        full_df = full_df.iloc[:MAX_ITEMS].reset_index(drop=True)

    total_candidates = len(full_df)
    pages = [full_df.iloc[i:i + PAGE_SIZE].copy().reset_index(drop=True) for i in range(0, total_candidates, PAGE_SIZE)]

    all_summaries = []
    total_ingested = 0

    for idx, df_page in enumerate(pages, start=1):
        logger.info("Processing page %d/%d with %d rows", idx, len(pages), len(df_page))

     
        try:
            upsert_to_bq(
                target_table=TARGET_TABLE,
                df=df_page,
                schema=None,                # allow existing table schema to handle coercion
                key_fields=KEY_FIELDS,
                staging_table=STAGING_TABLE
            )
            ingested_count = len(df_page)
            logger.info("Upserted page %d -> %d rows", idx, ingested_count)
        except Exception as e:
            logger.error("Failed upserting page %d: %s", idx, e)
            ingested_count = 0

        total_ingested += ingested_count

    
        for _, row in df_page.iterrows():
            summary = {
                "comment_id": row.get("comment_id"),
                "video_id": row.get("video_id"),
                "keyword": row.get("keyword"),
                "author": row.get("author"),
                "text": row.get("text"),
                "like_count": int(row.get("like_count") or 0),
                "published_at": None if pd.isnull(row.get("published_at")) else row.get("published_at").isoformat(),
                "page_number": idx
            }
            all_summaries.append(summary)

        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_summaries, total_ingested


def main():
    logger.info("Starting YouTube pagewise ingestion (keywords=%s)", YT_KEYWORDS)

    youtube = build_youtube_client(YOUTUBE_API_KEY)
    collected_frames = []

    for kw in YT_KEYWORDS:
        logger.info("Searching for keyword='%s'", kw)
        video_ids = search_videos(youtube, kw, max_results=YT_MAX_VIDEOS_PER_KEYWORD)
        logger.info("Found %d videos for '%s'", len(video_ids), kw)

        for vid in video_ids:
            title, desc, comment_count = get_video_meta(youtube, vid)

            title_desc_blob = f"{title} {desc}".strip()
            if not (is_relevant_comment(title_desc_blob)):
                logger.info("Skipping video %s (not relevant by CORE+CTX+proximity)", vid)
                continue

            if comment_count == 0:
                logger.info("Skipping %s (no comments / disabled)", vid)
                continue

            logger.info("Fetching up to %d comments for video %s", YT_MAX_COMMENTS_PER_VIDEO, vid)
            rows = fetch_comments_for_video(youtube, vid, max_comments=YT_MAX_COMMENTS_PER_VIDEO)
            if not rows:
                logger.info("No relevant/quality comments collected for %s", vid)
                continue

            df = build_df(rows, kw)
            collected_frames.append(df)

    if not collected_frames:
        logger.info("No comments collected across keywords.")
        return

    full_df = pd.concat(collected_frames, ignore_index=True)


    all_summaries, total_ingested = ingest_pages_and_build_summary(full_df)

    logger.info("Finished pagewise ingestion. Attempted/ingested rows total: %d", total_ingested)


    total_records = len(all_summaries)
    total_pages = math.ceil(total_records / PAGE_SIZE) if PAGE_SIZE > 0 else 0

    aggregated = {
        "data": all_summaries,
        "pagination": {
            "total_records": total_records,
            "current_page": 1,
            "total_pages": total_pages,
            "next_page": 2 if total_pages > 1 else None,
            "prev_page": None
        }
    }


    logger.info("Aggregated pagination summary: records=%d pages=%d", total_records, total_pages)
    print(json.dumps(aggregated, indent=2, ensure_ascii=False))

    try:
        with open("youtube_aggregated_pagination_summary.json", "w", encoding="utf-8") as f:
            json.dump(aggregated, f, indent=2, ensure_ascii=False)
        logger.info("Wrote aggregated summary to youtube_aggregated_pagination_summary.json")
    except Exception as e:
        logger.error("Could not write aggregated summary to file: %s", e)

if __name__ == "__main__":
    main()
