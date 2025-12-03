from dotenv import load_dotenv
load_dotenv()

import json
import time
import logging
import re
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
    """Fetch comments, applying ONLY the regex relevance filter before collecting."""
    comments = []
    req = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        textFormat="plainText",
        maxResults=10
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



def main():
    logger.info("Starting YouTube ingestion (keywords=%s)", YT_KEYWORDS)

    youtube = build_youtube_client(YOUTUBE_API_KEY)
    frames = []

    for kw in YT_KEYWORDS:
        logger.info("Searching for keyword='%s'", kw)
        videos = search_videos(youtube, kw, max_results=YT_MAX_VIDEOS_PER_KEYWORD)
        logger.info("Found %d videos for '%s'", len(videos), kw)

        for vid in videos:
            title, desc, comment_count = get_video_meta(youtube, vid)

       
            title_desc_blob = f"{title} {desc}".strip()
            if not is_relevant_comment(title_desc_blob):
                logger.info("Skipping video %s (not relevant by CORE+CTX+proximity)", vid)
                continue

            if comment_count == 0:
                logger.info("Skipping %s (no comments / disabled)", vid)
                continue

            logger.info("Fetching up to %d comments for video %s", YT_MAX_COMMENTS_PER_VIDEO, vid)
            rows = fetch_comments_for_video(youtube, vid, max_comments=YT_MAX_COMMENTS_PER_VIDEO)
            if not rows:
                logger.info("No relevant comments collected for %s", vid)
                continue

            df = build_df(rows, kw)
            frames.append(df)

    if not frames:
        logger.info("No comments collected across all keywords.")
        return

    full_df = pd.concat(frames, ignore_index=True)

    # Convert timestamps
    full_df["published_at"] = pd.to_datetime(full_df["published_at"], utc=True, errors="coerce")

    logger.info("Upserting %d comments → %s", len(full_df), TARGET_TABLE)

    upsert_to_bq(
        target_table=TARGET_TABLE,
        df=full_df,
        schema=None,               
        key_fields=KEY_FIELDS,
        staging_table=STAGING_TABLE
    )

    logger.info("YouTube ingestion complete.")


if __name__ == "__main__":
    main()
