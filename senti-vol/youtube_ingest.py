# youtube_ingest.py

from dotenv import load_dotenv
load_dotenv()

import json
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

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

YT_MAX_VIDEOS_PER_KEYWORD = int(get_env("YT_MAX_VIDEOS_PER_KEYWORD", default="10"))
YT_MAX_COMMENTS_PER_VIDEO = int(get_env("YT_MAX_COMMENTS_PER_VIDEO", default="500"))

TARGET_TABLE = bq_table("youtube_comments")
STAGING_TABLE = bq_table("youtube_comments_staging")
KEY_FIELDS = ["comment_id"]


init_logging()
logger = logging.getLogger("youtube_ingest")


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


def get_comment_count(youtube, video_id: str) -> int:

    try:
        resp = youtube.videos().list(
            part="statistics",
            id=video_id,
            maxResults=1
        ).execute()
        items = resp.get("items") or []
        if not items:
            return 0
        stat = items[0].get("statistics", {})
        return int(stat.get("commentCount") or 0)
    except Exception:
        return 0


def fetch_comments_for_video(youtube, video_id: str, max_comments: int = 500):
    # Fetch top 100 comments
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

            comment_id = item.get("id") or stable_id(
                f"{video_id}|{s.get('textOriginal')}|{s.get('publishedAt')}"
            )

            comments.append({
                "comment_id": comment_id,
                "video_id": video_id,
                "author": s.get("authorDisplayName"),
                "author_channel_id": (s.get("authorChannelId") or {}).get("value"),
                "text": s.get("textOriginal") or s.get("textDisplay"),
                "like_count": int(s.get("likeCount") or 0),
                "published_at": s.get("publishedAt"),
                "updated_at": s.get("updatedAt"),
                "raw": item,  
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
        "text", "like_count", "published_at", "updated_at", "raw", "raw_json",
        "ingested_at"
    ]

    # Ensure raw_json exists
    df["raw_json"] = df["raw"].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else str(x)
    )

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
            comment_count = get_comment_count(youtube, vid)
            if comment_count == 0:
                logger.info("Skipping %s (no comments / disabled)", vid)
                continue

            logger.info("Fetching %d comments for video %s", comment_count, vid)
            rows = fetch_comments_for_video(youtube, vid, max_comments=YT_MAX_COMMENTS_PER_VIDEO)
            if not rows:
                logger.info("No comments collected for %s", vid)
                continue

            df = build_df(rows, kw)
            frames.append(df)

    if not frames:
        logger.info("No comments collected across all keywords.")
        return

    full_df = pd.concat(frames, ignore_index=True)

    # Convert timestamps 
    full_df["published_at"] = pd.to_datetime(full_df["published_at"], utc=True, errors="coerce")
    full_df["updated_at"] = pd.to_datetime(full_df["updated_at"], utc=True, errors="coerce")

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
