from dotenv import load_dotenv
load_dotenv()

import json
import math
import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
import re
from google.cloud import bigquery

from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc


NEWS_API = "https://newsapi.org/v2/everything"

PAGE_SIZE = 10


MAX_PAGES = 10

MAX_RETRIES = 3
SLEEP_BETWEEN_PAGES = 0.5  
SLEEP_ON_RETRY = 1.5       

CORE_RE = re.compile(
    r"\b(wti|brent|crude oil|crude|petroleum|opec\+?|eia|nyme?x|ice(?: brent)?|barrel?s?|refiner(?:y|ies)|upstream|midstream|downstream)\b",
    re.I,
)
CTX_RE = re.compile(
    r"\b(price|prices|futures|spot|curve|spread|hedg(?:e|ing)|inventory|stocks?|output|production|exports?|imports?|demand|supply|rig count|shutdown|outage|sanctions?|disruption|capacity|maintenance|pipeline|refinery)\b",
    re.I,
)

PROXIMITY_CHARS = 40

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


# Quering to API with multiple core terms to stay within URL length limits
CORE_TERMS = [
    '"wti"', '"brent"', '"crude oil"', "petroleum", '"opec"', '"opec+"', "nymex", '"ice brent"', "eia"
]
CTX_TERMS = [
    "price", "prices", "futures", "spot", "inventory", "production", "demand", "supply",
    '"rig count"', "sanctions", "outage", "pipeline", "refinery"
]

def build_query(core_terms, ctx_terms):
    core = "(" + " OR ".join(core_terms) + ")"
    ctx = "(" + " OR ".join(ctx_terms) + ")"
    return f"{core} AND {ctx}"

def build_queries_with_limit(core_terms, ctx_terms, max_len=480):
    queries = []
    bucket = []
    for term in core_terms:
        test_core = bucket + [term]
        q = build_query(test_core, ctx_terms)
        if len(q) <= max_len:
            bucket.append(term)
        else:
            if bucket:
                queries.append(build_query(bucket, ctx_terms))
            bucket = [term]
    if bucket:
        queries.append(build_query(bucket, ctx_terms))
    return queries


DOMAINS = None

def retry_page_request(func, max_retries=MAX_RETRIES, backoff=SLEEP_ON_RETRY):
    
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except requests.RequestException as e:
            print(f"[NewsAPI] attempt {attempt} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(backoff * attempt)


def ingest_page(df_page):
    if df_page is None or df_page.empty:
        print("[NewsAPI] ingest_page called with empty df — skipping.")
        return 0

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

    try:
        upsert_to_bq(
            target_table,
            df_page,
            schema=news_schema,
            key_fields=["article_id"],
            staging_table=staging_table
        )
        print(f"[NewsAPI] Ingested {len(df_page)} rows from this page.")
        return len(df_page)
    except Exception as e:
        print(f"[NewsAPI] ERROR ingesting page: {e}")
        return 0

def fetch_news_newsapi():
    api_key = get_env("NEWSAPI_KEY", required=True)
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=7)
    queries = build_queries_with_limit(CORE_TERMS, CTX_TERMS, max_len=480)

    total_ingested = 0
    total_seen = 0

    
    all_ingested_summaries = []

    for q in queries:
        print(f"\n[NewsAPI] Fetching for query (preview): {q[:120]}...")

  
        params = {
            "q": q,
            "language": "en",
            "from": from_dt.isoformat(),
            "to": to_dt.isoformat(),
            "sortBy": "relevancy",
            "searchIn": "title,description",
            "pageSize": PAGE_SIZE,
            "page": 1,
            "apiKey": api_key,
        }
        if DOMAINS:
            params["domains"] = DOMAINS

        def do_call_first():
            r = requests.get(NEWS_API, params=params, timeout=45)
            r.raise_for_status()
            return r

        try:
            r = retry_page_request(do_call_first, max_retries=MAX_RETRIES)
        except Exception as e:
            print(f"[NewsAPI] Page 1 failed permanently for q='{q[:40]}': {e}")
            continue

        js = r.json()
        total_results = js.get("totalResults", 0) or 0
        api_pages = math.ceil(total_results / PAGE_SIZE) if PAGE_SIZE > 0 else 0
        pages_to_fetch = min(MAX_PAGES, api_pages) if api_pages > 0 else MAX_PAGES

     
        arts = js.get("articles", []) or []
        print(f"[NewsAPI] q_len={len(q)} page=1 got={len(arts)} articles (totalResults={total_results})")
        print("Effective URL:", r.url)

        def process_page_articles(page_num, arts_list):
            nonlocal total_ingested, total_seen, all_ingested_summaries
            if not arts_list:
                print(f"[NewsAPI] Page {page_num} returned no articles.")
                return

            total_seen += len(arts_list)
            page_rows = []
            for a in arts_list:
                title = (a.get("title") or "").strip()
                desc = (a.get("description") or "").strip()

                
                if not (is_relevant_text(title) or is_relevant_text(f"{title} {desc}".strip())):
                    continue

                published_at = a.get("publishedAt") or ""
                url = a.get("url") or None
                article_id = stable_id(url) if url else stable_id(title + "|" + (published_at or ""))

                page_rows.append({
                    "source": "newsapi",
                    "article_id": article_id,
                    "published_at": published_at,
                    "title": title,
                    "description": desc,
                    "url": url,
                    "author": a.get("author"),
                    "ingested_at": now_utc(),
                })

            df_page = pd.DataFrame(page_rows)
            if not df_page.empty:
                df_page["published_at"] = pd.to_datetime(df_page["published_at"], utc=True, errors="coerce")
                df_page = df_page.drop(columns=["tickers"], errors="ignore")
                df_page = df_page.dropna(subset=["article_id"])
                df_page = df_page.drop_duplicates(subset=["article_id"], keep="first")

              
                ingested_count = ingest_page(df_page)
                total_ingested += ingested_count

           
                for idx, row in df_page.iterrows():
                    summary = {
                        "article_id": row.get("article_id"),
                        "title": row.get("title"),
                        "published_at": None if pd.isnull(row.get("published_at")) else row.get("published_at").isoformat(),
                        "url": row.get("url"),
                        "source": row.get("source"),
                        "page_number": page_num
                    }
                    all_ingested_summaries.append(summary)
            else:
                print(f"[NewsAPI] Page {page_num}: no relevant rows after filtering - skipping ingestion for this page.")

      
        process_page_articles(1, arts)

        if pages_to_fetch >= 2:
            for page in range(2, pages_to_fetch + 1):
                params["page"] = page

                def do_call():
                    r2 = requests.get(NEWS_API, params=params, timeout=45)
                    r2.raise_for_status()
                    return r2

                try:
                    r2 = retry_page_request(do_call, max_retries=MAX_RETRIES)
                except Exception as e:
                    print(f"[NewsAPI] Page {page} failed permanently for q='{q[:40]}': {e}")
                    continue

                js2 = r2.json()
                arts2 = js2.get("articles", []) or []
                print(f"[NewsAPI] q_len={len(q)} page={page} got={len(arts2)} articles")
                print("Effective URL:", r2.url)

                process_page_articles(page, arts2)

                time.sleep(SLEEP_BETWEEN_PAGES)

        
    print(f"[NewsAPI] Finished fetch. Seen articles: {total_seen}, Ingested rows: {total_ingested}.")

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
        with open("newsapi_aggregated_pagination_summary.json", "w", encoding="utf-8") as f:
            json.dump(aggregated, f, indent=2, ensure_ascii=False)
        print("[NewsAPI] Wrote aggregated summary to newsapi_aggregated_pagination_summary.json")
    except Exception as e:
        print(f"[NewsAPI] Could not write aggregated summary to file: {e}")

    return aggregated


def main():
    init_logging()
    fetch_news_newsapi()


if __name__ == "__main__":
    main()
