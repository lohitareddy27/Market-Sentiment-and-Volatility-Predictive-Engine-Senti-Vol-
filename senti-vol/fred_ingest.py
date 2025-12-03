from dotenv import load_dotenv
load_dotenv()

import json
import time
import math
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

from common import get_env, bq_table, upsert_to_bq, init_logging, now_utc


FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

SERIES = [
    ("CPIAUCSL", "Consumer Price Index"),
    ("FEDFUNDS", "Effective Federal Funds Rate"),
    ("PAYEMS",   "Total Nonfarm Payrolls"),
    ("UNRATE",   "Unemployment Rate"),
    ("DCOILWTICO", "WTI Spot Price"),
]


PAGE_SIZE = int(get_env("FRED_PAGE_SIZE", default="10"))   # items per page
MAX_PAGES = int(get_env("FRED_MAX_PAGES", default="300"))   # number of pages to ingest
MAX_ITEMS = PAGE_SIZE * MAX_PAGES                          # cap total items
SLEEP_BETWEEN_SERIES = float(get_env("FRED_SLEEP_BETWEEN_SERIES", default="0.2"))


def fetch_series(series_id: str, api_key: str, start: str = "2020-01-01",
                 max_retries: int = 3, timeout: int = 20) -> pd.DataFrame:

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        r = None
        try:
            r = requests.get(FRED_BASE, params=params, timeout=timeout)
            r.raise_for_status()
            js = r.json()

            obs = js.get("observations", []) or []
            units = js.get("units")
            rt_start = js.get("realtime_start")
            rt_end = js.get("realtime_end")

            rows = []
            for o in obs:
                raw_value = o.get("value")
                date_value = o.get("date")


                if raw_value in (None, "", ".", "NaN"):
                    continue
                if date_value in (None, "", "."):
                    continue

                try:
                    val = float(raw_value)
                except Exception:
                    continue

                rows.append({
                    "series_id": series_id,
                    "observation_date": date_value,
                    "value": val,
                    "meta": json.dumps(
                        {
                            "units": units,
                            "realtime_start": rt_start,
                            "realtime_end": rt_end,
                        },
                        ensure_ascii=False,
                    ),
                    "ingested_at": now_utc(),
                })

            df = pd.DataFrame(rows)
            if not df.empty:
             
                df["observation_date"] = pd.to_datetime(
                    df["observation_date"], errors="coerce"
                ).dt.date

            print(f"[FRED] {series_id} fetched rows={len(df)}")
            return df

        except requests.exceptions.RequestException as e:
            url = getattr(r, "url", f"{FRED_BASE}?series_id={series_id}")
            print(f"[FRED] attempt {attempt} failed for {series_id}: {e}  URL: {url}")
            if attempt == max_retries:
                print(f"[FRED] giving up on {series_id} after {max_retries} attempts")
                return pd.DataFrame()

            time.sleep(backoff)
            backoff *= 2.0

    return pd.DataFrame()


def ingest_page(df_page, page_num):
    if df_page is None or df_page.empty:
        print(f"[FRED] Page {page_num} empty; skipping ingest.")
        return 0

    macro_schema = [
        bigquery.SchemaField("series_id", "STRING"),
        bigquery.SchemaField("observation_date", "DATE"),
        bigquery.SchemaField("value", "FLOAT64"),
        bigquery.SchemaField("meta", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    target_table = bq_table("macro_indicators")
    staging_table = bq_table("macro_indicators_staging")

    try:
        upsert_to_bq(
            target_table=target_table,
            df=df_page,
            schema=macro_schema,
            key_fields=["series_id", "observation_date"],
            staging_table=staging_table,
        )
        print(f"[FRED] Ingested page {page_num}: {len(df_page)} rows.")
        return len(df_page)
    except Exception as e:
        print(f"[FRED] ERROR ingesting page {page_num}: {e}")
        return 0


def main():
    init_logging()

    api_key = get_env("FRED_API_KEY", required=True)

    frames = []
    for sid, name in SERIES:
        df = fetch_series(sid, api_key)
        if df is None or df.empty:
            print(f"[FRED] no rows for {sid}")
        else:
            frames.append(df)
        time.sleep(SLEEP_BETWEEN_SERIES)


    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        print("[FRED] No rows fetched. Exiting.")
        return

    df_all = pd.concat(frames, ignore_index=True)

    # Null values dropping
    initial_count = len(df_all)
    df_all["observation_date"] = pd.to_datetime(
        df_all["observation_date"], errors="coerce"
    ).dt.date
    df_all = df_all.dropna(subset=["series_id", "observation_date"])

    df_all["value"] = pd.to_numeric(df_all["value"], errors="coerce")
    df_all = df_all.dropna(subset=["value"])

    if "meta" in df_all.columns:
        df_all["meta"] = df_all["meta"].fillna("")

    removed = initial_count - len(df_all)
    print(f"[FRED] removed {removed} invalid/null rows before load; remaining rows={len(df_all)}")

    if df_all.empty:
        print("[FRED] No valid rows to ingest after filtering — exiting without upsert.")
        return

    # Deduplicate (series_id + observation_date) and reset index
    df_all = df_all.drop_duplicates(subset=["series_id", "observation_date"], keep="first").reset_index(drop=True)

    if len(df_all) > MAX_ITEMS:
        df_all = df_all.iloc[:MAX_ITEMS].reset_index(drop=True)
        print(f"[FRED] Capped rows to MAX_ITEMS={MAX_ITEMS}")

    total_candidates = len(df_all)
    pages = [df_all.iloc[i:i + PAGE_SIZE].copy().reset_index(drop=True)
             for i in range(0, total_candidates, PAGE_SIZE)]

    total_ingested = 0
    all_ingested_summaries = []

    for idx, df_page in enumerate(pages, start=1):
        print(f"[FRED] Processing page {idx}/{len(pages)} with {len(df_page)} rows.")
        ingested_count = ingest_page(df_page, idx)
        total_ingested += ingested_count

        if len(df_page) > 0:
            for _, row in df_page.iterrows():
                summary = {
                    "series_id": row.get("series_id"),
                    "observation_date": None if pd.isnull(row.get("observation_date")) else row.get("observation_date").isoformat(),
                    "value": float(row.get("value")) if row.get("value") is not None else None,
                    "page_number": idx
                }
                all_ingested_summaries.append(summary)

        time.sleep(0.1)

    print(f"[FRED] Finished run. Pages: {len(pages)}, Ingested rows total: {total_ingested}.")

  
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

    # Print and write aggregated JSON
    print("\nAggregated Pagination JSON")
    print(json.dumps(aggregated, indent=2, ensure_ascii=False))

    try:
        with open("fred_aggregated_pagination_summary.json", "w", encoding="utf-8") as f:
            json.dump(aggregated, f, indent=2, ensure_ascii=False)
        print("[FRED] Wrote aggregated summary to fred_aggregated_pagination_summary.json")
    except Exception as e:
        print(f"[FRED] Could not write aggregated summary to file: {e}")

if __name__ == "__main__":
    main()
