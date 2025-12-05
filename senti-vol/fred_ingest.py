from dotenv import load_dotenv
load_dotenv()

import json
import time
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

                # Skip null/invalid data BEFORE adding to dataframe
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
                    "ingested_at": now_utc(),
                })

            df = pd.DataFrame(rows)
            if not df.empty:
                # Convert to datetime.date so it matches BigQuery DATE
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


def main():

    init_logging()
    
    api_key = get_env("FRED_API_KEY", required=True)

    frames = []
    for sid, name in SERIES:
        df = fetch_series(sid, api_key)
        frames.append(df)
        time.sleep(0.2)  


    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        print("[FRED] No rows fetched. Exiting.")
        return

    df_all = pd.concat(frames, ignore_index=True)

    
    initial_count = len(df_all)
    df_all["observation_date"] = pd.to_datetime(
        df_all["observation_date"], errors="coerce"
    ).dt.date
    df_all = df_all.dropna(subset=["series_id", "observation_date"])

    df_all["value"] = pd.to_numeric(df_all["value"], errors="coerce")
    df_all = df_all.dropna(subset=["value"])

    removed = initial_count - len(df_all)
    print(f"[FRED] removed {removed} invalid/null rows before load; remaining rows={len(df_all)}")

    if df_all.empty:
        print("[FRED] No valid rows to ingest after filtering — exiting without upsert.")
        return


    target_table = bq_table("macro_indicators")
    staging_table = bq_table("macro_indicators_staging")

    # Upsert on series_id, observation_date
    upsert_to_bq(
        target_table=target_table,
        df=df_all,
        schema=None,
        key_fields=["series_id", "observation_date"],
        staging_table=staging_table,
    )

    print(f"Loaded {len(df_all)} macro rows (staged + MERGE).")


if __name__ == "__main__":
    main()
