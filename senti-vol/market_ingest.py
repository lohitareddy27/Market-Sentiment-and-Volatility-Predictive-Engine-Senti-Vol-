from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from common import get_env, bq_table, upsert_to_bq, init_logging, now_utc

def main():
    init_logging()

    ticker = get_env("ASSET_TICKER", default="CL=F")
    print(f"[INFO] Fetching Yahoo Finance history for {ticker}...")

    yf_t = yf.Ticker(ticker)
    df = yf_t.history(
        period="60d",
        interval="1d",
        auto_adjust=False,
        actions=False,
        prepost=False
    )

    if df is None or df.empty:
        print("[WARN] No market data fetched (history() empty).")
        return

    df = df.reset_index()

    rename_map = {
        "Date": "ts",
        "Datetime": "ts",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    out = pd.DataFrame(index=range(len(df)))
    out["ticker"] = ticker
    out["ts"] = pd.to_datetime(df.get("ts"), utc=True, errors="coerce")
    for c_src, c_dst in [("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"), ("volume", "volume")]:
        s = df.get(c_src)
        out[c_dst] = pd.to_numeric(s, errors="coerce") if s is not None else pd.Series([None]*len(df), dtype="float64")
    out["meta"] = '{"source":"YahooFinance"}'
    out["ingested_at"] = now_utc()

    mask_all_nan = out[["open", "high", "low", "close", "volume"]].isna().all(axis=1)
    if mask_all_nan.any():
        out = out[~mask_all_nan].reset_index(drop=True)

    if out.empty:
        print("[WARN] After cleaning, no valid OHLCV rows to load.")
        return

    market_schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("volume", "FLOAT64"),
        bigquery.SchemaField("meta", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    target_table = bq_table("market_prices")
    staging_table = bq_table("market_prices_staging")

    upsert_to_bq(target_table, out, schema=market_schema, key_fields=["ticker", "ts"], staging_table=staging_table)
    print(f"Loaded {len(out)} rows into market_prices (staged + MERGE).")

if __name__ == "__main__":
    main()

