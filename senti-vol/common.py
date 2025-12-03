import os
import json
import hashlib
import logging
import numpy as np
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()


# Env
def get_env(key: str, required: bool = False, default=None):
    val = os.getenv(key, default)
    if required and (val is None or val == ""):
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return val


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )


# Timestamp
def now_utc():
    return datetime.now(timezone.utc)


def stable_id(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:32]


# BigQuery helpers
def bq_client() -> bigquery.Client:
    project = get_env("GCP_PROJECT_ID", required=True)
    return bigquery.Client(project=project)


def bq_table(table_name: str) -> str:
    project = get_env("GCP_PROJECT_ID", required=True)
    dataset = get_env("BQ_DATASET", required=True)
    return f"{project}.{dataset}.{table_name}"


# Null Removal
def _clean_df_drop_nulls(df: pd.DataFrame, required_non_null: list[str] | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    obj_cols = [c for c in df.columns if df[c].dtype == object]
    for c in obj_cols:
        df[c] = df[c].where(~df[c].astype(str).str.strip().eq(""), None)

    df = df.dropna(how="all")

    if required_non_null:
        df = df.dropna(subset=required_non_null, how="any")

    return df


def upsert_to_bq(target_table: str, df: pd.DataFrame, schema=None, key_fields=None, staging_table: str = None, location: str = "US"):
    if not key_fields:
        raise ValueError("key_fields is required for upsert_to_bq")

    client = bq_client()

    if df is None or df.empty:
        print(f"[upsert_to_bq] Empty dataframe, skipping upsert → {target_table}")
        return

    if "ingested_at" not in df.columns:
        df["ingested_at"] = pd.to_datetime(now_utc())

    if "tickers" in df.columns:
        def norm_tickers(x):
            if isinstance(x, list):
                return [str(v) for v in x]
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return []
            return [str(x)]
        df["tickers"] = df["tickers"].apply(norm_tickers)



    ts_cols = [c for c in df.columns if isinstance(c, str) and c.endswith("_at") and c != "ingested_at"]
    for c in ts_cols:
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    df = _clean_df_drop_nulls(df, required_non_null=key_fields)
    if df is None or df.empty:
        print(f"[upsert_to_bq] All rows dropped after null-cleaning (null keys/empty rows) → {target_table}")
        return

    staging = staging_table if staging_table else (target_table + "_staging")
    print(f"[upsert_to_bq] Using staging table: {staging}")

    # Fetch target schema
    try:
        target_tbl = client.get_table(target_table)
    except Exception as e:
        raise RuntimeError(f"[upsert_to_bq] Unable to fetch target table schema for {target_table}: {e}")

    target_schema = target_tbl.schema  

    # Coerce DataFrame columns to match target types where possible
    for fld in target_schema:
        name = fld.name
        ftype = fld.field_type.upper()
        mode = fld.mode.upper() if fld.mode else "NULLABLE"
        if name not in df.columns:
            continue

        # Repeated fields
        if mode == "REPEATED":
            def ensure_list(x):
                if isinstance(x, list):
                    return [str(v) for v in x]
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return []
                return [str(x)]
            df[name] = df[name].apply(ensure_list)
            continue

        # INT64 
        if ftype == "INT64":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
            continue

        # FLOAT64 / NUMERIC-like
        if ftype in ("FLOAT64", "NUMERIC", "BIGNUMERIC", "DOUBLE"):
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("float64")
            continue

        # TIMESTAMP / DATETIME 
        if ftype in ("TIMESTAMP", "DATETIME"):
            df[name] = pd.to_datetime(df[name], utc=True, errors="coerce")
            continue

        # DATE 
        if ftype == "DATE":
            def to_date(v):
                if pd.isna(v):
                    return None
                try:
                    return pd.to_datetime(v).date()
                except Exception:
                    return None
            df[name] = df[name].apply(to_date)
            continue

        # JSON/STRING ensure string (serialize dict/list)
        if ftype in ("STRING", "JSON"):
            def ensure_string(x):
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return None
                if isinstance(x, str):
                    return x
                if isinstance(x, (dict, list)):
                    return json.dumps(x, ensure_ascii=False)
                if isinstance(x, (bytes, bytearray)):
                    try:
                        return x.decode("utf-8")
                    except Exception:
                        return str(x)
                return str(x)
            df[name] = df[name].apply(ensure_string)
            continue

    #Truncate
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job_config.schema = schema if schema is not None else target_schema

    print(f"[upsert_to_bq] Loading {len(df)} rows → STAGING {staging}")
    load_job = client.load_table_from_dataframe(df, staging, job_config=job_config, location=location)
    load_job.result()
    print(f"[upsert_to_bq] Staging load complete: job_id={load_job.job_id}")

    # Build usable columns list and validate keys
    target_cols = [f.name for f in target_schema]
    usable_cols = [c for c in df.columns if c in target_cols]
    if not usable_cols:
        raise RuntimeError("[upsert_to_bq] No DataFrame columns match target table schema; aborting MERGE.")

    missing_keys = [k for k in key_fields if k not in target_cols]
    if missing_keys:
        raise ValueError(f"[upsert_to_bq] key_fields not in target table schema: {missing_keys}")

    rn_partition = ", ".join([f"`{k}`" for k in key_fields])
    if "ingested_at" in usable_cols:
        rn_order = "SAFE_CAST(ingested_at AS TIMESTAMP) DESC"
    else:
        rn_order = rn_partition

    col_list = ", ".join([f"`{c}`" for c in usable_cols])
    insert_cols = col_list
    insert_vals = ", ".join([f"S.`{c}`" for c in usable_cols])
    non_key_cols = [c for c in usable_cols if c not in key_fields]
    update_assignments = ",\n      ".join([f"T.`{c}` = S.`{c}`" for c in non_key_cols])
    on_clause = " AND ".join([f"T.`{k}` = S.`{k}`" for k in key_fields])

    staging_select = f"""
        SELECT
          {col_list},
          ROW_NUMBER() OVER (
            PARTITION BY {rn_partition}
            ORDER BY {rn_order}
          ) AS rn
        FROM `{staging}`
    """

    if non_key_cols:
        merge_sql = f"""
        MERGE `{target_table}` T
        USING (
          SELECT * FROM ({staging_select}) WHERE rn = 1
        ) S
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET
          {update_assignments}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals});
        """
    else:
        merge_sql = f"""
        MERGE `{target_table}` T
        USING (
          SELECT * FROM ({staging_select}) WHERE rn = 1
        ) S
        ON {on_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals});
        """

    print("[upsert_to_bq] Running MERGE...")
    merge_job = client.query(merge_sql, location=location)
    try:
        merge_job.result()
    except Exception as e:
        print(f"[upsert_to_bq] MERGE failed for target {target_table}: {e}\nSQL:\n{merge_sql}")
        raise
    print(f"[upsert_to_bq] MERGE complete: job_id={merge_job.job_id}")
