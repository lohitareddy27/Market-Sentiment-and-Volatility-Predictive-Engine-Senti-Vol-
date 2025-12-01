from dotenv import load_dotenv
load_dotenv()

import json, re, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from common import get_env, bq_table, upsert_to_bq, init_logging, stable_id, now_utc

FINNHUB_NEWS = "https://finnhub.io/api/v1/news"
OIL_WORDS = ["oil","crude","crude oil","wti","brent","petroleum","opec",r"opec\+","barrel","futures","refinery","eia","nymex","cme"]
KEYWORD_RE = re.compile(r"\b(" + "|".join(OIL_WORDS) + r")\b", re.I)

def is_rel(s:str): return bool(KEYWORD_RE.search(s or ""))

def main():
    init_logging()
    token = get_env("FINNHUB_API_KEY", required=True)
    days_back = int(get_env("FINNHUB_DAYS_BACK", default="7"))
    to_d = datetime.now(timezone.utc).date()
    from_d = (datetime.now(timezone.utc)-timedelta(days=days_back)).date()

    r = requests.get(FINNHUB_NEWS, params={
        "category":"general", "from":str(from_d), "to":str(to_d), "token":token
    }, timeout=45)
    r.raise_for_status()
    articles = r.json() or []

    rows=[]
    for a in articles:
        title=a.get("headline") or ""
        summary=a.get("summary") or ""
        if not is_rel(f"{title} {summary}"): continue
        ts=a.get("datetime")
        published_at=pd.to_datetime(ts, unit="s", utc=True, errors="coerce") if ts else None
        url=a.get("url")
        aid=stable_id(url) if url else stable_id(title+"|"+str(published_at))
        rows.append({
            "source":"finnhub",
            "article_id":aid,
            "published_at":published_at,
            "title":title,
            "description":summary,
            "url":url,
            "author":a.get("source"),
            "raw":json.dumps(a, ensure_ascii=False),
            "ingested_at":now_utc(),
        })

    df=pd.DataFrame(rows)
    if df.empty: 
        print("[FINNHUB] no rows"); return
    df["published_at"]=pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    df=df.dropna(subset=["article_id"])

    schema=[
        bigquery.SchemaField("source","STRING"),
        bigquery.SchemaField("article_id","STRING"),
        bigquery.SchemaField("published_at","TIMESTAMP"),
        bigquery.SchemaField("title","STRING"),
        bigquery.SchemaField("description","STRING"),
        bigquery.SchemaField("url","STRING"),
        bigquery.SchemaField("author","STRING"),
        bigquery.SchemaField("raw","STRING"),
        bigquery.SchemaField("ingested_at","TIMESTAMP"),
    ]
    upsert_to_bq(bq_table("news_articles"), df, schema=schema,
                 key_fields=["article_id"], staging_table=bq_table("news_articles_staging"))
    print(f"[FINNHUB] upserted {len(df)}")

if __name__=="__main__":
    main()
