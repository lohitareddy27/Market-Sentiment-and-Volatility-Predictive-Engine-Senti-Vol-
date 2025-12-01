🧠 Market Sentiment and Volatility Predictive Engine (Senti-Vol)

An AI-powered predictive analytics platform that analyzes public financial news, social media sentiment, and macroeconomic indicators to forecast short-term market volatility for major CME Group asset classes — such as WTI Crude Oil (CL=F)

📈 Overview

CME Group’s trading volume and volatility are driven by dynamic factors — from geopolitical events and central-bank announcements to social-media sentiment and economic indicators.
Senti-Vol builds a proof-of-concept (PoC) engine that automates ingestion, warehousing, and analysis of such public signals to identify early indicators of market volatility.

This Phase-1 implementation focuses on data ingestion, storage, and orchestration using Google Cloud Platform (GCP).

🏗️ Architecture
       ┌──────────────┐
       │ Data Sources │
       └──────────────┘
              │
   ┌──────────┴────────────────────────────────────┐
   │   News APIs   Reddit   YouTube   FRED   Yahoo │
   └──────────┬────────────────────────────────────┘
              │
       ┌──────▼──────┐
       │ Cloud Run   │  ← Dockerized Python scripts
       │ (Jobs)      │
       └──────┬──────┘
              │
       ┌──────▼──────┐
       │ Cloud       │  ← Hourly/Daily triggers
       │ Scheduler   │
       └──────┬──────┘
              │
       ┌──────▼──────────────────┐
       │ Google BigQuery         │
       │ (senti_vol_stage)       │
       └─────────────────────────┘

⚙️ Tech Stack

Languages: Python 3.11+, Bash
Cloud Platform: Google Cloud Platform (BigQuery, Cloud Run, Cloud Scheduler, Secret Manager)
Libraries:

requests, pandas, feedparser, yfinance, praw, google-cloud-bigquery

dotenv, tqdm, logging, json, datetime

🚀 Features

✅ Automated hourly and daily data ingestion
✅ Multi-source integration (News, Social Media, Macroeconomics, Market Data)
✅ Containerized with Docker for reproducibility
✅ Cloud-native scheduling (Cloud Scheduler + Cloud Run)
✅ Centralized data warehouse in BigQuery
✅ Duplicate-free upserts for data integrity

📂 Repository Structure
senti-vol/
├── common.py
├── news_ingest.py
├── yahoonews_ingest.py
├── finnhub_ingest.py
├── reddit_ingest.py
├── youtube_ingest.py
├── fred_ingest.py
├── market_ingest.py
├── main.py
├── Dockerfile
├── requirements.txt
└── README.md

🧩 Phase-1: Data Ingestion & Warehousing

Market Sentiment and Volatility…

1️⃣ Data Sources
Category	API	Description
News	NewsAPI
, Yahoo Finance
, Finnhub
	Fetches market-moving news related to crude oil and energy markets
Social Media	Reddit API
, YouTube Data API
	Captures retail investor sentiment and public discussion
Macroeconomics	FRED API
	Retrieves official data — CPI, unemployment, interest rates
Market Data	Yahoo Finance
	Historical OHLCV data for WTI Crude Oil Futures (CL=F)
2️⃣ Data Pipeline Components
Script	Function
common.py	Environment handling, BigQuery client setup, upsert helpers
news_ingest.py	Fetches oil-related articles from NewsAPI
yahoonews_ingest.py	Reads Yahoo Finance RSS feed for CL=F
finnhub_ingest.py	Retrieves institutional-grade financial news
reddit_ingest.py	Extracts Reddit posts from investing/energy subreddits
youtube_ingest.py	Collects YouTube comments on financial videos
fred_ingest.py	Loads macroeconomic series (CPI, unemployment, oil price)
market_ingest.py	Downloads OHLCV data from Yahoo Finance
main.py	Provides HTTP entrypoint for ingestion trigger
🐳 Docker Deployment (Google Cloud)
Step 1 — Enable APIs
gcloud services enable artifactregistry.googleapis.com run.googleapis.com cloudbuild.googleapis.com cloudscheduler.googleapis.com secretmanager.googleapis.com

Step 2 — Create Artifact Registry
gcloud artifacts repositories create containers \
  --repository-format=docker \
  --location=us-central1 \
  --description="Senti-Vol images"

Step 3 — Build & Push Docker Image
gcloud builds submit \
  --tag us-central1-docker.pkg.dev/absolute-bloom-477511-k3/containers/senti-vol:latest

🔐 Secrets Management
"YOUR_NEWSAPI_KEY"     | Out-File -Encoding ascii -NoNewline tmp_news.txt
"YOUR_YOUTUBE_API_KEY" | Out-File -Encoding ascii -NoNewline tmp_yt.txt
"YOUR_FRED_API_KEY"    | Out-File -Encoding ascii -NoNewline tmp_fred.txt
"YOUR_FINNHUB_API_KEY" | Out-File -Encoding ascii -NoNewline tmp_finn.txt

gcloud secrets create NEWSAPI_KEY     --data-file=tmp_news.txt
gcloud secrets create YOUTUBE_API_KEY --data-file=tmp_yt.txt
gcloud secrets create FRED_API_KEY    --data-file=tmp_fred.txt
gcloud secrets create FINNHUB_API_KEY --data-file=tmp_finn.txt

Remove-Item tmp_news.txt,tmp_yt.txt,tmp_fred.txt,tmp_finn.txt

👥 Service Accounts
Create:
gcloud iam service-accounts create run-jobs-sa --display-name="Run Jobs SA"
gcloud iam service-accounts create scheduler-sa --display-name="Scheduler SA"


Grant roles:

BigQuery Data Editor

BigQuery Job User

Secret Manager Secret Accessor

☁️ Cloud Run Jobs Setup
Hourly Jobs
gcloud run jobs create senti-vol-news \
  --image us-central1-docker.pkg.dev/absolute-bloom-477511-k3/containers/senti-vol:latest \
  --region us-central1 \
  --command=bash \
  --args="-lc","python news_ingest.py" \
  --set-env-vars GCP_PROJECT_ID=absolute-bloom-477511-k3,BQ_DATASET=senti_vol_stage \
  --set-secrets NEWSAPI_KEY=NEWSAPI_KEY:latest \
  --cpu=1 --memory=1Gi


Repeat for:

senti-vol-reddit

senti-vol-youtube

senti-vol-yahoonews

senti-vol-finnhub

Daily Jobs
gcloud run jobs create senti-vol-fred \
  --image us-central1-docker.pkg.dev/absolute-bloom-477511-k3/containers/senti-vol:latest \
  --region us-central1 \
  --command=bash \
  --args="-lc","python fred_ingest.py" \
  --set-env-vars GCP_PROJECT_ID=absolute-bloom-477511-k3,BQ_DATASET=senti_vol_stage \
  --set-secrets FRED_API_KEY=FRED_API_KEY:latest \
  --cpu=1 --memory=1Gi


And similarly for senti-vol-market.

⏰ Cloud Scheduler Setup

Allow Scheduler to invoke jobs:

$sa="scheduler-sa@absolute-bloom-477511-k3.iam.gserviceaccount.com"

foreach ($job in "senti-vol-news","senti-vol-reddit","senti-vol-youtube","senti-vol-yahoonews","senti-vol-finnhub","senti-vol-fred","senti-vol-market") {
  gcloud run jobs add-iam-policy-binding $job \
    --region us-central1 \
    --member="serviceAccount:$sa" \
    --role="roles/run.invoker"
}


Create Scheduler jobs (hourly & daily):

gcloud scheduler jobs create http senti-vol-news-hourly \
  --schedule="0 * * * *" \
  --http-method=POST \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/absolute-bloom-477511-k3/jobs/senti-vol-news:run" \
  --oauth-service-account-email=$sa \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"

🧠 Data Warehouse (BigQuery)

Dataset: senti_vol_stage

Tables:

news_articles

reddit_posts

youtube_comments

macro_indicators

market_prices

Each with corresponding _staging table for deduplication and upsert logic.
