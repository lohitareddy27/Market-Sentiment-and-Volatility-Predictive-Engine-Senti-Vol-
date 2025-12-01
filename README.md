# 🧠 Market Sentiment & Volatility Predictive Engine (Senti-Vol)

An AI-powered data ingestion and preprocessing engine that collects financial news, market sentiment, social media activity, macroeconomic indicators, and futures price data to build the foundation for **short-term volatility prediction** for CME-traded assets such as WTI Crude Oil.

---

## 🚀 Features

* **Automated Data Ingestion:** Pulls hourly & daily data from NewsAPI, Yahoo Finance, Reddit, YouTube, Finnhub, and FRED.
* **Cloud-Native Architecture:** Works entirely on Google Cloud using Cloud Run Jobs + Cloud Scheduler.
* **Fully Containerized:** Python-based ingestion pipeline packaged as a portable Docker image.
* **Secure Credential Handling:** API keys stored in Google Secret Manager.
* **Scalable BigQuery Warehouse:** Clean dataset tables stored in `senti_vol_stage`.
* **Modular Design:** Each ingestion source is a separate Python script for easy extension.
* **ML-Ready Output:** Clean structured data for future sentiment modelling and volatility forecasting.

---

## 🛠 Tech Stack

### Languages

* Python 3.11+


### Cloud Services

* Google Cloud Run
* Google Cloud Scheduler
* BigQuery
* Secret Manager
* Artifact Registry


---

## 📂 Repository Structure

```
senti-vol/
├── common.py                   # Shared helpers (BQ loading, env handling)
├── news_ingest.py              # NewsAPI ingestion
├── yahoonews_ingest.py         # Yahoo Finance RSS ingestion
├── finnhub_ingest.py           # Finnhub institutional news
├── reddit_ingest.py            # Reddit sentiment ingestion
├── youtube_ingest.py           # YouTube comments & metadata
├── fred_ingest.py              # Macroeconomic indicators (CPI, jobs, interest rates)
├── market_ingest.py            # OHLCV price ingestion via Yahoo Finance
├── Dockerfile                  # Container definition
├── requirements.txt            # Python dependencies
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/YourUserName/senti-vol.git
cd senti-vol
```

### 2️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 3️⃣ Authenticate with Google Cloud

```bash
gcloud auth login
gcloud config set project absolute-bloom-477511-k3
```

---

## 🐳 Docker Deployment

### Enable Required APIs

```bash
gcloud services enable artifactregistry.googleapis.com run.googleapis.com cloudbuild.googleapis.com cloudscheduler.googleapis.com secretmanager.googleapis.com
```

### Create Artifact Registry

```bash
gcloud artifacts repositories create containers \
  --repository-format=docker \
  --location=us-central1 \
  --description="Senti-Vol images"
```

### Build & Push Docker Image

```bash
gcloud builds submit \
  --tag us-central1-docker.pkg.dev/absolute-bloom-477511-k3/containers/senti-vol:latest
```

---

## 🔐 Secrets Configuration

### Store API Keys in Secret Manager

```bash
"YOUR_NEWSAPI_KEY"     | Out-File -Encoding ascii -NoNewline tmp_news.txt
"YOUR_YOUTUBE_API_KEY" | Out-File -Encoding ascii -NoNewline tmp_yt.txt
"YOUR_FRED_API_KEY"    | Out-File -Encoding ascii -NoNewline tmp_fred.txt
"YOUR_FINNHUB_API_KEY" | Out-File -Encoding ascii -NoNewline tmp_finn.txt
```

Upload them:

```bash
gcloud secrets create NEWSAPI_KEY     --data-file=tmp_news.txt
gcloud secrets create YOUTUBE_API_KEY --data-file=tmp_yt.txt
gcloud secrets create FRED_API_KEY    --data-file=tmp_fred.txt
gcloud secrets create FINNHUB_API_KEY --data-file=tmp_finn.txt
```

Cleanup:

```bash
Remove-Item tmp_news.txt,tmp_yt.txt,tmp_fred.txt,tmp_finn.txt
```

---

## 🧩 Cloud Run Jobs Setup

### Example — NewsAPI Job (Hourly)

```bash
gcloud run jobs create senti-vol-news \
  --image us-central1-docker.pkg.dev/absolute-bloom-477511-k3/containers/senti-vol:latest \
  --region us-central1 \
  --command=bash \
  --args="-lc","python news_ingest.py" \
  --set-env-vars GCP_PROJECT_ID=absolute-bloom-477511-k3,BQ_DATASET=senti_vol_stage \
  --set-secrets NEWSAPI_KEY=NEWSAPI_KEY:latest \
  --cpu=1 --memory=1Gi
```

### Additional Jobs

* senti-vol-reddit
* senti-vol-youtube
* senti-vol-yahoonews
* senti-vol-finnhub
* senti-vol-fred (daily)
* senti-vol-market (daily)

---

## ⏰ Cloud Scheduler Triggers

### Example — Run News Every Hour

```bash
gcloud scheduler jobs create http senti-vol-news-hourly \
  --schedule="0 * * * *" \
  --http-method=POST \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/absolute-bloom-477511-k3/jobs/senti-vol-news:run" \
  --oauth-service-account-email=scheduler-sa@absolute-bloom-477511-k3.iam.gserviceaccount.com \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
```

---

## 📊 BigQuery Outputs

Tables stored under:

```
absolute-bloom-477511-k3.senti_vol_stage
```

| Table Name         | Description                       |
| ------------------ | --------------------------------- |
| `news_articles`    | NewsAPI, Yahoo, Finnhub articles  |
| `reddit_posts`     | Reddit sentiment & discussions    |
| `youtube_comments` | Comments from financial content   |
| `macro_indicators` | CPI, unemployment, interest rates |
| `market_prices`    | OHLCV futures price data          |

---

## 💡 Core Concepts

* Sentiment extraction from public sources
* Macro indicators influencing volatility
* Automated serverless cloud ingestion
* ML-ready structured dataset

---

## 🏁 Results Summary (Phase-1)

* Fully automated data ingestion
* Hourly & daily scheduled jobs
* Dockerized & cloud-native
* Secure key management
* BigQuery as unified data warehouse

---

## 👨‍💻 Author

**Mary Lohita Swarup Reddy Gade**




