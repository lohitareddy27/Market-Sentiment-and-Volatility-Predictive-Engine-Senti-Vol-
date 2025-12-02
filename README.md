# Market Sentiment & Volatility Predictive Engine (Senti-Vol)

An AI-powered data ingestion and preprocessing engine that collects financial news, market sentiment, social media activity, macroeconomic indicators, and futures price data to build the foundation for **short-term volatility prediction** for CME-traded assets such as WTI Crude Oil.

---

## Tech Stack

### Languages

* Python 3.11+


### Cloud Services

* Google Cloud Run
* Google Cloud Scheduler
* BigQuery
* Secret Manager
* Artifact Registry


---

## Repository Structure

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

## Setup Instructions

### Clone the repository

```bash
git clone https://github.com/YourUserName/senti-vol.git
cd senti-vol
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Authenticate with Google Cloud

```bash
gcloud auth login
gcloud config set project absolute-bloom-477511-k3
```

---

## Docker Deployment

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

## Cloud Run Jobs Setup

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

## Cloud Scheduler Triggers

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

## BigQuery Outputs

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


## Results Summary 

* Fully automated data ingestion
* Hourly & daily scheduled jobs
* Dockerized & cloud-native
* BigQuery as unified data warehouse






