# Market Sentiment & Volatility Predictive Engine (Senti-Vol)

An automated data ingestion pipeline that collects financial news, market sentiment, social media activity, macroeconomic indicators, and futures price data to build the foundation for short-term volatility prediction for CME-traded assets such as WTI Crude Oil.

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
├── common.py                   
├── news_ingest.py              
├── yahoonews_ingest.py         
├── reddit_ingest.py           
├── youtube_ingest.py           
├── fred_ingest.py              
├── market_ingest.py            
├── Dockerfile                 
├── doc.py
├── requirements.txt            
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
gcloud config set project `project_id`
```

---

## Docker Deployment

### Create Artifact Registry

```bash
gcloud artifacts repositories create senti-repo --repository-format=docker --location=`location` --project=`project_id`
```

### Build & Push Docker Image

```bash
docker build -t us-central1-docker.pkg.dev/`project_id`/senti-repo/senti-vol:latest .
```
```bash
docker push us-central1-docker.pkg.dev/`project_id`/senti-repo/senti-vol:latest 
```

## Cloud Run Jobs Setup


```bash
gcloud run jobs create senti-vol-job --image us-central1-docker.pkg.dev/absolute-bloom-477511-k3/senti-repo/senti-vol:latest --region us-central1 --service-account absolute-bloom-477511-k3@appspot.gserviceaccount.com --memory 1Gi --task-timeout 900
```

```bash
gcloud run jobs execute senti-vol-job --region us-central1
```

## Cloud Scheduler Triggers

### Example — Run News Every Hour

```bash
gcloud scheduler jobs create http senti-vol-hourly --schedule="0 * * * *" --uri="https://`location`-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/`project_id`/jobs/senti-vol-job:run" --http-method=POST --oauth-service-account-email=`project_id`@appspot.gserviceaccount.com --location=`location` --time-zone="Asia/Kolkata"
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






