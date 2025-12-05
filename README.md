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

## Set Up Cloud Run Jobs and Cloud Scheduler Trigger


## BigQuery Outputs

Tables stored under:

```
absolute-bloom-477511-k3.senti_vol_stage
```

| Table Name         | Description                       |
| ------------------ | --------------------------------- |
| `news_articles`    | NewsAPI, Yahoo                    |
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






