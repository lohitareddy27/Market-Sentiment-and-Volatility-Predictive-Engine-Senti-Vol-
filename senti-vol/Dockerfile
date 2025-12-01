 FROM python:3.11-slim

# Dependencies for python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libssl-dev \
    libbz2-dev \
    liblzma-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app


COPY requirements.txt .


RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Default envs 
ENV PYTHONUNBUFFERED=1


CMD ["bash", "-c", "\
    echo \"[ENTRYPOINT] Mode=batch — running all ingestion scripts...\"; \
    python news_ingest.py || exit 1; \
    python reddit_ingest.py || exit 1; \
    python fred_ingest.py || exit 1; \
    python market_ingest.py || exit 1; \
    python youtube_ingest.py || exit 1; \
    python yahoonews_ingest.py || exit 1; \
    python finnhub_ingest.py || exit 1; \
    echo \"[ENTRYPOINT] All ingestions completed.\"; \
"]
