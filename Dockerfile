FROM python:3.12-slim

WORKDIR /app

RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PORT=8000
EXPOSE 8000
# Workers configurables pour adapter la RAM du VPS.
# Défaut à 1 worker: évite les OOM observés avec 2 workers sur un conteneur limité.
# UVICORN_LIMIT_MAX_REQUESTS recycle périodiquement le process pour limiter les dérives mémoire.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000 --workers ${UVICORN_WORKERS:-1} --limit-max-requests ${UVICORN_LIMIT_MAX_REQUESTS:-1000}"]
