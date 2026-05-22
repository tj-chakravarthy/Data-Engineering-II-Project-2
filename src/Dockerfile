FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "matplotlib>=3.8" \
    "requests>=2.31" \
    "pulsar-client>=3.5"

COPY analytics/ ./analytics/
COPY crawler/ ./crawler/
COPY streaming/ ./streaming/

RUN mkdir -p data/cache data/output data/results data/figures
