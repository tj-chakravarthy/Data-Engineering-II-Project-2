FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "requests>=2.31" \
    "pulsar-client>=3.5"

COPY crawler/ ./crawler/
COPY streaming/ ./streaming/

RUN mkdir -p data/cache data/output
