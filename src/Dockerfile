FROM python:3.11-slim

WORKDIR /app

# fixed by TJ: install from the project requirements so Docker does not drift
# from local/VM setup when dependencies change.
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/crawler/ ./crawler/
COPY src/streaming/ ./streaming/

RUN mkdir -p data/cache data/output
