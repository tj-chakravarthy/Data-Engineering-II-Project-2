FROM python:3.11-slim

WORKDIR /app

# fixed by TJ: install from the project requirements so Docker does not drift
# from local/VM setup when dependencies change.
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/crawler/ ./crawler/
COPY src/streaming/ ./streaming/
# fixed by TJ: the docs tell people to run the validator before ingestion,
# but it was never copied into the image so the container could not run it.
COPY scripts/validate_crawler_output.py ./scripts/validate_crawler_output.py

RUN mkdir -p data/cache data/output
