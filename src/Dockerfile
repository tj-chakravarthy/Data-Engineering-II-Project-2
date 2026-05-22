FROM python:3.11-slim

WORKDIR /app

# fixed by TJ: requirements.txt was removed upstream, so keep the container
# self-contained with the two runtime deps the crawler actually imports.
RUN pip install --no-cache-dir \
    "requests>=2.31" \
    "pulsar-client>=3.5"

COPY src/crawler/ ./crawler/
COPY src/streaming/ ./streaming/
# fixed by TJ: the docs tell people to run the validator before ingestion,
# but it was never copied into the image so the container could not run it.
COPY scripts/validate_crawler_output.py ./scripts/validate_crawler_output.py

RUN mkdir -p data/cache data/output
