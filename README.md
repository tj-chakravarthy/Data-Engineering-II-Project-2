# Data Engineering II Project 2

GitHub analytic system using a streaming framework, developed for Uppsala University 1TD076 Data Engineering II, VT 2026, Group 16.

The system will collect GitHub repository metadata from the GitHub REST API, process it through a layered streaming architecture, and answer the four required project questions:

1. Top 10 programming languages by number of projects.
2. Top 10 most frequently updated GitHub projects.
3. Top 10 programming languages with the most projects using unit tests.
4. Top 10 programming languages with the most projects using both unit tests and CI/DevOps.

The original assignment PDF is in `Project 2 docs/DE-II_project_2-1.pdf`.

## Setup

Target environment: Linux (course VMs and team workstations).

### 1. Clone

```bash
git clone https://github.com/tj-chakravarthy/Data-Engineering-II-Project-2.git
cd Data-Engineering-II-Project-2
```

### 2. Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Credentials

```bash
cp .env.example .env
```

Fill in `GITHUB_TOKEN` in `.env`.

For team crawls, add all available GitHub tokens to `.env` as `GITHUB_TOKEN`,
`GITHUB_TOKEN_2`, `GITHUB_TOKEN_3`, etc. The crawler automatically loads every
environment variable whose name starts with `GITHUB_TOKEN`, rotates to the next
token when one is rate-limited, and waits only when the full token pool is
exhausted.

## Running the Crawler

The crawler collects repository metadata, handles pagination/rate limits, writes disk-backed cache files, and removes duplicates before downstream streaming/application logic receives the data.

The system has two entry points sharing the same crawler core:

- **`streaming.pulsar_producer`** (primary): streams each crawled record live to a Pulsar topic. This is what the graded streaming pipeline uses end-to-end.
- **`crawler.cli`** (offline): runs the same crawl but writes records only to a local NDJSON file. Useful for offline analysis, the validator script, and when no broker is available.

### Live-publish to Pulsar (primary path)

```bash
export PYTHONPATH=src
python3 -m streaming.pulsar_producer \
  --broker pulsar://localhost:6650 \
  --topic repos.raw \
  --days 365 \
  --date-field created-or-pushed \
  --cache-dir data/cache \
  --output data/output/repos.ndjson \
  --checkpoint-path data/output/repos.published.json
```

- `--output` (optional) also mirrors each published record to a local NDJSON file, so the validator can pre-check the streaming path and offline analysis stays possible.
- `--checkpoint-path` (optional) records every successfully published `repo_id` in a JSON file. On restart, `repo_id`s already in that file are skipped, giving idempotent recovery without consumer-side dedup state.
- `--max-retries N` (default 3) bounds per-record retry on transient publisher failures.

### Offline NDJSON crawl

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 1 --limit 25 --cache-dir data/cache --output data/output/repos.ndjson
```

```bash
python3 -m crawler.cli --days 365 --date-field created-or-pushed --cache-dir data/cache --output data/output/repos.ndjson
```

Useful options:

- `--date-field created|pushed|created-or-pushed`: choose the GitHub date qualifier. `created-or-pushed` runs both date-sliced searches and globally deduplicates the output. The PDF's "updated" criterion is implemented as `pushed:` because GitHub's search API does not expose an `updated:` qualifier.
- `--query "stars:>=10 archived:false"`: add extra GitHub search qualifiers.
- `--refresh-cache`: refetch slices even if cache files exist.
- `--no-cache`: stream without writing cache files.
- `--limit-per-day N`: cap each date slice for testing. Limited slices are not written to cache because they are incomplete.
- `--limit N`: cap total emitted records and API fetches for testing. Limited slices are not written to cache because they are incomplete.
- `--memory-log-every N`: log Python memory samples every N emitted records.
- `--max-memory-mb N`: stop the crawl if tracked Python memory exceeds N MB.

Crawler output:

- Cache files: `data/cache/repos_<date-field>_<YYYY-MM-DD>*.ndjson`
- Output file: `data/output/repos.ndjson`
- Records are deduplicated by stable GitHub repository ID before output.
- CLI logs include `search_splits`, `search_cap_warnings`, and `incomplete_search_warnings`. Warning counters must be zero before treating a crawl as complete for report-grade results.
- The output file is overwritten on each run by design. Use a different `--output` path for experiments or comparison runs. Cache files remain separate and are reused unless `--refresh-cache` is passed.
- The Pulsar producer streams records as the crawler yields them. Complete API slices are written to a temporary cache file while records are published, then promoted to the final cache file only after the slice finishes successfully.
- Live delivery is at-least-once across reruns. If publishing fails partway through a slice, already acknowledged messages remain in Pulsar and the incomplete temporary cache file is removed; downstream consumers should continue to use `repo_id` as the primary key.

Validate a crawler handoff file before streaming ingestion:

```bash
python3 scripts/validate_crawler_output.py data/output/repos.ndjson
```

Crawler handoff status:

- Implemented: date-sliced GitHub search, adaptive UTC time-range splitting for full uncapped runs, pagination, token-pool rate-limit handling, cache reuse, deduplication, memory sampling/limit checks, normalized NDJSON output, handoff validation, and unit tests.
- Handoff contract: live producers publish raw crawler JSON unchanged to the `repos.raw` metadata topic and use `repo_id` as the message key.
- Completeness guardrail: full uncapped runs split any slice that GitHub reports above 1000 results. If a slice still cannot be split enough, the crawler logs and counts a search-cap warning. Narrow the query before using that data as final.
- Local smoke artifacts currently exist only under ignored `data/` paths and are not part of the committed submission.

Crawler handoff details are documented in `docs/crawler_handoff.md`.

## Infrastructure Status

Initial UPPMAX/OpenStack infrastructure scripts are available in
`scripts/infrastructure/`.

Implemented:

- Provision one master VM named `group16-master`.
- Provision four worker VMs named `group16-worker-1` through `group16-worker-4`.
- Use the course `ssc.medium` flavor and Ubuntu 22.04 image.
- Install base build tools and Docker on the master and workers through cloud-init.
- Generate a cluster SSH key on the master and inject its public key into workers.
- Configure the master so workers can be reached as `w1`, `w2`, etc.
- Keep local OpenStack credentials out of git via `UPPMAX-openrc.sh.ignore`.

Run from the infrastructure directory:

```bash
cd scripts/infrastructure
cp UPPMAX-openrc.sh.template UPPMAX-openrc.sh.ignore
# Fill in OpenStack values in UPPMAX-openrc.sh.ignore.
./run.sh <PUBLIC_KEY_NAME> <PRIVATE_KEY_PATH>
```

Current infrastructure QUESTIONS:

- Worker count is currently hard-coded to four.
- Floating IP assignment is manual; the script asks for the assigned master IP.
- The scripts provision VMs and Docker, but do not yet clone this repository on the VMs.
- The scripts do not yet configure `.env`, GitHub tokens, Pulsar, or project services.
- Docker Compose/service definitions for the full system are still missing.
- A clean-VM reproduction guide still needs to be written once the streaming stack is connected.

## Streaming and Application Logic TODO

Required by project PDF:

- TODO: Choose and configure the streaming framework, expected to be Apache Pulsar unless the team decides otherwise.
- TODO: Define producer behavior for crawler output.
- TODO: Define layered topics for raw metadata, commit enrichment, unit-test evidence, CI/DevOps evidence, and final aggregates.
- TODO: Implement consumers/enrichers for Q2-Q4 metadata.
- TODO: Implement aggregators for Q1-Q4.
- TODO: Keep top-N configurable so top 10 can become top 20 without source-code changes.
- TODO: Write reproducible result files for graph generation.

Expected topic layout:

```text
repos.raw
repos.with_commits
repos.with_tests
repos.with_ci
repos.aggregates
```

Current live producer command shape:

```bash
docker compose up -d
python3 -m streaming.pulsar_producer --broker pulsar://localhost:6650 --topic repos.raw
python3 -m app.run_questions --top-n 10
```

Producer contract:

- Publish each crawler JSON object unchanged to `repos.raw`.
- Use `repo_id` as the Pulsar partition key.
- Send asynchronously via `send_async`; `producer.flush()` blocks at end of run until every outstanding send is acked.
- Retry transient send failures up to `--max-retries` (default 3) with exponential backoff before logging a `permanent_failures` record and continuing.
- Delivery is at-least-once. Consumers MUST be idempotent on `repo_id` because retries can produce duplicate broker writes for the same record.
- The optional publish checkpoint (`--checkpoint-path`) skips already-published `repo_id`s on restart, reducing — but not eliminating — duplicate writes after a crash.

## Experiments TODO

- TODO: Measure scalability across multiple worker/VM counts, such as 1, 2, and 4 workers where feasible.
- TODO: Measure throughput.
- TODO: Measure end-to-end runtime.
- TODO: Measure memory use.
- TODO: Measure GitHub API waiting/rate-limit time.
- TODO: Identify bottlenecks.
- TODO: Evaluate whether unnecessary inter-VM communication was avoided.
- TODO: Save experiment results in a reproducible format.

Expected final command shape:

```bash
# TODO: replace with real experiment command
python3 -m experiments.run_scalability --workers 1,2,4 --input data/output/repos.ndjson
```

## Report TODO

The final report must be max four pages and 3000 words and include:

- TODO: Introduction.
- TODO: Related work.
- TODO: System architecture.
- TODO: Results.

The Results section must include:

- TODO: Graph for Q1.
- TODO: Graph for Q2.
- TODO: Graph for Q3.
- TODO: Graph for Q4.
- TODO: Interesting findings from the collected data.
- TODO: Discussion of adaptability, including changing top 10 to top 20 without source-code changes.
- TODO: Scalability results and interpretation.

Crawler report notes to include:

- The crawler stores results on disk as NDJSON cache files.
- Cache files are used to resume/reuse GitHub API results and reduce repeated API calls.
- Duplicate repositories are removed at the crawler/cache boundary using stable GitHub repository IDs.
- Rate limits are handled through token pooling, token rotation, and looped reset waiting bounded by a total per-request wait budget.
- Full uncapped runs recursively split high-volume day slices into smaller UTC time ranges to avoid GitHub's 1000-result search cap.
- Completeness warnings are emitted when a final search slice still has more than 1000 results or GitHub marks results incomplete.

## Testing

Current crawler tests:

```bash
export PYTHONPATH=src
python3 -m unittest discover -s tests
```

Validate crawler output:

```bash
python3 scripts/validate_crawler_output.py data/output/repos.ndjson
```

TODO:

- Add integration tests for streaming producer/consumer flow.
- Add smoke tests for Docker/services.
- Add experiment reproducibility checks.
- Add result/graph generation checks.

## Deadlines

- Tentative status presentation: 25 May 2026 at 13:15.
- Final submission: 29 May 2026 at 23:59.

## Definition of Done

The project is complete only when:

- Docker containers and scripts can run the system.
- The crawler collects last-year GitHub metadata with pagination, rate-limit handling, cache reuse, and deduplication.
- Metadata flows through the streaming framework using documented producers, consumers, and topics.
- Q1-Q4 produce quantified outputs and graphs.
- Scalability experiments support the report claims.
- The four-page report includes Introduction, Related work, System architecture, and Results.
- The final demo/presentation explains architecture, results, experiments, and individual contributions.
