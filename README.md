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

## Running the Crawler

The crawler collects repository metadata, handles pagination/rate limits, writes disk-backed cache files, and removes duplicates before downstream streaming/application logic receives the data.

Run a small local crawl:

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 1 --limit 25 --cache-dir data/cache --output data/output/repos.ndjson
```

Run a larger crawl:

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 365 --date-field created-or-updated --cache-dir data/cache --output data/output/repos.ndjson
```

Useful options:

- `--date-field created|updated|pushed|created-or-updated`: choose the GitHub date qualifier. `created-or-updated` runs both date-sliced searches and globally deduplicates the output.
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

Validate a crawler handoff file before streaming ingestion:

```bash
python3 scripts/validate_crawler_output.py data/output/repos.ndjson
```

Crawler handoff status:

- Implemented: date-sliced GitHub search, adaptive UTC time-range splitting for full uncapped runs, pagination, token-pool rate-limit handling, cache reuse, deduplication, memory sampling/limit checks, normalized NDJSON output, handoff validation, and unit tests.
- Handoff contract: producers should publish each NDJSON line unchanged to the raw repository metadata topic and treat `repo_id` as the primary key.
- Completeness guardrail: full uncapped runs split any slice that GitHub reports above 1000 results. If a slice still cannot be split enough, the crawler logs and counts a search-cap warning. Narrow the query before using that data as final.
- Local smoke artifacts currently exist only under ignored `data/` paths and are not part of the committed submission.

Crawler handoff details are documented in `docs/crawler_handoff.md`.

## Infrastructure TODO

Required by project for highest grade:

- TODO: Provide one script to initialize the head VM and configurable worker VMs.
- TODO: Install all required packages on the head VM and worker VMs.
- TODO: Configure Docker/services needed to run the full system.
- TODO: Configure shared SSH access for team members.
- TODO: Assume `ssc.medium` VMs unless the team decides otherwise.
- TODO: Make worker count configurable instead of hard-coding four workers.
- TODO: Document how to reproduce the environment from a clean VM.

Expected final command shape:

```bash
# TODO: replace with real infrastructure command
./scripts/provision.sh --workers 4
```

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

Expected final command shape:

```bash
# TODO: replace with real streaming/application commands
docker compose up -d
python3 -m app.producer --input data/output/repos.ndjson
python3 -m app.run_questions --top-n 10
```

Producer input contract:

- Read `data/output/repos.ndjson` line by line.
- Validate the file first with `scripts/validate_crawler_output.py`.
- Publish each JSON object unchanged to `repos.raw`.
- Use `repo_id` as the message key when the streaming framework supports keyed messages.
- Do not rededuplicate downstream unless a validation failure or manual data edit is detected.

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
