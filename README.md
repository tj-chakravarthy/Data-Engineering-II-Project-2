# Data Engineering II Project 2

GitHub analytics system using a streaming framework. Uppsala University 1TD076 Data Engineering II, VT 2026, Group 16.

We crawl GitHub repository metadata, stream it through Pulsar, and answer the four project questions from the brief:

1. Top 10 programming languages by number of projects.
2. Top 10 most frequently updated GitHub projects.
3. Top 10 programming languages with the most projects using unit tests.
4. Top 10 programming languages with the most projects using both unit tests and CI/DevOps.

Assignment PDF: `Project 2 docs/DE-II_project_2-1.pdf`.

## Setup

Target environment: Linux (course VMs and team workstations).

```bash
git clone https://github.com/tj-chakravarthy/Data-Engineering-II-Project-2.git
cd Data-Engineering-II-Project-2
python3 -m venv .venv
source .venv/bin/activate
pip install requests pulsar-client
```

Runtime config lives in `scripts/infrastructure/.env`, tracked in the repo. Tokens go in as `GITHUB_TOKEN_1` through `GITHUB_TOKEN_5` — anything starting with `GITHUB_TOKEN` joins the pool. The crawler rotates through them on rate limits and only sleeps once the whole pool is dry.

## Running the crawler

Two entry points share the same crawler core:

- **`streaming.pulsar_producer`** — streams each record live to a Pulsar topic. This is the path the graded pipeline uses end-to-end.
- **`crawler.cli`** — same crawl but writes to a local NDJSON file instead. Handy when the broker isn't up or for offline checks.

### Live publish to Pulsar (primary)

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

Notable flags:

- `--output PATH` — write every successfully sent record to NDJSON. The file is a **publication log for this run** (truncated on each run, does not include records skipped via `--checkpoint-path`), not a snapshot of the topic.
- `--checkpoint-path PATH` — store every published `repo_id` in a JSON file; restarts skip ids already in it.
- `--max-retries N` (default 3) — per-record retry budget on transient send failure.
- `--max-in-flight N` (default 1000) — cap on outstanding async sends, keeps memory bounded when the broker is slow.

Delivery is **at-least-once**. Retries and crashes can produce duplicate messages; downstream consumers must dedupe by `repo_id`. The checkpoint file shrinks the duplicate window but does not eliminate it.

The producer exits with status 1 if any record permanently failed to publish — pipelines can check `$?`.

### Offline NDJSON crawl

When Pulsar isn't around:

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 1 --limit 25 \
  --output data/output/repos.ndjson
```

A larger run:

```bash
python3 -m crawler.cli --days 365 --date-field created-or-pushed \
  --output data/output/repos.ndjson
```

Useful options (shared between both entry points):

- `--date-field created|pushed|created-or-pushed` — GitHub search date qualifier. `created-or-pushed` runs both and globally dedupes. GitHub's search API doesn't actually expose an `updated:` qualifier (we caught this in a live test), so the brief's "updated in the last year" criterion maps to `pushed:` (last commit) instead.
- `--query "stars:>=10 archived:false"` — extra GitHub search qualifiers. **Very useful for scoping** — a full unfiltered last-year crawl is probably infeasible within the rate-limit budget, so adding a stars filter is the practical move.
- `--refresh-cache` — refetch even if cache files exist.
- `--no-cache` — stream without writing cache files.
- `--limit N` / `--limit-per-day N` — caps for tests; limited slices are not cached.
- `--memory-log-every N` / `--max-memory-mb N` — memory observability and kill switch.

The crawler logs `search_splits`, `search_cap_warnings`, and `incomplete_search_warnings`. Both warning counters must be **zero** before treating a crawl as complete for report-grade results.

Validate a handoff file before downstream ingestion:

```bash
python3 scripts/validate_crawler_output.py data/output/repos.ndjson
```

Non-zero exit means the file should not be streamed.

## Infrastructure

Provisioning scripts live in `scripts/infrastructure/`. They use OpenStack + cloud-init to provision a master and four workers, all `ssc.medium` running Ubuntu 22.04 with Docker installed and SSH between nodes set up.

```bash
cd scripts/infrastructure
cp UPPMAX-openrc.sh.template UPPMAX-openrc.sh.ignore
# Fill in OpenStack values in UPPMAX-openrc.sh.ignore.
./run.sh <PUBLIC_KEY_NAME> <PRIVATE_KEY_PATH>
```

Open items on the infra side:

- Worker count is hard-coded to four; should be configurable.
- Floating IP assignment is manual.
- `run.sh` provisions the VMs and ships the repo to the master; `setup_swarm.sh` deploys the Pulsar/crawler/analytics Swarm stack.
- `setup_swarm.sh` deploys the image named by `CRAWLER_IMAGE`; run `src/build_and_push.sh` first when the image changes.
- `setup_swarm.sh` runs `verify_swarm_pipeline.sh` after deployment; rerun it on the master with `bash /home/ubuntu/app/scripts/infrastructure/verify_swarm_pipeline.sh`.
- A clean-VM reproduction guide still needs a final pass once the full stack is demo-tested.

## Streaming and application logic

Topic layout we're planning for, layered as the brief suggests:

```text
repos.raw            # producer writes here
repos.with_commits   # commit-count enrichment for Q2
repos.with_tests     # unit-test detection for Q3
repos.with_ci        # CI/DevOps detection for Q4
repos.aggregates     # final top-N tables for Q1–Q4
```

Implemented analytics path:

- `streaming.pulsar_producer` publishes crawler records to `repos.raw`.
- `analytics.runner` consumes `repos.raw`, dedupes by `repo_id`, enriches each new repository with commit-count, unit-test, and CI evidence, and publishes derived messages to `repos.with_commits`, `repos.with_tests`, and `repos.with_ci`.
- The same worker writes `data/results/q1_languages.json`, `q2_commits.json`, `q3_tdd_languages.json`, `q4_tdd_ci_languages.json`, and `all_results.json`, then publishes aggregate snapshots to `repos.aggregates`.
- `TOP_N` controls ranking size at runtime, so top 10 → top 20 does not require source changes.

Q2 commits is the expensive part — GitHub's search response doesn't include commit counts, so the analytics worker uses `GET /repos/.../commits?per_page=1` + the `Link: last` pagination header. Q3 and Q4 also require extra content checks for test and CI files.

Producer ↔ consumer contract:

- Messages on `repos.raw` are raw crawler JSON objects, byte-for-byte the same as the NDJSON output, no envelope.
- Pulsar partition key is `str(repo_id)`.
- Delivery is at-least-once; consumers use `repo_id`.

## Experiments

The brief wants scalability across multiple worker/VM counts (1, 2, 4 where feasible), throughput, runtime, memory, GitHub API wait time, and bottleneck identification.

Realistic plan:

- **Now**: producer-side experiments using `crawler.cli` (no broker needed). Vary token pool size, date window, query qualifier. The crawler already tracks `rate_limit_wait_seconds`, `peak_python_memory_kb`, and `search_splits` — those are the numbers to capture.
- **Once broker + consumers are up**: end-to-end experiments where the "worker count" knob actually means something (consumer parallelism, partition count on the topic).

Expected end-to-end command shape, once everything exists:

```bash
python3 -m experiments.run_scalability --workers 1,2,4 --input data/output/repos.ndjson
```

## Report

Max 4 pages AND 3000 words. Sections: Introduction, Related work, System architecture, Results. Results needs a graph per Q1–Q4, an adaptability discussion (top-10 vs top-20), interesting findings, and the scalability story.

Crawler-side things worth mentioning in the architecture / results sections:

- Day-sliced GitHub search with adaptive UTC time-range subdivision when a day exceeds the 1000-result API cap. We verified this works against real GitHub: a one-day `pushed:` query at `stars:>=100` returns ~17k records and the splitter recursively narrows it to 24 leaf ranges, all under 1000.
- Token-pool rate-limit handling with rotation + bounded sleep + total-wait budget.
- Cache-backed NDJSON storage; reruns hit the cache and don't burn API calls.
- Dedup by stable `repo_id` at the crawler/cache boundary.
- Live producer uses async `send_async` + `flush()`, bounded retry, optional checkpoint. At-least-once delivery, consumer-idempotent on `repo_id`.

## Testing

```bash
export PYTHONPATH=src
python3 -m unittest discover -s tests
```

52 tests right now. Crawler covers date slicing, dedup, adaptive range splitting, streaming cache writes, rate-limit retry/budget, and local `.env` loading. Producer covers async send, transient retry recovery, permanent failure handling, in-flight bound, checkpoint skip + persist, ack-gated NDJSON mirror.

Still TODO when the rest of the stack lands: integration tests for the producer→broker→consumer flow, smoke tests against the Docker Swarm deployment, experiment reproducibility checks.

## Deadlines

- [Status presentation](https://use.mazemap.com/#v=1&config=uu&campusid=49&search=%C3%85ngstr%C3%B6m%202002&zlevel=1&center=17.647455,59.839254&zoom=18&sharepoitype=poi&sharepoi=390487): **25 May 2026 at 13:15**.
- [Final submission](https://uppsala.instructure.com/courses/115762/assignments/389958): **29 May 2026 at 23:59**.

## Definition of done

We're done when:

- Docker containers + scripts bring the whole system up reproducibly on the SSC VMs.
- The crawler collects last-year GitHub metadata with pagination, rate-limit handling, cache reuse, and dedup.
- Metadata flows through Pulsar via documented producer + consumers + topics.
- Q1–Q4 produce quantified answers and graphs.
- Scalability experiments back up the report claims.
- The 4-page report is written.
- The demo explains architecture, results, experiments, and individual contributions.
