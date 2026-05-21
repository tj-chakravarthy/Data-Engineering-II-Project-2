# Crawler Handoff

Owner: TJ

This document defines the boundary between the crawler-owned work and the downstream streaming/application logic.

## Responsibility Boundary

The crawler is responsible for:

- collecting GitHub repository metadata from the GitHub REST API;
- partitioning searches by date to work around GitHub's 1000-result search cap;
- recursively splitting full uncapped runs into smaller UTC time ranges when a day still exceeds the search cap;
- supporting `created`, `pushed`, and combined `created-or-pushed` date-sliced crawl modes (GitHub does not expose an `updated:` search qualifier; the PDF's "updated" criterion is implemented as `pushed:`);
- following pagination;
- handling rate limits with token pooling, token rotation, and looped reset waiting bounded by a total per-request wait budget;
- warning when GitHub reports a search slice above the 1000-result API cap or marks results incomplete;
- writing and reusing disk-backed cache files;
- removing duplicate repository records before output;
- producing normalized NDJSON records for the streaming layer.

Downstream application logic should not detect or correct duplicate repositories. It can assume crawler output is deduplicated by stable GitHub repository ID.

## Output Location

Default crawler output:

```text
data/output/repos.ndjson
```

Default cache location:

```text
data/cache/
```

The CLI overwrites `data/output/repos.ndjson` on each run so that this path
always represents the latest materialized crawler output. For experiments,
comparisons, or partial runs, pass a run-specific `--output` path such as
`data/output/repos_7day_stars10.ndjson`. Cache files are independent from the
output file and are reused unless `--refresh-cache` is passed.

For live streaming, `streaming.pulsar_producer` consumes the crawler iterator
directly and publishes records as they are yielded. When the crawler fetches a
complete API slice, it writes each record to a temporary cache file and yields it
immediately; the temporary cache file is promoted to the final cache path only
after the full slice completes.

Cache files are one date/query slice per file:

```text
data/cache/repos_<date-field>_<YYYY-MM-DD>*.ndjson
```

Examples:

```text
data/cache/repos_created_2026-05-19.ndjson
data/cache/repos_pushed_2026-05-19_stars_10_archived_false.ndjson
```

Limited test runs using `--limit` or `--limit-per-day` do not write cache files for fetched slices because the slice is intentionally incomplete. Full slices are cached and can be reused by later runs.

## GitHub Tokens and Rate Limits

The crawler uses a token pool. Any environment variable whose name starts with
`GITHUB_TOKEN` is loaded automatically, for example:

```text
GITHUB_TOKEN=...
GITHUB_TOKEN_2=...
GITHUB_TOKEN_3=...
GITHUB_TOKEN_4=...
GITHUB_TOKEN_5=...
```

For each GitHub request, the client tries the current token and rotates through
the pool when a token is rate-limited. If every configured token is exhausted,
the client waits for reset, bounded by the configured per-request total wait
budget.

## Output Format

Each output line is one JSON object. Current fields:

```json
{
  "repo_id": 123456,
  "full_name": "owner/repository",
  "language": "Python",
  "stars": 42,
  "forks": 3,
  "created_at": "2026-05-19T00:00:00Z",
  "updated_at": "2026-05-19T00:00:00Z",
  "pushed_at": "2026-05-19T00:00:00Z",
  "size_kb": 1024,
  "default_branch": "main",
  "crawl_day": "2026-05-19",
  "archived": false,
  "topics": ["data", "api"],
  "open_issues_count": 7
}
```

Downstream producers should publish these records to the raw repository metadata topic without changing the crawler-owned fields.

Recommended producer behavior:

- use `python3 -m streaming.pulsar_producer` for live publishing (the primary path);
- the producer streams records directly from the crawler — no intermediate file read pass is required;
- pass `--output data/output/repos.ndjson` if an NDJSON mirror is also wanted for the validator or for offline analysis;
- publish each JSON object unchanged to the raw metadata topic;
- use `repo_id` as the Pulsar partition key;
- treat `full_name` as display data, not as the primary key;
- keep downstream enrichment fields additive instead of rewriting crawler-owned fields.

## Delivery Semantics

The producer uses asynchronous `send_async` with `producer.flush()` at end of
run, and bounded retry (`--max-retries`, default 3, with exponential backoff)
on transient send failures. Delivery is **at-least-once**: a record can be
written to Pulsar more than once if a retry fires after the broker actually
accepted the original send, or across reruns of a crashed producer.

**Downstream consumers MUST be idempotent on `repo_id`.** The crawler key in
the message and the natural primary key in the data are both `repo_id`, so
consumer-side dedup is straightforward (e.g., upsert keyed on `repo_id`, or
read-before-write).

The optional `--checkpoint-path` flag persists every successfully published
`repo_id` to a JSON file and skips already-published `repo_id`s on restart.
This reduces — but does not eliminate — duplicate broker writes after a crash,
because in-flight sends may have been acked just before the crash without yet
being recorded in the checkpoint file. The consumer-idempotency requirement
stands either way.

Records that exhaust `--max-retries` are logged as `permanent_failures` and
the run continues with the remaining records. Permanent failures should be
treated as a runbook alert: re-run the crawler with the same checkpoint to
re-attempt only those records.

## Deduplication Contract

The crawler deduplicates using:

1. `repo_id`, GitHub's stable repository identifier;
2. `full_name.lower()` only as a fallback if `repo_id` is unavailable.

Deduplication happens:

- within each cache slice before writing the cache file;
- across the final emitted output stream.

This means downstream consumers should treat `repo_id` as the primary key for repository records.

## Completeness Guardrails

GitHub search exposes at most 1000 results for a single search query. The crawler starts with one-day queries. For full uncapped runs, it checks GitHub's reported `total_count` and recursively splits high-volume days into smaller UTC time ranges before fetching records.

When the crawler splits a high-volume range, it increments `search_splits`. When a range cannot be split further and still reports `total_count > 1000`, it logs a warning and increments `search_cap_warnings`. When GitHub marks a search response as incomplete, it increments `incomplete_search_warnings`.

Do not use a crawl as final report data unless both counters are zero in the CLI stats line:

```text
search_cap_warnings=0 incomplete_search_warnings=0
```

If either warning counter is non-zero, rerun with a narrower qualifier such as a language/star partition, or agree on the limitation explicitly before producing final graphs. A positive `search_splits` value is acceptable; it means the crawler had to subdivide busy ranges.

Limited smoke runs using `--limit` or `--limit-per-day` do not recursively split because they are intentionally incomplete and are not cached as final data.

## Running Locally

Target environment: Linux (course VMs and team workstations).

Small smoke crawl:

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 1 --limit 25 --cache-dir data/cache --output data/output/repos.ndjson
```

Full last-year crawl using both created-date and pushed-date slices:

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 365 --date-field created-or-pushed --cache-dir data/cache --output data/output/repos.ndjson
```

Live Pulsar publishing to `repos.raw`:

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

`--output` mirrors the published records to NDJSON so the validator can run
against the streaming path; omit it if you only need the live topic. See
"Delivery Semantics" above for at-least-once behavior, retry, and idempotency
requirements.

Individual date modes are also supported:

```bash
python3 -m crawler.cli --days 365 --date-field created
python3 -m crawler.cli --days 365 --date-field pushed
```

Useful operational flags:

```bash
python3 -m crawler.cli \
  --days 7 \
  --query "stars:>=10 archived:false" \
  --refresh-cache \
  --memory-log-every 500 \
  --max-memory-mb 512
```

Validate the handoff file before a producer reads it:

```bash
python3 scripts/validate_crawler_output.py data/output/repos.ndjson
```

The validator checks JSON parsing, required crawler fields, duplicate repository keys, and prints a short language preview. A non-zero exit means the file should not be streamed into `repos.raw`.

## Expected CLI Stats

The crawler prints one final stats line. The fields most relevant for handoff are:

```text
emitted=<deduplicated records written>
fetched=<records fetched from GitHub before dedupe>
cache_written=<records written to cache>
loaded_from_cache=<records reused from cache>
slice_duplicates=<duplicates removed inside one date/query slice>
global_duplicates=<duplicates removed across final output>
memory_samples=<number of memory checks>
peak_python_memory_kb=<tracked Python peak memory>
search_splits=<high-volume ranges split before fetching>
search_cap_warnings=<slices that may be capped by GitHub>
incomplete_search_warnings=<slices GitHub marked incomplete>
rate_limit_waits=<rate-limit sleep cycles>
rate_limit_wait_seconds=<total rate-limit sleep time>
```

For report-grade data, `search_cap_warnings` and `incomplete_search_warnings` should be zero. `search_splits` and `global_duplicates` can be positive. `global_duplicates` is expected in `created-or-pushed` mode because the same repository can appear in both date fields.

## Report Notes

The report should state that:

- GitHub API results are stored on disk as NDJSON cache files;
- caching supports resumable/repeatable runs and reduces repeated API calls;
- duplicate repositories are removed at the crawler/cache boundary;
- the `created-or-pushed` mode covers repositories created or last-pushed in the last year and deduplicates overlap globally; the PDF's "updated" criterion is implemented via GitHub's `pushed:` qualifier because no `updated:` search qualifier exists;
- downstream application logic assumes deduplicated repository records;
- full uncapped runs recursively split busy day ranges into smaller UTC time ranges to stay under GitHub's 1000-result search cap;
- rate limits are handled through token pooling, token rotation, and looped reset waiting bounded by a total per-request wait budget;
- crawler memory is controlled by streaming records, cache-backed processing, and optional memory-limit checks;
- search-cap and incomplete-result counters are used to decide whether a crawl is complete enough for final analytics.
