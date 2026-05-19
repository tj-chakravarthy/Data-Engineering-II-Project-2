# Crawler Handoff

Owner: TJ

This document defines the boundary between the crawler-owned work and the downstream streaming/application logic.

## Responsibility Boundary

The crawler is responsible for:

- collecting GitHub repository metadata from the GitHub REST API;
- partitioning searches by date to work around GitHub's 1000-result search cap;
- supporting `created`, `updated`, `pushed`, and combined `created-or-updated` date-sliced crawl modes;
- following pagination;
- handling rate limits with token pooling, token rotation, and looped reset waiting bounded by a total per-request wait budget;
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

Cache files are one date/query slice per file:

```text
data/cache/repos_<date-field>_<YYYY-MM-DD>*.ndjson
```

Examples:

```text
data/cache/repos_created_2026-05-19.ndjson
data/cache/repos_updated_2026-05-19_stars_10_archived_false.ndjson
```

Limited test runs using `--limit` or `--limit-per-day` do not write cache files for fetched slices because the slice is intentionally incomplete. Full slices are cached and can be reused by later runs.

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

## Deduplication Contract

The crawler deduplicates using:

1. `repo_id`, GitHub's stable repository identifier;
2. `full_name.lower()` only as a fallback if `repo_id` is unavailable.

Deduplication happens:

- within each cache slice before writing the cache file;
- across the final emitted output stream.

This means downstream consumers should treat `repo_id` as the primary key for repository records.

## Running Locally

Target environment: Linux (course VMs and team workstations).

Small smoke crawl:

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 1 --limit 25 --cache-dir data/cache --output data/output/repos.ndjson
```

Full last-year crawl using both created-date and updated-date slices:

```bash
export PYTHONPATH=src
python3 -m crawler.cli --days 365 --date-field created-or-updated --cache-dir data/cache --output data/output/repos.ndjson
```

Individual date modes are also supported:

```bash
python3 -m crawler.cli --days 365 --date-field created
python3 -m crawler.cli --days 365 --date-field updated
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

## Report Notes

The report should state that:

- GitHub API results are stored on disk as NDJSON cache files;
- caching supports resumable/repeatable runs and reduces repeated API calls;
- duplicate repositories are removed at the crawler/cache boundary;
- the `created-or-updated` mode covers repositories created or updated in the last year and deduplicates overlap globally;
- downstream application logic assumes deduplicated repository records;
- rate limits are handled through token pooling, token rotation, and looped reset waiting bounded by a total per-request wait budget;
- crawler memory is controlled by streaming records, cache-backed processing, and optional memory-limit checks.
