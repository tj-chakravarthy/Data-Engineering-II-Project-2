"""Small helper functions for the analytics scripts."""

from __future__ import annotations

import base64
import json
import os
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

from crawler.github_client import GitHubClient

TEST_PATHS = ["tests", "test", "spec", "__tests__", "pytest.ini", "package.json", "pom.xml", "Cargo.toml"]
CI_PATHS = [".github/workflows", ".gitlab-ci.yml", ".travis.yml", "Jenkinsfile", ".circleci/config.yml"]
TEST_WORDS = ["pytest", "unittest", "jest", "junit", "rspec", "go test", "cargo test"]


def config() -> dict:
    results_dir = Path(os.getenv("RESULTS_DIR", "data/results"))
    return {
        "broker_url": os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650"),
        "raw_topic": os.getenv("RAW_TOPIC", "repos.raw"),
        "commits_topic": os.getenv("COMMITS_TOPIC", "repos.with_commits"),
        "tests_topic": os.getenv("TESTS_TOPIC", "repos.with_tests"),
        "ci_topic": os.getenv("CI_TOPIC", "repos.with_ci"),
        "aggregate_topic": os.getenv("AGGREGATE_TOPIC", "repos.aggregates"),
        "subscription": os.getenv("ANALYTICS_SUBSCRIPTION", "analytics-q1-q4"),
        "top_n": int(os.getenv("TOP_N", "10")),
        "results_dir": results_dir,
        "state_path": Path(os.getenv("ANALYTICS_STATE_PATH", str(results_dir / "analytics_state.json"))),
        "flush_every": int(os.getenv("FLUSH_EVERY", "100")),
        "enrich_github": os.getenv("ENRICH_GITHUB", "true").lower() != "false",
        "max_repos": int(os.getenv("MAX_REPOS")) if os.getenv("MAX_REPOS") else None,
    }


class AnalyticsState:
    def __init__(self) -> None:
        self.seen: set[int] = set()
        self.languages: Counter[str] = Counter()
        self.commits: dict[str, int] = {}
        self.test_languages: Counter[str] = Counter()
        self.test_ci_languages: Counter[str] = Counter()

    @classmethod
    def load(cls, state_path: Path) -> AnalyticsState:
        """Rebuild state from a previous run's state file for crash recovery.

        Returns empty state when the file does not exist yet.
        """
        state = cls()
        if not state_path.exists():
            return state
        with state_path.open(encoding="utf-8") as file:
            data = json.load(file)
        state.seen = {int(repo_id) for repo_id in data.get("seen_repo_ids", [])}
        state.languages = Counter(data.get("language_counts", {}))
        state.commits = {
            str(name): int(count)
            for name, count in data.get("commit_counts", {}).items()
        }
        state.test_languages = Counter(data.get("tdd_language_counts", {}))
        state.test_ci_languages = Counter(data.get("tdd_ci_language_counts", {}))
        return state

    def add_repo(self, repo: dict, enrichment: dict | None = None) -> bool:
        repo_id = int(repo["repo_id"])
        if repo_id in self.seen:
            return False
        self.seen.add(repo_id)

        enrichment = enrichment or {}
        language = repo.get("language") or "Unknown"
        full_name = repo.get("full_name") or str(repo_id)

        self.languages[language] += 1
        if enrichment.get("commit_count") is not None:
            self.commits[full_name] = int(enrichment["commit_count"])
        if enrichment.get("has_tests"):
            self.test_languages[language] += 1
        if enrichment.get("has_tests") and enrichment.get("has_ci"):
            self.test_ci_languages[language] += 1
        return True

    def results(self, top_n: int) -> dict:
        return {
            "q1_top_languages_by_projects": top_counter(self.languages, top_n),
            "q2_top_projects_by_commits": top_dict(self.commits, top_n),
            "q3_top_languages_with_tests": top_counter(self.test_languages, top_n),
            "q4_top_languages_with_tests_and_ci": top_counter(self.test_ci_languages, top_n),
            "processed_unique_repositories": len(self.seen),
            "top_n": top_n,
        }

    def save(self, results_dir: Path, state_path: Path, top_n: int) -> None:
        results = self.results(top_n)
        results_dir.mkdir(parents=True, exist_ok=True)
        write_json(results_dir / "q1_languages.json", results["q1_top_languages_by_projects"])
        write_json(results_dir / "q2_commits.json", results["q2_top_projects_by_commits"])
        write_json(results_dir / "q3_tdd_languages.json", results["q3_top_languages_with_tests"])
        write_json(results_dir / "q4_tdd_ci_languages.json", results["q4_top_languages_with_tests_and_ci"])
        write_json(results_dir / "all_results.json", results)
        write_json(state_path, {
            "seen_repo_ids": sorted(self.seen),
            "language_counts": dict(self.languages),
            "commit_counts": self.commits,
            "tdd_language_counts": dict(self.test_languages),
            "tdd_ci_language_counts": dict(self.test_ci_languages),
        })


def enrich_repo(client: GitHubClient, repo: dict) -> dict:
    full_name = repo["full_name"]
    branch = repo.get("default_branch")
    has_tests, test_evidence = path_matches(client, full_name, TEST_PATHS, branch, check_test_file=True)
    has_ci, ci_evidence = path_matches(client, full_name, CI_PATHS, branch)
    return {
        "commit_count": commit_count(client, full_name),
        "has_tests": has_tests,
        "test_evidence": test_evidence,
        "has_ci": has_ci,
        "ci_evidence": ci_evidence,
    }


def commit_count(client: GitHubClient, full_name: str) -> int | None:
    try:
        response = client.get(f"/repos/{full_name}/commits", {"per_page": 1})
    except requests.HTTPError as exc:
        # 409 = empty repository (no commits); anything else = unknown.
        status = exc.response.status_code if exc.response is not None else None
        return 0 if status == 409 else None
    last = last_page(response.headers.get("Link", ""))
    if last is not None:
        return last
    try:
        payload = response.json()
    except ValueError:
        return None
    return len(payload) if isinstance(payload, list) else None


def path_matches(client: GitHubClient, full_name: str, paths: list[str], branch: str | None, check_test_file: bool = False) -> tuple[bool, str | None]:
    for path in paths:
        try:
            response = client.get(f"/repos/{full_name}/contents/{path}", {"ref": branch} if branch else None)
        except requests.HTTPError:
            # 404 = path absent (the common case); try the next path.
            continue
        if not check_test_file or path in {"tests", "test", "spec", "__tests__"}:
            return True, path
        text = decode_github_file(response)
        if any(word in text.lower() for word in TEST_WORDS):
            return True, path
    return False, None


def decode_github_file(response: requests.Response) -> str:
    try:
        payload = response.json()
        return base64.b64decode(payload.get("content", "")).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def last_page(link_header: str) -> int | None:
    for part in link_header.split(","):
        if 'rel="last"' in part:
            url = part.split(";", 1)[0].strip().strip("<>")
            page = parse_qs(urlparse(url).query).get("page", [None])[0]
            return int(page) if page else None
    return None


def top_counter(counter: Counter[str], n: int) -> list[dict]:
    return [{"name": name, "count": count} for name, count in counter.most_common(n)]


def top_dict(values: dict[str, int], n: int) -> list[dict]:
    items = sorted(values.items(), key=lambda item: item[1], reverse=True)
    return [{"name": name, "count": count} for name, count in items[:n]]


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp file and atomically replace, so a crash mid-write never
    # leaves a truncated file — the state file must stay loadable on restart.
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, sort_keys=True)
        file.write("\n")
    tmp.replace(path)
