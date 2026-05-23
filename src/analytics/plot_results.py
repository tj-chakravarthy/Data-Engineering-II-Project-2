"""Generate one graph per project question from JSON result files."""

from __future__ import annotations

import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path("data/.matplotlib").resolve()))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULT_SPECS = {
    "q1_languages": {
        "filename": "q1_languages",
        "aggregate_key": "q1_top_languages_by_projects",
        "title": "Q1 Top languages by number of projects",
    },
    "q2_commits": {
        "filename": "q2_commits",
        "aggregate_key": "q2_top_projects_by_commits",
        "title": "Q2 Top projects by commit count",
    },
    "q3_tdd_languages": {
        "filename": "q3_tdd_languages",
        "aggregate_key": "q3_top_languages_with_tests",
        "title": "Q3 Top languages with unit tests",
    },
    "q4_tdd_ci_languages": {
        "filename": "q4_tdd_ci_languages",
        "aggregate_key": "q4_top_languages_with_tests_and_ci",
        "title": "Q4 Top languages with unit tests and CI",
    },
}


def main() -> None:
    results_dir = Path(os.getenv("RESULTS_DIR", "data/results"))
    figures_dir = Path(os.getenv("FIGURES_DIR", "data/figures"))
    plot_result_files(results_dir, figures_dir)


def plot_result_files(results_dir: Path, figures_dir: Path) -> list[Path]:
    figures_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for spec in RESULT_SPECS.values():
        path = results_dir / f"{spec['filename']}.json"
        if not path.exists():
            print(f"Skipping missing {path}")
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        output_path = figures_dir / f"{spec['filename']}.png"
        _plot_bar(data, spec["title"], output_path)
        written.append(output_path)
    return written


def plot_aggregate_payload(payload: dict, figures_dir: Path) -> list[Path]:
    """Plot all Q1-Q4 result tables from one aggregate payload."""
    figures_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for spec in RESULT_SPECS.values():
        data = payload.get(spec["aggregate_key"])
        if data is None:
            continue
        output_path = figures_dir / f"{spec['filename']}.png"
        _plot_bar(data, spec["title"], output_path)
        written.append(output_path)
    return written


def _plot_bar(data: list[dict], title: str, output_path: Path) -> None:
    names = [str(row["name"]) for row in data]
    counts = [int(row["count"]) for row in data]

    plt.figure(figsize=(10, 6))
    plt.barh(names, counts)
    plt.gca().invert_yaxis()
    plt.title(title)
    plt.xlabel("Count")
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
