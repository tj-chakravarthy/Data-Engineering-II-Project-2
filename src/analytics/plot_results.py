"""Generate one graph per project question from JSON result files."""

from __future__ import annotations

import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path("data/.matplotlib").resolve()))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULT_FILES = {
    "q1_languages.json": "Q1 Top languages by number of projects",
    "q2_commits.json": "Q2 Top projects by commit count",
    "q3_tdd_languages.json": "Q3 Top languages with unit tests",
    "q4_tdd_ci_languages.json": "Q4 Top languages with unit tests and CI",
}


def main() -> None:
    results_dir = Path("data/results")
    figures_dir = Path("data/figures")
    figures_dir.mkdir(parents=True, exist_ok=True)

    for filename, title in RESULT_FILES.items():
        path = results_dir / filename
        if not path.exists():
            print(f"Skipping missing {path}")
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        _plot_bar(data, title, figures_dir / filename.replace(".json", ".png"))


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
