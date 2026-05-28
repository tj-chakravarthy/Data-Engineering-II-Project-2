import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from datetime import datetime
import os

# ── output folder ──────────────────────────────────────────────────────────────
OUT = "/home/claude/figures"
os.makedirs(OUT, exist_ok=True)

# ── IEEE-ish style ─────────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family": "DejaVu Serif",
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
    "axes.labelsize": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "axes.grid.axis": "x",
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "figure.dpi": 150,
    "savefig.bbox": "tight",
    "savefig.dpi": 200,
})

BLUE   = "#2166ac"
ORANGE = "#d6604d"
GREEN  = "#4dac26"
PURPLE = "#7b2d8b"
COLORS = [BLUE, ORANGE, GREEN, PURPLE,
          "#f4a582", "#92c5de", "#a6dba0", "#c2a5cf",
          "#878787", "#1a1a1a"]

# ── load data ──────────────────────────────────────────────────────────────────
with open("/mnt/user-data/uploads/1779877020865_all_results.json") as f:
    final = json.load(f)

history = []
with open("/mnt/user-data/uploads/1779877020866_results_history.jsonl") as f:
    for line in f:
        history.append(json.loads(line.strip()))

print(f"Loaded final results: {final['processed_unique_repositories']:,} repos")
print(f"Loaded history: {len(history)} snapshots")

# ══════════════════════════════════════════════════════════════════════════════
# HELPER: horizontal bar chart
# ══════════════════════════════════════════════════════════════════════════════
def hbar(ax, names, counts, colors, title, xlabel, note=None):
    y = np.arange(len(names))
    bars = ax.barh(y, counts, color=colors[:len(names)], edgecolor="white",
                   linewidth=0.6, height=0.65)
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_title(title, pad=8)
    # value labels
    for bar, val in zip(bars, counts):
        ax.text(bar.get_width() + max(counts)*0.01, bar.get_y() + bar.get_height()/2,
                f"{val:,}", va="center", ha="left", fontsize=8, color="#333333")
    ax.set_xlim(0, max(counts) * 1.18)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{int(x/1000)}k" if x >= 2000 else str(int(x))))
    if note:
        ax.annotate(note, xy=(0.98, 0.04), xycoords="axes fraction",
                    ha="right", fontsize=7.5, color="#666666",
                    style="italic")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 1 — Q1: Top 10 languages by repo count
# ══════════════════════════════════════════════════════════════════════════════
q1 = final["q1_top_languages_by_projects"]
names  = [r["name"] for r in q1]
counts = [r["count"] for r in q1]

fig, ax = plt.subplots(figsize=(6.5, 4.2))
hbar(ax, names, counts, COLORS,
     "Q1 — Top 10 Programming Languages by Repository Count",
     "Number of repositories",
     note=f"n = {final['processed_unique_repositories']:,} repositories")
plt.tight_layout()
plt.savefig(f"{OUT}/q1_languages.png")
plt.close()
print("✓ q1_languages.png")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 2 — Q2: Top 10 repos by commit count  (shorten long names)
# ══════════════════════════════════════════════════════════════════════════════
q2 = final["q2_top_projects_by_commits"]

def shorten(name, maxlen=34):
    parts = name.split("/")
    short = parts[-1] if len(parts) > 1 else name
    return short[:maxlen] + "…" if len(short) > maxlen else short

names2  = [shorten(r["name"]) for r in q2]
counts2 = [r["count"] for r in q2]

fig, ax = plt.subplots(figsize=(6.5, 4.2))
hbar(ax, names2, counts2, COLORS,
     "Q2 — Top 10 Repositories by Commit Count",
     "Total commits",
     note=f"n = {final['processed_unique_repositories']:,} repositories")
plt.tight_layout()
plt.savefig(f"{OUT}/q2_commits.png")
plt.close()
print("✓ q2_commits.png")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 3 — Q3: Top 10 TDD languages
# ══════════════════════════════════════════════════════════════════════════════
q3 = final["q3_top_languages_with_tests"]
names3  = [r["name"] for r in q3]
counts3 = [r["count"] for r in q3]

fig, ax = plt.subplots(figsize=(6.5, 4.2))
hbar(ax, names3, counts3, COLORS,
     "Q3 — Top 10 Languages by Test-Driven Development Adoption",
     "Number of repositories with test files",
     note=f"n = {final['processed_unique_repositories']:,} repositories")
plt.tight_layout()
plt.savefig(f"{OUT}/q3_tdd.png")
plt.close()
print("✓ q3_tdd.png")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 4 — Q4: Top 10 TDD+CI languages
# ══════════════════════════════════════════════════════════════════════════════
q4 = final["q4_top_languages_with_tests_and_ci"]
names4  = [r["name"] for r in q4]
counts4 = [r["count"] for r in q4]

fig, ax = plt.subplots(figsize=(6.5, 4.2))
hbar(ax, names4, counts4, COLORS,
     "Q4 — Top 10 Languages with TDD and CI/DevOps",
     "Number of repositories with tests and CI config",
     note=f"n = {final['processed_unique_repositories']:,} repositories")
plt.tight_layout()
plt.savefig(f"{OUT}/q4_tdd_ci.png")
plt.close()
print("✓ q4_tdd_ci.png")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 5 — Trend: Q1 rank stability over time  (top-5 languages)
# ══════════════════════════════════════════════════════════════════════════════
TRACK_Q1 = ["HTML", "Python", "JavaScript", "TypeScript", "Java"]
TRACK_COLORS = {"HTML": BLUE, "Python": ORANGE, "JavaScript": GREEN,
                "TypeScript": PURPLE, "Java": "#878787"}

repo_counts_hist = [h["processed_unique_repositories"] for h in history]

def get_rank(snapshot, key, lang):
    for i, item in enumerate(snapshot.get(key, [])):
        if item["name"] == lang:
            return i + 1
    return 11   # not in top-10

fig, ax = plt.subplots(figsize=(6.5, 3.8))
for lang in TRACK_Q1:
    ranks = [get_rank(h, "q1_top_languages_by_projects", lang) for h in history]
    ax.plot(repo_counts_hist, ranks, label=lang,
            color=TRACK_COLORS[lang], linewidth=1.6, alpha=0.9)

ax.invert_yaxis()
ax.set_ylim(10.5, 0.5)
ax.yaxis.set_major_locator(mticker.MultipleLocator(1))
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"#{int(x)}"))
ax.set_xlabel("Repositories processed", fontsize=9)
ax.set_ylabel("Rank", fontsize=9)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{int(x/1000)}k" if x >= 2000 else str(int(x))))
ax.set_title("Q1 — Language Rank Stability as More Repos are Processed", pad=8)
ax.legend(fontsize=8.5, loc="upper right", framealpha=0.8)
ax.grid(axis="y", alpha=0.2)
plt.tight_layout()
plt.savefig(f"{OUT}/trend_q1_rank_stability.png")
plt.close()
print("✓ trend_q1_rank_stability.png")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 6 — Trend: Q3 + Q4 count convergence  (Python, TypeScript, JS)
# ══════════════════════════════════════════════════════════════════════════════
TRACK_Q34 = ["Python", "TypeScript", "JavaScript"]
TC = {"Python": ORANGE, "TypeScript": PURPLE, "JavaScript": GREEN}

def get_count(snapshot, key, lang):
    for item in snapshot.get(key, []):
        if item["name"] == lang:
            return item["count"]
    return 0

fig, axes = plt.subplots(1, 2, figsize=(9, 3.6), sharey=False)

for lang in TRACK_Q34:
    counts_q3 = [get_count(h, "q3_top_languages_with_tests", lang) for h in history]
    axes[0].plot(repo_counts_hist, counts_q3, label=lang,
                 color=TC[lang], linewidth=1.6)

axes[0].set_title("Q3 — TDD Adoption Convergence", pad=8)
axes[0].set_xlabel("Repositories processed", fontsize=9)
axes[0].set_ylabel("Repos with test files", fontsize=9)
axes[0].legend(fontsize=8.5)
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{int(x/1000)}k" if x >= 2000 else str(int(x))))

for lang in TRACK_Q34:
    counts_q4 = [get_count(h, "q4_top_languages_with_tests_and_ci", lang) for h in history]
    axes[1].plot(repo_counts_hist, counts_q4, label=lang,
                 color=TC[lang], linewidth=1.6)

axes[1].set_title("Q4 — TDD + CI Adoption Convergence", pad=8)
axes[1].set_xlabel("Repositories processed", fontsize=9)
axes[1].set_ylabel("Repos with tests and CI", fontsize=9)
axes[1].legend(fontsize=8.5)
axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{int(x/1000)}k" if x >= 2000 else str(int(x))))

plt.suptitle("Live Aggregator Convergence — Q3 and Q4", fontsize=11, fontweight="bold", y=1.01)
plt.tight_layout()
plt.savefig(f"{OUT}/trend_q3_q4_convergence.png")
plt.close()
print("✓ trend_q3_q4_convergence.png")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 7 — Throughput over time (repos processed per snapshot interval)
# ══════════════════════════════════════════════════════════════════════════════
timestamps = [h["timestamp"] for h in history]
repo_counts = [h["processed_unique_repositories"] for h in history]

# derive per-interval throughput (repos between consecutive snapshots)
intervals_sec  = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
repos_delta    = [repo_counts[i+1] - repo_counts[i] for i in range(len(repo_counts)-1)]
# throughput: repos per minute (smoothed with a rolling 10-window average)
throughput_rpm = [d / (s/60) if s > 0 else 0 for d, s in zip(repos_delta, intervals_sec)]

# rolling average (window=10)
window = 10
rolling = np.convolve(throughput_rpm, np.ones(window)/window, mode="valid")
rolling_x = repo_counts[window//2 : window//2 + len(rolling)]

fig, ax = plt.subplots(figsize=(6.5, 3.5))
ax.plot(repo_counts[1:], throughput_rpm, color=BLUE, alpha=0.2, linewidth=0.8, label="Raw")
ax.plot(rolling_x, rolling, color=BLUE, linewidth=2, label=f"Rolling avg (n={window})")
ax.set_xlabel("Repositories processed", fontsize=9)
ax.set_ylabel("Throughput (repos / min)", fontsize=9)
ax.set_title("System Throughput Over the Full Run", pad=8)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{int(x/1000)}k" if x >= 2000 else str(int(x))))
ax.legend(fontsize=8.5)

# annotate total run stats
total_hours = (timestamps[-1] - timestamps[0]) / 3600
avg_rpm     = repo_counts[-1] / ((timestamps[-1] - timestamps[0]) / 60)
ax.annotate(f"Total: {repo_counts[-1]:,} repos in {total_hours:.1f} h  |  avg {avg_rpm:.1f} repos/min",
            xy=(0.5, 0.96), xycoords="axes fraction", ha="center",
            fontsize=8, color="#444444", style="italic")
plt.tight_layout()
plt.savefig(f"{OUT}/throughput_over_time.png")
plt.close()
print("✓ throughput_over_time.png")

# ══════════════════════════════════════════════════════════════════════════════
# PRINT SUMMARY STATS  (useful for writing the Results section)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("SUMMARY STATS FOR REPORT")
print("="*60)
total_min = (timestamps[-1] - timestamps[0]) / 60
total_hrs = total_min / 60
print(f"Total repos processed : {repo_counts[-1]:,}")
print(f"Total run time        : {total_hrs:.1f} hours ({total_min:.0f} minutes)")
print(f"Average throughput    : {avg_rpm:.2f} repos/min  ({avg_rpm*60:.0f} repos/hr)")
print(f"History snapshots     : {len(history)}")

# when did Q1 top-3 stabilise? (rank unchanged for last 20% of run)
threshold_repos = int(repo_counts[-1] * 0.80)
stable_at = {}
for lang in ["HTML","Python","JavaScript","TypeScript"]:
    ranks = [(h["processed_unique_repositories"], get_rank(h,"q1_top_languages_by_projects",lang))
             for h in history]
    late  = [r for repos,r in ranks if repos >= threshold_repos]
    if len(set(late)) == 1:
        # find when it first reached that final rank and stayed
        final_rank = late[0]
        for repos,r in ranks:
            if r == final_rank:
                stable_at[lang] = repos
                break
print(f"\nQ1 rank stabilised by (approx):")
for lang, at in stable_at.items():
    print(f"  {lang:<16}: ~{at:,} repos ({at/repo_counts[-1]*100:.0f}% of total)")

print(f"\nQ3 TDD rate  (Python): {q3[0]['count']}/{repo_counts[-1]:,} = {q3[0]['count']/repo_counts[-1]*100:.2f}%")
print(f"Q4 TDD+CI rate (Py) : {q4[0]['count']}/{repo_counts[-1]:,} = {q4[0]['count']/repo_counts[-1]*100:.2f}%")
print(f"Q4/Q3 ratio (Python): {q4[0]['count']/q3[0]['count']*100:.1f}% of TDD Python repos also use CI")
print(f"\nAll figures saved to: {OUT}/")
print("="*60)
