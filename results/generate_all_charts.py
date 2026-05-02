"""
Generate all benchmark charts from scaling results.
Author: Gnaneswarudu Kuna
"""
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os

# Load scaling results
with open("results/scaling_results.json") as f:
    data = json.load(f)

scales = [5, 25, 50, 100]
os.makedirs("results/charts", exist_ok=True)

colors = {
    "RDD": "#e74c3c",
    "DataFrame": "#2ecc71",
    "SQL": "#3498db"
}

# ============================================================
# CHART 1 — Wall Clock Scaling
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("Wall Clock Scaling by Query and API\nGnaneswarudu Kuna — COMPSCI 532",
             fontsize=13, fontweight='bold')

queries = ["perhost_profiling", "sessionization"]
titles = ["Per-Host Traffic Profiling", "Sessionization"]

for idx, (query, title) in enumerate(zip(queries, titles)):
    ax = axes[idx]
    apis_in_query = set()
    for scale_key in data:
        for r in data[scale_key]["queries"][query]:
            apis_in_query.add(r["api"])

    for api in sorted(apis_in_query):
        times = []
        for scale in scales:
            scale_key = f"pct_{scale}"
            query_data = data[scale_key]["queries"][query]
            for r in query_data:
                if r["api"] == api:
                    times.append(r["elapsed_sec"])
        ax.plot(scales, times, marker='o', label=api,
                color=colors[api], linewidth=2, markersize=6)

    ax.set_title(title, fontsize=11, fontweight='bold')
    ax.set_xlabel("Data Scale (%)", fontsize=10)
    ax.set_ylabel("Elapsed Time (seconds)", fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig("results/charts/wall_clock_scaling.png", dpi=150, bbox_inches='tight')
print("Saved: wall_clock_scaling.png")
plt.close()

# ============================================================
# CHART 2 — Shuffle Read/Write Volume at 100%
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("Shuffle Read/Write Volume by Query and API (100% scale)\nGnaneswarudu Kuna — COMPSCI 532",
             fontsize=13, fontweight='bold')

for idx, (query, title) in enumerate(zip(queries, titles)):
    ax = axes[idx]
    query_data = data["pct_100"]["queries"][query]
    apis = [r["api"] for r in query_data]
    reads = [r["shuffle_read_mb"] for r in query_data]
    writes = [r["shuffle_write_mb"] for r in query_data]

    x = np.arange(len(apis))
    width = 0.35

    bars1 = ax.bar(x - width/2, reads, width, label='Shuffle Read',
                   color=[colors[a] for a in apis], alpha=0.8, edgecolor='black')
    bars2 = ax.bar(x + width/2, writes, width, label='Shuffle Write',
                   color=[colors[a] for a in apis], alpha=0.4, edgecolor='black')

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f"{bar.get_height():.0f}MB", ha='center', va='bottom',
                fontsize=8, fontweight='bold')

    ax.set_title(title, fontsize=11, fontweight='bold')
    ax.set_xlabel("API", fontsize=10)
    ax.set_ylabel("Shuffle Volume (MB)", fontsize=10)
    ax.set_xticks(x)
    ax.set_xticklabels(apis)
    ax.legend(fontsize=9)
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig("results/charts/shuffle_volume.png", dpi=150, bbox_inches='tight')
print("Saved: shuffle_volume.png")
plt.close()

# ============================================================
# CHART 3 — Stages and Tasks at 100%
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("Number of Stages and Tasks by Query and API (100% scale)\nGnaneswarudu Kuna — COMPSCI 532",
             fontsize=13, fontweight='bold')

for idx, (query, title) in enumerate(zip(queries, titles)):
    ax = axes[idx]
    query_data = data["pct_100"]["queries"][query]
    apis = [r["api"] for r in query_data]
    stages = [r["num_stages"] for r in query_data]
    tasks = [r["num_tasks"] for r in query_data]

    x = np.arange(len(apis))
    width = 0.35

    ax2 = ax.twinx()
    bars1 = ax.bar(x - width/2, stages, width, label='Stages',
                   color=[colors[a] for a in apis], alpha=0.9, edgecolor='black')
    bars2 = ax2.bar(x + width/2, tasks, width, label='Tasks',
                    color=[colors[a] for a in apis], alpha=0.4, edgecolor='black')

    for bar, val in zip(bars1, stages):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                str(val), ha='center', va='bottom', fontsize=9, fontweight='bold')

    for bar, val in zip(bars2, tasks):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                 str(val), ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.set_title(title, fontsize=11, fontweight='bold')
    ax.set_xlabel("API", fontsize=10)
    ax.set_ylabel("Number of Stages", fontsize=10, color='black')
    ax2.set_ylabel("Number of Tasks", fontsize=10, color='gray')
    ax.set_xticks(x)
    ax.set_xticklabels(apis)
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)

lines1, labels1 = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax.legend(lines1 + lines2, labels1 + labels2, fontsize=9)

plt.tight_layout()
plt.savefig("results/charts/stages_and_tasks.png", dpi=150, bbox_inches='tight')
print("Saved: stages_and_tasks.png")
plt.close()

# ============================================================
# CHART 4 — Shuffle Scaling
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("Shuffle Volume Scaling by Query and API\nGnaneswarudu Kuna — COMPSCI 532",
             fontsize=13, fontweight='bold')

for idx, (query, title) in enumerate(zip(queries, titles)):
    ax = axes[idx]
    apis_in_query = set()
    for scale_key in data:
        for r in data[scale_key]["queries"][query]:
            apis_in_query.add(r["api"])

    for api in sorted(apis_in_query):
        shuffle_reads = []
        for scale in scales:
            scale_key = f"pct_{scale}"
            query_data = data[scale_key]["queries"][query]
            for r in query_data:
                if r["api"] == api:
                    shuffle_reads.append(r["shuffle_read_mb"])
        ax.plot(scales, shuffle_reads, marker='s', label=api,
                color=colors[api], linewidth=2, markersize=6)

    ax.set_title(title, fontsize=11, fontweight='bold')
    ax.set_xlabel("Data Scale (%)", fontsize=10)
    ax.set_ylabel("Shuffle Read (MB)", fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig("results/charts/shuffle_scaling.png", dpi=150, bbox_inches='tight')
print("Saved: shuffle_scaling.png")
plt.close()

print("\nAll charts saved to results/charts/")
