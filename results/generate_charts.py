"""
Generate comparison charts for benchmark results.
Author: Gnaneswarudu Kuna
"""
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

# Load results
with open("results/wall_clock.json") as f:
    data = json.load(f)

# Colors for each API
colors = {
    "RDD": "#e74c3c",        # Red
    "DataFrame": "#2ecc71",  # Green
    "SQL": "#3498db"         # Blue
}

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle(
    "PySpark API Performance Comparison\nGnaneswarudu Kuna — COMPSCI 532",
    fontsize=14, fontweight='bold'
)

for idx, (query_name, ax) in enumerate(zip(data.keys(), axes)):
    query_data = data[query_name]
    apis = [d["api"] for d in query_data]
    times = [d["elapsed_sec"] for d in query_data]
    bar_colors = [colors[api] for api in apis]

    bars = ax.bar(apis, times, color=bar_colors, width=0.5, edgecolor='black')

    # Add time labels on top of bars
    for bar, time in zip(bars, times):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            f"{time:.1f}s",
            ha='center', va='bottom', fontweight='bold', fontsize=11
        )

    # Add speedup annotations
    rdd_time = times[0]
    for i, (bar, time) in enumerate(zip(bars[1:], times[1:]), 1):
        speedup = rdd_time / time
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() / 2,
            f"{speedup:.1f}x\nfaster",
            ha='center', va='center',
            color='white', fontweight='bold', fontsize=9
        )

    title = query_name.replace("_", " ").title()
    ax.set_title(f"{title}", fontsize=12, fontweight='bold')
    ax.set_ylabel("Execution Time (seconds)", fontsize=10)
    ax.set_xlabel("API", fontsize=10)
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

# Legend
legend_patches = [
    mpatches.Patch(color=colors["RDD"], label="RDD — Manual"),
    mpatches.Patch(color=colors["DataFrame"], label="DataFrame — Catalyst"),
    mpatches.Patch(color=colors["SQL"], label="SQL — Catalyst"),
]
fig.legend(handles=legend_patches, loc='lower center',
           ncol=3, fontsize=10, bbox_to_anchor=(0.5, -0.02))

plt.tight_layout()
os.makedirs("results/charts", exist_ok=True)
plt.savefig("results/charts/benchmark_comparison.png",
            dpi=150, bbox_inches='tight')
print("Chart saved to results/charts/benchmark_comparison.png")
plt.show()
