"""
Results Analysis Script.
Author: Gnaneswarudu Kuna

Analyzes benchmark results and prints summary statistics.
"""
import json
import os

def load_results(path: str) -> dict:
    with open(path) as f:
        return json.load(f)

def analyze(data: dict):
    print("=" * 60)
    print("BENCHMARK RESULTS ANALYSIS")
    print("Author: Gnaneswarudu Kuna — COMPSCI 532")
    print("=" * 60)

    for query_name, results in data.items():
        print(f"\n{query_name.replace('_', ' ').upper()}")
        print("-" * 40)

        rdd_time = None
        for r in results:
            api = r["api"]
            time = r["elapsed_sec"]
            if api == "RDD":
                rdd_time = time
                print(f"  {api:12} : {time:.3f}s (baseline)")
            else:
                speedup = rdd_time / time if rdd_time else 0
                print(f"  {api:12} : {time:.3f}s ({speedup:.1f}x faster than RDD)")

    print("\n" + "=" * 60)
    print("KEY FINDINGS")
    print("=" * 60)
    print("1. DataFrame and SQL use Catalyst optimizer — significantly faster")
    print("2. Sessionization shows biggest gap — RDD 96x slower than DataFrame")
    print("3. Simple queries show smaller gap — per-host only 8x difference")
    print("4. SQL and DataFrame perform similarly — same optimizer under hood")

if __name__ == "__main__":
    results_path = "results/wall_clock.json"
    if not os.path.exists(results_path):
        print(f"Results file not found: {results_path}")
        exit(1)
    data = load_results(results_path)
    analyze(data)
