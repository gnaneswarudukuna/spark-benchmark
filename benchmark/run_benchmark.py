"""
Benchmark Runner — measures wall clock time
for all queries across all 3 APIs.
Author: Gnaneswarudu Kuna
"""
import time
import json
import os
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.utils.spark_session import get_spark_session
from src.queries.perhost_profiling import rdd as perhost_rdd
from src.queries.perhost_profiling import dataframe as perhost_df
from src.queries.perhost_profiling import sql as perhost_sql
from src.queries.sessionization import rdd as session_rdd
from src.queries.sessionization import dataframe as session_df
from src.queries.sessionization import sql as session_sql

def run_benchmark(parquet_path: str, output_path: str):
    load_dotenv()
    spark = get_spark_session("benchmark")
    results = {}

    print("\n" + "="*50)
    print("SPARK BENCHMARK — Gnaneswarudu Kuna")
    print("="*50)

    # Per-Host Profiling
    print("\n--- Per-Host Traffic Profiling ---")
    results["perhost_profiling"] = []
    for api_name, query_fn in [
        ("RDD", lambda: perhost_rdd.build_query(spark, parquet_path).collect()),
        ("DataFrame", lambda: perhost_df.build_query(spark, parquet_path).collect()),
        ("SQL", lambda: perhost_sql.build_query(spark, parquet_path).collect()),
    ]:
        print(f"Running {api_name}...")
        start = time.time()
        query_fn()
        elapsed = round(time.time() - start, 3)
        print(f"  Completed in {elapsed}s")
        results["perhost_profiling"].append({
            "api": api_name,
            "elapsed_sec": elapsed
        })

    # Sessionization
    print("\n--- Sessionization ---")
    results["sessionization"] = []
    for api_name, query_fn in [
        ("RDD", lambda: session_rdd.build_query(spark, parquet_path).collect()),
        ("DataFrame", lambda: session_df.build_query(spark, parquet_path)[1].collect()),
        ("SQL", lambda: session_sql.build_query(spark, parquet_path)[1].collect()),
    ]:
        print(f"Running {api_name}...")
        start = time.time()
        query_fn()
        elapsed = round(time.time() - start, 3)
        print(f"  Completed in {elapsed}s")
        results["sessionization"].append({
            "api": api_name,
            "elapsed_sec": elapsed
        })

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)

    print("\n" + "="*50)
    print("RESULTS SUMMARY")
    print("="*50)
    print(json.dumps(results, indent=2))
    print(f"\nSaved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    run_benchmark(parquet_path, "results/wall_clock.json")
