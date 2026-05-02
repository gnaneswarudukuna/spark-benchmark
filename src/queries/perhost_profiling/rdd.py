"""
Per-Host Traffic Profiling using RDD API.
Author: Gnaneswarudu Kuna

For each unique IP address computes:
1. Total requests
2. Total bytes transferred
3. Average bytes per request
4. Error rate percentage (4xx and 5xx status codes)
5. Number of distinct endpoints visited
"""
import time
import os
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from src.utils.spark_session import get_spark_session

def build_query(spark, parquet_path: str):
    """Run per-host profiling using RDD API."""

    # Read parquet and select only needed columns
    rdd = spark.read.parquet(parquet_path).select(
        "client_ip", "request_path", "status_code", "response_bytes"
    ).rdd

    # Map each row to (client_ip, (requests, bytes, errors, endpoints))
    mapped = rdd.map(lambda row: (
        row.client_ip,
        (
            1,
            row.response_bytes if row.response_bytes else 0,
            1 if row.status_code >= 400 else 0,
            frozenset([row.request_path])
        )
    ))

    # Aggregate all metrics per IP using reduceByKey
    # reduceByKey is more efficient than groupByKey
    aggregated = mapped.reduceByKey(lambda a, b: (
        a[0] + b[0],        # total requests
        a[1] + b[1],        # total bytes
        a[2] + b[2],        # total errors
        a[3] | b[3]         # union of endpoints
    ))

    # Compute final derived metrics
    result = aggregated.map(lambda x: (
        x[0],                                                      # client_ip
        x[1][0],                                                   # total_requests
        x[1][1],                                                   # total_bytes
        round(x[1][1] / x[1][0], 2) if x[1][0] > 0 else 0,      # avg_bytes
        round(x[1][2] / x[1][0] * 100, 2) if x[1][0] > 0 else 0, # error_rate
        len(x[1][3])                                               # distinct_endpoints
    ))

    return result

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    spark = get_spark_session("perhost-rdd")

    print("\nRunning Per-Host Profiling — RDD API...")
    start = time.time()
    result = build_query(spark, parquet_path)
    rows = result.collect()
    elapsed = time.time() - start

    print(f"Completed in {elapsed:.3f} seconds")
    print(f"Total unique hosts: {len(rows)}")

    print("\nTop 10 hosts by total requests:")
    for r in sorted(rows, key=lambda x: -x[1])[:10]:
        print(f"  {r[0]}: requests={r[1]:,}, bytes={r[2]:,}")

    print("\nTop 10 hosts by error rate:")
    for r in sorted(rows, key=lambda x: -x[4])[:10]:
        print(f"  {r[0]}: error_rate={r[4]:.2f}%")

    print("\nTop 10 hosts by avg bytes per request:")
    for r in sorted(rows, key=lambda x: -x[3])[:10]:
        print(f"  {r[0]}: avg_bytes={r[3]:,.2f}")

    spark.stop()
