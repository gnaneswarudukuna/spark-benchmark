"""
Per-Host Traffic Profiling using DataFrame API.
Author: Gnaneswarudu Kuna

Same analysis as RDD but using Catalyst optimizer.
Catalyst automatically optimizes the execution plan
making it faster than manual RDD code.
"""
import time
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from src.utils.spark_session import get_spark_session

def build_query(spark, parquet_path: str):
    """Run per-host profiling using DataFrame API."""

    df = spark.read.parquet(parquet_path)

    # Single aggregation pass — Catalyst optimizer handles everything
    result = df.groupBy("client_ip").agg(
        F.count("*").alias("total_requests"),
        F.sum("response_bytes").alias("total_bytes"),
        F.round(F.avg("response_bytes"), 2).alias("avg_bytes"),
        F.round(
            F.sum(F.when(F.col("status_code") >= 400, 1).otherwise(0))
            / F.count("*") * 100, 2
        ).alias("error_rate"),
        F.countDistinct("request_path").alias("distinct_endpoints")
    )

    return result

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    spark = get_spark_session("perhost-dataframe")

    print("\nRunning Per-Host Profiling — DataFrame API...")
    start = time.time()
    result = build_query(spark, parquet_path)
    rows = result.collect()
    elapsed = time.time() - start

    print(f"Completed in {elapsed:.3f} seconds")
    print(f"Total unique hosts: {len(rows)}")

    print("\nTop 10 hosts by total requests:")
    for r in sorted(rows, key=lambda x: -x['total_requests'])[:10]:
        print(f"  {r['client_ip']}: requests={r['total_requests']:,}, bytes={r['total_bytes']:,}")

    print("\nTop 10 hosts by error rate:")
    for r in sorted(rows, key=lambda x: -x['error_rate'])[:10]:
        print(f"  {r['client_ip']}: error_rate={r['error_rate']:.2f}%")

    print("\nTop 10 hosts by avg bytes:")
    for r in sorted(rows, key=lambda x: -x['avg_bytes'])[:10]:
        print(f"  {r['client_ip']}: avg_bytes={r['avg_bytes']:,.2f}")

    spark.stop()
