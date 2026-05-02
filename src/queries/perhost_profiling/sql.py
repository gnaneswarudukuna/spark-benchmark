"""
Per-Host Traffic Profiling using SQL API.
Author: Gnaneswarudu Kuna

Same analysis as RDD and DataFrame but using plain SQL.
SQL uses the same Catalyst optimizer as DataFrame
but written in familiar database query syntax.
"""
import time
import os
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from src.utils.spark_session import get_spark_session

def build_query(spark, parquet_path: str):
    """Run per-host profiling using SQL API."""

    df = spark.read.parquet(parquet_path)
    df.createOrReplaceTempView("web_logs")

    result = spark.sql("""
        SELECT
            client_ip,
            COUNT(*) AS total_requests,
            SUM(response_bytes) AS total_bytes,
            ROUND(AVG(response_bytes), 2) AS avg_bytes,
            ROUND(
                SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END)
                / COUNT(*) * 100, 2
            ) AS error_rate,
            COUNT(DISTINCT request_path) AS distinct_endpoints
        FROM web_logs
        GROUP BY client_ip
        ORDER BY total_requests DESC
    """)

    return result

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    spark = get_spark_session("perhost-sql")

    print("\nRunning Per-Host Profiling — SQL API...")
    start = time.time()
    result = build_query(spark, parquet_path)
    rows = result.collect()
    elapsed = time.time() - start

    print(f"Completed in {elapsed:.3f} seconds")
    print(f"Total unique hosts: {len(rows)}")

    print("\nTop 10 hosts by total requests:")
    for r in rows[:10]:
        print(f"  {r['client_ip']}: requests={r['total_requests']:,}, bytes={r['total_bytes']:,}")

    print("\nTop 10 hosts by error rate:")
    for r in sorted(rows, key=lambda x: -x['error_rate'])[:10]:
        print(f"  {r['client_ip']}: error_rate={r['error_rate']:.2f}%")

    print("\nTop 10 hosts by avg bytes:")
    for r in sorted(rows, key=lambda x: -x['avg_bytes'])[:10]:
        print(f"  {r['client_ip']}: avg_bytes={r['avg_bytes']:,.2f}")

    spark.catalog.dropTempView("web_logs")
    spark.stop()
