"""
Sessionization using DataFrame API.
Author: Gnaneswarudu Kuna

Uses Spark Window functions instead of Python loops.
LAG() detects time gaps between consecutive requests.
Cumulative SUM() assigns unique session IDs.
Catalyst optimizer handles execution plan automatically.
"""
import time
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql import Window
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from src.utils.spark_session import get_spark_session

SESSION_TIMEOUT = 1800

def build_query(spark, parquet_path: str):
    df = spark.read.parquet(parquet_path).select("client_ip", "log_ts")

    w_ordered = Window.partitionBy("client_ip").orderBy("log_ts")
    w_cumulative = Window.partitionBy("client_ip").orderBy("log_ts")\
                         .rowsBetween(Window.unboundedPreceding, 0)

    df = df.withColumn("prev_ts", F.lag("log_ts").over(w_ordered))
    df = df.withColumn("gap_secs",
        F.when(F.col("prev_ts").isNull(), 0)
         .otherwise(F.unix_timestamp("log_ts") - F.unix_timestamp("prev_ts"))
    )
    df = df.withColumn("is_new_session",
        F.when(
            F.col("prev_ts").isNull() | (F.col("gap_secs") > SESSION_TIMEOUT), 1
        ).otherwise(0)
    )
    df = df.withColumn("session_id",
        F.sum("is_new_session").over(w_cumulative)
    )

    sessions = df.groupBy("client_ip", "session_id").agg(
        F.count("*").alias("request_count"),
        (F.unix_timestamp(F.max("log_ts")) -
         F.unix_timestamp(F.min("log_ts"))).alias("duration_secs")
    ).cache()

    per_ip = sessions.groupBy("client_ip").agg(
        F.count("session_id").alias("session_count"),
        F.round(F.avg("duration_secs"), 2).alias("avg_duration_secs"),
        F.round(F.avg("request_count"), 2).alias("avg_requests_per_session")
    )

    return sessions, per_ip

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    spark = get_spark_session("sessionization-dataframe")

    print("\nRunning Sessionization — DataFrame API...")
    start = time.time()
    sessions, per_ip = build_query(spark, parquet_path)
    rows = per_ip.collect()
    elapsed = time.time() - start

    print(f"Completed in {elapsed:.3f} seconds")
    print(f"Total unique hosts: {len(rows)}")

    print("\nTop 10 hosts by session count:")
    for r in sorted(rows, key=lambda x: -x['session_count'])[:10]:
        print(f"  {r['client_ip']}: {r['session_count']} sessions, avg_duration={r['avg_duration_secs']:.1f}s")

    print("\nTop 10 hosts by avg duration:")
    for r in sorted(rows, key=lambda x: -x['avg_duration_secs'])[:10]:
        print(f"  {r['client_ip']}: avg_duration={r['avg_duration_secs']:.1f}s")

    spark.stop()
