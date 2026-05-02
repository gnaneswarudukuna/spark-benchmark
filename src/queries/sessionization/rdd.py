"""
Sessionization using RDD API.
Author: Gnaneswarudu Kuna

Groups each user's requests into sessions.
A new session begins when gap between consecutive
requests exceeds 30 minutes (1800 seconds).

Computes per user:
1. Total number of sessions
2. Average session duration in seconds
3. Average requests per session
"""
import time
import os
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from src.utils.spark_session import get_spark_session

SESSION_TIMEOUT = 1800

def assign_sessions(timestamps: list, timeout: int) -> list:
    """Given sorted timestamps assign session boundaries."""
    sorted_ts = sorted(timestamps)
    sessions = []
    session_start = sorted_ts[0]
    session_count = 1
    prev = sorted_ts[0]

    for ts in sorted_ts[1:]:
        gap = (ts - prev).total_seconds()
        if gap > timeout:
            sessions.append({
                "request_count": session_count,
                "duration_secs": (prev - session_start).total_seconds()
            })
            session_start = ts
            session_count = 1
        else:
            session_count += 1
        prev = ts

    sessions.append({
        "request_count": session_count,
        "duration_secs": (prev - session_start).total_seconds()
    })
    return sessions

def build_query(spark, parquet_path: str):
    """Run sessionization using RDD API."""

    rdd = spark.read.parquet(parquet_path)\
               .select("client_ip", "log_ts").rdd

    pairs = rdd.map(lambda row: (row.client_ip, row.log_ts))

    sessions_rdd = (
        pairs
        .groupByKey()
        .mapValues(lambda ts: assign_sessions(list(ts), SESSION_TIMEOUT))
        .cache()
    )

    per_ip = sessions_rdd.map(lambda kv: (
        kv[0],
        len(kv[1]),
        sum(s["duration_secs"] for s in kv[1]) / len(kv[1]),
        sum(s["request_count"] for s in kv[1]) / len(kv[1])
    ))

    return per_ip

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    spark = get_spark_session("sessionization-rdd")

    print("\nRunning Sessionization — RDD API...")
    start = time.time()
    result = build_query(spark, parquet_path)
    rows = result.collect()
    elapsed = time.time() - start

    print(f"Completed in {elapsed:.3f} seconds")
    print(f"Total unique hosts: {len(rows)}")

    print("\nTop 10 hosts by session count:")
    for r in sorted(rows, key=lambda x: -x[1])[:10]:
        print(f"  {r[0]}: {r[1]} sessions, avg_duration={r[2]:.1f}s")

    print("\nTop 10 hosts by avg session duration:")
    for r in sorted(rows, key=lambda x: -x[2])[:10]:
        print(f"  {r[0]}: avg_duration={r[2]:.1f}s, sessions={r[1]}")

    spark.stop()
