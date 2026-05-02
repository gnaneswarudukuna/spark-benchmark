"""
Sessionization using SQL API.
Author: Gnaneswarudu Kuna

This is my main contribution to the project.
Uses SQL Window functions to detect session boundaries:
- LAG() looks at previous request timestamp per user
- Cumulative SUM() assigns unique session IDs
- 30 minute inactivity gap = new session starts

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

def build_query(spark, parquet_path: str, timeout: int = SESSION_TIMEOUT):
    """Run sessionization using SQL API."""

    df = spark.read.parquet(parquet_path).select("client_ip", "log_ts")
    df.createOrReplaceTempView("web_logs")

    # Step 1 — detect new session flag using LAG window function
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW sessions_flagged AS
        SELECT
            client_ip,
            log_ts,
            CASE
                WHEN LAG(log_ts) OVER (
                    PARTITION BY client_ip ORDER BY log_ts
                ) IS NULL THEN 1
                WHEN (
                    UNIX_TIMESTAMP(log_ts) -
                    UNIX_TIMESTAMP(LAG(log_ts) OVER (
                        PARTITION BY client_ip ORDER BY log_ts
                    ))
                ) > {timeout} THEN 1
                ELSE 0
            END AS is_new_session
        FROM web_logs
    """)

    # Step 2 — assign session IDs using cumulative sum
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW sessions_numbered AS
        SELECT
            client_ip,
            log_ts,
            SUM(is_new_session) OVER (
                PARTITION BY client_ip
                ORDER BY log_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS session_id
        FROM sessions_flagged
    """)

    # Step 3 — aggregate per session
    sessions = spark.sql("""
        SELECT
            client_ip,
            session_id,
            COUNT(*) AS request_count,
            UNIX_TIMESTAMP(MAX(log_ts)) -
            UNIX_TIMESTAMP(MIN(log_ts)) AS duration_secs
        FROM sessions_numbered
        GROUP BY client_ip, session_id
    """).cache()

    # Step 4 — aggregate per IP across all sessions
    per_ip = spark.sql("""
        SELECT
            client_ip,
            COUNT(session_id) AS session_count,
            ROUND(AVG(duration_secs), 2) AS avg_duration_secs,
            ROUND(AVG(request_count), 2) AS avg_requests_per_session
        FROM (
            SELECT
                client_ip,
                session_id,
                COUNT(*) AS request_count,
                UNIX_TIMESTAMP(MAX(log_ts)) -
                UNIX_TIMESTAMP(MIN(log_ts)) AS duration_secs
            FROM sessions_numbered
            GROUP BY client_ip, session_id
        )
        GROUP BY client_ip
        ORDER BY session_count DESC
    """)

    return sessions, per_ip

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    spark = get_spark_session("sessionization-sql")

    print("\nRunning Sessionization — SQL API...")
    start = time.time()
    sessions, per_ip = build_query(spark, parquet_path)
    rows = per_ip.collect()
    elapsed = time.time() - start

    print(f"Completed in {elapsed:.3f} seconds")
    print(f"Total unique hosts: {len(rows)}")

    print("\nTop 10 hosts by session count:")
    for r in rows[:10]:
        print(f"  {r['client_ip']}: {r['session_count']} sessions, avg_duration={r['avg_duration_secs']:.1f}s")

    print("\nTop 10 hosts by avg session duration:")
    for r in sorted(rows, key=lambda x: -x['avg_duration_secs'])[:10]:
        print(f"  {r['client_ip']}: avg_duration={r['avg_duration_secs']:.1f}s, sessions={r['session_count']}")

    # Cleanup temp views
    spark.catalog.dropTempView("web_logs")
    spark.catalog.dropTempView("sessions_flagged")
    spark.catalog.dropTempView("sessions_numbered")
    spark.stop()
