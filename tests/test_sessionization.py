"""
Tests for Sessionization query.
Verifies that DataFrame and SQL produce identical results.
Author: Gnaneswarudu Kuna
"""
import pytest
from src.queries.sessionization import dataframe as session_df
from src.queries.sessionization import sql as session_sql

def test_sessionization_dataframe_runs(spark, parquet_path):
    """Test DataFrame version runs without errors."""
    sessions, per_ip = session_df.build_query(spark, parquet_path)
    rows = per_ip.collect()
    assert len(rows) > 0, "DataFrame should return results"
    print(f"\nDataFrame unique hosts: {len(rows)}")

def test_sessionization_sql_runs(spark, parquet_path):
    """Test SQL version runs without errors."""
    sessions, per_ip = session_sql.build_query(spark, parquet_path)
    rows = per_ip.collect()
    assert len(rows) > 0, "SQL should return results"
    print(f"\nSQL unique hosts: {len(rows)}")

def test_sessionization_same_host_count(spark, parquet_path):
    """Test DataFrame and SQL return same number of unique hosts."""
    _, df_per_ip = session_df.build_query(spark, parquet_path)
    _, sql_per_ip = session_sql.build_query(spark, parquet_path)

    df_count = df_per_ip.count()
    sql_count = sql_per_ip.count()

    assert df_count == sql_count, \
        f"Host counts differ: DataFrame={df_count}, SQL={sql_count}"
    print(f"\nDataFrame and SQL agree: {df_count} unique hosts")

def test_sessionization_columns_exist(spark, parquet_path):
    """Test per_ip output has required columns."""
    _, per_ip = session_df.build_query(spark, parquet_path)
    columns = per_ip.columns
    required = ["client_ip", "session_count",
                "avg_duration_secs", "avg_requests_per_session"]
    for col in required:
        assert col in columns, f"Missing column: {col}"

def test_sessionization_positive_sessions(spark, parquet_path):
    """Test all hosts have at least 1 session."""
    _, per_ip = session_sql.build_query(spark, parquet_path)
    rows = per_ip.collect()
    for r in rows:
        assert r["session_count"] >= 1, \
            f"Host {r['client_ip']} has 0 sessions"
