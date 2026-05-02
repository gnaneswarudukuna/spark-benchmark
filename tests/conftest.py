"""
Test configuration and fixtures.
Author: Gnaneswarudu Kuna
"""
import pytest
import os
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.utils.spark_session import get_spark_session

load_dotenv()

@pytest.fixture(scope="session")
def spark():
    """Create a shared Spark session for all tests."""
    spark = get_spark_session("test-session")
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def parquet_path():
    """Return path to processed parquet files."""
    return os.getenv("PROCESSED_PATH", "data/processed/access_logs")
