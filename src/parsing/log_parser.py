"""
Log Parser — reads raw Apache Combined Log Format
and converts to clean Parquet files.
"""
import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.spark_session import get_spark_session

LOG_PATTERN = r'^(\S+) \S+ \S+ \[(.+?)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)'

def parse_logs(spark: SparkSession, raw_path: str, output_path: str):
    print(f"Reading raw logs from: {raw_path}")

    raw = spark.read.text(raw_path)
    total_lines = raw.count()
    print(f"Total lines: {total_lines:,}")

    parsed = raw.select(
        F.regexp_extract('value', LOG_PATTERN, 1).alias('client_ip'),
        F.regexp_extract('value', LOG_PATTERN, 2).alias('log_ts_raw'),
        F.regexp_extract('value', LOG_PATTERN, 3).alias('http_method'),
        F.regexp_extract('value', LOG_PATTERN, 4).alias('request_path'),
        F.regexp_extract('value', LOG_PATTERN, 5).alias('http_version'),
        F.regexp_extract('value', LOG_PATTERN, 6).cast('int').alias('status_code'),
        F.regexp_extract('value', LOG_PATTERN, 7).alias('response_bytes_raw'),
    )

    parsed = parsed.filter(F.col('client_ip') != '')

    cleaned = parsed.select(
        F.col('client_ip'),
        F.to_timestamp(
            F.col('log_ts_raw'), 'dd/MMM/yyyy:HH:mm:ss Z'
        ).alias('log_ts'),
        F.col('http_method'),
        F.col('request_path'),
        F.col('http_version'),
        F.col('status_code'),
        F.when(
            F.col('response_bytes_raw') == '-', 0
        ).otherwise(
            F.col('response_bytes_raw').cast('long')
        ).alias('response_bytes'),
    ).filter(F.col('log_ts').isNotNull())

    cleaned_count = cleaned.count()
    print(f"Cleaned rows: {cleaned_count:,}")
    print(f"Parse rate: {cleaned_count/total_lines:.2%}")

    print(f"Saving to: {output_path}")
    cleaned.repartition(16).write.mode('overwrite').parquet(output_path)
    print("Done! Parquet files saved.")

    cleaned.printSchema()
    cleaned.show(5, truncate=False)

if __name__ == "__main__":
    load_dotenv()
    raw_path = os.getenv("RAW_LOG_PATH")
    output_path = os.getenv("PROCESSED_PATH")
    spark = get_spark_session("log-parser")
    parse_logs(spark, raw_path, output_path)
    spark.stop()
