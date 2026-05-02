"""
Scaling Benchmark — runs all queries at different data scales
and collects wall clock timing results.
Author: Gnaneswarudu Kuna
"""
import time
import json
import os
import requests
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.utils.spark_session import get_spark_session
from src.queries.perhost_profiling import rdd as perhost_rdd
from src.queries.perhost_profiling import dataframe as perhost_df
from src.queries.perhost_profiling import sql as perhost_sql
from src.queries.sessionization import dataframe as session_df
from src.queries.sessionization import sql as session_sql

def get_stage_metrics(spark):
    """Fetch shuffle and stage metrics from Spark REST API."""
    try:
        app_id = spark.sparkContext.applicationId
        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        response = requests.get(url, timeout=5)
        stages = response.json()
        
        total_shuffle_read = sum(
            s.get("shuffleReadBytes", 0) for s in stages
        )
        total_shuffle_write = sum(
            s.get("shuffleWriteBytes", 0) for s in stages
        )
        total_stages = len(stages)
        total_tasks = sum(s.get("numTasks", 0) for s in stages)
        
        return {
            "shuffle_read_mb": round(total_shuffle_read / (1024*1024), 2),
            "shuffle_write_mb": round(total_shuffle_write / (1024*1024), 2),
            "num_stages": total_stages,
            "num_tasks": total_tasks
        }
    except Exception as e:
        print(f"Could not fetch stage metrics: {e}")
        return {
            "shuffle_read_mb": 0,
            "shuffle_write_mb": 0,
            "num_stages": 0,
            "num_tasks": 0
        }

def run_scaling_benchmark(base_parquet_path: str, output_path: str):
    """Run benchmark at multiple scales and collect all metrics."""
    load_dotenv()
    
    scales = [5, 25, 50, 100]
    all_results = {}

    for scale in scales:
        print(f"\n{'='*60}")
        print(f"Running benchmark at {scale}% scale")
        print(f"{'='*60}")

        # Sample the parquet data
        spark = get_spark_session(f"scaling-benchmark-{scale}pct")
        
        # Read and sample data
        df_full = spark.read.parquet(base_parquet_path)
        total_rows = df_full.count()
        
        if scale < 100:
            sample_df = df_full.sample(
                withReplacement=False,
                fraction=scale/100,
                seed=42
            )
        else:
            sample_df = df_full
        
        # Save sampled data as temp parquet
        sample_path = f"data/processed/access_logs_sample_{scale}pct"
        sample_df.write.mode("overwrite").parquet(sample_path)
        sample_count = sample_df.count()
        print(f"Using {sample_count:,} rows ({scale}% of {total_rows:,})")
        spark.stop()

        scale_results = {"scale_pct": scale, "row_count": sample_count, "queries": {}}

        queries = [
            ("perhost_profiling", [
                ("RDD", lambda s, p: perhost_rdd.build_query(s, p).collect()),
                ("DataFrame", lambda s, p: perhost_df.build_query(s, p).collect()),
                ("SQL", lambda s, p: perhost_sql.build_query(s, p).collect()),
            ]),
            ("sessionization", [
                ("DataFrame", lambda s, p: session_df.build_query(s, p)[1].collect()),
                ("SQL", lambda s, p: session_sql.build_query(s, p)[1].collect()),
            ]),
        ]

        for query_name, apis in queries:
            scale_results["queries"][query_name] = []
            
            for api_name, query_fn in apis:
                print(f"\nRunning {query_name} — {api_name}...")
                spark = get_spark_session(f"{query_name}-{api_name}-{scale}pct")
                
                start = time.time()
                query_fn(spark, sample_path)
                elapsed = round(time.time() - start, 3)
                
                metrics = get_stage_metrics(spark)
                spark.stop()

                result = {
                    "api": api_name,
                    "elapsed_sec": elapsed,
                    **metrics
                }
                scale_results["queries"][query_name].append(result)
                print(f"  Completed in {elapsed}s — shuffle: {metrics['shuffle_read_mb']}MB read, {metrics['shuffle_write_mb']}MB write")

        all_results[f"pct_{scale}"] = scale_results

    # Save results
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(all_results, f, indent=2)

    print(f"\nScaling results saved to {output_path}")
    return all_results

if __name__ == "__main__":
    load_dotenv()
    parquet_path = os.getenv("PROCESSED_PATH")
    run_scaling_benchmark(parquet_path, "results/scaling_results.json")
