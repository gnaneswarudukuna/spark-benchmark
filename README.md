# A Comparative Analysis of PySpark's RDD, DataFrame, and SQL APIs on E-Commerce Web Server Logs

**Author:** Gnaneswarudu Kuna  
**Course:** COMPSCI 532 — Systems for Data Science  
**University:** UMass Amherst

## Project Overview

This project compares three PySpark APIs — RDD, DataFrame and SQL — by implementing the same data analysis queries in all three ways and measuring performance differences on real world data containing 10 million HTTP requests.

**Core Question:** How much does Spark's Catalyst optimizer improve performance over manual RDD code?

## Dataset

- **Name:** Zanbil.ir E-Commerce Web Server Access Logs
- **Size:** 3.3 GB uncompressed
- **Records:** 10,365,152 HTTP requests
- **Source:** Kaggle
- **Format:** Apache Combined Log Format

## The 3 APIs Compared

| API | Description | Optimization |
|-----|-------------|-------------|
| RDD | Low level manual code | None — fully manual |
| DataFrame | Structured table operations | Catalyst optimizer |
| SQL | Plain SQL queries | Catalyst optimizer |

## Queries Implemented

### 1. Per-Host Traffic Profiling
For each unique IP address computes total requests, total bytes, average bytes, error rate and distinct endpoints visited.
Implemented in RDD, DataFrame and SQL.

### 2. Sessionization
Groups each user's requests into sessions. A new session starts when inactivity exceeds 30 minutes.
Computes session count, average duration and average requests per session.
Implemented in RDD, DataFrame and SQL.

## Benchmark Results — 10 Million Rows

### Per-Host Traffic Profiling
| API | Time | Speedup vs RDD |
|-----|------|---------------|
| RDD | 92.916s | baseline |
| DataFrame | 10.605s | 8.7x faster |
| SQL | 11.157s | 8.3x faster |

### Sessionization
| API | Time | Speedup vs RDD |
|-----|------|---------------|
| RDD | 597.373s | baseline |
| DataFrame | 6.216s | 96x faster |
| SQL | 6.651s | 90x faster |

## Key Findings

1. DataFrame and SQL are up to 96x faster than RDD for complex queries
2. The Catalyst optimizer is the reason — it automatically optimizes execution plans
3. RDD is more competitive for simple queries — only 8x slower for per-host profiling
4. SQL and DataFrame perform similarly — both use the same Catalyst optimizer

## Setup Instructions

### Prerequisites
- Python 3.12+
- Java 21
- 4GB RAM minimum

### Installation
```bash
git clone https://github.com/gnaneswarudukuna/spark-benchmark.git
cd spark-benchmark
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run Log Parsing
```bash
python3 -m src.parsing.log_parser
```

### Run Queries
```bash
python3 -m src.queries.perhost_profiling.rdd
python3 -m src.queries.perhost_profiling.dataframe
python3 -m src.queries.perhost_profiling.sql
python3 -m src.queries.sessionization.rdd
python3 -m src.queries.sessionization.dataframe
python3 -m src.queries.sessionization.sql
```

### Run Full Benchmark
```bash
python3 -m benchmark.run_benchmark
```

## Technologies
- Python 3.13
- PySpark 3.5.3
- Java 21
- Git and GitHub
- Matplotlib
- Pandas
## Test Cases

- We have unit tests using pytest to ensure that each API produces consistent output
- Tests verify that DataFrame and SQL sessionization return the same number of unique hosts
- Tests verify all required output columns exist for each query
- Tests verify all hosts have at least 1 session

To run the tests locally (Parquet must already exist):

```bash
pytest tests/ -v
```