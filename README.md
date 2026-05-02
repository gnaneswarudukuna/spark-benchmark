# A Comparative Analysis of PySpark's RDD, DataFrame, and SQL APIs on E-Commerce Web Server Logs

**Author:** Gnaneswarudu Kuna  
**Course:** COMPSCI 532 — Systems for Data Science  
**University:** UMass Amherst

## Project Description and Relevance

Modern data engineering teams routinely process logs, clickstreams, 
and event records at scale using distributed batch-processing 
frameworks. Understanding how different programming abstractions 
within these frameworks affect performance is a critical systems 
concern. This project investigates that question empirically using 
Apache Spark and a real-world e-commerce web server log dataset.

Specifically, I benchmark three levels of abstraction available in 
PySpark — the low-level RDD API, the structured DataFrame API, and 
Spark SQL — by implementing the same analytical queries at each level 
and measuring key systems performance metrics. The dataset used is the 
[Zanbil.ir E-Commerce Web Server Access Logs](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs) [1] 
— approximately 10.3 million HTTP requests totaling ~3.3 GB 
uncompressed, recorded from a real Iranian e-commerce platform. It 
uses the Apache Combined Log Format, containing fields for client IP, 
timestamp, HTTP method, request path, status code, and response bytes. 
Two additional fields (Referer and User-Agent) are present but ignored 
during parsing.

The analytical pipeline consists of two core queries:

1. **Per-Host Traffic Profiling** — aggregating request counts, byte 
volumes, error rates, and distinct endpoint counts per unique client IP
2. **Sessionization** — grouping each client's requests into sessions 
defined by a 30-minute inactivity threshold, then computing session-level 
metrics such as duration and request count per session

Each query is implemented independently using all three APIs — RDD, 
DataFrame, and SQL — producing identical outputs to ensure a fair 
comparison. The central systems question is: **how much does Spark's 
Catalyst query optimizer improve performance over hand-tuned RDD code, 
and under what conditions?**

The RDD API requires the programmer to manually manage every 
transformation — including partitioning strategy, shuffle operations, 
and aggregation logic. In contrast, the DataFrame and SQL interfaces 
delegate these decisions to Spark's Catalyst optimizer, which 
automatically rewrites query plans, pushes filters early, and selects 
efficient join and aggregation strategies. By holding the workload 
constant across all three implementations, we can isolate the effect 
of the optimizer on real execution behavior.

This work connects directly to the course's coverage of the Spark 
execution model (Lecture 8), the original RDD paper by Zaharia et 
al. [2], and the broader theme of evaluating system design tradeoffs 
between programmer control and automatic optimization.

To characterize performance, four systems-level metrics are collected 
for each query and API combination:
- **Wall-clock execution time** — end-to-end latency measured using Python timers
- **Shuffle read/write volume** — data moved across the network during wide transformations
- **Number of stages and tasks** — complexity of the physical execution plan
- **Peak memory usage** — maximum JVM heap consumed during execution
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