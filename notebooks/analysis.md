# Benchmark Analysis

## Per-Host Profiling Results
- RDD: 92.916s
- DataFrame: 10.605s (8.7x faster)
- SQL: 11.157s (8.3x faster)

## Sessionization Results
- RDD: 597.373s
- DataFrame: 6.216s (96x faster)
- SQL: 6.651s (90x faster)

## Key Finding
The Catalyst optimizer makes DataFrame and SQL
dramatically faster than RDD for complex queries
like sessionization involving window functions.
