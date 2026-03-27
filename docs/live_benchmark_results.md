## Live Benchmark Results

![LMS query latency comparison](docs/latency_comparison.png)

| Benchmark | Naive (ms) | Optimized (ms) | Improvement |
|---|---:|---:|---:|
| Benchmark A - 12M Lead Aggregation | 506.246 | 248.765 | 50.86% |
| Benchmark B - Bulk Ingestion (5k rows) | N/A | 143.157 | N/A |
| Benchmark C - Analytics Read Path | 1021.292 | 17.127 | 98.32% |
| Benchmark C - MV Concurrent Refresh | N/A | 1.127 | N/A |
| Benchmark D - Recursive Streaks | N/A | 5180.367 | N/A |

Moving from repeated monthly query dispatch in the N+1 aggregation loop to a single cursor-backed grouped execution shifted planning from repeated index-access cycles toward one consolidated aggregate path, reducing transfer and compute latency by 50.86%.

For analytics, replacing repeated live window aggregation over daily_activity_logs with reads from a precomputed materialized surface eliminated heavy per-request aggregation work and reduced read latency by 98.32% while preserving concurrent refresh semantics.
