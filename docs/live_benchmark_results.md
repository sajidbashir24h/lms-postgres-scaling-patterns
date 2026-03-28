## Live Benchmark Results

![LMS query latency comparison](docs/latency_comparison.png)

| Benchmark | Naive (ms) | Optimized (ms) | Improvement |
|---|---:|---:|---:|
| Benchmark A - 12M Lead Aggregation | 731.001 | 485.330 | 33.61% |
| Benchmark B - Bulk Ingestion (5k rows) | N/A | 230.877 | N/A |
| Benchmark C - Analytics Read Path | 1348.441 | 16.319 | 98.79% |
| Benchmark C - MV Concurrent Refresh | N/A | 1.278 | N/A |
| Benchmark D - Recursive Streaks | N/A | 7875.846 | N/A |

Moving from repeated monthly query dispatch in the N+1 aggregation loop to a single cursor-backed grouped execution shifted planning from repeated index-access cycles toward one consolidated aggregate path, reducing transfer and compute latency by 33.61%.

For analytics, replacing repeated live window aggregation over daily_activity_logs with reads from a precomputed materialized surface eliminated heavy per-request aggregation work and reduced read latency by 98.79% while preserving concurrent refresh semantics.
