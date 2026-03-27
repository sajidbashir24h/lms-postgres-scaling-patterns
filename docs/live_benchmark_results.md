## Live Benchmark Results

![LMS query latency comparison](docs/latency_comparison.png)

| Benchmark | Naive (ms) | Optimized (ms) | Improvement |
|---|---:|---:|---:|
| Benchmark A - 12M Lead Aggregation | 484.608 | 214.233 | 55.79% |
| Benchmark B - Bulk Ingestion (5k rows) | N/A | 125.151 | N/A |
| Benchmark C - Analytics Read Path | 810.950 | 10.707 | 98.68% |
| Benchmark C - MV Concurrent Refresh | N/A | 1.931 | N/A |
| Benchmark D - Recursive Streaks | N/A | 4837.524 | N/A |

Moving from repeated monthly query dispatch in the N+1 aggregation loop to a single cursor-backed grouped execution shifted planning from repeated index-access cycles toward one consolidated aggregate path, reducing transfer and compute latency by 55.79%.

For analytics, replacing repeated live window aggregation over daily_activity_logs with reads from a precomputed materialized surface eliminated heavy per-request aggregation work and reduced read latency by 98.68% while preserving concurrent refresh semantics.
