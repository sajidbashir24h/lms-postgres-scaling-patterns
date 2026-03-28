## Live Benchmark Results

![LMS query latency comparison](docs/latency_comparison.png)

| Benchmark | Naive (ms) | Optimized (ms) | Improvement |
|---|---:|---:|---:|
| Benchmark A - 12M Lead Aggregation | 865.565 | 437.740 | 49.43% |
| Benchmark B - Bulk Ingestion (5k rows) | N/A | 249.247 | N/A |
| Benchmark C - Analytics Read Path | 1718.528 | 18.705 | 98.91% |
| Benchmark C - MV Concurrent Refresh | N/A | 1.336 | N/A |
| Benchmark D - Recursive Streaks | N/A | 8475.884 | N/A |
