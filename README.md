# LMS Data Gravity Post-Mortem and Sanitized Case Study

## NDA and Privacy Disclaimer
This repository contains synthetic code, schemas, and data designed to demonstrate architecture and database engineering patterns only. Table structures, trigger logic, function signatures, and sample values are intentionally anonymized and do not represent proprietary business rules, confidential infrastructure details, or real user data. Any resemblance to production identifiers, workflows, or metrics is coincidental.

## Executive Summary
The LMS began as a monolithic MVP where a single web process handled reads, writes, and business logic. As mobile clients were added, the platform moved to a decoupled API architecture (Node.js and Python services) and exposed the original bottlenecks.

The primary failure mode was data movement. The API layer repeatedly pulled large intermediate datasets from PostgreSQL, serialized them to application objects, then recomputed aggregates and sequential rules out-of-process. That design increased:

- Network round-trips and payload sizes.
- Serialization/deserialization CPU on API nodes.
- Memory pressure during list materialization.
- Lock contention windows due to long-lived transactions.

The architectural correction was to execute data-heavy logic inside PostgreSQL using set-based SQL, PL/pgSQL cursors, statement-level trigger validation, materialized views, and recursive CTEs.

## Failure Pattern: Data Gravity Was Ignored
When the data lives in PostgreSQL, moving raw rows to an API tier for aggregation is usually the wrong default. The asymptotic behavior is straightforward:

- API-side monthly aggregation with loops: often behaves like O(12 * N) in row transfer plus O(N) memory footprint in process.
- Database-side grouped aggregation: O(N) scan with in-engine hash/sort aggregation and minimal return cardinality.

The issue was not only algorithmic complexity. It was I/O placement. Pulling millions of rows over the network to compute a 12-row report is an avoidable transfer amplification problem.

## Challenge 1: The N+1 Aggregation Problem
### Symptom
A monthly lead report was computed by iterating month-by-month in the API. Each loop executed one query, fetched raw leads, and merged results in application memory. Under load, this caused N+1 query behavior and redundant scans.

### Why It Failed
- Excessive query count per request.
- Network overhead from repeatedly transferring overlapping date windows.
- API heap growth from buffering intermediate collections.

### Database-Centric Fix
A PL/pgSQL function with a cursor executes one grouped query for the 12-month window and streams rows sequentially via `FETCH`. The `GROUP BY` stays in PostgreSQL memory management and planner execution paths.

### Performance Engineering Notes
- Use `EXPLAIN (ANALYZE, BUFFERS)` to verify index-assisted scans and aggregation strategy (HashAggregate vs GroupAggregate).
- Validate reduced `rows` and `bytes` crossing the client boundary.
- Confirm stable latency under concurrent report access.

## Challenge 2: Bulk Ingestion and Lock Contention
### Symptom
Bulk API ingestion (10,000+ rows per call) fired row-level validation triggers for every row and held transactional resources longer than necessary.

### Why It Failed
- Row-level trigger work multiplied by batch size.
- Increased lock hold times and contention with concurrent writes.
- Higher rollback cost when validation failed late in the transaction.

### Database-Centric Fix
A statement-level trigger with transition tables (`REFERENCING NEW TABLE`) validates the inserted batch once per statement, then applies batched updates. Row-level triggers remain only for strict per-row integrity invariants (chronological checks on `last_active_date`).

### Trade-Off
- Statement-level validation improves throughput and reduces repeated CPU work.
- Row-level integrity checks are still necessary for invariants that must hold regardless of ingestion path.

## Challenge 3: Analytical Read-Locks on Transactional Tables
### Symptom
The new dashboard queried live transactional tables with expensive joins and ranking logic, degrading OLTP throughput.

### Why It Failed
- Heavy analytical reads competed for cache and I/O with write paths.
- Long-running queries amplified contention and plan instability.

### Database-Centric Fix
Precompute dashboard metrics in a materialized view and refresh asynchronously using `REFRESH MATERIALIZED VIEW CONCURRENTLY`. Use window functions (`RANK() OVER`) during materialization so read endpoints query a compact pre-aggregated surface.

### Operational Notes
- Concurrent refresh requires a unique index on the materialized view.
- Use refresh cadence aligned with dashboard staleness tolerance (for example every 5-15 minutes).
- Monitor refresh duration and I/O budget.

## Challenge 4: In-Memory Sequential Traversal
### Symptom
Consecutive daily learning streaks were calculated in Pandas and Node.js by sorting and scanning full activity histories in memory.

### Why It Failed
- High memory overhead for per-user timelines.
- Repeated data extraction from PostgreSQL.
- Slow end-to-end latency for leaderboard endpoints.

### Database-Centric Fix
Use a temporary table for active-user day-level activity, then compute streak chains with `WITH RECURSIVE`. This keeps ordering, adjacency checks, and chain expansion inside the planner and executor.

### Engineering Notes
- Temporary tables reduce repeated base-table scans in a session.
- Recursive CTEs model adjacency (`day + 1`) directly and avoid client-side loops.

## Observability and Validation Strategy
For each migration, compare before/after plans and runtime metrics:

- `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)` plan shape.
- `actual time`, `rows`, and shared/local buffer hits.
- API memory usage and p95/p99 endpoint latency.
- Lock wait events and transaction duration.

Target outcome was not only lower mean latency but tighter tail behavior and fewer lock-related incidents.

## Key Takeaways
- Data gravity is a design constraint, not a preference.
- If result cardinality is small and source cardinality is large, aggregate near storage.
- Use statement-level trigger validation for batch operations; reserve row-level triggers for strict invariants.
- Isolate analytics from OLTP with materialized views and controlled refresh cycles.
- Use recursive SQL for sequential logic before reaching for application-level iteration.

## Repository Contents
- `00_schema_and_synthetic_data.sql`: normalized LMS schema, indexes, and synthetic data generation.
- `01_cursor_aggregation.sql`: cursor-driven 12-month lead aggregation function.
- `02_bulk_ingestion_triggers.sql`: statement-level batch validation and row-level chronology trigger architecture.
- `03_analytics_views_and_recursion.sql`: materialized view + concurrent refresh and recursive streak computation.
