#!/usr/bin/env python3
"""Run PostgreSQL LMS setup and benchmark workloads for portfolio reporting.

This script executes schema/setup SQL, runs EXPLAIN ANALYZE workloads for naive
and optimized patterns, renders a latency chart, and prints a README-ready
markdown section with measured timings.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import psycopg2
import seaborn as sns
from psycopg2.extensions import connection as PgConnection

DEFAULT_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/lms_db",
)

EXPLAIN_EXECUTION_RE = re.compile(r"Execution Time:\s*([0-9]+(?:\.[0-9]+)?)\s*ms")


@dataclass
class BenchmarkRow:
    benchmark: str
    naive_ms: Optional[float]
    optimized_ms: Optional[float]
    notes: str


def resolve_paths(script_dir: Path) -> Dict[str, Path]:
    """Resolve SQL and output paths for this repository layout."""
    repo_root = script_dir.parent
    sql_dir_primary = repo_root / "src"
    sql_dir_local = script_dir / "src"
    sql_dir_root = script_dir

    if sql_dir_primary.exists() and any(sql_dir_primary.glob("*.sql")):
        sql_dir = sql_dir_primary
    elif sql_dir_local.exists() and any(sql_dir_local.glob("*.sql")):
        sql_dir = sql_dir_local
    elif any(sql_dir_root.glob("*.sql")):
        sql_dir = sql_dir_root
    else:
        raise FileNotFoundError(
            "Unable to locate SQL source directory. "
            "Expected ./src, ../src, or SQL files alongside this script."
        )

    docs_dir = repo_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    return {
        "sql_dir": sql_dir,
        "docs_dir": docs_dir,
        "chart_path": docs_dir / "latency_comparison.png",
        "results_md_path": docs_dir / "live_benchmark_results.md",
    }


def connect_db(dsn: str) -> PgConnection:
    """Open a PostgreSQL connection with explicit exception reporting."""
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        return conn
    except psycopg2.Error as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc


def prepare_schema(conn: PgConnection, schema_name: str, reset_schema: bool) -> None:
    """Create an isolated schema and pin search_path for benchmark execution."""
    with conn.cursor() as cur:
        if reset_schema:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cur.execute(f'SET search_path TO "{schema_name}", public')
    conn.commit()


def execute_sql_file(conn: PgConnection, sql_file: Path) -> None:
    """Execute SQL file contents in one transaction."""
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    sql_text = sql_file.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def explain_execution_ms(conn: PgConnection, sql: str, params: Optional[tuple] = None) -> float:
    """Run EXPLAIN ANALYZE and extract execution time in milliseconds."""
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql}"
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(explain_sql, params)
        lines = [row[0] for row in cur.fetchall()]
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    for line in reversed(lines):
        match = EXPLAIN_EXECUTION_RE.search(line)
        if match:
            return float(match.group(1))

    return elapsed_ms


def phase_setup(conn: PgConnection, sql_dir: Path) -> None:
    """Apply schema, synthetic data, and optimization objects."""
    ordered_files = [
        sql_dir / "00_schema_and_synthetic_data.sql",
        sql_dir / "01_cursor_aggregation.sql",
        sql_dir / "02_bulk_ingestion_triggers.sql",
        sql_dir / "03_analytics_views_and_recursion.sql",
    ]
    for sql_file in ordered_files:
        execute_sql_file(conn, sql_file)


def month_starts(end_month: dt.date) -> List[dt.date]:
    """Return month starts for the rolling 12-month window."""
    starts: List[dt.date] = []
    for i in range(11, -1, -1):
        first_of_month = end_month.replace(day=1)
        year = first_of_month.year
        month = first_of_month.month - i
        while month <= 0:
            month += 12
            year -= 1
        while month > 12:
            month -= 12
            year += 1
        starts.append(dt.date(year, month, 1))
    return starts


def add_month(date_val: dt.date) -> dt.date:
    """Return first day of the next month."""
    if date_val.month == 12:
        return dt.date(date_val.year + 1, 1, 1)
    return dt.date(date_val.year, date_val.month + 1, 1)


def benchmark_aggregation(conn: PgConnection, baseline_mode: str) -> BenchmarkRow:
    """Benchmark A: naive 12-query loop vs cursor function."""
    end_month = dt.date.today().replace(day=1)
    naive_total_ms = 0.0

    if baseline_mode == "db-aggregate":
        naive_sql = (
            "SELECT COUNT(*)::bigint AS total_leads, "
            "COUNT(DISTINCT user_id)::bigint AS converted_users "
            "FROM newsletter_leads "
            "WHERE captured_at >= %s AND captured_at < %s"
        )
        for month_start in month_starts(end_month):
            month_end = add_month(month_start)
            naive_total_ms += explain_execution_ms(conn, naive_sql, (month_start, month_end))

        optimized_sql = "SELECT * FROM fn_monthly_lead_report_cursor(%s::date)"
        optimized_ms = explain_execution_ms(conn, optimized_sql, (end_month,))
        notes = "N+1 month-scoped DB aggregates vs single grouped execution in PostgreSQL."
    elif baseline_mode == "app-materialize-json":
        naive_sql = (
            "SELECT user_id "
            "FROM newsletter_leads "
            "WHERE captured_at >= %s AND captured_at < %s"
        )
        for month_start in month_starts(end_month):
            month_end = add_month(month_start)
            start = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute(naive_sql, (month_start, month_end))
                rows = cur.fetchall()
            materialized_rows = [{"user_id": row[0]} for row in rows]
            _wire_payload = json.dumps(materialized_rows, default=str)
            _total_leads = len(materialized_rows)
            _converted_users = len({item["user_id"] for item in materialized_rows if item["user_id"] is not None})
            naive_total_ms += (time.perf_counter() - start) * 1000.0

        optimized_sql = "SELECT * FROM fn_monthly_lead_report_cursor(%s::date)"
        start = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(optimized_sql, (end_month,))
            rows = cur.fetchall()
        optimized_rows = [
            {
                "report_month": row[0].isoformat() if row[0] is not None else None,
                "total_leads": int(row[1]),
                "converted_users": int(row[2]),
                "conversion_rate_pct": float(row[3]),
            }
            for row in rows
        ]
        _wire_payload = json.dumps(optimized_rows, default=str)
        optimized_ms = (time.perf_counter() - start) * 1000.0
        notes = "N+1 raw-row pulls + app materialization/JSON serialization vs single grouped execution."
    else:
        raise ValueError(f"Unsupported aggregation baseline mode: {baseline_mode}")

    return BenchmarkRow(
        benchmark="Benchmark A - 12M Lead Aggregation",
        naive_ms=round(naive_total_ms, 3),
        optimized_ms=round(optimized_ms, 3),
        notes=notes,
    )


def benchmark_bulk_ingestion(conn: PgConnection) -> BenchmarkRow:
    """Benchmark B: statement-level trigger path on 5,000-row insert."""
    offset_days = 180 + int(dt.datetime.now().timestamp()) % 1000

    insert_sql = """
    WITH candidate_users AS (
        SELECT user_id, row_number() OVER (ORDER BY user_id) AS rn
        FROM lms_users
        WHERE is_active = TRUE
        LIMIT 5000
    )
    INSERT INTO daily_activity_logs (
        user_id,
        activity_date,
        minutes_learned,
        lessons_completed,
        created_at
    )
    SELECT
        cu.user_id,
        (CURRENT_DATE - (%s::int + (cu.rn %% 30)::int))::date,
        20 + (cu.rn %% 120),
        cu.rn %% 5,
        clock_timestamp()
    FROM candidate_users cu
    """

    optimized_ms = explain_execution_ms(conn, insert_sql, (offset_days,))

    return BenchmarkRow(
        benchmark="Benchmark B - Bulk Ingestion (5k rows)",
        naive_ms=None,
        optimized_ms=round(optimized_ms, 3),
        notes="Statement-level trigger with transition table and nested row invariant checks.",
    )


def benchmark_analytics(conn: PgConnection) -> Dict[str, BenchmarkRow]:
    """Benchmark C: raw heavy analytical query vs materialized view read."""
    naive_sql = """
    SELECT
        date_trunc('month', dal.activity_date)::date AS metric_month,
        dal.user_id,
        SUM(dal.minutes_learned)::bigint AS total_minutes,
        SUM(dal.lessons_completed)::bigint AS total_lessons,
        COUNT(*)::bigint AS active_days,
        RANK() OVER (
            PARTITION BY date_trunc('month', dal.activity_date)::date
            ORDER BY SUM(dal.minutes_learned) DESC,
                     SUM(dal.lessons_completed) DESC,
                     dal.user_id
        ) AS engagement_rank
    FROM daily_activity_logs dal
    JOIN lms_users u ON u.user_id = dal.user_id
    WHERE u.is_active = TRUE
    GROUP BY date_trunc('month', dal.activity_date)::date, dal.user_id
    ORDER BY metric_month DESC, engagement_rank ASC
    LIMIT 5000
    """
    naive_ms = explain_execution_ms(conn, naive_sql)

    refresh_sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_user_monthly_learning_metrics"
    refresh_ms = explain_execution_ms(conn, refresh_sql)

    optimized_read_sql = """
    SELECT metric_month, user_id, total_minutes, total_lessons, active_days, engagement_rank
    FROM mv_user_monthly_learning_metrics
    ORDER BY metric_month DESC, engagement_rank ASC
    LIMIT 5000
    """
    optimized_read_ms = explain_execution_ms(conn, optimized_read_sql)

    return {
        "analytics_read": BenchmarkRow(
            benchmark="Benchmark C - Analytics Read Path",
            naive_ms=round(naive_ms, 3),
            optimized_ms=round(optimized_read_ms, 3),
            notes="Live window aggregation over base logs vs precomputed materialized metrics.",
        ),
        "analytics_refresh": BenchmarkRow(
            benchmark="Benchmark C - MV Concurrent Refresh",
            naive_ms=None,
            optimized_ms=round(refresh_ms, 3),
            notes="Asynchronous refresh cost for maintaining analytical read surface.",
        ),
    }


def prepare_temp_activity_subset(conn: PgConnection) -> None:
    """Prepare temporary daily activity subset for recursive streak traversal."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS temp_active_user_days_bench")
        cur.execute(
            """
            CREATE TEMP TABLE temp_active_user_days_bench (
                user_id UUID NOT NULL,
                activity_date DATE NOT NULL,
                PRIMARY KEY (user_id, activity_date)
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            INSERT INTO temp_active_user_days_bench (user_id, activity_date)
            SELECT DISTINCT dal.user_id, dal.activity_date
            FROM daily_activity_logs dal
            JOIN lms_users u ON u.user_id = dal.user_id
            WHERE u.is_active = TRUE
              AND dal.activity_date >= CURRENT_DATE - INTERVAL '365 days'
            """
        )
        cur.execute(
            "CREATE INDEX idx_temp_active_user_days_bench_user_date "
            "ON temp_active_user_days_bench (user_id, activity_date)"
        )


def benchmark_recursion(conn: PgConnection) -> BenchmarkRow:
    """Benchmark D: recursive CTE streak computation."""
    prepare_temp_activity_subset(conn)

    recursive_sql = """
    WITH RECURSIVE streak_chains AS (
        SELECT
            t.user_id,
            t.activity_date AS streak_start,
            t.activity_date AS activity_date,
            1 AS streak_len
        FROM temp_active_user_days_bench t
        LEFT JOIN temp_active_user_days_bench prev
          ON prev.user_id = t.user_id
         AND prev.activity_date = t.activity_date - 1
        WHERE prev.user_id IS NULL

        UNION ALL

        SELECT
            sc.user_id,
            sc.streak_start,
            nxt.activity_date,
            sc.streak_len + 1 AS streak_len
        FROM streak_chains sc
        JOIN temp_active_user_days_bench nxt
          ON nxt.user_id = sc.user_id
         AND nxt.activity_date = sc.activity_date + 1
    )
    SELECT sc.user_id, MAX(sc.streak_len) AS longest_streak_days
    FROM streak_chains sc
    GROUP BY sc.user_id
    ORDER BY longest_streak_days DESC, sc.user_id
    """
    optimized_ms = explain_execution_ms(conn, recursive_sql)

    return BenchmarkRow(
        benchmark="Benchmark D - Recursive Streaks",
        naive_ms=None,
        optimized_ms=round(optimized_ms, 3),
        notes="In-engine recursive traversal over temporary deduplicated day-level activity.",
    )


def render_latency_chart(rows: List[BenchmarkRow], chart_path: Path) -> None:
    """Render grouped latency comparison chart with dark styling."""
    categories = [
        "Benchmark A - 12M Lead Aggregation",
        "Benchmark C - Analytics Read Path",
        "Benchmark B - Bulk Ingestion (5k rows)",
        "Benchmark D - Recursive Streaks",
    ]

    naive_values: Dict[str, Optional[float]] = {r.benchmark: r.naive_ms for r in rows}
    optimized_values: Dict[str, Optional[float]] = {r.benchmark: r.optimized_ms for r in rows}

    plot_data = {
        "Benchmark": [],
        "Approach": [],
        "Execution Time (ms)": [],
    }

    for category in categories:
        naive = naive_values.get(category)
        optimized = optimized_values.get(category)

        if naive is not None:
            plot_data["Benchmark"].append(category.replace("Benchmark ", ""))
            plot_data["Approach"].append("Naive")
            plot_data["Execution Time (ms)"].append(naive)

        if optimized is not None:
            plot_data["Benchmark"].append(category.replace("Benchmark ", ""))
            plot_data["Approach"].append("Optimized")
            plot_data["Execution Time (ms)"].append(optimized)

    sns.set_theme(style="darkgrid", context="talk")
    plt.figure(figsize=(14, 8), facecolor="#111418")
    ax = sns.barplot(
        x=plot_data["Benchmark"],
        y=plot_data["Execution Time (ms)"],
        hue=plot_data["Approach"],
        palette={"Naive": "#FF6B6B", "Optimized": "#4ECDC4"},
    )

    ax.set_title("LMS Analytical Pipeline: Query Latency Optimization", color="#E6EDF3", pad=20)
    ax.set_xlabel("Workload", color="#D1D7E0")
    ax.set_ylabel("Execution Time (ms)", color="#D1D7E0")
    ax.tick_params(colors="#D1D7E0")

    for spine in ax.spines.values():
        spine.set_color("#2B3137")

    ax.legend(facecolor="#111418", edgecolor="#2B3137", labelcolor="#D1D7E0")
    ax.set_facecolor("#161B22")

    for container in ax.containers:
        ax.bar_label(container, fmt="%.1f", color="#E6EDF3", padding=3, fontsize=9)

    plt.xticks(rotation=12, ha="right")
    plt.tight_layout()
    plt.savefig(chart_path, dpi=220)
    plt.close()


def percent_improvement(naive_ms: Optional[float], optimized_ms: Optional[float]) -> str:
    """Return percentage improvement where comparable."""
    if naive_ms is None or optimized_ms is None or naive_ms <= 0:
        return "N/A"
    value = ((naive_ms - optimized_ms) / naive_ms) * 100.0
    return f"{value:.2f}%"


def improvement_phrase(naive_ms: Optional[float], optimized_ms: Optional[float]) -> str:
    """Return human-readable directional improvement text."""
    if naive_ms is None or optimized_ms is None or naive_ms <= 0:
        return "with no comparable baseline in this run"
    value = ((naive_ms - optimized_ms) / naive_ms) * 100.0
    if value >= 0:
        return f"reducing transfer and compute latency by {value:.2f}%"
    return f"increasing transfer and compute latency by {abs(value):.2f}%"


def format_ms(value: Optional[float]) -> str:
    """Format milliseconds for markdown output."""
    if value is None:
        return "N/A"
    return f"{value:.3f}"


def build_readme_section(rows: List[BenchmarkRow]) -> str:
    """Create a README-ready markdown section with chart and benchmark table."""
    lines = [
        "## Live Benchmark Results",
        "",
        "![LMS query latency comparison](docs/latency_comparison.png)",
        "",
        "| Benchmark | Naive (ms) | Optimized (ms) | Improvement |",
        "|---|---:|---:|---:|",
    ]

    for row in rows:
        lines.append(
            f"| {row.benchmark} | {format_ms(row.naive_ms)} | "
            f"{format_ms(row.optimized_ms)} | {percent_improvement(row.naive_ms, row.optimized_ms)} |"
        )

    aggregation = next(r for r in rows if r.benchmark == "Benchmark A - 12M Lead Aggregation")
    analytics = next(r for r in rows if r.benchmark == "Benchmark C - Analytics Read Path")

    lines.extend(
        [
            "",
            "Moving from repeated monthly query dispatch in the N+1 aggregation loop "
            "to a single cursor-backed grouped execution shifted planning from repeated "
            "index-access cycles toward one consolidated aggregate path, "
            f"{improvement_phrase(aggregation.naive_ms, aggregation.optimized_ms)}.",
            "",
            "For analytics, replacing repeated live window aggregation over "
            "daily_activity_logs with reads from a precomputed materialized surface "
            "eliminated heavy per-request aggregation work and reduced read latency by "
            f"{percent_improvement(analytics.naive_ms, analytics.optimized_ms)} while preserving "
            "concurrent refresh semantics.",
        ]
    )

    return "\n".join(lines)


def run_benchmarks(
    dsn: str,
    schema_name: str,
    reset_schema: bool,
    aggregation_baseline_mode: str,
    require_positive_benchmark_a: bool,
    output_readme_only: bool = False,
) -> int:
    """Execute setup, benchmark phases, chart generation, and markdown output."""
    script_dir = Path(__file__).resolve().parent
    paths = resolve_paths(script_dir)

    try:
        conn = connect_db(dsn)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        print(f"[Init] Preparing benchmark schema '{schema_name}' ...")
        prepare_schema(conn, schema_name=schema_name, reset_schema=reset_schema)

        print("[Phase 1] Applying schema, synthetic data, and optimization SQL ...")
        phase_setup(conn, paths["sql_dir"])

        print("[Phase 2] Running EXPLAIN ANALYZE benchmarks ...")
        benchmark_rows: List[BenchmarkRow] = []

        bench_a = benchmark_aggregation(conn, aggregation_baseline_mode)
        benchmark_rows.append(bench_a)
        print(f"  - {bench_a.benchmark}: naive={bench_a.naive_ms} ms, optimized={bench_a.optimized_ms} ms")
        if require_positive_benchmark_a and bench_a.naive_ms is not None and bench_a.optimized_ms is not None:
            if bench_a.optimized_ms >= bench_a.naive_ms:
                print(
                    "Benchmark A failed assertion: optimized path is not faster than naive baseline.",
                    file=sys.stderr,
                )
                return 2

        bench_b = benchmark_bulk_ingestion(conn)
        benchmark_rows.append(bench_b)
        print(f"  - {bench_b.benchmark}: optimized={bench_b.optimized_ms} ms")

        bench_c = benchmark_analytics(conn)
        benchmark_rows.append(bench_c["analytics_read"])
        benchmark_rows.append(bench_c["analytics_refresh"])
        print(
            "  - Benchmark C - Analytics Read Path: "
            f"naive={bench_c['analytics_read'].naive_ms} ms, "
            f"optimized={bench_c['analytics_read'].optimized_ms} ms"
        )
        print(f"  - Benchmark C - MV Concurrent Refresh: optimized={bench_c['analytics_refresh'].optimized_ms} ms")

        bench_d = benchmark_recursion(conn)
        benchmark_rows.append(bench_d)
        print(f"  - {bench_d.benchmark}: optimized={bench_d.optimized_ms} ms")

        conn.commit()

        print("[Phase 3] Rendering latency chart ...")
        render_latency_chart(benchmark_rows, paths["chart_path"])

        print("[Phase 4] Building README benchmark section ...")
        section_md = build_readme_section(benchmark_rows)
        paths["results_md_path"].write_text(section_md + "\n", encoding="utf-8")

        if output_readme_only:
            print(section_md)
        else:
            print("\n===== README SECTION START =====")
            print(section_md)
            print("===== README SECTION END =====\n")
            print(f"Chart written to: {paths['chart_path']}")
            print(f"Markdown snapshot written to: {paths['results_md_path']}")

        return 0
    except (psycopg2.Error, RuntimeError, FileNotFoundError) as exc:
        conn.rollback()
        print(f"Benchmark execution failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run LMS PostgreSQL benchmark suite.")
    parser.add_argument(
        "--dsn",
        default=DEFAULT_DSN,
        help="PostgreSQL DSN/connection string.",
    )
    parser.add_argument(
        "--readme-only",
        action="store_true",
        help="Print only the markdown benchmark section to stdout.",
    )
    parser.add_argument(
        "--schema",
        default="lms_benchmark",
        help="Schema used for isolated benchmark objects.",
    )
    parser.add_argument(
        "--no-reset-schema",
        action="store_true",
        help="Do not drop schema before execution.",
    )
    parser.add_argument(
        "--aggregation-baseline-mode",
        choices=["app-materialize-json", "db-aggregate"],
        default="app-materialize-json",
        help=(
            "Benchmark A baseline mode. "
            "'app-materialize-json' measures end-to-end API-like N+1 cost; "
            "'db-aggregate' measures DB-only month-by-month aggregates."
        ),
    )
    parser.add_argument(
        "--require-positive-benchmark-a",
        action="store_true",
        help="Exit with code 2 if Benchmark A optimized_ms is not lower than naive_ms.",
    )
    return parser.parse_args()


def main() -> int:
    """Entry point."""
    args = parse_args()
    random.seed(42)
    return run_benchmarks(
        args.dsn,
        schema_name=args.schema,
        reset_schema=not args.no_reset_schema,
        aggregation_baseline_mode=args.aggregation_baseline_mode,
        require_positive_benchmark_a=args.require_positive_benchmark_a,
        output_readme_only=args.readme_only,
    )


if __name__ == "__main__":
    raise SystemExit(main())
