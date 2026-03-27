#!/usr/bin/env python3
"""Cross-platform reproducibility verifier for LMS PostgreSQL benchmarks.

This script launches an isolated PostgreSQL container, waits for readiness,
runs the benchmark suite with strict Benchmark A semantics, and reports
artifact locations.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True)


def wait_for_postgres(container_name: str, user: str, db: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        probe = subprocess.run(
            [
                "docker",
                "exec",
                container_name,
                "pg_isready",
                "-U",
                user,
                "-d",
                db,
            ],
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if probe.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("PostgreSQL container did not become ready before timeout.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run reproducible LMS benchmark verification.")
    parser.add_argument("--container-name", default="lms_pg15_bench", help="Docker container name.")
    parser.add_argument("--image", default="postgres:15", help="PostgreSQL Docker image.")
    parser.add_argument("--port", type=int, default=5433, help="Host port to bind to PostgreSQL 5432.")
    parser.add_argument("--db", default="lms_db", help="Database name.")
    parser.add_argument("--user", default="postgres", help="Database user.")
    parser.add_argument("--password", default="postgres", help="Database password.")
    parser.add_argument("--schema", default="lms_benchmark", help="Benchmark schema name.")
    parser.add_argument(
        "--aggregation-baseline-mode",
        choices=["app-materialize-json", "db-aggregate"],
        default="app-materialize-json",
        help="Benchmark A baseline mode.",
    )
    parser.add_argument(
        "--no-require-positive-benchmark-a",
        action="store_true",
        help="Disable fail-fast check for Benchmark A positive improvement.",
    )
    parser.add_argument(
        "--keep-container",
        action="store_true",
        help="Keep the benchmark container running after completion.",
    )
    parser.add_argument(
        "--startup-timeout-seconds",
        type=int,
        default=60,
        help="Timeout waiting for PostgreSQL readiness.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if shutil.which("docker") is None:
        print("ERROR: Docker CLI not found in PATH.", file=sys.stderr)
        return 1

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    bench_script = script_dir / "run_lms_benchmarks.py"
    if not bench_script.exists():
        print(f"ERROR: benchmark script not found: {bench_script}", file=sys.stderr)
        return 1

    dsn = f"postgresql://{args.user}:{args.password}@localhost:{args.port}/{args.db}"

    run(["docker", "rm", "-f", args.container_name], check=False)
    run(
        [
            "docker",
            "run",
            "--name",
            args.container_name,
            "-e",
            f"POSTGRES_DB={args.db}",
            "-e",
            f"POSTGRES_USER={args.user}",
            "-e",
            f"POSTGRES_PASSWORD={args.password}",
            "-p",
            f"{args.port}:5432",
            "-d",
            args.image,
        ]
    )

    try:
        wait_for_postgres(
            container_name=args.container_name,
            user=args.user,
            db=args.db,
            timeout_seconds=args.startup_timeout_seconds,
        )

        cmd = [
            sys.executable,
            str(bench_script),
            "--dsn",
            dsn,
            "--schema",
            args.schema,
            "--aggregation-baseline-mode",
            args.aggregation_baseline_mode,
        ]
        if not args.no_require_positive_benchmark_a:
            cmd.append("--require-positive-benchmark-a")

        proc = subprocess.run(cmd, cwd=repo_root, text=True)
        if proc.returncode != 0:
            return proc.returncode

        print("\nVerification succeeded.")
        print(f"Results markdown: {repo_root / 'docs' / 'live_benchmark_results.md'}")
        print(f"Latency chart: {repo_root / 'docs' / 'latency_comparison.png'}")
        return 0
    finally:
        if not args.keep_container:
            run(["docker", "rm", "-f", args.container_name], check=False)


if __name__ == "__main__":
    raise SystemExit(main())
