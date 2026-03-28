#!/usr/bin/env python3
"""Export LMS benchmark tables to CSV files for Kaggle uploads."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2

DEFAULT_DSN = os.getenv("DATABASE_URL")

TABLES = [
    "lms_users",
    "course_enrollments",
    "daily_activity_logs",
    "newsletter_leads",
]


def export_tables_to_csv(dsn: str, schema: str = "lms_benchmark") -> None:
    """Export selected tables from a schema into kaggle_dataset/*.csv files."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    output_dir = repo_root / "kaggle_dataset"
    output_dir.mkdir(parents=True, exist_ok=True)

    with psycopg2.connect(dsn) as conn:
        for table_name in TABLES:
            query = f'SELECT * FROM "{schema}"."{table_name}"'
            df = pd.read_sql_query(query, conn)
            output_path = output_dir / f"{table_name}.csv"
            df.to_csv(output_path, index=False)
            print(f"Exported {schema}.{table_name} -> {output_path}")


def main() -> None:
    if not DEFAULT_DSN:
        raise RuntimeError(
            "Missing DATABASE_URL. Set DATABASE_URL before running export, "
            "for example: postgresql://user:password@host:5432/dbname"
        )
    export_tables_to_csv(DEFAULT_DSN)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Export failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
