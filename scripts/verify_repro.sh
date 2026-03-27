#!/usr/bin/env bash
set -euo pipefail

if command -v python3 >/dev/null 2>&1; then
  python3 scripts/verify_repro.py "$@"
elif command -v python >/dev/null 2>&1; then
  python scripts/verify_repro.py "$@"
else
  echo "ERROR: Python not found in PATH" >&2
  exit 1
fi
