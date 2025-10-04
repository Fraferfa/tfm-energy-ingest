#!/usr/bin/env python
"""Universal entrypoint.
Usage examples inside container:
  # Default (no args) -> run ingestion help
  docker run image

  # Ingest dataset
  docker run image ingest prices_pvpc --backfill-day 2025-09-10

  # Compaction
  docker run image compact --dataset prices_pvpc --month 2025-09 --dry-run

  # QC month
  docker run image qc --dataset prices_pvpc --month 2025-09

Any other first token will be treated as a python module/script path.
"""
from __future__ import annotations
import os
import sys
import subprocess

BASE_CMD = [sys.executable]

SCRIPTS = {
    "ingest": "pipelines/ingest/main.py",
    "compact": "pipelines/ingest/compact.py",
    "qc": "scripts/qc_month.py",
}

def main():
    args = sys.argv[1:]
    if not args:
        # Show simple help
        print("Usage: ingest|compact|qc [args...]  OR provide a python script path")
        print("Examples:")
        print("  ingest interconn --backfill-day 2025-09-09")
        print("  compact --dataset interconn --month 2025-09 --dry-run")
        sys.exit(0)

    first = args[0]
    if first in SCRIPTS:
        script = SCRIPTS[first]
        cmd = BASE_CMD + [script] + args[1:]
    else:
        # treat as direct script or module path
        script = first
        cmd = BASE_CMD + [script] + args[1:]

    # Propagate ESIOS_TOKEN presence info
    if "ESIOS_TOKEN" not in os.environ:
        print("[entrypoint] WARNING: ESIOS_TOKEN not set in environment.", file=sys.stderr)

    # Execute
    try:
        completed = subprocess.run(cmd, check=False)
        sys.exit(completed.returncode)
    except FileNotFoundError:
        print(f"[entrypoint] Script not found: {script}", file=sys.stderr)
        sys.exit(127)

if __name__ == "__main__":
    main()
