import argparse
import os
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "pipelines" / "ingest" / "main.py"
CFG = ROOT / "config" / "ingest.yaml"


def load_cfg():
    with open(CFG, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_dataset(ds_name: str, target_date: str | None, extra_env: dict | None) -> tuple[str, int, str]:
    cmd = [sys.executable, str(MAIN), ds_name, "--local"]
    if target_date:
        cmd += ["--target-date", target_date]
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    try:
        print(f"\n=== Running {ds_name} ===")
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(ROOT))
        print(proc.stdout)
        if proc.returncode != 0:
            print(proc.stderr, file=sys.stderr)
        tail = ""
        if proc.stdout:
            lines = [l for l in proc.stdout.splitlines() if l.strip()]
            tail = lines[-1] if lines else ""
        return ds_name, proc.returncode, tail
    except Exception as e:
        return ds_name, 1, str(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", help="Filtra datasets por nombre")
    parser.add_argument("--target-date", help="YYYY-MM-DD para cargas DST-safe", default=None)
    parser.add_argument("--auth-mode", choices=["x-api-key", "authorization", "both"], help="Forzar cabeceras ESIOS")
    args = parser.parse_args()

    cfg = load_cfg()
    datasets = [name for name, ds in cfg.get("datasets", {}).items() if ds.get("enabled", True)]
    if args.only:
        allow = set(args.only)
        datasets = [d for d in datasets if d in allow]

    extra_env = {}
    if args.auth_mode:
        extra_env["ESIOS_AUTH_MODE"] = args.auth_mode

    results = []
    for ds_name in datasets:
        ds, rc, tail = run_dataset(ds_name, args.target_date, extra_env)
        results.append((ds, rc, tail))

    print("\n===== RESUMEN =====")
    for ds, rc, tail in results:
        status = "OK" if rc == 0 else f"FAIL({rc})"
        print(f"- {ds}: {status} | {tail}")

    any_fail = any(rc != 0 for _, rc, _ in results)
    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
