#!/usr/bin/env python3
"""
run_pipeline.py
Runs all fetch/compute scripts in order for the defense equity event study.
Usage: python run_pipeline.py
"""

import subprocess
import sys
import time
from pathlib import Path

SCRIPTS = [
    ("fetch_prices.py",  "Fetching stock price data (yfinance)..."),
    ("fetch_gdelt.py",   "Fetching GDELT geopolitical events (BigQuery)..."),
    ("fetch_macro.py",   "Fetching macro data from FRED..."),
    ("compute_cars.py",  "Computing cumulative abnormal returns (CARs)..."),
]

SCRIPTS_DIR = Path(__file__).parent

def run_script(script_name: str, label: str) -> bool:
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        print(f"  [SKIP] {script_name} not found at {script_path}")
        return False

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Running: {script_path}")
    print(f"{'='*60}")

    start = time.time()
    result = subprocess.run(
        [sys.executable, str(script_path)],
        check=False,
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"  [OK] {script_name} finished in {elapsed:.1f}s")
        return True
    else:
        print(f"  [FAIL] {script_name} exited with code {result.returncode} after {elapsed:.1f}s")
        return False


def main():
    print("\nDefense Equity Event Study — Data Pipeline")
    print(f"Scripts directory: {SCRIPTS_DIR}\n")

    results = {}
    for script_name, label in SCRIPTS:
        success = run_script(script_name, label)
        results[script_name] = success
        if not success:
            print(f"\n  Pipeline stopped: {script_name} failed.")
            print("  Fix the error above and re-run.\n")
            sys.exit(1)

    print(f"\n{'='*60}")
    print("  All scripts completed successfully.")
    print(f"{'='*60}\n")
    for name, ok in results.items():
        status = "OK  " if ok else "FAIL"
        print(f"  [{status}] {name}")
    print()


if __name__ == "__main__":
    main()