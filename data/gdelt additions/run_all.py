"""
run_all.py
Orchestrates the complete GDELT Arms Sales Enhancement pipeline.

Modes:
    1. Full historical fetch + continuous updates (default)
    2. Fetch-only mode: One-time historical population
    3. Update-only mode: Start continuous pollers
    4. Custom: Mix and match (e.g., DSCA only, custom intervals)

Usage:
    # Full run: fetch everything historically, then start continuous updaters
    python run_all.py

    # One-shot fetch only (good for first-time setup)
    python run_all.py --fetch-only

    # Just poll for updates (if you've already done the full fetch)
    python run_all.py --update-only

    # Only DSCA, check every 12 hours
    python run_all.py --dsca-only --dsca-interval 12

    # Run each updater once and exit (for cron)
    python run_all.py --update-only --dsca-interval 0 --sipri-interval 0

Dependencies:
    Requires: fetch_dsca.py, fetch_sipri.py, update_dsca.py, update_sipri.py
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path


def run_script(script_name: str, args: list = None) -> bool:
    """
    Run a Python script in the same directory.
    
    Args:
        script_name: Name of the script to run (e.g., 'fetch_dsca.py')
        args: Additional command-line arguments to pass
    
    Returns:
        True if successful, False if failed
    """
    script_path = Path(__file__).parent / script_name
    
    if not script_path.exists():
        print(f"  [ERROR] {script_name} not found at {script_path}")
        return False
    
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(
        description="GDELT Arms Sales Enhancement pipeline orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="Run full historical fetches only, then exit"
    )
    parser.add_argument(
        "--update-only",
        action="store_true",
        help="Start update pollers only (assumes data already fetched)"
    )
    parser.add_argument(
        "--dsca-only",
        action="store_true",
        help="Only run DSCA (skip SIPRI)"
    )
    parser.add_argument(
        "--sipri-only",
        action="store_true",
        help="Only run SIPRI (skip DSCA)"
    )
    parser.add_argument(
        "--dsca-interval",
        type=float,
        default=24,
        help="DSCA update interval in hours (default: 24, use 0 for one-shot)"
    )
    parser.add_argument(
        "--sipri-interval",
        type=float,
        default=24,
        help="SIPRI update interval in hours (default: 24, use 0 for one-shot)"
    )
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    print(f"Scripts directory: {script_dir}\n")
    
    # Determine what to run
    fetch_dsca = not args.sipri_only
    fetch_sipri = not args.dsca_only
    update_dsca = not args.sipri_only
    update_sipri = not args.dsca_only
    
    # If fetch-only or update-only specified, respect that
    if args.fetch_only:
        print("Mode: Fetch-only\n")
        success = True
        if fetch_dsca and not run_script("fetch_dsca.py"):
            success = False
        if fetch_sipri and not run_script("fetch_sipri.py"):
            success = False
        if not success:
            print("\nPipeline stopped: One or more fetch scripts failed.")
            sys.exit(1)
        print("\nFetch complete!")
        return
    
    if args.update_only:
        print("Mode: Update-only\n")
        if update_dsca:
            interval_args = ["--interval", str(int(args.dsca_interval))] if args.dsca_interval != 24 else []
            run_script("update_dsca.py", interval_args)
        if update_sipri:
            interval_args = ["--interval", str(int(args.sipri_interval))] if args.sipri_interval != 24 else []
            run_script("update_sipri.py", interval_args)
        return
    
    # Default: Full run (fetch + update)
    print("Mode: Full run (fetch + continuous updates)\n")
    print("Phase 1: Historical fetch")
    if fetch_dsca and not run_script("fetch_dsca.py"):
        print("Pipeline stopped: fetch_dsca.py failed.")
        print("Fix the error above and re-run.")
        sys.exit(1)
    if fetch_sipri and not run_script("fetch_sipri.py"):
        print("Pipeline stopped: fetch_sipri.py failed.")
        print("Fix the error above and re-run.")
        sys.exit(1)
    
    print("\nPhase 2: Starting continuous updaters")
    if update_dsca:
        interval_args = ["--interval", str(int(args.dsca_interval))] if args.dsca_interval != 24 else []
        run_script("update_dsca.py", interval_args)
    if update_sipri:
        interval_args = ["--interval", str(int(args.sipri_interval))] if args.sipri_interval != 24 else []
        run_script("update_sipri.py", interval_args)


if __name__ == "__main__":
    main()