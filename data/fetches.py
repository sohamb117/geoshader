#!/usr/bin/env python3
"""
run_pipeline.py
Runs all fetch/compute scripts in order for the defense equity event study.
Usage: python run_pipeline.py
"""

import subprocess
import sys
import time
import platform
import shutil
from pathlib import Path

SCRIPTS = [
    ("fetch_prices.py",  "Fetching stock price data (yfinance)..."),
    ("fetch_gdelt.py",   "Fetching GDELT geopolitical events (BigQuery)..."),
    ("fetch_macro.py",   "Fetching macro data from FRED..."),
    ("compute_cars.py",  "Computing cumulative abnormal returns (CARs)..."),
]

SCRIPTS_DIR = Path(__file__).parent

def check_gcloud_installed() -> bool:
    """Check if gcloud CLI is installed."""
    return shutil.which("gcloud") is not None

def install_gcloud_sdk():
    """Install gcloud SDK based on the operating system."""
    system = platform.system()

    print("\n" + "="*60)
    print("  Installing Google Cloud SDK...")
    print("="*60 + "\n")

    if system == "Darwin":  # macOS
        print("  Detected macOS - installing via Homebrew...")
        try:
            # Check if Homebrew is installed
            if not shutil.which("brew"):
                print("  [ERROR] Homebrew not found. Please install Homebrew first:")
                print("  /bin/bash -c \"$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\"")
                sys.exit(1)

            result = subprocess.run(
                ["brew", "install", "--cask", "google-cloud-sdk"],
                check=False
            )
            if result.returncode != 0:
                print("  [ERROR] Failed to install gcloud SDK via Homebrew")
                sys.exit(1)
            print("  [OK] gcloud SDK installed successfully")
        except Exception as e:
            print(f"  [ERROR] Installation failed: {e}")
            sys.exit(1)

    elif system == "Windows":
        print("  Detected Windows - please install manually...")
        print("  Download from: https://cloud.google.com/sdk/docs/install-sdk#windows")
        print("  Or use PowerShell:")
        print("  (New-Object Net.WebClient).DownloadFile('https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe', '$env:Temp\\GoogleCloudSDKInstaller.exe')")
        print("  & $env:Temp\\GoogleCloudSDKInstaller.exe")
        print("\n  After installation, restart your terminal and run this script again.")
        sys.exit(1)

    else:  # Linux or other
        print(f"  Detected {system} - installing via script...")
        try:
            # Download and run the install script
            result = subprocess.run(
                ["curl", "https://sdk.cloud.google.com", "|", "bash"],
                shell=True,
                check=False
            )
            if result.returncode != 0:
                print("  [ERROR] Failed to install gcloud SDK")
                sys.exit(1)
            print("  [OK] gcloud SDK installed successfully")
            print("  Please restart your terminal and run: exec -l $SHELL")
        except Exception as e:
            print(f"  [ERROR] Installation failed: {e}")
            sys.exit(1)

def authenticate_gcloud():
    """Run gcloud authentication."""
    print("\n" + "="*60)
    print("  Authenticating with Google Cloud...")
    print("  This will open a browser window for authentication.")
    print("="*60 + "\n")

    result = subprocess.run(
        ["gcloud", "auth", "application-default", "login"],
        check=False
    )

    if result.returncode != 0:
        print("\n  [ERROR] Authentication failed")
        sys.exit(1)

    print("\n  [OK] Authentication successful")

def setup_gcloud():
    """Ensure gcloud SDK is installed and authenticated."""
    if not check_gcloud_installed():
        print("  gcloud SDK not found. Installing...")
        install_gcloud_sdk()
    else:
        print("  [OK] gcloud SDK already installed")

    authenticate_gcloud()

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

    # Setup gcloud SDK and authenticate
    setup_gcloud()

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