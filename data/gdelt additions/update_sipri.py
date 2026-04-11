"""
update_sipri.py
Continuously monitors SIPRI for their annual database refresh
(typically published in March each year) and appends new rows.

Strategy:
  1. Check the SIPRI arms transfers landing page for the "updated on"
     date in the announcement banner.
  2. Compare against the last-known update date stored locally.
  3. If newer, re-run the full download + parse pipeline from fetch_sipri.py.

Because SIPRI only updates once per year, the default poll interval
is 7 days. Run with --interval 0 for a one-shot check.

Usage:
    python python_scripts/update_sipri.py [--interval DAYS]

Example cron (every Sunday at 6am):
    0 6 * * 0 /path/to/venv/bin/python /path/to/python_scripts/update_sipri.py --interval 0
"""

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path

import requests

from fetch_sipri import download_sipri_export, parse_excel
from utils import append_new_rows, parse_date

SIPRI_LANDING = "https://www.sipri.org/databases/armstransfers"
HEADERS       = {"User-Agent": "Mozilla/5.0 (research scraper; contact: your@email.com)"}
STATE_FILE    = Path(__file__).parent.parent / "data" / "sipri_update_state.json"


# ── State persistence ─────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_sipri_update": ""}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Check SIPRI landing page ──────────────────────────────────────────────────

def get_sipri_update_date() -> str:
    """
    Scrape the SIPRI arms transfers page for the announced update date.
    The page contains text like:
    'The SIPRI Arms Transfers Database was updated on 9 March 2026.'
    Returns an ISO date string or empty string if not found.
    """
    try:
        resp = requests.get(SIPRI_LANDING, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[update_sipri] Could not fetch SIPRI landing page: {exc}")
        return ""

    m = re.search(
        r"updated on\s+(\d{1,2}\s+\w+\s+\d{4})",
        resp.text, re.I,
    )
    if m:
        raw = m.group(1)  # e.g. "9 March 2026"
        return parse_date(raw, "%d %B %Y")
    return ""


# ── Core update logic ─────────────────────────────────────────────────────────

def run_once() -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{now}] Checking SIPRI for database updates...")

    state = load_state()
    last_known = state.get("last_sipri_update", "")

    announced = get_sipri_update_date()
    if not announced:
        print("  [WARN] Could not detect SIPRI update date from landing page.")
        return

    print(f"  SIPRI announced update date : {announced}")
    print(f"  Last processed update date  : {last_known or '(never)'}")

    if announced <= last_known:
        print("  → No new SIPRI data. Nothing to do.")
        return

    print(f"  → New SIPRI data detected ({announced}). Downloading...")
    filepath = download_sipri_export()
    if filepath is None or not filepath.exists():
        print("  [ERROR] Download failed. Will retry on next check.")
        return

    rows = parse_excel(filepath)
    if not rows:
        print("  [ERROR] No rows parsed from SIPRI export.")
        return

    written = append_new_rows(rows)
    print(f"  → {len(rows)} records parsed, {written} new rows added to CSV.")

    state["last_sipri_update"] = announced
    save_state(state)
    print(f"  State saved: last_sipri_update = {announced}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="SIPRI continuous updater")
    parser.add_argument(
        "--interval", type=float, default=7.0,
        help="Days between checks (0 = run once and exit). Default: 7",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("SIPRI Arms Transfers Database — continuous updater")
    print(f"Interval: {'once' if args.interval == 0 else f'{args.interval} days'}")
    print("=" * 60)

    run_once()
    if args.interval == 0:
        return

    while True:
        sleep_secs = args.interval * 86400
        print(f"\nSleeping {args.interval} day(s) until next check...")
        time.sleep(sleep_secs)
        run_once()


if __name__ == "__main__":
    main()
