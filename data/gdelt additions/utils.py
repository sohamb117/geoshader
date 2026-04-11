"""
utils.py
Shared helpers for CSV I/O and deduplication across all scraper scripts.

Output CSV schema:
    date        – ISO 8601 date string (YYYY-MM-DD)
    description – Human-readable description of the event
    event_type  – One of: arms_deal | budget_resolution | arms_transfer |
                          aid_package | training_services | other
"""

import csv
import hashlib
import os
from datetime import date, datetime
from pathlib import Path
from typing import Optional

CSV_PATH = Path(__file__).parent.parent / "data" / "manual_events.csv"
FIELDNAMES = ["date", "description", "event_type"]


def ensure_csv() -> None:
    """Create the output CSV with headers if it doesn't already exist."""
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
        print(f"[utils] Created {CSV_PATH}")


def _row_hash(row: dict) -> str:
    """Stable hash of (date, description) used for deduplication."""
    key = f"{row['date']}|{row['description'].strip().lower()}"
    return hashlib.sha256(key.encode()).hexdigest()


def load_existing() -> dict[str, dict]:
    """Return {hash: row} for every row already in the CSV."""
    ensure_csv()
    rows = {}
    with open(CSV_PATH, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[_row_hash(row)] = row
    return rows


def append_new_rows(new_rows: list[dict]) -> int:
    """
    Append only rows that are not already present in the CSV.
    Returns the number of rows actually written.
    """
    ensure_csv()
    existing = load_existing()
    to_write = []
    for row in new_rows:
        row = {k: str(v).strip() for k, v in row.items()}
        if _row_hash(row) not in existing:
            to_write.append(row)

    if to_write:
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writerows(to_write)
        print(f"[utils] Appended {len(to_write)} new rows → {CSV_PATH}")
    else:
        print("[utils] No new rows to add.")

    return len(to_write)


def parse_date(raw: str, fmt: Optional[str] = None) -> str:
    """
    Parse a date string into ISO 8601 (YYYY-MM-DD).
    Tries fmt first, then a set of common patterns.
    Returns empty string on failure.
    """
    formats = [fmt] if fmt else []
    formats += [
        "%Y-%m-%d", "%d %b %Y", "%B %d, %Y", "%b %d, %Y",
        "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y",
    ]
    for f in formats:
        try:
            return datetime.strptime(raw.strip(), f).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    return ""
