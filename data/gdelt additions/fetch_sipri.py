"""
fetch_sipri.py
Downloads the full SIPRI Arms Transfers Database export and converts
it to rows in data/manual_events.csv.

The SIPRI database (armstransfers.sipri.org) is a JavaScript SPA.
This script uses Playwright to:
  1. Navigate to the query interface
  2. Select all suppliers / all recipients / all years
  3. Trigger the Excel export
  4. Parse the downloaded file

Usage:
    pip install playwright openpyxl
    playwright install chromium
    python python_scripts/fetch_sipri.py

Notes:
    - SIPRI data is updated annually (typically in March).
    - The export covers 1950 to the most recent full calendar year.
    - One row per arms transfer order/delivery, not per press release.
    - event_type is always "arms_transfer" for SIPRI data.
"""

import re
import time
from pathlib import Path

import openpyxl
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from utils import append_new_rows, parse_date

SIPRI_URL    = "https://armstransfers.sipri.org/"
DOWNLOAD_DIR = Path(__file__).parent.parent / "data" / "sipri_downloads"
DELAY_SEC    = 2


# ── SIPRI-specific helpers ────────────────────────────────────────────────────

def classify_sipri(row_data: dict) -> str:
    """
    Map SIPRI weapon category to event_type.
    SIPRI uses TIV (Trend Indicator Values) and weapon descriptions.
    """
    weapon = str(row_data.get("weapon_description", "")).lower()
    category = str(row_data.get("weapon_category", "")).lower()
    combined = weapon + " " + category
    if any(kw in combined for kw in ["aircraft", "fighter", "helicopter", "uav", "drone"]):
        return "arms_deal"
    if any(kw in combined for kw in ["ship", "frigate", "submarine", "destroyer", "vessel"]):
        return "arms_deal"
    if any(kw in combined for kw in ["missile", "rocket", "torpedo", "bomb"]):
        return "arms_deal"
    if any(kw in combined for kw in ["armoured", "tank", "apc", "vehicle"]):
        return "arms_deal"
    if any(kw in combined for kw in ["radar", "sensor", "electronics", "communication"]):
        return "arms_deal"
    if any(kw in combined for kw in ["engine", "component", "parts"]):
        return "other"
    return "arms_transfer"


def build_description(row_data: dict) -> str:
    """
    Construct a human-readable description from SIPRI row fields.
    Typical SIPRI columns: supplier, recipient, weapon_description,
    number_ordered, year_of_order, weapon_category, tiv_deal_unit
    """
    supplier  = row_data.get("supplier", "Unknown supplier")
    recipient = row_data.get("recipient", "Unknown recipient")
    weapon    = row_data.get("weapon_description", "equipment")
    number    = row_data.get("number_ordered", "")
    tiv       = row_data.get("tiv_deal_unit", "")

    qty = f"{number} × " if str(number).strip() not in ("", "0", "nan") else ""
    tiv_str = f" (TIV: {tiv})" if str(tiv).strip() not in ("", "nan") else ""
    return f"{supplier} → {recipient}: {qty}{weapon}{tiv_str}"


def parse_excel(filepath: Path) -> list[dict]:
    """
    Parse the SIPRI Excel export into a list of row dicts.
    SIPRI exports vary slightly between years; this handles the
    common column naming conventions.
    """
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)

    # Find header row (first row where 'supplier' or 'Supplier' appears)
    headers = None
    header_row_idx = 0
    for i, row in enumerate(rows_iter):
        cells = [str(c).strip().lower() if c is not None else "" for c in row]
        if "supplier" in cells or "exporter" in cells:
            headers = cells
            header_row_idx = i
            break

    if headers is None:
        print("[ERROR] Could not locate header row in SIPRI Excel export.")
        return []

    # Normalize common header variants
    HEADER_MAP = {
        "exporter": "supplier",
        "importer": "recipient",
        "armament category": "weapon_category",
        "weapon": "weapon_description",
        "description": "weapon_description",
        "no. ordered": "number_ordered",
        "numbers delivered": "number_delivered",
        "year of order": "year_of_order",
        "order date": "year_of_order",
        "tiv deal": "tiv_deal_unit",
        "tiv (deal unit)": "tiv_deal_unit",
        "status": "status",
        "comments": "comments",
    }
    normalized = [HEADER_MAP.get(h, h) for h in headers]

    result_rows = []
    for raw_row in rows_iter:
        if all(c is None for c in raw_row):
            continue
        row_dict = {col: (str(val).strip() if val is not None else "")
                    for col, val in zip(normalized, raw_row)}

        # Date: SIPRI typically has year_of_order as a 4-digit year
        year_raw = row_dict.get("year_of_order", "")
        if not year_raw or not re.match(r"\d{4}", year_raw):
            continue
        date_str = parse_date(year_raw[:4], "%Y")
        if not date_str:
            continue

        description = build_description(row_dict)
        event_type  = classify_sipri(row_dict)

        result_rows.append({
            "date":        date_str,
            "description": description,
            "event_type":  event_type,
        })

    print(f"[SIPRI] Parsed {len(result_rows)} records from {filepath.name}")
    return result_rows


# ── Playwright download ───────────────────────────────────────────────────────

def download_sipri_export() -> Path | None:
    """
    Use Playwright to navigate the SIPRI SPA and download the full
    Excel export. Returns the path to the downloaded file, or None.
    """
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True)
        page = ctx.new_page()

        print(f"[SIPRI] Navigating to {SIPRI_URL} ...")
        page.goto(SIPRI_URL, wait_until="networkidle", timeout=60_000)
        time.sleep(DELAY_SEC)

        # ── Step 1: Select all suppliers ──────────────────────────────────
        # The query form uses dropdown selects or multiselect lists.
        # Try common selectors; fall back gracefully if the UI differs.
        try:
            # "Supplier" / "Exporter" select-all option
            for sel_label in ["Supplier", "Exporter", "All suppliers"]:
                el = page.query_selector(f"text={sel_label}")
                if el:
                    el.click()
                    time.sleep(0.5)
                    break

            # Select all in the supplier list
            select_all_btns = page.query_selector_all("button:has-text('Select all')")
            for btn in select_all_btns:
                btn.click()
                time.sleep(0.5)

        except PWTimeout:
            print("[SIPRI] Timeout clicking supplier select-all; continuing...")

        # ── Step 2: Set year range to maximum ─────────────────────────────
        try:
            # Look for year-from input and set to 1950
            year_inputs = page.query_selector_all("input[type='number'], input[placeholder*='year'], input[placeholder*='Year']")
            if len(year_inputs) >= 2:
                year_inputs[0].triple_click()
                year_inputs[0].type("1950")
                year_inputs[1].triple_click()
                year_inputs[1].type("2025")
        except Exception as exc:
            print(f"[SIPRI] Could not set year range: {exc}")

        # ── Step 3: Submit / Generate ─────────────────────────────────────
        try:
            for btn_text in ["Generate", "Search", "Submit", "Apply"]:
                btn = page.query_selector(f"button:has-text('{btn_text}')")
                if btn:
                    btn.click()
                    page.wait_for_load_state("networkidle", timeout=30_000)
                    break
        except PWTimeout:
            print("[SIPRI] Timeout waiting after Generate; continuing...")

        time.sleep(DELAY_SEC)

        # ── Step 4: Trigger the Excel download ────────────────────────────
        try:
            with page.expect_download(timeout=60_000) as dl_info:
                for btn_text in ["Export to Excel", "Download Excel", "Excel", "Download"]:
                    btn = page.query_selector(f"button:has-text('{btn_text}'), a:has-text('{btn_text}')")
                    if btn:
                        btn.click()
                        break
            download = dl_info.value
            dest = DOWNLOAD_DIR / download.suggested_filename
            download.save_as(dest)
            print(f"[SIPRI] Downloaded: {dest}")
            browser.close()
            return dest
        except PWTimeout:
            print("[SIPRI][WARN] Download button not found or timed out.")
            browser.close()
            return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("SIPRI Arms Transfers Database — full historical download")
    print("=" * 60)

    filepath = download_sipri_export()
    if filepath is None or not filepath.exists():
        print("[ERROR] SIPRI export download failed. "
              "If the UI has changed, inspect armstransfers.sipri.org "
              "and update the selectors in download_sipri_export().")
        return

    rows = parse_excel(filepath)
    if not rows:
        print("[ERROR] No rows parsed from SIPRI export.")
        return

    written = append_new_rows(rows)
    print(f"\nDone. {len(rows)} SIPRI records parsed, {written} new rows added to CSV.")


if __name__ == "__main__":
    main()
