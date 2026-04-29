"""
fetch_sipri.py
Downloads the full SIPRI Arms Transfers Database export and converts
it to rows in data/manual_events.csv.

The SIPRI database (armstransfers.sipri.org) provides data export in CSV format.
This script uses Playwright to:
  1. Navigate to the Import/Export page
  2. Select all suppliers / all recipients / all years
  3. Trigger the CSV export download

Usage:
    pip install playwright
    playwright install chromium
    python fetch_sipri.py

Notes:
    - SIPRI data is updated annually (typically in March).
    - The export covers 1950 to the most recent full calendar year.
    - One row per arms transfer order/delivery, not per press release.
    - event_type is always "arms_transfer" for SIPRI data.
"""

import re
import time
import csv
from pathlib import Path
from io import StringIO

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from utils import append_new_rows, parse_date

SIPRI_BASE_URL = "https://armstransfers.sipri.org"
SIPRI_EXPORT_URL = f"{SIPRI_BASE_URL}/ArmsTransfer/ImportExport"
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


def parse_csv(filepath: Path) -> list[dict]:
    """
    Parse the SIPRI CSV export into a list of row dicts.
    Handles the standard SIPRI CSV format with named columns.
    """
    rows_result = []
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            if reader.fieldnames is None:
                print("[ERROR] CSV file has no headers")
                return []
            
            # Normalize column names to lowercase
            fieldnames_lower = [name.lower() for name in (reader.fieldnames or [])]
            print(f"[SIPRI] CSV columns: {fieldnames_lower}")
            
            for row in reader:
                # Normalize the row keys to lowercase for easier access
                row_lower = {k.lower(): v for k, v in row.items()}
                
                # Extract date - try different column names
                year_raw = ""
                for col_name in ["year of order", "year_of_order", "year", "order year"]:
                    if col_name in row_lower:
                        year_raw = row_lower[col_name]
                        break
                
                if not year_raw or not re.match(r"\d{4}", str(year_raw)):
                    continue
                
                date_str = parse_date(str(year_raw)[:4], "%Y")
                if not date_str:
                    continue
                
                # Extract key fields
                supplier = row_lower.get("supplier", row_lower.get("exporter", "Unknown"))
                recipient = row_lower.get("recipient", row_lower.get("importer", "Unknown"))
                weapon = row_lower.get("weapon description", row_lower.get("weapon", "equipment"))
                number = row_lower.get("no. ordered", row_lower.get("numbers delivered", ""))
                tiv = row_lower.get("tiv (deal unit)", row_lower.get("tiv deal", ""))
                
                # Build description
                qty = f"{number} × " if str(number).strip() not in ("", "0", "nan", "-") else ""
                tiv_str = f" (TIV: {tiv})" if str(tiv).strip() not in ("", "nan", "-") else ""
                description = f"{supplier} → {recipient}: {qty}{weapon}{tiv_str}"
                
                # Classify event type
                weapon_lower = str(weapon).lower()
                if any(kw in weapon_lower for kw in ["aircraft", "fighter", "helicopter", "uav", "drone"]):
                    event_type = "arms_deal"
                elif any(kw in weapon_lower for kw in ["ship", "frigate", "submarine", "destroyer", "vessel"]):
                    event_type = "arms_deal"
                elif any(kw in weapon_lower for kw in ["missile", "rocket", "torpedo", "bomb", "ammunition"]):
                    event_type = "arms_deal"
                elif any(kw in weapon_lower for kw in ["tank", "apc", "vehicle"]):
                    event_type = "arms_deal"
                elif any(kw in weapon_lower for kw in ["radar", "sensor", "electronics", "communication"]):
                    event_type = "arms_deal"
                else:
                    event_type = "arms_transfer"
                
                rows_result.append({
                    "date": date_str,
                    "description": description,
                    "event_type": event_type,
                })
        
        print(f"[SIPRI] Parsed {len(rows_result)} records from {filepath.name}")
        return rows_result
        
    except Exception as e:
        print(f"[ERROR] Failed to parse CSV: {e}")
        return []


# ── Playwright download ───────────────────────────────────────────────────────

def download_sipri_export() -> Path | None:
    """
    Use Playwright to navigate the SIPRI export interface and download the full
    CSV export. First clicks "VIEW ON SCREEN" to populate data, then downloads.
    Returns the path to the downloaded file, or None.
    """
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True)
        page = ctx.new_page()

        try:
            # Navigate to the import/export page
            print(f"[SIPRI] Navigating to {SIPRI_EXPORT_URL}")
            page.goto(SIPRI_EXPORT_URL, wait_until="domcontentloaded", timeout=60_000)
            time.sleep(3)
            
            # First, click "VIEW ON SCREEN" to populate the table with all data
            print("[SIPRI] Clicking 'VIEW ON SCREEN' to populate data...")
            view_button = page.query_selector("button:has-text('VIEW ON SCREEN')")
            if view_button:
                view_button.click()
                # Wait for data to load
                page.wait_for_timeout(5000)
                print("[SIPRI] Data loaded")
            else:
                print("[WARN] Could not find 'VIEW ON SCREEN' button, proceeding to download anyway")
            
            time.sleep(2)
            
            # Now look for and click the DOWNLOAD AS CSV button
            print("[SIPRI] Clicking 'DOWNLOAD AS CSV' button...")
            download_button = None
            
            # Try multiple selectors for the download button
            for selector in [
                "button:has-text('DOWNLOAD AS CSV')",
                "button:has-text('Download as CSV')",
                "button",  # Fallback
            ]:
                try:
                    btn = page.query_selector(selector)
                    if btn and "DOWNLOAD" in (btn.text_content() or "").upper():
                        download_button = btn
                        break
                except:
                    pass
            
            if not download_button:
                print("[ERROR] Could not find DOWNLOAD AS CSV button")
                browser.close()
                return None
            
            # Click the download button and wait for download
            print("[SIPRI] Waiting for CSV download...")
            with page.expect_download(timeout=90_000) as dl_info:
                download_button.click()
                # Sometimes the browser needs a moment to start the download
                time.sleep(2)
            
            download = dl_info.value
            dest = DOWNLOAD_DIR / f"sipri_export_{int(time.time())}.csv"
            download.save_as(dest)
            print(f"[SIPRI] Downloaded: {dest}")
            browser.close()
            return dest
            
        except PWTimeout as e:
            print(f"[SIPRI] Timeout during export: {e}")
            browser.close()
            return None
        except Exception as e:
            print(f"[ERROR] Failed to download SIPRI export: {e}")
            import traceback
            traceback.print_exc()
            browser.close()
            return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 70)
    print("SIPRI Arms Transfers Database — full historical download")
    print("=" * 70)

    filepath = download_sipri_export()
    if filepath is None or not filepath.exists():
        print("\n" + "=" * 70)
        print("[!] SIPRI CSV DOWNLOAD ISSUE")
        print("=" * 70)
        print("\nThe SIPRI website uses JavaScript blob downloads which Playwright")
        print("cannot reliably capture. This is a known limitation with browser")
        print("automation tools.")
        print("\nWORKAROUND: Manual Download")
        print("-" * 70)
        print("1. Visit: https://armstransfers.sipri.org/ArmsTransfer/ImportExport")
        print("2. Click 'VIEW ON SCREEN' to show the data table")
        print("3. Click 'DOWNLOAD AS CSV'")
        print("4. Save the file to: data/sipri_downloads/sipri_export.csv")
        print("5. Run this script again - it will parse the CSV automatically")
        print("\nALTERNATIVE: Contact SIPRI")
        print("-" * 70)
        print("Email: atp@sipri.org")
        print("Request: API access or direct CSV export link")
        print("=" * 70 + "\n")
        return

    rows = parse_csv(filepath)
    if not rows:
        print("[ERROR] No rows parsed from SIPRI export.")
        return

    written = append_new_rows(rows)
    print(f"\nDone. {len(rows)} SIPRI records parsed, {written} new rows added to CSV.")


if __name__ == "__main__":
    main()
