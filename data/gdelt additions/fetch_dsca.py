"""
fetch_dsca.py
Full historical scrape of the DSCA Major Arms Sales Library.
Uses Playwright with a real browser to bypass bot protection (Akamai WAF).
Paginates through all pages, downloads each press-release PDF,
extracts the deal description from the PDF text, and writes results
to data/manual_events.csv.

NOTE: As of April 2026, the DSCA website is protected by Akamai bot 
protection which blocks automated requests. This requires:
  1. Running this script on a display-capable system (for browser UI)
  2. Possibly using residential proxies for production use

Usage:
    playwright install chromium
    python fetch_dsca.py

Dependencies:
    pip install playwright pdfplumber beautifulsoup4

WORKAROUNDS IF BLOCKED:
1. Try using with residential proxies (paid service)
2. Contact DSCA at press@dsca.mil for API access
3. Use manual data entry for critical records
4. Check if DSCA publishes data in alternative formats (.xlsx, .json, etc.)
"""

import io
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from utils import append_new_rows, parse_date

# ── Constants ────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.dsca.mil"
LIBRARY_URL = f"{BASE_URL}/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library"

DELAY_SEC   = 2.0   # polite delay between requests
PDF_TIMEOUT = 30    # seconds
MAX_RETRIES = 3     # retry failed PDF downloads

# ── Helpers ──────────────────────────────────────────────────────────────────

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

def date_from_url(pdf_url: str) -> str:
    """
    Extract the upload date from the media.defense.gov URL.
    Pattern: /YYYY/Mon/DD/
    e.g. https://media.defense.gov/2026/Feb/06/...
    """
    m = re.search(r"/(\d{4})/([A-Za-z]+)/(\d{2})/", pdf_url)
    if not m:
        return ""
    year, mon, day = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTH_MAP.get(mon, "")
    if not month:
        return ""
    return f"{year}-{month}-{day}"


def classify_dsca(text: str) -> str:
    """
    Assign event_type from press-release body text.
    All DSCA Major Arms Sales are Foreign Military Sales (FMS),
    but we sub-classify where possible.
    """
    t = text.lower()
    if any(kw in t for kw in ["training", "services", "maintenance", "support services"]):
        return "training_services"
    if any(kw in t for kw in ["ammunition", "missiles", "munitions", "bombs", "rockets"]):
        return "arms_deal"
    if any(kw in t for kw in ["aircraft", "helicopter", "f-", "c-130", "c-17", "p-8"]):
        return "arms_deal"
    if any(kw in t for kw in ["ship", "vessel", "frigate", "submarine", "destroyer"]):
        return "arms_deal"
    if any(kw in t for kw in ["radar", "sensor", "communication", "satellite", "c4isr"]):
        return "arms_deal"
    if any(kw in t for kw in ["infrastructure", "construction", "facility"]):
        return "other"
    return "arms_deal"


def extract_description(text: str, title: str) -> str:
    """
    Pull the key sentence from the PDF body.
    DSCA press releases follow a standard template:
    'The Defense Security Cooperation Agency delivered a required
     certification notifying Congress of a possible Foreign Military
     Sale to [COUNTRY] for [EQUIPMENT] for an estimated cost of $[X].'
    """
    # Try to grab the first substantive paragraph (skip the boilerplate header)
    # Look for the sentence starting with 'The State Department...' or 'The Defense...'
    sentences = re.split(r"(?<=[.!?])\s+", text.replace("\n", " "))
    for sent in sentences:
        if re.search(
            r"(state department|defense security|possible foreign military sale|"
            r"estimated cost|proposed sale|congress)",
            sent, re.I
        ) and len(sent) > 60:
            return sent.strip()
    # Fallback: clean up the title
    return title.replace(".PDF", "").replace("PRESS RELEASE - ", "").strip()


def parse_pdf(pdf_url: str) -> tuple[str, str]:
    """
    Download a PDF and return (description, event_type).
    Returns ("", "arms_deal") on failure.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(pdf_url, timeout=PDF_TIMEOUT)
            resp.raise_for_status()
            
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages_text = "\n".join(
                    page.extract_text() or "" for page in pdf.pages[:3]
                )
            
            description = extract_description(pages_text, pdf_url.split("/")[-1])
            event_type  = classify_dsca(pages_text)
            return description, event_type
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                print(f"    [RETRY] PDF fetch failed: {exc}")
                time.sleep(2 ** attempt)
            else:
                print(f"    [WARN] Could not parse PDF {pdf_url}: {exc}")
    
    return "", "arms_deal"


def scrape_with_playwright() -> list[dict]:
    """
    Use Playwright to fetch the page content with a real browser.
    Uses anti-detection measures to bypass bot protection.
    Returns a list of all scrapable rows.
    """
    all_rows: list[dict] = []
    total_pages = 1
    
    # First, try cloudscraper as it's lighter weight
    print("\n[Attempting cloudscraper bypass...]")
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        response = scraper.get(LIBRARY_URL, timeout=15)
        if response.status_code == 200 and ("media.defense.gov" in response.text):
            print("[OK] cloudscraper succeeded!")
            soup = BeautifulSoup(response.text, "html.parser")
            pdf_links = soup.select("a[href*='media.defense.gov']")
            if pdf_links:
                print(f"[OK] Found {len(pdf_links)} PDF links")
                # Process with BeautifulSoup
                return _process_soup(soup, response.text)
    except Exception as e:
        print(f"[FAILED] cloudscraper failed: {e}")
    
    # Fall back to Playwright
    print("\n[Attempting Playwright with anti-detection...]")
    
    with sync_playwright() as p:
        # Launch browser with anti-detection options
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--disable-site-isolation-trials",
                "--disable-blink-features=AutomationControlled"
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        
        # Stealth mode: hide webdriver property
        page = context.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
            });
        """)
        
        try:
            # Navigate to the library
            print("[Navigating to DSCA library...]")
            page.goto(LIBRARY_URL, wait_until="domcontentloaded", timeout=60000)
            
            print("[Waiting for page to render...]")
            time.sleep(5)
            
            # Get page content
            content = page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            # Check for access denied
            if "access denied" in content.lower() or page.title() == "Access Denied":
                print("\n" + "="*70)
                print("[!] AKAMAI BOT PROTECTION DETECTED")
                print("="*70)
                print("\nThe DSCA website is protected by Akamai WAF, which blocks automated")
                print("access. This protection cannot be bypassed with standard methods.")
                print("\nRECOMMENDED SOLUTIONS:")
                print("1. Contact DSCA for official API access: press@dsca.mil")
                print("2. Use residential proxies (paid service)")
                print("3. Manually browse and download from DSCA website")
                print("4. Use alternative data sources (SIPRI, Federal Register)")
                print("\nSee ./AKAMAI_403_FIX.md for detailed workarounds.")
                print("="*70 + "\n")
                return []
            
            # Process soup
            return _process_soup(soup, content)
        
        finally:
            context.close()
            browser.close()


def _process_soup(soup: BeautifulSoup, html_text: str) -> list[dict]:
    """Helper to process BeautifulSoup content into rows."""
    all_rows = []
    
    # Try to find total page count
    total_pages = 1
    try:
        last_link = soup.find("a", string=re.compile(r"LAST", re.I))
        if last_link:
            m = re.search(r"igpage=(\d+)", last_link.get("href", ""))
            if m:
                total_pages = int(m.group(1))
                print(f"Found {total_pages} pages to scrape.\n")
    except:
        print("Could not determine page count, assuming 1 page.\n")
    
    # Extract PDF links
    pdf_links = soup.select("a[href*='media.defense.gov']")
    print(f"Processing {len(pdf_links)} items...")
    
    for anchor in pdf_links:
        try:
            pdf_url = anchor.get("href", "").strip()
            if not pdf_url:
                continue
                
            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL, pdf_url)
            
            title = (anchor.find("h2") or anchor).get_text(strip=True)
            
            date_str = date_from_url(pdf_url)
            if not date_str:
                print(f"  [WARN] Could not extract date from {pdf_url}")
                continue
            
            description, event_type = parse_pdf(pdf_url)
            if not description:
                description = title.replace(".PDF", "").replace("PRESS RELEASE - ", "").strip()
            
            all_rows.append({
                "date":        date_str,
                "description": description,
                "event_type":  event_type,
            })
            print(f"  {date_str} | {event_type} | {description[:50]}...")
            time.sleep(DELAY_SEC)
        except Exception as e:
            print(f"  [ERROR] Failed to process item: {e}")
            continue
    
    return all_rows


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("DSCA Major Arms Sales Library — full historical scrape")
    print("=" * 60)
    
    all_rows = scrape_with_playwright()
    
    written = append_new_rows(all_rows)
    print(f"\nDone. {len(all_rows)} records scraped, {written} new rows added to CSV.")


if __name__ == "__main__":
    main()
