"""
fetch_dsca.py
Full historical scrape of the DSCA Major Arms Sales Library.
Paginates through all pages, downloads each press-release PDF,
extracts the deal description from the PDF text, and writes results
to data/manual_events.csv.

Usage:
    python python_scripts/fetch_dsca.py

Dependencies:
    pip install requests beautifulsoup4 pdfplumber
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

from utils import append_new_rows, parse_date

# ── Constants ────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.dsca.mil"
LIBRARY_URL = f"{BASE_URL}/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library"
HEADERS     = {"User-Agent": "Mozilla/5.0 (research scraper; contact: your@email.com)"}
DELAY_SEC   = 1.5   # polite delay between requests
PDF_TIMEOUT = 30    # seconds


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
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=PDF_TIMEOUT)
        resp.raise_for_status()
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            pages_text = "\n".join(
                page.extract_text() or "" for page in pdf.pages[:3]
            )
        description = extract_description(pages_text, pdf_url.split("/")[-1])
        event_type  = classify_dsca(pages_text)
        return description, event_type
    except Exception as exc:
        print(f"    [WARN] Could not parse PDF {pdf_url}: {exc}")
        return "", "arms_deal"


def scrape_page(page_num: int) -> list[dict]:
    """Scrape one paginated results page and return a list of row dicts."""
    url = LIBRARY_URL if page_num == 1 else f"{LIBRARY_URL}?igpage={page_num}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  [ERROR] Failed to fetch page {page_num}: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = []

    # Each press release is an <a> wrapping a card with an <h2> title.
    for anchor in soup.select("a[href*='media.defense.gov']"):
        pdf_url = anchor["href"].strip()
        title   = (anchor.find("h2") or anchor).get_text(strip=True)

        date_str = date_from_url(pdf_url)
        if not date_str:
            print(f"    [WARN] Could not extract date from {pdf_url}")
            continue

        description, event_type = parse_pdf(pdf_url)
        if not description:
            # Use filename as fallback description
            description = title.replace(".PDF", "").replace("PRESS RELEASE - ", "").strip()

        rows.append({
            "date":        date_str,
            "description": description,
            "event_type":  event_type,
        })
        print(f"    {date_str} | {event_type} | {description[:60]}...")
        time.sleep(DELAY_SEC)

    return rows


def get_total_pages() -> int:
    """Determine the total number of paginated pages."""
    try:
        resp = requests.get(LIBRARY_URL, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # The last page link looks like ?igpage=59
        last_link = soup.find("a", string=re.compile(r"LAST", re.I))
        if last_link:
            m = re.search(r"igpage=(\d+)", last_link.get("href", ""))
            if m:
                return int(m.group(1))
    except Exception as exc:
        print(f"[WARN] Could not determine page count: {exc}")
    return 1


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("DSCA Major Arms Sales Library — full historical scrape")
    print("=" * 60)

    total_pages = get_total_pages()
    print(f"Found {total_pages} pages to scrape.\n")

    all_rows: list[dict] = []
    for page in range(1, total_pages + 1):
        print(f"\n[Page {page}/{total_pages}]")
        rows = scrape_page(page)
        all_rows.extend(rows)
        time.sleep(DELAY_SEC)

    written = append_new_rows(all_rows)
    print(f"\nDone. {len(all_rows)} records scraped, {written} new rows added to CSV.")


if __name__ == "__main__":
    main()
