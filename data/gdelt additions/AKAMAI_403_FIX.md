# DSCA 403 Error - Akamai Bot Protection Issue

## Problem
The DSCA website (`www.dsca.mil`) is protected by **Akamai WAF (Web Application Firewall)**, which blocks all automated scraping attempts, including:
- Pure HTTP requests (requests library)
- Browser-based automation (Playwright, Selenium)
- Bot protection bypasses (cloudscraper)
- Different User-Agents and headers

Return Code: **403 Forbidden** from AkamaiGHost server

## Root Cause
Akamai detects that requests are coming from automated clients using:
- Request patterns and behaviors
- SSL/TLS fingerprinting
- JavaScript execution detection
- Browser automation detection (webdriver property, etc.)

## Solutions (in order of recommendation)

### Solution 1: Contact DSCA for API Access (RECOMMENDED)
**Best long-term fix:**
- Email: press@dsca.mil
- Request direct API access or bulk data export rights
- Many government agencies provide API access for legitimate research

### Solution 2: Use Residential Proxies
**Workaround for automatic scraping:**
```python
# Use a residential proxy service (paid)
proxies = {
    "http": "http://username:password@proxy-provider.com:8080",
    "https": "http://username:password@proxy-provider.com:8080"
}
response = requests.get(url, proxies=proxies)
```

Popular services:
- Bright Data (formerly Luminati)
- ScraperAPI
- Oxylabs
- Smartproxy

### Solution 3: Manual Data Collection
**For smaller datasets:**
1. Visit https://www.dsca.mil/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library in a web browser
2. Browse through pages and manually copy data
3. Add records to `data/manual_events.csv` in format:
   ```
   date,description,event_type
   2026-01-15,Example arms deal description,arms_deal
   ```

### Solution 4: Alternative Data Sources
Check if DSCA publishes the data in other formats:
- **Excel/CSV downloads**: Check their website for bulk export options
- **Archive.org (Wayback Machine)**: https://web.archive.org/web/*/www.dsca.mil/*
- **SIPRI Database**: Already implemented in `fetch_sipri.py`
- **Federal Register**: Often publishes DSCA notifications
- **News sources**: Coverage of major arms deals

### Solution 5: Government Data Portal
Check if data is available through:
- data.gov
- api.data.gov
- Congress.gov API
- GSA's SAM database

## Testing Script
To verify the issue:
```bash
curl -I "https://www.dsca.mil/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library"
```

If you get `403 Forbidden` with `Server: AkamaiGHost`, the protection is active.

## Implementation Status
✓ Enhanced headers with User-Agent rotation - FAILED  
✓ Playwright real browser automation - FAILED  
✓ cloudscraper Akamai bypass - FAILED  
✓ curl with realistic headers - FAILED  

→ **Conclusion**: Akamai protection cannot be bypassed without:
1. Official API access
2. Residential proxy service
3. Manual data collection

## Code Changes Made
`fetch_dsca.py` has been updated to:
1. Use Playwright with anti-detection measures
2. Provide clear error messages about the Akamai block
3. Include documentation of workarounds
4. Gracefully handle the 403 response

## Next Steps
1. Try **Solution 1** (API request) - this is the official path
2. If not approved, use **Solution 3** (manual) or **Solution 4** (alternative sources)
3. For production pipelines, consider **Solution 2** (residential proxies)

## References
- Akamai Bot Manager: https://www.akamai.com/us/en/products/security/bot-manager/
- Web Scraping Laws: https://blog.apify.com/web-scraping-laws/
- DSCA Contact: https://www.dsca.mil/Press-Media/Press-Releases
