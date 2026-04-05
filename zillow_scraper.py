#!/usr/bin/env python3
"""
zillow_scraper.py — Scrape listing details from Zillow pages.

Extracts description and key property details from the listing page.
"""

import time
import requests
from bs4 import BeautifulSoup

REQUEST_TIMEOUT = 20

# More realistic user agent and headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.zillow.com/homes/for_sale/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
}


def scrape_listing(zpid: str, delay: float = 1.0) -> dict:
    """
    Scrape a Zillow listing page for description and details.

    Args:
        zpid: Zillow property ID
        delay: seconds to wait before requesting (rate limiting)

    Returns:
        {
            "description": "...",
            "facts": {...},  # HOA, lot size, etc if available
            "url": "...",
        }
    """
    time.sleep(delay)  # Be respectful to Zillow servers

    url = f"https://www.zillow.com/homedetails/{zpid}_zpid/"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        html = resp.text
    except requests.exceptions.HTTPError as e:
        return {
            "description": None,
            "facts": {},
            "error": f"HTTP {e.response.status_code}: {str(e)}",
        }
    except Exception as e:
        return {
            "description": None,
            "facts": {},
            "error": str(e),
        }

    soup = BeautifulSoup(html, "html.parser")

    # Try multiple selectors for the description
    description = None

    # Look for description in various common Zillow structures
    description_selectors = [
        ("div[data-testid='home-details-description']", "text"),
        ("div[class*='description']", "text"),
        ("p[class*='description']", "text"),
        ("div[class*='home-description']", "text"),
    ]

    for selector, method in description_selectors:
        elem = soup.select_one(selector)
        if elem:
            if method == "text":
                description = elem.get_text(strip=True)
                if description and len(description) > 20:
                    break

    # If still no description, try to find any div with substantial text
    if not description:
        for div in soup.find_all("div", {"class": lambda x: x and "details" in (x or "").lower()}):
            text = div.get_text(strip=True)
            if len(text) > 100 and len(text) < 2000:
                description = text
                break

    # Try to extract facts/features
    facts = {}
    facts_elem = soup.find("div", {"class": lambda x: x and "facts" in (x or "").lower()})
    if facts_elem:
        for row in facts_elem.find_all("div", {"class": lambda x: x and "row" in (x or "").lower()}):
            cells = row.find_all("div")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key and val:
                    facts[key] = val

    return {
        "description": description,
        "facts": facts,
        "url": url,
        "raw_html": html[:1000],  # first 1000 chars for debugging
    }


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Usage: python3 zillow_scraper.py <zpid>")
        sys.exit(1)

    zpid = sys.argv[1]
    print(f"Scraping Zillow listing {zpid}...\n")

    result = scrape_listing(zpid)
    print(f"Description: {result.get('description')[:200] if result.get('description') else 'NOT FOUND'}...\n")
    print(f"Facts: {json.dumps(result.get('facts'), indent=2)}\n")
    if result.get('error'):
        print(f"Error: {result['error']}")
