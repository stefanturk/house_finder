#!/usr/bin/env python3
"""
debug_listing.py — Debug Claude scoring quality for a single Zillow listing.

Shows exactly what data is available for a listing, what we send to Claude,
and Claude's raw response with scores.

Usage:
    python3 debug_listing.py 24757025  # 6421 Shattuck Ave, Oakland
"""

import os
import sys
import json
import requests
import anthropic
from find_houses import (
    RAPIDAPI_HOST, RAPIDAPI_KEY, CLAUDE_SYSTEM, CLAUDE_PROMPT_TEMPLATE,
    _lot_to_sqft, _parse_listing, _HOME_TYPE_DISPLAY
)

REQUEST_TIMEOUT = 20


def fetch_listing_detail(zpid: str):
    """Try to fetch full property details including description from ZLLW API."""

    headers = {
        "Content-Type": "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }

    # Try the /search/byid endpoint (if it exists)
    endpoints = [
        f"https://{RAPIDAPI_HOST}/search/byid",
        f"https://{RAPIDAPI_HOST}/property/details",
        f"https://{RAPIDAPI_HOST}/property/{zpid}",
    ]

    for url in endpoints:
        try:
            resp = requests.get(url, params={"zpid": zpid}, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                print(f"✓ Fetched from {url}")
                return resp.json()
        except Exception as e:
            pass

    print(f"✗ Could not fetch property details for zpid {zpid} (description API not available)")
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 debug_listing.py <zpid>")
        print("Example: python3 debug_listing.py 24757025")
        sys.exit(1)

    zpid = sys.argv[1]

    if not RAPIDAPI_KEY:
        print("ERROR: RAPIDAPI_KEY env var not set.")
        sys.exit(1)

    print(f"\n{'='*70}")
    print(f"DEBUG: Zillow Listing {zpid}")
    print(f"{'='*70}\n")

    # Try to get full details
    detail_data = fetch_listing_detail(zpid)

    if detail_data:
        print(f"\nFull detail response:\n{json.dumps(detail_data, indent=2)[:500]}...\n")

    # Try to get from search API by fetching a small area
    print("Attempting to fetch from search/bycoordinates...")
    headers = {
        "Content-Type": "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }

    # Oakland center (rough)
    params = {
        "latitude": 37.805,
        "longitude": -122.271,
        "radius": 0.1,  # very small radius
        "listingStatus": "For_Sale",
        "propertyType": "SingleFamily,MultiFamily",
        "page": 1,
    }

    try:
        resp = requests.get(
            f"https://{RAPIDAPI_HOST}/search/bycoordinates",
            params=params, headers=headers, timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("searchResults", [])

            # Find the zpid we're looking for
            found = None
            for result in results:
                prop = result.get("property", {})
                if str(prop.get("zpid")) == str(zpid):
                    found = result
                    break

            if found:
                print(f"\nFound in search results:")
                listing = _parse_listing(found)
                print(json.dumps(listing, indent=2, default=str))
            else:
                print(f"✗ zpid {zpid} not found in search results")
    except Exception as e:
        print(f"Search error: {e}")

    # For now, use sample data if we need to debug scoring
    print(f"\n{'='*70}")
    print("CLAUDE PROMPT TEST")
    print(f"{'='*70}\n")

    # Build sample listing for Claude test
    sample = {
        "address": "6421 Shattuck Ave, Oakland, CA",
        "price": 600000,
        "home_type": "SingleFamily",
        "bedrooms": 3,
        "bathrooms": 1,
        "sqft": 1090,
        "year_built": 2007,
        "lot_sqft": 7841,
    }

    print("Sample listing data (what we'd send to Claude):")
    print(json.dumps(sample, indent=2))

    lot_sqft = sample["lot_sqft"]
    if lot_sqft:
        lot_str = f"{lot_sqft:,.0f} sqft"
        if lot_sqft >= 43560:
            lot_str += f" ({lot_sqft / 43560:.2f} acres)"
    else:
        lot_str = "unknown"

    prompt = CLAUDE_PROMPT_TEMPLATE.format(
        address=sample["address"],
        price=f"${sample['price']:,}",
        home_type=sample["home_type"],
        bedrooms=sample["bedrooms"],
        bathrooms=sample["bathrooms"],
        sqft=sample["sqft"],
        year_built=sample["year_built"],
        lot_size=lot_str,
    )

    print(f"\nPrompt sent to Claude:\n")
    print("─" * 70)
    print(prompt)
    print("─" * 70)

    print(f"\nCalling Claude Haiku...\n")

    client = anthropic.Anthropic()
    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=500,
            system=CLAUDE_SYSTEM,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        print("Raw Claude response:")
        print(raw)
        print()

        # Parse JSON
        try:
            parsed = json.loads(raw)
            print("Parsed scores:")
            print(json.dumps(parsed, indent=2))
        except json.JSONDecodeError:
            print("✗ Could not parse Claude response as JSON")

    except Exception as e:
        print(f"✗ Claude API error: {e}")


if __name__ == "__main__":
    main()
