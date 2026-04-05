#!/usr/bin/env python3
"""
test_rapidapi.py — Block 1 sanity check.

Run this BEFORE find_houses.py to verify:
  1. The API key works
  2. The bypolygon endpoint returns Oakland/Berkeley houses
  3. Field names match what the main script expects

Usage:
    python3 test_rapidapi.py
"""

import os
import json
import requests

RAPIDAPI_KEY  = os.environ.get("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = "zllw-working-api.p.rapidapi.com"

# Oakland Hills center
SEARCH_LAT    = 37.8268
SEARCH_LON    = -122.2380
SEARCH_RADIUS = 8  # miles

EXPECTED_FIELDS = ["zpid", "address", "price", "bedrooms", "bathrooms",
                   "livingArea", "yearBuilt", "lotSizeWithUnit", "propertyType"]


def main():
    if not RAPIDAPI_KEY:
        print("ERROR: RAPIDAPI_KEY env var not set. Run: export RAPIDAPI_KEY=your_key")
        return

    print(f"Testing ZLLW Working API — bycoordinates endpoint")
    print(f"Center: ({SEARCH_LAT}, {SEARCH_LON}), radius: {SEARCH_RADIUS}mi\n")

    headers = {
        "Content-Type":    "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key":  RAPIDAPI_KEY,
    }
    params = {
        "latitude":      SEARCH_LAT,
        "longitude":     SEARCH_LON,
        "radius":        SEARCH_RADIUS,
        "listingStatus": "For_Sale",
        "propertyType":  "SingleFamily,MultiFamily",
        "page":          1,
    }

    resp = requests.get(
        f"https://{RAPIDAPI_HOST}/search/bycoordinates",
        params=params, headers=headers, timeout=20
    )
    print(f"HTTP {resp.status_code}")

    if resp.status_code != 200:
        print(f"Response: {resp.text[:500]}")
        return

    data = resp.json()
    counts = data.get("resultsCount", {})
    pages  = data.get("pagesInfo", {})
    print(f"Total matching: {counts.get('totalMatchingCount')}")
    print(f"Pages: {pages.get('totalPages')}, per page: {pages.get('resultsPerPage')}")

    results = data.get("searchResults", [])
    print(f"This page: {len(results)} listings\n")

    if not results:
        print("No results — check params.")
        return

    # ── Show first 3 listings ─────────────────────────────────────────────────
    print("=" * 60)
    print("FIRST 3 LISTINGS")
    print("=" * 60)
    for i, item in enumerate(results[:3]):
        prop = item.get("property", {})
        addr = prop.get("address", {})
        print(f"\n--- Listing {i+1} ---")
        print(f"  Address  : {addr.get('streetAddress')}, {addr.get('city')}, {addr.get('state')}")
        print(f"  zpid     : {prop.get('zpid')}")
        print(f"  type     : {prop.get('propertyType')}")
        print(f"  price    : ${(prop.get('price') or {}).get('value', 0):,}")
        print(f"  beds/ba  : {prop.get('bedrooms')} / {prop.get('bathrooms')}")
        print(f"  sqft     : {prop.get('livingArea')}")
        print(f"  yearBuilt: {prop.get('yearBuilt')}")
        print(f"  lot      : {prop.get('lotSizeWithUnit')}")
        photo = ((prop.get("media") or {}).get("propertyPhotoLinks") or {})
        print(f"  photo    : {photo.get('highResolutionLink','')[:70]}")

    # ── Field check ───────────────────────────────────────────────────────────
    sample_prop = results[0].get("property", {})
    print(f"\n{'=' * 60}")
    print("FIELD CHECK")
    print("=" * 60)
    all_ok = True
    for field in EXPECTED_FIELDS:
        present = field in sample_prop
        status  = "OK" if present else "MISSING"
        if not present:
            all_ok = False
        print(f"  [{status}] {field}")

    if all_ok:
        print("\nAll expected fields present — good to proceed!")
    else:
        print("\nSome fields missing — check FIELD_MAP in funk_dungeon_finder.py")


if __name__ == "__main__":
    main()
