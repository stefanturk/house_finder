#!/usr/bin/env python3
"""
house_finder.py — Searches Zillow (via ZLLW Working API) for homes in the
Oakland/Berkeley area, scores them on 5 dimensions with Claude, and writes
qualifying results to Google Sheets.

Scores are 1–5 (5=best):
  Dungeon     — basement/bonus room/detached garage studio potential
  Backyard    — outdoor space / lot size
  Lighting    — natural indoor light
  Neighborhood — desirability (Rockridge=5, industrial=1)
  Turnkey     — move-in readiness (5=turnkey, 1=major reno)

Run any time: fetches fresh listings, skips already-processed ones (SQLite),
appends new qualifying rows to the sheet.

NOTE: The ZLLW search API does not return listing descriptions. Claude uses
structural data (year built, sqft, lot size, address) and its knowledge of
Bay Area housing stock to score each dimension.

Upgrade path for descriptions: subscribe to "Zillow Property Data" by APIlive
on RapidAPI and implement fetch_property_description(zpid) — see TODO below.

Usage:
    python3 house_finder.py
"""

import warnings

# Suppress warnings BEFORE importing anything else
warnings.filterwarnings("ignore", category=FutureWarning, message=".*Python version.*")
warnings.filterwarnings("ignore", message=".*urllib3.*only supports OpenSSL.*")

import os
import sys
import re
import json
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
import anthropic
from dotenv import load_dotenv
from email_digest import send_email

# Load environment variables from .env file (if it exists)
load_dotenv()

# ── Exceptions ────────────────────────────────────────────────────────────────

class QuotaExceededException(Exception):
    """Raised when RapidAPI quota is exceeded (HTTP 429)."""
    pass

# ── Config ────────────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── RapidAPI Keys ────────────────────────────────────────────────────────────

# PRIMARY: private-zillow (250 requests/month, free tier)
# Capabilities: /search/bypolygon (polygon search), /propimages (photos), /walk_transit_bike (scores)
RAPIDAPI_KEY_PRIVATE_ZILLOW  = os.environ.get("RAPIDAPI_KEY_PRIVATE_ZILLOW", "")
RAPIDAPI_HOST_PRIVATE_ZILLOW = "private-zillow.p.rapidapi.com"

# ENRICHMENT: US Property Market (600 requests/month)
# Capabilities: /property (descriptions by address), /photos (photos by zpid), /walkTransitBikeScores (scores by zpid)
RAPIDAPI_KEY_US_PROPERTY_MARKET  = os.environ.get("RAPIDAPI_KEY_US_PROPERTY_MARKET", "")
RAPIDAPI_HOST_US_PROPERTY_MARKET = "us-property-market1.p.rapidapi.com"

# FALLBACK: ZLLW Working API (limited quota, often hits 429)
# Capabilities: /search/bypolygon (polygon search)
RAPIDAPI_KEY_ZLLW  = os.environ.get("RAPIDAPI_KEY_ZLLW", "")
RAPIDAPI_HOST_ZLLW = "zllw-working-api.p.rapidapi.com"

# Search areas — load from polygons.json (created by GUI)
# Falls back to hardcoded polygon if file not found
POLYGONS_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "polygons.json")

# Fallback polygon (used if polygons.json not found)
_FALLBACK_COORDS = [
    [-122.28107884234528, 37.86430689180793],
    [-122.2775151935528, 37.843896501632074],
    [-122.24658191947155, 37.847923592652435],
    [-122.25216342051922, 37.867467513916694],
    [-122.28107884234528, 37.86430689180793],  # closes polygon
]


def _load_polygons() -> list:
    """
    Load polygon strings from polygons.json (created by GUI).
    Handles both new format {name, coords} and old format plain arrays.
    Falls back to hardcoded polygon if file not found or invalid.
    Returns list of polygon strings in "lat lon, lat lon, ..." format.
    """
    try:
        with open(POLYGONS_FILE) as f:
            data = json.load(f)
        if not data:
            raise ValueError("empty polygons.json")

        result = []
        for item in data:
            # Handle both new format {name, coords} and old format [lon, lat] array
            coords = item.get("coords") if isinstance(item, dict) else item
            if coords:
                result.append(", ".join(f"{lat} {lon}" for lon, lat in coords))

        if result:
            return result
        raise ValueError("no valid polygons")
    except Exception:
        # Fallback: return hardcoded polygon
        fallback_str = ", ".join(f"{lat} {lon}" for lon, lat in _FALLBACK_COORDS)
        return [fallback_str]

# ── Search filters (adjust these to change what properties are considered) ────────
# Buy mode
PRICE_MIN         = 500000   # Minimum price in dollars
PRICE_MAX         = 2000000  # Maximum price in dollars

# Rent mode
RENT_PRICE_MIN    = 3000   # Minimum rent per month
RENT_PRICE_MAX    = 6000   # Maximum rent per month

MIN_BEDS          = 2
MAX_BEDS          = 4      # reject if > 4 beds (Duplex/Triplex max, not 4+ unit buildings)
MIN_BATHS         = 1      # Minimum bathrooms
MAX_BATHS         = 3      # Maximum bathrooms
MAX_LISTING_AGE_DAYS = 0   # skip listings on market > this many days (0 = no limit)
MIN_DUNGEON_SCORE = 2      # minimum dungeon score to add to sheet
MAX_PAGES         = 1      # 1 API request per page; free tier = 500 req/month
MAX_JUDGEMENTS    = 10     # max Claude analyses per run (set to None for no limit)
REQUEST_TIMEOUT   = 20

# ── Hard pre-filters (no Claude cost, aggressive unsuitable listings filtering) ───
MIN_SQFT       = 900   # skip studios / very small units
MIN_YEAR_OLD   = 1978   # if newer than this AND small lot, skip (no basement likely)

# ── Listing mode: "For_Sale" or "For_Rent" (change to search rentals) ───────────
LISTING_STATUS = "For_Sale"   # "For_Sale" or "For_Rent"

# ── Sheet tabs (renamed from "House Finder" to "Buy Finder") ─────────────────
SKIPPED_SHEET_TAB = "Skipped Houses"

# ── Email config ──────────────────────────────────────────────────────────────
# When False: always attempt to send email (testing). When True: only during automated runs.
DAILY_RUN_MODE = False

SPREADSHEET_ID = "1MRKLmSjIkWUArbJwVgz9fgCSsh0WM7UoxPJCEeWe-ms"
SHEET_TAB      = "Buy Finder"
RENT_SHEET_TAB = "Rent Finder"
CREDS_FILE     = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "credentials.json")

# ── Claude config ─────────────────────────────────────────────────────────────

HAIKU_PRICE_INPUT_PER_MTOK  = 0.80
HAIKU_PRICE_OUTPUT_PER_MTOK = 4.00

CLAUDE_SYSTEM = (
    "You are a real estate assistant helping a musician find a home in Oakland/Berkeley, CA. "
    "Respond ONLY with valid JSON. No markdown fences, no text outside the JSON."
)

CLAUDE_PROMPT_TEMPLATE = """\
Rate this Oakland/Berkeley area home on 5 dimensions, each scored 1–5 (5 = best).
Use the listing description (if available) plus structural data and your
knowledge of Bay Area neighborhoods and housing stock.

Use the FULL 1–5 range. Scores of 1 and 2 are expected and correct for average listings.
Reserve 4–5 for genuinely standout properties. A 3 means "nothing special."

Address  : {address}
Price    : {price}
API Type : {home_type}
Beds/Ba  : {bedrooms}bd / {bathrooms}ba
Living   : {sqft} sqft
Year     : {year_built}
Lot      : {lot_size}
Listing Type: {listing_type}

Listing Description:
{description}

─── FIRST: Determine property type ────────────────────────────────────────────

The API classifies this as "{home_type}". Use these rules:

RULE 1 — If API says "Single Family", trust it. Output "Single".

RULE 2 — If API says "Multi Family", determine unit count:
  Use beds/baths/sqft to determine how many units:

  Output "Duplex" (2 units) if:
    - Beds 3–6, baths 2–4, sqft 1500–3500
    - Address contains hyphen range (e.g., "123-125 Main St")
    - sqft/beds ratio 400–700 (two side-by-side flats)

  Output "Triplex" (3 units) if:
    - Beds 4–8, baths 3–6, sqft 2500–5000
    - Address range spans 3 addresses (e.g., "2811-2815 Telegraph")
    - sqft/beds ratio 350–600

  Output "Quadruplex" (4 units) if:
    - Beds 6+, baths 4+, sqft 3500+
    - Address range spans 4 addresses (e.g., "1000-1004 Main St")
    - Or other strong signals of 4 separate units

  If it looks like 5+ units, still output "Quadruplex" (we reject 4+ anyway).

RULE 3 — Address range is a strong signal:
  "1000-1004 Main" (4 numbers) = Quadruplex or higher
  "2811-2815 Telegraph" (4 numbers) = Triplex or Quadruplex
  "1234-1236 Main" (3 numbers) = Duplex or Triplex
  "123-125 Main" (2 numbers) = Duplex

─── Score each dimension 1–5 ────────────────────────────────────────────────────────────

DUNGEON (music studio / bonus space potential):
  ⚠️  STRICTLY EVIDENCE-BASED. NO GUESSING.

  If description is empty/missing OR doesn't mention basement/bonus room/studio/detached garage/workshop → MUST SCORE 1.

  CRITICAL RULE: If description mentions ADU → score 1. (ADUs are rental units, not for owner)

  ONLY score 2+ if description explicitly mentions:
    - "basement" or "finished basement"
    - "bonus room" or "bonus space"
    - "studio" or "workshop"
    - "detached garage" or "separate structure"
    - similar music-studio-worthy space

  5 = description brags about basement, bonus room, or studio space
  4 = description mentions bonus room conversion or workshop space
  3 = description explicitly mentions basement or detached garage
  2 = description vaguely hints at "extra space" or "storage" but no explicit bonus/music room mention
  1 = description silent on bonus space, mentions ADU, or no description provided

BACKYARD (outdoor space):
  RULE: If description doesn't mention backyard, yard, patio, deck, or outdoor space → score 1.
  5 = description specifically highlights large backyard, deck, patio, or outdoor features
  4 = description mentions decent yard or outdoor space
  3 = description mentions some outdoor area or yard
  2 = description mentions patio/deck but minimal yard
  1 = description silent on backyard/yard/outdoor space OR mentions none

LIGHTING (natural indoor light):
  Score 4–5 ONLY if description explicitly mentions: "large windows", "bright",
    "sun-drenched", "fantastic lighting", "natural light", "skylights",
    "open and airy", "floor-to-ceiling windows"
  Score 1 if description mentions: "dark", "dim", "cave", "no windows", "depressing"
  Score 2 (default) if description is empty or has no lighting keywords
  Score 3 if description is generally positive about the home but no lighting keywords
  Do NOT score based on year built — year alone is unreliable for lighting

NEIGHBORHOOD (desirability / character):
  Transit score: {transit_score_line}
  Use transit score as supporting evidence: 80+ = excellent, 40-79 = moderate, <40 = poor.
  Transit <40 = penalize unless neighborhood name is top-tier (Rockridge, Elmwood, Montclair).

  5 = Rockridge, Temescal, College Ave, Montclair, Piedmont Ave, Elmwood, Claremont
  4 = North Oakland, Grand Ave, Maxwell Park, Glenview, Albany, El Cerrito hills
  3 = Mid-Oakland, central Berkeley flatlands, Alameda
  2 = West Oakland, East Oakland flatlands, San Leandro
  1 = Industrial corridors or very high crime areas

TURNKEY (move-in readiness):
  5 = higher price for the area + well-maintained era = likely turnkey
  4 = priced fairly, reasonable age — probably in good shape
  3 = average — could go either way without seeing it
  2 = older + below-market price = likely needs meaningful work
  1 = clear fixer — very low price for area, very old, or both

─── Return EXACTLY this JSON (with property_type filled in) ──────────────────────────

CRITICAL: property_type must be ONLY: "Single" or "Duplex" or "Triplex" or "Quadruplex"
  (We reject 4+ unit properties, so Quadruplex is the max we accept)

{{
  "property_type":      "Single" or "Duplex" or "Triplex" or "Quadruplex",
  "dungeon_score":      <1-5>,
  "backyard_score":     <1-5>,
  "lighting_score":     <1-5>,
  "neighborhood_score": <1-5>,
  "turnkey_score":      <1-5>,
  "reasoning":  "<1 sentence (max 20 words) naming the key strengths and weaknesses>",
  "concerns":   "<notable flags: HOA likely, flood zone, major arterial road, etc. — or null>"
}}\
"""

# ── Google Sheets ─────────────────────────────────────────────────────────────

SHEET_HEADERS = [
    "Address",            # A
    "Zillow Link",        # B
    "Price",         # C
    "Home Type",          # D
    "Beds",               # E
    "Baths",              # F
    "Overall",            # G
    "Dungeon",            # H
    "Backyard",           # I
    "Lighting",           # J
    "Neighborhood",       # K
    "Turnkey",            # L
    "Living Sqft",        # M
    "Lot Sqft",           # N
    "Reasoning",          # O
    "Concerns",           # P
    "Date Found",         # Q
    "Favorite",           # R
]

SKIPPED_HEADERS = SHEET_HEADERS + ["Mode", "Skip Reason"]


def _sheets_call(fn, retries=4, delay=5):
    """Retry a gspread call on transient 500/400 errors."""
    for attempt in range(retries):
        try:
            return fn()
        except gspread.exceptions.APIError as e:
            code = str(e)
            if attempt < retries - 1 and ("500" in code or "400" in code):
                print(f"  [sheets] Transient error, retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise


def _get_sheet(tab_name: str = None) -> tuple[gspread.Worksheet, gspread.Spreadsheet]:
    """Get or create a sheet tab. Defaults to SHEET_TAB (buy) or RENT_SHEET_TAB (rent).
    Returns (worksheet, spreadsheet) so we can apply data validation."""
    if tab_name is None:
        tab_name = RENT_SHEET_TAB if LISTING_STATUS == "For_Rent" else SHEET_TAB

    creds = Credentials.from_service_account_file(
        CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    existing = [ws.title for ws in spreadsheet.worksheets()]
    if tab_name not in existing:
        print(f"  Creating '{tab_name}' tab...")
        ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(SHEET_HEADERS))
        _sheets_call(lambda: ws.append_row(SHEET_HEADERS))

        # Add checkbox data validation to Favorite column (last column)
        fav_col_idx = len(SHEET_HEADERS) - 1  # 0-indexed
        try:
            spreadsheet.batch_update({"requests": [{
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": 1000,
                        "startColumnIndex": fav_col_idx,
                        "endColumnIndex": fav_col_idx + 1
                    },
                    "rule": {
                        "condition": {"type": "BOOLEAN"},
                        "strict": True,
                        "showCustomUi": True
                    }
                }
            }]})
        except Exception as e:
            print(f"  Warning: Could not set checkbox validation: {e}")
    else:
        ws = spreadsheet.worksheet(tab_name)
    return ws, spreadsheet


def _format_price(price) -> str:
    try:
        p = float(price)
        return f"${p / 1_000_000:.2f}M"
    except (TypeError, ValueError):
        return ""


_HOME_TYPE_DISPLAY = {
    "singlefamily": "Single",
    "multifamily": "Multi Family",
}


def _write_sheet_row(ws: gspread.Worksheet, listing: dict, analysis: dict) -> None:
    zpid     = listing["zpid"]
    lot_sqft = listing["lot_sqft"]
    lot_str  = f"{lot_sqft:,.0f}" if lot_sqft else ""

    # Map home type to display string (use Claude's inferred type if available)
    home_type_display = _HOME_TYPE_DISPLAY.get(
        (listing["home_type"] or "").lower(), listing["home_type"]
    )
    # Use Claude's property_type assessment if present (better discrimination of MultiFamily)
    property_type = analysis.get("property_type", home_type_display)

    # Calculate overall score (average of 5 scores)
    d = analysis.get("dungeon_score") or 0
    b = analysis.get("backyard_score") or 0
    l = analysis.get("lighting_score") or 0
    n = analysis.get("neighborhood_score") or 0
    t = analysis.get("turnkey_score") or 0
    overall = round((d + b + l + n + t) / 5, 1) if any([d, b, l, n, t]) else ""

    row = [
        listing["address"],                                          # A
        f"https://www.zillow.com/homedetails/{zpid}_zpid/",         # B
        _format_price(listing["price"]),                            # C
        property_type,                                               # D
        listing["bedrooms"],                                         # E
        listing["bathrooms"],                                        # F
        overall,                                                     # G
        d,                                                           # H
        b,                                                           # I
        l,                                                           # J
        n,                                                           # K
        t,                                                           # L
        listing["sqft"],                                             # M
        lot_str,                                                     # N
        analysis.get("reasoning", ""),                              # O
        analysis.get("concerns") or "",                             # P
        datetime.now().strftime("%Y-%m-%d %H:%M"),                  # Q
        "FALSE",                                                     # R (Favorite)
    ]
    # Insert at row 2 (right after header) so new listings appear at the top
    _sheets_call(lambda: ws.insert_row(row, index=2, value_input_option="USER_ENTERED"))


# ── Sheet-based deduplication (replaces SQLite) ───────────────────────────────

def _load_processed_zpids_from_sheets(ws_active: gspread.Worksheet, ws_skipped: gspread.Worksheet) -> set:
    """Load all zpids from both the active tab (Buy/Rent Finder) and Skipped tab.
    Returns union of zpids — these are already processed and should be skipped."""
    zpids = set()
    for ws in [ws_active, ws_skipped]:
        if ws is None:
            continue
        try:
            rows = ws.get_all_values()
            for row in rows[1:]:  # skip header
                if row and len(row) > 1:  # Check row exists and has at least 2 columns
                    # Zillow Link is column B (index 1), format: "https://www.zillow.com/homedetails/ZPID_zpid/"
                    link = row[1]
                    match = re.search(r'(\d+)_zpid', link)
                    if match:
                        zpids.add(match.group(1))
        except Exception:
            pass
    return zpids


def _get_skipped_sheet() -> tuple[gspread.Worksheet, gspread.Spreadsheet]:
    """Get or create the Skipped Houses tab (shared across buy and rent runs).
    Returns (worksheet, spreadsheet)."""
    creds = Credentials.from_service_account_file(
        CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    existing = [ws.title for ws in spreadsheet.worksheets()]

    if SKIPPED_SHEET_TAB not in existing:
        print(f"  Creating '{SKIPPED_SHEET_TAB}' tab...")
        ws = spreadsheet.add_worksheet(title=SKIPPED_SHEET_TAB, rows=2000, cols=len(SKIPPED_HEADERS))
        _sheets_call(lambda: ws.append_row(SKIPPED_HEADERS))

        # Add checkbox data validation to Favorite column (same index as main tab)
        fav_col_idx = len(SHEET_HEADERS) - 1  # "Favorite" is at this index in SKIPPED_HEADERS too
        try:
            spreadsheet.batch_update({"requests": [{
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": 2000,
                        "startColumnIndex": fav_col_idx,
                        "endColumnIndex": fav_col_idx + 1
                    },
                    "rule": {
                        "condition": {"type": "BOOLEAN"},
                        "strict": True,
                        "showCustomUi": True
                    }
                }
            }]})
        except Exception as e:
            print(f"  Warning: Could not set checkbox validation on Skipped tab: {e}")
    else:
        ws = spreadsheet.worksheet(SKIPPED_SHEET_TAB)
    return ws, spreadsheet


def _build_skipped_row(listing: dict, analysis: dict, mode_str: str, skip_reason: str) -> list:
    """Build a skipped listing row (does not write). Returns row data as list."""
    zpid = listing["zpid"]
    lot_sqft = listing["lot_sqft"]
    lot_str = f"{lot_sqft:,.0f}" if lot_sqft else ""

    # Scores: include if analysis exists, else empty
    d = analysis.get("dungeon_score", "") if analysis else ""
    b = analysis.get("backyard_score", "") if analysis else ""
    l = analysis.get("lighting_score", "") if analysis else ""
    n = analysis.get("neighborhood_score", "") if analysis else ""
    t = analysis.get("turnkey_score", "") if analysis else ""

    # Overall: only calculate if scores exist
    if analysis and any([d, b, l, n, t]):
        overall = round((d + b + l + n + t) / 5, 1) if all([d, b, l, n, t]) else ""
    else:
        overall = ""

    # Map home type
    home_type_display = _HOME_TYPE_DISPLAY.get(
        (listing["home_type"] or "").lower(), listing["home_type"]
    )
    property_type = analysis.get("property_type", home_type_display) if analysis else home_type_display

    row = [
        listing["address"],                                          # A
        f"https://www.zillow.com/homedetails/{zpid}_zpid/",         # B
        _format_price(listing["price"]),                            # C
        property_type,                                               # D
        listing["bedrooms"],                                         # E
        listing["bathrooms"],                                        # F
        overall,                                                     # G
        d,                                                           # H
        b,                                                           # I
        l,                                                           # J
        n,                                                           # K
        t,                                                           # L
        listing["sqft"],                                             # M
        lot_str,                                                     # N
        analysis.get("reasoning", "") if analysis else "",          # O
        analysis.get("concerns", "") if analysis else "",           # P
        datetime.now().strftime("%Y-%m-%d %H:%M"),                  # Q
        "FALSE",                                                     # R (Favorite)
        mode_str,                                                    # S (Mode)
        skip_reason,                                                 # T (Skip Reason)
    ]
    return row


def _write_skipped_rows_batch(ws: gspread.Worksheet, rows: list) -> None:
    """Write multiple skipped rows in a single batch API call."""
    if not rows:
        return
    try:
        _sheets_call(lambda: ws.append_rows(rows, value_input_option="USER_ENTERED"))
    except Exception as e:
        print(f"  Warning: batch write failed: {e}, retrying one-by-one...")
        # Fallback: write one at a time
        for row in rows:
            try:
                _sheets_call(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))
            except Exception as e2:
                print(f"    Skipped row write failed: {e2}")


def _load_pending_rows(ws_skipped: gspread.Worksheet) -> list:
    """Load rows marked 'Pending analysis — delete row to re-analyze' from Skipped Houses.
    Returns list of dicts: {row_index, row_data, zpid}."""
    pending = []
    try:
        rows = ws_skipped.get_all_values()
        # Find the Skip Reason column (last column in SKIPPED_HEADERS)
        skip_reason_idx = len(SKIPPED_HEADERS) - 1  # "Skip Reason" is the last column

        for idx, row in enumerate(rows[1:], start=2):  # start at row 2 (1-indexed, after header)
            if len(row) > skip_reason_idx and "Pending analysis" in row[skip_reason_idx]:
                # Extract zpid from Zillow Link (column B, index 1)
                zpid = None
                if len(row) > 1:
                    link = row[1]
                    match = re.search(r'(\d+)_zpid', link)
                    if match:
                        zpid = match.group(1)

                if zpid:
                    pending.append({
                        "row_index": idx,
                        "row_data": row,
                        "zpid": zpid
                    })
    except Exception:
        pass
    return pending


def _row_to_listing(row: list) -> dict:
    """Convert a skipped row back into a listing dict for re-analysis.
    Reconstructs the listing structure from sheet row data."""
    # Row structure matches SHEET_HEADERS (first 17 columns)
    # A=address, B=zillow_link, C=price, D=home_type, E=beds, F=baths,
    # G=overall, H=dungeon, I=backyard, J=lighting, K=neighborhood, L=turnkey,
    # M=sqft, N=lot_sqft, O=reasoning, P=concerns, Q=date_found, R=favorite

    zpid = None
    if len(row) > 1:
        match = re.search(r'(\d+)_zpid', row[1])
        if match:
            zpid = match.group(1)

    try:
        price = float(row[2].replace('$', '').replace(',', '')) if len(row) > 2 and row[2] else None
    except (ValueError, AttributeError):
        price = None

    # Parse lot_sqft, handling comma-formatted numbers
    lot_sqft_val = None
    if len(row) > 13 and row[13]:
        try:
            lot_sqft_val = float(row[13].replace(',', ''))
        except (ValueError, AttributeError):
            lot_sqft_val = None

    listing = {
        "zpid": zpid or "",
        "address": row[0] if len(row) > 0 else "",
        "price": price,
        "home_type": row[3] if len(row) > 3 else "",
        "bedrooms": int(row[4]) if len(row) > 4 and row[4] and str(row[4]).isdigit() else None,
        "bathrooms": int(row[5]) if len(row) > 5 and row[5] and str(row[5]).isdigit() else None,
        "sqft": int(row[12]) if len(row) > 12 and row[12] and str(row[12]).isdigit() else None,
        "lot_sqft": _lot_to_sqft({"lotSize": lot_sqft_val}) if lot_sqft_val else None,
        "year_built": None,  # Not stored in skipped rows, but OK for re-analysis
        "listing_type": "",
    }
    return listing


def _delete_row_by_index(ws: gspread.Worksheet, row_index: int) -> None:
    """Delete a row from the worksheet by 1-indexed row number."""
    try:
        _sheets_call(lambda: ws.delete_rows(row_index))
    except Exception as e:
        print(f"  Warning: Could not delete row {row_index}: {e}")


# ── RapidAPI ──────────────────────────────────────────────────────────────────

def _lot_to_sqft(lot_obj: dict):
    """Normalize lot size to square feet."""
    if not lot_obj:
        return None
    size = lot_obj.get("lotSize")
    unit = (lot_obj.get("lotSizeUnit") or "").lower()
    try:
        size = float(size)
    except (TypeError, ValueError):
        return None
    return size * 43560 if "acre" in unit else size


def _parse_listing(raw: dict):
    """Flatten the nested ZLLW API response. Returns None if essential fields missing."""
    prop = raw.get("property", {})
    if not prop:
        return None
    zpid = prop.get("zpid")
    if not zpid:
        return None

    addr_obj = prop.get("address", {})
    street   = addr_obj.get("streetAddress", "")
    city     = addr_obj.get("city", "")
    state    = addr_obj.get("state", "CA")
    address  = f"{street}, {city}, {state}".strip(", ")

    price_obj = prop.get("price", {})
    price     = price_obj.get("value") if isinstance(price_obj, dict) else None

    return {
        "zpid":       str(zpid),
        "address":    address,
        "city":       city,
        "price":      price or 0,
        "bedrooms":   prop.get("bedrooms", ""),
        "bathrooms":  prop.get("bathrooms", ""),
        "sqft":       prop.get("livingArea", ""),
        "year_built": prop.get("yearBuilt", ""),
        "lot_sqft":   _lot_to_sqft(prop.get("lotSizeWithUnit")),
        "home_type":  prop.get("propertyType", ""),
        "days_on_zillow": prop.get("daysOnZillow", ""),
        "status_type": prop.get("homeStatus") or prop.get("statusType") or prop.get("listingSubStatus") or "",
        "listing_type": None,  # Will be filled in by _fetch_property_description
    }


def _normalize_zillow56_result(raw: dict) -> dict:
    """
    Adapter: convert Zillow56 search result format to ZLLW Working API format.
    Zillow56 returns flat fields; ZLLW Working wraps them in a 'property' object.
    """
    return {
        "property": {
            "zpid": raw.get("zpid"),
            "address": {
                "streetAddress": raw.get("streetAddress", ""),
                "city": raw.get("city", ""),
                "state": raw.get("state", "CA"),
            },
            "price": {"value": raw.get("price")},
            "bedrooms": raw.get("bedrooms"),
            "bathrooms": raw.get("bathrooms"),
            "livingArea": raw.get("livingArea"),
            "yearBuilt": raw.get("yearBuilt"),
            "lotSizeWithUnit": {
                "lotSize": raw.get("lotSize"),
                "lotSizeUnit": raw.get("lotSizeUnit", "sqft"),
            } if raw.get("lotSize") else None,
            "propertyType": raw.get("homeType", ""),
            "daysOnZillow": raw.get("daysOnZillow"),
        }
    }


def _fetch_photo(zpid: str) -> Optional[str]:
    """Fetch the first photo URL for a property via private-zillow. Returns None on failure."""
    if not RAPIDAPI_KEY_PRIVATE_ZILLOW:
        return None
    try:
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-host": RAPIDAPI_HOST_PRIVATE_ZILLOW,
            "x-rapidapi-key": RAPIDAPI_KEY_PRIVATE_ZILLOW,
        }
        resp = requests.get(
            f"https://{RAPIDAPI_HOST_PRIVATE_ZILLOW}/propimages",
            params={"byzpid": zpid},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        # Try common response formats
        images = data if isinstance(data, list) else data.get("images", data.get("photos", []))
        if images:
            if isinstance(images[0], str):
                return images[0]
            elif isinstance(images[0], dict):
                return images[0].get("url") or images[0].get("src") or images[0].get("href")
    except Exception:
        pass
    return None


def _fetch_walk_scores(zpid: str) -> Optional[dict]:
    """Fetch walk/transit/bike scores via private-zillow. Returns dict or None."""
    if not RAPIDAPI_KEY_PRIVATE_ZILLOW:
        return None
    try:
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-host": RAPIDAPI_HOST_PRIVATE_ZILLOW,
            "x-rapidapi-key": RAPIDAPI_KEY_PRIVATE_ZILLOW,
        }
        resp = requests.get(
            f"https://{RAPIDAPI_HOST_PRIVATE_ZILLOW}/walk_transit_bike",
            params={"byzpid": zpid},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        # Normalize to {walk, transit, bike} ints
        return {
            "walk":    int(data.get("walkScore",    data.get("walk_score",    0))),
            "transit": int(data.get("transitScore", data.get("transit_score", 0))),
            "bike":    int(data.get("bikeScore",    data.get("bike_score",    0))),
        }
    except Exception:
        pass
    return None


def _fetch_page(page: int = 1, polygon: str = "", api_stats: dict = None) -> tuple[list, int, dict]:
    """Fetch one page for a given polygon. Returns (parsed_listings, total_pages, api_calls_made).
    Retries with backup API on primary failure (except 429 quota)."""
    if api_stats is None:
        api_stats = {}

    if not polygon:
        return [], 0, api_stats
    if page == 1:
        print(f"    [polygon] {polygon[:50]}...")

    # Use rent prices in rent mode, buy prices in buy mode
    if LISTING_STATUS == "For_Rent":
        min_price, max_price = RENT_PRICE_MIN, RENT_PRICE_MAX
    else:
        min_price, max_price = PRICE_MIN, PRICE_MAX

    params = {
        "polygon":       polygon,
        "listingStatus": LISTING_STATUS,
        "propertyType":  "SingleFamily,MultiFamily",
        "page":          page,
    }
    if min_price is not None:
        params["minPrice"] = min_price
    if max_price is not None:
        params["maxPrice"] = max_price
    if MIN_BEDS:
        params["minBeds"] = MIN_BEDS

    def _attempt(host, key):
        """Single attempt against a specific API key/host. Returns (status_code, response)."""
        h = {
            "Content-Type":    "application/json",
            "x-rapidapi-host": host,
            "x-rapidapi-key":  key,
        }
        try:
            r = requests.get(
                f"https://{host}/search/bypolygon",
                params=params, headers=h, timeout=REQUEST_TIMEOUT
            )
            return r.status_code, r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"  [api] Network error on {host}: {e}")
            return None, None

    # Try primary API (private-zillow)
    status, resp = _attempt(RAPIDAPI_HOST_PRIVATE_ZILLOW, RAPIDAPI_KEY_PRIVATE_ZILLOW)
    use_adapter = False
    api_stats["private-zillow"] = api_stats.get("private-zillow", 0) + 1

    # Retry with BACKUP2 (ZLLW Working) on failure
    if status != 200 and RAPIDAPI_KEY_ZLLW:
        if status == 429:
            print(f"  [api] Primary quota exceeded (429), trying ZLLW backup...")
        elif status:
            print(f"  [api] Primary returned {status}, trying ZLLW backup...")
        else:
            print(f"  [api] Primary network error, trying ZLLW backup...")
        status, resp = _attempt(RAPIDAPI_HOST_ZLLW, RAPIDAPI_KEY_ZLLW)
        use_adapter = False  # ZLLW uses same /search/bypolygon endpoint, no adapter needed
        api_stats["zllw"] = api_stats.get("zllw", 0) + 1

    # Final fallback to BACKUP (only if manually set to something, with adapter)
    if status != 200 and RAPIDAPI_KEY_US_PROPERTY_MARKET and RAPIDAPI_HOST_US_PROPERTY_MARKET != "us-property-market1.p.rapidapi.com":
        print(f"  [api] Backups exhausted, trying final fallback...")
        status, resp = _attempt(RAPIDAPI_HOST_US_PROPERTY_MARKET, RAPIDAPI_KEY_US_PROPERTY_MARKET)
        use_adapter = True  # adapter if needed
        api_stats["backup"] = api_stats.get("backup", 0) + 1

    # If both APIs fail, raise or return empty
    if status == 429:
        raise QuotaExceededException(resp.text if resp else "quota exceeded")
    if status != 200:
        if status is not None:
            print(f"  [api] HTTP {status}: {resp.text[:200] if resp else 'no response'}")
        return [], 0, api_stats

    try:
        data = resp.json()
    except Exception:
        print(f"  [api] Non-JSON response")
        return [], 0, api_stats

    raw_results = data.get("searchResults", [])

    # If using a backup API that needs format normalization (currently not used — Zillow56 adapter is dead code)
    if use_adapter:
        if not raw_results:
            raw_results = data.get("results", data.get("props", []))
        raw_results = [_normalize_zillow56_result(r) for r in raw_results]

    listings    = [l for l in (_parse_listing(r) for r in raw_results) if l]
    total_pages = int((data.get("pagesInfo") or {}).get("totalPages") or 1)

    return listings, total_pages, api_stats


# ── Pre-filter ────────────────────────────────────────────────────────────────

def _passes_prefilter(listing: dict) -> tuple[bool, str]:
    """Hard pre-filters — skip if fails any of these checks."""
    # Validate listing status matches requested mode (API doesn't always honor filter)
    status_type = (listing.get("status_type") or "").lower()
    if LISTING_STATUS == "For_Rent":
        # In rent mode: reject if status suggests it's for sale
        if "forsale" in status_type or status_type == "for_sale":
            return False, f"listing status '{status_type}' is for sale, not rent"
    else:
        # In buy mode: reject if status suggests it's for rent
        if "forrent" in status_type or status_type == "for_rent":
            return False, f"listing status '{status_type}' is for rent, not sale"

    # Check price (use rent prices if in rent mode, buy prices otherwise)
    try:
        price = int(listing.get("price") or 0)

        # For rent: reject $0 price immediately (indicates missing data from API)
        if LISTING_STATUS == "For_Rent" and price == 0:
            return False, "price missing (API returned $0)"

        if LISTING_STATUS == "For_Rent":
            min_price, max_price = RENT_PRICE_MIN, RENT_PRICE_MAX
        else:
            min_price, max_price = PRICE_MIN, PRICE_MAX

        if min_price and price < min_price:
            return False, f"price ${price:,} < ${min_price:,}"
        if max_price and price > max_price:
            return False, f"price ${price:,} > ${max_price:,}"
    except (ValueError, TypeError):
        pass

    # Strict allowlist: reject anything that's not SingleFamily or MultiFamily
    # This catches condos, apartments, townhouses, manufactured, unknown types, AND empty types
    home_type = (listing.get("home_type") or "").lower()
    allowed_types = {"singlefamily", "multifamily"}

    # Explicit blocklist for common non-qualifying types (even if they slip through as MultiFamily)
    blocked_types = {"apartment", "condo", "townhouse", "townhome", "manufactured", "mobile"}
    if any(blocked in home_type for blocked in blocked_types):
        return False, f"property type '{listing['home_type'] or 'unknown'}' is explicitly blocked"

    if not any(t in home_type for t in allowed_types):
        return False, f"property type '{listing['home_type'] or 'unknown'}' not singleFamily/multiFamily"

    # Check sqft
    try:
        if listing["sqft"] and int(listing["sqft"]) < MIN_SQFT:
            return False, f"sqft {listing['sqft']} < {MIN_SQFT}"
    except (ValueError, TypeError):
        pass

    # Check beds (max limit)
    try:
        if listing["bedrooms"] and int(listing["bedrooms"]) > MAX_BEDS:
            return False, f"beds {listing['bedrooms']} > {MAX_BEDS}"
    except (ValueError, TypeError):
        pass

    # Check baths (max limit)
    if MAX_BATHS:
        try:
            if listing["bathrooms"] and float(listing["bathrooms"]) > MAX_BATHS:
                return False, f"baths {listing['bathrooms']} > {MAX_BATHS}"
        except (ValueError, TypeError):
            pass

    # Check baths (min limit)
    if MIN_BATHS:
        try:
            if listing["bathrooms"] and float(listing["bathrooms"]) < MIN_BATHS:
                return False, f"baths {listing['bathrooms']} < {MIN_BATHS}"
        except (ValueError, TypeError):
            pass

    # Check listing age (days on market) — skip if MAX_LISTING_AGE_DAYS is 0 (no limit)
    if MAX_LISTING_AGE_DAYS and MAX_LISTING_AGE_DAYS > 0:
        try:
            days = int(listing.get("days_on_zillow") or 0)
            if days > MAX_LISTING_AGE_DAYS:
                return False, f"on market {days} days > {MAX_LISTING_AGE_DAYS} day limit"
        except (ValueError, TypeError):
            pass

    # Check age + lot size combo: new construction on small lot = no basement likely
    year = listing["year_built"]
    lot = listing["lot_sqft"]
    try:
        if year and int(year) > MIN_YEAR_OLD and (not lot or lot < 5000):
            return False, f"post-{MIN_YEAR_OLD} + small lot ({lot} sqft) = no basement"
    except (ValueError, TypeError):
        pass

    return True, ""


def _is_sparse(listing: dict) -> bool:
    """Check if listing has insufficient data (foreclosure/pre-foreclosure)."""
    sparse_fields = ("sqft", "year_built", "lot_sqft", "bedrooms", "bathrooms")
    missing = sum(1 for f in sparse_fields if not listing.get(f))
    return missing >= 2


# ── Property Details (descriptions) ───────────────────────────────────────────

def _fetch_property_description(address: str, zpid: str = None) -> dict:
    """Fetch full property details from US Property Market /property endpoint (600 calls/month quota).
    Falls back to web scraping via zillow_scraper if API returns no description."""
    description = None
    listing_type = None

    try:
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-host": RAPIDAPI_HOST_US_PROPERTY_MARKET,
            "x-rapidapi-key": RAPIDAPI_KEY_US_PROPERTY_MARKET,
        }
        resp = requests.get(
            f"https://{RAPIDAPI_HOST_US_PROPERTY_MARKET}/property",
            params={"address": address},
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            data = resp.json()
            description = data.get("topLevelDescription") or data.get("description")
            listing_type = data.get("listingType")
    except Exception:
        pass

    # Fallback: web scrape if API returned nothing and we have a zpid
    if not description and zpid:
        try:
            from zillow_scraper import scrape_listing
            scraped = scrape_listing(str(zpid))
            description = scraped.get("description")
        except Exception:
            pass

    return {
        "description": description,
        "bedrooms": None,
        "bathrooms": None,
        "sqft": None,
        "listing_type": listing_type,
    }


# ── Claude ────────────────────────────────────────────────────────────────────

def _apply_lighting_override(parsed: dict, description: str) -> None:
    """Apply evidence-based lighting score enforcement (mirrors dungeon override pattern)."""
    LIGHTING_POSITIVE = {
        "large windows", "bright", "sun-drenched", "fantastic lighting",
        "natural light", "skylights", "open and airy", "floor-to-ceiling windows",
        "sunlit", "sun-filled", "sun-filled", "light-filled", "lots of light",
        "abundant light", "cheerful", "airy"
    }
    LIGHTING_NEGATIVE = {"dark", "dim", "cave", "no windows", "depressing"}

    desc = (description or "").lower()
    has_desc = description and "(No listing description available" not in description
    has_pos = any(kw in desc for kw in LIGHTING_POSITIVE)
    has_neg = any(kw in desc for kw in LIGHTING_NEGATIVE)
    has_any = has_pos or has_neg

    current = parsed.get("lighting_score", 2)

    if not has_desc or not has_any:
        parsed["lighting_score"] = min(current, 2)  # cap at 2
    elif has_pos:
        parsed["lighting_score"] = max(current, 4)  # floor of 4
    elif has_neg:
        parsed["lighting_score"] = 1  # force to 1


def _analyze_with_claude(listing: dict, token_totals: dict, description: str = None, transit_score: Optional[int] = None):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    try:
        price_str = f"${int(listing['price']):,}"
    except (ValueError, TypeError):
        price_str = str(listing["price"])

    lot_sqft = listing["lot_sqft"]
    if lot_sqft:
        lot_str = f"{lot_sqft:,.0f} sqft"
        if lot_sqft >= 43560:
            lot_str += f" ({lot_sqft / 43560:.2f} acres)"
    else:
        lot_str = "unknown"

    # If no description provided, add a note
    if not description:
        description = "(No listing description available — use structural data and neighborhood knowledge)"

    # Format transit score line for prompt
    transit_score_line = f"{transit_score}/100" if transit_score is not None else "unavailable"

    prompt = CLAUDE_PROMPT_TEMPLATE.format(
        address=listing["address"],
        price=price_str,
        home_type=listing["home_type"],
        bedrooms=listing["bedrooms"] or "?",
        bathrooms=listing["bathrooms"] or "?",
        sqft=listing["sqft"] or "?",
        year_built=listing["year_built"] or "unknown",
        lot_size=lot_str,
        listing_type=listing.get("listing_type", "Unknown"),
        description=description,
        transit_score_line=transit_score_line,
    )

    print(f"    → Sending to Claude...")
    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=500,
            system=CLAUDE_SYSTEM,
            messages=[{"role": "user", "content": prompt}]
        )
        token_totals["input"]  += response.usage.input_tokens
        token_totals["output"] += response.usage.output_tokens
        raw = response.content[0].text.strip()
    except Exception as e:
        print(f"    ✗ Claude API error: {e}")
        return None

    try:
        parsed = json.loads(raw)

        # ── Post-processing: Enforce strict dungeon evidence rules ──────────────
        # If no description or description doesn't mention dungeon features, force score to 1
        dungeon_keywords = {"basement", "bonus room", "bonus space", "studio", "workshop",
                           "detached garage", "detached structure", "separate garage", "in-law",
                           "art room", "creative room", "craft room", "music room",
                           "rehearsal", "recording", "home studio", "flex room", "den",
                           "office", "media room", "game room"}

        description_lower = (description or "").lower()
        has_description = description and "(No listing description available" not in description
        has_dungeon_mention = any(kw in description_lower for kw in dungeon_keywords)

        if not has_description or (has_description and not has_dungeon_mention):
            # No description or no mention of dungeon features → force score to 1
            parsed["dungeon_score"] = 1
            parsed["reasoning"] = "No dungeon features mentioned in listing description."

        # ── Post-processing: Enforce evidence-based lighting rules ────────────────
        _apply_lighting_override(parsed, description)

        # Log the response
        d = parsed.get("dungeon_score", "?")
        b = parsed.get("backyard_score", "?")
        l = parsed.get("lighting_score", "?")
        n = parsed.get("neighborhood_score", "?")
        turnkey = parsed.get("turnkey_score", "?")
        print(f"    ← Dungeon:{d} Backyard:{b} Light:{l} Hood:{n} Turnkey:{turnkey}")
        return parsed
    except json.JSONDecodeError:
        raw_clean = re.sub(r"```(?:json)?", "", raw).strip()
        match = re.search(r"\{.*\}", raw_clean, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group())

                # ── Post-processing: Enforce strict dungeon evidence rules ──────────────
                dungeon_keywords = {"basement", "bonus room", "bonus space", "studio", "workshop",
                                   "detached garage", "detached structure", "separate garage", "in-law",
                                   "art room", "creative room", "craft room", "music room",
                                   "rehearsal", "recording", "home studio", "flex room", "den",
                                   "office", "media room", "game room"}

                description_lower = (description or "").lower()
                has_description = description and "(No listing description available" not in description
                has_dungeon_mention = any(kw in description_lower for kw in dungeon_keywords)

                if not has_description or (has_description and not has_dungeon_mention):
                    parsed["dungeon_score"] = 1
                    parsed["reasoning"] = "No dungeon features mentioned in listing description."

                # ── Post-processing: Enforce evidence-based lighting rules ────────────────
                _apply_lighting_override(parsed, description)

                d = parsed.get("dungeon_score", "?")
                b = parsed.get("backyard_score", "?")
                l = parsed.get("lighting_score", "?")
                n = parsed.get("neighborhood_score", "?")
                turnkey = parsed.get("turnkey_score", "?")
                print(f"    ← Dungeon:{d} Backyard:{b} Light:{l} Hood:{n} Turnkey:{turnkey} [parsed]")
                return parsed
            except json.JSONDecodeError:
                pass
        print(f"    ✗ Could not parse JSON. Raw: {raw[:200]}")
        return None


# ── TODO: Description upgrade ─────────────────────────────────────────────────
# To make all 5 scores much more accurate, subscribe to "Zillow Property Data"
# by APIlive on RapidAPI and implement:
#
# def _fetch_description(zpid: str) -> str | None:
#     resp = requests.get(
#         "https://zillow-property-data.p.rapidapi.com/property",
#         params={"zpid": zpid},
#         headers={"x-rapidapi-host": "zillow-property-data.p.rapidapi.com",
#                  "x-rapidapi-key": RAPIDAPI_KEY},
#         timeout=REQUEST_TIMEOUT,
#     )
#     if resp.status_code == 200:
#         data = resp.json()
#         return data.get("description") or data.get("homeDescription")
#     return None
#
# Then add the description to the Claude prompt template.
# ─────────────────────────────────────────────────────────────────────────────


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not RAPIDAPI_KEY_PRIVATE_ZILLOW:
        print("ERROR: RAPIDAPI_KEY_PRIVATE_ZILLOW env var not set.")
        print("Run: export RAPIDAPI_KEY_PRIVATE_ZILLOW=your_key   (or add to ~/.zshrc)")
        return

    # ── 1. Connect Sheets ─────────────────────────────────────────────────────
    print("\nConnecting to Google Sheets...")
    try:
        ws, spreadsheet = _get_sheet()
        ws_skipped, _ = _get_skipped_sheet()
        # Also load the opposite mode tab (for cross-mode deduplication)
        # If in rent mode, load buy tab; if in buy mode, load rent tab
        opposite_tab = RENT_SHEET_TAB if LISTING_STATUS == "ForSale" else SHEET_TAB
        try:
            ws_opposite = spreadsheet.worksheet(opposite_tab)
        except gspread.exceptions.WorksheetNotFound:
            ws_opposite = None  # Opposite tab doesn't exist yet
        print(f"  Connected to '{SHEET_TAB}' and '{SKIPPED_SHEET_TAB}'.")
    except Exception as e:
        print(f"  Sheets error: {e}")
        return

    # Buffer for batching skipped row writes (to avoid quota exhaustion)
    skipped_rows_buffer = []
    BATCH_SIZE = 20  # Flush buffer every N rows

    def _flush_skipped_buffer():
        """Flush accumulated skipped rows to sheet in one batch."""
        nonlocal skipped_rows_buffer
        if skipped_rows_buffer:
            _write_skipped_rows_batch(ws_skipped, skipped_rows_buffer)
            skipped_rows_buffer = []

    # ── 2. Load processed zpids from both modes + skipped tabs ──────────────────
    print("\nLoading processed listings...")
    # Load from active tab, opposite tab, and skipped (to prevent cross-mode duplicates)
    processed = _load_processed_zpids_from_sheets(ws, ws_skipped)
    if ws_opposite:
        opposite_processed = _load_processed_zpids_from_sheets(ws_opposite, None)
        processed = processed.union(opposite_processed)
    print(f"  {len(processed)} already processed (across all tabs)")

    # ── 3. Fetch listings ─────────────────────────────────────────────────────
    polygons = _load_polygons()
    print(f"\nFetching listings ({len(polygons)} polygon{'s' if len(polygons) != 1 else ''})...")
    all_listings = []
    seen_zpids_this_run = set()
    api_call_stats = {}
    use_all_pages = "--all-pages" in sys.argv
    for poly_idx, polygon in enumerate(polygons, 1):
        print(f"  Polygon {poly_idx}/{len(polygons)}...")
        # Determine max pages: use all pages if --all-pages flag, else respect MAX_PAGES
        effective_max_pages = 1000 if use_all_pages else MAX_PAGES
        for page in range(1, effective_max_pages + 1):
            print(f"    Page {page}...", end=" ", flush=True)
            try:
                listings, total_pages, api_call_stats = _fetch_page(page, polygon, api_call_stats)
            except QuotaExceededException:
                now = datetime.now()
                reset = datetime(
                    now.year + (1 if now.month == 12 else 0),
                    now.month % 12 + 1,
                    1
                )
                days_left = (reset.date() - now.date()).days
                print(f"\n  API quota exceeded.")
                print(f"  Resets ~{reset.strftime('%B 1')} ({days_left} day(s) from now).")
                print(f"  Tip: set MAX_LISTING_AGE_DAYS = 7 once quota resets.\n")
                return
            # Deduplicate across polygons (same zpid shouldn't appear twice)
            new_listings = [l for l in listings if l["zpid"] not in seen_zpids_this_run]
            seen_zpids_this_run.update(l["zpid"] for l in new_listings)
            print(f"{len(new_listings)} new listings (total pages: {total_pages})")

            # Warn if pagination is truncated
            if page == 1 and total_pages > MAX_PAGES and not use_all_pages:
                print(f"  ⚠️  {total_pages} pages available — only fetching {MAX_PAGES}.")
                print(f"     Run with --all-pages to fetch all, or increase MAX_PAGES in config.")

            all_listings.extend(new_listings)
            if page >= total_pages:
                break
            time.sleep(0.5)
    print(f"  {len(all_listings)} total fetched.")

    # ── 4. Deduplicate ────────────────────────────────────────────────────────
    new_listings = [l for l in all_listings if l["zpid"] not in processed]
    print(f"  {len(new_listings)} new, {len(all_listings) - len(new_listings)} already seen.")

    # ── 5-7. Pre-filter → Claude → Sheet ─────────────────────────────────────
    # Initialize counters BEFORE checking if there are new listings (needed for pending re-analysis)
    token_totals      = {"input": 0, "output": 0}
    count_prefiltered = 0
    count_sparse      = 0
    count_claude      = 0
    count_score_skip  = 0
    count_added       = 0
    count_error       = 0
    count_shelved     = 0
    newly_added_houses = []  # collect house dicts for email digest

    if new_listings:
        print(f"\nAnalyzing {len(new_listings)} new listings "
              f"(writing to sheet if dungeon_score >= {MIN_DUNGEON_SCORE})...\n")

    cap_reached = False
    for listing in new_listings:

        zpid    = listing["zpid"]
        address = listing["address"]
        price   = listing["price"]
        sqft    = listing["sqft"]
        year    = listing["year_built"]
        lot     = listing["lot_sqft"]

        lot_str = f"{lot:,.0f}sqft lot" if lot else "lot unknown"
        print(f"  {address}")
        print(f"    {sqft}sqft | built {year} | {lot_str} | {_format_price(price)}")

        passes, skip_reason = _passes_prefilter(listing)
        if not passes:
            print(f"    → SKIP: {skip_reason}")
            count_prefiltered += 1
            mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
            skipped_rows_buffer.append(_build_skipped_row(listing, None, mode_str, skip_reason))
            if len(skipped_rows_buffer) >= BATCH_SIZE:
                _flush_skipped_buffer()
            continue

        # Handle sparse listings (foreclosures/pre-foreclosures with minimal data)
        if _is_sparse(listing):
            # Reject Multi Family properties with insufficient data — can't verify unit count
            home_type_api = (listing.get("home_type") or "").lower()
            is_multifamily = "multifamily" in home_type_api

            # Safely parse bed/bath counts
            try:
                beds = int(listing.get("bedrooms") or 0)
            except (ValueError, TypeError):
                beds = 0
            try:
                baths = int(listing.get("bathrooms") or 0)
            except (ValueError, TypeError):
                baths = 0

            has_range_address = "-" in address and not address.startswith("-")

            # Reject Multi Family if: sparse data + range address (can't determine unit count safely)
            # OR bed/bath counts suggesting potential 4+ units
            if is_multifamily and (has_range_address or beds >= 7 or baths >= 5):
                mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
                skipped_rows_buffer.append(_build_skipped_row(listing, None, mode_str, "Multi Family sparse — can't verify unit count"))
                if len(skipped_rows_buffer) >= BATCH_SIZE:
                    _flush_skipped_buffer()
                print(f"    → SKIP: Multi Family with insufficient data (can't verify unit count)")
                count_score_skip += 1
                continue

            print(f"    → SPARSE: insufficient data")

            # Derive label based on status_type
            status_type_lower = (listing.get("status_type") or "").lower()
            if any(x in status_type_lower for x in ("foreclosure", "auction", "pre_foreclosure")):
                sparse_label = "Foreclosure/pre-foreclosure — insufficient data to score"
            else:
                sparse_label = "Sparse data — listing has minimal fields, unable to score"

            # Build synthetic analysis with all 1s
            home_type_display = _HOME_TYPE_DISPLAY.get(
                (listing["home_type"] or "").lower(), listing["home_type"]
            )
            analysis = {
                "property_type": home_type_display,
                "dungeon_score": 1,
                "backyard_score": 1,
                "lighting_score": 1,
                "neighborhood_score": 1,
                "turnkey_score": 1,
                "reasoning": sparse_label,
                "concerns": None,
            }
            # Sparse listings (all 1s) go to Skipped Houses only, not main sheet
            mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
            skipped_rows_buffer.append(_build_skipped_row(listing, analysis, mode_str, sparse_label))
            if len(skipped_rows_buffer) >= BATCH_SIZE:
                _flush_skipped_buffer()
            print(f"    → SKIP (sparse): {sparse_label}")
            count_sparse += 1
            continue

        # Budget cap: shelf remaining listings without analyzing them
        if MAX_JUDGEMENTS and count_claude >= MAX_JUDGEMENTS:
            if not cap_reached:
                cap_reached = True
                print(f"  [MAX_JUDGEMENTS={MAX_JUDGEMENTS} reached — shelving remaining listings]")
            mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
            skipped_rows_buffer.append(_build_skipped_row(listing, None, mode_str, "Pending analysis — delete row to re-analyze"))
            if len(skipped_rows_buffer) >= BATCH_SIZE:
                _flush_skipped_buffer()
            count_shelved += 1
            continue

        # Fetch full property details including description (with fallback to web scraping)
        print(f"    → Fetching property description...")
        prop_details = _fetch_property_description(listing["address"], listing["zpid"])
        description = prop_details.get("description")
        listing["listing_type"] = prop_details.get("listing_type")

        # Fetch transit score (and other walkability scores, but only transit to Claude)
        walk_scores = _fetch_walk_scores(str(listing["zpid"]))
        transit_score = walk_scores.get("transit") if walk_scores else None
        api_call_stats["walk_scores"] = api_call_stats.get("walk_scores", 0) + 1

        analysis = _analyze_with_claude(listing, token_totals, description=description, transit_score=transit_score)
        count_claude += 1

        if analysis is None:
            count_error += 1
            mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
            skipped_rows_buffer.append(_build_skipped_row(listing, None, mode_str, "Claude API error"))
            if len(skipped_rows_buffer) >= BATCH_SIZE:
                _flush_skipped_buffer()
            continue

        # Reject 4+ unit properties — check both Claude's output and API data
        prop_type = analysis.get("property_type", "")
        home_type_api = (listing.get("home_type") or "").lower()
        is_multifamily_api = "multifamily" in home_type_api

        # Safely parse bed/bath counts
        try:
            beds = int(listing.get("bedrooms") or 0)
        except (ValueError, TypeError):
            beds = 0
        try:
            baths = int(listing.get("bathrooms") or 0)
        except (ValueError, TypeError):
            baths = 0

        has_range_address = "-" in address and not address.startswith("-")

        # Multi-pronged rejection: Claude output OR API data + bed/bath signals
        skip_4plus = (
            "4+" in prop_type or
            "quad" in prop_type.lower() or
            "multi family (4+" in prop_type.lower() or
            (is_multifamily_api and (baths >= 5 or (has_range_address and beds >= 3)))
        )

        if skip_4plus:
            mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
            skipped_rows_buffer.append(_build_skipped_row(listing, analysis, mode_str, f"4+ unit property ({prop_type})"))
            if len(skipped_rows_buffer) >= BATCH_SIZE:
                _flush_skipped_buffer()
            print(f"    → SKIP: 4+ unit property (API: {home_type_api}, beds: {beds}, baths: {baths})")
            count_score_skip += 1
            continue

        d = analysis.get("dungeon_score", 0)
        b = analysis.get("backyard_score", 0)
        l = analysis.get("lighting_score", 0)
        n = analysis.get("neighborhood_score", 0)
        turnkey = analysis.get("turnkey_score", 0)
        overall = round((d + b + l + n + turnkey) / 5, 1) if any([d, b, l, n, turnkey]) else 0
        print(f"    Dungeon:{d} Backyard:{b} Light:{l} Hood:{n} Turnkey:{turnkey} Overall:{overall}")

        if d >= MIN_DUNGEON_SCORE:
            # Collect houses with Overall > 3 for email digest
            if overall > 3:
                house_for_email = {
                    "address": listing["address"],
                    "price": _format_price(listing["price"]),
                    "type": analysis.get("property_type", listing.get("home_type", "")),
                    "beds": listing["bedrooms"],
                    "baths": listing["bathrooms"],
                    "overall": overall,
                    "dungeon": d,
                    "backyard": b,
                    "lighting": l,
                    "neighborhood": n,
                    "turnkey": turnkey,
                    "reasoning": analysis.get("reasoning", ""),
                    "zillow_link": f"https://www.zillow.com/homedetails/{listing['zpid']}_zpid/",
                    "favorite": "FALSE",
                    "date_added": datetime.now().strftime("%Y-%m-%d"),
                }
                newly_added_houses.append(house_for_email)

            _write_sheet_row(ws, listing, analysis)
            print(f"    → ADDED ✓")
            count_added += 1
        else:
            mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
            skipped_rows_buffer.append(_build_skipped_row(listing, analysis, mode_str, f"Dungeon score {d} < {MIN_DUNGEON_SCORE}"))
            if len(skipped_rows_buffer) >= BATCH_SIZE:
                _flush_skipped_buffer()
            print(f"    → SKIP: dungeon {d} < {MIN_DUNGEON_SCORE}")
            count_score_skip += 1

    # ── 7b. Re-analyze pending listings if budget available ──────────────────
    count_pending_analyzed = 0
    if count_claude < MAX_JUDGEMENTS and MAX_JUDGEMENTS:
        pending_rows = _load_pending_rows(ws_skipped)
        if pending_rows:
            print(f"\n  {len(pending_rows)} pending analysis row(s) found. Re-analyzing with remaining budget...")
            for pending_item in pending_rows:
                if MAX_JUDGEMENTS and count_claude >= MAX_JUDGEMENTS:
                    break

                row_idx = pending_item["row_index"]
                row_data = pending_item["row_data"]
                zpid = pending_item["zpid"]

                # Reconstruct listing from row data
                listing = _row_to_listing(row_data)
                address = listing["address"]
                print(f"  {count_pending_analyzed + 1}. {address}")

                # Fetch description
                description_data = _fetch_property_description(address, zpid)
                description = description_data.get("description")

                # Analyze with Claude
                analysis = _analyze_with_claude(listing, token_totals, description=description)
                count_claude += 1

                if analysis is None:
                    # Claude error: update skip reason
                    new_skip_reason = "Pending (Claude error on re-analysis)"
                    _delete_row_by_index(ws_skipped, row_idx)
                    mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
                    row = _build_skipped_row(listing, None, mode_str, new_skip_reason)
                    _write_skipped_rows_batch(ws_skipped, [row])
                    print(f"    → Claude error, marked as pending")
                    count_pending_analyzed += 1
                    continue

                # Check dungeon score
                d = analysis.get("dungeon_score", 0)
                if d >= MIN_DUNGEON_SCORE:
                    # Qualifies! Check if already in active sheet before adding
                    existing_in_active = zpid in _load_processed_zpids_from_sheets(ws, None)

                    if existing_in_active:
                        # Already in Buy/Rent Finder, just delete from pending
                        _delete_row_by_index(ws_skipped, row_idx)
                        print(f"    → Dungeon:{d} Already in sheet, removed from pending")
                    else:
                        # Add to main sheet and delete from pending
                        _delete_row_by_index(ws_skipped, row_idx)
                        _write_sheet_row(ws, listing, analysis)
                        print(f"    → Dungeon:{d} ADDED ✓ (re-analyzed from pending)")
                        count_added += 1

                    # Collect for email if overall > 3
                    b = analysis.get("backyard_score", 0)
                    l = analysis.get("lighting_score", 0)
                    n = analysis.get("neighborhood_score", 0)
                    turnkey = analysis.get("turnkey_score", 0)
                    overall = round((d + b + l + n + turnkey) / 5, 1) if any([d, b, l, n, turnkey]) else 0
                    if overall > 3:
                        house_for_email = {
                            "address": listing["address"],
                            "price": _format_price(listing["price"]),
                            "type": analysis.get("property_type", listing.get("home_type", "")),
                            "beds": listing["bedrooms"],
                            "baths": listing["bathrooms"],
                            "overall": overall,
                            "dungeon": d,
                            "backyard": b,
                            "lighting": l,
                            "neighborhood": n,
                            "turnkey": turnkey,
                            "reasoning": analysis.get("reasoning", ""),
                            "zillow_link": f"https://www.zillow.com/homedetails/{listing['zpid']}_zpid/",
                            "favorite": "FALSE",
                            "date_added": datetime.now().strftime("%Y-%m-%d"),
                        }
                        newly_added_houses.append(house_for_email)
                else:
                    # Still fails: update skip reason
                    _delete_row_by_index(ws_skipped, row_idx)
                    mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
                    row = _build_skipped_row(listing, analysis, mode_str, f"Dungeon score {d} < {MIN_DUNGEON_SCORE}")
                    _write_skipped_rows_batch(ws_skipped, [row])
                    print(f"    → Dungeon:{d} SKIP (re-analyzed from pending)")
                    count_score_skip += 1

                count_pending_analyzed += 1

            if count_pending_analyzed > 0:
                print(f"  Re-analyzed {count_pending_analyzed} pending listing(s)")

    # Flush any remaining buffered skipped rows
    _flush_skipped_buffer()

    # ── 8. Summary ────────────────────────────────────────────────────────────
    tok_in  = token_totals["input"]
    tok_out = token_totals["output"]
    cost    = (tok_in  / 1_000_000 * HAIKU_PRICE_INPUT_PER_MTOK +
               tok_out / 1_000_000 * HAIKU_PRICE_OUTPUT_PER_MTOK)

    print(f"\n{'=' * 60}")
    print(f"  Fetched        : {len(all_listings)}")
    print(f"  New            : {len(new_listings)}")
    print(f"  Pre-filter skip: {count_prefiltered}")
    print(f"  Sparse (no data): {count_sparse}")
    print(f"  Claude analyzed: {count_claude}")
    print(f"  Score skip     : {count_score_skip}")
    print(f"  Errors         : {count_error}")
    print(f"  Shelved (budget): {count_shelved}")
    if 'count_pending_analyzed' in locals() and count_pending_analyzed > 0:
        print(f"  Pending re-analyzed: {count_pending_analyzed}")
    print(f"  Added to sheet : {count_added}")
    # Count descriptions and walk_scores calls (they're only made for non-sparse listings that pass pre-filter)
    descriptions = api_call_stats.get('walk_scores', 0)  # descriptions called same # of times as walk_scores
    print(f"  API calls: search={{private-zillow={api_call_stats.get('private-zillow', 0)}, zllw={api_call_stats.get('zllw', 0)}}} | descriptions={descriptions} | walk_scores={api_call_stats.get('walk_scores', 0)}")
    print(f"  Tokens         : {tok_in:,} in / {tok_out:,} out")
    print(f"  Est. cost      : ${cost:.4f}")

    # Pirate mode summary
    if count_added > 0:
        print()
        print(f"  🏴‍☠️  Ahoy! {count_added} dungeon(s) plundered from Ye Olde Zillow, matey!")
    print()

    # ── 9. Email new qualifying houses ────────────────────────────────────────
    if newly_added_houses:
        mode_str = "rent" if LISTING_STATUS == "For_Rent" else "buy"
        print(f"Sending email for {len(newly_added_houses)} qualifying house(s)...")
        send_email(newly_added_houses, mode=mode_str)
    else:
        print("No new qualifying houses (Overall > 3). No email sent.")


if __name__ == "__main__":
    # Support --rent flag to search for rentals instead of for-sale listings
    if "--rent" in sys.argv:
        LISTING_STATUS = "For_Rent"
    main()
