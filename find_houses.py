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
import re
import json
import time
import sqlite3
from datetime import datetime, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials
import anthropic
from dotenv import load_dotenv

# Load environment variables from .env file (if it exists)
load_dotenv()

# ── Exceptions ────────────────────────────────────────────────────────────────

class QuotaExceededException(Exception):
    """Raised when RapidAPI quota is exceeded (HTTP 429)."""
    pass

# ── Config ────────────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
RAPIDAPI_KEY  = os.environ.get("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = "zllw-working-api.p.rapidapi.com"

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
MAX_LISTING_AGE_DAYS = 30  # skip listings on market > this many days (saves API calls)
                           # Increase to 90+ on first run to catch all existing inventory;
                           # set back to 30 for daily use to minimize /pro/byaddress calls
MIN_DUNGEON_SCORE = 2      # minimum dungeon score to add to sheet
MAX_PAGES         = 1      # 1 API request per page; free tier = 500 req/month
MAX_PER_RUN       = 20     # cap Claude calls per run (set to None for no limit)
REQUEST_TIMEOUT   = 20

# ── Hard pre-filters (no Claude cost, aggressive unsuitable listings filtering) ───
MIN_SQFT       = 1000   # skip studios / very small units
MIN_YEAR_OLD   = 1978   # if newer than this AND small lot, skip (no basement likely)

# ── Listing mode: "For_Sale" or "For_Rent" (change to search rentals) ───────────
LISTING_STATUS = "For_Sale"   # "For_Sale" or "For_Rent"

SPREADSHEET_ID = "1MRKLmSjIkWUArbJwVgz9fgCSsh0WM7UoxPJCEeWe-ms"
SHEET_TAB      = "House Finder"
RENT_SHEET_TAB = "Rent Finder"
CREDS_FILE     = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "credentials.json")
DB_FILE        = os.path.join(os.path.dirname(os.path.realpath(__file__)), "house_finder.db")

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

  NEVER output "Multi Family (4+ units)" — we don't want 4+ unit properties.
  If it looks like 4+ units (Beds ≥ 8, baths ≥ 6, sqft > 5000), cap it at "Triplex" instead.

RULE 3 — Address range is a strong signal:
  "2811-2815 Telegraph" (4 numbers) = cap at Triplex
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
  5 = older craftsman / bungalow style — known for large windows
  4 = pre-1960, likely good natural light
  3 = 1960s–70s ranch, variable
  2 = post-1978 or dense urban lot, likely less light
  1 = very unlikely to have good natural light

NEIGHBORHOOD (desirability / character):
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

CRITICAL: property_type must be ONLY: "Single" or "Duplex" or "Triplex"
  (If you think it's 4+ units, output "Triplex" as the max)

{{
  "property_type":      "Single" or "Duplex" or "Triplex",
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
    "Price ($M)",         # C
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
]


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


def _get_sheet(tab_name: str = None) -> gspread.Worksheet:
    """Get or create a sheet tab. Defaults to SHEET_TAB (buy) or RENT_SHEET_TAB (rent)."""
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
        ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(SHEET_HEADERS) + 2)
        _sheets_call(lambda: ws.append_row(SHEET_HEADERS))
    else:
        ws = spreadsheet.worksheet(tab_name)
    return ws


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
    ]
    _sheets_call(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))


# ── SQLite deduplication ──────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS processed_listings (
    zpid          TEXT PRIMARY KEY,
    address       TEXT,
    price         INTEGER,
    status        TEXT NOT NULL,
    dungeon_score REAL,
    processed_at  TEXT NOT NULL,
    retry_after   TEXT
)"""

def _load_processed_zpids() -> set:
    """Load all zpids marked as processed (permanent skip)."""
    with sqlite3.connect(DB_FILE) as con:
        con.execute(_CREATE_TABLE)
        rows = con.execute(
            "SELECT zpid FROM processed_listings"
        ).fetchall()
    return {r[0] for r in rows}


def _save_processed(zpid: str, address: str, price, status: str, score=None) -> None:
    """Save a processed listing (permanent — all entries marked as never retry)."""
    with sqlite3.connect(DB_FILE) as con:
        con.execute(_CREATE_TABLE)
        con.execute(
            """INSERT OR REPLACE INTO processed_listings
               (zpid, address, price, status, dungeon_score, processed_at, retry_after)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (str(zpid), address, price, status, score,
             datetime.now().isoformat(), None)  # NULL = permanent skip
        )


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
        "listing_type": None,  # Will be filled in by _fetch_property_description
    }


def _fetch_page(page: int = 1, polygon: str = "") -> tuple[list, int]:
    """Fetch one page for a given polygon. Returns (parsed_listings, total_pages)."""
    if not polygon:
        return [], 0
    if page == 1:
        print(f"    [polygon] {polygon[:50]}...")
    params = {
        "polygon":       polygon,
        "listingStatus": LISTING_STATUS,
        "propertyType":  "SingleFamily,MultiFamily",
        "page":          page,
    }
    if PRICE_MIN is not None:
        params["minPrice"] = PRICE_MIN
    if PRICE_MAX is not None:
        params["maxPrice"] = PRICE_MAX
    if MIN_BEDS:
        params["minBeds"] = MIN_BEDS

    headers = {
        "Content-Type":    "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key":  RAPIDAPI_KEY,
    }

    try:
        resp = requests.get(
            f"https://{RAPIDAPI_HOST}/search/bypolygon",
            params=params, headers=headers, timeout=REQUEST_TIMEOUT
        )
    except requests.exceptions.Timeout:
        print(f"  [api] Timeout on page {page}")
        return [], 0
    except requests.exceptions.ConnectionError as e:
        print(f"  [api] Connection error: {e}")
        return [], 0

    if resp.status_code == 429:
        raise QuotaExceededException(resp.text)

    if resp.status_code != 200:
        print(f"  [api] HTTP {resp.status_code}: {resp.text[:200]}")
        return [], 0

    try:
        data = resp.json()
    except Exception:
        print(f"  [api] Non-JSON response")
        return [], 0

    raw_results = data.get("searchResults", [])
    listings    = [l for l in (_parse_listing(r) for r in raw_results) if l]
    total_pages = int((data.get("pagesInfo") or {}).get("totalPages") or 1)

    return listings, total_pages


# ── Pre-filter ────────────────────────────────────────────────────────────────

def _passes_prefilter(listing: dict) -> tuple[bool, str]:
    """Hard pre-filters — skip if fails any of these checks."""
    # Check price
    try:
        price = int(listing.get("price") or 0)
        if PRICE_MIN and price < PRICE_MIN:
            return False, f"price ${price:,} < ${PRICE_MIN:,}"
        if PRICE_MAX and price > PRICE_MAX:
            return False, f"price ${price:,} > ${PRICE_MAX:,}"
    except (ValueError, TypeError):
        pass

    # Reject condos / townhouses — no studio space potential
    home_type = (listing.get("home_type") or "").lower()
    allowed_types = {"singlefamily", "multifamily"}
    if home_type and not any(t in home_type for t in allowed_types):
        return False, f"property type '{listing['home_type']}' not singleFamily/multiFamily"

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

    # Check listing age (days on market)
    if MAX_LISTING_AGE_DAYS:
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

def _fetch_property_description(address: str) -> dict:
    """Fetch full property details from /pro/byaddress endpoint."""
    try:
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-host": RAPIDAPI_HOST,
            "x-rapidapi-key": RAPIDAPI_KEY,
        }
        resp = requests.get(
            f"https://{RAPIDAPI_HOST}/pro/byaddress",
            params={"propertyaddress": address},
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            data = resp.json()
            details = data.get("propertyDetails", {})
            return {
                "description": details.get("description"),
                "bedrooms": details.get("resoFacts", {}).get("bedrooms"),
                "bathrooms": details.get("resoFacts", {}).get("bathroomsFloat"),
                "sqft": details.get("resoFacts", {}).get("aboveGradeFinishedArea"),
                "listing_type": details.get("listingTypeDimension"),  # e.g., "Pre-Foreclosure"
            }
    except Exception as e:
        pass
    return {"description": None, "bedrooms": None, "bathrooms": None, "sqft": None, "listing_type": None}


# ── Claude ────────────────────────────────────────────────────────────────────

def _analyze_with_claude(listing: dict, token_totals: dict, description: str = None):
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
    if not RAPIDAPI_KEY:
        print("ERROR: RAPIDAPI_KEY env var not set.")
        print("Run: export RAPIDAPI_KEY=your_key   (or add to ~/.zshrc)")
        return

    # ── 1. Connect Sheets ─────────────────────────────────────────────────────
    print("\nConnecting to Google Sheets...")
    try:
        ws = _get_sheet()
        print(f"  Connected to '{SHEET_TAB}'.")
    except Exception as e:
        print(f"  Sheets error: {e}")
        return

    # ── 2. Load processed zpids ───────────────────────────────────────────────
    print("\nLoading processed listings...")
    processed = _load_processed_zpids()
    with sqlite3.connect(DB_FILE) as con:
        con.execute(_CREATE_TABLE)
        total_db       = con.execute("SELECT COUNT(*) FROM processed_listings").fetchone()[0]
        retry_eligible = total_db - len(processed)
    print(f"  {total_db} tracked ({len(processed)} active skips, {retry_eligible} retry-eligible)")

    # ── 3. Fetch listings ─────────────────────────────────────────────────────
    polygons = _load_polygons()
    print(f"\nFetching listings ({len(polygons)} polygon{'s' if len(polygons) != 1 else ''})...")
    all_listings = []
    seen_zpids_this_run = set()
    for poly_idx, polygon in enumerate(polygons, 1):
        print(f"  Polygon {poly_idx}/{len(polygons)}...")
        for page in range(1, MAX_PAGES + 1):
            print(f"    Page {page}...", end=" ", flush=True)
            try:
                listings, total_pages = _fetch_page(page, polygon)
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
            all_listings.extend(new_listings)
            if page >= total_pages:
                break
            time.sleep(0.5)
    print(f"  {len(all_listings)} total fetched.")

    # ── 4. Deduplicate ────────────────────────────────────────────────────────
    new_listings = [l for l in all_listings if l["zpid"] not in processed]
    print(f"  {len(new_listings)} new, {len(all_listings) - len(new_listings)} already seen.")

    if not new_listings:
        print("\nNo new listings. Done.")
        return

    # ── 5-7. Pre-filter → Claude → Sheet ─────────────────────────────────────
    print(f"\nAnalyzing {len(new_listings)} new listings "
          f"(writing to sheet if dungeon_score >= {MIN_DUNGEON_SCORE})...\n")

    token_totals      = {"input": 0, "output": 0}
    count_prefiltered = 0
    count_sparse      = 0
    count_claude      = 0
    count_score_skip  = 0
    count_added       = 0
    count_error       = 0

    for listing in new_listings:
        # Stop after MAX_PER_RUN Claude calls
        if MAX_PER_RUN and count_claude >= MAX_PER_RUN:
            print(f"  [stopping: reached MAX_PER_RUN={MAX_PER_RUN} Claude calls]")
            break

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
            _save_processed(zpid, address, price, "skipped_prefilter")
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
                _save_processed(zpid, address, price, "skipped_score", score=1)
                print(f"    → SKIP: Multi Family with insufficient data (can't verify unit count)")
                count_score_skip += 1
                continue

            print(f"    → SPARSE: insufficient data (foreclosure/pre-foreclosure)")
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
                "reasoning": "Foreclosure/pre-foreclosure — insufficient data to score",
                "concerns": None,
            }
            _write_sheet_row(ws, listing, analysis)
            _save_processed(zpid, address, price, "analyzed", score=1)
            print(f"    → ADDED ✓")
            count_sparse += 1
            count_added += 1
            continue

        # Fetch full property details including description
        print(f"    → Fetching property description...")
        prop_details = _fetch_property_description(listing["address"])
        description = prop_details.get("description")
        listing["listing_type"] = prop_details.get("listing_type")

        analysis = _analyze_with_claude(listing, token_totals, description=description)
        count_claude += 1

        if analysis is None:
            count_error += 1
            _save_processed(zpid, address, price, "error")
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
            _save_processed(zpid, address, price, "skipped_score", score=1)
            print(f"    → SKIP: 4+ unit property (API: {home_type_api}, beds: {beds}, baths: {baths})")
            count_score_skip += 1
            continue

        d = analysis.get("dungeon_score", 0)
        b = analysis.get("backyard_score", 0)
        l = analysis.get("lighting_score", 0)
        n = analysis.get("neighborhood_score", 0)
        turnkey = analysis.get("turnkey_score", 0)
        print(f"    Dungeon:{d} Backyard:{b} Light:{l} Hood:{n} Turnkey:{turnkey}")

        if d >= MIN_DUNGEON_SCORE:
            _write_sheet_row(ws, listing, analysis)
            _save_processed(zpid, address, price, "analyzed", score=d)
            print(f"    → ADDED ✓")
            count_added += 1
        else:
            _save_processed(zpid, address, price, "skipped_score", score=d)
            print(f"    → SKIP: dungeon {d} < {MIN_DUNGEON_SCORE}")
            count_score_skip += 1

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
    print(f"  Added to sheet : {count_added}")
    print(f"  Tokens         : {tok_in:,} in / {tok_out:,} out")
    print(f"  Est. cost      : ${cost:.4f}")

    # Pirate mode summary
    if count_added > 0:
        print()
        print(f"  🏴‍☠️  Ahoy! {count_added} dungeon(s) plundered from Ye Olde Zillow, matey!")
    print()


if __name__ == "__main__":
    # Support --rent flag to search for rentals instead of for-sale listings
    if "--rent" in sys.argv:
        LISTING_STATUS = "For_Rent"
    main()
