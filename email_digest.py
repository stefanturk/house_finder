#!/usr/bin/env python3
"""
email_digest.py — Sends a daily summary email of new houses found.

Fetches houses from Google Sheet and sends a nicely formatted email via Resend.

Usage:
  python3 email_digest.py                    # Sends all houses from today
  python3 email_digest.py --zpids 24753898 24755469   # Sends specific zpids (for testing)
"""

import os
import sys
import re
import json
import requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
SPREADSHEET_ID = "1MRKLmSjIkWUArbJwVgz9fgCSsh0WM7UoxPJCEeWe-ms"
SHEET_TAB = "House Finder"
CREDS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "credentials.json")
EMAIL_TO = "stefanturkowski@gmail.com"


def get_sheet_data(mode="buy"):
    """Fetch all rows from the sheet."""
    try:
        creds = Credentials.from_service_account_file(
            CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        tab = "Rent Finder" if mode == "rent" else SHEET_TAB
        ws = gc.open_by_key(SPREADSHEET_ID).worksheet(tab)
        rows = ws.get_all_values()
        return rows
    except Exception as e:
        print(f"✗ Error fetching sheet: {e}")
        return []


def build_house_html(house):
    """Build HTML snippet for a single house."""
    favorite = "♥ " if house.get("favorite") == "TRUE" else ""

    return f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin-bottom: 16px; background: #fff;">
        <h3 style="margin: 0 0 8px 0; color: #333;">{favorite}{house.get('address', 'N/A')}</h3>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; font-size: 14px; color: #666;">
            <div><strong>${house.get('price', 'N/A')}</strong></div>
            <div>{house.get('type', 'N/A')}</div>
            <div>{house.get('beds', '—')} bed / {house.get('baths', '—')} bath</div>
            <div>Added: {house.get('date_added', 'N/A')}</div>
        </div>

        <div style="background: #f9f9f9; padding: 12px; border-radius: 4px; margin-bottom: 12px; font-size: 13px;">
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                <div>⭐ Overall: <strong>{house.get('overall', '—')}</strong></div>
                <div>🏚 Dungeon: <strong>{house.get('dungeon', '—')}</strong></div>
                <div>🌳 Backyard: <strong>{house.get('backyard', '—')}</strong></div>
                <div>💡 Lighting: <strong>{house.get('lighting', '—')}</strong></div>
                <div>🏘 Neighborhood: <strong>{house.get('neighborhood', '—')}</strong></div>
                <div>🔑 Turnkey: <strong>{house.get('turnkey', '—')}</strong></div>
            </div>
        </div>

        <div style="font-size: 13px; color: #555; font-style: italic; margin-bottom: 12px;">
            {house.get('reasoning', 'No notes')}
        </div>

        <a href="{house.get('zillow_link', '#')}" style="display: inline-block; padding: 8px 16px; background: #0066cc; color: white; text-decoration: none; border-radius: 4px; font-weight: 600; font-size: 13px;">View on Zillow →</a>
    </div>
    """


def send_email(houses, mode="buy"):
    """Send email with house digest via Resend."""
    if not RESEND_API_KEY:
        print("✗ Error: RESEND_API_KEY not set in .env")
        return False

    if not houses:
        print("ℹ No houses to email")
        return True

    # Build email body
    mode_label = "Rental" if mode == "rent" else "Buy"
    houses_html = "".join(build_house_html(h) for h in houses)

    html_body = f"""
    <html>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; margin: 0; padding: 16px;">
            <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; padding: 24px;">
                <h1 style="margin: 0 0 8px 0; color: #333;">House Finder — {mode_label} Digest</h1>
                <p style="margin: 0 0 24px 0; color: #666; font-size: 14px;">{datetime.now().strftime('%A, %B %d, %Y')}</p>

                <p style="color: #666; margin-bottom: 20px;">Found <strong>{len(houses)}</strong> new {mode} listing{"s" if len(houses) != 1 else ""}:</p>

                {houses_html}

                <hr style="border: none; border-top: 1px solid #ddd; margin: 24px 0;">
                <p style="color: #999; font-size: 12px; margin: 0;">
                    View full listing at: <a href="https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}">Google Sheet</a>
                </p>
            </div>
        </body>
    </html>
    """

    try:
        response = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "onboarding@resend.dev",
                "to": EMAIL_TO,
                "subject": f"House Finder — {mode_label} Digest ({len(houses)} new listing{'s' if len(houses) != 1 else ''})",
                "html": html_body,
            },
        )

        if response.status_code == 200:
            data = response.json()
            print(f"✓ Email sent successfully")
            print(f"  To: {EMAIL_TO}")
            print(f"  Houses: {len(houses)}")
            print(f"  ID: {data.get('id')}")
            return True
        else:
            print(f"✗ Error sending email: {response.status_code}")
            print(f"  {response.text}")
            return False

    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    """Fetch houses and send email."""
    zpids = None

    # Check for --zpids argument
    if "--zpids" in sys.argv:
        idx = sys.argv.index("--zpids")
        zpids = sys.argv[idx + 1:]

    mode = "buy"
    if "--rent" in sys.argv:
        mode = "rent"

    # Fetch sheet data
    rows = get_sheet_data(mode)
    if len(rows) < 2:
        print(f"ℹ No data in {mode} sheet")
        return

    headers = rows[0]

    # Find column indices
    address_col = headers.index("Address") if "Address" in headers else 0
    price_col = headers.index("Price") if "Price" in headers else -1
    type_col = headers.index("Home Type") if "Home Type" in headers else -1
    beds_col = headers.index("Beds") if "Beds" in headers else -1
    baths_col = headers.index("Baths") if "Baths" in headers else -1
    overall_col = headers.index("Overall") if "Overall" in headers else -1
    dungeon_col = headers.index("Dungeon") if "Dungeon" in headers else -1
    backyard_col = headers.index("Backyard") if "Backyard" in headers else -1
    lighting_col = headers.index("Lighting") if "Lighting" in headers else -1
    neighborhood_col = headers.index("Neighborhood") if "Neighborhood" in headers else -1
    turnkey_col = headers.index("Turnkey") if "Turnkey" in headers else -1
    reasoning_col = headers.index("Reasoning") if "Reasoning" in headers else -1
    zillow_col = headers.index("Zillow Link") if "Zillow Link" in headers else -1
    favorite_col = headers.index("Favorite") if "Favorite" in headers else -1
    date_col = headers.index("Date Added") if "Date Added" in headers else -1

    # Build house list
    houses = []
    for row in rows[1:]:
        if not row or not row[0]:
            continue

        # Extract zpid from zillow link if present
        zpid = None
        if zillow_col >= 0 and len(row) > zillow_col:
            link = row[zillow_col]
            try:
                # Extract zpid from URL like: https://www.zillow.com/homedetails/24753898_zpid/
                match = re.search(r'homedetails/(\d+)_zpid', link)
                zpid = match.group(1) if match else None
            except:
                pass

        # If filtering by zpid, skip if not in list
        if zpids:
            if not zpid or zpid not in zpids:
                continue

        house = {
            "address": row[address_col] if address_col < len(row) else "N/A",
            "price": row[price_col] if price_col >= 0 and price_col < len(row) else "N/A",
            "type": row[type_col] if type_col >= 0 and type_col < len(row) else "N/A",
            "beds": row[beds_col] if beds_col >= 0 and beds_col < len(row) else "—",
            "baths": row[baths_col] if baths_col >= 0 and baths_col < len(row) else "—",
            "overall": row[overall_col] if overall_col >= 0 and overall_col < len(row) else "—",
            "dungeon": row[dungeon_col] if dungeon_col >= 0 and dungeon_col < len(row) else "—",
            "backyard": row[backyard_col] if backyard_col >= 0 and backyard_col < len(row) else "—",
            "lighting": row[lighting_col] if lighting_col >= 0 and lighting_col < len(row) else "—",
            "neighborhood": row[neighborhood_col] if neighborhood_col >= 0 and neighborhood_col < len(row) else "—",
            "turnkey": row[turnkey_col] if turnkey_col >= 0 and turnkey_col < len(row) else "—",
            "reasoning": row[reasoning_col] if reasoning_col >= 0 and reasoning_col < len(row) else "",
            "zillow_link": row[zillow_col] if zillow_col >= 0 and zillow_col < len(row) else "#",
            "favorite": row[favorite_col] if favorite_col >= 0 and favorite_col < len(row) else "FALSE",
            "date_added": row[date_col] if date_col >= 0 and date_col < len(row) else "N/A",
        }
        houses.append(house)

    if not houses:
        print("ℹ No houses found")
        return

    # Send email
    send_email(houses, mode)


if __name__ == "__main__":
    main()
