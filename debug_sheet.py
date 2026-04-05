#!/usr/bin/env python3
import os
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_ID = "1MRKLmSjIkWUArbJwVgz9fgCSsh0WM7UoxPJCEeWe-ms"
CREDS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "credentials.json")

try:
    creds = Credentials.from_service_account_file(
        CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet("House Finder")
    rows = ws.get_all_values()

    print(f"Total rows: {len(rows)}")
    if len(rows) > 0:
        print(f"Headers: {rows[0]}")

    if len(rows) > 1:
        print(f"\nAll rows (Address and Zillow Link only):")
        for i, row in enumerate(rows):
            if i == 0:
                continue
            addr = row[0] if len(row) > 0 else "—"
            link = row[1] if len(row) > 1 else "—"
            print(f"  {i}: {addr} | {link}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
