#!/usr/bin/env python3
"""
test_email.py — sends a test email via Resend API.

Setup (one time):
  1. Sign up at https://resend.com (free tier available)
  2. Create an API key from your dashboard
  3. Add to .env:
       RESEND_API_KEY=re_xxxxxxxxxxxxx

Run:
  python3 test_email.py
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")

if not RESEND_API_KEY:
    print("Error: missing RESEND_API_KEY in .env")
    print("See setup instructions at top of this file.")
    exit(1)

try:
    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": "onboarding@resend.dev",
            "to": "stefanturkowski@gmail.com",
            "subject": "House Finder — Test Email",
            "html": "<p>Hello from House Finder! Email is working. 🎉</p>",
        },
    )

    if response.status_code == 200:
        data = response.json()
        print(f"✓ Email sent successfully")
        print(f"  ID: {data.get('id')}")
    else:
        print(f"✗ Error sending email: {response.status_code}")
        print(f"  {response.text}")
        exit(1)

except Exception as e:
    print(f"✗ Error: {e}")
    exit(1)
