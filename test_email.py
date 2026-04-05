#!/usr/bin/env python3
"""
test_email.py — sends a test email to verify email sending works.

Setup (one time):
  1. Enable 2-Step Verification on your Google account (myaccount.google.com → Security)
  2. Go to: Security → 2-Step Verification → App Passwords
  3. Select "Mail" and "Other (custom name)" → "House Finder"
  4. Google generates a 16-character app password (looks like: xxxx xxxx xxxx xxxx)
  5. Add to .env:
       GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
       EMAIL_FROM=your@gmail.com
       EMAIL_TO=recipient@gmail.com  (can be same as EMAIL_FROM)

Run:
  python3 test_email.py
"""

import smtplib
import ssl
import os
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "")

if not all([GMAIL_APP_PASSWORD, EMAIL_FROM, EMAIL_TO]):
    print("Error: missing GMAIL_APP_PASSWORD, EMAIL_FROM, or EMAIL_TO in .env")
    print("See setup instructions at top of this file.")
    exit(1)

msg = MIMEText("Hello from House Finder! Email is working. 🎉")
msg["Subject"] = "House Finder — Test Email"
msg["From"] = EMAIL_FROM
msg["To"] = EMAIL_TO

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
        s.login(EMAIL_FROM, GMAIL_APP_PASSWORD)
        s.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    print(f"✓ Email sent to {EMAIL_TO}")
except Exception as e:
    print(f"✗ Error sending email: {e}")
    exit(1)
