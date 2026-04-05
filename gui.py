#!/usr/bin/env python3
"""
gui.py — Flask web server for drawing and managing Zillow search polygons.

Run: python3 gui.py or ./gui
Then open http://localhost:5001 (or http://<local_ip>:5001 to share with others on the same WiFi)
"""

import warnings

# Suppress warnings BEFORE importing anything else
warnings.filterwarnings("ignore", category=FutureWarning, message=".*Python version.*")
warnings.filterwarnings("ignore", message=".*urllib3.*only supports OpenSSL.*")

import os
import json
import subprocess
import sys
from flask import Flask, jsonify, request, render_template, Response
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# Load environment variables from .env file (if it exists)
load_dotenv()

app = Flask(__name__)
POLYGONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polygons.json")
GEOCODE_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geocode_cache.json")
CREDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "credentials.json")
SPREADSHEET_ID = "1MRKLmSjIkWUArbJwVgz9fgCSsh0WM7UoxPJCEeWe-ms"
SHEET_TAB = "House Finder"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/polygons", methods=["GET"])
def get_polygons():
    """Load saved polygons from polygons.json."""
    try:
        with open(POLYGONS_FILE) as f:
            data = json.load(f)
        return jsonify(data)
    except FileNotFoundError:
        return jsonify([])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/polygons", methods=["POST"])
def save_polygons():
    """Save polygons to polygons.json."""
    try:
        with open(POLYGONS_FILE, "w") as f:
            json.dump(request.json, f, indent=2)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _load_geocache():
    """Load geocoding cache from file."""
    try:
        with open(GEOCODE_CACHE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def _save_geocache(cache):
    """Save geocoding cache to file."""
    with open(GEOCODE_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


@app.route("/listings")
def get_listings():
    """Fetch listings from Google Sheet and geocode addresses."""
    try:
        creds = Credentials.from_service_account_file(
            CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TAB)
        rows = ws.get_all_values()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if len(rows) < 2:
        return jsonify([])

    headers = rows[0]
    geocache = _load_geocache()
    geolocator = Nominatim(user_agent="house_finder")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    results = []
    cache_updated = False

    for row in rows[1:]:  # skip header
        if not row or not row[0]:
            continue

        record = dict(zip(headers, row))
        address = record.get("Address", "")

        # Geocode with cache
        if address not in geocache:
            try:
                loc = geocode(address + ", CA")
                if loc:
                    geocache[address] = {"lat": loc.latitude, "lng": loc.longitude}
                    cache_updated = True
                else:
                    geocache[address] = None
                    cache_updated = True
            except Exception:
                geocache[address] = None
                cache_updated = True

        coords = geocache.get(address)
        if not coords:
            continue

        results.append({
            "lat": coords["lat"],
            "lng": coords["lng"],
            "address": address,
            "zillow": record.get("Zillow Link", ""),
            "price": record.get("Price ($M)", ""),
            "type": record.get("Home Type", ""),
            "beds": record.get("Beds", ""),
            "baths": record.get("Baths", ""),
            "overall": record.get("Overall", ""),
            "dungeon": record.get("Dungeon", ""),
            "backyard": record.get("Backyard", ""),
            "lighting": record.get("Lighting", ""),
            "neighborhood": record.get("Neighborhood", ""),
            "turnkey": record.get("Turnkey", ""),
            "reasoning": record.get("Reasoning", ""),
        })

    if cache_updated:
        _save_geocache(geocache)

    return jsonify(results)


@app.route("/run")
def run_search():
    """Stream output from house_finder.py as server-sent events."""
    def stream():
        try:
            proc = subprocess.Popen(
                [sys.executable, "house_finder.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=os.path.dirname(os.path.abspath(__file__))
            )
            for line in proc.stdout:
                yield f"data: {line.rstrip()}\n\n"
            proc.wait()
            yield "data: [done]\n\n"
        except Exception as e:
            yield f"data: ERROR: {str(e)}\n\n"
            yield "data: [done]\n\n"

    return Response(stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    import socket

    # Try to get the actual local network IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        host_ip = s.getsockname()[0]
        s.close()
    except Exception:
        try:
            host_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            host_ip = "127.0.0.1"

    print("\n" + "="*60)
    print("House Finder — Polygon Search GUI")
    print("="*60)
    print(f"\n  Local:   http://localhost:5001")
    print(f"  Network: http://{host_ip}:5001")
    print(f"\n  Share the Network URL with others on your WiFi.")
    print("="*60 + "\n")

    app.run(host="0.0.0.0", port=5001, debug=False)
