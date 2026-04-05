#!/usr/bin/env python3
"""
gui.py — Flask web server for drawing and managing Zillow search polygons.

Run: python3 gui.py or ./gui
Then open http://localhost:5001 (or http://<local_ip>:5001 to share with others on the same WiFi)
"""

import os
import json
import subprocess
import sys
from flask import Flask, jsonify, request, render_template, Response

app = Flask(__name__)
POLYGONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polygons.json")


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
    try:
        host_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        host_ip = "127.0.0.1"

    print("\n" + "="*60)
    print("House Finder — Polygon Search GUI")
    print("="*60)
    print(f"\n  Local:   http://localhost:5001")
    print(f"  Network: http://{host_ip}:5001")
    print(f"\nShare the network URL with others on your WiFi.")
    print("="*60 + "\n")

    app.run(host="0.0.0.0", port=5001, debug=False)
