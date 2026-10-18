"""
NBA Forensic Scouting Engine - Local App Launcher
==================================================
Launches the Flask server on http://localhost:5000 and automatically
opens the interactive dashboard in your default browser.
"""

import time
import webbrowser
import threading
from app import app

def open_browser():
    time.sleep(1.5)
    webbrowser.open("http://localhost:5000")

if __name__ == "__main__":
    print("=" * 60)
    print("STARTING NBA FORENSIC SCOUTING ENGINE")
    print("Descriptive Play-by-Play & Tracking Matchup Alignment")
    print("=" * 60)
    print("Dashboard will open automatically at: http://localhost:5000")
    print("Press Ctrl+C in this terminal to stop the server.")
    print("=" * 60)

    threading.Thread(target=open_browser, daemon=True).start()
    app.run(host="127.0.0.1", port=5000, debug=False)
