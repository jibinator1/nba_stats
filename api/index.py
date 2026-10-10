"""
Vercel Serverless Function Entry Point for NBA Matchup Hub
============================================================
Handles automatic path routing and exports the WSGI application.
"""

import sys
import os

# Inject project root to sys.path so app imports resolve seamlessly
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from app import app, handler
