# NBA Forensic Scouting Engine 🏀

A strictly descriptive, play-by-play, and tracking-backed NBA matchup analytics platform. Built on multi-season historical distributions, sample-gated percentiles, and 4-pillar forensic structural alignment.

**Live site:** https://nba-stats-silk.vercel.app/

---

## Forensic Philosophy

* **No Predictions / No Betting Language:** The engine does not emit projections, subjective labels, or recommendations.
* **Sample Size Gating:** Teams or players with under 10 games or under 200 possessions are explicitly flagged with `[Small Sample]` and excluded from categorical tier rankings.
* **Date Range Gating:** All metrics and distributions dynamically adapt to selected date windows. Matches with zero completed games in a chosen window cleanly render unskewed blank states.
* **Euclidean Analogs:** Similar-opponent defense matching uses normalized Euclidean feature distance ($\le 1.50\sigma$) across Pace, 3PT attempt rate, Paint attempt rate, and PnR Switch rate.

---

## 4 Forensic Scouting Pillars

1. **Pace, Game Flow & Transition:** Neutral pace (differential $\le 5$ pts), early shot-clock frequency (0–8s), mid shot-clock frequency (8–15s), late shot-clock pressure (15–24s), and game script tempo under 10+ point leads or deficits.
2. **Perimeter & Paint Defense:** Pick & Roll coverage frequencies (Drop, Switch, Ice, Hedge), Restricted Area allowed PPP, Paint non-RA allowed PPP, Corner 3 allowed PPP, Wide-Open 3PT rate allowed, and Defensive Rebounding percentage (DREB%).
3. **Offensive Play Type & Scheme:** Frequency and points per possession across PnR Ball Handler, Spot-Up, and Isolation play types, plus live-ball vs. dead-ball turnover splits.
4. **Player Usage Profiles:** Top 4 rotation players with empirical 10-game distributions (25th percentile Floor, 50th percentile Median, 75th percentile Ceiling) alongside USG%, AST%, and situational clutch volume.

---

## Quick Start (Local Development)

### Prerequisites
- Python 3.9+

### Setup

```bash
# 1. Navigate to the project folder
cd "nba_stats"

# 2. (Recommended) Create a virtual environment
python -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the forensic engine
python app.py
```

Then open **http://127.0.0.1:5000** in your browser.

---

## Automated Verification Suite

Run the full forensic audit suite to verify zero forbidden terms, JSON schema integrity, date gating, and Euclidean clustering:

```bash
python verify_forensic.py
```

---

## Architecture & Project Structure

```
nba_stats/
├── app.py                           # Core Flask application and REST routes
├── forensic_data.py                 # Multi-season tracking data pipeline & Euclidean analog model
├── daily_update.py                  # Automated daily game log sync
├── verify_forensic.py               # Comprehensive verification and compliance suite
├── schedule_games.json              # Full 2025-26 & 2026-27 schedule with box scores
├── forensic_teams.json              # Team 4-pillar baselines and allowed distributions
├── forensic_players.json            # Top 4 player rotation percentile profiles
├── forensic_similar_opponents.json  # Precomputed Euclidean opponent clusters
├── forensic_team_game_logs.json     # Per-game tracking logs for dynamic date range recalculations
├── static/
│   ├── style.css                    # Unified dark-mode stylesheet & tooltip layout
│   └── js/
│       ├── app.js                   # Dashboard state, date filtering, and schedule grid
│       ├── visuals.js               # Responsive SVG shot zones, play types & pace gauge
│       ├── modal.js                 # Deep Dive modal, 4-pillar stats, and callout generator
│       └── profiles.js              # Player usage profile table, slate chips, and sorting
├── templates/
│   └── index.html                   # Semantic HTML template
└── vercel.json                      # Serverless packaging configuration
```

---

*NBA Forensic Scouting Engine • Strictly Descriptive Analytics*
