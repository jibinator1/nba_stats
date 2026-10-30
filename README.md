# NBA Forensic Scouting Engine 🏀

A strictly descriptive, play-by-play, and tracking-backed NBA matchup analytics platform. Built on multi-season historical distributions, sample-gated percentiles, and 4-pillar forensic structural alignment.

**Live site:** https://nba-stats-silk.vercel.app/

---

## Forensic Philosophy

* **No Predictions / No Betting Language:** The engine does not emit projections, subjective labels, or recommendations.
* **Universal Rankings:** All teams and players with recorded game data are explicitly ranked across all metrics, shot zones, and play types regardless of sample size.
* **Date Range Gating:** All metrics and distributions dynamically adapt to selected date windows. Matches with zero completed games in a chosen window cleanly render unskewed blank states.
* **Euclidean Analogs:** Similar-opponent defense matching uses normalized Euclidean feature distance ($\le 1.50\sigma$) across Pace, 3PT attempt rate, Paint attempt rate, and PnR Switch rate.

---

## 4 Forensic Scouting Pillars

1. **Pace, Game Flow & Transition:** Neutral pace (differential $\le 5$ pts), early shot-clock frequency (0–8s), mid shot-clock frequency (8–15s), late shot-clock pressure (15–24s), and game script tempo under 10+ point leads or deficits.
2. **Perimeter & Paint Defense:** Pick & Roll coverage frequencies (Drop, Switch, Ice, Hedge), Restricted Area allowed PPP, Paint non-RA allowed PPP, Corner 3 allowed PPP, Wide-Open 3PT rate allowed, and Defensive Rebounding percentage (DREB%).
3. **Offensive Play Type & Scheme:** Frequency and points per possession across PnR Ball Handler, Spot-Up, and Isolation play types, plus live-ball vs. dead-ball turnover splits.
4. **Player Usage Profiles:** Top 4 rotation players with empirical 10-game distributions (25th percentile Floor, 50th percentile Median, 75th percentile Ceiling) alongside USG%, AST%, and situational clutch volume.

---

## Key Features & Customizations

* **Garbage Minutes Filter (`Min Mins Avg`):** Configurable in the Matchup Lab header controls bar (default: 20 min/game). Filters out end-of-bench and garbage-time players from positional allowances, defensive correlation baselines, and player streak evaluations.
* **Pure Individual Metrics Only:** Cleanly separated into single-category metrics (`PTS`, `REB`, `AST`, `3PM`). All combo aggregates (`PRA`, `PR`, `PA`, `RA`) have been removed across all tabs and modals for precision scouting.
* **Dynamic Positional Matchup Lab:** Real-time ANOVA statistical significance testing, t-tests, and Cohen's d effect sizes across individual positions (PG, SG, SF, PF, C) and grouped tiers (Guards, Wings, Bigs).
* **Player Floor Streaks:** Unbroken consecutive game streaks exceeding empirical floor thresholds ((1 - margin) $\times$ baseline median).

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
python test_position_correlations.py
```

---

## Architecture & Project Structure

```
nba_stats/
├── app.py                           # Core Flask application and REST routes
├── forensic_data.py                 # Multi-season tracking data pipeline & Euclidean analog model
├── daily_update.py                  # Automated daily game log sync
├── position_correlations.py         # Statistical ANOVA & positional correlation engine
├── verify_forensic.py               # Comprehensive verification and compliance suite (17/17 audits)
├── test_position_correlations.py    # Positional unit test suite (18/18 tests)
├── schedule_games.json              # Full 2025-26 & 2026-27 schedule with box scores
├── forensic_teams.json              # Team 4-pillar baselines and allowed distributions
├── forensic_players.json            # Top 4 player rotation percentile profiles
├── forensic_similar_opponents.json  # Precomputed Euclidean opponent clusters
├── forensic_position_correlations.json # Precomputed positional correlations (960 records)
├── forensic_team_game_logs.json     # Per-game tracking logs for dynamic date range recalculations
├── static/
│   ├── style.css                    # Unified dark-mode stylesheet & tooltip layout
│   └── js/
│       ├── app.js                   # Dashboard state, date filtering, min minutes & schedule grid
│       ├── visuals.js               # Responsive SVG shot zones, play types & pace gauge
│       ├── modal.js                 # Deep Dive modal, 4-pillar stats, and callout generator
│       ├── positional.js            # Positional matchup correlation table and filters
│       └── profiles.js              # Player floor streaks table, slate chips, and sorting
├── templates/
│   └── index.html                   # Semantic HTML template
└── vercel.json                      # Serverless packaging configuration
```

---

## 📋 Engineering Handoff & Checklist

### ✅ Completed
- [x] **Combo Metrics Removal:** Completely removed all combo stats (`PRA`, `PR`, `PA`, `RA`) across the entire engine:
  - Positional defense allowances (`c_pra_allowed`, `pf_pra_allowed`, `sf_pra_allowed`, `sg_pra_allowed`, `pg_pra_allowed`).
  - Positional matchup correlations (`position_correlations.py`, `METRICS = ['PTS', 'REB', 'AST', 'FG3M']`).
  - Positional matchup UI tab and metric selector pills (`templates/index.html`, `static/js/positional.js`).
  - Matchup comparison cards and deep-dive modals (`static/js/app.js`, `public/static/js/app.js`).
  - Player floor streaks filter bar (`templates/index.html`, `static/js/profiles.js`, `public/static/js/profiles.js`).
- [x] **Garbage Minutes Filter (Minimum Minutes Avg per Player):**
  - Added interactive `Min Mins Avg` input box in the Matchup Lab top control bar (`templates/index.html`, default `20`).
  - Filtered opponent positional allowances and correlations by player average minutes (`player_avg_min >= min_minutes`).
  - Dynamic API integration: `/api/positional-correlations?min_minutes=...`.
  - Filtered Player Floor Streaks table and `/api/floor-streak` to exclude players averaging below the specified minimum minutes.
  - Regenerated `forensic_position_correlations.json` (960 records) and `forensic_teams.json`.
- [x] **Thermonuclear Line Count Standard:** Confirmed all `.py` and `.js` files remain strictly under 1,000 lines.
- [x] **Audit & Unit Tests Passing:**
  - `python verify_forensic.py` passed with 100% compliance across all 17 audit suites.
  - `python test_position_correlations.py` passed with 18/18 unit tests OK.

### 📝 Next Deployment / Follow-Up Items
- [ ] **Vercel Remote Deployment Sync:** Verify live Vercel deployment builds cleanly and reflects updated JSON data.
- [ ] **Daily Sync Verification:** Confirm `python daily_update.py` scheduled job continues regular season log updates cleanly.

---

*NBA Forensic Scouting Engine • Strictly Descriptive Analytics*
