# NBA Forensic Scouting Engine 🏀

A strictly descriptive, play-by-play, and tracking-backed NBA matchup analytics platform. Built on multi-season historical distributions, sample-gated percentiles, and 4-pillar forensic structural alignment.

**Live site:** https://nba-stats-silk.vercel.app/

---

## Forensic Philosophy

* **No Predictions / No Betting Language:** The engine does not emit projections, subjective labels, or recommendations.
* **Universal Rankings:** All teams and players with recorded game data are explicitly ranked across all metrics, shot zones, and play types regardless of sample size.
* **Date Range Gating:** All metrics and distributions dynamically adapt to selected date windows. Matches with zero completed games in a chosen window cleanly render unskewed blank states.
* **Euclidean Analogs ($k$-NN):** Similar-opponent defense matching uses $k$-Nearest Neighbors ($k=5$) with an adaptive distance threshold over pure offensive style features (Pace, 3PT attempt rate, Paint touch rate, Transition frequency).
* **Leave-One-Out (LOO) Deltas:** Player baselines strictly exclude the game being evaluated to eliminate target leakage and delta dampening.
* **Minutes-Weighted Scheme Fidelity:** Matchup aggregations report both unweighted and minutes-weighted metrics to prevent reserve minutes from masking scheme impact against primary stars.
* **Detailed Mathematical Specification:** See [DOCUMENTATION.md](./DOCUMENTATION.md) for full statistical formulations, LaTeX math, and code variable mapping.

---

## 4 Forensic Scouting Pillars

1. **Pace, Game Flow & Transition:** Neutral pace (differential $\le 5$ pts), early shot-clock frequency (0–8s), mid shot-clock frequency (8–15s), late shot-clock pressure (15–24s), and game script tempo under 10+ point leads or deficits.
2. **Perimeter & Paint Defense:** Pick & Roll coverage frequencies (Drop, Switch, Ice, Hedge), Restricted Area allowed PPP, Paint non-RA allowed PPP, Corner 3 allowed PPP, Wide-Open 3PT rate allowed, and Defensive Rebounding percentage (DREB%).
3. **Offensive Play Type & Scheme:** Frequency and points per possession across PnR Ball Handler, Spot-Up, and Isolation play types, plus live-ball vs. dead-ball turnover splits.
4. **Similar-Opponent Analogs & Player Profiles:** $k$-Nearest Neighbors ($k=5$) Euclidean clustering over offensive style vectors, alongside top 4 rotation player empirical 10-game distributions (25th percentile Floor, 50th percentile Median, 75th percentile Ceiling) with USG%, AST%, and situational clutch volume.

---

## Key Features & Customizations

* **Leave-One-Out (LOO) Baseline Math:** $\Delta_{p,i}^{\text{LOO}} = m_{p,i} - \overline{m}_{p,-i}$ guarantees that early-season small samples and evaluated games never dampen defensive deltas.
* **Minutes-Weighted vs. Unweighted Aggregation:** High-minute stars (e.g. 38 mins) are weighted appropriately alongside 20-minute rotation players (`mean_diff` and `weighted_mean_diff`).
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
- [x] **Leave-One-Out (LOO) Player Baseline Calculation:** Eliminated target leakage and dampening in player deltas ($m_{p,i} - \overline{m}_{p,-i}$).
- [x] **Minutes-Weighted Aggregation:** Added minutes-weighted positional deltas (`weighted_mean_diff` / `weighted_diff`) alongside unweighted metrics.
- [x] **Pure Offensive Style Vectors:** Separated offensive identity (`[Pace, 3PT Rate, Paint Touch Rate, Transition Freq]`) from defensive schemes (PnR Switch Rate).
- [x] **k-NN (k=5) Outlier Fallback:** Refined stylistic clustering with adaptive thresholds ($\overline{d} + 1.50\sigma_d$) and guaranteed $k=5$ nearest-neighbor fallback.
- [x] **Comprehensive Technical Documentation:** Authored [DOCUMENTATION.md](./DOCUMENTATION.md) detailing 4 pillars, LaTeX formulas, and variable mappings.
- [x] **Thermonuclear Line Count Standard:** Confirmed all `.py` and `.js` files remain strictly under 1,000 lines.
- [x] **Audit & Unit Tests Passing:**
  - `python verify_forensic.py` passed with 100% compliance across all 17 audit suites.
  - `python test_position_correlations.py` passed with 22/22 unit tests OK.

### 📝 Next Deployment / Follow-Up Items
- [ ] **Vercel Remote Deployment Sync:** Verify live Vercel deployment builds cleanly and reflects updated JSON data.
- [ ] **Daily Sync Verification:** Confirm `python daily_update.py` scheduled job continues regular season log updates cleanly.

---

*NBA Forensic Scouting Engine • Strictly Descriptive Analytics*
