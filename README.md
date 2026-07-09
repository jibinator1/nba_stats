# NBA Matchup Hub 🏀

A data-driven dashboard for analyzing NBA defensive vulnerabilities and finding exploitable player matchups.

**Live site:** https://nba-stats-silk.vercel.app/

---

## Quick Start (Local)

### Prerequisites
- Python 3.9+

### Setup

```bash
# 1. Navigate to the project folder
cd "f:\learning to code\nba_website\nba_stats"

# 2. (Recommended) Create a virtual environment
python -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the server
python app.py
```

Then open **http://127.0.0.1:5000** in your browser.

> **Note:** On first run, the app will automatically fetch the latest NBA game logs from the NBA API if `logs.csv` is missing or outdated. This can take a minute.

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `Flask` | Web server |
| `pandas` | Data processing |
| `nba_api` | NBA stats data source |
| `scikit-learn` | ML scoring |
| `requests` | HTTP calls |
| `gunicorn` | Production server (Vercel) |

---

## Features

### Defensive Heatmap (Home Tab)
- Ranks all 30 teams on how much they give up to each position (PG, SG, SF, PF, C)
- **Rank 1** = worst defense (most exploitable), **Rank 30** = stingiest
- **Matchup Score** = aggregated softness score across all stats (higher = better matchup)
- **Trend arrows** (↑↓→) compare season avg vs. last N games

### Today's Matchups
- Dropdown at the top auto-loads today's NBA schedule
- Selecting a game auto-fills the team comparison inputs

### Pick Finder (Good Matchups Tab)
- Automatically flags players facing weak defenses tonight
- **OVER**: player faces a leaky defense for a specific stat
- **UNDER**: player faces an elite, stifling defense

### Team Radar Profile
- Click any team row to open a radar chart
- Shows exploitability across 8 metrics: PTS, REB, AST, eFG, 3PM, TOV, DEF RTG, PACE
- Score of **100** = peak exploitability

---

## Refreshing Data

Data is cached daily in `vs_Position_withavg.csv`.

- **Manual refresh:** Click **"Update Analysis"** on the website to rebuild with custom thresholds.
- **Custom filters:** Set the minimum minutes played and last-N-games window from the UI.
- The app auto-updates logs from the NBA API on startup if they are stale.

---

## Project Structure

```
nba_stats/
├── app.py              # Flask routes and data logic
├── update.py           # NBA API fetching & CSV generation
├── daily_update.py     # Scheduled refresh script
├── vs_Position_withavg.csv  # Pre-computed defensive stats (cached)
├── positions.csv       # Player position data
├── logs.csv            # Raw game logs
├── todays_games.csv    # Today's schedule
├── requirements.txt
├── templates/          # Jinja2 HTML templates
├── static/             # CSS, JS, assets
└── vercel.json         # Vercel serverless config
```

---

*Created by Jibin Im*
