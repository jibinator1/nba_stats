# NBA Matchup Hub 🏀

A premium, data-driven dashboard for analyzing NBA defensive vulnerabilities and finding exploitable player matchups. You can access it using this link: https://nba-stats-silk.vercel.app/

## How It Works

### 1. Opponent Defensive Ranks
The main dashboard provides a "Heatmap" of the league's defensive performance against specific positions (PG, SG, SF, PF, C).
- **Ranking System**: A **Rank 1** indicates the *worst* defense (most points/rebounds/assists allowed), while a **Rank 30** indicates the *stinger* defense.
- **Matchup Score**: An aggregated metric (0-30+) that summarizes how "soft" a team is across all major stats. The higher the score, the better the matchup.
- **Trend Indicators**: The arrows ($\uparrow, \downarrow, \rightarrow$) compare a team's **Season Average** vs. their **Last X Games** (default 20). 
    - $\uparrow$ means they are allowing *more* of that stat recently (getting softer).
    - $\downarrow$ means they are tightening up.

### 2. Today's Matchups (Dropdown)
Located at the top of the page, this dropdown automatically fetches the current day's NBA schedule. Selecting a game will auto-fill the team inputs for instantaneous comparison.

### 3. Pick Finder (Automated Analysis)
Navigate to the **Good Matchups** tab to see the **Pick Finder**. 
- It automatically evaluates tonight's games against our defensive depth charts.
- **OVER**: Flags players facing a leaky defense that allows high volume in a specific stat.
- **UNDER**: Flags players facing a stifling, elite defense.

### 4. Interactive Radar Profile
Click on any team row to slide out the **Team Profile Panel**. This shows a radar chart of their "Exploitability" across 8 key metrics (Points, Rebounds, Assists, eFG, 3PM, TOV, Def Rating, and Pace). A score of **100** on the radar represents a peak exploitability (the softest possible matchup).

---

## Running Locally

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Start the Server**:
   ```bash
   python app.py
   ```

3. **Open in Browser**:
   Visit [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Data Refresh
The data is cached daily. To force a refresh:
- Click **"Update Analysis"** on the website to rebuild the `vs_Position_withavg.csv` with your custom thresholds.
- The `app.py` automatically fetches the latest logs from the NBA API if they are missing or outdated.

---
*Created by Jibin Im*
