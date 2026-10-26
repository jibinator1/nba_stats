import os
import sys
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

# Standard 30 NBA Teams
NBA_TEAMS = {
    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
    'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
    'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
}

ESPN_TO_NBA = {
    'GS': 'GSW',
    'NO': 'NOP',
    'NY': 'NYK',
    'SA': 'SAS',
    'UTAH': 'UTA',
    'WSH': 'WAS'
}

def normalize_team(abbr):
    abbr = abbr.strip().upper()
    return ESPN_TO_NBA.get(abbr, abbr)

def fetch_date_events(date_str):
    url = f"{ESPN_SCOREBOARD_URL}?dates={date_str}"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            return r.json().get('events', [])
    except Exception as e:
        print(f"Error fetching date {date_str}: {e}")
    return []

def main():
    print("Fetching calendar from ESPN NBA API...")
    r = requests.get(ESPN_SCOREBOARD_URL, timeout=12)
    if r.status_code != 200:
        print(f"Failed to fetch calendar from ESPN: status {r.status_code}")
        sys.exit(1)

    data = r.json()
    calendar = data.get('leagues', [{}])[0].get('calendar', [])
    if not calendar:
        print("No calendar entries found in ESPN response.")
        sys.exit(1)

    # Filter for 2026-27 regular season dates (starting opening night 2026-10-20 through April 2027)
    reg_dates = [d[:10].replace('-', '') for d in calendar if d >= '2026-10-20']
    print(f"Found {len(reg_dates)} regular season game dates ({reg_dates[0]} to {reg_dates[-1]}).")

    print(f"Fetching game events for all {len(reg_dates)} dates concurrently...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=10) as executor:
        all_results = list(executor.map(fetch_date_events, reg_dates))
    t1 = time.time()
    print(f"Completed ESPN queries in {t1 - t0:.2f} seconds.")

    all_events = [ev for ev_list in all_results for ev in ev_list]
    print(f"Total raw events fetched from ESPN: {len(all_events)}")

    # Load existing schedule to preserve NBA IDs and final scores for completed games
    existing_sched_file = os.path.join(SCRIPT_DIR, 'schedule_games.json')
    existing_lookup = {}
    if os.path.exists(existing_sched_file):
        try:
            with open(existing_sched_file, 'r', encoding='utf-8') as f:
                existing_games = json.load(f)
                for g in existing_games:
                    key = (g.get('gameday'), frozenset([g.get('away_team'), g.get('home_team')]))
                    existing_lookup[key] = g
            print(f"Loaded {len(existing_lookup)} existing game records for metadata preservation.")
        except Exception as e:
            print(f"Warning: Could not read existing schedule_games.json: {e}")

    parsed_games = []
    seen_ids = set()
    skipped_tbd = 0

    for ev in all_events:
        ev_id = str(ev.get('id', ''))
        if not ev_id or ev_id in seen_ids:
            continue

        competitions = ev.get('competitions', [])
        if not competitions:
            continue
        comp = competitions[0]
        competitors = comp.get('competitors', [])
        if len(competitors) < 2:
            continue

        home_comp = next((c for c in competitors if c.get('homeAway') == 'home'), None)
        away_comp = next((c for c in competitors if c.get('homeAway') == 'away'), None)
        if not home_comp or not away_comp:
            continue

        home_raw = home_comp.get('team', {}).get('abbreviation', '')
        away_raw = away_comp.get('team', {}).get('abbreviation', '')
        home_team = normalize_team(home_raw)
        away_team = normalize_team(away_raw)

        # Exclude placeholder / TBD bracket games
        if home_team not in NBA_TEAMS or away_team not in NBA_TEAMS:
            skipped_tbd += 1
            continue

        # Parse tipoff time in US Eastern Time
        iso_date = ev.get('date', '')
        try:
            dt = datetime.fromisoformat(iso_date).astimezone(ZoneInfo('America/New_York'))
            gameday = dt.strftime('%Y-%m-%d')
            gametime = dt.strftime('%I:%M %p ET').lstrip('0')
        except Exception:
            gameday = iso_date[:10]
            gametime = '7:30 PM ET'

        status = comp.get('status', {})
        status_type = status.get('type', {})
        is_final = bool(status_type.get('completed', False))

        away_score = None
        home_score = None
        if away_comp.get('score') is not None and str(away_comp.get('score')).isdigit():
            away_score = int(away_comp.get('score'))
        if home_comp.get('score') is not None and str(home_comp.get('score')).isdigit():
            home_score = int(home_comp.get('score'))

        # Check existing match to preserve NBA Game IDs and scores if present
        match_key = (gameday, frozenset([away_team, home_team]))
        chosen_game_id = ev_id
        if match_key in existing_lookup:
            ex = existing_lookup[match_key]
            if ex.get('game_id'):
                chosen_game_id = str(ex['game_id'])
            if ex.get('is_final'):
                is_final = True
                if ex.get('away_score') is not None:
                    away_score = ex['away_score']
                if ex.get('home_score') is not None:
                    home_score = ex['home_score']

        parsed_games.append({
            'game_id': chosen_game_id,
            'gameday': gameday,
            'away_team': away_team,
            'home_team': home_team,
            'away_score': away_score,
            'home_score': home_score,
            'is_final': is_final,
            'gametime': 'FINAL' if is_final else gametime
        })
        seen_ids.add(ev_id)

    # Sort games chronologically by date and game_id
    parsed_games.sort(key=lambda g: (g['gameday'], g['game_id']))
    print(f"Parsed {len(parsed_games)} valid regular season games (skipped {skipped_tbd} TBD placeholders).")

    # Save to schedule_games.json
    with open(existing_sched_file, 'w', encoding='utf-8') as f:
        json.dump(parsed_games, f, indent=2)

    print(f"Successfully saved {len(parsed_games)} games to {existing_sched_file}!")

if __name__ == '__main__':
    main()
