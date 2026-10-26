import os
import sys
import time
import json
import argparse
import subprocess
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from nba_api.stats.endpoints import playergamelogs

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
AUTO_PUSH = True
LAST_RUN_FILE = os.path.join(SCRIPT_DIR, "last_run.txt")

# NBA API Headers
headers = {
    "Host": "stats.nba.com",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Accept-Language": "en-US,en;q=0.9"
}

ESPN_TO_NBA = {
    'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'SA': 'SAS', 'UTAH': 'UTA', 'WSH': 'WAS'
}

def get_current_season():
    """Dynamically determines active NBA season string (e.g. '2026-27')."""
    now = datetime.now()
    if now.month >= 10:
        return f"{now.year}-{str(now.year + 1)[-2:]}"
    else:
        return f"{now.year - 1}-{str(now.year)[-2:]}"

def has_run_today():
    """Checks if the daily update has already executed today."""
    if not os.path.exists(LAST_RUN_FILE):
        return False
    try:
        with open(LAST_RUN_FILE, 'r', encoding='utf-8') as f:
            last_date = f.read().strip().split()[0]
        today_str = datetime.now().strftime('%Y-%m-%d')
        return last_date == today_str
    except Exception:
        return False

def record_run_success():
    """Records successful execution timestamp to last_run.txt."""
    try:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(LAST_RUN_FILE, 'w', encoding='utf-8') as f:
            f.write(now_str + '\n')
        print(f"[Tracker] Recorded successful sync at {now_str} in last_run.txt")
    except Exception as e:
        print(f"[Tracker Warning] Could not record last run timestamp: {e}")

def update_schedule_scores(logs_df=None):
    """Updates completed games in schedule_games.json with final scores and is_final status."""
    sched_path = os.path.join(SCRIPT_DIR, 'schedule_games.json')
    if not os.path.exists(sched_path):
        return
    if logs_df is None:
        logs_path = os.path.join(SCRIPT_DIR, 'logs.csv')
        if not os.path.exists(logs_path):
            return
        logs_df = pd.read_csv(logs_path)
    
    if logs_df.empty:
        return

    try:
        print("Synchronizing completed game box scores to schedule_games.json...")
        with open(sched_path, 'r', encoding='utf-8') as f:
            schedule = json.load(f)

        df = logs_df.copy()
        df['DATE_STR'] = pd.to_datetime(df['GAME_DATE']).dt.strftime('%Y-%m-%d')
        team_scores = df.groupby(['GAME_ID', 'DATE_STR', 'TEAM_ABBREVIATION'])['PTS'].sum().reset_index()

        game_lookup = {}
        for (gid, d_str), grp in team_scores.groupby(['GAME_ID', 'DATE_STR']):
            pts_map = dict(zip(grp['TEAM_ABBREVIATION'], grp['PTS']))
            if len(pts_map) == 2:
                team_key = frozenset(pts_map.keys())
                game_lookup[(d_str, team_key)] = {
                    'game_id': str(gid).lstrip('0'),
                    'scores': pts_map
                }

        updated_count = 0
        for g in schedule:
            gday = g.get('gameday')
            away = g.get('away_team')
            home = g.get('home_team')
            key = (gday, frozenset([away, home]))
            if key in game_lookup:
                info = game_lookup[key]
                g['away_score'] = int(info['scores'].get(away, 0))
                g['home_score'] = int(info['scores'].get(home, 0))
                g['is_final'] = True
                g['game_id'] = info['game_id']
                updated_count += 1

        with open(sched_path, 'w', encoding='utf-8') as f:
            json.dump(schedule, f, indent=2)
        print(f"Successfully updated {updated_count} completed games in schedule_games.json!")
    except Exception as e:
        print(f"Warning: Failed to update schedule scores: {e}")

def fetch_logs():
    current_season = get_current_season()
    print(f"Fetching latest game logs for {current_season} regular season from NBA API...")
    logs_file = os.path.join(SCRIPT_DIR, "logs.csv")
    try:
        current_logs = playergamelogs.PlayerGameLogs(
            season_nullable=current_season,
            season_type_nullable='Regular Season',
            headers=headers
        ).get_data_frames()[0]
        
        if os.path.exists(logs_file):
            try:
                existing_logs = pd.read_csv(logs_file)
                # Normalize GAME_ID (strip leading zeros / str) and PLAYER_ID before merging
                existing_logs['GAME_ID'] = existing_logs['GAME_ID'].astype(str).str.lstrip('0')
                current_logs['GAME_ID'] = current_logs['GAME_ID'].astype(str).str.lstrip('0')
                existing_logs['PLAYER_ID'] = existing_logs['PLAYER_ID'].astype(int)
                current_logs['PLAYER_ID'] = current_logs['PLAYER_ID'].astype(int)

                combined_logs = pd.concat([existing_logs, current_logs], ignore_index=True)
                combined_logs.drop_duplicates(subset=['GAME_ID', 'PLAYER_ID'], keep='last', inplace=True)
                combined_logs.to_csv(logs_file, index=False)
                print(f"Successfully updated logs.csv! Total records: {len(combined_logs)} (Current season: {len(current_logs)})")
                update_schedule_scores(combined_logs)
            except Exception as e:
                print(f"Warning: Could not merge existing logs ({e}), saving current season logs...")
                current_logs['GAME_ID'] = current_logs['GAME_ID'].astype(str).str.lstrip('0')
                current_logs.to_csv(logs_file, index=False)
                update_schedule_scores(current_logs)
        else:
            current_logs['GAME_ID'] = current_logs['GAME_ID'].astype(str).str.lstrip('0')
            current_logs.to_csv(logs_file, index=False)
            print("Successfully created logs.csv!")
            update_schedule_scores(current_logs)

        return True
    except Exception as e:
        print(f"Error fetching logs: {e}")
        return False

def update_todays_games():
    print("Fetching today's schedule from NBA API...")
    url = "https://stats.nba.com/stats/scoreboardv2"
    params = {
        'DayOffset': '0',
        'LeagueID': '00',
        'gameDate': datetime.now().strftime('%m/%d/%Y')
    }
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                result_sets = data.get('resultSets', [])
                line_score = next((rs for rs in result_sets if rs['name'] == 'LineScore'), None)
                if line_score:
                    df = pd.DataFrame(line_score['rowSet'], columns=line_score['headers'])
                    df[['GAME_ID', 'TEAM_ABBREVIATION']].to_csv(os.path.join(SCRIPT_DIR, 'todays_games.csv'), index=False)
                    print("Successfully updated todays_games.csv from NBA API")
                    return True
        except Exception as e:
            print(f"Attempt {attempt + 1} updating today's games via NBA API: {e}")
            time.sleep(1)

    print("Falling back to ESPN Scoreboard API for today's games...")
    try:
        today_espn = datetime.now().strftime('%Y%m%d')
        espn_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={today_espn}"
        r = requests.get(espn_url, timeout=10)
        if r.status_code == 200:
            events = r.json().get('events', [])
            rows = []
            for ev in events:
                gid = str(ev.get('id', ''))
                comps = ev.get('competitions', [{}])[0].get('competitors', [])
                for comp in comps:
                    raw_abbr = comp.get('team', {}).get('abbreviation', '')
                    abbr = ESPN_TO_NBA.get(raw_abbr, raw_abbr)
                    rows.append({'GAME_ID': gid, 'TEAM_ABBREVIATION': abbr})
            if rows:
                pd.DataFrame(rows).to_csv(os.path.join(SCRIPT_DIR, 'todays_games.csv'), index=False)
                print(f"Successfully updated todays_games.csv from ESPN ({len(events)} matchups)")
                return True
    except Exception as e:
        print(f"ESPN fallback for today's games failed: {e}")

    print("Notice: Could not refresh todays_games.csv from API; using existing cache if present.")
    return os.path.exists(os.path.join(SCRIPT_DIR, 'todays_games.csv'))

def fetch_injuries():
    print("Fetching injury report from CBS Sports...")
    try:
        url = "https://www.cbssports.com/nba/injuries/"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        injured_players = [a.text.strip() for a in soup.select('span.CellPlayerName--long a')]
        if injured_players:
            pd.Series(injured_players, name="Player").to_csv(os.path.join(SCRIPT_DIR, "injuries.csv"), index=False)
            print(f"Successfully updated injuries.csv ({len(injured_players)} players)")
            return True
    except Exception as e:
        print(f"Error fetching injuries: {e}")
        
    print("Notice: Could not refresh injuries.csv; using existing cache if present.")
    return os.path.exists(os.path.join(SCRIPT_DIR, "injuries.csv"))

def push_to_github():
    print("\n--- Syncing to GitHub ---")
    try:
        os.chdir(SCRIPT_DIR)
        subprocess.run('git add .', shell=True, check=True)
        commit_msg = f"Auto-update: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(f'git commit -m "{commit_msg}"', shell=True, check=True)
        subprocess.run('git push', shell=True, check=True)
        print("Successfully pushed to GitHub!")

    except Exception as e:
        print(f"Git sync failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="NBA Website Daily Data Ingestion")
    parser.add_argument("--force", action="store_true", help="Force run even if already executed today")
    args = parser.parse_args()

    start_time = datetime.now()
    today_str = start_time.strftime('%Y-%m-%d')
    print(f"[{start_time}] Starting NBA daily update...")

    if not args.force and has_run_today():
        print(f"[SKIP] NBA daily update has already completed today ({today_str}).")
        print("Use '--force' if you wish to override and run again.")
        return
    
    # Local Data Fetching & Forensic Processing
    from forensic_data import run_forensic_pipeline
    logs_ok = fetch_logs()
    update_todays_games()
    fetch_injuries()

    if logs_ok:
        print("Executing forensic scouting analytics pipeline...")
        run_forensic_pipeline()
        if AUTO_PUSH:
            push_to_github()
        record_run_success()
            
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\n[{end_time}] Process complete! Time taken: {duration}")
    else:
        print("Game log fetching failed.")

if __name__ == "__main__":
    main()
