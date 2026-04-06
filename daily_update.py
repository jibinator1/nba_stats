import os
import sys
import time
import subprocess
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from nba_api.stats.endpoints import playergamelogs

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
AUTO_PUSH = True

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

def fetch_logs():
    print("Fetching latest game logs from NBA API...")
    try:
        logs = playergamelogs.PlayerGameLogs(season_nullable='2025-26', headers=headers).get_data_frames()[0]
        logs.to_csv(os.path.join(SCRIPT_DIR, "logs.csv"), index=False)
        print("Successfully updated logs.csv!")
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
    try:
        r = requests.get(url, params=params, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            result_sets = data.get('resultSets', [])
            line_score = next((rs for rs in result_sets if rs['name'] == 'LineScore'), None)
            if line_score:
                df = pd.DataFrame(line_score['rowSet'], columns=line_score['headers'])
                df[['GAME_ID', 'TEAM_ABBREVIATION']].to_csv(os.path.join(SCRIPT_DIR, 'todays_games.csv'), index=False)
                print("Successfully updated todays_games.csv")
                return True
    except Exception as e:
        print(f"Error updating today's games: {e}")
    return False

def fetch_injuries():
    print("Fetching injury report from CBS Sports...")
    try:
        url = "https://www.cbssports.com/nba/injuries/"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, 'html.parser')
        injured_players = [a.text.strip() for a in soup.select('span.CellPlayerName--long a')]
        pd.Series(injured_players, name="Player").to_csv(os.path.join(SCRIPT_DIR, "injuries.csv"), index=False)
        print(f"Successfully updated injuries.csv ({len(injured_players)} players)")
        return True
    except Exception as e:
        print(f"Error fetching injuries: {e}")
    return False

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
    start_time = datetime.now()
    print(f"[{start_time}] Starting NBA daily update...")
    
    # Local Data Fetching
    if fetch_logs() and update_todays_games() and fetch_injuries():
        if AUTO_PUSH:
            push_to_github()
            
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\n[{end_time}] Process complete! Time taken: {duration}")
    else:
        print("Data fetching failed.")

if __name__ == "__main__":
    main()
