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
VM_NAME = "instance-20260403-034225"
ZONE = "us-central1-b"
VM_REMOTE_CMD = "source ~/nba_env/bin/activate && python ~/nba_model.py"
REQUIRED_CSVS = ['logs.csv', 'positions.csv', 'injuries.csv', 'todays_games.csv', 'nba_model.py']
AUTO_PUSH = True
SCP_RETRIES = 3
TIMEOUT_SSH = 1800  # 30 minutes
TIMEOUT_SCP = 300   # 5 minutes

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

def run_gcloud(cmd_list):
    """Wait for gcloud command to finish and return result."""
    try:
        result = subprocess.run(cmd_list, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Gcloud error: {e.stderr}")
        return None

def manage_cloud_execution():
    print(f"\n--- Cloud Orchestration: {VM_NAME} ---")
    
    # 1. Check VM Status and Start if needed
    print(f"Checking status of VM {VM_NAME}...")
    status = run_gcloud(["compute", "instances", "describe", VM_NAME, "--zone", ZONE, "--format", "get(status)"])
    
    if status != "RUNNING":
        print(f"Starting VM {VM_NAME} (Current status: {status})...")
        subprocess.run(f"gcloud compute instances start {VM_NAME} --zone {ZONE}", shell=True, check=True)
        # Wait for SSH to be ready (approximate)
        print("Waiting 30 seconds for VM to boot...")
        time.sleep(30)
    else:
        print(f"VM {VM_NAME} is already running.")

    # 2. Upload CSV files with absolute paths and retries
    print(f"Uploading files to @{VM_NAME}...")
    abs_csvs = [os.path.join(SCRIPT_DIR, f) for f in REQUIRED_CSVS]
    scp_cmd = f"gcloud compute scp {' '.join(abs_csvs)} {VM_NAME}: --zone {ZONE}"
    
    success = False
    for i in range(SCP_RETRIES):
        try:
            print(f"Attempt {i+1}/{SCP_RETRIES} to upload files...")
            subprocess.run(scp_cmd, shell=True, check=True, timeout=TIMEOUT_SCP)
            success = True
            break
        except Exception as e:
            print(f"Upload attempt {i+1} failed: {e}")
            if i < SCP_RETRIES - 1:
                time.sleep(10)
    
    if not success:
        print("[!] All upload attempts failed.")
        raise Exception("Failed to upload required files to VM.")

    # 3. Execute Model Remote
    print("Executing model remotely (Estimated time: 10-15 minutes)...")
    # Using 'python -u' for unbuffered output to keep the connection alive with real-time logs
    remote_cmd = VM_REMOTE_CMD.replace("python", "python -u")
    ssh_cmd = f'gcloud compute ssh {VM_NAME} --zone {ZONE} --command "bash -c \'{remote_cmd}\'"'
    
    try:
        # Run with a generous timeout to prevent infinite hangs
        subprocess.run(ssh_cmd, shell=True, check=True, timeout=TIMEOUT_SSH)
        
        # 4. Pull Results Back (Only if model finishes successfully)
        print("Downloading results...")
        local_rf_path = os.path.join(SCRIPT_DIR, "rf_predictions.csv")
        local_hist_path = os.path.join(SCRIPT_DIR, "prediction_history.csv")
        temp_result_path = os.path.join(SCRIPT_DIR, "new_predictions.csv")
        
        pull_cmd = f"gcloud compute scp {VM_NAME}:rf_predictions.csv {temp_result_path} --zone {ZONE}"
        subprocess.run(pull_cmd, shell=True, check=True, timeout=TIMEOUT_SCP)
        
        # 1. Update Featured Picks (Today only - simply overwrite)
        os.replace(temp_result_path, local_rf_path)
        print("Updated Featured Picks (rf_predictions.csv) with today's results.")
        
        # 2. Update Model History (Append and deduplicate)
        try:
            new_df = pd.read_csv(local_rf_path)
            if os.path.exists(local_hist_path):
                hist_df = pd.read_csv(local_hist_path)
                # Combine, deduplicate by Date/Player/Matchup, and sort by Date (descending)
                updated_hist = pd.concat([hist_df, new_df]).drop_duplicates(subset=['Date', 'Player', 'Matchup'], keep='last')
                updated_hist['Date'] = pd.to_datetime(updated_hist['Date'])
                updated_hist.sort_values(by='Date', ascending=False, inplace=True)
                updated_hist.to_csv(local_hist_path, index=False)
                print(f"Successfully archived results to {local_hist_path}.")
            else:
                new_df.to_csv(local_hist_path, index=False)
                print(f"Initialized history log: {local_hist_path}.")
        except Exception as e:
            print(f"Warning: Could not update history log: {e}")
        
        # 5. Stop VM to save costs
        print(f"Shutting down {VM_NAME}...")
        subprocess.run(f"gcloud compute instances stop {VM_NAME} --zone {ZONE}", shell=True, check=True)

    except subprocess.TimeoutExpired:
        print(f"\n[!] Model execution TIMED OUT after {TIMEOUT_SSH/60} minutes.")
        print(f"[!] Stopping VM {VM_NAME} for cost safety.")
        subprocess.run(f"gcloud compute instances stop {VM_NAME} --zone {ZONE}", shell=True)
        raise Exception("Remote execution timed out.")
    except subprocess.CalledProcessError as e:
        print(f"\n[!] Model execution failed or connection dropped: {e}")
        print(f"[!] Stopping VM {VM_NAME} for cost safety.")
        subprocess.run(f"gcloud compute instances stop {VM_NAME} --zone {ZONE}", shell=True)
        raise e
    except Exception as e:
        print(f"\n[!] An unexpected error occurred: {e}")
        print(f"[!] Stopping VM {VM_NAME} for cost safety.")
        subprocess.run(f"gcloud compute instances stop {VM_NAME} --zone {ZONE}", shell=True)
        raise e


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
    print(f"[{start_time}] Starting consolidated NBA daily update...")
    
    # Local Data Fetching
    if fetch_logs() and update_todays_games() and fetch_injuries():
        # Cloud Execution
        try:
            manage_cloud_execution()
        except Exception as cloud_err:
            print(f"Cloud workflow failed: {cloud_err}")
            return

        # GitHub Sync
        if AUTO_PUSH:
            push_to_github()
            
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\n[{end_time}] Process complete! Time taken: {duration}")
    else:
        print("Data fetching failed. Aborting cloud execution.")

if __name__ == "__main__":
    main()
