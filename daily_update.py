import os
import sys
from datetime import datetime
import pandas as pd

# Add the current directory to sys.path to ensure we can import update.py
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from update import fetch_logs, make_data, update_todays_games_local

# --- Configuration ---
CSV_FILE = os.path.join(current_dir, 'vs_Position_withavg.csv')
POSITIONS_FILE = os.path.join(current_dir, 'positions.csv')
LOGS_FILE = os.path.join(current_dir, 'logs.csv')
MINUTES_CUTOFF = 25
AUTO_PUSH = True  # Set to True to push to GitHub automatically

def should_update():
    if not os.path.exists(CSV_FILE):
        return True
    
    # Get last modified time
    last_modified = os.path.getmtime(CSV_FILE)
    last_modified_date = datetime.fromtimestamp(last_modified).date()
    today = datetime.now().date()
    
    return last_modified_date < today

def run_update():
    print(f"[{datetime.now()}] Starting NBA stats update...")
    try:
        # Change to the project directory to ensure git and relative paths work
        os.chdir(current_dir)
        
        # 1. Fetch newest logs
        fetch_logs()
        
        # 2. Fetch today's schedule
        update_todays_games_local()
        
        # 3. Load positions
        if not os.path.exists(POSITIONS_FILE):
            print(f"Error: {POSITIONS_FILE} not found.")
            return
            
        pos_df = pd.read_csv(POSITIONS_FILE)
        
        # 4. Generate data
        make_data(pos_df, MINUTES_CUTOFF)
        
        print("Data update complete.")
        
        if AUTO_PUSH:
            push_to_github()
            
    except Exception as e:
        print(f"Error during update: {e}")

def push_to_github():
    print("Pushing updates to GitHub...")
    # Using 'git push' assuming it's already configured with credentials
    os.system('git add .')
    commit_msg = f"Auto-update NBA stats: {datetime.now().strftime('%Y-%m-%d %I:%M %p EDT')}"
    os.system(f'git commit -m "{commit_msg}"')
    os.system('git push')
    print("GitHub push complete.")

if __name__ == "__main__":
    if should_update():
        run_update()
    else:
        print(f"[{datetime.now()}] Data is already up to date for today. Exiting.")
