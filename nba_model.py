from nba_api.stats.static import teams
import pandas as pd
import numpy as np
import warnings
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
import requests
import csv
import io
import os
import sys
import threading
import time

# Suppress warnings
warnings.filterwarnings(action='ignore', category=UserWarning)
warnings.filterwarnings(action='ignore', category=FutureWarning)

# --- 30-MINUTE GLOBAL TIMEOUT ---
TIMEOUT_MINUTES = 30

def _timeout_handler():
    print(f"\n[TIMEOUT] Script exceeded {TIMEOUT_MINUTES} minutes. Forcing exit.")
    sys.stdout.flush()
    os._exit(1)  # Hard kill — works even if threads are stuck

_timer = threading.Timer(TIMEOUT_MINUTES * 60, _timeout_handler)
_timer.daemon = True
_timer.start()

# --- PROGRESS TRACKER ---
_start_time = time.time()
_total_steps = 10

def progress(step, msg):
    elapsed = time.time() - _start_time
    mins, secs = divmod(int(elapsed), 60)
    print(f"[STEP {step}/{_total_steps}] [{mins:02d}:{secs:02d}] {msg}", flush=True)

progress(0, f"Script started. {TIMEOUT_MINUTES}-min timeout armed.")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()

MIN_MINUTES = 17      
ROLLING_WINDOW = 20 #defense stats for last 20
PRED_WINDOW = 15 # offense stats for last 15
SEASON = '2025-26'    
RECENT_GAMES = 4 #number of games for recent column

# Optimized Parameters for VM to finish under 30 minutes
PARAM_DIST = {
    'n_estimators': [200, 300, 400],
    'max_depth': [10, 15, None],
    'min_samples_leaf': [2, 4],
    'max_features': ['sqrt', 'log2']
}
RF_RANDOM_STATE = 42  

# --- HELPER: INJURY SCRAPER ---
def get_injury_blacklist():
    print("Fetching current injury report...")
    url = "https://www.cbssports.com/nba/injuries/"
    header = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=header)
        # Wrap r.text in StringIO to avoid the FutureWarning
        dfs = pd.read_html(io.StringIO(r.text))
        injured_players = []
        for df in dfs:
            if 'Player' in df.columns:
                injured_players.extend(df['Player'].astype(str).tolist())
        return {name.strip() for name in injured_players}
    except Exception as e:
        print(f"Injury scraper failed: {e}. Falling back to local injuries.csv...")
        injuries_path = os.path.join(SCRIPT_DIR, 'injuries.csv')
        if os.path.exists(injuries_path):
            try:
                df_inj = pd.read_csv(injuries_path)
                return {normalize_name(name) for name in df_inj.iloc[:, 0].astype(str).tolist()}
            except: pass
        return set()

# ===== STEP 1: LOAD DATA =====
progress(1, "Loading player logs and positions...")
logs_path = os.path.join(SCRIPT_DIR, 'logs.csv')
pos_path = os.path.join(SCRIPT_DIR, 'positions.csv')

logs = pd.read_csv(logs_path)
pos_df = pd.read_csv(pos_path)

pos_map = pos_df[['Player', 'Pos']].drop_duplicates(subset=['Player']).rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

logs['MIN'] = pd.to_numeric(logs['MIN'], errors='coerce') #make it numeric so i can filter for certain minutes
logs = logs[logs['MIN'] >= MIN_MINUTES] 
merged = logs.merge(pos_map, on='PLAYER_NAME') #merge oin player name so I can get their positions as well (pg, sg)
progress(1, f"Loaded {len(merged)} rows after filtering MIN >= {MIN_MINUTES}.")

# ===== STEP 2: FEATURE ENGINEERING =====
progress(2, "Computing rest days, home/away, opponent mapping...")
merged['GAME_DATE'] = pd.to_datetime(merged['GAME_DATE']) #get the game date

merged = merged.sort_values(['PLAYER_NAME', 'GAME_DATE'])
merged['LAST_GAME_DATE'] = merged.groupby('PLAYER_NAME')['GAME_DATE'].shift(1)
merged['DAYS_REST'] = (merged['GAME_DATE'] - merged['LAST_GAME_DATE']).dt.days.fillna(3) #rest days as players should do better after having more rest days
merged['DAYS_REST'] = merged['DAYS_REST'].clip(upper=7) 
merged['IS_B2B'] = (merged['DAYS_REST'] <= 1).astype(int)

merged = merged.sort_values('GAME_DATE')

merged['IS_HOME'] = merged['MATCHUP'].apply(lambda x: 1 if 'vs.' in x else 0) #whether team is playing at home or not
team_id_lookup = dict(zip(merged['TEAM_ABBREVIATION'], merged['TEAM_ID'])) #team abbreviation name like Detroit Pistons is DET to id
merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1] #formatted as "team vs opp" so get the end of a list split by a space
merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup) #get the id for each opp abv

# ===== STEP 3: DEFENSIVE STATS =====
progress(3, "Computing opponent defensive stats (rolling window)...")
pts_by_pos = merged.groupby(['GAME_ID', 'TEAM_ID', 'POSITION'])['PTS'].sum().reset_index() #get the sum of the points for each position
matchup_map = merged[['GAME_ID', 'TEAM_ID', 'OPPONENT_ID']].drop_duplicates() #get the matchup 

# Calculate Team Possessions for PACE
game_poss = logs.groupby(['GAME_ID', 'TEAM_ID'])[['FGA', 'FTA', 'TOV']].sum().reset_index()
game_poss['POSS'] = game_poss['FGA'] + 0.44 * game_poss['FTA'] + game_poss['TOV']
game_poss = game_poss[['GAME_ID', 'TEAM_ID', 'POSS']].rename(columns={'TEAM_ID': 'OPPONENT_ID', 'POSS': 'OPP_PACE'})

team_def = pd.merge(pts_by_pos, matchup_map, on=['GAME_ID', 'TEAM_ID']) #merge the player stats and the opponent defense stats
team_def = pd.merge(team_def, game_poss, on=['GAME_ID', 'OPPONENT_ID'], how='left').fillna(100)
team_def = team_def.rename(columns={'PTS': 'PTS_ALLOWED', 'OPPONENT_ID': 'DEFENDING_TEAM_ID'})

# Pandas 3.0 FIX: Do not use groupby.apply with include_groups since it is deprecated and drops columns
team_def = team_def.sort_values(['DEFENDING_TEAM_ID', 'GAME_ID'])
team_def['OPP_PTS_ALLOWED'] = (
    team_def.groupby(['DEFENDING_TEAM_ID', 'POSITION'])['PTS_ALLOWED']
    .transform(lambda x: x.rolling(window=ROLLING_WINDOW, min_periods=3).mean().shift(1))
)
team_def['OPP_PACE_AVG'] = (
    team_def.groupby('DEFENDING_TEAM_ID')['OPP_PACE']
    .transform(lambda x: x.rolling(window=ROLLING_WINDOW, min_periods=3).mean().shift(1))
)
progress(3, "Defensive stats done.")

# ===== STEP 4: ROLLING AVERAGES =====
progress(4, "Computing player rolling averages (7 stats x 2 windows)...")
merged = merged.sort_values(['PLAYER_NAME', 'GAME_DATE'])

merged['TS_PCT'] = np.where(
    (merged['FGA'] + 0.44 * merged['FTA']) > 0,
    merged['PTS'] / (2 * (merged['FGA'] + 0.44 * merged['FTA'])),
    0
)
merged['USG_RATE'] = np.where(
    merged['MIN'] > 0,
    (merged['FGA'] + 0.44 * merged['FTA'] + merged['TOV']) / merged['MIN'],
    0
)

for col in ['MIN', 'FGA', 'FG3A', 'FTA', 'AST', 'TOV', 'PTS']:
    merged[f'PREV_AVG_{col}'] = merged.groupby('PLAYER_NAME')[col].transform(
        lambda x: x.rolling(window=PRED_WINDOW, min_periods=3).mean().shift(1)
    )
    merged[f'RECENT_{col}'] = merged.groupby('PLAYER_NAME')[col].transform(
        lambda x: x.rolling(window=RECENT_GAMES, min_periods=1).mean().shift(1)
    )

for col in ['PLUS_MINUS', 'FG_PCT', 'TS_PCT', 'USG_RATE']:
    merged[f'PREV_AVG_{col}'] = merged.groupby('PLAYER_NAME')[col].transform(
        lambda x: x.rolling(window=PRED_WINDOW, min_periods=3).mean().shift(1)
    )

merged['PREV_STD_PTS'] = merged.groupby('PLAYER_NAME')['PTS'].transform(
    lambda x: x.rolling(window=PRED_WINDOW, min_periods=3).std().shift(1)
).fillna(0)

progress(4, "Rolling averages done.")

# ===== STEP 5: PREPARE TRAINING DATA =====
progress(5, "Merging features and preparing training data...")
training_data = pd.merge(
    merged, team_def[['GAME_ID', 'DEFENDING_TEAM_ID', 'POSITION', 'OPP_PTS_ALLOWED', 'OPP_PACE_AVG']],
    left_on=['GAME_ID', 'OPPONENT_ID', 'POSITION'],right_on=['GAME_ID', 'DEFENDING_TEAM_ID', 'POSITION']
    ,how='left').dropna(subset=['OPP_PTS_ALLOWED', 'PREV_AVG_FGA', 'RECENT_FGA'])

features = [
    'PREV_AVG_MIN', 'PREV_AVG_FGA', 'PREV_AVG_FG3A', 'PREV_AVG_FTA', 'PREV_AVG_AST',
    'RECENT_MIN', 'RECENT_FGA', 'RECENT_FG3A', 'RECENT_FTA', 'RECENT_AST', 'RECENT_TOV', 'RECENT_PTS',
    'OPP_PTS_ALLOWED', 'IS_HOME', 'DAYS_REST', 'IS_B2B',
    'PREV_STD_PTS', 'PREV_AVG_FG_PCT', 'OPP_PACE_AVG', 'PREV_AVG_PLUS_MINUS', 'PREV_AVG_USG_RATE', 'PREV_AVG_TS_PCT'
]
progress(5, f"Training data ready: {len(training_data)} rows, {len(features)} features.")

# ===== STEP 6: MODEL TRAINING (SLOWEST STEP) =====
progress(6, f"Starting RandomForest training: n_iter=40, cv=5, total=200 fits...")
model_search = RandomizedSearchCV(
    # CPU INTENSIVE: n_jobs=-1 uses all CPU cores to build trees concurrently
    estimator=RandomForestRegressor(random_state=RF_RANDOM_STATE, n_jobs=1),
    param_distributions=PARAM_DIST,
    n_iter=40, 
    cv=TimeSeriesSplit(n_splits=5), 
    verbose=1,
    # MEMORY SAFE: n_jobs=1 runs one fit at a time so memory doesn't duplicate datasets
    n_jobs=1,
    random_state=RF_RANDOM_STATE
)

model_search.fit(training_data[features], training_data['PTS'])
model_pts = model_search.best_estimator_
progress(6, f"Training COMPLETE. Best: {model_search.best_params_}")

# ===== STEP 7: LOAD TODAY'S MATCHUPS =====
progress(7, "Loading today's matchups...")
games_path = os.path.join(SCRIPT_DIR, 'todays_games.csv')
TODAYS_GAMES = []
if os.path.exists(games_path):
    try:
        games_df = pd.read_csv(games_path)
        # Using the standard format for your todays_games.csv
        for gid, group in games_df.groupby('GAME_ID'):
            teams_list = group['TEAM_ABBREVIATION'].tolist()
            if len(teams_list) == 2:
                TODAYS_GAMES.append([teams_list[1], teams_list[0]]) # home, away
    except Exception as e:
        print(f"Failed to parse todays_games.csv: {e}")
else:
    print(f"Warning: {games_path} not found. No predictions will be made.")
progress(7, f"Found {len(TODAYS_GAMES)} games today.")

current_def = team_def.groupby(['DEFENDING_TEAM_ID', 'POSITION']).tail(1).copy()
current_def['Def_Rank'] = current_def.groupby('POSITION')['OPP_PTS_ALLOWED'].rank(ascending=True)

active_in_last_2weeks = merged[merged['GAME_DATE'] > pd.Timestamp.now() - pd.Timedelta(days=14)]['PLAYER_NAME'].unique()
injured_blacklist = get_injury_blacklist()

# Ensure players are only predicted for their current team
player_current_team = merged.sort_values('GAME_DATE').groupby('PLAYER_NAME')['TEAM_ABBREVIATION'].last().to_dict()

# ===== STEP 8: GENERATE PREDICTIONS =====
progress(8, "Generating predictions for each player...")
predictions = []

for home, away in TODAYS_GAMES: #go through all the teams
    for team, opp in [(home, away), (away, home)]: #set each tream as hjome or away each
        is_home_tonight = 1 if team == home else 0
        o_id = team_id_lookup.get(opp)
        team_players = [p for p, t in player_current_team.items() if t == team]
        
        for p_name in team_players:
            # CHECK 1: Must have played recently
            if p_name not in active_in_last_2weeks:
                continue
            
            # CHECK 2: Must NOT be on the injury report
            if any(p_name in inj for inj in injured_blacklist):
                continue
            
            p_history = merged[merged['PLAYER_NAME'] == p_name].tail(PRED_WINDOW) #skip players with less than 5 games
            if len(p_history) < 5:
                continue 
            
            p_pos = p_history['POSITION'].iloc[-1]
            d_data = current_def[(current_def['DEFENDING_TEAM_ID'] == o_id) & (current_def['POSITION'] == p_pos)]
            
            if not d_data.empty:
                last_game_date = p_history['GAME_DATE'].iloc[-1]
                days_rest = min((pd.Timestamp.now() - last_game_date).days, 7)

                #last 15 games points
                last_15_avg = p_history['PTS'].mean()
                
                avg_min = p_history['MIN'].mean()
                avg_fga = p_history['FGA'].mean()
                avg_fg3a = p_history['FG3A'].mean()
                avg_fta = p_history['FTA'].mean()
                avg_ast = p_history['AST'].mean()
                
                std_pts = p_history['PTS'].std()
                if pd.isna(std_pts): std_pts = 0
                avg_fg_pct = p_history['FG_PCT'].mean()
                avg_pm = p_history['PLUS_MINUS'].mean()
                avg_ts = p_history['TS_PCT'].mean()
                avg_usg = p_history['USG_RATE'].mean()
                
                rec_min = p_history['MIN'].tail(RECENT_GAMES).mean()
                rec_fga = p_history['FGA'].tail(RECENT_GAMES).mean()
                rec_fg3a = p_history['FG3A'].tail(RECENT_GAMES).mean()
                rec_fta = p_history['FTA'].tail(RECENT_GAMES).mean()
                rec_ast = p_history['AST'].tail(RECENT_GAMES).mean()
                rec_tov = p_history['TOV'].tail(RECENT_GAMES).mean()
                rec_pts = p_history['PTS'].tail(RECENT_GAMES).mean()
                is_b2b = 1 if days_rest <= 1 else 0
                
                raw_input = [
                    avg_min, avg_fga, avg_fg3a, avg_fta, avg_ast,
                    rec_min, rec_fga, rec_fg3a, rec_fta, rec_ast, rec_tov, rec_pts,
                    d_data['OPP_PTS_ALLOWED'].iloc[-1], 
                    is_home_tonight,
                    days_rest,
                    is_b2b,
                    std_pts, avg_fg_pct, d_data['OPP_PACE_AVG'].iloc[-1], avg_pm, avg_usg, avg_ts
                ]
                
                input_vec = np.nan_to_num(raw_input, nan=0.0)
                pred_pts = model_pts.predict([input_vec])[0]
                edge = abs(pred_pts - last_15_avg)
                
                ou_call = "OVER" if pred_pts > last_15_avg else "UNDER"
                
                if edge >= 2.5:
                    # Safeguard 1: Minute Trend Check
                    def get_min_trend(p_name):
                        p_data = merged[merged['PLAYER_NAME'] == p_name].tail(15)
                        if p_data.empty: return 0
                        avg_m = p_data['MIN'].mean()
                        if avg_m == 0: return 0
                        return (p_data['MIN'].tail(2).mean() - avg_m) / avg_m

                    min_trend = get_min_trend(p_name)
                    
                    if min_trend > -0.15: # Safeguard: Only pick if playing time isn't cratering
                        predictions.append({
                            'Date': datetime.today().date(),
                            'Player': p_name,
                            'Matchup': f"{team} vs {opp}" if is_home_tonight else f"{team} @ {opp}",
                            'Pos': p_pos,
                            'Def Rank': int(d_data['Def_Rank'].iloc[-1]),
                            'Avg': round(last_15_avg, 1),
                            'PRED': round(pred_pts, 1),
                            'Edge': round(edge, 2),
                            'O/U': ou_call,
                            'Min_Trend': round(min_trend, 2)
                        })

progress(8, f"Generated {len(predictions)} raw predictions.")

df_final = pd.DataFrame(predictions).drop_duplicates(subset=['Player', 'Matchup']).sort_values('Edge', ascending=False)
column_order = ['Date', 'Player', 'Matchup', 'Pos', 'Def Rank', 'Avg', 'PRED', 'Edge', 'O/U']
df_final = df_final[column_order]

# ===== STEP 9: ODDS API INTEGRATION =====
progress(9, "Fetching live lines from Odds API...")

API_KEY = '0c4ef5ea63d88b0eb418d5e0afaefdaa'
SPORT = 'basketball_nba' 
MARKET = 'player_points'
BOOKMAKER = 'draftkings'
TARGET_DATE = datetime.now().date().isoformat()

# **FIX:** Define players_to_track based on the model's output
players_to_track = df_final['Player'].tolist() if not df_final.empty else []

def normalize_name(name):
    return str(name).lower().replace('.', '').replace(' jr', '').replace("'", "").strip() if pd.notnull(name) else ""

def fetch_live_lines():
    """Hits the API and returns a dictionary of {normalized_name: line}"""
    # If no players to track, return empty immediately
    if not players_to_track:
        return {}
        
    start_dt = datetime.strptime(TARGET_DATE, '%Y-%m-%d')
    # Use timezone-aware comparison if needed, but for now simple 30hr window
    end_dt = start_dt + timedelta(hours=30)
    time_from = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    time_to = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    normalized_targets = {normalize_name(p): p for p in players_to_track}
    lines_found = {}

    # 1. Get Events
    events_url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/events'
    try:
        events_res = requests.get(events_url, params={'api_key': API_KEY, 'commenceTimeFrom': time_from, 'commenceTimeTo': time_to}, timeout=15)
        if events_res.status_code != 200:
            print(f"Error: {events_res.text}")
            return {}
        events = events_res.json()
    except Exception as e:
        print(f"Failed to fetch events: {e}")
        return {}
    
    print(f"  Found {len(events)} events, fetching odds...", flush=True)
    
    # 2. Get Odds for each event
    for i, event in enumerate(events):
        odds_url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event["id"]}/odds'
        try:
            odds_res = requests.get(odds_url, params={'api_key': API_KEY, 'regions': 'us', 'markets': MARKET, 'bookmakers': BOOKMAKER}, timeout=15)
            if odds_res.status_code == 200:
                data = odds_res.json()
                for bookmaker in data.get('bookmakers', []):
                    for market in bookmaker.get('markets', []):
                        for outcome in market.get('outcomes', []):
                            norm_name = normalize_name(outcome['description'])
                            if norm_name in normalized_targets:
                                lines_found[norm_name] = outcome.get('point')
        except Exception as e:
            print(f"Failed to fetch odds for event {event['id']}: {e}")
            continue
    
    return lines_found

# 1. Fetch the live data
live_lines_map = fetch_live_lines()
progress(9, f"Got lines for {len(live_lines_map)} players.")

# 2. Create the 'Line' column by mapping the player name to the API results
df_final['Line'] = df_final['Player'].apply(lambda x: live_lines_map.get(normalize_name(x)))

# 3. Calculate True Edge (PRED - Line) and apply Line Deviation Safeguard
df_final['True Edge'] = (df_final['PRED'] - df_final['Line']).round(2)

# Safeguard 2: Line Deviation Filter
# Filters out lines that are more than 35% away from the player's average (blowout/role risk)
df_final['Line_Dev'] = (abs(df_final['Line'] - df_final['Avg']) / df_final['Avg'])
df_final = df_final[df_final['Line_Dev'] < 0.35]

# Safeguard 3: True Edge Minimum Threshold
df_final = df_final[abs(df_final['True Edge']) >= 2.5]

# 4. Set the final column order
column_order = [
    'Date', 'Player', 'Matchup', 'Pos', 'Def Rank', 
    'Avg', 'PRED', 'Edge', 'O/U', 'Line', 'True Edge'
]

# Ensure all columns exist
for col in column_order:
    if col not in df_final.columns:
        df_final[col] = None

# ===== STEP 10: SAVE RESULTS =====
progress(10, "Saving results...")
df_final = df_final[column_order]
out_path = os.path.join(SCRIPT_DIR, "rf_predictions.csv")
df_final.to_csv(out_path, index=False)

# Cancel the timeout timer since we finished successfully
_timer.cancel()

elapsed_total = time.time() - _start_time
mins, secs = divmod(int(elapsed_total), 60)
print(f"\n{'='*50}", flush=True)
print(f"Process Complete in {mins}m {secs}s. Saved {len(df_final)} predictions to {out_path}", flush=True)
print(f"{'='*50}", flush=True)
print(df_final.head())