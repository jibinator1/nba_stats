import os
import pandas as pd
import numpy as np
import warnings
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit

# Suppress warnings
warnings.filterwarnings('ignore')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()

# --- CONFIGURATION ---
MIN_MINUTES = 20      
ROLLING_WINDOW = 20 # defense stats for last 20
PRED_WINDOW = 15 # offense stats for last 15
RECENT_GAMES = 4 # number of games for recent column
RF_RANDOM_STATE = 42

# High-Performance Parameters
PARAM_DIST = {
    'n_estimators': [1000, 1500, 2000],
    'max_depth': [15, 20, 25],
    'min_samples_leaf': [1, 2, 4],
    'max_features': ['sqrt', 'log2', 0.5] # 0.5 is mathematically better than None and avoids memory crash
}

def normalize_name(name):
    return str(name).lower().replace('.', '').replace(' jr', '').replace("'", "").strip() if pd.notnull(name) else ""

# --- STEP 1: LOAD LOCAL CSV DATA ---
print("Loading local CSV files...")

logs_path = os.path.join(SCRIPT_DIR, 'logs.csv')
pos_path = os.path.join(SCRIPT_DIR, 'positions.csv')
injuries_path = os.path.join(SCRIPT_DIR, 'injuries.csv')
games_path = os.path.join(SCRIPT_DIR, 'todays_games.csv')

if not os.path.exists(logs_path) or not os.path.exists(pos_path):
    raise FileNotFoundError("Missing logs.csv or positions.csv in the script directory!")

logs = pd.read_csv(logs_path)
pos_df = pd.read_csv(pos_path)

# Load Injuries
injured_players = set()
if os.path.exists(injuries_path):
    df_inj = pd.read_csv(injuries_path)
    # Assumes player names are in the first column
    injured_players = {normalize_name(name) for name in df_inj.iloc[:, 0].tolist()}
    print(f"Loaded {len(injured_players)} injured players from injuries.csv")
else:
    print("Warning: injuries.csv not found. Proceeding with empty blacklist.")

# Prepare Position Map
pos_map = pos_df[['Player', 'Pos']].drop_duplicates(subset=['Player']).rename(
    columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'}
)

# --- STEP 2: FEATURE ENGINEERING ---
print("Engineering features from logs...")

logs['MIN'] = pd.to_numeric(logs['MIN'], errors='coerce')
logs = logs[logs['MIN'] >= MIN_MINUTES]
merged = logs.merge(pos_map, on='PLAYER_NAME')
merged['GAME_DATE'] = pd.to_datetime(merged['GAME_DATE'])

merged = merged.sort_values(['PLAYER_NAME', 'GAME_DATE'])
merged['LAST_GAME_DATE'] = merged.groupby('PLAYER_NAME')['GAME_DATE'].shift(1)
merged['DAYS_REST'] = (merged['GAME_DATE'] - merged['LAST_GAME_DATE']).dt.days.fillna(3)
merged['DAYS_REST'] = merged['DAYS_REST'].clip(upper=7)
merged['IS_B2B'] = (merged['DAYS_REST'] <= 1).astype(int)

merged = merged.sort_values('GAME_DATE')

merged['IS_HOME'] = merged['MATCHUP'].apply(lambda x: 1 if 'vs.' in x else 0)
team_id_lookup = dict(zip(merged['TEAM_ABBREVIATION'], merged['TEAM_ID']))
merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1]
merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup)

pts_by_pos = merged.groupby(['GAME_ID', 'TEAM_ID', 'POSITION'])['PTS'].sum().reset_index()
matchup_map = merged[['GAME_ID', 'TEAM_ID', 'OPPONENT_ID']].drop_duplicates()
team_def = pd.merge(pts_by_pos, matchup_map, on=['GAME_ID', 'TEAM_ID'])
team_def = team_def.rename(columns={'PTS': 'PTS_ALLOWED', 'OPPONENT_ID': 'DEFENDING_TEAM_ID'})

team_def = team_def.sort_values(['DEFENDING_TEAM_ID', 'GAME_ID'])
team_def['OPP_PTS_ALLOWED'] = (
    team_def.groupby(['DEFENDING_TEAM_ID', 'POSITION'])['PTS_ALLOWED']
    .transform(lambda x: x.rolling(window=ROLLING_WINDOW, min_periods=3).mean().shift(1))
)

merged = merged.sort_values(['PLAYER_NAME', 'GAME_DATE'])

for col in ['MIN', 'FGA', 'FG3A', 'FTA', 'AST', 'TOV', 'PTS']:
    merged[f'PREV_AVG_{col}'] = merged.groupby('PLAYER_NAME')[col].transform(
        lambda x: x.rolling(window=PRED_WINDOW, min_periods=3).mean().shift(1)
    )
    merged[f'RECENT_{col}'] = merged.groupby('PLAYER_NAME')[col].transform(
        lambda x: x.rolling(window=RECENT_GAMES, min_periods=1).mean().shift(1)
    )

training_data = pd.merge(
    merged, team_def[['GAME_ID', 'DEFENDING_TEAM_ID', 'POSITION', 'OPP_PTS_ALLOWED']],
    left_on=['GAME_ID', 'OPPONENT_ID', 'POSITION'],right_on=['GAME_ID', 'DEFENDING_TEAM_ID', 'POSITION']
    ,how='left').dropna(subset=['OPP_PTS_ALLOWED', 'PREV_AVG_FGA', 'RECENT_FGA'])

features = [
    'PREV_AVG_MIN', 'PREV_AVG_FGA', 'PREV_AVG_FG3A', 'PREV_AVG_FTA', 'PREV_AVG_AST',
    'RECENT_MIN', 'RECENT_FGA', 'RECENT_FG3A', 'RECENT_FTA', 'RECENT_AST', 'RECENT_TOV', 'RECENT_PTS',
    'OPP_PTS_ALLOWED', 'IS_HOME', 'DAYS_REST', 'IS_B2B'
]

# --- STEP 3: MODEL TRAINING ---
print("Training RandomForest model using RandomizedSearchCV...")

model_search = RandomizedSearchCV(
    estimator=RandomForestRegressor(random_state=RF_RANDOM_STATE, n_jobs=-1),
    param_distributions=PARAM_DIST,
    n_iter=50,
    cv=TimeSeriesSplit(n_splits=5),
    verbose=3, # Shows progress per CV fold natively
    n_jobs=1,
    random_state=RF_RANDOM_STATE
)

model_search.fit(training_data[features], training_data['PTS'])

model_pts = model_search.best_estimator_

print("Model training complete.")

# --- STEP 4: OUTPUT PREDICTIONS FOR TODAY ---
# Load TODAY'S GAMES from CSV instead of API
TODAYS_GAMES = []
if os.path.exists(games_path):
    games_df = pd.read_csv(games_path)
    for gid, group in games_df.groupby('GAME_ID'):
        teams = group['TEAM_ABBREVIATION'].tolist()
        if len(teams) == 2:
            TODAYS_GAMES.append([teams[1], teams[0]]) # home, away assumed
else:
    print(f"Warning: {games_path} not found. No games to predict.")

current_def = team_def.groupby(['DEFENDING_TEAM_ID', 'POSITION']).tail(1).copy()
current_def['Def_Rank'] = current_def.groupby('POSITION')['OPP_PTS_ALLOWED'].rank(ascending=True)

active_in_last_2weeks = merged[merged['GAME_DATE'] > pd.Timestamp.now() - pd.Timedelta(days=14)]['PLAYER_NAME'].unique()

player_current_team = merged.sort_values('GAME_DATE').groupby('PLAYER_NAME')['TEAM_ABBREVIATION'].last().to_dict()

predictions = []

for home, away in TODAYS_GAMES:
    for team, opp in [(home, away), (away, home)]:
        is_home_tonight = 1 if team == home else 0
        o_id = team_id_lookup.get(opp)
        team_players = [p for p, t in player_current_team.items() if t == team]
        
        for p_name in team_players:
            if p_name not in active_in_last_2weeks:
                continue
            
            norm_name = normalize_name(p_name)
            if norm_name in injured_players:
                continue
            
            p_history = merged[merged['PLAYER_NAME'] == p_name].tail(PRED_WINDOW)
            if len(p_history) < 5:
                continue 
            
            p_pos = p_history['POSITION'].iloc[-1]
            d_data = current_def[(current_def['DEFENDING_TEAM_ID'] == o_id) & (current_def['POSITION'] == p_pos)]
            
            if not d_data.empty:
                last_game_date = p_history['GAME_DATE'].iloc[-1]
                days_rest = min((pd.Timestamp.now() - last_game_date).days, 7)

                last_15_avg = p_history['PTS'].mean()
                
                avg_min = p_history['MIN'].mean()
                avg_fga = p_history['FGA'].mean()
                avg_fg3a = p_history['FG3A'].mean()
                avg_fta = p_history['FTA'].mean()
                avg_ast = p_history['AST'].mean()
                
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
                    is_b2b
                ]
                
                input_vec = np.nan_to_num(raw_input, nan=0.0)
                pred_pts = model_pts.predict([input_vec])[0]
                edge = abs(pred_pts - last_15_avg)
                
                ou_call = "OVER" if pred_pts > last_15_avg else "UNDER"
                
                if edge >= 2.5:
                    predictions.append({
                        'Date': datetime.today().date(),
                        'Player': p_name,
                        'Matchup': f"{team} vs {opp}" if is_home_tonight else f"{team} @ {opp}",
                        'Pos': p_pos,
                        'Def Rank': int(d_data['Def_Rank'].iloc[-1]),
                        'Avg': round(last_15_avg, 1),
                        'PRED': round(pred_pts, 1),
                        'Edge': round(edge, 2),
                        'O/U': ou_call
                    })

df_final = pd.DataFrame(predictions)
if not df_final.empty:
    df_final = df_final.drop_duplicates(subset=['Player', 'Matchup']).sort_values('Edge', ascending=False)
    column_order = ['Date', 'Player', 'Matchup', 'Pos', 'Def Rank', 'Avg', 'PRED', 'Edge', 'O/U']
    df_final = df_final[column_order]

output_path = os.path.join(SCRIPT_DIR, "rf_predictions.csv")
df_final.to_csv(output_path, index=False)

print(f"Process Complete. Data saved to {output_path}")