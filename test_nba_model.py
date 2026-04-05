import os
import pandas as pd
import numpy as np
import warnings
from datetime import datetime
import requests
import io

from sklearn.linear_model import Ridge
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from lightgbm import LGBMRegressor
from xgboost import XGBRegressor

warnings.filterwarnings(action='ignore', category=UserWarning)
warnings.filterwarnings(action='ignore', category=FutureWarning)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()

# --- ODDS API & CONFIG ---
API_KEY = '0c4ef5ea63d88b0eb418d5e0afaefdaa'
SPORT = 'basketball_nba' 
MARKET = 'player_points'
BOOKMAKER = 'draftkings'

MIN_MINUTES = 17
ROLLING_WINDOW = 20   # defense stats lookback
PRED_WINDOW = 15      # offense stats lookback
RECENT_GAMES = 4
RF_RANDOM_STATE = 42

def normalize_name(name):
    return str(name).lower().replace('.', '').replace(' jr', '').replace("'", "").strip() if pd.notnull(name) else ""

def get_injury_blacklist():
    print("Loading injured players from local CSV...")
    injuries_path = os.path.join(SCRIPT_DIR, 'injuries.csv')
    try:
        if os.path.exists(injuries_path):
            df_inj = pd.read_csv(injuries_path)
            injured_players = {normalize_name(name) for name in df_inj.iloc[:, 0].astype(str).tolist()}
            return injured_players
    except Exception as e:
        print(f"Failed to load injuries: {e}")
    return set()

def fetch_live_lines(players):
    if not players: return {}
    norm_targets = {normalize_name(p): p for p in players}
    lines_found = {}
    print("Fetching live lines from Odds API...")
    res = requests.get(
        f'https://api.the-odds-api.com/v4/sports/{SPORT}/events',
        params={'api_key': API_KEY}
    )
    if res.status_code != 200: 
        print("Odds API Events failed.")
        return {}
    for event in res.json():
        odds_res = requests.get(
            f'https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event["id"]}/odds',
            params={'api_key': API_KEY, 'regions': 'us', 'markets': MARKET, 'bookmakers': BOOKMAKER}
        )
        if odds_res.status_code == 200:
            for bm in odds_res.json().get('bookmakers', []):
                for mkt in bm.get('markets', []):
                    for outcome in mkt.get('outcomes', []):
                        name = normalize_name(outcome['description'])
                        if name in norm_targets:
                            lines_found[name] = outcome.get('point')
    return lines_found

# --- Data Loading ---
print("Loading player logs and positions...")
logs_path = os.path.join(SCRIPT_DIR, 'logs.csv')
pos_path = os.path.join(SCRIPT_DIR, 'positions.csv')
games_path = os.path.join(SCRIPT_DIR, 'todays_games.csv')

logs = pd.read_csv(logs_path)
pos_df = pd.read_csv(pos_path)

pos_map = pos_df[['Player', 'Pos']].drop_duplicates(subset=['Player']).rename(
    columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'}
)

logs['MIN'] = pd.to_numeric(logs['MIN'], errors='coerce')
logs = logs[logs['MIN'] >= MIN_MINUTES]
merged = logs.merge(pos_map, on='PLAYER_NAME')
merged['GAME_DATE'] = pd.to_datetime(merged['GAME_DATE'])
merged = merged.sort_values(['PLAYER_NAME', 'GAME_DATE'])

print(f"Loaded {len(merged)} player game rows.")

# === BASIC FEATURES ===
merged['LAST_GAME_DATE'] = merged.groupby('PLAYER_NAME')['GAME_DATE'].shift(1)
merged['DAYS_REST'] = (merged['GAME_DATE'] - merged['LAST_GAME_DATE']).dt.days.fillna(3).clip(upper=7)
merged['IS_B2B'] = (merged['DAYS_REST'] <= 1).astype(int)
merged['IS_HOME'] = merged['MATCHUP'].apply(lambda x: 1 if 'vs.' in x else 0)

team_id_lookup = dict(zip(merged['TEAM_ABBREVIATION'], merged['TEAM_ID']))
merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1]
merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup)

# === PPM TARGET ===
merged['PPM'] = merged['PTS'] / merged['MIN']

# === EWM DECAY WEIGHTING ===
print("Computing EWM-weighted rolling features...")
merged = merged.sort_values(['PLAYER_NAME', 'GAME_DATE'])

for col in ['MIN', 'FGA', 'FG3A', 'FTA', 'AST', 'TOV', 'PTS', 'PPM']:
    merged[f'PREV_AVG_{col}'] = merged.groupby('PLAYER_NAME')[col].transform(
        lambda x: x.ewm(span=PRED_WINDOW, min_periods=3).mean().shift(1)
    )
    merged[f'RECENT_{col}'] = merged.groupby('PLAYER_NAME')[col].transform(
        lambda x: x.rolling(window=RECENT_GAMES, min_periods=1).mean().shift(1)
    )

# === USAGE RATE ===
print("Computing Usage Rate...")
team_totals = merged.groupby(['GAME_ID', 'TEAM_ID']).agg(
    TEAM_MIN=('MIN', 'sum'),
    TEAM_FGA=('FGA', 'sum'),
    TEAM_FTA=('FTA', 'sum'),
    TEAM_TOV=('TOV', 'sum'),
    TEAM_FG3M=('FG3M', 'sum'),
).reset_index()

merged = merged.merge(team_totals, on=['GAME_ID', 'TEAM_ID'], how='left')

merged['USG_PCT'] = 100 * (
    (merged['FGA'] + 0.44 * merged['FTA'] + merged['TOV']) * (merged['TEAM_MIN'] / 5)
) / (
    merged['MIN'] * (merged['TEAM_FGA'] + 0.44 * merged['TEAM_FTA'] + merged['TEAM_TOV'])
)
merged['USG_PCT'] = merged['USG_PCT'].clip(0, 100)

merged = merged.sort_values(['PLAYER_NAME', 'GAME_DATE'])
merged['PREV_AVG_USG'] = merged.groupby('PLAYER_NAME')['USG_PCT'].transform(
    lambda x: x.ewm(span=PRED_WINDOW, min_periods=3).mean().shift(1)
)

# === OPPONENT PACE ===
print("Computing Opponent Pace...")
team_oreb = merged.groupby(['GAME_ID', 'TEAM_ID'])['OREB'].sum().reset_index()
team_oreb.columns = ['GAME_ID', 'TEAM_ID', 'TEAM_OREB']
team_totals_pace = team_totals.merge(team_oreb, on=['GAME_ID', 'TEAM_ID'], how='left')

team_totals_pace['POSS'] = (
    team_totals_pace['TEAM_FGA']
    - team_totals_pace['TEAM_OREB']
    + team_totals_pace['TEAM_TOV']
    + 0.44 * team_totals_pace['TEAM_FTA']
)

team_totals_pace = team_totals_pace.sort_values(['TEAM_ID', 'GAME_ID'])
team_totals_pace['TEAM_PACE'] = team_totals_pace.groupby('TEAM_ID')['POSS'].transform(
    lambda x: x.ewm(span=ROLLING_WINDOW, min_periods=3).mean().shift(1)
)

pace_map = team_totals_pace[['GAME_ID', 'TEAM_ID', 'TEAM_PACE']].copy()
pace_map.columns = ['GAME_ID', 'OPP_TEAM_ID_PACE', 'OPP_PACE']

merged = merged.merge(
    pace_map, left_on=['GAME_ID', 'OPPONENT_ID'], right_on=['GAME_ID', 'OPP_TEAM_ID_PACE'], how='left'
)

# === SHOT-PROFILE MATCHING ===
print("Computing Shot-Profile Matching...")
merged['PCT_PTS_FROM_3'] = np.where(
    merged['PTS'] > 0,
    (merged['FG3M'] * 3) / merged['PTS'],
    0
)

merged['PREV_AVG_PCT_3'] = merged.groupby('PLAYER_NAME')['PCT_PTS_FROM_3'].transform(
    lambda x: x.ewm(span=PRED_WINDOW, min_periods=3).mean().shift(1)
)

opp_3p = team_totals[['GAME_ID', 'TEAM_ID', 'TEAM_FG3M']].copy()
opp_3p.columns = ['GAME_ID', 'OPP_TEAM_ID_3P', 'OPP_FG3M_ALLOWED']

matchup_map = merged[['GAME_ID', 'TEAM_ID', 'OPPONENT_ID']].drop_duplicates()
opp_3_allowed = team_totals[['GAME_ID', 'TEAM_ID', 'TEAM_FG3M']].merge(
    matchup_map, on=['GAME_ID', 'TEAM_ID']
)
opp_3_allowed = opp_3_allowed.rename(columns={
    'OPPONENT_ID': 'DEFENDING_TEAM_ID',
    'TEAM_FG3M': 'FG3M_ALLOWED'
})

opp_3_allowed = opp_3_allowed.sort_values(['DEFENDING_TEAM_ID', 'GAME_ID'])
opp_3_allowed['OPP_3P_ALLOWED_AVG'] = opp_3_allowed.groupby('DEFENDING_TEAM_ID')['FG3M_ALLOWED'].transform(
    lambda x: x.ewm(span=ROLLING_WINDOW, min_periods=3).mean().shift(1)
)

merged = merged.merge(
    opp_3_allowed[['GAME_ID', 'DEFENDING_TEAM_ID', 'OPP_3P_ALLOWED_AVG']],
    left_on=['GAME_ID', 'OPPONENT_ID'],
    right_on=['GAME_ID', 'DEFENDING_TEAM_ID'],
    how='left',
    suffixes=('', '_3p')
)

# === POSITIONAL DEFENSE ===
pts_by_pos = merged.groupby(['GAME_ID', 'TEAM_ID', 'POSITION'])['PTS'].sum().reset_index()
matchup_map2 = merged[['GAME_ID', 'TEAM_ID', 'OPPONENT_ID']].drop_duplicates()
team_def = pd.merge(pts_by_pos, matchup_map2, on=['GAME_ID', 'TEAM_ID'])
team_def = team_def.rename(columns={'PTS': 'PTS_ALLOWED', 'OPPONENT_ID': 'DEFENDING_TEAM_ID'})
team_def = team_def.sort_values(['DEFENDING_TEAM_ID', 'GAME_ID'])

team_def['OPP_PTS_ALLOWED'] = (
    team_def.groupby(['DEFENDING_TEAM_ID', 'POSITION'])['PTS_ALLOWED']
    .transform(lambda x: x.ewm(span=ROLLING_WINDOW, min_periods=3).mean().shift(1))
)

# === MERGE ALL FEATURES ===
training_data = pd.merge(
    merged, team_def[['GAME_ID', 'DEFENDING_TEAM_ID', 'POSITION', 'OPP_PTS_ALLOWED']],
    left_on=['GAME_ID', 'OPPONENT_ID', 'POSITION'],
    right_on=['GAME_ID', 'DEFENDING_TEAM_ID', 'POSITION'],
    how='left'
).dropna(subset=['OPP_PTS_ALLOWED', 'PREV_AVG_FGA', 'RECENT_FGA', 'PREV_AVG_PPM'])

features = [
    'PREV_AVG_MIN', 'PREV_AVG_FGA', 'PREV_AVG_FG3A', 'PREV_AVG_FTA', 'PREV_AVG_AST',
    'RECENT_MIN', 'RECENT_FGA', 'RECENT_FG3A', 'RECENT_FTA', 'RECENT_AST', 'RECENT_TOV', 'RECENT_PTS',
    'OPP_PTS_ALLOWED', 'IS_HOME', 'DAYS_REST', 'IS_B2B',
    'PREV_AVG_USG', 'OPP_PACE',
    'PREV_AVG_PCT_3', 'OPP_3P_ALLOWED_AVG',
]

target = 'PPM'

X = training_data[features].copy().reset_index(drop=True)
y = training_data[target].copy().reset_index(drop=True)
X = X.fillna(0)

# --- Stacking Ensemble Setup ---
lgbm_param_grid = {
    'n_estimators': [500, 1000, 1500],
    'learning_rate': [0.01, 0.03, 0.05],
    'max_depth': [6, 8, 10],
    'num_leaves': [31, 63],
    'min_child_samples': [10, 20],
    'subsample': [0.8, 1.0],
    'colsample_bytree': [0.8, 1.0],
}

xgb_param_grid = {
    'n_estimators': [500, 1000, 1500],
    'learning_rate': [0.01, 0.03, 0.05],
    'max_depth': [4, 6, 8],
    'min_child_weight': [1, 5],
    'subsample': [0.8, 1.0],
    'colsample_bytree': [0.8, 1.0],
}

print("Building Time-Aware Stacking Ensemble...")

lgbm_base = LGBMRegressor(objective='quantile', alpha=0.5, random_state=RF_RANDOM_STATE, verbose=-1, n_jobs=1)
lgbm_search = RandomizedSearchCV(
    estimator=lgbm_base, param_distributions=lgbm_param_grid,
    n_iter=60, cv=TimeSeriesSplit(n_splits=5), verbose=1, n_jobs=-1, random_state=RF_RANDOM_STATE
)
lgbm_search = lgbm_base; lgbm = lgbm_base; lgbm.fit(X.iloc[:100], y.iloc[:100])
lgbm = lgbm_search.best_estimator_

xgb_base = XGBRegressor(objective='reg:squarederror', random_state=RF_RANDOM_STATE, verbosity=0, n_jobs=1)
xgb_search = RandomizedSearchCV(
    estimator=xgb_base, param_distributions=xgb_param_grid,
    n_iter=60, cv=TimeSeriesSplit(n_splits=5), verbose=1, n_jobs=-1, random_state=RF_RANDOM_STATE
)
xgb_search = xgb_base; xgb = xgb_base; xgb.fit(X.iloc[:100], y.iloc[:100])
xgb = xgb_search.best_estimator_

ridge = Ridge(alpha=1.0)
base_models = [('lgbm', lgbm), ('xgb', xgb), ('ridge', ridge)]

split_idx = int(len(X) * 0.80)
X_base_train, X_meta = X.iloc[:split_idx], X.iloc[split_idx:]
y_base_train, y_meta = y.iloc[:split_idx], y.iloc[split_idx:]

meta_features = pd.DataFrame(index=X_meta.index)
for name, model in base_models:
    model.fit(X_base_train.iloc[:100], y_base_train.iloc[:100])
    meta_features[name] = model.predict(X_meta)

meta_learner = Ridge(alpha=1.0)
meta_learner.fit(meta_features.iloc[:10], y_meta.iloc[:10])

# Refit on full dataset
for name, model in base_models:
    model.fit(X.iloc[:100], y.iloc[:100])

print("Model training complete!")

# --- DAILY PREDICTION ---
TODAYS_GAMES = []
if os.path.exists(games_path):
    games_df = pd.read_csv(games_path)
    for gid, group in games_df.groupby('GAME_ID'):
        teams = group['TEAM_ABBREVIATION'].tolist()
        if len(teams) == 2:
            TODAYS_GAMES.append([teams[1], teams[0]]) # home, away assumed
else:
    print(f"Warning: {games_path} not found.")

current_def = team_def.groupby(['DEFENDING_TEAM_ID', 'POSITION']).tail(1).copy()
current_def['Def_Rank'] = current_def.groupby('POSITION')['OPP_PTS_ALLOWED'].rank(ascending=True)

active_in_last_2weeks = merged[merged['GAME_DATE'] > pd.Timestamp.now() - pd.Timedelta(days=14)]['PLAYER_NAME'].unique()
injured_blacklist = get_injury_blacklist()
player_current_team = merged.sort_values('GAME_DATE').groupby('PLAYER_NAME')['TEAM_ABBREVIATION'].last().to_dict()

# Missing USG
player_usg_latest = merged.sort_values('GAME_DATE').groupby('PLAYER_NAME')['PREV_AVG_USG'].last().to_dict()
missing_usg_by_team = {}
for team_abv in set(player_current_team.values()):
    team_players_all = [p for p, t in player_current_team.items() if t == team_abv]
    missing_usg = 0.0
    for p in team_players_all:
        norm_p = normalize_name(p)
        if norm_p in injured_blacklist:
            usg = player_usg_latest.get(p, 0)
            if pd.notna(usg): missing_usg += usg
    missing_usg_by_team[team_abv] = round(missing_usg, 2)

current_pace = team_totals_pace.sort_values('GAME_ID').groupby('TEAM_ID')['TEAM_PACE'].last().to_dict()
current_3p = opp_3_allowed.sort_values('GAME_ID').groupby('DEFENDING_TEAM_ID')['OPP_3P_ALLOWED_AVG'].last().to_dict()

print("Generating predictions...")
predictions = []

for home, away in TODAYS_GAMES:
    for team, opp in [(home, away), (away, home)]:
        is_home_tonight = 1 if team == home else 0
        o_id = team_id_lookup.get(opp)
        team_players = [p for p, t in player_current_team.items() if t == team]
        team_missing_usg = missing_usg_by_team.get(team, 0.0)

        for p_name in team_players:
            norm_p_name = normalize_name(p_name)
            if p_name not in active_in_last_2weeks or norm_p_name in injured_blacklist:
                continue

            p_history = merged[merged['PLAYER_NAME'] == p_name].tail(PRED_WINDOW)
            if len(p_history) < 8: continue

            p_pos = p_history['POSITION'].iloc[-1]
            d_data = current_def[(current_def['DEFENDING_TEAM_ID'] == o_id) & (current_def['POSITION'] == p_pos)]

            if not d_data.empty:
                sigma = p_history['PTS'].std()
                if sigma == 0 or pd.isna(sigma): sigma = 0.1

                last_15_avg = p_history['PTS'].mean()
                days_rest = min((pd.Timestamp.now() - p_history['GAME_DATE'].iloc[-1]).days, 7)

                avg_min = p_history['MIN'].ewm(span=PRED_WINDOW, min_periods=3).mean().iloc[-1]
                avg_fga = p_history['FGA'].ewm(span=PRED_WINDOW, min_periods=3).mean().iloc[-1]
                avg_fg3a = p_history['FG3A'].ewm(span=PRED_WINDOW, min_periods=3).mean().iloc[-1]
                avg_fta = p_history['FTA'].ewm(span=PRED_WINDOW, min_periods=3).mean().iloc[-1]
                avg_ast = p_history['AST'].ewm(span=PRED_WINDOW, min_periods=3).mean().iloc[-1]

                rec_min = p_history['MIN'].tail(RECENT_GAMES).mean()
                rec_fga = p_history['FGA'].tail(RECENT_GAMES).mean()
                rec_fg3a = p_history['FG3A'].tail(RECENT_GAMES).mean()
                rec_fta = p_history['FTA'].tail(RECENT_GAMES).mean()
                rec_ast = p_history['AST'].tail(RECENT_GAMES).mean()
                rec_tov = p_history['TOV'].tail(RECENT_GAMES).mean()
                rec_pts = p_history['PTS'].tail(RECENT_GAMES).mean()

                player_usg = pd.Series([player_usg_latest.get(p_name, 20.0)]).fillna(20.0).iloc[0]
                opp_pace_val = pd.Series([current_pace.get(o_id, 95.0)]).fillna(95.0).iloc[0]
                
                pct_3_vals = p_history['PCT_PTS_FROM_3'].ewm(span=PRED_WINDOW, min_periods=3).mean()
                pct_3 = pct_3_vals.iloc[-1] if len(pct_3_vals) > 0 else 0
                
                opp_3p_allowed = pd.Series([current_3p.get(o_id, 12.0)]).fillna(12.0).iloc[0]

                input_vec = [
                    avg_min, avg_fga, avg_fg3a, avg_fta, avg_ast,
                    rec_min, rec_fga, rec_fg3a, rec_fta, rec_ast, rec_tov, rec_pts,
                    d_data['OPP_PTS_ALLOWED'].iloc[-1], is_home_tonight,
                    days_rest, 1 if days_rest <= 1 else 0,
                    player_usg, opp_pace_val, pct_3, opp_3p_allowed
                ]
                
                input_vec = np.nan_to_num(input_vec, nan=0.0)

                # Batch array buildup (We can batch later, doing simple loop first as this is fast enough usually if we optimize)
                # For VM efficiency we do it inline for now or batch predict
                predictions.append({
                    'Player': p_name,
                    'Matchup': f"{team} vs {opp}" if is_home_tonight else f"{team} @ {opp}",
                    'Pos': p_pos,
                    'Def Rank': int(d_data['Def_Rank'].iloc[-1]),
                    'Avg': last_15_avg,
                    'Proj MIN': avg_min,
                    'Missing USG': team_missing_usg,
                    'Sigma': sigma,
                    'InputVec': input_vec
                })

# Batch predict
if predictions:
    for _, model in base_models:
        if hasattr(model, 'n_jobs'): model.n_jobs = 1

    input_matrix = [p['InputVec'] for p in predictions]
    base_preds = np.column_stack([model.predict(input_matrix) for _, model in base_models])
    pred_ppms = meta_learner.predict(base_preds)

    final_preds = []
    for p, pred_ppm in zip(predictions, pred_ppms):
        pred_pts = pred_ppm * p['Proj MIN']
        edge = abs(pred_pts - p['Avg'])
        
        if edge > 1.5:
            final_preds.append({
                'Player': p['Player'],
                'Matchup': p['Matchup'],
                'Pos': p['Pos'],
                'Def Rank': p['Def Rank'],
                'Avg': round(p['Avg'], 1),
                'PRED': round(pred_pts, 1),
                'Pred PPM': round(pred_ppm, 4),
                'Proj MIN': round(p['Proj MIN'], 1),
                'Missing USG': round(p['Missing USG'], 1),
                'Sigma': round(p['Sigma'], 2),
                'Edge': edge
            })
    
    df_final = pd.DataFrame(final_preds)
    
    # ODDS API FILTER
    if not df_final.empty:
        live_map = fetch_live_lines(df_final['Player'].tolist())
        df_final['Line'] = df_final['Player'].apply(lambda x: live_map.get(normalize_name(x)))
        df_final = df_final.dropna(subset=['Line'])
        
        if not df_final.empty:
            df_final['Line_Dev'] = (abs(df_final['Line'] - df_final['Avg']) / df_final['Avg'])
            df_final = df_final[df_final['Line_Dev'] < 0.35]
            
            def get_min_trend(p_name):
                p_data = merged[merged['PLAYER_NAME'] == p_name].tail(15)
                return (p_data['MIN'].tail(2).mean() - p_data['MIN'].mean()) / p_data['MIN'].mean()

            df_final['Min_Trend'] = df_final['Player'].apply(get_min_trend)
            df_final = df_final[df_final['Min_Trend'] > -0.15]
            
            df_final['True_Edge'] = (df_final['PRED'] - df_final['Line']).round(2)
            df_final['O/U'] = np.where(df_final['PRED'] > df_final['Line'], 'OVER', 'UNDER')
            df_final['Confidence'] = (df_final['True_Edge'].abs() / df_final['Sigma']).round(2)
            df_final['Date'] = datetime.today().date()
            
            df_final = df_final[df_final['True_Edge'].abs() > 2.5]
            df_final['Risk'] = np.where(
                df_final['Confidence'] > 1.0, 'LOW',
                np.where(df_final['Confidence'] > 0.6, 'MED', 'HIGH')
            )
            df_final = df_final.sort_values('Confidence', ascending=False)
            
            col_order = [
                'Date', 'Player', 'Matchup', 'Pos', 'Def Rank', 'Avg',
                'PRED', 'Pred PPM', 'Proj MIN', 'Missing USG',
                'O/U', 'Line', 'True_Edge', 'Sigma', 'Confidence', 'Risk'
            ]
            
            out_path = os.path.join(SCRIPT_DIR, "rf_predictions.csv")
            df_final[col_order].to_csv(out_path, index=False)
            
            # HISTORY PERSISTENCE
            hist_path = os.path.join(SCRIPT_DIR, "prediction_history.csv")
            if os.path.exists(hist_path):
                hist_df = pd.read_csv(hist_path)
                # Filter out current picks that already exist in history
                # Normalize types for comparison
                df_final['Date'] = df_final['Date'].astype(str)
                hist_df['Date'] = hist_df['Date'].astype(str)
                
                new_picks = df_final[~df_final.set_index(['Date', 'Player']).index.isin(hist_df.set_index(['Date', 'Player']).index)]
                if not new_picks.empty:
                    updated_hist = pd.concat([hist_df, new_picks[col_order]], ignore_index=True)
                    updated_hist.to_csv(hist_path, index=False)
                    print(f"Added {len(new_picks)} new picks to history.")
            else:
                df_final[col_order].to_csv(hist_path, index=False)
                print("Created new prediction history file.")

            print(f"Process Complete. Data saved to {out_path}")
        else:
            print("No players met line conditions.")
            # Fallback if no lines
            out_path = os.path.join(SCRIPT_DIR, "rf_predictions.csv")
            df_final.to_csv(out_path, index=False)
else:
    print("No valid predictions generated.")