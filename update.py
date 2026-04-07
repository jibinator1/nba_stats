import os
import pandas as pd
import requests
from datetime import datetime
from nba_api.stats.endpoints import playergamelogs
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Configuration & Headers (Shared with daily_update.py) ---
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
    """Manual fallback to fetch logs locally."""
    print("Fetching newest game logs from NBA API...")
    logs = playergamelogs.PlayerGameLogs(season_nullable='2025-26', headers=headers).get_data_frames()[0]
    logs.to_csv(os.path.join(BASE_DIR, "logs.csv"), index=False)
    print("Successfully updated logs.csv!")

def update_todays_games_local():
    """Manual fallback to fetch today's schedule locally."""
    print("Fetching today's schedule from NBA API...")
    url = "https://stats.nba.com/stats/scoreboardv2"
    params = {'DayOffset': '0', 'LeagueID': '00', 'gameDate': datetime.now().strftime('%m/%d/%Y')}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            result_sets = data.get('resultSets', [])
            line_score = next((rs for rs in result_sets if rs['name'] == 'LineScore'), None)
            if line_score:
                df = pd.DataFrame(line_score['rowSet'], columns=line_score['headers'])
                df[['GAME_ID', 'TEAM_ABBREVIATION']].to_csv(os.path.join(BASE_DIR, 'todays_games.csv'), index=False)
                return True
    except Exception as e:
        print(f"Error updating today's games: {e}")
    return False

def get_todays_games():
    """Used by app.py to read the local schedule."""
    path = os.path.join(BASE_DIR, 'todays_games.csv')
    if not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        games_list = df.groupby('GAME_ID')['TEAM_ABBREVIATION'].apply(list).tolist()
        return [g for g in games_list if len(g) == 2]
    except Exception:
        return []

def make_data(pos_df, minutes, last_n_games=20, logs_df=None, return_df=False):
    """Core data processing for the web dashboard."""
    #get positions for each players
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

    if logs_df is not None:
        logs_raw = logs_df.copy()
    else:
        path = os.path.join(BASE_DIR, "logs.csv")
        if not os.path.exists(path):
            return None if return_df else None
        logs_raw = pd.read_csv(path)
    team_id_map = logs_raw[['TEAM_ABBREVIATION', 'TEAM_ID']].drop_duplicates()
    team_id_lookup = dict(zip(team_id_map['TEAM_ABBREVIATION'], team_id_map['TEAM_ID']))

    logs = logs_raw[logs_raw['MIN'] >= minutes] 
    merged = logs.merge(pos_map, on='PLAYER_NAME')
    merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1]
    merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup)
    merged['GAME_DATE'] = pd.to_datetime(merged['GAME_DATE'])
    
    # ... Rest of the logic (Keeping it for the web app UI) ...
    # (Abbreviated to keep the file cleaner, assuming core math logic remains same)
    # Note: Users app.py relies on this for the table view.
    
    # I will keep the full make_data logic here to ensure the website doesn't break.
    # [Full implementation follows...]
    
    # Step 1: Opponent game schedule
    logs_with_opp = logs_raw.copy()
    logs_with_opp['OPPONENT_ABV'] = logs_with_opp['MATCHUP'].str.split(' ').str[-1]
    logs_with_opp['OPPONENT_ID'] = logs_with_opp['OPPONENT_ABV'].map(team_id_lookup)
    logs_with_opp['GAME_DATE'] = pd.to_datetime(logs_with_opp['GAME_DATE'])

    all_opp_games = logs_with_opp[['OPPONENT_ID', 'GAME_ID', 'GAME_DATE']].drop_duplicates()
    all_opp_games = all_opp_games.sort_values(by=['OPPONENT_ID', 'GAME_DATE'], ascending=[True, False])
    ln_dates = all_opp_games.groupby('OPPONENT_ID').head(last_n_games)
    ln_keys = ln_dates[['OPPONENT_ID', 'GAME_DATE']].drop_duplicates()

    ln_merged = pd.merge(merged, ln_keys, on=['OPPONENT_ID', 'GAME_DATE'], how='inner')

    if not ln_merged.empty:
        ln_per_game = ln_merged.groupby(['OPPONENT_ID', 'POSITION', 'GAME_DATE'])[['PTS', 'REB', 'AST']].mean().reset_index()
        ln_opp_stats = ln_per_game.groupby(['OPPONENT_ID', 'POSITION'])[['PTS', 'REB', 'AST']].mean().reset_index()
    else:
        ln_opp_stats = pd.DataFrame(columns=['OPPONENT_ID', 'POSITION', 'PTS', 'REB', 'AST'])
    ln_opp_stats.rename(columns={'PTS': 'L5_PTS', 'REB': 'L5_REB', 'AST': 'L5_AST'}, inplace=True)

    opp_stats = merged.groupby(['OPPONENT_ID', 'POSITION'])[
        ['PTS', 'REB', 'AST', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTA', 'FTM', 'TOV', 'STL', 'BLK', 'MIN']
    ].mean().reset_index()

    opp_stats['eFG_PCT'] = ((opp_stats['FGM'] + 0.5 * opp_stats['FG3M']) / opp_stats['FGA']) * 100
    opp_stats['TS_PCT'] = (opp_stats['PTS'] / (2 * (opp_stats['FGA'] + 0.44 * opp_stats['FTA']))) * 100
    opp_stats['FTr'] = opp_stats['FTA'] / opp_stats['FGA']

    opp_stats = pd.merge(opp_stats, ln_opp_stats, on=['OPPONENT_ID', 'POSITION'], how='left')
    team_stats = merged.groupby(['TEAM_ID', 'POSITION'])[['PTS', 'REB', 'AST']].mean().reset_index()
    team_stats.rename(columns={'PTS': 'TEAM_PTS','REB': 'TEAM_REB','AST': 'TEAM_AST'}, inplace=True)

    game_team = logs_raw.groupby(['GAME_ID', 'TEAM_ID'], as_index=False).agg({'PTS': 'sum','FGA': 'sum','FTA': 'sum','OREB': 'sum','TOV': 'sum'})
    game_team['POSS'] = game_team['FGA'] + 0.44 * game_team['FTA'] - game_team['OREB'] + game_team['TOV']
    game_team = game_team.rename(columns={'PTS': 'OPP_PTS', 'POSS': 'OPP_POSS'})

    defense_games = game_team.merge(game_team, on='GAME_ID', suffixes=('_ALLOWED', '_DEF'))
    defense_games = defense_games[defense_games['TEAM_ID_ALLOWED'] != defense_games['TEAM_ID_DEF']]

    defense_context = defense_games.groupby('TEAM_ID_DEF', as_index=False).agg({'OPP_PTS_ALLOWED': 'sum','OPP_POSS_ALLOWED': 'sum'}).rename(columns={'TEAM_ID_DEF': 'TEAM_ID', 'OPP_PTS_ALLOWED': 'OPP_PTS', 'OPP_POSS_ALLOWED': 'OPP_POSS'})
    defense_context['PACE'] = defense_games.groupby('TEAM_ID_DEF')['OPP_POSS_ALLOWED'].mean().values
    defense_context['DEF_RTG'] = (defense_context['OPP_PTS'] / defense_context['OPP_POSS']) * 100

    final_result = pd.merge(opp_stats, team_stats, left_on=['OPPONENT_ID', 'POSITION'], right_on=['TEAM_ID', 'POSITION'], how='left')
    if 'TEAM_ID' in final_result.columns: final_result.drop(columns=['TEAM_ID'], inplace=True)
    final_result = pd.merge(final_result, defense_context[['TEAM_ID', 'PACE', 'DEF_RTG']], left_on='OPPONENT_ID', right_on='TEAM_ID', how='left')
    final_result['TEAM'] = final_result['OPPONENT_ID'].map({v: k for k, v in team_id_lookup.items()})
    final_result = final_result.round(2)
    final_result[['TEAM_PTS', 'TEAM_REB', 'TEAM_AST']] = final_result[['TEAM_PTS', 'TEAM_REB', 'TEAM_AST']].fillna(0)
    final_result = final_result[~final_result['TEAM'].isin(['SEM', 'MEL', 'GUA', 'HAP'])]

    for s in ['PTS', 'REB', 'AST', 'FG3M', 'FG3A', 'STL', 'BLK', 'eFG_PCT', 'TS_PCT', 'FTr', 'PACE', 'DEF_RTG']:
        col_name = s.replace('_PCT', '')
        final_result[f'{col_name}_RANK'] = final_result.groupby('POSITION')[s].rank(ascending=False, method='min')
    
    final_result['TOV_RANK'] = final_result.groupby('POSITION')['TOV'].rank(ascending=True, method='min')

    final_result = final_result.round(2)
    export_cols = ['TEAM', 'POSITION', 'MIN', 'PTS', 'PTS_RANK', 'L5_PTS','REB', 'REB_RANK', 'L5_REB','AST', 'AST_RANK', 'L5_AST','PACE', 'PACE_RANK', 'DEF_RTG', 'DEF_RTG_RANK','eFG_PCT', 'eFG_RANK', 'TS_PCT', 'TS_RANK','FGA', 'FG3M', 'FG3M_RANK', 'FG3A', 'FG3A_RANK','FTA', 'FTr', 'FTr_RANK','STL', 'STL_RANK', 'BLK', 'BLK_RANK','TOV', 'TOV_RANK','TEAM_PTS', 'TEAM_REB', 'TEAM_AST']
    final_result.rename(columns={'MIN': 'OPP_MIN'}, inplace=True)
    if return_df:
        return final_result
    else:
        final_result.to_csv(os.path.join(BASE_DIR, 'vs_Position_withavg.csv'), index=False)

def find_streaks(pos_df, minutes, streak_len=5, pts_thresh=3.0, reb_thresh=1.5, ast_thresh=1.5, logs_df=None):
    """Finds players on a 5-game streak where they exceed their season averages."""
    if logs_df is not None:
        logs_raw = logs_df.copy()
    else:
        path = os.path.join(BASE_DIR, "logs.csv")
        if not os.path.exists(path):
            return pd.DataFrame()
        logs_raw = pd.read_csv(path)
    
    # Calculate season averages using ALL games (not just the ones that meet the min threshold)
    season_avgs = logs_raw.groupby('PLAYER_NAME')[['PTS', 'REB', 'AST']].mean().reset_index()
    season_avgs.rename(columns={'PTS': 'AVG_PTS', 'REB': 'AVG_REB', 'AST': 'AVG_AST'}, inplace=True)
    
    # Filter for players who AVERAGE at least the 'minutes' threshold over the season
    avg_mins = logs_raw.groupby('PLAYER_NAME')['MIN'].mean().reset_index()
    valid_rotation_players = avg_mins[avg_mins['MIN'] >= minutes]['PLAYER_NAME'].tolist()
    
    logs_filtered = logs_raw[logs_raw['PLAYER_NAME'].isin(valid_rotation_players)].copy()
    if logs_filtered.empty:
        return pd.DataFrame()
    
    logs_filtered['GAME_DATE'] = pd.to_datetime(logs_filtered['GAME_DATE'])
    
    # Get last N games from the filtered logs
    logs_sorted = logs_filtered.sort_values(by=['PLAYER_NAME', 'GAME_DATE'], ascending=[True, False])
    recent_games = logs_sorted.groupby('PLAYER_NAME').head(streak_len)
    
    # Only keep players who have exactly 'streak_len' recent games
    game_counts = recent_games.groupby('PLAYER_NAME').size()
    valid_players = game_counts[game_counts == streak_len].index
    recent_games = recent_games[recent_games['PLAYER_NAME'].isin(valid_players)]
    
    if recent_games.empty:
        return pd.DataFrame()

    # Get the minimum stat values in the recent stretch
    recent_mins = recent_games.groupby('PLAYER_NAME')[['PTS', 'REB', 'AST']].min().reset_index()
    recent_mins.rename(columns={'PTS': 'MIN_STREAK_PTS', 'REB': 'MIN_STREAK_REB', 'AST': 'MIN_STREAK_AST'}, inplace=True)
    
    # Get the average of the streak
    recent_avgs = recent_games.groupby('PLAYER_NAME')[['PTS', 'REB', 'AST']].mean().reset_index()
    recent_avgs.rename(columns={'PTS': 'STREAK_AVG_PTS', 'REB': 'STREAK_AVG_REB', 'AST': 'STREAK_AVG_AST'}, inplace=True)

    # NEW: Get the individual game logs for display
    def get_logs_str(group, col):
        return ", ".join(group.sort_values('GAME_DATE', ascending=True)[col].astype(str).tolist())

    game_logs = recent_games.groupby('PLAYER_NAME').apply(lambda x: pd.Series({
        'LOGS_PTS': get_logs_str(x, 'PTS'),
        'LOGS_REB': get_logs_str(x, 'REB'),
        'LOGS_AST': get_logs_str(x, 'AST')
    })).reset_index()

    # Merge everything together
    player_data = pd.merge(season_avgs, recent_mins, on='PLAYER_NAME')
    player_data = pd.merge(player_data, recent_avgs, on='PLAYER_NAME')
    player_data = pd.merge(player_data, game_logs, on='PLAYER_NAME')
    
    # Merge with position and team info
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})
    team_map = recent_games.drop_duplicates('PLAYER_NAME', keep='first')[['PLAYER_NAME', 'TEAM_ABBREVIATION']]
    team_map.rename(columns={'TEAM_ABBREVIATION': 'TEAM'}, inplace=True)

    player_data = pd.merge(player_data, pos_map, on='PLAYER_NAME', how='left')
    player_data = pd.merge(player_data, team_map, on='PLAYER_NAME', how='left')

    streaks = []
    current_date = datetime.now().strftime("%Y-%m-%d")

    for _, row in player_data.iterrows():
        if row['MIN_STREAK_PTS'] >= row['AVG_PTS'] + pts_thresh:
            streaks.append({
                'Run_Date': current_date,
                'Player': row['PLAYER_NAME'],
                'Team': row['TEAM'],
                'Pos': row['POSITION'],
                'Stat': 'PTS',
                'Direction': 'OVER',
                'Season_Avg': round(row['AVG_PTS'], 1),
                'Streak_Avg': round(row['STREAK_AVG_PTS'], 1),
                'Min_In_Streak': row['MIN_STREAK_PTS'],
                'Threshold': f"+{pts_thresh}",
                'Game_Logs': row['LOGS_PTS']
            })
        
        if row['MIN_STREAK_REB'] >= row['AVG_REB'] + reb_thresh:
            streaks.append({
                'Run_Date': current_date,
                'Player': row['PLAYER_NAME'],
                'Team': row['TEAM'],
                'Pos': row['POSITION'],
                'Stat': 'REB',
                'Direction': 'OVER',
                'Season_Avg': round(row['AVG_REB'], 1),
                'Streak_Avg': round(row['STREAK_AVG_REB'], 1),
                'Min_In_Streak': row['MIN_STREAK_REB'],
                'Threshold': f"+{reb_thresh}",
                'Game_Logs': row['LOGS_REB']
            })

        if row['MIN_STREAK_AST'] >= row['AVG_AST'] + ast_thresh:
            streaks.append({
                'Run_Date': current_date,
                'Player': row['PLAYER_NAME'],
                'Team': row['TEAM'],
                'Pos': row['POSITION'],
                'Stat': 'AST',
                'Direction': 'OVER',
                'Season_Avg': round(row['AVG_AST'], 1),
                'Streak_Avg': round(row['STREAK_AVG_AST'], 1),
                'Min_In_Streak': row['MIN_STREAK_AST'],
                'Threshold': f"+{ast_thresh}",
                'Game_Logs': row['LOGS_AST']
            })

    return pd.DataFrame(streaks).drop_duplicates() if streaks else pd.DataFrame()
