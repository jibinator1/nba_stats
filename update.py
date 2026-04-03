import os
import pandas as pd
import requests
from datetime import datetime
from nba_api.stats.endpoints import playergamelogs

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
    logs.to_csv("logs.csv", index=False)
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
                df[['GAME_ID', 'TEAM_ABBREVIATION']].to_csv('todays_games.csv', index=False)
                return True
    except Exception as e:
        print(f"Error updating today's games: {e}")
    return False

def get_todays_games():
    """Used by app.py to read the local schedule."""
    if not os.path.exists('todays_games.csv'):
        return []
    try:
        df = pd.read_csv('todays_games.csv')
        games_list = df.groupby('GAME_ID')['TEAM_ABBREVIATION'].apply(list).tolist()
        return [g for g in games_list if len(g) == 2]
    except Exception:
        return []

def make_data(pos_df, minutes, last_n_games=20):
    """Core data processing for the web dashboard."""
    #get positions for each players
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

    if not os.path.exists("logs.csv"):
        return
        
    logs_raw = pd.read_csv("logs.csv")
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
    final_result.to_csv('vs_Position_withavg.csv', index=False)

def create_matchups(pos_df, final_result, ALL_TEAMS, minutes):
    """Generates matchup picks for the web app UI."""
    if not ALL_TEAMS: return pd.DataFrame()
    
    positions = ['C', 'PF', 'PG', 'SF', 'SG']
    current_date = datetime.now().strftime("%Y-%m-%d")
    over_thresholds = {'PG': {'PTS': 22.5, 'REB': 5.2, 'AST': 8.5}, 'SG': {'PTS': 21.0, 'REB': 5.0, 'AST': 4.5}, 'SF': {'PTS': 20.5, 'REB': 6.8, 'AST': 5.0}, 'PF': {'PTS': 19.5, 'REB': 8.5, 'AST': 3.5}, 'C':  {'PTS': 18.5, 'REB': 12.0, 'AST': 3.5} }
    under_thresholds = {'PG': {'PTS': 18.7, 'REB': 4.2, 'AST': 5.75}, 'SG': {'PTS': 16.5, 'REB': 3.9, 'AST': 3.5}, 'SF': {'PTS': 17.1, 'REB': 5.6, 'AST': 3.5}, 'PF': {'PTS': 15.5, 'REB': 5.5, 'AST': 2.4}, 'C':  {'PTS': 15.0, 'REB': 9.0, 'AST': 2.5}}
    under_buffers = {'PTS': 8.0, 'REB': 4.0, 'AST': 4.0}

    good_matchups = []
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})
    logs = pd.read_csv("logs.csv")
    logs = logs[logs['MIN'] >= minutes]
    merged = logs.merge(pos_map, on='PLAYER_NAME')
    
    for T1, T2 in ALL_TEAMS:
        selected_teams = final_result[final_result['TEAM'].isin([T1, T2])]
        for pos in positions:
            t1_r = selected_teams[(selected_teams['TEAM'] == T1) & (selected_teams['POSITION'] == pos)]
            t2_r = selected_teams[(selected_teams['TEAM'] == T2) & (selected_teams['POSITION'] == pos)]
            if not t1_r.empty and not t2_r.empty:
                for stat in ['PTS', 'REB', 'AST']:
                    if t1_r[f'TEAM_{stat}'].values[0] >= over_thresholds[pos][stat] and t2_r[stat].values[0] >= over_thresholds[pos][stat]:
                        ps = merged[(merged['TEAM_ABBREVIATION'] == T1) & (merged['POSITION'] == pos)]['PLAYER_NAME'].unique()
                        for p in ps: good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T1} vs {T2}", 'Player': p, 'Pos': pos, 'Stat': stat, 'Direction': 'OVER'})
                    if t1_r[f'TEAM_{stat}'].values[0] >= (under_thresholds[pos][stat] + under_buffers[stat]) and t2_r[stat].values[0] <= under_thresholds[pos][stat]:
                        ps = merged[(merged['TEAM_ABBREVIATION'] == T1) & (merged['POSITION'] == pos)]['PLAYER_NAME'].unique()
                        for p in ps: good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T1} vs {T2}", 'Player': p, 'Pos': pos, 'Stat': stat, 'Direction': 'UNDER'})

    return pd.DataFrame(good_matchups).drop_duplicates() if good_matchups else pd.DataFrame()
