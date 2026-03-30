from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster, playergamelogs
import pandas as pd
import time
import os
from datetime import datetime

# --- NBA API HEADERS (fixes timeout/connection errors) ---
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
    print("Fetching newest game logs from NBA API...")
    # Fetch latest 2025-26 game logs using headers to avert ReadTimeout
    logs = playergamelogs.PlayerGameLogs(season_nullable='2025-26', headers=headers).get_data_frames()[0]
    logs.to_csv("logs.csv", index=False)
    print("Successfully updated logs.csv!")

def make_data(pos_df, minutes):
    #get positions for each players
    
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

    # get the game logs
    logs_raw = pd.read_csv("logs.csv")
    logs = logs_raw.copy()

    #only select players who have more than 20 minutes play time
    logs = logs[logs['MIN'] >= minutes] 


    merged = logs.merge(pos_map, on='PLAYER_NAME')


    team_id_map = merged[['TEAM_ABBREVIATION', 'TEAM_ID']].drop_duplicates()
    team_id_lookup = dict(zip(team_id_map['TEAM_ABBREVIATION'], team_id_map['TEAM_ID']))

    merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1]
    merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup)

    merged['GAME_DATE'] = pd.to_datetime(merged['GAME_DATE'])
    
    # Calculate Last 5 games for each Opponent
    opp_game_dates = merged[['OPPONENT_ID', 'GAME_DATE']].drop_duplicates().sort_values(by=['OPPONENT_ID', 'GAME_DATE'], ascending=[True, False])
    l5_dates = opp_game_dates.groupby('OPPONENT_ID').head(5)
    
    l5_merged = pd.merge(merged, l5_dates, on=['OPPONENT_ID', 'GAME_DATE'], how='inner')
    l5_opp_stats = l5_merged.groupby(['OPPONENT_ID', 'POSITION'])[['PTS', 'REB', 'AST']].mean().reset_index()
    l5_opp_stats.rename(columns={'PTS': 'L5_PTS', 'REB': 'L5_REB', 'AST': 'L5_AST'}, inplace=True)

    opp_stats = merged.groupby(['OPPONENT_ID', 'POSITION'])[
        ['PTS', 'REB', 'AST', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTA', 'FTM', 'TOV', 'STL', 'BLK', 'MIN']
    ].mean().reset_index()

    opp_stats['eFG_PCT'] = ((opp_stats['FGM'] + 0.5 * opp_stats['FG3M']) / opp_stats['FGA']) * 100
    opp_stats['TS_PCT'] = (opp_stats['PTS'] / (2 * (opp_stats['FGA'] + 0.44 * opp_stats['FTA']))) * 100
    opp_stats['FTr'] = opp_stats['FTA'] / opp_stats['FGA']

    opp_stats = pd.merge(opp_stats, l5_opp_stats, on=['OPPONENT_ID', 'POSITION'], how='left')

    # get the avg for the team stats as well
    team_stats = merged.groupby(['TEAM_ID', 'POSITION'])[['PTS', 'REB', 'AST']].mean().reset_index()
    team_stats.rename(columns={'PTS': 'TEAM_PTS','REB': 'TEAM_REB','AST': 'TEAM_AST'}, inplace=True)

    # team-level defensive context metrics from full (unfiltered) logs
    game_team = logs_raw.groupby(['GAME_ID', 'TEAM_ID'], as_index=False).agg({
        'PTS': 'sum',
        'FGA': 'sum',
        'FTA': 'sum',
        'OREB': 'sum',
        'TOV': 'sum'
    })
    game_team['POSS'] = game_team['FGA'] + 0.44 * game_team['FTA'] - game_team['OREB'] + game_team['TOV']
    game_team = game_team.rename(columns={'TEAM_ID': 'TEAM_ID_ALLOWED', 'PTS': 'OPP_PTS', 'POSS': 'OPP_POSS'})

    defense_games = game_team.merge(game_team, on='GAME_ID', suffixes=('_ALLOWED', '_DEF'))
    defense_games = defense_games[defense_games['TEAM_ID_ALLOWED'] != defense_games['TEAM_ID_DEF']]

    defense_context = defense_games.groupby('TEAM_ID_DEF', as_index=False).agg({
        'OPP_PTS': 'sum',
        'OPP_POSS': 'sum'
    }).rename(columns={'TEAM_ID_DEF': 'TEAM_ID'})
    defense_context['PACE'] = defense_games.groupby('TEAM_ID_DEF')['OPP_POSS'].mean().values
    defense_context['DEF_RTG'] = (defense_context['OPP_PTS'] / defense_context['OPP_POSS']) * 100

    final_result = pd.merge(opp_stats, team_stats, left_on=['OPPONENT_ID', 'POSITION'], right_on=['TEAM_ID', 'POSITION'], how='left')
    final_result = pd.merge(final_result, defense_context[['TEAM_ID', 'PACE', 'DEF_RTG']], on='TEAM_ID', how='left')

    final_result['TEAM'] = final_result['TEAM_ID'].map({v: k for k, v in team_id_lookup.items()})

    final_result = final_result.round(2)
    final_result[['TEAM_PTS', 'TEAM_REB', 'TEAM_AST']] = final_result[['TEAM_PTS', 'TEAM_REB', 'TEAM_AST']].fillna(0)

    final_result = final_result[~final_result['TEAM'].isin(['SEM', 'MEL', 'GUA', 'HAP'])]

    #rank pra and 3 pointers
    final_result['PTS_RANK'] = final_result.groupby('POSITION')['PTS'].rank(ascending=False, method='min')
    final_result['REB_RANK'] = final_result.groupby('POSITION')['REB'].rank(ascending=False, method='min')
    final_result['AST_RANK'] = final_result.groupby('POSITION')['AST'].rank(ascending=False, method='min')
    final_result['FG3M_RANK'] = final_result.groupby('POSITION')['FG3M'].rank(ascending=False, method='min')
    final_result['FG3A_RANK'] = final_result.groupby('POSITION')['FG3A'].rank(ascending=False, method='min')
    final_result['STL_RANK'] = final_result.groupby('POSITION')['STL'].rank(ascending=False, method='min')
    final_result['BLK_RANK'] = final_result.groupby('POSITION')['BLK'].rank(ascending=False, method='min')
    
    # rank new metrics
    final_result['eFG_RANK'] = final_result.groupby('POSITION')['eFG_PCT'].rank(ascending=False, method='min')
    final_result['TS_RANK'] = final_result.groupby('POSITION')['TS_PCT'].rank(ascending=False, method='min')
    final_result['FTr_RANK'] = final_result.groupby('POSITION')['FTr'].rank(ascending=False, method='min')
    # TOV is better when high, so ascending=True means lowest TOV gets rank 1 (worst defense)
    final_result['TOV_RANK'] = final_result.groupby('POSITION')['TOV'].rank(ascending=True, method='min')
    final_result['PACE_RANK'] = final_result.groupby('POSITION')['PACE'].rank(ascending=False, method='min')
    final_result['DEF_RTG_RANK'] = final_result.groupby('POSITION')['DEF_RTG'].rank(ascending=False, method='min')

    final_result = final_result.round(2)
    export_cols = [
        'TEAM', 'POSITION', 'OPP_MIN',
        'PTS', 'PTS_RANK', 'L5_PTS',
        'REB', 'REB_RANK', 'L5_REB',
        'AST', 'AST_RANK', 'L5_AST',
        'PACE', 'PACE_RANK', 'DEF_RTG', 'DEF_RTG_RANK',
        'eFG_PCT', 'eFG_RANK', 'TS_PCT', 'TS_RANK',
        'FGA', 'FG3M', 'FG3M_RANK', 'FG3A', 'FG3A_RANK',
        'FTA', 'FTr', 'FTr_RANK',
        'STL', 'STL_RANK', 'BLK', 'BLK_RANK',
        'TOV', 'TOV_RANK',
        'TEAM_PTS', 'TEAM_REB', 'TEAM_AST'
    ]
    # Rename MIN to OPP_MIN
    final_result.rename(columns={'MIN': 'OPP_MIN'}, inplace=True)
    final_result[export_cols].to_csv('vs_Position_withavg.csv', index=False)

def create_matchups(pos_df, final_result, ALL_TEAMS, minutes):
    """
    Flags both 'High-Volume Clashes' (OVERS) and 'Stifling Defenses' (UNDERS)
    based on separate threshold dictionaries and buffers.
    """
    if ALL_TEAMS:
        positions = ['C', 'PF', 'PG', 'SF', 'SG']
        output_file = 'threshold_test_results.csv'
        current_date = datetime.now().strftime("%Y-%m-%d")

        # 1. OVER Thresholds (High Volume Clash)
        over_thresholds = {
            'PG': {'PTS': 22.5, 'REB': 5.2, 'AST': 8.5}, 
            'SG': {'PTS': 21.0, 'REB': 5.0, 'AST': 4.5}, 
            'SF': {'PTS': 20.5, 'REB': 6.8, 'AST': 5.0}, 
            'PF': {'PTS': 19.5, 'REB': 8.5, 'AST': 3.5}, 
            'C':  {'PTS': 18.5, 'REB': 12.0, 'AST': 3.5} 
        }

        # 2. UNDER Thresholds (Stifling Defense Baseline)
        under_thresholds = {
            'PG': {'PTS': 18.7, 'REB': 4.2, 'AST': 5.75},
            'SG': {'PTS': 16.5, 'REB': 3.9, 'AST': 3.5},
            'SF': {'PTS': 17.1, 'REB': 5.6, 'AST': 3.5},
            'PF': {'PTS': 15.5, 'REB': 5.5, 'AST': 2.4},
            'C':  {'PTS': 15.0, 'REB': 9.0, 'AST': 2.5}
        }

        # Buffers for Under: Team avg must be baseline + buffer
        under_buffers = {'PTS': 8.0, 'REB': 4.0, 'AST': 4.0}

        good_matchups = []
        
        # Prepare position and log data
        pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})
        logs = pd.read_csv("logs.csv")
        logs = logs[logs['MIN'] >= minutes] # Using 20 as per your latest request
        merged = logs.merge(pos_map, on='PLAYER_NAME')

        # Map Team ABV to IDs for opponent lookup
        team_id_map = merged[['TEAM_ABBREVIATION', 'TEAM_ID']].drop_duplicates()
        team_id_lookup = dict(zip(team_id_map['TEAM_ABBREVIATION'], team_id_map['TEAM_ID']))
        merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1]
        merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup)
        
        for team_pair in ALL_TEAMS:
            T1, T2 = team_pair
            selected_teams = final_result[final_result['TEAM'].isin([T1, T2])]
            
            for pos in positions:
                t1_row = selected_teams[(selected_teams['TEAM'] == T1) & (selected_teams['POSITION'] == pos)]
                t2_row = selected_teams[(selected_teams['TEAM'] == T2) & (selected_teams['POSITION'] == pos)]

                if not t1_row.empty and not t2_row.empty:
                    o_limit = over_thresholds[pos]
                    u_limit = under_thresholds[pos]
                    
                    def get_players(team_abv, position):
                        return merged[(merged['TEAM_ABBREVIATION'] == team_abv) & 
                                    (merged['POSITION'] == position)]['PLAYER_NAME'].unique().tolist()

                    for stat in ['PTS', 'REB', 'AST']:
                        # --- OVER LOGIC ---
                        # T1 Over (High Offense vs Leaky Defense)
                        if t1_row[f'TEAM_{stat}'].values[0] >= o_limit[stat] and t2_row[stat].values[0] >= o_limit[stat]:
                            for p in get_players(T1, pos):
                                good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T1} vs {T2}", 'Player': p, 'Pos': pos, 'Stat': stat, 'Direction': 'OVER'})
                        
                        # T2 Over
                        if t2_row[f'TEAM_{stat}'].values[0] >= o_limit[stat] and t1_row[stat].values[0] >= o_limit[stat]:
                            for p in get_players(T2, pos):
                                good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T2} vs {T1}", 'Player': p, 'Pos': pos, 'Stat': stat, 'Direction': 'OVER'})

                        # --- UNDER LOGIC ---
                        buff = under_buffers[stat]
                        
                        # T1 Under (T1 produces high, but T2 defense is stingy)
                        if t1_row[f'TEAM_{stat}'].values[0] >= (u_limit[stat] + buff) and t2_row[stat].values[0] <= u_limit[stat]:
                            for p in get_players(T1, pos):
                                good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T1} vs {T2}", 'Player': p, 'Pos': pos, 'Stat': stat, 'Direction': 'UNDER'})

                        # T2 Under
                        if t2_row[f'TEAM_{stat}'].values[0] >= (u_limit[stat] + buff) and t1_row[stat].values[0] <= u_limit[stat]:
                            for p in get_players(T2, pos):
                                good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T2} vs {T1}", 'Player': p, 'Pos': pos, 'Stat': stat, 'Direction': 'UNDER'})

        if good_matchups:
            new_results_df = pd.DataFrame(good_matchups).drop_duplicates()
            file_exists = os.path.isfile(output_file)
            new_results_df.to_csv(output_file, mode='a', index=False, header=not file_exists)
            print(f"Added {len(new_results_df)} rows to {output_file}")
            return new_results_df
            
    return pd.DataFrame()
