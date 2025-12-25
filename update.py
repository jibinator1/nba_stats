from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster, playergamelogs
import pandas as pd
import time
import os
from datetime import datetime

# Standard headers to prevent the NBA from blocking your request
HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://stats.nba.com/',
    'Connection': 'keep-alive'
}

def make_data(pos_df):
    #get positions for each players
    
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

    #get the game logs
    logs = playergamelogs.PlayerGameLogs(
        season_nullable='2025-26', 
        last_n_games_nullable=20,
        headers=HEADERS,
        timeout=60
    ).get_data_frames()[0]

    #only select players who have more than 20 minutes play time
    logs = logs[logs['MIN'] >= 30] 


    merged = logs.merge(pos_map, on='PLAYER_NAME')


    team_id_map = merged[['TEAM_ABBREVIATION', 'TEAM_ID']].drop_duplicates()
    team_id_lookup = dict(zip(team_id_map['TEAM_ABBREVIATION'], team_id_map['TEAM_ID']))

    merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1]
    merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup)

    opp_stats = merged.groupby(['OPPONENT_ID', 'POSITION'])[['PTS', 'REB', 'AST', 'FGM', 'FGA','FG3M', 'FG3A', 'OREB', 'DREB', 'STL', 'BLK', 'PF']].mean().reset_index()

    #get the avg for the team stats as well
    team_stats = merged.groupby(['TEAM_ID', 'POSITION'])[['PTS', 'REB', 'AST']].mean().reset_index()
    team_stats.rename(columns={'PTS': 'TEAM_PTS','REB': 'TEAM_REB','AST': 'TEAM_AST'}, inplace=True)

    final_result = pd.merge(opp_stats, team_stats, left_on=['OPPONENT_ID', 'POSITION'], right_on=['TEAM_ID', 'POSITION'], how='left')

    final_result['TEAM'] = final_result['TEAM_ID'].map({v: k for k, v in team_id_lookup.items()})

    final_result = final_result.round(2)
    final_result[['TEAM_PTS', 'TEAM_REB', 'TEAM_AST']] = final_result[['TEAM_PTS', 'TEAM_REB', 'TEAM_AST']].fillna(0)

    final_result = final_result[~final_result['TEAM'].isin(['SEM', 'MEL', 'GUA', 'HAP'])]

    #rank pra and 3 pointers
    final_result['PTS_Rank'] = final_result.groupby('POSITION')['PTS'].rank(ascending=False, method='min')
    final_result['REB_Rank'] = final_result.groupby('POSITION')['REB'].rank(ascending=False, method='min')
    final_result['AST_Rank'] = final_result.groupby('POSITION')['AST'].rank(ascending=False, method='min')
    final_result['FG3A_RANK'] = final_result.groupby('POSITION')['FG3A'].rank(ascending=False, method='min')

    final_result[['TEAM', 'POSITION','PTS','PTS_Rank', 'REB','REB_Rank', 'AST','AST_Rank', 'FGM', 'FGA','FG3M', 'FG3A', 'OREB', 'DREB', 'STL', 'BLK', 'PF','TEAM_PTS', 'TEAM_REB', 'TEAM_AST']].to_csv('vs_Position_withavg.csv', index= False)






def create_matchups(pos_df, final_result, ALL_TEAMS, thresholds):
    output_file = 'threshold_test_results.csv'
    column_names = ['Date', 'Team', 'Player', 'Role', 'Stat']
    
    # 1. Handle empty input immediately
    if not ALL_TEAMS:
        empty_df = pd.DataFrame(columns=column_names)
        empty_df.to_csv(output_file, index=False)
        return empty_df

    positions = ['C', 'PF', 'PG', 'SF', 'SG']
    current_date = datetime.now().strftime("%Y-%m-%d")
    good_matchups = []

    # 2. Prepare Reference Data
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})
    
    # Fetching logs (Note: verify season string matches current year)
    logs = playergamelogs.PlayerGameLogs(
        season_nullable='2024-25', 
        last_n_games_nullable=20,
        headers=HEADERS,
        timeout=60
    ).get_data_frames()[0]
    logs = logs[logs['MIN'] >= 30] 
    merged = logs.merge(pos_map, on='PLAYER_NAME')

    # 3. Logic Loop
    for team_pair in ALL_TEAMS:
        T1, T2 = team_pair
        selected_teams = final_result[final_result['TEAM'].isin([T1, T2])]
        
        for pos in positions:
            t1_row = selected_teams[(selected_teams['TEAM'] == T1) & (selected_teams['POSITION'] == pos)]
            t2_row = selected_teams[(selected_teams['TEAM'] == T2) & (selected_teams['POSITION'] == pos)]

            if not t1_row.empty and not t2_row.empty:
                limit = thresholds[pos]
                
                for stat in ['PTS', 'REB', 'AST']:
                    # T1 Check (Team 1 Player vs Team 2 Defense)
                    if t1_row[f'TEAM_{stat}'].values[0] >= limit[stat] and t2_row[stat].values[0] >= limit[stat]:
                        players = merged[(merged['TEAM_ABBREVIATION'] == T1) & (merged['POSITION'] == pos)]['PLAYER_NAME'].unique()
                        for p in players:
                            good_matchups.append({
                                'Date': current_date, 
                                'Team': T1, 
                                'Player': p, 
                                'Role': pos, 
                                'Stat': stat
                            })
                    
                    # T2 Check (Team 2 Player vs Team 1 Defense)
                    if t2_row[f'TEAM_{stat}'].values[0] >= limit[stat] and t1_row[stat].values[0] >= limit[stat]:
                        players = merged[(merged['TEAM_ABBREVIATION'] == T2) & (merged['POSITION'] == pos)]['PLAYER_NAME'].unique()
                        for p in players:
                            good_matchups.append({
                                'Date': current_date, 
                                'Team': T2, 
                                'Player': p, 
                                'Role': pos, 
                                'Stat': stat
                            })

    # 4. Finalize, Save (Replace), and Return
    if good_matchups:
        # Create DataFrame and ensure specific column order
        results_df = pd.DataFrame(good_matchups)
        results_df = results_df[column_names].drop_duplicates()
        
        # Saves and overwrites existing file
        results_df.to_csv(output_file, index=False)
        return results_df
    else:
        # Create empty file with headers if no matches found
        empty_df = pd.DataFrame(columns=column_names)
        empty_df.to_csv(output_file, index=False)
        return empty_df
