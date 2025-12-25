from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster, playergamelogs
import pandas as pd
import time
import os
from datetime import datetime


def make_data(pos_df):
    #get positions for each players
    
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

    #get the game logs
    logs = pd.read_csv("logs.csv")

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
    
    if ALL_TEAMS:
        positions = ['C', 'PF', 'PG', 'SF', 'SG']
        output_file = 'threshold_test_results.csv'

        # Add current date for tracking
        current_date = datetime.now().strftime("%Y-%m-%d")
        thresholds = {

            'PG': {'PTS': 22.5, 'REB': 5.2, 'AST': 8.5}, 
            'SG': {'PTS': 21.0, 'REB': 5.0, 'AST': 4.5}, 
            'SF': {'PTS': 20.5, 'REB': 6.8, 'AST': 5.0}, 
            'PF': {'PTS': 19.5, 'REB': 8.5, 'AST': 3.5}, 
            'C':  {'PTS': 18.5, 'REB': 12.0, 'AST': 3.5} 
        }

        good_matchups = []

        pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

        #get the game logs
        logs = pd.read_csv("logs.csv")

        #only select players who have more than 20 minutes play time
        logs = logs[logs['MIN'] >= 30] 


        merged = logs.merge(pos_map, on='PLAYER_NAME')


        team_id_map = merged[['TEAM_ABBREVIATION', 'TEAM_ID']].drop_duplicates()
        team_id_lookup = dict(zip(team_id_map['TEAM_ABBREVIATION'], team_id_map['TEAM_ID']))

        merged['OPPONENT_ABV'] = merged['MATCHUP'].str.split(' ').str[-1]
        merged['OPPONENT_ID'] = merged['OPPONENT_ABV'].map(team_id_lookup)
        
        # 2. Logic Loop (Applied across ALL_TEAMS pairs)
        for team_pair in ALL_TEAMS:
            T1, T2 = team_pair
            selected_teams = final_result[final_result['TEAM'].isin([T1, T2])]
            
            for pos in positions:
                t1_row = selected_teams[(selected_teams['TEAM'] == T1) & (selected_teams['POSITION'] == pos)]
                t2_row = selected_teams[(selected_teams['TEAM'] == T2) & (selected_teams['POSITION'] == pos)]

                if not t1_row.empty and not t2_row.empty:
                    limit = thresholds[pos]
                    
                    # Internal helper to find players from the 'merged' dataframe
                    def get_players(team_abv, position):
                        return merged[(merged['TEAM_ABBREVIATION'] == team_abv) & 
                                    (merged['POSITION'] == position)]['PLAYER_NAME'].unique().tolist()

                    # Check PTS, REB, AST for "High-Volume Clash"
                    for stat in ['PTS', 'REB', 'AST']:
                        # T1 Check
                        if t1_row[f'TEAM_{stat}'].values[0] >= limit[stat] and t2_row[stat].values[0] >= limit[stat]:
                            for p in get_players(T1, pos):
                                good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T1} vs {T2}", 'Player': p, 'Pos': pos, 'Stat': stat})
                        
                        # T2 Check
                        if t2_row[f'TEAM_{stat}'].values[0] >= limit[stat] and t1_row[stat].values[0] >= limit[stat]:
                            for p in get_players(T2, pos):
                                good_matchups.append({'Run_Date': current_date, 'Matchup': f"{T2} vs {T1}", 'Player': p, 'Pos': pos, 'Stat': stat})

        # 3. Create DataFrame and APPEND to CSV
        if good_matchups:
            new_results_df = pd.DataFrame(good_matchups).drop_duplicates()

            # If file exists, don't write header. If it doesn't exist, write header.
            file_exists = os.path.isfile(output_file)
            new_results_df.to_csv(output_file, mode='a', index=False, header=not file_exists)
            
            print(f"Added {len(new_results_df)} rows to {output_file} for date {current_date}")
            return new_results_df
        else:
            print("No matches found for these thresholds today.")
            return pd.DataFrame()
    return pd.DataFrame()