from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster, playergamelogs
import pandas as pd
import time
import os
from datetime import datetime


def make_data(pos_df, minutes):
    #get positions for each players
    
    pos_map = pos_df[['Player', 'Pos']].rename(columns={'Player': 'PLAYER_NAME', 'Pos': 'POSITION'})

    #get the game logs
    logs = pd.read_csv("logs.csv")

    #only select players who have more than 20 minutes play time
    logs = logs[logs['MIN'] >= minutes] 


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