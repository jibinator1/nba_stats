from flask import Flask, render_template, request, redirect, url_for
from update import make_data, create_matchups, fetch_logs
import pandas as pd
import os 
from datetime import datetime

app = Flask(__name__)

# Helper to load global data frames
def load_data():
    csv_path = 'vs_Position_withavg.csv'
    df = pd.read_csv(csv_path)
    pos_df = pd.read_csv('positions.csv')
    
    # Auto-update if missing rank columns
    if 'eFG_RANK' not in df.columns:
        from update import make_data
        make_data(pos_df, 25)
        df = pd.read_csv(csv_path)
        
    last_updated = datetime.fromtimestamp(os.path.getmtime(csv_path)).strftime('%Y-%m-%d %I:%M %p')
    return df, pos_df, last_updated

@app.route('/manual_update', methods=['POST'])
def manual_update():
    min_val = request.form.get('minutes', 25)
    try:
        min_val = int(min_val)
    except ValueError:
        min_val = 25

    # Fetch new data from NBA API before loading CSVs
    fetch_logs()
    
    df, pos_df, last_updated = load_data()
    make_data(pos_df, min_val) 
    
    return redirect(url_for('index'))

@app.route('/', methods=['GET', 'POST'])
def index():
    df_global, pos_df_global, last_updated = load_data()
    team1 = ""
    team2 = ""
    minutes = 25
    
    if request.method == 'POST':
        # 1. Get user input
        team1 = request.form.get('team1', '')
        team2 = request.form.get('team2', '')
        selected_teams = [team1, team2]
        sql_filter = request.form.get('sql_filter', '')
        
        raw_minutes = request.form.get('minutes', '25')
        minutes = int(raw_minutes) if raw_minutes.isdigit() else 25

        # 2. Rebuild the CSV first so the view reflects the new threshold
        make_data(pos_df_global, minutes)
        
        # 3. Reload the global dataframe after the file is updated
        df_global, _, last_updated = load_data()
    else:
        selected_teams = ["", ""]
        sql_filter = ""
    
    df = df_global.copy()
    if team1 !="" and team2!="":
        df = df[df['TEAM'].isin(selected_teams)]
    elif team1!="" and team2=="":
        df = df[df['TEAM']==team1]
    elif team1=="" and  team2!="":
        df = df[df['TEAM']==team2]

    error_msg = None
    if sql_filter:
        try:
            # Simple translation from SQL-like to Pandas-like
            query_str = sql_filter.replace('=', '==').replace('====', '==').replace(' AND ', ' and ').replace(' OR ', ' or ')
            df = df.query(query_str)
        except Exception as e:
            error_msg = f"Invalid search query: {e}"

    team_summary = ""
    selected_team = team1 if (team1 and not team2) else team2 if (team2 and not team1) else ""
    if selected_team and not df.empty and not error_msg:
        rank_cols = [c for c in df.columns if 'RANK' in c]
        if rank_cols:
            df_team = df[df['TEAM'] == selected_team]
            if not df_team.empty:
                best_rank_val, worst_rank_val = -1, 999
                best_stat, worst_stat = "", ""
                best_pos, worst_pos = "", ""
                for _, row in df_team.iterrows():
                    pos = row['POSITION']
                    for col in rank_cols:
                        val = row[col]
                        if pd.notna(val):
                            stat_name = col.replace('_RANK', '')
                            # Rank 1 is worst defense, Rank 30 is best
                            if val < worst_rank_val:
                                worst_rank_val, worst_stat, worst_pos = val, stat_name, pos
                            if val > best_rank_val:
                                best_rank_val, best_stat, best_pos = val, stat_name, pos
                
                if best_stat and worst_stat:
                    team_summary = f"The {selected_team} are most vulnerable to {worst_pos} {worst_stat} (Rank {int(worst_rank_val)}) but have elite defense against {best_pos} {best_stat} (Rank {int(best_rank_val)})."

    return render_template('index.html', 
                           records=df.to_dict('records'), 
                           colnames=df.columns.values,
                           selected_teams=selected_teams, minutes = minutes,
                           sql_filter=sql_filter, error_msg=error_msg, team_summary=team_summary,
                           last_updated=last_updated)

@app.route('/matchup', methods=['GET', 'POST'])
def matchup():
    df_global, pos_df_global, last_updated = load_data()
    df = df_global.copy()
    pos_df = pos_df_global.copy()
    
    team_vs_list = []
    teams1 = ""
    teams2 = ""
    minutes = 25

    if request.method == 'POST':
        raw_t1 = request.form.get('teams1', '')
        raw_t2 = request.form.get('teams2', '')
        
        teams1 = raw_t1.replace(" ", "").upper()
        teams2 = raw_t2.replace(" ", "").upper()
        
        raw_minutes = request.form.get('minutes', '25')
        minutes = int(raw_minutes) if raw_minutes.isdigit() else 25

        # Rebuild data for matchups based on new minute threshold
        make_data(pos_df, minutes)
        df, _, last_updated = load_data() # Reload updated data

        teams1_list = teams1.split(",")
        teams2_list = teams2.split(",")
        
        length = min(len(teams1_list), len(teams2_list))
        for i in range(length):
            team_vs_list.append([teams1_list[i], teams2_list[i]])
        
    if team_vs_list:
        matchup_df = create_matchups(pos_df, df, team_vs_list, minutes)
    else:
        matchup_df = pd.DataFrame()

    return render_template('index.html', 
                           records=matchup_df.to_dict('records'), 
                           colnames=matchup_df.columns.values,
                           team_vs_list=team_vs_list, 
                           teams1=teams1, 
                           teams2=teams2,
                           selected_teams=["", ""],
                           page_type='matchup', minutes = minutes,
                           last_updated=last_updated)

if __name__ == '__main__':
    app.run(debug=True)