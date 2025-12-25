from flask import Flask, render_template, request, redirect, url_for
from update import make_data, create_matchups
import pandas as pd
import os 

app = Flask(__name__)

# Helper to load global data frames
def load_data():
    df = pd.read_csv('vs_Position_withavg.csv')
    pos_df = pd.read_csv('positions.csv')
    return df, pos_df

@app.route('/manual_update', methods=['POST'])
def manual_update():
    df, pos_df = load_data()
    make_data(pos_df)
    return redirect(url_for('index'))

@app.route('/', methods=['GET', 'POST'])
def index():
    df_global, _ = load_data()
    team1 = ""
    team2 = ""
    if request.method == 'POST':
        team1 = request.form.get('team1', '')
        team2 = request.form.get('team2', '')
        selected_teams = [team1, team2]
    else:
        selected_teams = ["", ""]
    
    df = df_global.copy()
    if team1 !="" and team2!="":
        df = df[df['TEAM'].isin(selected_teams)]
    elif team1!="" and team2=="":
        df = df[df['TEAM']==team1]
    elif team1=="" and  team2!="":
        df = df[df['TEAM']==team2]

    return render_template('index.html', 
                           records=df.to_dict('records'), 
                           colnames=df.columns.values,
                           selected_teams=selected_teams)

@app.route('/matchup', methods=['GET', 'POST'])
def matchup():
    df_global, pos_df_global = load_data()
    df = df_global.copy()
    pos_df = pos_df_global.copy()
    
    thresholds = {
        'PG': {'PTS': 22.5, 'REB': 5.2, 'AST': 8.5}, 
        'SG': {'PTS': 21.0, 'REB': 5.0, 'AST': 4.5}, 
        'SF': {'PTS': 20.5, 'REB': 6.8, 'AST': 5.0}, 
        'PF': {'PTS': 19.5, 'REB': 8.5, 'AST': 3.5}, 
        'C':  {'PTS': 18.5, 'REB': 12.0, 'AST': 3.5} 
    }
    
    team_vs_list = []
    teams1 = ""
    teams2 = ""

    if request.method == 'POST':
        raw_t1 = request.form.get('teams1', '')
        raw_t2 = request.form.get('teams2', '')
        
        teams1 = raw_t1.replace(" ", "").upper()
        teams2 = raw_t2.replace(" ", "").upper()

        teams1_list = teams1.split(",")
        teams2_list = teams2.split(",")

        length = min(len(teams1_list), len(teams2_list))
        for i in range(length):
            team_vs_list.append([teams1_list[i], teams2_list[i]])
        
    if team_vs_list:
        matchup_df = create_matchups(pos_df, df, team_vs_list, thresholds)
    else:
        matchup_df = pd.DataFrame()

    return render_template('index.html', 
                           records=matchup_df.to_dict('records'), 
                           colnames=matchup_df.columns.values,
                           team_vs_list=team_vs_list, 
                           teams1=teams1, 
                           teams2=teams2,
                           selected_teams=["", ""],
                           page_type='matchup')

if __name__ == '__main__':
    app.run(debug=True)