from flask import Flask, render_template, request, redirect, url_for
from update import make_data, create_matchups, fetch_logs, get_todays_games
import pandas as pd
import os 
from datetime import datetime

TODAYS_GAMES_CACHE = []
TODAYS_CACHE_DATE = None

app = Flask(__name__)

QUICK_VIEW_COLUMNS = [
    'TEAM', 'POSITION', 'MATCHUP_SCORE',
    'PTS_RANK', 'REB_RANK', 'AST_RANK', 'eFG_RANK', 'FG3M_RANK', 'TOV_RANK'
]

DEFAULT_HIDDEN_COLUMNS = {
    'TS_PCT', 'TS_RANK',
    'FGM', 'FGA', 'FG3A', 'FG3A_RANK',
    'FTA', 'FTM', 'FTr', 'FTr_RANK',
    'TEAM_PTS', 'TEAM_REB', 'TEAM_AST',
}

RADAR_COLUMNS = [
    ('PTS_RANK', 'PTS'),
    ('REB_RANK', 'REB'),
    ('AST_RANK', 'AST'),
    ('eFG_RANK', 'eFG'),
    ('FG3M_RANK', '3PM'),
    ('TOV_RANK', 'TOV'),
    ('DEF_RTG_RANK', 'DEF RTG'),
    ('PACE_RANK', 'PACE'),
]

def fetch_todays_games_cache():
    global TODAYS_GAMES_CACHE, TODAYS_CACHE_DATE
    current_date = datetime.now().date()
    
    # If date changed OR cache is currently empty (due to a previous failed fetch), try again
    if TODAYS_CACHE_DATE != current_date or not TODAYS_GAMES_CACHE:
        TODAYS_GAMES_CACHE = get_todays_games()
        TODAYS_CACHE_DATE = current_date
    return TODAYS_GAMES_CACHE

# Helper to load global data frames
def load_data():
    csv_path = 'vs_Position_withavg.csv'
    df = pd.read_csv(csv_path)
    pos_df = pd.read_csv('positions.csv')
    
    # Auto-update if missing rank columns
    if 'eFG_RANK' not in df.columns:
        from update import make_data
        make_data(pos_df, 20)
        df = pd.read_csv(csv_path)
        
    last_updated = datetime.fromtimestamp(os.path.getmtime(csv_path)).strftime('%Y-%m-%d %I:%M %p')
    return df, pos_df, last_updated

def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    rank_cols = [c for c in df.columns if c.endswith('_RANK')]
    if rank_cols:
        df['MATCHUP_SCORE'] = (31 - df[rank_cols].mean(axis=1)).round(2)

    for stat in ['PTS', 'REB', 'AST']:
        base_col = stat
        l5_col = f'L5_{stat}'
        trend_col = f'{stat}_TREND'
        if base_col in df.columns and l5_col in df.columns:
            diff = df[l5_col] - df[base_col]
            df[trend_col] = diff.apply(lambda x: '↑' if x > 0.5 else ('↓' if x < -0.5 else '→'))
    return df

def apply_query_filter(df: pd.DataFrame, sql_filter: str):
    if not sql_filter:
        return df, None

    try:
        return df.query(sql_filter, engine='python'), None
    except Exception as e:
        return df, f"Invalid search query: {e}"

def build_league_average_row(df: pd.DataFrame):
    if df.empty:
        return None
    avg_row = {}
    numeric_cols = df.select_dtypes(include='number').columns
    for col in df.columns:
        if col in numeric_cols:
            avg_row[col] = round(df[col].mean(), 2)
        else:
            avg_row[col] = ''
    if 'TEAM' in avg_row:
        avg_row['TEAM'] = 'LEAGUE_AVG'
    if 'POSITION' in avg_row:
        avg_row['POSITION'] = 'ALL'
    return avg_row

def build_radar_payload():
    return [{'column': col, 'label': label} for col, label in RADAR_COLUMNS]

@app.route('/manual_update', methods=['POST'])
def manual_update():
    min_val = request.form.get('minutes', 20)
    try:
        min_val = int(min_val)
    except ValueError:
        min_val = 20

    last_n = request.form.get('last_n_games', 20)
    try:
        last_n = int(last_n)
    except ValueError:
        last_n = 20

    # Fetch new data from NBA API before loading CSVs
    fetch_logs()
    
    df, pos_df, last_updated = load_data()
    make_data(pos_df, min_val, last_n) 
    
    return redirect(url_for('index'))

@app.route('/', methods=['GET', 'POST'])
def index():
    df_global, pos_df_global, last_updated = load_data()
    todays_games = fetch_todays_games_cache()
    team1 = ""
    team2 = ""
    minutes = 20
    last_n_games = 20
    
    if request.method == 'POST':
        # 1. Get user input
        team1 = request.form.get('team1', '')
        team2 = request.form.get('team2', '')
        selected_teams = [team1, team2]
        sql_filter = request.form.get('sql_filter', '')
        
        raw_minutes = request.form.get('minutes', '20')
        minutes = int(raw_minutes) if raw_minutes.isdigit() else 20

        raw_last_n = request.form.get('last_n_games', '20')
        last_n_games = int(raw_last_n) if raw_last_n.isdigit() else 20

        # 2. Rebuild the CSV first so the view reflects the new threshold
        make_data(pos_df_global, minutes, last_n_games)
        
        # 3. Reload the global dataframe after the file is updated
        df_global, _, last_updated = load_data()
    else:
        selected_teams = ["", ""]
        sql_filter = ""
    
    df = enrich_dataframe(df_global.copy())
    if team1 !="" and team2!="":
        df = df[df['TEAM'].isin(selected_teams)]
    elif team1!="" and team2=="":
        df = df[df['TEAM']==team1]
    elif team1=="" and  team2!="":
        df = df[df['TEAM']==team2]

    error_msg = None
    if sql_filter:
        df, error_msg = apply_query_filter(df, sql_filter)

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

    league_avg_row = build_league_average_row(df)
    available_cols = list(df.columns.values)
    quick_cols = [c for c in QUICK_VIEW_COLUMNS if c in available_cols]
    radar_metrics = [item for item in build_radar_payload() if item['column'] in available_cols]

    return render_template('index.html', 
                           records=df.to_dict('records'), 
                           colnames=available_cols,
                            quick_cols=quick_cols,
                            default_hidden_cols=list(DEFAULT_HIDDEN_COLUMNS),
                            radar_metrics=radar_metrics,
                            league_avg_row=league_avg_row,
                            selected_teams=selected_teams, minutes=minutes,
                            last_n_games=last_n_games,
                            sql_filter=sql_filter, error_msg=error_msg, team_summary=team_summary,
                            last_updated=last_updated, todays_games=todays_games)

@app.route('/matchup', methods=['GET', 'POST'])
def matchup():
    df_global, pos_df_global, last_updated = load_data()
    todays_games = fetch_todays_games_cache()
    df = enrich_dataframe(df_global.copy())
    pos_df = pos_df_global.copy()
    
    team_vs_list = []
    teams1 = ""
    teams2 = ""
    minutes = 20
    last_n_games = 20

    if request.method == 'POST':
        raw_t1 = request.form.get('teams1', '')
        raw_t2 = request.form.get('teams2', '')
        
        teams1 = raw_t1.replace(" ", "").upper()
        teams2 = raw_t2.replace(" ", "").upper()
        
        raw_minutes = request.form.get('minutes', '20')
        minutes = int(raw_minutes) if raw_minutes.isdigit() else 20

        raw_last_n = request.form.get('last_n_games', '20')
        last_n_games = int(raw_last_n) if raw_last_n.isdigit() else 20

        # Rebuild data for matchups based on new minute threshold
        make_data(pos_df, minutes, last_n_games)
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

    matchup_df = enrich_dataframe(matchup_df)
    
    # Generate today's picks
    todays_picks_df = create_matchups(pos_df, df_global.copy(), todays_games, minutes)
    todays_picks = todays_picks_df.to_dict('records') if not todays_picks_df.empty else []

    league_avg_row = build_league_average_row(matchup_df)
    available_cols = list(matchup_df.columns.values) if not matchup_df.empty else []
    quick_cols = [c for c in QUICK_VIEW_COLUMNS if c in available_cols]
    radar_metrics = [item for item in build_radar_payload() if item['column'] in available_cols]

    return render_template('index.html', 
                           records=matchup_df.to_dict('records'), 
                           colnames=available_cols,
                            quick_cols=quick_cols,
                            default_hidden_cols=list(DEFAULT_HIDDEN_COLUMNS),
                            radar_metrics=radar_metrics,
                            league_avg_row=league_avg_row,
                            team_vs_list=team_vs_list, 
                           teams1=teams1, 
                           teams2=teams2,
                           selected_teams=["", ""],
                           page_type='matchup', minutes=minutes,
                           last_n_games=last_n_games,
                           last_updated=last_updated,
                           todays_games=todays_games,
                           todays_picks=todays_picks)

if __name__ == '__main__':
    app.run(debug=True)
