from flask import Flask, render_template, request, redirect, url_for, jsonify
from update import make_data, find_streaks, fetch_logs, get_todays_games
import pandas as pd
import os
from datetime import datetime
from urllib.parse import urlparse
import requests

# --- Configuration ---
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/jibinator1/nba_stats/main/"
IS_VERCEL = os.environ.get('VERCEL') == '1'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)

NBA_TEAMS = [
    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
    'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
    'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
]

NBA_TEAM_NAMES = {
    'ATL': 'Atlanta Hawks',
    'BOS': 'Boston Celtics',
    'BKN': 'Brooklyn Nets',
    'CHA': 'Charlotte Hornets',
    'CHI': 'Chicago Bulls',
    'CLE': 'Cleveland Cavaliers',
    'DAL': 'Dallas Mavericks',
    'DEN': 'Denver Nuggets',
    'DET': 'Detroit Pistons',
    'GSW': 'Golden State Warriors',
    'HOU': 'Houston Rockets',
    'IND': 'Indiana Pacers',
    'LAC': 'LA Clippers',
    'LAL': 'Los Angeles Lakers',
    'MEM': 'Memphis Grizzlies',
    'MIA': 'Miami Heat',
    'MIL': 'Milwaukee Bucks',
    'MIN': 'Minnesota Timberwolves',
    'NOP': 'New Orleans Pelicans',
    'NYK': 'New York Knicks',
    'OKC': 'Oklahoma City Thunder',
    'ORL': 'Orlando Magic',
    'PHI': 'Philadelphia 76ers',
    'PHX': 'Phoenix Suns',
    'POR': 'Portland Trail Blazers',
    'SAC': 'Sacramento Kings',
    'SAS': 'San Antonio Spurs',
    'TOR': 'Toronto Raptors',
    'UTA': 'Utah Jazz',
    'WAS': 'Washington Wizards'
}

DEFAULT_PAIRINGS = [
    ('BOS', 'MIA'),
    ('LAL', 'GSW'),
    ('NYK', 'PHI'),
    ('MIL', 'IND'),
    ('OKC', 'DEN'),
    ('MIN', 'DAL'),
    ('CLE', 'DET'),
    ('PHX', 'SAC'),
    ('ORL', 'ATL'),
    ('HOU', 'NOP'),
    ('LAC', 'MEM'),
    ('TOR', 'CHI'),
    ('SAS', 'POR'),
    ('CHA', 'BKN'),
    ('UTA', 'WAS')
]

class VercelPathMiddleware:
    """
    Normalizes Vercel serverless request paths.
    Vercel rewrites requests to /app.py and supplies the original client path
    in HTTP_X_MATCHED_PATH or HTTP_X_VERCEL_MATCHED_PATH headers.
    This middleware restores PATH_INFO and QUERY_STRING so Flask routing matches correctly.
    """
    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app

    def __call__(self, environ, start_response):
        matched_path = environ.get('HTTP_X_MATCHED_PATH') or environ.get('HTTP_X_VERCEL_MATCHED_PATH')
        if matched_path:
            parsed = urlparse(matched_path)
            if not parsed.path.endswith('app.py') and not parsed.path.endswith('index.py'):
                environ['PATH_INFO'] = parsed.path
                if parsed.query and not environ.get('QUERY_STRING'):
                    environ['QUERY_STRING'] = parsed.query
            else:
                environ['PATH_INFO'] = '/'
        elif environ.get('PATH_INFO') in ('/app.py', 'app.py', '/api/index.py', 'api/index.py'):
            environ['PATH_INFO'] = '/'

        return self.wsgi_app(environ, start_response)

app.wsgi_app = VercelPathMiddleware(app.wsgi_app)
handler = app

def load_data():
    """Helper to load global data frames (local first, GitHub raw fallback)."""
    csv_filename = os.path.join(BASE_DIR, 'vs_Position_withavg.csv')
    pos_filename = os.path.join(BASE_DIR, 'positions.csv')
    
    if os.path.exists(csv_filename) and os.path.exists(pos_filename):
        try:
            df = pd.read_csv(csv_filename)
            pos_df = pd.read_csv(pos_filename)
            last_updated = datetime.fromtimestamp(os.path.getmtime(csv_filename)).strftime('%Y-%m-%d %I:%M %p')
            return df, pos_df, last_updated
        except Exception as e:
            print(f"Local file load error: {e}")

    try:
        df = pd.read_csv(f"{GITHUB_RAW_BASE}{os.path.basename(csv_filename)}")
        pos_df = pd.read_csv(f"{GITHUB_RAW_BASE}{os.path.basename(pos_filename)}")
        last_updated = "Remote Sync"
        return df, pos_df, last_updated
    except Exception as e:
        print(f"Remote file load error: {e}")
        return pd.DataFrame(), pd.DataFrame(), "Sync Error"

def fetch_todays_games():
    """Returns today's matches. Checks local file first, then remote source."""
    local_path = os.path.join(BASE_DIR, 'todays_games.csv')
    if os.path.exists(local_path):
        try:
            df = pd.read_csv(local_path)
            games_list = df.groupby('GAME_ID')['TEAM_ABBREVIATION'].apply(list).tolist()
            valid = [g for g in games_list if len(g) == 2]
            if valid:
                return valid
        except Exception:
            pass

    if IS_VERCEL:
        try:
            url = f"{GITHUB_RAW_BASE}todays_games.csv"
            df = pd.read_csv(url)
            games_list = df.groupby('GAME_ID')['TEAM_ABBREVIATION'].apply(list).tolist()
            valid = [g for g in games_list if len(g) == 2]
            if valid:
                return valid
        except Exception as e:
            print(f"Error fetching remote games: {e}")

    local_fallback = get_todays_games()
    return local_fallback if local_fallback else []

def compute_team_stats_map(df: pd.DataFrame):
    """Computes defensive ratings, pace, and positional rankings for each NBA team."""
    stats_map = {}
    if df is None or df.empty:
        return stats_map

    teams = df['TEAM'].unique()
    for team in teams:
        t_df = df[df['TEAM'] == team]
        if t_df.empty:
            continue

        c_row = t_df[t_df['POSITION'] == 'C']
        pf_row = t_df[t_df['POSITION'] == 'PF']
        pg_row = t_df[t_df['POSITION'] == 'PG']
        sf_row = t_df[t_df['POSITION'] == 'SF']
        sg_row = t_df[t_df['POSITION'] == 'SG']

        c_row = c_row.iloc[0] if not c_row.empty else t_df.iloc[0]
        pf_row = pf_row.iloc[0] if not pf_row.empty else t_df.iloc[0]
        pg_row = pg_row.iloc[0] if not pg_row.empty else t_df.iloc[0]
        sf_row = sf_row.iloc[0] if not sf_row.empty else t_df.iloc[0]
        sg_row = sg_row.iloc[0] if not sg_row.empty else t_df.iloc[0]

        def_rtg_val = float(c_row.get('DEF_RTG', 112.0))
        def_rtg_rank = int(c_row.get('DEF_RTG_RANK', 15))
        pace_val = float(c_row.get('PACE', 100.0))
        pace_rank = int(c_row.get('PACE_RANK', 15))

        # Overall character based on Defensive Rating Rank (Lower def rtg is better, rank 25-30 or 1-10 depending on scale)
        # In this dataset OKC is rank 30 (105.05) and UTA is rank 1 (120.34).
        # We invert rank so #1 is the best defense in the league!
        league_def_rank = int(31 - def_rtg_rank) if 1 <= def_rtg_rank <= 30 else def_rtg_rank

        if league_def_rank <= 10:
            char = "Elite Defense"
        elif league_def_rank >= 21:
            char = "Vulnerable Defense"
        else:
            char = "Solid Defense"

        stats_map[team] = {
            'team': team,
            'team_name': NBA_TEAM_NAMES.get(team, team),
            'def_overall_character': char,
            'def_rtg': round(def_rtg_val, 1),
            'rank_def_rtg': league_def_rank,
            'pace': round(pace_val, 1),
            'rank_pace': pace_rank,
            'pts_allowed': round(float(c_row.get('TEAM_PTS', 114.0)), 1),
            'rank_pts': int(c_row.get('PTS_RANK', 15)),
            'reb_allowed': round(float(c_row.get('TEAM_REB', 44.0)), 1),
            'rank_reb': int(c_row.get('REB_RANK', 15)),
            'ast_allowed': round(float(c_row.get('TEAM_AST', 26.0)), 1),
            'rank_ast': int(c_row.get('AST_RANK', 15)),
            'fg3m_allowed': round(float(c_row.get('FG3M', 12.5)), 1),
            'rank_fg3m': int(c_row.get('FG3M_RANK', 15)),
            'tov_forced': round(float(c_row.get('TOV', 14.0)), 1),
            'rank_tov': int(c_row.get('TOV_RANK', 15)),
            'vs_pg_pts': round(float(pg_row.get('PTS', 22.0)), 1),
            'rank_vs_pg': int(pg_row.get('PTS_RANK', 15)),
            'vs_sg_pts': round(float(sg_row.get('PTS', 20.0)), 1),
            'rank_vs_sg': int(sg_row.get('PTS_RANK', 15)),
            'vs_sf_pts': round(float(sf_row.get('PTS', 19.0)), 1),
            'rank_vs_sf': int(sf_row.get('PTS_RANK', 15)),
            'vs_pf_pts': round(float(pf_row.get('PTS', 21.0)), 1),
            'rank_vs_pf': int(pf_row.get('PTS_RANK', 15)),
            'vs_c_pts': round(float(c_row.get('PTS', 23.0)), 1),
            'rank_vs_c': int(c_row.get('PTS_RANK', 15)),
        }

    return stats_map

def build_matchups(team_stats_map, todays_games):
    """Builds matchup cards comparing Away vs Home teams."""
    pairs = todays_games if todays_games else DEFAULT_PAIRINGS
    matchups = []
    today_str = datetime.now().strftime("%Y-%m-%d")

    for idx, pair in enumerate(pairs):
        away, home = pair[0], pair[1]
        a_stats = team_stats_map.get(away, {
            'team': away, 'team_name': NBA_TEAM_NAMES.get(away, away),
            'def_overall_character': 'Active Defense', 'def_rtg': 112.0, 'rank_def_rtg': 15,
            'pace': 100.0, 'rank_pace': 15, 'pts_allowed': 114.0, 'rank_pts': 15,
            'reb_allowed': 44.0, 'rank_reb': 15, 'ast_allowed': 26.0, 'rank_ast': 15,
            'fg3m_allowed': 12.5, 'rank_fg3m': 15, 'tov_forced': 14.0, 'rank_tov': 15,
            'vs_pg_pts': 22.0, 'rank_vs_pg': 15, 'vs_sg_pts': 20.0, 'rank_vs_sg': 15,
            'vs_sf_pts': 19.0, 'rank_vs_sf': 15, 'vs_pf_pts': 21.0, 'rank_vs_pf': 15,
            'vs_c_pts': 23.0, 'rank_vs_c': 15
        })
        h_stats = team_stats_map.get(home, {
            'team': home, 'team_name': NBA_TEAM_NAMES.get(home, home),
            'def_overall_character': 'Active Defense', 'def_rtg': 112.0, 'rank_def_rtg': 15,
            'pace': 100.0, 'rank_pace': 15, 'pts_allowed': 114.0, 'rank_pts': 15,
            'reb_allowed': 44.0, 'rank_reb': 15, 'ast_allowed': 26.0, 'rank_ast': 15,
            'fg3m_allowed': 12.5, 'rank_fg3m': 15, 'tov_forced': 14.0, 'rank_tov': 15,
            'vs_pg_pts': 22.0, 'rank_vs_pg': 15, 'vs_sg_pts': 20.0, 'rank_vs_sg': 15,
            'vs_sf_pts': 19.0, 'rank_vs_sf': 15, 'vs_pf_pts': 21.0, 'rank_vs_pf': 15,
            'vs_c_pts': 23.0, 'rank_vs_c': 15
        })

        matchups.append({
            'game_id': f"NBA_{away}_{home}",
            'game_label': f"G{idx + 1}",
            'gameday': today_str,
            'gametime': '7:30 PM ET',
            'is_final': False,
            'away_team': away,
            'away_team_name': NBA_TEAM_NAMES.get(away, away),
            'away_record': '42-20' if away in ['BOS', 'OKC', 'DEN', 'MIN', 'CLE'] else '35-27',
            'home_team': home,
            'home_team_name': NBA_TEAM_NAMES.get(home, home),
            'home_record': '40-22' if home in ['BOS', 'OKC', 'DEN', 'MIN', 'CLE', 'MIA'] else '32-30',
            'away_stats': a_stats,
            'home_stats': h_stats
        })

    return matchups

def get_floor_streaks(pos_df: pd.DataFrame, minutes: int = 20, last_n_games: int = 5):
    """Calculates player production floor streaks."""
    local_logs = os.path.join(BASE_DIR, "logs.csv")
    streak_df = pd.DataFrame()

    if os.path.exists(local_logs):
        try:
            streak_df = find_streaks(pos_df, minutes, streak_len=last_n_games)
        except Exception as e:
            print(f"Local streak error: {e}")

    if streak_df.empty:
        try:
            logs_url = f"{GITHUB_RAW_BASE}logs.csv"
            logs_df = pd.read_csv(logs_url)
            streak_df = find_streaks(pos_df, minutes, streak_len=last_n_games, logs_df=logs_df)
        except Exception as e:
            print(f"Remote streak error: {e}")

    if streak_df is None or streak_df.empty:
        return [], {
            'longest': '--',
            'elite': '0',
            'solid': '0',
            'developing': '0'
        }

    records = []
    stat_labels = {'PTS': 'Points', 'REB': 'Rebounds', 'AST': 'Assists'}

    for idx, row in streak_df.iterrows():
        streak_len = int(last_n_games)
        raw_logs = str(row.get('Game_Logs', '')).split(',')
        cleaned_logs = [x.strip() for x in raw_logs if x.strip()]

        if streak_len >= 8:
            tier = "Elite (8+)"
        elif streak_len >= 5:
            tier = "Solid (5-7)"
        else:
            tier = "Developing (1-4)"

        records.append({
            'rank': idx + 1,
            'player_name': str(row.get('Player', 'Unknown')),
            'team': str(row.get('Team', '--')),
            'position': str(row.get('Pos', '--')),
            'stat': str(row.get('Stat', 'PTS')),
            'stat_label': stat_labels.get(str(row.get('Stat')), 'Production'),
            'baseline': float(row.get('Season_Avg', 0.0)),
            'streak_avg': float(row.get('Streak_Avg', 0.0)),
            'threshold': str(row.get('Threshold', '+1.0')),
            'streak': streak_len,
            'tier': tier,
            'game_logs': cleaned_logs
        })

    elite_count = sum(1 for r in records if r['streak'] >= 8)
    solid_count = sum(1 for r in records if 5 <= r['streak'] < 8)
    dev_count = sum(1 for r in records if r['streak'] < 5)
    longest_streak = max([r['streak'] for r in records], default=0)

    kpis = {
        'longest': f"{longest_streak} Games" if longest_streak > 0 else "--",
        'elite': str(elite_count),
        'solid': str(solid_count),
        'developing': str(dev_count)
    }

    return records, kpis

@app.route('/favicon.ico')
def favicon():
    return ('', 204)

@app.route('/', methods=['GET'])
@app.route('/app.py', methods=['GET'])
def index():
    df, pos_df, last_updated = load_data()
    todays_games = fetch_todays_games()
    team_stats_map = compute_team_stats_map(df)
    matchups = build_matchups(team_stats_map, todays_games)
    floor_streaks, streak_kpis = get_floor_streaks(pos_df, minutes=20, last_n_games=5)

    return render_template('index.html',
                           page_type='schedule',
                           matchups=matchups,
                           floor_streaks=floor_streaks,
                           streak_kpis=streak_kpis,
                           all_teams=NBA_TEAMS,
                           team_names=NBA_TEAM_NAMES,
                           team_stats=team_stats_map,
                           todays_games=todays_games,
                           last_updated=last_updated)

@app.route('/streak', methods=['GET'])
def streak():
    df, pos_df, last_updated = load_data()
    todays_games = fetch_todays_games()
    team_stats_map = compute_team_stats_map(df)
    matchups = build_matchups(team_stats_map, todays_games)
    floor_streaks, streak_kpis = get_floor_streaks(pos_df, minutes=20, last_n_games=5)

    return render_template('index.html',
                           page_type='floor-streak',
                           matchups=matchups,
                           floor_streaks=floor_streaks,
                           streak_kpis=streak_kpis,
                           all_teams=NBA_TEAMS,
                           team_names=NBA_TEAM_NAMES,
                           team_stats=team_stats_map,
                           todays_games=todays_games,
                           last_updated=last_updated)

@app.route('/api/full-schedule')
@app.route('/api/schedule')
def api_schedule():
    df, _, _ = load_data()
    todays_games = fetch_todays_games()
    team_stats_map = compute_team_stats_map(df)
    matchups = build_matchups(team_stats_map, todays_games)
    return jsonify({
        'total_games': len(matchups),
        'completed_games': 0,
        'games': matchups
    })

@app.route('/api/floor-streak')
@app.route('/api/streaks')
def api_floor_streak():
    min_val = request.args.get('min_games', 20, type=int)
    last_n = request.args.get('streak_len', 5, type=int)
    _, pos_df, _ = load_data()
    streaks, kpis = get_floor_streaks(pos_df, minutes=min_val, last_n_games=last_n)
    return jsonify({
        'players': streaks,
        'kpis': kpis
    })

@app.route('/api/team-stats')
def api_team_stats():
    df, _, last_updated = load_data()
    team_stats_map = compute_team_stats_map(df)
    return jsonify({
        'team_stats': team_stats_map,
        'last_updated': last_updated
    })

@app.route('/api/status')
def api_status():
    df, _, last_updated = load_data()
    return jsonify({
        'status': 'OPERATIONAL',
        'last_updated': last_updated,
        'teams_tracked': len(NBA_TEAMS),
        'is_vercel': IS_VERCEL
    })

if __name__ == '__main__':
    app.run(debug=True)
