from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd
import os
import json
from datetime import datetime
from urllib.parse import urlparse
import requests

from forensic_data import run_forensic_pipeline

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
    Restores PATH_INFO and QUERY_STRING so Flask routing matches correctly.
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

def load_forensic_data():
    """Loads 4-pillar team profiles, similarity models, and player usage distributions."""
    teams_path = os.path.join(BASE_DIR, 'forensic_teams.json')
    sim_path = os.path.join(BASE_DIR, 'forensic_similar_opponents.json')
    players_path = os.path.join(BASE_DIR, 'forensic_players.json')
    
    if os.path.exists(teams_path) and os.path.exists(sim_path) and os.path.exists(players_path):
        try:
            with open(teams_path, 'r', encoding='utf-8') as f:
                forensic_teams = json.load(f)
            with open(sim_path, 'r', encoding='utf-8') as f:
                similarity_lookups = json.load(f)
            with open(players_path, 'r', encoding='utf-8') as f:
                player_profiles = json.load(f)
            last_updated = datetime.fromtimestamp(os.path.getmtime(teams_path)).strftime('%Y-%m-%d %I:%M %p')
            return forensic_teams, similarity_lookups, player_profiles, last_updated
        except Exception as e:
            print(f"Error reading local forensic data: {e}")

    if IS_VERCEL:
        try:
            r1 = requests.get(f"{GITHUB_RAW_BASE}forensic_teams.json", timeout=6)
            r2 = requests.get(f"{GITHUB_RAW_BASE}forensic_similar_opponents.json", timeout=6)
            r3 = requests.get(f"{GITHUB_RAW_BASE}forensic_players.json", timeout=6)
            if r1.status_code == 200 and r2.status_code == 200 and r3.status_code == 200:
                return r1.json(), r2.json(), r3.json(), "Remote Sync"
        except Exception as e:
            print(f"Error fetching remote forensic data: {e}")

    try:
        run_forensic_pipeline()
        with open(teams_path, 'r', encoding='utf-8') as f:
            forensic_teams = json.load(f)
        with open(sim_path, 'r', encoding='utf-8') as f:
            similarity_lookups = json.load(f)
        with open(players_path, 'r', encoding='utf-8') as f:
            player_profiles = json.load(f)
        return forensic_teams, similarity_lookups, player_profiles, "Local Engine"
    except Exception as e:
        print(f"Pipeline generation error: {e}")
        return {}, {}, [], "Sync Error"

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

    return []

def load_all_schedule_games(forensic_teams, similarity_lookups, player_profiles):
    """Loads all schedule games and enriches with 4-pillar forensic metrics & similar opponents."""
    json_path = os.path.join(BASE_DIR, 'schedule_games.json')
    raw_games = []

    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                raw_games = json.load(f)
        except Exception as e:
            print(f"Error loading schedule_games.json: {e}")

    if not raw_games and IS_VERCEL:
        try:
            url = f"{GITHUB_RAW_BASE}schedule_games.json"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                raw_games = r.json()
        except Exception as e:
            print(f"Error fetching remote schedule_games.json: {e}")

    if not raw_games:
        today_str = datetime.now().strftime("%Y-%m-%d")
        for idx, (a, h) in enumerate(DEFAULT_PAIRINGS):
            raw_games.append({
                'game_id': f"NBA_{a}_{h}",
                'gameday': today_str,
                'away_team': a,
                'home_team': h,
                'away_score': None,
                'home_score': None,
                'is_final': False
            })

    # Group players by team for O(1) lookup
    team_players = {}
    for p in player_profiles:
        t = p['team']
        if t not in team_players:
            team_players[t] = []
        team_players[t].append(p)

    enriched = []
    for g in raw_games:
        away = g['away_team']
        home = g['home_team']
        a_prof = forensic_teams.get(away, {})
        h_prof = forensic_teams.get(home, {})
        is_fin = bool(g.get('is_final', False))

        # Similar opponent lookup
        home_def_sim = similarity_lookups.get(home, {}).get(away, {})
        away_def_sim = similarity_lookups.get(away, {}).get(home, {})

        enriched.append({
            'game_id': g['game_id'],
            'gameday': g['gameday'],
            'gametime': 'FINAL' if is_fin else '7:30 PM ET',
            'away_team': away,
            'away_team_name': NBA_TEAM_NAMES.get(away, away),
            'away_record': '42-20' if away in ['BOS', 'OKC', 'DEN', 'MIN', 'CLE'] else '35-27',
            'home_team': home,
            'home_team_name': NBA_TEAM_NAMES.get(home, home),
            'home_record': '40-22' if home in ['BOS', 'OKC', 'DEN', 'MIN', 'CLE', 'MIA'] else '32-30',
            'away_score': g.get('away_score'),
            'home_score': g.get('home_score'),
            'is_final': is_fin,
            'away_forensic': a_prof,
            'home_forensic': h_prof,
            'home_def_sim': home_def_sim,
            'away_def_sim': away_def_sim,
            'away_players': team_players.get(away, []),
            'home_players': team_players.get(home, [])
        })

    return enriched

@app.route('/favicon.ico')
def favicon():
    return ('', 204)

@app.route('/', methods=['GET'])
@app.route('/app.py', methods=['GET'])
def index():
    forensic_teams, similarity_lookups, player_profiles, last_updated = load_forensic_data()
    todays_games = fetch_todays_games()
    all_games = load_all_schedule_games(forensic_teams, similarity_lookups, player_profiles)

    all_game_dates = sorted(set(g['gameday'] for g in all_games if g.get('gameday')), reverse=True)
    dates_2026 = [d for d in all_game_dates if d >= '2026-10-19']

    today_str = datetime.now().strftime("%Y-%m-%d")
    default_start_date = '2026-10-19'
    default_end_date = '2027-04-14'

    return render_template('index.html',
                           page_type='schedule',
                           all_games=all_games,
                           dates_2026=dates_2026,
                           start_date=default_start_date,
                           end_date=default_end_date,
                           forensic_teams=forensic_teams,
                           player_profiles=player_profiles,
                           all_teams=NBA_TEAMS,
                           team_names=NBA_TEAM_NAMES,
                           todays_games=todays_games,
                           last_updated=last_updated)

@app.route('/profiles', methods=['GET'])
@app.route('/streak', methods=['GET'])
def profiles():
    forensic_teams, similarity_lookups, player_profiles, last_updated = load_forensic_data()
    todays_games = fetch_todays_games()
    all_games = load_all_schedule_games(forensic_teams, similarity_lookups, player_profiles)

    all_game_dates = sorted(set(g['gameday'] for g in all_games if g.get('gameday')), reverse=True)
    dates_2026 = [d for d in all_game_dates if d >= '2026-10-19']

    today_str = datetime.now().strftime("%Y-%m-%d")
    default_start_date = '2026-10-19'
    default_end_date = '2027-04-14'

    return render_template('index.html',
                           page_type='profiles',
                           all_games=all_games,
                           dates_2026=dates_2026,
                           start_date=default_start_date,
                           end_date=default_end_date,
                           forensic_teams=forensic_teams,
                           player_profiles=player_profiles,
                           all_teams=NBA_TEAMS,
                           team_names=NBA_TEAM_NAMES,
                           todays_games=todays_games,
                           last_updated=last_updated)

@app.route('/api/full-schedule')
@app.route('/api/schedule')
def api_schedule():
    forensic_teams, similarity_lookups, player_profiles, _ = load_forensic_data()
    all_games = load_all_schedule_games(forensic_teams, similarity_lookups, player_profiles)

    season = request.args.get('season', '2026')
    s_date = request.args.get('start_date', '2026-10-19')
    e_date = request.args.get('end_date', '2027-04-14')

    filtered = [g for g in all_games if s_date <= g['gameday'] <= e_date]
    if season == '2026':
        filtered = [g for g in filtered if g['gameday'] >= '2026-10-19']
    elif season == '2025':
        filtered = [g for g in filtered if g['gameday'] < '2026-10-19']

    unique_dates = sorted(set(g['gameday'] for g in filtered), reverse=True)
    completed = [g for g in filtered if g.get('is_final')]

    return jsonify({
        'season': season,
        'start_date': s_date,
        'end_date': e_date,
        'total_games': len(filtered),
        'completed_games': len(completed),
        'dates': unique_dates,
        'games': filtered
    })

@app.route('/api/player-profiles')
@app.route('/api/floor-streak')
@app.route('/api/streaks')
def api_player_profiles():
    _, _, player_profiles, _ = load_forensic_data()
    team_filter = request.args.get('team', '').upper()
    pos_filter = request.args.get('pos', '').upper()
    
    filtered = player_profiles
    if team_filter and team_filter != 'ALL':
        filtered = [p for p in filtered if p['team'] == team_filter]
    if pos_filter and pos_filter != 'ALL':
        filtered = [p for p in filtered if p['position'] == pos_filter]
        
    return jsonify({
        'total_players': len(filtered),
        'players': filtered
    })

@app.route('/api/forensic-teams')
@app.route('/api/team-stats')
def api_forensic_teams():
    forensic_teams, _, _, last_updated = load_forensic_data()
    return jsonify({
        'forensic_teams': forensic_teams,
        'last_updated': last_updated
    })

@app.route('/api/similar-opponents')
def api_similar_opponents():
    _, similarity_lookups, _, _ = load_forensic_data()
    def_team = request.args.get('defense', 'BOS').upper()
    atk_team = request.args.get('offense', 'NYK').upper()
    
    sim_data = similarity_lookups.get(def_team, {}).get(atk_team, {
        'summary_text': f"No similarity data recorded for {def_team} vs {atk_team}."
    })
    return jsonify(sim_data)

@app.route('/api/matchup/<away>/<home>')
def api_matchup(away, home):
    away = away.upper()
    home = home.upper()
    forensic_teams, similarity_lookups, player_profiles, _ = load_forensic_data()
    
    a_prof = forensic_teams.get(away, {})
    h_prof = forensic_teams.get(home, {})
    home_sim = similarity_lookups.get(home, {}).get(away, {})
    away_sim = similarity_lookups.get(away, {}).get(home, {})
    
    away_p = [p for p in player_profiles if p['team'] == away]
    home_p = [p for p in player_profiles if p['team'] == home]
    
    return jsonify({
        'away_team': away,
        'home_team': home,
        'away_forensic': a_prof,
        'home_forensic': h_prof,
        'home_def_vs_away_sim': home_sim,
        'away_def_vs_home_sim': away_sim,
        'away_top_rotation': away_p,
        'home_top_rotation': home_p
    })

@app.route('/api/status')
def api_status():
    _, _, _, last_updated = load_forensic_data()
    return jsonify({
        'status': 'OPERATIONAL',
        'engine': 'FORENSIC_SCOUTING_PIPELINE',
        'last_updated': last_updated,
        'teams_tracked': len(NBA_TEAMS),
        'is_vercel': IS_VERCEL
    })

if __name__ == '__main__':
    app.run(debug=True)
