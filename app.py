"""
NBA Forensic Scouting Engine - Core Backend
===========================================
Strictly descriptive, play-by-play, tracking-backed matchup analytics engine.
Zero judgment language, multi-season baseline, Euclidean similar-opponent modeling.
"""

import os
import json
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode
import numpy as np
import requests
from flask import Flask, render_template, request, jsonify

from forensic_data import run_forensic_pipeline, NBA_TEAMS, NBA_TEAM_NAMES, make_empty_team_profile, load_injuries_by_team

# --- Configuration ---
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/jibinator1/nba_stats/main/"
IS_VERCEL = os.environ.get('VERCEL') == '1'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)

DEFAULT_PAIRINGS = [
    ('BOS', 'MIA'), ('LAL', 'GSW'), ('NYK', 'PHI'), ('MIL', 'IND'),
    ('OKC', 'DEN'), ('MIN', 'DAL'), ('CLE', 'DET'), ('PHX', 'SAC'),
    ('ORL', 'ATL'), ('HOU', 'NOP'), ('LAC', 'MEM'), ('TOR', 'CHI'),
    ('SAS', 'POR'), ('CHA', 'BKN'), ('UTA', 'WAS')
]

class VercelPathMiddleware:
    """Normalizes Vercel serverless request paths so Flask matches correctly."""
    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app

    def __call__(self, environ, start_response):
        qs = environ.get('QUERY_STRING', '')
        if '__path__' in qs:
            params = parse_qs(qs, keep_blank_values=True)
            if '__path__' in params and params['__path__']:
                req_path = params['__path__'][0]
                if not req_path.startswith('/'):
                    req_path = '/' + req_path
                environ['PATH_INFO'] = req_path
                del params['__path__']
                environ['QUERY_STRING'] = urlencode(params, doseq=True)
        else:
            current_path = environ.get('PATH_INFO', '')
            matched_path = environ.get('HTTP_X_MATCHED_PATH') or environ.get('HTTP_X_VERCEL_MATCHED_PATH')
            raw_path = environ.get('HTTP_X_FORWARDED_URI') or environ.get('REQUEST_URI') or environ.get('RAW_URI')

            if current_path in ('/app.py', 'app.py', '/api/index.py', 'api/index.py', '/api/index', ''):
                candidate = matched_path if (matched_path and not matched_path.endswith('app.py') and not matched_path.endswith('index.py') and not matched_path.endswith('index')) else raw_path
                if candidate and not candidate.endswith('app.py') and not candidate.endswith('index.py') and not candidate.endswith('index'):
                    parsed = urlparse(candidate)
                    environ['PATH_INFO'] = parsed.path or '/'
                    if parsed.query and not environ.get('QUERY_STRING'):
                        environ['QUERY_STRING'] = parsed.query
                else:
                    environ['PATH_INFO'] = '/'

        return self.wsgi_app(environ, start_response)

app.wsgi_app = VercelPathMiddleware(app.wsgi_app)
handler = app

# =========================================================================
# DATA LOADERS & ENGINES
# =========================================================================

def load_forensic_data():
    """Loads team profiles, similarity models, player distributions, and game logs."""
    teams_path = os.path.join(BASE_DIR, 'forensic_teams.json')
    sim_path = os.path.join(BASE_DIR, 'forensic_similar_opponents.json')
    players_path = os.path.join(BASE_DIR, 'forensic_players.json')
    logs_path = os.path.join(BASE_DIR, 'forensic_team_game_logs.json')

    if os.path.exists(teams_path) and os.path.exists(sim_path) and os.path.exists(players_path):
        try:
            with open(teams_path, 'r', encoding='utf-8') as f:
                forensic_teams = json.load(f)
            with open(sim_path, 'r', encoding='utf-8') as f:
                similarity_lookups = json.load(f)
            with open(players_path, 'r', encoding='utf-8') as f:
                player_profiles = json.load(f)
            team_game_logs = []
            if os.path.exists(logs_path):
                with open(logs_path, 'r', encoding='utf-8') as f:
                    team_game_logs = json.load(f)
            last_updated = datetime.fromtimestamp(os.path.getmtime(teams_path)).strftime('%Y-%m-%d %I:%M %p')
            return forensic_teams, similarity_lookups, player_profiles, team_game_logs, last_updated
        except Exception as e:
            print(f"Error reading local forensic data: {e}")

    if IS_VERCEL:
        try:
            r1 = requests.get(f"{GITHUB_RAW_BASE}forensic_teams.json", timeout=6)
            r2 = requests.get(f"{GITHUB_RAW_BASE}forensic_similar_opponents.json", timeout=6)
            r3 = requests.get(f"{GITHUB_RAW_BASE}forensic_players.json", timeout=6)
            r4 = requests.get(f"{GITHUB_RAW_BASE}forensic_team_game_logs.json", timeout=6)
            if r1.status_code == 200 and r2.status_code == 200 and r3.status_code == 200:
                t_logs = r4.json() if r4.status_code == 200 else []
                return r1.json(), r2.json(), r3.json(), t_logs, "Remote Sync"
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
        team_game_logs = []
        if os.path.exists(logs_path):
            with open(logs_path, 'r', encoding='utf-8') as f:
                team_game_logs = json.load(f)
        return forensic_teams, similarity_lookups, player_profiles, team_game_logs, "Local Engine"
    except Exception as e:
        print(f"Pipeline generation error: {e}")
        return {}, {}, [], [], "Sync Error"


def load_all_schedule_games():
    """Loads all schedule games with normalized metadata."""
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
        for a, h in DEFAULT_PAIRINGS:
            raw_games.append({
                'game_id': f"NBA_{a}_{h}",
                'gameday': '2026-10-20',
                'away_team': a,
                'home_team': h,
                'away_score': None,
                'home_score': None,
                'is_final': False,
                'gametime': '7:30 PM ET'
            })

    # Exclusively filter and sort for 2026-27 season games
    raw_games = [g for g in raw_games if g.get('gameday', '') >= '2026-10-20']
    raw_games.sort(key=lambda x: (x.get('gameday', ''), x.get('game_id', '')))

    enriched = []
    for g in raw_games:
        away = g['away_team']
        home = g['home_team']
        is_fin = bool(g.get('is_final', False))

        enriched.append({
            'game_id': g['game_id'],
            'gameday': g['gameday'],
            'gametime': g.get('gametime') or ('FINAL' if is_fin else '7:30 PM ET'),
            'away_team': away,
            'away_team_name': NBA_TEAM_NAMES.get(away, away),
            'away_record': g.get('away_record'),
            'home_team': home,
            'home_team_name': NBA_TEAM_NAMES.get(home, home),
            'home_record': g.get('home_record'),
            'away_score': g.get('away_score'),
            'home_score': g.get('home_score'),
            'is_final': is_fin
        })

    return enriched


def calculate_dynamic_teams_for_range(team_game_logs, start_date, end_date):
    """Calculates date-range filtered team statistics and rankings. Returns None for teams with 0 games."""
    if not team_game_logs:
        return {}

    filtered = [g for g in team_game_logs if start_date <= g['d'] <= end_date]
    by_team = {}
    for g in filtered:
        by_team.setdefault(g['t'], []).append(g)

    teams_dict = {}
    injuries_by_team = load_injuries_by_team()
    base_forensic_teams, _, _, _, _ = load_forensic_data()
    for t in NBA_TEAMS:
        t_games = by_team.get(t, [])
        gp = len(t_games)
        if gp == 0:
            teams_dict[t] = make_empty_team_profile(t)
        else:
            tot_poss = sum(g['poss'] for g in t_games)
            pace = round((tot_poss / (gp * 48)) * 48, 1) if gp > 0 else 100.0
            tot_pts = sum(g['pts'] for g in t_games)
            tot_pts_opp = sum(g['opp_pts'] for g in t_games)
            tot_fga = sum(g['fga'] for g in t_games)
            tot_fgm = sum(g['fgm'] for g in t_games)
            tot_fga_opp = sum(g['opp_fga'] for g in t_games)
            tot_fgm_opp = sum(g['opp_fgm'] for g in t_games)

            tot_reb = sum(g['reb'] for g in t_games)
            tot_reb_opp = sum(g['opp_reb'] for g in t_games)
            tot_oreb = sum(g.get('oreb', 0) for g in t_games)
            tot_dreb = sum(g.get('dreb', 0) for g in t_games)
            tot_opp_oreb = sum(g.get('opp_oreb', 0) for g in t_games)
            tot_opp_dreb = sum(g.get('opp_dreb', 0) for g in t_games)
            tot_tov = sum(g.get('tov', 0) for g in t_games)
            tot_opp_tov = sum(g.get('opp_tov', 0) for g in t_games)

            base_p = base_forensic_teams.get(t, make_empty_team_profile(t))
            entry = dict(base_p)
            entry.update({
                'team': t,
                'team_name': NBA_TEAM_NAMES.get(t, t),
                'gp': gp,
                'is_small_sample': (gp < 10) or (tot_poss < 200),
                'injuries': injuries_by_team.get(t, []),
                'pts_scored': round(tot_pts / gp, 1),
                'pts_allowed': round(tot_pts_opp / gp, 1),
                'reb_total': round(tot_reb / gp, 1),
                'reb_allowed': round(tot_reb_opp / gp, 1),
                'oreb_pct': round((tot_oreb / (tot_oreb + tot_opp_dreb) * 100), 1) if (tot_oreb + tot_opp_dreb) > 0 else None,
                'dreb_pct': round((tot_dreb / (tot_dreb + tot_opp_oreb) * 100), 1) if (tot_dreb + tot_opp_oreb) > 0 else None,
                'ast_total': round(sum(g['ast'] for g in t_games) / gp, 1),
                'ast_allowed': round(sum(g['opp_ast'] for g in t_games) / gp, 1),
                'tov_total': round(tot_tov / gp, 1),
                'tov_allowed': round(tot_opp_tov / gp, 1),
                'fg3m_total': round(sum(g['fg3m'] for g in t_games) / gp, 1),
                'fg3m_allowed': round(sum(g['opp_fg3m'] for g in t_games) / gp, 1),
                'fg3a_total': round(sum(g['fg3a'] for g in t_games) / gp, 1),
                'fg3a_allowed': round(sum(g['opp_fg3a'] for g in t_games) / gp, 1),
                'fg_pct': round((tot_fgm / tot_fga * 100), 1) if tot_fga > 0 else None,
                'fg_pct_opp': round((tot_fgm_opp / tot_fga_opp * 100), 1) if tot_fga_opp > 0 else None,
                'pace': pace,
                'off_rtg': round((tot_pts / tot_poss * 100), 1) if tot_poss > 0 else None,
                'def_rtg': round((tot_pts_opp / tot_poss * 100), 1) if tot_poss > 0 else None
            })
            teams_dict[t] = entry

    # Rank metrics across teams with games
    metrics_to_rank = [
        (('pace',), False, 'rank_pace'), (('neutral_pace',), False, 'rank_neutral_pace'),
        (('transition_freq',), False, 'rank_transition_freq'), (('def_rtg',), True, 'rank_def_rtg'),
        (('off_rtg',), False, 'rank_off_rtg'), (('pts_scored',), False, 'rank_pts_scored'),
        (('pts_allowed',), True, 'rank_pts_allowed'), (('reb_total',), False, 'rank_reb_total'),
        (('reb_allowed',), True, 'rank_reb_allowed'), (('oreb_pct',), False, 'rank_oreb_pct'),
        (('dreb_pct',), False, 'rank_dreb_pct'), (('ast_total',), False, 'rank_ast_total'),
        (('ast_allowed',), True, 'rank_ast_allowed'), (('tov_total',), True, 'rank_tov_total'),
        (('tov_allowed',), False, 'rank_tov_allowed'), (('fg3m_total',), False, 'rank_fg3m_total'),
        (('fg3m_allowed',), True, 'rank_fg3m_allowed'), (('fg3a_total',), False, 'rank_fg3a_total'),
        (('fg3a_allowed',), True, 'rank_fg3a_allowed'), (('fg_pct',), False, 'rank_fg_pct'),
        (('fg_pct_opp',), True, 'rank_fg_pct_opp'),
        (('rebounding', 'dreb_secured_pct'), False, 'rank_dreb_secured_pct'),
        (('rebounding', 'oreb_allowed_pct'), True, 'rank_oreb_allowed_pct'),
        (('shot_clock', 'early_0_8'), False, 'rank_early_0_8'),
        (('shot_clock', 'mid_8_15'), False, 'rank_mid_8_15'),
        (('shot_clock', 'late_15_24'), True, 'rank_late_15_24'),
        (('game_script', 'leading_10', 'pace'), False, 'rank_pace_leading_10'),
        (('game_script', 'trailing_10', 'pace'), False, 'rank_pace_trailing_10'),
        (('pnr_coverage', 'drop'), False, 'rank_pnr_drop'),
        (('pnr_coverage', 'switch'), False, 'rank_pnr_switch'),
        (('shot_profile_allowed', 'restricted_area', 'ppp'), True, 'rank_ra_ppp_allowed'),
        (('shot_profile_allowed', 'paint_non_ra', 'ppp'), True, 'rank_paint_ppp_allowed'),
        (('shot_profile_allowed', 'mid_range', 'ppp'), True, 'rank_mid_ppp_allowed'),
        (('shot_profile_allowed', 'corner_3', 'ppp'), True, 'rank_corner3_ppp_allowed'),
        (('shot_profile_allowed', 'above_break_3', 'ppp'), True, 'rank_above_break3_ppp_allowed'),
        (('transition_ppp_allowed',), True, 'rank_transition_ppp_allowed'),
        (('playtypes_offense', 'PnR_Ball_Handler', 'freq'), False, 'rank_pnr_bh_freq'),
        (('playtypes_offense', 'Isolation', 'freq'), False, 'rank_iso_freq'),
        (('playtypes_offense', 'Spot_Up', 'freq'), False, 'rank_spot_freq'),
        (('playtypes_defense', 'PnR_Ball_Handler', 'allowed_ppp'), True, 'rank_pnr_bh_allowed_ppp'),
        (('playtypes_defense', 'Spot_Up', 'allowed_ppp'), True, 'rank_spot_up_allowed_ppp'),
        (('playtypes_defense', 'Isolation', 'allowed_ppp'), True, 'rank_iso_allowed_ppp'),
        (('playtypes_defense', 'PnR_Roll_Man', 'allowed_ppp'), True, 'rank_pnr_rm_allowed_ppp'),
        (('shot_profile_offense', 'restricted_area', 'freq'), False, 'rank_ra_off_freq'),
        (('shot_profile_offense', 'paint_non_ra', 'freq'), False, 'rank_paint_off_freq'),
        (('shot_profile_offense', 'mid_range', 'freq'), False, 'rank_mid_off_freq'),
        (('shot_profile_offense', 'corner_3', 'freq'), False, 'rank_corner3_off_freq'),
        (('shot_profile_offense', 'above_break_3', 'freq'), False, 'rank_above_break3_off_freq'),
        (('turnover_profile', 'live_ball_per_game'), True, 'rank_live_tov'),
        (('turnover_profile', 'dead_ball_per_game'), True, 'rank_dead_tov'),
        (('turnover_profile', 'points_generated_off_tov'), False, 'rank_pts_generated_off_tov'),
        (('turnover_profile', 'points_surrendered_off_tov'), True, 'rank_pts_surrendered_off_tov')
    ]
    for path, ascending, r_key in metrics_to_rank:
        tuples = []
        for t, prof in teams_dict.items():
            if prof.get('gp', 0) == 0:
                continue
            curr = prof
            for k in path:
                if isinstance(curr, dict) and k in curr:
                    curr = curr[k]
                else:
                    curr = None
                    break
            if curr is not None:
                tuples.append((t, float(curr)))
        tuples.sort(key=lambda x: x[1], reverse=not ascending)
        ranked = set()
        r = 1
        for idx, (t, v) in enumerate(tuples):
            ranked.add(t)
            if idx > 0 and v != tuples[idx - 1][1]:
                r = idx + 1
            teams_dict[t][r_key] = 'Small Sample' if teams_dict[t].get('is_small_sample') else r
        for t in NBA_TEAMS:
            if t not in ranked:
                teams_dict[t][r_key] = None

    return teams_dict


# =========================================================================
# APPLICATION ROUTES (DESCRIPTIVE FORENSIC ENGINE)
# =========================================================================

def render_main_dashboard(page_type='schedule', initial_away=None, initial_home=None):
    """Unified rendering controller for main dashboard, profiles tab, and matchup views."""
    forensic_teams, _, player_profiles, team_game_logs, last_updated = load_forensic_data()
    all_games = load_all_schedule_games()
    today_str = datetime.now().strftime("%Y-%m-%d")

    return render_template('index.html',
                           page_type=page_type,
                           all_games=all_games,
                           start_date='2025-10-02',
                           end_date=today_str,
                           forensic_teams=forensic_teams,
                           player_profiles=player_profiles,
                           team_game_logs=team_game_logs,
                           all_teams=NBA_TEAMS,
                           team_names=NBA_TEAM_NAMES,
                           last_updated=last_updated,
                           initial_away=initial_away,
                           initial_home=initial_home)

@app.route('/favicon.ico')
def favicon():
    return ('', 204)

@app.route('/', methods=['GET'])
@app.route('/app.py', methods=['GET'])
def index():
    return render_main_dashboard(page_type='schedule')

@app.route('/profiles', methods=['GET'])
def profiles():
    return render_main_dashboard(page_type='profiles')

@app.route('/insights', methods=['GET'])
def insights_view():
    return render_main_dashboard(page_type='insights')

@app.route('/matchup/<away>/<home>', methods=['GET'])
def matchup_view(away, home):
    return render_main_dashboard(page_type='insights', initial_away=away.upper(), initial_home=home.upper())

@app.route('/api/schedule')
def api_schedule():
    all_games = load_all_schedule_games()
    s_date = request.args.get('start_date', '2026-10-20')
    e_date = request.args.get('end_date', '2027-04-14')

    filtered = [g for g in all_games if s_date <= g['gameday'] <= e_date]
    unique_dates = sorted(set(g['gameday'] for g in filtered))
    completed = [g for g in filtered if g.get('is_final')]

    return jsonify({
        'season': '2026-27',
        'start_date': s_date,
        'end_date': e_date,
        'total_games': len(filtered),
        'completed_games': len(completed),
        'dates': unique_dates,
        'games': filtered
    })

@app.route('/api/player-profiles')
def api_player_profiles():
    _, _, player_profiles, _, _ = load_forensic_data()
    team_filter = request.args.get('team', '').upper()
    pos_filter = request.args.get('pos', '').upper()
    s_date = request.args.get('start_date')
    e_date = request.args.get('end_date')

    filtered = player_profiles
    if team_filter and team_filter != 'ALL':
        filtered = [p for p in filtered if p['team'] == team_filter]
    if pos_filter and pos_filter != 'ALL':
        filtered = [p for p in filtered if p['position'] == pos_filter]

    if s_date and e_date:
        out_players = []
        for p in filtered:
            p_copy = dict(p)
            all_logs = p.get('all_logs', [])
            matching = [lg for lg in all_logs if s_date <= lg['d'] <= e_date]
            if not matching:
                p_copy['games_played'] = 0
                p_copy['is_small_sample'] = True
                p_copy['season_avg'] = {'pts': None, 'reb': None, 'ast': None, 'min': None, 'fg3a': None, 'fg3m': None, 'paint_pts': None}
                p_copy['distribution_10g'] = {
                    'pts': {'floor': None, 'median': None, 'ceiling': None, 'logs': []},
                    'reb': {'floor': None, 'median': None, 'ceiling': None, 'logs': []},
                    'ast': {'floor': None, 'median': None, 'ceiling': None, 'logs': []}
                }
            else:
                p_copy['games_played'] = len(matching)
                p_copy['is_small_sample'] = len(matching) < 10
                pts_list = [lg['pts'] for lg in matching]
                reb_list = [lg['reb'] for lg in matching]
                ast_list = [lg['ast'] for lg in matching]
                min_list = [lg['min'] for lg in matching]
                p_copy['season_avg'] = {
                    'pts': round(sum(pts_list)/len(pts_list), 1),
                    'reb': round(sum(reb_list)/len(reb_list), 1),
                    'ast': round(sum(ast_list)/len(ast_list), 1),
                    'min': round(sum(min_list)/len(min_list), 1),
                    'fg3a': round(sum(lg['fg3a'] for lg in matching)/len(matching), 1),
                    'fg3m': round(sum(lg['fg3m'] for lg in matching)/len(matching), 1),
                    'paint_pts': round(sum(lg.get('paint_pts', 0) for lg in matching)/len(matching), 1)
                }
                last10 = matching[:10]
                p_copy['distribution_10g'] = {
                    'pts': {'floor': round(float(np.percentile([x['pts'] for x in last10], 25)), 1),
                            'median': round(float(np.percentile([x['pts'] for x in last10], 50)), 1),
                            'ceiling': round(float(np.percentile([x['pts'] for x in last10], 75)), 1),
                            'logs': [x['pts'] for x in last10]},
                    'reb': {'floor': round(float(np.percentile([x['reb'] for x in last10], 25)), 1),
                            'median': round(float(np.percentile([x['reb'] for x in last10], 50)), 1),
                            'ceiling': round(float(np.percentile([x['reb'] for x in last10], 75)), 1),
                            'logs': [x['reb'] for x in last10]},
                    'ast': {'floor': round(float(np.percentile([x['ast'] for x in last10], 25)), 1),
                            'median': round(float(np.percentile([x['ast'] for x in last10], 50)), 1),
                            'ceiling': round(float(np.percentile([x['ast'] for x in last10], 75)), 1),
                            'logs': [x['ast'] for x in last10]}
                }
            out_players.append(p_copy)

        # Compute dynamic player stat ranks across out_players
        for stat, r_key in [('pts', 'pts_rank'), ('reb', 'reb_rank'), ('ast', 'ast_rank')]:
            tuples = []
            for idx, p in enumerate(out_players):
                v = p.get('season_avg', {}).get(stat)
                if v is not None:
                    tuples.append((idx, float(v)))
            tuples.sort(key=lambda x: x[1], reverse=True)
            rank = 1
            for r_idx, (p_idx, v) in enumerate(tuples):
                if r_idx > 0 and v != tuples[r_idx - 1][1]:
                    rank = r_idx + 1
                p = out_players[p_idx]
                if p.get('is_small_sample'):
                    p['season_avg'][r_key] = 'Small Sample'
                else:
                    p['season_avg'][r_key] = rank

        return jsonify({
            'total_players': len(out_players),
            'start_date': s_date,
            'end_date': e_date,
            'players': out_players
        })

    return jsonify({
        'total_players': len(filtered),
        'players': filtered
    })

@app.route('/api/floor-streak')
def api_floor_streak():
    """
    Returns the Player Floor Breach Streak for all tracked rotation players against a hard floor threshold line.
    Streak = consecutive most-recent games meeting or exceeding the hard floor threshold line ((1 - margin) * baseline median).
    """
    stat_param = request.args.get('stat', 'PTS').upper()
    if stat_param not in ['PTS', 'REB', 'AST']:
        stat_param = 'PTS'
    stat_key = stat_param.lower()

    try:
        margin = float(request.args.get('margin', 0.30))
    except (ValueError, TypeError):
        margin = 0.30

    try:
        min_games = int(request.args.get('min_games', 4))
    except (ValueError, TypeError):
        min_games = 4

    try:
        min_streak = int(request.args.get('min_streak', 0))
    except (ValueError, TypeError):
        min_streak = 0

    pos_filter = request.args.get('pos', 'ALL').upper()
    team_filter = request.args.get('team', 'ALL').upper()
    s_date = request.args.get('start_date')
    e_date = request.args.get('end_date')

    _, _, player_profiles, _, _ = load_forensic_data()

    results = []
    for p in player_profiles:
        if pos_filter != 'ALL' and p.get('position', '').upper() != pos_filter:
            continue
        if team_filter != 'ALL' and p.get('team', '').upper() != team_filter:
            continue

        logs = p.get('all_logs', [])
        if s_date and e_date:
            logs = [lg for lg in logs if s_date <= lg.get('d', '') <= e_date]
        elif s_date:
            logs = [lg for lg in logs if lg.get('d', '') >= s_date]
        elif e_date:
            logs = [lg for lg in logs if lg.get('d', '') <= e_date]

        if len(logs) < min_games:
            continue

        vals = [float(lg.get(stat_key, 0)) for lg in logs]
        if not vals:
            continue

        s_vals = sorted(vals)
        n = len(s_vals)
        med = float(s_vals[n // 2] if n % 2 == 1 else (s_vals[n // 2 - 1] + s_vals[n // 2]) / 2.0)
        threshold = round((1.0 - margin) * med, 1)

        streak = 0
        for v in vals:
            if v >= threshold:
                streak += 1
            else:
                break

        if streak < min_streak:
            continue

        tier = 'Breached (0)'
        if streak >= 8:
            tier = 'Consistent (8+)'
        elif streak >= 5:
            tier = 'Solid Floor (5-7)'
        elif streak >= 1:
            tier = 'Developing (1-4)'

        results.append({
            'player_name': p.get('player_name'),
            'team': p.get('team'),
            'team_name': p.get('team_name'),
            'position': p.get('position'),
            'stat': stat_param,
            'baseline_median': med,
            'margin_used': margin,
            'floor_threshold': threshold,
            'floor_streak': streak,
            'reliability_tier': tier,
            'games_played': len(logs),
            'recent_logs': vals[:10]
        })

    results.sort(key=lambda x: x['floor_streak'], reverse=True)
    return jsonify({
        'status': 'success',
        'count': len(results),
        'stat': stat_param,
        'margin': margin,
        'players': results
    })

@app.route('/api/forensic-teams')
def api_forensic_teams():
    forensic_teams, _, _, team_game_logs, last_updated = load_forensic_data()
    s_date = request.args.get('start_date')
    e_date = request.args.get('end_date')
    if s_date and e_date:
        dynamic_teams = calculate_dynamic_teams_for_range(team_game_logs, s_date, e_date)
        return jsonify({
            'forensic_teams': dynamic_teams,
            'start_date': s_date,
            'end_date': e_date,
            'last_updated': last_updated
        })
    return jsonify({
        'forensic_teams': forensic_teams,
        'last_updated': last_updated
    })

@app.route('/api/similar-opponents')
def api_similar_opponents():
    _, similarity_lookups, _, _, _ = load_forensic_data()
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
    forensic_teams, similarity_lookups, player_profiles, _, _ = load_forensic_data()

    return jsonify({
        'away_team': away,
        'home_team': home,
        'away_forensic': forensic_teams.get(away, {}),
        'home_forensic': forensic_teams.get(home, {}),
        'home_def_vs_away_sim': similarity_lookups.get(home, {}).get(away, {}),
        'away_def_vs_home_sim': similarity_lookups.get(away, {}).get(home, {}),
        'away_top_rotation': [p for p in player_profiles if p['team'] == away],
        'home_top_rotation': [p for p in player_profiles if p['team'] == home]
    })

@app.route('/api/status')
def api_status():
    _, _, _, _, last_updated = load_forensic_data()
    return jsonify({
        'status': 'OPERATIONAL',
        'engine': 'FORENSIC_SCOUTING_PIPELINE',
        'last_updated': last_updated,
        'teams_tracked': len(NBA_TEAMS),
        'is_vercel': IS_VERCEL
    })

if __name__ == '__main__':
    app.run(debug=True)
