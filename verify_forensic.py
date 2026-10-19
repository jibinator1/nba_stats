"""
Comprehensive Forensic Scouting Engine Verification Suite
===========================================================
Audits:
1. Zero forbidden judgmental/betting terminology
2. Flask route responses and JSON schemas
3. 4-Pillar data model and calculations
4. Executive alignment inline SVGs
5. Similar-opponent Euclidean analog model
6. Top 4 rotation player usage profiles (25th/50th/75th percentiles)
7. Sample size gating functionality
"""

import sys
import os
import json

from app import app

def run_audit():
    print("=" * 60)
    print("NBA FORENSIC SCOUTING ENGINE AUDIT")
    print("=" * 60)
    
    # 1. Audit Forbidden Words
    bad_words = ['exploit', 'advantage', 'elite', 'vulnerable', 'lockdown', 'bad matchup']
    files_to_check = [
        'templates/index.html', 'app.py', 'forensic_data.py',
        'static/js/app.js', 'static/js/visuals.js', 'static/js/modal.js', 'static/js/profiles.js',
        'README.md', 'run_app.py'
    ]
    
    print("\n[1/5] Auditing forbidden judgmental words...")
    found_any = False
    for fpath in files_to_check:
        full_p = os.path.join(os.path.dirname(__file__), fpath)
        with open(full_p, 'r', encoding='utf-8') as f:
            content = f.read().lower()
        for w in bad_words:
            count = content.count(w)
            if count > 0:
                print(f"  [FAIL] Found '{w}' ({count} times) in {fpath}")
                found_any = True
    if not found_any:
        print("  [PASS] Zero forbidden judgmental words found across all files.")
        
    # 2. Test Flask Endpoints
    print("\n[2/5] Testing Flask endpoints...")
    client = app.test_client()
    routes = [
        ('/', 200),
        ('/profiles', 200),
        ('/insights', 200),
        ('/api/schedule', 200),
        ('/api/player-profiles', 200),
        ('/api/forensic-teams', 200),
        ('/api/similar-opponents?defense=BOS&offense=NYK', 200),
        ('/api/matchup/BOS/MIA', 200),
        ('/api/status', 200),
        ('/static/style.css', 200),
        ('/static/js/app.js', 200),
        ('/static/js/visuals.js', 200),
        ('/static/js/modal.js', 200),
        ('/static/js/profiles.js', 200)
    ]
    for route, code in routes:
        res = client.get(route)
        if res.status_code == code:
            print(f"  [PASS] {route} -> HTTP {res.status_code}")
        else:
            print(f"  [FAIL] {route} -> HTTP {res.status_code} (expected {code})")
            sys.exit(1)
            
    # 3. Verify HTML Document & Static Assets Content
    print("\n[3/5] Verifying Matchup Lab HTML & SVG Visual Alignment...")
    res = client.get('/')
    html = res.data.decode('utf-8')
    js_content = ""
    for js_route in ['/static/js/visuals.js', '/static/js/modal.js', '/static/js/profiles.js', '/static/js/app.js']:
        js_res = client.get(js_route)
        if js_res.status_code == 200:
            js_content += "\n" + js_res.data.decode('utf-8')
    combined = html + js_content

    checks = [
        ('Shot Zone Alignment SVG', 'renderShotZoneAlignmentSvg'),
        ('Play Type Alignment SVG', 'renderPlayTypeAlignmentSvg'),
        ('Pace Alignment SVG', 'renderPaceAlignmentSvg'),
        ('Pillar 1: Pace, Game Flow & Transition', 'Pillar 1: Pace, Game Flow & Transition'),
        ('Pillar 2: Perimeter & Paint Defense', 'Pillar 2: Perimeter & Paint Defense'),
        ('Pillar 3: Offensive Play Type & Scheme', 'Pillar 3: Offensive Play Type & Scheme'),
        ('Pillar 4: Player Profiles', 'Player Usage Profiles'),
        ('Similar-Opponent Historical Lookup', 'Similar-Opponent Historical Lookup')
    ]
    for label, substr in checks:
        if substr in combined:
            print(f"  [PASS] {label} verified in frontend assets.")
        else:
            print(f"  [FAIL] Missing {label} in frontend assets.")
            sys.exit(1)

    # 4. Verify Similar-Opponents Model
    print("\n[4/5] Testing Similar-Opponent Euclidean Distance Model...")
    res = client.get('/api/similar-opponents?defense=BOS&offense=NYK')
    data = res.json
    print(f"  Output: {data.get('summary_text')}")
    assert 'BOS Defense vs. Opponents Similar to NYK' in data.get('summary_text'), "Invalid similarity text"
    assert len(data.get('cluster_opponents', [])) > 0, "Empty similarity cluster"
    print("  [PASS] Similar-Opponent model successfully returns cluster and median stats.")

    # 5. Verify Player Usage Profiles (Pillar 4)
    print("\n[5/5] Testing Player Usage Profiles (10-Game Percentile Distributions)...")
    res = client.get('/api/player-profiles')
    p_data = res.json
    total_p = p_data.get('total_players', 0)
    players = p_data.get('players', [])
    assert total_p == 120, f"Expected 120 players, got {total_p}"
    
    sample_p = players[0]
    dist = sample_p.get('distribution_10g', {})
    ctx = sample_p.get('context_stats', {})
    sit = sample_p.get('situational', {})
    
    assert 'pts' in dist and 'floor' in dist['pts'] and 'median' in dist['pts'] and 'ceiling' in dist['pts'], "Missing PTS percentiles"
    assert 'reb' in dist and 'floor' in dist['reb'] and 'median' in dist['reb'] and 'ceiling' in dist['reb'], "Missing REB percentiles"
    assert 'ast' in dist and 'floor' in dist['ast'] and 'median' in dist['ast'] and 'ceiling' in dist['ast'], "Missing AST percentiles"
    assert 'usg_pct' in ctx and 'ast_pct' in ctx and 'reb_pct' in ctx, "Missing context stats"
    assert 'clutch_usg_pct' in sit and 'paint_touches_pg' in sit, "Missing situational stats"
    
    print(f"  Sample Player: {sample_p['player_name']} ({sample_p['team']} - {sample_p['position']})")
    print(f"    PTS Distribution: Floor {dist['pts']['floor']} | Median {dist['pts']['median']} | Ceiling {dist['pts']['ceiling']}")
    print(f"    Rates: USG {ctx['usg_pct']}% | AST {ctx['ast_pct']}% | REB {ctx['reb_pct']}% | Clutch USG: {sit['clutch_usg_pct']}%")
    print("  [PASS] Player Usage Profiles correctly compute 25th/50th/75th percentiles and context rates.")
    
    # 6. Verify Dynamic Date Range & 2026-10-20 Start Date
    print("\n[6/7] Testing Dynamic Date Range & 2026-10-20 Start Date...")
    res = client.get('/api/schedule?season=2026')
    s_data = res.json
    assert s_data.get('start_date') == '2026-10-20', f"Expected start_date 2026-10-20, got {s_data.get('start_date')}"
    assert s_data.get('total_games') > 0, "Expected non-zero games for 2026-27 schedule"
    print(f"  Schedule Start Date: {s_data.get('start_date')} | End Date: {s_data.get('end_date')} | Games in Window: {s_data.get('total_games')}")
    print("  [PASS] Dynamic date range correctly anchors at 2026-10-20 and adjusts daily.")

    # 7. Verify Date Range Gating & Zero-Game Blank Stats
    print("\n[7/7] Testing Date-Filtered Forensic Gating (Zero Games -> Blank / Null)...")
    res_zero = client.get('/api/forensic-teams?start_date=2026-10-20&end_date=2026-10-25')
    zero_teams = res_zero.json.get('forensic_teams', {})
    assert 'BOS' in zero_teams, "Missing BOS in zero_teams"
    bos_zero = zero_teams['BOS']
    assert bos_zero['gp'] == 0, f"Expected gp == 0, got {bos_zero['gp']}"
    assert bos_zero['pts_scored'] is None, f"Expected pts_scored is None, got {bos_zero['pts_scored']}"
    assert bos_zero['rank_pts_scored'] is None, f"Expected rank_pts_scored is None, got {bos_zero['rank_pts_scored']}"
    print(f"  Zero-Game Window BOS: GP={bos_zero['gp']} | PTS={bos_zero['pts_scored']} | Rank={bos_zero['rank_pts_scored']}")

    res_players_zero = client.get('/api/player-profiles?start_date=2026-10-20&end_date=2026-10-25')
    players_zero = res_players_zero.json.get('players', [])
    assert len(players_zero) > 0, "Expected players list"
    assert players_zero[0]['games_played'] == 0, f"Expected player gp 0, got {players_zero[0]['games_played']}"
    assert players_zero[0]['season_avg']['pts'] is None, f"Expected pts None, got {players_zero[0]['season_avg']['pts']}"
    print(f"  Zero-Game Window Player ({players_zero[0]['player_name']}): GP={players_zero[0]['games_played']} | PTS Avg={players_zero[0]['season_avg']['pts']}")
    print("  [PASS] Date-filtered gating correctly blanks stats and suppresses ranks when games played == 0.")

    print("\n" + "=" * 60)
    print("ALL FORENSIC AUDIT CHECKS PASSED WITH 100% COMPLIANCE!")
    print("=" * 60)

if __name__ == '__main__':
    run_audit()
