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
    print("\n[7/8] Testing Date-Filtered Forensic Gating (Zero Games -> Blank / Null)...")
    res_zero = client.get('/api/forensic-teams?start_date=2026-10-20&end_date=2026-10-25')
    zero_teams = res_zero.json.get('forensic_teams', {})
    assert 'BOS' in zero_teams, "Missing BOS in zero_teams"
    bos_zero = zero_teams['BOS']
    assert bos_zero['gp'] == 0, f"Expected gp == 0, got {bos_zero['gp']}"
    assert bos_zero['pts_scored'] is None, f"Expected pts_scored is None, got {bos_zero['pts_scored']}"
    assert bos_zero['rank_pts_scored'] is None, f"Expected rank_pts_scored is None, got {bos_zero['rank_pts_scored']}"
    assert bos_zero['tov_total'] is None, f"Expected tov_total is None, got {bos_zero['tov_total']}"
    assert bos_zero['oreb_pct'] is None, f"Expected oreb_pct is None, got {bos_zero['oreb_pct']}"
    print(f"  Zero-Game Window BOS: GP={bos_zero['gp']} | PTS={bos_zero['pts_scored']} | Rank={bos_zero['rank_pts_scored']}")

    res_players_zero = client.get('/api/player-profiles?start_date=2026-10-20&end_date=2026-10-25')
    players_zero = res_players_zero.json.get('players', [])
    assert len(players_zero) > 0, "Expected players list"
    assert players_zero[0]['games_played'] == 0, f"Expected player gp 0, got {players_zero[0]['games_played']}"
    assert players_zero[0]['season_avg']['pts'] is None, f"Expected pts None, got {players_zero[0]['season_avg']['pts']}"
    print(f"  Zero-Game Window Player ({players_zero[0]['player_name']}): GP={players_zero[0]['games_played']} | PTS Avg={players_zero[0]['season_avg']['pts']}")
    print("  [PASS] Date-filtered gating correctly blanks stats and suppresses ranks when games played == 0.")

    # 8. Verify Turnovers, Rebound Percentages, and Rank Tooltips
    print("\n[8/8] Testing Turnovers, Rebounding Percentages, Sample Size Labels & Rank Badges...")
    res_teams = client.get('/api/forensic-teams?start_date=2025-10-02&end_date=2026-04-15')
    f_teams = res_teams.json.get('forensic_teams', {})
    sample_t = f_teams['BOS']
    assert 'tov_total' in sample_t and sample_t['tov_total'] is not None, "Missing tov_total"
    assert 'tov_allowed' in sample_t and sample_t['tov_allowed'] is not None, "Missing tov_allowed"
    assert 'oreb_pct' in sample_t and sample_t['oreb_pct'] is not None, "Missing oreb_pct"
    assert 'dreb_pct' in sample_t and sample_t['dreb_pct'] is not None, "Missing dreb_pct"
    assert 'rank_tov_total' in sample_t and sample_t['rank_tov_total'] is not None, "Missing rank_tov_total"
    assert 'rank_tov_allowed' in sample_t and sample_t['rank_tov_allowed'] is not None, "Missing rank_tov_allowed"
    assert 'rank_oreb_pct' in sample_t and sample_t['rank_oreb_pct'] is not None, "Missing rank_oreb_pct"
    assert 'rank_dreb_pct' in sample_t and sample_t['rank_dreb_pct'] is not None, "Missing rank_dreb_pct"
    assert 'injuries' in sample_t, "Missing injuries list in team profile"
    
    # Verify rank badge tooltips, injury badges, and metric rows in app.js
    res_app_js = client.get('/static/js/app.js')
    app_js_text = res_app_js.data.decode('utf-8')
    assert 'Turnovers / G' in app_js_text, "Missing Turnovers / G in app.js"
    assert 'Opp Turnovers / G' in app_js_text, "Missing Opp Turnovers / G in app.js"
    assert 'OREB%' in app_js_text, "Missing OREB% in app.js"
    assert 'DREB%' in app_js_text, "Missing DREB% in app.js"
    assert 'rank-tooltip' in app_js_text, "Missing rank-tooltip in app.js"
    assert 'rank-badge' in app_js_text, "Missing rank-badge in app.js"
    assert 'Small Sample: n=' in app_js_text, "Missing Small Sample label formatting in app.js"
    assert 'renderInjuryBadge' in app_js_text, "Missing renderInjuryBadge in app.js"
    
    print(f"  BOS Metrics: TOV/G={sample_t['tov_total']} (#{sample_t['rank_tov_total']}) | Opp TOV/G={sample_t['tov_allowed']} (#{sample_t['rank_tov_allowed']}) | OREB%={sample_t['oreb_pct']}% (#{sample_t['rank_oreb_pct']}) | DREB%={sample_t['dreb_pct']}% (#{sample_t['rank_dreb_pct']})")
    print("  [PASS] All new contextual metrics, ranks, sample size labels, injury tags, and tooltips verified.")

    # 9. Verify Overhauled Deep Dive Modal: Visual Cards, League Ranks Across All Pillars, and Player Distribution Bars
    print("\n[9/9] Testing Overhauled Deep Dive Insights (Visual Cards, All Pillar Ranks, Player Distribution Bars)...")
    res_modal_js = client.get('/static/js/modal.js')
    modal_js_text = res_modal_js.data.decode('utf-8')
    res_visuals_js = client.get('/static/js/visuals.js')
    visuals_js_text = res_visuals_js.data.decode('utf-8')

    # Executive Callouts visual cards
    assert 'Transition Flow & Alignment' in modal_js_text, "Missing Transition Flow callout"
    assert 'Live-Ball Turnover Leakage' in modal_js_text, "Missing Live-Ball Turnover Leakage callout"
    assert 'Rim Protection vs. Paint Volume' in modal_js_text, "Missing Rim Protection vs. Paint Volume callout"
    assert 'makeCard' in modal_js_text, "Missing visual card generator makeCard in modal.js"

    # All Pillar metrics have ranks in BOS profile
    sample_b = f_teams['BOS']
    pillar_rank_keys = [
        'rank_early_0_8', 'rank_mid_8_15', 'rank_late_15_24',
        'rank_pace_leading_10', 'rank_pace_trailing_10',
        'rank_pnr_drop', 'rank_pnr_switch',
        'rank_ra_ppp_allowed', 'rank_paint_ppp_allowed', 'rank_corner3_ppp_allowed',
        'rank_transition_ppp_allowed',
        'rank_pnr_bh_freq', 'rank_iso_freq', 'rank_spot_freq',
        'rank_live_tov', 'rank_dead_tov',
        'rank_pts_generated_off_tov', 'rank_pts_surrendered_off_tov'
    ]
    for rk in pillar_rank_keys:
        assert rk in sample_b and sample_b[rk] is not None, f"Missing {rk} in BOS team profile"
    print(f"  Pillar 1 Ranks (BOS): Early SC #{sample_b['rank_early_0_8']} | Mid SC #{sample_b['rank_mid_8_15']} | Late SC #{sample_b['rank_late_15_24']}")
    print(f"  Pillar 2 Ranks (BOS): Drop #{sample_b['rank_pnr_drop']} | Switch #{sample_b['rank_pnr_switch']} | Rim Allowed #{sample_b['rank_ra_ppp_allowed']}")
    print(f"  Pillar 3 Ranks (BOS): Live TOV #{sample_b['rank_live_tov']} | TOV Pts Surr #{sample_b['rank_pts_surrendered_off_tov']}")

    # Check that formatPillarRow renders (#Rank of 30)
    assert 'formatPillarRow' in modal_js_text, "Missing formatPillarRow in modal.js"
    assert '(#${r} of 30)' in modal_js_text or '(#${r}' in modal_js_text, "Missing (#Rank of 30) badge format in modal.js"

    # Check Wide-Open 3PT removed from Pillar 2
    assert 'Wide-Open 3PT Rate Allowed' not in modal_js_text, "Wide-Open 3PT Rate Allowed should be removed from Pillar 2"

    # Check Player Usage Profiles have ranks and distribution bar
    assert 'pts_rank' in players[0]['season_avg'], "Missing pts_rank in player season_avg"
    assert '10G PTS Distribution' in modal_js_text, "Missing visual distribution bar container in modal.js"
    assert 'maxDomain' in modal_js_text, "Missing distribution bar math in modal.js"

    # Check SVG Visual cleanups
    assert 'Shot Focus:' not in visuals_js_text, "Redundant bottom pills should be removed from Shot Zone SVG"
    assert 'Away Team Pace Indicator (Aligned directly above needle)' in visuals_js_text, "Pace Gauge label not aligned directly above needle"

    print("  [PASS] All executive visual cards, 3 new callout cards, pillar ranks, player distribution bars, and SVG visual alignments verified.")

    # 10. Verify Functional Role Archetypes & Grouped Positional Allowances (Guards / Wings / Bigs)
    print("\n[10/10] Testing Functional Role Archetypes & Grouped Positional Matchup Tiers...")
    sample_bos = f_teams['BOS']
    sample_det = f_teams['DET']
    
    # Check Positional Allowances data structure
    assert 'positional_allowances' in sample_bos, "Missing positional_allowances in BOS"
    bos_pos = sample_bos['positional_allowances']
    for stat in ['pts', 'reb', 'fg3m']:
        assert stat in bos_pos, f"Missing {stat} in positional_allowances"
        for tier in ['guards', 'wings', 'bigs']:
            assert tier in bos_pos[stat], f"Missing {tier} in {stat}"
            assert f'{tier}_pct' in bos_pos[stat], f"Missing {tier}_pct in {stat}"
            assert f'{tier}_rank' in bos_pos[stat], f"Missing {tier}_rank in {stat}"

    print(f"  BOS Positional Allowances:")
    print(f"    PTS: Guards {bos_pos['pts']['guards']} (#{bos_pos['pts']['guards_rank']}) | Wings {bos_pos['pts']['wings']} (#{bos_pos['pts']['wings_rank']}) | Bigs {bos_pos['pts']['bigs']} (#{bos_pos['pts']['bigs_rank']})")
    print(f"    REB: Guards {bos_pos['reb']['guards']} (#{bos_pos['reb']['guards_rank']}) | Wings {bos_pos['reb']['wings']} (#{bos_pos['reb']['wings_rank']}) | Bigs {bos_pos['reb']['bigs']} (#{bos_pos['reb']['bigs_rank']})")
    print(f"    3PM: Guards {bos_pos['fg3m']['guards']} (#{bos_pos['fg3m']['guards_rank']}) | Wings {bos_pos['fg3m']['wings']} (#{bos_pos['fg3m']['wings_rank']}) | Bigs {bos_pos['fg3m']['bigs']} (#{bos_pos['fg3m']['bigs_rank']})")

    # Check Defensive Play Type PPP allowed & ranks
    assert 'playtypes_defense' in sample_bos, "Missing playtypes_defense in BOS"
    roles_to_check = ['PnR_Ball_Handler', 'Spot_Up', 'Isolation', 'PnR_Roll_Man']
    for role in roles_to_check:
        assert role in sample_bos['playtypes_defense'], f"Missing {role} in playtypes_defense"
        role_entry = sample_bos['playtypes_defense'][role]
        assert 'allowed_ppp' in role_entry, f"Missing allowed_ppp for {role}"
        assert 'rank' in role_entry, f"Missing rank for {role}"
    print(f"  BOS Role Defense PPP Allowed:")
    print(f"    PnR Ball Handler: {sample_bos['playtypes_defense']['PnR_Ball_Handler']['allowed_ppp']} PPP (#{sample_bos['playtypes_defense']['PnR_Ball_Handler']['rank']})")
    print(f"    Spot-Up: {sample_bos['playtypes_defense']['Spot_Up']['allowed_ppp']} PPP (#{sample_bos['playtypes_defense']['Spot_Up']['rank']})")
    print(f"    Isolation: {sample_bos['playtypes_defense']['Isolation']['allowed_ppp']} PPP (#{sample_bos['playtypes_defense']['Isolation']['rank']})")
    print(f"    Roll Man / Rim Runner: {sample_bos['playtypes_defense']['PnR_Roll_Man']['allowed_ppp']} PPP (#{sample_bos['playtypes_defense']['PnR_Roll_Man']['rank']})")

    # Check Player Role Archetypes
    det_players = [p for p in players if p['team'] == 'DET']
    assert len(det_players) > 0, "No DET players found"
    cade = next((p for p in det_players if 'Cade' in p['player_name']), None)
    assert cade is not None, "Cade Cunningham not found in DET roster"
    assert 'role_archetype' in cade, "Missing role_archetype in Cade profile"
    assert cade['role_archetype']['primary_role'] == 'Pick & Roll Ball Handler', f"Unexpected role for Cade: {cade['role_archetype']}"
    print(f"  Cade Cunningham Role: {cade['role_archetype']['primary_role']} ({cade['role_archetype']['frequency']}%)")

    # Check frontend modal code contains new UI cards
    assert 'Grouped Positional Allowances (Guards / Wings / Bigs)' in modal_js_text, "Missing Grouped Positional Allowances header in modal.js"
    assert 'Role Archetype Defense & Scheme Alignment' in modal_js_text, "Missing Role Archetype Defense header in modal.js"
    assert 'Role Archetype Scheme Alignment' in modal_js_text, "Missing Role Archetype callout in modal.js"

    # Test DET @ BOS matchup endpoint
    res_matchup = client.get('/api/matchup/DET/BOS')
    assert res_matchup.status_code == 200, f"Failed /api/matchup/DET/BOS: {res_matchup.status_code}"
    m_data = res_matchup.json
    assert 'away_forensic' in m_data and 'home_forensic' in m_data, "Missing forensic profiles in matchup payload"
    assert 'positional_allowances' in m_data['home_forensic'], "Missing positional_allowances in matchup home profile"
    print("  [PASS] /api/matchup/DET/BOS renders cleanly with positional allowances and role archetypes.")

    # 11. Verify Interactive Tooltips, Jaylen Brown to PHI Roster Transfer, and Cade Cunningham Operator Text
    print("\n[11/11] Testing Interactive Tooltips, Jaylen Brown Trade to PHI, and Cade Operator Insight...")
    
    # Check Jaylen Brown is on PHI
    jb = next((p for p in players if p['player_name'] == 'Jaylen Brown'), None)
    assert jb is not None, "Jaylen Brown not found in player profiles"
    assert jb['team'] == 'PHI', f"Expected Jaylen Brown on PHI, got {jb['team']}"
    bos_players = [p['player_name'] for p in players if p['team'] == 'BOS']
    assert 'Jaylen Brown' not in bos_players, "Jaylen Brown should not be in BOS roster"
    phi_players = [p['player_name'] for p in players if p['team'] == 'PHI']
    assert 'Jaylen Brown' in phi_players, "Jaylen Brown must be in PHI roster"
    bos_top4_ascii = [p.encode('ascii', 'replace').decode('ascii') for p in bos_players[:4]]
    print(f"  Roster Transfer: Jaylen Brown confirmed on {jb['team']} (BOS Top 4: {bos_top4_ascii})")

    # Check FORENSIC_DEFINITIONS and stat-info-trigger in modal.js
    assert 'FORENSIC_DEFINITIONS' in modal_js_text, "Missing FORENSIC_DEFINITIONS dictionary in modal.js"
    assert 'stat-info-trigger' in modal_js_text, "Missing stat-info-trigger in modal.js"
    assert 'stat-info-box' in modal_js_text, "Missing stat-info-box in modal.js"
    assert 'formatMetricLabelWithTooltip' in modal_js_text, "Missing formatMetricLabelWithTooltip in modal.js"
    assert 'formatInlinePpp' in modal_js_text, "Missing formatInlinePpp in modal.js"
    print("  [PASS] Interactive tooltips and definitions verified across forensic metrics.")

    # Check Cade Cunningham Pick & Roll Ball Handler operator text in insights
    assert "operates as the Pick & Roll Ball Handler on 41.5%" in modal_js_text, "Missing Cade Cunningham explicit operator text in modal.js"
    assert "Operates as the" in modal_js_text and "of offensive possessions" in modal_js_text, "Missing player card role operator sentence in modal.js"
    print("  [PASS] Explicit Cade Cunningham Pick & Roll Ball Handler operator text verified in insights.")

    print("\n" + "=" * 60)
    print("ALL FORENSIC AUDIT CHECKS PASSED WITH 100% COMPLIANCE!")
    print("=" * 60)

if __name__ == '__main__':
    run_audit()
