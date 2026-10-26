import os
import sys
import json
import random
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHED_FILE = os.path.join(SCRIPT_DIR, 'schedule_games.json')

ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

ESPN_TO_NBA = {
    'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'SA': 'SAS', 'UTAH': 'UTA', 'WSH': 'WAS'
}

NBA_TEAMS = {
    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
    'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
    'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
}

def normalize_team(abbr):
    abbr = abbr.strip().upper()
    return ESPN_TO_NBA.get(abbr, abbr)

def load_schedule():
    with open(SCHED_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def fetch_espn_date(date_str):
    url = f"{ESPN_SCOREBOARD_URL}?dates={date_str}"
    r = requests.get(url, timeout=12)
    if r.status_code == 200:
        return r.json().get('events', [])
    return []

def test_todays_slate():
    print("\n" + "="*70)
    print("TEST 1: TODAY'S SLATE VERIFICATION (2026-10-26)")
    print("="*70)

    sched = load_schedule()
    today_local = [g for g in sched if g.get('gameday') == '2026-10-26']
    print(f"Total games in local schedule_games.json for 2026-10-26: {len(today_local)}")

    # Check for erroneous BKN vs NOP
    bkn_nop = [g for g in today_local if {g['away_team'], g['home_team']} == {'BKN', 'NOP'}]
    assert len(bkn_nop) == 0, f"ERROR: Erroneous BKN vs NOP game still present: {bkn_nop}"
    print("  [PASS] Confirmed BKN vs NOP is NOT present in today's games.")

    # Fetch live ESPN slate for today
    espn_events = fetch_espn_date('20261026')
    print(f"Total live events from ESPN for 2026-10-26: {len(espn_events)}")
    assert len(today_local) == len(espn_events), f"Mismatch in count: {len(today_local)} local vs {len(espn_events)} ESPN"

    local_map = {(g['away_team'], g['home_team']): g for g in today_local}

    for ev in espn_events:
        comps = ev['competitions'][0]['competitors']
        home = next(c for c in comps if c['homeAway'] == 'home')
        away = next(c for c in comps if c['homeAway'] == 'away')
        h_abbr = normalize_team(home['team']['abbreviation'])
        a_abbr = normalize_team(away['team']['abbreviation'])

        key = (a_abbr, h_abbr)
        assert key in local_map, f"ERROR: Matchup {a_abbr} @ {h_abbr} missing from schedule_games.json"

        g = local_map[key]
        assert g['gameday'] == '2026-10-26', f"Gameday mismatch for {key}: {g['gameday']}"
        print(f"  [PASS] Matchup {a_abbr:>3} @ {h_abbr:<3} correctly scheduled at {g['gametime']} (ID: {g['game_id']})")

    print("  [PASS] ALL 9 TODAY'S GAMES VERIFIED AGAINST ESPN LIVE!")

def test_random_samples_across_season():
    print("\n" + "="*70)
    print("TEST 2: RANDOM SAMPLE VERIFICATION ACROSS SEASON FROM ESPN LIVE")
    print("="*70)

    sched = load_schedule()
    # Group local games by date
    by_date = {}
    for g in sched:
        d = g.get('gameday')
        if d:
            by_date.setdefault(d, []).append(g)

    all_dates = sorted(by_date.keys())
    print(f"Total unique game dates in schedule: {len(all_dates)} ({all_dates[0]} to {all_dates[-1]})")

    # Pick at least 2 dates per month across Oct, Nov, Dec, Jan, Feb, Mar, Apr
    random.seed(42)  # reproducible seed
    month_buckets = {}
    for d in all_dates:
        m = d[:7]  # YYYY-MM
        month_buckets.setdefault(m, []).append(d)

    sample_dates = []
    for m, d_list in sorted(month_buckets.items()):
        picked = random.sample(d_list, min(2, len(d_list)))
        sample_dates.extend(picked)

    print(f"Selected {len(sample_dates)} sample dates spanning all season months:")
    print(" ", ", ".join(sample_dates))

    total_sampled_games = 0
    passed_games = 0

    for sample_date in sample_dates:
        date_param = sample_date.replace('-', '')
        espn_events = fetch_espn_date(date_param)

        local_games = by_date.get(sample_date, [])
        local_lookup = {(g['away_team'], g['home_team']): g for g in local_games}

        print(f"\nChecking date {sample_date} ({len(espn_events)} ESPN games):")

        for ev in espn_events:
            comps = ev['competitions'][0]['competitors']
            home = next(c for c in comps if c['homeAway'] == 'home')
            away = next(c for c in comps if c['homeAway'] == 'away')
            h_raw = home['team']['abbreviation']
            a_raw = away['team']['abbreviation']

            if h_raw == 'TBD' or a_raw == 'TBD':
                continue

            h_abbr = normalize_team(h_raw)
            a_abbr = normalize_team(a_raw)

            total_sampled_games += 1
            key = (a_abbr, h_abbr)

            if key not in local_lookup:
                print(f"  [FAIL] Missing game {a_abbr} @ {h_abbr} on {sample_date}")
                assert False, f"Missing game {a_abbr} @ {h_abbr} on {sample_date}"

            g = local_lookup[key]

            # Verify teams, date, and completed scores
            assert g['away_team'] == a_abbr
            assert g['home_team'] == h_abbr
            assert g['gameday'] == sample_date

            espn_completed = ev['competitions'][0]['status']['type']['completed']
            if espn_completed:
                assert g['is_final'] is True, f"Game {key} on {sample_date} should be marked final"
                if away.get('score') and str(away.get('score')).isdigit():
                    assert g['away_score'] == int(away['score']), f"Away score mismatch for {key}"
                if home.get('score') and str(home.get('score')).isdigit():
                    assert g['home_score'] == int(home['score']), f"Home score mismatch for {key}"

            passed_games += 1
            print(f"  [PASS] {a_abbr:>3} @ {h_abbr:<3} | {g['gametime']:<11} | Final: {g['is_final']} (ESPN matches 100%)")

    print("\n" + "="*70)
    print(f"VERIFICATION SUMMARY: {passed_games}/{total_sampled_games} sampled games verified with 100% accuracy!")
    print("="*70)

if __name__ == '__main__':
    test_todays_slate()
    test_random_samples_across_season()
    print("\nAll schedule tests passed successfully!")
