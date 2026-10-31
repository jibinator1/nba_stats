"""
fetch_espn_depth_charts.py

Fetches official depth charts for all 30 NBA teams from ESPN API,
extracts player position hierarchies (PG, SG, SF, PF, C), resolves primary positions
by depth-chart precedence (starter rank 0 > backup rank 1/2), and synchronizes
both espn_depth_charts.json and positions.csv.
"""

import os
import json
import re
import unicodedata
import urllib.request
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ESPN_SLUGS = [
    'atl', 'bos', 'bkn', 'cha', 'chi', 'cle', 'dal', 'den', 'det', 'gs',
    'hou', 'ind', 'lac', 'lal', 'mem', 'mia', 'mil', 'min', 'no', 'ny',
    'okc', 'orl', 'phi', 'phx', 'por', 'sac', 'sa', 'tor', 'utah', 'wsh'
]

ESPN_TO_NBA = {
    'atl': 'ATL', 'bos': 'BOS', 'bkn': 'BKN', 'cha': 'CHA', 'chi': 'CHI',
    'cle': 'CLE', 'dal': 'DAL', 'den': 'DEN', 'det': 'DET', 'gs': 'GSW',
    'hou': 'HOU', 'ind': 'IND', 'lac': 'LAC', 'lal': 'LAL', 'mem': 'MEM',
    'mia': 'MIA', 'mil': 'MIL', 'min': 'MIN', 'no': 'NOP', 'ny': 'NYK',
    'okc': 'OKC', 'orl': 'ORL', 'phi': 'PHI', 'phx': 'PHX', 'por': 'POR',
    'sac': 'SAC', 'sa': 'SAS', 'tor': 'TOR', 'utah': 'UTA', 'wsh': 'WAS'
}

POSITIONS_ORDER = ['PG', 'SG', 'SF', 'PF', 'C']

def normalize_name(name):
    """Normalize player name for fuzzy matching across diacritics and suffixes."""
    if not isinstance(name, str):
        return ""
    n = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    n = re.sub(r'\b(Jr\.?|Sr\.?|II|III|IV)\b', '', n, flags=re.IGNORECASE)
    n = re.sub(r'[\.\'\-]', '', n)
    return " ".join(n.lower().split())

def fetch_espn_depth_charts():
    """
    Fetches raw depth charts for all 30 NBA teams from ESPN API.
    Returns:
        team_depth_charts: dict of { TEAM: { POS: [player_names] } }
        espn_players: dict of { player_name: [ {team, pos, depth} ] }
    """
    team_depth_charts = {}
    espn_players = {}

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    for slug in ESPN_SLUGS:
        team_abbr = ESPN_TO_NBA.get(slug, slug.upper())
        team_depth_charts[team_abbr] = {pos: [] for pos in POSITIONS_ORDER}
        url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{slug}/depthcharts'

        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            
            for item in data.get('depthchart', []):
                for pos_key, pos_val in item.get('positions', {}).items():
                    pos_abbr = pos_val.get('position', {}).get('abbreviation')
                    if pos_abbr not in POSITIONS_ORDER:
                        continue
                    athletes = pos_val.get('athletes', [])
                    for depth_idx, a in enumerate(athletes):
                        name = a.get('displayName', '').strip()
                        if not name:
                            continue
                        team_depth_charts[team_abbr][pos_abbr].append(name)
                        if name not in espn_players:
                            espn_players[name] = []
                        espn_players[name].append({
                            'team': team_abbr,
                            'pos': pos_abbr,
                            'depth': depth_idx,
                            'espn_id': a.get('id')
                        })
        except Exception as e:
            print(f"Warning: Failed to fetch ESPN depth chart for {slug} ({team_abbr}): {e}")

    return team_depth_charts, espn_players

def resolve_player_primary_positions(espn_players):
    """
    Resolves each player's primary position from ESPN depth chart entries.
    Priority: lowest depth index (depth 0 = starter beats backup depth 1/2/3).
    Tie-breaking: prefer primary position defined in POSITIONS_ORDER priority.
    """
    resolved = {}
    for name, entries in espn_players.items():
        # Sort by depth ascending, then by tiebreaker index in POSITIONS_ORDER
        sorted_entries = sorted(
            entries,
            key=lambda x: (x['depth'], POSITIONS_ORDER.index(x['pos']) if x['pos'] in POSITIONS_ORDER else 99)
        )
        best = sorted_entries[0]
        resolved[name] = {
            'pos': best['pos'],
            'team': best['team'],
            'depth': best['depth'],
            'espn_id': best.get('espn_id'),
            'all_positions': [(e['pos'], e['depth']) for e in sorted_entries]
        }
    return resolved

def sync_espn_positions(save_json=True, update_csv=True):
    """
    Main entry point:
    1. Fetches ESPN depth charts.
    2. Saves espn_depth_charts.json.
    3. Updates positions.csv with ESPN primary positions and active teams.
    """
    print("Fetching ESPN depth charts for all 30 NBA teams...")
    team_depth_charts, espn_players = fetch_espn_depth_charts()
    resolved = resolve_player_primary_positions(espn_players)

    json_path = os.path.join(BASE_DIR, 'espn_depth_charts.json')
    if save_json:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump({
                'teams': team_depth_charts,
                'players': resolved
            }, f, indent=2)
        print(f"Saved ESPN depth charts ({len(resolved)} players) to {json_path}")

    csv_path = os.path.join(BASE_DIR, 'positions.csv')
    if update_csv and os.path.exists(csv_path):
        pos_df = pd.read_csv(csv_path)

        # Build normalized lookups
        norm_resolved = {normalize_name(name): (name, data) for name, data in resolved.items()}
        matched_espn_names = set()
        changes = []

        for idx, row in pos_df.iterrows():
            p_name = row['Player']
            curr_pos = row['Pos']
            curr_tm = row['Team']

            info = None
            if p_name in resolved:
                info = resolved[p_name]
                matched_espn_names.add(p_name)
            elif normalize_name(p_name) in norm_resolved:
                orig_name, info = norm_resolved[normalize_name(p_name)]
                matched_espn_names.add(orig_name)

            if info:
                new_pos = info['pos']
                new_tm = info['team']
                if curr_pos != new_pos or curr_tm != new_tm:
                    changes.append({
                        'Player': p_name,
                        'Old_Pos': curr_pos,
                        'New_Pos': new_pos,
                        'Old_Team': curr_tm,
                        'New_Team': new_tm
                    })
                pos_df.at[idx, 'Pos'] = new_pos
                pos_df.at[idx, 'Team'] = new_tm

        # Add players in ESPN depth charts who are not yet in positions.csv
        unmatched_espn = [name for name in resolved if name not in matched_espn_names and normalize_name(name) not in norm_resolved]
        new_rows = []
        max_rk = pos_df['Rk'].max() if 'Rk' in pos_df.columns and not pos_df['Rk'].empty else 0
        for name in unmatched_espn:
            max_rk += 1
            info = resolved[name]
            new_row = {col: None for col in pos_df.columns}
            new_row['Rk'] = max_rk
            new_row['Player'] = name
            new_row['Team'] = info['team']
            new_row['Pos'] = info['pos']
            new_rows.append(new_row)

        if new_rows:
            pos_df = pd.concat([pos_df, pd.DataFrame(new_rows)], ignore_index=True)
            print(f"Added {len(new_rows)} new ESPN depth-chart players to positions.csv")

        pos_df.to_csv(csv_path, index=False)
        print(f"Updated {csv_path}: {len(changes)} existing position/team changes applied.")

    return team_depth_charts, resolved

if __name__ == '__main__':
    sync_espn_positions()
