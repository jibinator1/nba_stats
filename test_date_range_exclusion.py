import os
import unittest
import json
import pandas as pd
from forensic_data import load_clean_logs
from app import app, calculate_dynamic_teams_for_range

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class TestDateRangeExclusion(unittest.TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_load_clean_logs_strict_bounds(self):
        """Verifies that load_clean_logs strictly excludes all games outside [2026-10-20, 2026-10-30]."""
        df = load_clean_logs(start_date='2026-10-20', end_date='2026-10-30')
        self.assertFalse(df.empty, "Expected non-empty dataframe for active season")
        
        # Verify min and max game dates
        min_date = df['GAME_DATE'].min()
        max_date = df['GAME_DATE'].max()
        self.assertGreaterEqual(min_date, pd.to_datetime('2026-10-20'))
        self.assertLessEqual(max_date, pd.to_datetime('2026-10-30'))

        # Verify no 2025-26 games leaked in
        old_games = df[df['GAME_DATE'] < pd.to_datetime('2026-10-20')]
        self.assertEqual(len(old_games), 0, f"Found {len(old_games)} games from before 2026-10-20!")

    def test_boston_celtics_gp_and_c_pts_allowed(self):
        """Verifies Boston has exactly 4 GP and Center PTS Allowed = 9.8 in active date range."""
        with open(os.path.join(BASE_DIR, 'forensic_teams.json'), 'r') as f:
            teams = json.load(f)
        
        bos = teams.get('BOS', {})
        self.assertEqual(bos.get('gp'), 4, f"Expected Boston GP = 4, got {bos.get('gp')}")
        self.assertEqual(bos.get('c_pts_allowed'), 9.8, f"Expected Boston c_pts_allowed = 9.8, got {bos.get('c_pts_allowed')}")
        self.assertEqual(bos.get('rank_c_pts_allowed'), 8, f"Expected Boston rank_c_pts_allowed = 8, got {bos.get('rank_c_pts_allowed')}")
        
        # Check positional breakdown
        pts_allow = bos.get('positional_allowances', {}).get('pts', {})
        self.assertEqual(pts_allow.get('guards'), 26.8)
        self.assertEqual(pts_allow.get('wings'), 43.5)
        self.assertEqual(pts_allow.get('bigs'), 9.8)

    def test_dynamic_sub_window_exclusion(self):
        """Verifies dynamic recalculation on sub-window (2026-10-20 to 2026-10-24) excludes outside games."""
        with open(os.path.join(BASE_DIR, 'forensic_team_game_logs.json'), 'r') as f:
            team_logs = json.load(f)
        
        dyn = calculate_dynamic_teams_for_range(team_logs, '2026-10-20', '2026-10-24')
        bos_sub = dyn.get('BOS', {})
        self.assertEqual(bos_sub.get('gp'), 2, f"Expected Boston GP = 2 in sub-window, got {bos_sub.get('gp')}")
        self.assertEqual(bos_sub.get('c_pts_allowed'), 14.0)
        self.assertEqual(bos_sub.get('rank_c_pts_allowed'), 16)

    def test_api_forensic_teams_date_filter(self):
        """Verifies /api/forensic-teams endpoint respects start_date and end_date."""
        res = self.client.get('/api/forensic-teams?start_date=2026-10-20&end_date=2026-10-30')
        self.assertEqual(res.status_code, 200)
        payload = res.get_json()
        teams = payload.get('forensic_teams', {})
        bos = teams.get('BOS', {})
        self.assertEqual(bos.get('gp'), 4)
        self.assertEqual(bos.get('c_pts_allowed'), 9.8)

if __name__ == '__main__':
    unittest.main()
