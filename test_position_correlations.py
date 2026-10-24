import os
import sys
import unittest
import json
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from position_correlations import calculate_position_correlations, load_position_map
from app import app, load_position_correlations

class TestPositionalCorrelations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = calculate_position_correlations(min_minutes=12.0)
        cls.client = app.test_client()

    def test_metadata_structure(self):
        meta = self.data.get('metadata', {})
        self.assertIn('qualifying_logs_count', meta)
        self.assertGreater(meta['qualifying_logs_count'], 15000)
        self.assertEqual(meta['teams_count'], 30)
        self.assertListEqual(meta['positions'], ['PG', 'SG', 'SF', 'PF', 'C'])
        self.assertListEqual(meta['tiers'], ['Guards', 'Wings', 'Bigs'])

    def test_anova_significance(self):
        anova = self.data.get('anova', {})
        self.assertIn('C', anova)
        self.assertIn('PTS', anova['C'])
        c_pts = anova['C']['PTS']
        # Center points should be statistically significant across 30 teams (p < 0.001)
        self.assertTrue(c_pts['is_significant_001'])
        self.assertLess(c_pts['p_value'], 1e-4)

    def test_matchup_effect_categorization(self):
        matchups = self.data.get('matchups', [])
        self.assertGreater(len(matchups), 1000)
        
        categories = set(m['effect_category'] for m in matchups)
        self.assertTrue({'boosted', 'neutral', 'suppressed'}.issubset(categories))
        
        for m in matchups[:50]:
            self.assertIn('mean_diff', m)
            self.assertIn('median_diff', m)
            self.assertIn('cohens_d', m)
            self.assertIn('p_val_ttest', m)
            self.assertIn('pct_over_med', m)
            if m['cohens_d'] >= 0.15:
                self.assertEqual(m['effect_category'], 'boosted')
            elif m['cohens_d'] <= -0.15:
                self.assertEqual(m['effect_category'], 'suppressed')
            else:
                self.assertEqual(m['effect_category'], 'neutral')

    def test_cross_position_correlations(self):
        cross = self.data.get('cross_position_correlations', {})
        self.assertIn('PTS', cross)
        self.assertIn('REB', cross)
        pts_corr = cross['PTS']
        # Diagonal should be 1.0
        for pos in ['PG', 'SG', 'SF', 'PF', 'C']:
            self.assertAlmostEqual(pts_corr[pos][pos], 1.0, places=2)

    def test_api_positional_correlations_endpoint(self):
        response = self.client.get('/api/positional-correlations')
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn('matchups', payload)
        self.assertIn('anova', payload)
        self.assertEqual(payload['count'], len(payload['matchups']))

    def test_api_positional_correlations_filters(self):
        # Filter by PG and PTS
        response = self.client.get('/api/positional-correlations?position=PG&metric=PTS')
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload['count'], 30)
        for m in payload['matchups']:
            self.assertEqual(m['position'], 'PG')
            self.assertEqual(m['metric'], 'PTS')

        # Filter by category boosted
        response_b = self.client.get('/api/positional-correlations?position=C&metric=PTS&category=boosted')
        self.assertEqual(response_b.status_code, 200)
        payload_b = response_b.get_json()
        for m in payload_b['matchups']:
            self.assertEqual(m['effect_category'], 'boosted')

    def test_positional_html_view(self):
        response = self.client.get('/positional')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'tab-positional', response.data)
        self.assertIn(b'Positional Matchup Correlation Lab', response.data)

    def test_significant_positional_allowances_in_teams(self):
        with open(os.path.join(BASE_DIR, 'forensic_teams.json'), 'r') as f:
            teams = json.load(f)
        bos = teams.get('BOS', {})
        # Significant metrics must be populated
        self.assertIsNotNone(bos.get('c_pts_allowed'))
        self.assertIsNotNone(bos.get('rank_c_pts_allowed'))
        self.assertIsNotNone(bos.get('c_reb_allowed'))
        self.assertIsNotNone(bos.get('rank_c_reb_allowed'))
        self.assertIsNotNone(bos.get('c_fg3m_allowed'))
        self.assertIsNotNone(bos.get('sg_fg3m_allowed'))
        self.assertIsNotNone(bos.get('pg_ast_allowed'))

    def test_non_significant_positional_allowances_omitted(self):
        with open(os.path.join(BASE_DIR, 'forensic_teams.json'), 'r') as f:
            teams = json.load(f)
        for t in ['BOS', 'NYK', 'LAL', 'DEN']:
            prof = teams.get(t, {})
            # Non-significant metrics (p >= 0.05) must remain None
            self.assertIsNone(prof.get('sf_pts_allowed'))
            self.assertIsNone(prof.get('sf_fg3m_allowed'))
            self.assertIsNone(prof.get('pf_fg3m_allowed'))

if __name__ == '__main__':
    unittest.main()
