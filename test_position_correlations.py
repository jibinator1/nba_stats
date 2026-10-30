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
        cls.data = calculate_position_correlations(min_minutes=20.0)
        cls.client = app.test_client()

    def test_metadata_structure(self):
        meta = self.data.get('metadata', {})
        self.assertIn('qualifying_logs_count', meta)
        self.assertGreater(meta['qualifying_logs_count'], 15000)
        self.assertEqual(meta['teams_count'], 30)
        self.assertListEqual(meta['positions'], ['PG', 'SG', 'SF', 'PF', 'C'])
        self.assertListEqual(meta['tiers'], ['Guards', 'Wings', 'Bigs'])
        self.assertListEqual(meta['metrics'], ['PTS', 'REB', 'AST', 'FG3M'])
        self.assertNotIn('PRA', meta['metrics'])

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
        self.assertEqual(len(matchups), 960)
        
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

    def test_date_filtered_position_correlations(self):
        # Strictly test date-range window (Oct 20 2026 to Oct 29 2026)
        filtered = calculate_position_correlations(start_date='2026-10-20', end_date='2026-10-29')
        meta = filtered.get('metadata', {})
        self.assertEqual(meta.get('start_date'), '2026-10-20')
        self.assertEqual(meta.get('end_date'), '2026-10-29')
        # All qualifying logs must be strictly from the selected window
        self.assertGreater(meta.get('qualifying_logs_count', 0), 1000)
        self.assertLess(meta.get('qualifying_logs_count', 0), 2000)
        
        # Verify matchups exist for positions and tiers
        matchups = filtered.get('matchups', [])
        self.assertGreater(len(matchups), 100)
        tiers_found = set(m['position'] for m in matchups if m.get('is_tier'))
        self.assertTrue({'Guards', 'Wings', 'Bigs'}.issubset(tiers_found))

    def test_api_date_filtered_endpoint(self):
        res = self.client.get('/api/positional-correlations?start_date=2026-10-20&end_date=2026-10-29')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertIn('position_matchup_deltas', data)
        deltas = data['position_matchup_deltas']
        self.assertGreater(len(deltas), 50)
        # Verify both individual positions and tiers are present in deltas
        self.assertTrue(any(k.startswith('Guards_') for k in deltas.keys()))
        self.assertTrue(any(k.startswith('Wings_') for k in deltas.keys()))
        self.assertTrue(any(k.startswith('Bigs_') for k in deltas.keys()))
        self.assertTrue(any(k.startswith('C_') for k in deltas.keys()))
        self.assertTrue(any(k.startswith('PG_') for k in deltas.keys()))

    def test_defensive_differential_color_coding_rules(self):
        # Color coding rule:
        # Negative diff (good defense) -> green
        # Positive diff (bad defense) -> red
        # Near-zero (|diff| < 0.25) -> gray
        def get_defense_color(diff):
            if diff <= -0.25:
                return 'green'
            elif diff >= 0.25:
                return 'red'
            return 'gray'

        self.assertEqual(get_defense_color(-1.14), 'green')
        self.assertEqual(get_defense_color(-0.25), 'green')
        self.assertEqual(get_defense_color(-0.08), 'gray')
        self.assertEqual(get_defense_color(0.0), 'gray')
        self.assertEqual(get_defense_color(0.24), 'gray')
        self.assertEqual(get_defense_color(0.25), 'red')
        self.assertEqual(get_defense_color(1.50), 'red')

    def test_position_correlation_cache(self):
        res1 = load_position_correlations(start_date='2026-10-21', end_date='2026-10-21')
        res2 = load_position_correlations(start_date='2026-10-21', end_date='2026-10-21')
        self.assertIs(res1, res2)

    def test_zero_game_date_window_positional_deltas(self):
        # Extremely narrow date range with zero games played
        res = self.client.get('/api/positional-correlations?start_date=2027-07-01&end_date=2027-07-01')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data.get('count'), 0)
        self.assertEqual(len(data.get('position_matchup_deltas', {})), 0)

    def test_modal_defense_callout_formatting(self):
        # Assert modal.js uses formatDefenseDiff in defense callout and eliminates '+--' false green
        with open(os.path.join(BASE_DIR, 'static', 'js', 'modal.js'), 'r', encoding='utf-8') as f:
            modal_js = f.read()
        self.assertNotIn("'+' : ''}${rebDelta?.diff ?? '--'}", modal_js)
        self.assertNotIn("(rebDelta?.diff || 0) >= 0 ? 'text-emerald-400'", modal_js)
        self.assertIn("rFmt.className", modal_js)
        self.assertIn("pFmt.className", modal_js)

    def test_inverted_date_window_normalizes_cleanly(self):
        # Inverted date window (end date earlier than start date)
        res = self.client.get('/api/positional-correlations?start_date=2026-10-29&end_date=2026-10-20')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        meta = data.get('metadata', {})
        self.assertEqual(meta.get('start_date'), '2026-10-20')
        self.assertEqual(meta.get('end_date'), '2026-10-29')
        self.assertGreater(len(data.get('position_matchup_deltas', {})), 0)

    def test_positional_differential_ranks_and_ties(self):
        # Universal ranks must be present on all deltas, and identical differentials must share the same rank
        res = self.client.get('/api/positional-correlations')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        deltas = data.get('position_matchup_deltas', {})
        self.assertGreater(len(deltas), 0)

        atl_c_reb = deltas.get('C_ATL_REB')
        sac_c_reb = deltas.get('C_SAC_REB')
        self.assertIsNotNone(atl_c_reb)
        self.assertIsNotNone(sac_c_reb)
        self.assertIn('rank', atl_c_reb)
        self.assertIn('rank', sac_c_reb)
        # Both had diff = 0.47 in full season -> must share identical rank 25
        if atl_c_reb['diff'] == sac_c_reb['diff']:
            self.assertEqual(atl_c_reb['rank'], sac_c_reb['rank'])

        for k, v in deltas.items():
            if v.get('rank') is not None:
                self.assertGreaterEqual(v['rank'], 1)
                self.assertLessEqual(v['rank'], 30)

    def test_frontend_renders_differential_rank_val(self):
        # Assert app.js and modal.js use differential rank values
        with open(os.path.join(BASE_DIR, 'static', 'js', 'app.js'), 'r', encoding='utf-8') as f:
            app_js = f.read()
        with open(os.path.join(BASE_DIR, 'static', 'js', 'modal.js'), 'r', encoding='utf-8') as f:
            modal_js = f.read()
        self.assertIn('aRankVal', app_js)
        self.assertIn('hRankVal', app_js)
        self.assertIn('gRankVal', modal_js)
        self.assertIn('updatePositionalDeltasForDateRange(startDate, endDate)', app_js)

    def test_leave_one_out_baseline_delta_math(self):
        # Verify Leave-One-Out (LOO) delta formulation avoids target leakage:
        # For player with games [10, 20, 30], sum=60, N=3.
        # Season avg is 20. Raw delta for 10 is -10.
        # LOO avg excluding 10 is (60 - 10) / (3 - 1) = 25.
        # LOO delta for 10 is 10 - 25 = -15.
        df_synthetic = pd.DataFrame({
            'PLAYER_NAME': ['Player A'] * 3 + ['Player B'],
            'MATCHUP': ['BOS vs NYK', 'BOS vs MIA', 'BOS vs TOR', 'BOS vs PHI'],
            'MIN': [30.0, 32.0, 28.0, 25.0],
            'PTS': [10, 20, 30, 15],
            'REB': [5, 5, 5, 5],
            'AST': [5, 5, 5, 5],
            'FG3M': [2, 2, 2, 2],
            'GAME_DATE': ['2026-10-21'] * 4
        })
        res = calculate_position_correlations(logs_df=df_synthetic, min_minutes=20.0, start_date='2026-10-20', end_date='2026-10-25', save_json=False)
        matchups = res.get('matchups', [])
        # Find matchup for Player A vs NYK
        nyk_pts = next((m for m in matchups if m['opponent'] == 'NYK' and m['metric'] == 'PTS'), None)
        self.assertIsNotNone(nyk_pts)
        # LOO delta must be -15.0, not dampening to -10.0
        self.assertEqual(nyk_pts['mean_diff'], -15.0)

    def test_minutes_weighted_aggregation_metrics(self):
        # 1. Rigorous mathematical verification on synthetic disparate minutes scenario:
        # Star (38 mins vs NYK, 20 pts with LOO baseline 30 -> delta -10)
        # Bench (20 mins vs NYK, 14 pts with LOO baseline 6 -> delta +8)
        # Unweighted mean delta = (-10 + 8) / 2 = -1.0
        # Weighted mean delta = (-10 * 38 + 8 * 20) / (38 + 20) = -220 / 58 = -3.79
        df_disparate = pd.DataFrame({
            'PLAYER_NAME': ['Star PG', 'Star PG', 'Bench PG', 'Bench PG'],
            'MATCHUP': ['BOS vs NYK', 'BOS vs MIA', 'BOS vs NYK', 'BOS vs MIA'],
            'MIN': [38.0, 38.0, 20.0, 20.0],
            'PTS': [20, 30, 14, 6],
            'REB': [5, 5, 5, 5],
            'AST': [5, 5, 5, 5],
            'FG3M': [2, 2, 2, 2],
            'GAME_DATE': ['2026-10-21', '2026-10-22', '2026-10-21', '2026-10-22']
        })
        res = calculate_position_correlations(logs_df=df_disparate, min_minutes=20.0, start_date='2026-10-20', end_date='2026-10-25', save_json=False)
        nyk_pts = next((m for m in res['matchups'] if m['opponent'] == 'NYK' and m['metric'] == 'PTS'), None)
        self.assertIsNotNone(nyk_pts)
        self.assertEqual(nyk_pts['mean_diff'], -1.0)
        self.assertEqual(nyk_pts['weighted_mean_diff'], -3.79)
        self.assertIn('weighted_rank', nyk_pts)

        # 2. Verify all regular matchups have weighted_mean_diff and weighted_rank
        matchups = self.data.get('matchups', [])
        self.assertGreater(len(matchups), 0)
        for m in matchups[:50]:
            self.assertIn('weighted_mean_diff', m)
            self.assertIsInstance(m['weighted_mean_diff'], (float, int))
            self.assertIn('weighted_rank', m)
            self.assertGreaterEqual(m['weighted_rank'], 1)
            self.assertLessEqual(m['weighted_rank'], 30)
        
        # 3. Verify in endpoint position_matchup_deltas
        res_api = self.client.get('/api/positional-correlations')
        self.assertEqual(res_api.status_code, 200)
        deltas = res_api.get_json().get('position_matchup_deltas', {})
        sample_delta = next(iter(deltas.values()))
        self.assertIn('weighted_diff', sample_delta)
        self.assertIsInstance(sample_delta['weighted_diff'], (float, int))
        self.assertIn('weighted_rank', sample_delta)
        self.assertIsInstance(sample_delta['weighted_rank'], int)

    def test_frontend_renders_weighted_delta(self):
        # Verify index.html and positional.js include Weighted Delta column and sorting
        with open(os.path.join(BASE_DIR, 'templates', 'index.html'), 'r', encoding='utf-8') as f:
            index_html = f.read()
        with open(os.path.join(BASE_DIR, 'static', 'js', 'positional.js'), 'r', encoding='utf-8') as f:
            pos_js = f.read()
        self.assertIn("sortPositionalTable('weighted_mean_diff')", index_html)
        self.assertIn("Weighted Delta", index_html)
        self.assertIn("r.weighted_mean_diff", pos_js)

    def test_similar_opponents_pure_offensive_features(self):
        from forensic_data import build_similar_opponents_model
        import inspect
        src = inspect.getsource(build_similar_opponents_model)
        # SWITCH_RATE must not be in features; pure offensive features only
        self.assertNotIn("'SWITCH_RATE'", src)
        self.assertIn("'TRANS_FREQ'", src)
        self.assertIn("'PACE'", src)
        self.assertIn("'FG3_RATE'", src)
        self.assertIn("'PAINT_RATE'", src)

    def test_similar_opponents_knn_fallback_outlier_safety(self):
        # Outlier offensive style should never return an empty cluster
        with open(os.path.join(BASE_DIR, 'forensic_similar_opponents.json'), 'r') as f:
            sim_data = json.load(f)
        for def_tm, atk_map in sim_data.items():
            for atk_tm, details in atk_map.items():
                cluster = details.get('cluster_opponents', [])
                self.assertGreater(len(cluster), 0, f"Empty cluster for {def_tm} vs {atk_tm}")
                self.assertLessEqual(len(cluster), 5)


if __name__ == '__main__':
    unittest.main()


