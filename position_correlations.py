import os
import json
import numpy as np
import pandas as pd
from scipy import stats

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Comprehensive position override mapping for names with suffixes, diacritics, or missing entries
MANUAL_POSITIONS = {
    'Lamont Butler': 'PG', 'Trevon Scott': 'SF', 'Jimmy Butler III': 'SF',
    'Alperen Sengun': 'C', 'Adama Bal': 'SG', 'AJ Green': 'SG',
    'Luka Dončić': 'PG', 'Nikola Jokić': 'C', 'Nikola Joki?': 'C', 'Luka Don?i?': 'PG',
    'Ron Holland': 'SF', 'Ronald Holland II': 'SF', 'Tristan Da Silva': 'SF',
    'Tristan da Silva': 'SF', 'Walter Clayton': 'PG', 'Walter Clayton Jr.': 'PG',
    'Bobby Portis': 'PF', 'Bobby Portis Jr.': 'PF', 'Jonas Valanciunas': 'C',
    'Jonas Valančiūnas': 'C', 'Robert Williams III': 'C', 'Robert Williams': 'C',
    'Nolan Traore': 'PG', 'GG Jackson': 'PF', 'GG Jackson II': 'PF',
    'Terrence Shannon Jr': 'SG', 'Terrence Shannon Jr.': 'SG', 'Xavier Tillman': 'C',
    'Xavier Tillman Sr.': 'C', 'DaRon Holmes II': 'PF', 'Trey Jemison III': 'C',
    'David Jones Garcia': 'SF', 'Dario Saric': 'PF', 'Josh Oduro': 'C',
    'Skal Labissiere': 'C', 'Mouhamadou Gueye': 'PF', 'Tristen Newton': 'PG', 'Hayden Gray': 'PG'
}

TIER_MAP = {
    'PG': 'Guards',
    'SG': 'Guards',
    'SF': 'Wings',
    'PF': 'Wings',
    'C': 'Bigs'
}

METRICS = ['PTS', 'REB', 'AST', 'FG3M', 'PRA']
POSITIONS = ['PG', 'SG', 'SF', 'PF', 'C']
TIERS = ['Guards', 'Wings', 'Bigs']

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

def load_position_map():
    pos_file = os.path.join(BASE_DIR, 'positions.csv')
    pos_map = {}
    if os.path.exists(pos_file):
        pos_df = pd.read_csv(pos_file)
        pos_map = dict(zip(pos_df['Player'], pos_df['Pos']))
    pos_map.update(MANUAL_POSITIONS)
    return pos_map

def calculate_position_correlations(logs_df=None, min_minutes=12.0):
    """
    Computes statistical correlation and performance deltas (vs player season avg & median)
    for each position against every opponent team.
    """
    if logs_df is None:
        logs_path = os.path.join(BASE_DIR, 'logs.csv')
        if not os.path.exists(logs_path):
            raise FileNotFoundError(f"logs.csv not found at {logs_path}")
        logs_df = pd.read_csv(logs_path)

    pos_map = load_position_map()
    
    clean_logs = logs_df.dropna(subset=['PLAYER_NAME', 'MATCHUP']).copy()
    clean_logs['MIN_NUM'] = pd.to_numeric(clean_logs['MIN'], errors='coerce')
    clean_logs = clean_logs[clean_logs['MIN_NUM'] >= min_minutes].copy()

    clean_logs['POS'] = clean_logs['PLAYER_NAME'].map(pos_map).fillna('SF')
    clean_logs['TIER'] = clean_logs['POS'].map(TIER_MAP).fillna('Wings')
    clean_logs['OPP_TEAM'] = clean_logs['MATCHUP'].apply(lambda x: str(x).split()[-1] if pd.notna(x) else None)
    clean_logs['PRA'] = clean_logs['PTS'] + clean_logs['REB'] + clean_logs['AST']

    # Compute player baselines (mean & median) across qualifying games
    for m in METRICS:
        clean_logs[f'{m}_SEASON_AVG'] = clean_logs.groupby('PLAYER_NAME')[m].transform('mean')
        clean_logs[f'{m}_SEASON_MED'] = clean_logs.groupby('PLAYER_NAME')[m].transform('median')
        clean_logs[f'{m}_DIFF_AVG'] = clean_logs[m] - clean_logs[f'{m}_SEASON_AVG']
        clean_logs[f'{m}_DIFF_MED'] = clean_logs[m] - clean_logs[f'{m}_SEASON_MED']

    all_positions = POSITIONS + TIERS
    dataset = {
        'metadata': {
            'min_minutes': min_minutes,
            'qualifying_logs_count': int(len(clean_logs)),
            'metrics': METRICS,
            'positions': POSITIONS,
            'tiers': TIERS,
            'teams_count': int(clean_logs['OPP_TEAM'].nunique())
        },
        'anova': {},
        'cross_position_correlations': {},
        'matchups': []
    }

    # 1. ANOVA Significance per Position & Metric
    for pos in all_positions:
        dataset['anova'][pos] = {}
        df_pos = clean_logs[clean_logs['TIER'] == pos] if pos in TIERS else clean_logs[clean_logs['POS'] == pos]
        
        for m in METRICS:
            team_groups = [group[f'{m}_DIFF_AVG'].values for _, group in df_pos.groupby('OPP_TEAM') if len(group) >= 10]
            if len(team_groups) > 1:
                f_stat, p_val = stats.f_oneway(*team_groups)
                dataset['anova'][pos][m] = {
                    'f_stat': round(float(f_stat), 3),
                    'p_value': float(p_val),
                    'is_significant_05': bool(p_val < 0.05),
                    'is_significant_01': bool(p_val < 0.01),
                    'is_significant_001': bool(p_val < 0.001)
                }
            else:
                dataset['anova'][pos][m] = {
                    'f_stat': 0.0,
                    'p_value': 1.0,
                    'is_significant_05': False,
                    'is_significant_01': False,
                    'is_significant_001': False
                }

    # 2. Team-by-Team Matchup Performance
    for pos in all_positions:
        df_pos = clean_logs[clean_logs['TIER'] == pos] if pos in TIERS else clean_logs[clean_logs['POS'] == pos]
        opponents = sorted(df_pos['OPP_TEAM'].dropna().unique())
        
        for opp in opponents:
            sub = df_pos[df_pos['OPP_TEAM'] == opp]
            n_games = len(sub)
            if n_games < 10:
                continue

            for m in METRICS:
                diff_avg_vals = sub[f'{m}_DIFF_AVG'].values
                diff_med_vals = sub[f'{m}_DIFF_MED'].values
                
                mean_diff = float(np.mean(diff_avg_vals))
                median_diff = float(np.median(diff_med_vals))
                std_diff = float(np.std(diff_avg_vals, ddof=1))
                cohens_d = mean_diff / (std_diff + 1e-9)

                t_res = stats.ttest_1samp(diff_avg_vals, 0.0)
                p_ttest = float(t_res.pvalue)

                # Wilcoxon signed rank test
                try:
                    non_zero = diff_avg_vals[diff_avg_vals != 0]
                    if len(non_zero) >= 6:
                        w_res = stats.wilcoxon(non_zero)
                        p_wilcoxon = float(w_res.pvalue)
                    else:
                        p_wilcoxon = p_ttest
                except Exception:
                    p_wilcoxon = p_ttest

                pct_over_avg = float(np.mean(sub[m] > sub[f'{m}_SEASON_AVG']) * 100.0)
                pct_over_med = float(np.mean(sub[m] > sub[f'{m}_SEASON_MED']) * 100.0)

                # Categorize effect
                if cohens_d >= 0.15:
                    effect_category = 'boosted'
                elif cohens_d <= -0.15:
                    effect_category = 'suppressed'
                else:
                    effect_category = 'neutral'

                dataset['matchups'].append({
                    'position': pos,
                    'is_tier': bool(pos in TIERS),
                    'opponent': opp,
                    'metric': m,
                    'sample_size': n_games,
                    'mean_diff': round(mean_diff, 2),
                    'median_diff': round(median_diff, 2),
                    'std_diff': round(std_diff, 2),
                    'cohens_d': round(cohens_d, 3),
                    'p_val_ttest': p_ttest,
                    'p_val_wilcoxon': p_wilcoxon,
                    'pct_over_avg': round(pct_over_avg, 1),
                    'pct_over_med': round(pct_over_med, 1),
                    'effect_category': effect_category
                })

    # 3. Cross-Position Correlation Matrices
    matchup_df = pd.DataFrame(dataset['matchups'])
    for m in METRICS:
        piv = matchup_df[(matchup_df['metric'] == m) & (matchup_df['position'].isin(POSITIONS))].pivot(
            index='opponent', columns='position', values='mean_diff'
        )
        corr_matrix = piv.corr().round(3).to_dict()
        dataset['cross_position_correlations'][m] = corr_matrix

    # Save to JSON
    out_json = os.path.join(BASE_DIR, 'forensic_position_correlations.json')
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(dataset, f, indent=2, cls=NumpyEncoder)
    print(f"[Positional Engine] Successfully generated {len(dataset['matchups'])} records -> {out_json}")

    return dataset

if __name__ == '__main__':
    calculate_position_correlations()
