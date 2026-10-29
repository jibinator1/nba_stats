import os
import json
import warnings
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

def calculate_position_correlations(logs_df=None, min_minutes=12.0, start_date=None, end_date=None, save_json=None):
    """
    Computes statistical correlation and performance deltas (vs player season avg & median)
    for each position against every opponent team. When start_date or end_date is provided,
    baselines and differentials are strictly calculated within that date window.
    """
    if save_json is None:
        save_json = bool(start_date is None and end_date is None)

    if logs_df is None:
        logs_path = os.path.join(BASE_DIR, 'logs.csv')
        if not os.path.exists(logs_path):
            raise FileNotFoundError(f"logs.csv not found at {logs_path}")
        logs_df = pd.read_csv(logs_path)

    pos_map = load_position_map()
    
    clean_logs = logs_df.dropna(subset=['PLAYER_NAME', 'MATCHUP']).copy()
    clean_logs['MIN_NUM'] = pd.to_numeric(clean_logs['MIN'], errors='coerce')
    clean_logs = clean_logs[clean_logs['MIN_NUM'] >= min_minutes].copy()

    # Filter strictly to selected date range before computing any averages or deltas
    if start_date and end_date and str(start_date) > str(end_date):
        start_date, end_date = end_date, start_date
    if start_date or end_date:
        clean_logs['GAME_DATE_STR'] = clean_logs['GAME_DATE'].astype(str).str[:10]
        if start_date:
            clean_logs = clean_logs[clean_logs['GAME_DATE_STR'] >= str(start_date)]
        if end_date:
            clean_logs = clean_logs[clean_logs['GAME_DATE_STR'] <= str(end_date)]

    clean_logs['POS'] = clean_logs['PLAYER_NAME'].map(pos_map).fillna('SF')
    clean_logs['TIER'] = clean_logs['POS'].map(TIER_MAP).fillna('Wings')
    clean_logs['OPP_TEAM'] = clean_logs['MATCHUP'].apply(lambda x: str(x).split()[-1] if pd.notna(x) else None)
    clean_logs['PRA'] = clean_logs['PTS'] + clean_logs['REB'] + clean_logs['AST']

    # Compute player baselines (mean & median) strictly across qualifying games in window
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
            'teams_count': int(clean_logs['OPP_TEAM'].nunique()) if not clean_logs.empty else 0,
            'start_date': start_date,
            'end_date': end_date
        },
        'anova': {},
        'cross_position_correlations': {},
        'matchups': []
    }

    min_anova_games = 2 if (start_date or end_date) else 10
    min_opp_games = 1 if (start_date or end_date) else 10

    # 1. ANOVA Significance per Position & Metric
    for pos in all_positions:
        dataset['anova'][pos] = {}
        df_pos = clean_logs[clean_logs['TIER'] == pos] if pos in TIERS else clean_logs[clean_logs['POS'] == pos]
        
        for m in METRICS:
            team_groups = [group[f'{m}_DIFF_AVG'].values for _, group in df_pos.groupby('OPP_TEAM') if len(group) >= min_anova_games]
            if len(team_groups) > 1:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        f_stat, p_val = stats.f_oneway(*team_groups)
                    if np.isnan(f_stat) or np.isnan(p_val):
                        f_stat, p_val = 0.0, 1.0
                except Exception:
                    f_stat, p_val = 0.0, 1.0
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
            if n_games < min_opp_games:
                continue

            for m in METRICS:
                diff_avg_vals = sub[f'{m}_DIFF_AVG'].values
                diff_med_vals = sub[f'{m}_DIFF_MED'].values
                
                mean_diff = float(np.mean(diff_avg_vals)) if len(diff_avg_vals) > 0 else 0.0
                median_diff = float(np.median(diff_med_vals)) if len(diff_med_vals) > 0 else 0.0
                std_diff = float(np.std(diff_avg_vals, ddof=1)) if len(diff_avg_vals) > 1 else 0.0
                cohens_d = mean_diff / (std_diff + 1e-9)

                if len(diff_avg_vals) > 1:
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter('ignore')
                            t_res = stats.ttest_1samp(diff_avg_vals, 0.0)
                        p_ttest = float(t_res.pvalue) if not np.isnan(t_res.pvalue) else 1.0
                    except Exception:
                        p_ttest = 1.0
                else:
                    p_ttest = 1.0

                # Wilcoxon signed rank test
                try:
                    non_zero = diff_avg_vals[diff_avg_vals != 0]
                    if len(non_zero) >= 6:
                        with warnings.catch_warnings():
                            warnings.simplefilter('ignore')
                            w_res = stats.wilcoxon(non_zero)
                        p_wilcoxon = float(w_res.pvalue) if not np.isnan(w_res.pvalue) else p_ttest
                    else:
                        p_wilcoxon = p_ttest
                except Exception:
                    p_wilcoxon = p_ttest

                pct_over_avg = float(np.mean(sub[m] > sub[f'{m}_SEASON_AVG']) * 100.0) if len(sub) > 0 else 50.0
                pct_over_med = float(np.mean(sub[m] > sub[f'{m}_SEASON_MED']) * 100.0) if len(sub) > 0 else 50.0

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

    # 2b. Compute universal ranks for each (position, metric) across all opponents
    # Lower mean_diff = better defense (suppression) -> rank 1 is lowest mean_diff
    matchup_groups = {}
    for item in dataset['matchups']:
        grp_key = (item['position'], item['metric'])
        if grp_key not in matchup_groups:
            matchup_groups[grp_key] = []
        matchup_groups[grp_key].append(item)

    for (pos, metric), group in matchup_groups.items():
        group.sort(key=lambda x: x['mean_diff'])
        prev_val = None
        prev_rank = 1
        for i, item in enumerate(group):
            val = item['mean_diff']
            if prev_val is not None and val == prev_val:
                item['rank'] = prev_rank
            else:
                item['rank'] = i + 1
                prev_rank = i + 1
                prev_val = val

    # 3. Cross-Position Correlation Matrices
    matchup_df = pd.DataFrame(dataset['matchups'])
    if not matchup_df.empty:
        for m in METRICS:
            m_sub = matchup_df[(matchup_df['metric'] == m) & (matchup_df['position'].isin(POSITIONS))]
            if not m_sub.empty and m_sub['position'].nunique() > 1:
                try:
                    piv = m_sub.pivot(index='opponent', columns='position', values='mean_diff')
                    corr_matrix = piv.corr().round(3).to_dict()
                    dataset['cross_position_correlations'][m] = corr_matrix
                except Exception:
                    dataset['cross_position_correlations'][m] = {}

    # Save to JSON only if requested
    if save_json:
        out_json = os.path.join(BASE_DIR, 'forensic_position_correlations.json')
        with open(out_json, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, indent=2, cls=NumpyEncoder)
        print(f"[Positional Engine] Successfully generated {len(dataset['matchups'])} records -> {out_json}")

    return dataset

if __name__ == '__main__':
    calculate_position_correlations()
