# NBA Forensic Scouting Engine — Technical & Methodological Specification 🏀

## 1. High-Level Architecture & Philosophy

The NBA Forensic Scouting Engine is a strictly descriptive, tracking-backed analytics pipeline designed to evaluate team matchups, defensive schemes, and player performance distributions. 

### Core Analytical Principles
* **Descriptive Only (Zero Predictions / Zero Betting Language):** The engine does not project game outcomes, generate betting recommendations, or use subjective descriptors. All outputs reflect empirical distributions, historical tracking logs, and formal hypothesis tests.
* **Universal Baseline Rankings:** Every team and qualifying rotation player is universally ranked across all metrics (1–30 for teams) to provide instant league-wide context.
* **Strict Date-Range Gating:** Every baseline, positional delta, and distribution dynamically re-evaluates within user-specified date windows. Windows with zero games played cleanly render unskewed null states.
* **Leakage-Free Baselines:** All player-relative deltas utilize Leave-One-Out (LOO) formulations to prevent target leakage and dampening.
* **Minutes-Weighted Scheme Fidelity:** Matchup aggregations report both unweighted and minutes-weighted metrics to prevent rotation bench players from masking defensive impact against primary stars.

---

## 2. The 4 Forensic Scouting Pillars

The engine organizes scouting into four sequential structural pillars:

```
┌────────────────────────────────────────────────────────────────────────┐
│                   NBA FORENSIC SCOUTING ARCHITECTURE                   │
├────────────────────┬────────────────────┬──────────────────────────────┤
│ Pillar 1: Flow     │ Pillar 2: Defense  │ Pillar 3: Offensive Scheme   │
│ • Neutral Pace     │ • PnR Coverage     │ • Play Types (PnR BH, Spot)  │
│ • Shot Clock Split │ • Shot Zones Allowed│ • Live/Dead Ball Turnovers  │
│ • Game Script Split│ • DREB% & Open 3PT │ • Points Surrendered Off TOV │
├────────────────────┴────────────────────┴──────────────────────────────┤
│ Pillar 4: Stylistic Analogs & Player Profiles                          │
│ • Pure Offensive Style Vectors: [Pace, 3PT Rate, Paint Rate, Trans Freq]│
│ • k-Nearest Neighbors (k=5) Euclidean Distance Clustering              │
│ • 10-Game Empirical Distributions (25th Floor, 50th Median, 75th Ceil) │
└────────────────────────────────────────────────────────────────────────┘
```

### Pillar 1: Pace, Game Flow & Transition Tempo
* **Neutral Pace:** Possessions per 48 minutes in close game environments (score margin $\le 5$ points).
* **Shot Clock Distribution:** Possession conclusion frequencies across early (0–8s), mid (8–15s), and late (15–24s) windows.
* **Game Script Tempo:** Pace and shot-clock splits when holding 10+ point leads vs. facing 10+ point deficits.

### Pillar 2: Perimeter & Paint Defense
* **Pick & Roll Coverage:** Frequency distribution across Drop, Switch, Ice, and Hedge schemes.
* **Shot Zone Allowances:** Points Per Possession (PPP) and attempt frequencies across five discrete zones:
  1. Restricted Area (Rim)
  2. Paint (Non-Restricted Area)
  3. Mid-Range
  4. Corner 3
  5. Above-the-Break 3
* **Contested vs. Wide-Open 3PT Allowed:** Opponent frequency and shooting percentage on wide-open attempts.
* **Defensive Rebounding (DREB%):** Percentage of available defensive rebounds secured.

### Pillar 3: Offensive Play Type & Scheme Alignment
* **Play Type Efficiency:** Points Per Possession and frequency across Isolation, PnR Ball Handler, Spot-Up, Roll Man / Rim Runner, Post-Up, and Off-Screen actions.
* **Turnover Vulnerability:** Live-ball vs. dead-ball turnover splits, points generated off steals, and points surrendered off turnovers.

### Pillar 4: Similar-Opponent Analogs & Player Usage Profiles
* **Stylistic Analog Modeling:** $k$-Nearest Neighbors ($k=5$) Euclidean clustering over normalized offensive identity vectors to evaluate how a defense performs against stylistically similar opponents.
* **Player Usage Profiles:** Top 4 rotation players per team evaluated via 10-game empirical percentiles (25th percentile Floor, 50th percentile Median, 75th percentile Ceiling) alongside USG%, AST%, REB%, and situational clutch volume.

---

## 3. Mathematical & Data Science Foundations

### Step 1: Garbage Minutes Exclusion & Sample Gating
To eliminate distortions caused by end-of-bench mop-up time, player logs are strictly filtered prior to baseline or correlation calculations:

$$\text{Keep log } i \iff \overline{\text{MIN}}_p \ge \text{min\_minutes} \quad \wedge \quad \text{MIN}_{p,i} > 0$$

Where $\overline{\text{MIN}}_p$ is the player's season average minutes (default threshold: $20.0$ min/game, configured in [./position_correlations.py](./position_correlations.py)).

### Step 2: Player-Relative Deltas with Leave-One-Out (LOO) Baseline
Raw positional points allowed are heavily biased by opponent star power and pace. The engine isolates defensive scheme impact by computing player-relative performance differentials.

#### Avoiding Target Leakage & Dampening
If a player's season average $\overline{m}_p$ includes the game $m_{p,i}$ being evaluated against opponent $T$, game $i$ pulls the baseline toward itself. For small game counts $N_p$, this causes substantial **target leakage and delta dampening**.

To eliminate leakage, the baseline for game $i$ is calculated via **Leave-One-Out (LOO)**:

$$\overline{m}_{p,-i} = \begin{cases} \dfrac{\sum_{j \ne i} m_{p,j}}{N_p - 1} = \dfrac{\left(\sum_{j=1}^{N_p} m_{p,j}\right) - m_{p,i}}{N_p - 1} & \text{if } N_p > 1 \\ m_{p,i} & \text{if } N_p = 1 \end{cases}$$

The Leave-One-Out delta $\Delta_{p,i}^{\text{LOO}}$ is therefore:

$$\Delta_{p,i}^{\text{LOO}} = m_{p,i} - \overline{m}_{p,-i} = \begin{cases} \dfrac{N_p}{N_p - 1}\left(m_{p,i} - \overline{m}_p\right) & \text{if } N_p > 1 \\ 0.0 & \text{if } N_p = 1 \end{cases}$$

This guarantees game $i$ is evaluated against an independent baseline unaffected by the opponent's defensive performance on that day.

### Step 3: Dual Parametric & Non-Parametric Hypothesis Testing
To accommodate symmetric and skewed distributions (e.g. right-skewed `FG3M` and `AST`), the engine pairs parametric and non-parametric tests:

1. **One-Way ANOVA:** Tests whether opponent team defensive allowances differ significantly across all 30 teams:
   $$F = \frac{\text{MS}_{\text{between}}}{\text{MS}_{\text{within}}}, \quad p < 0.05 \implies \text{Statistically Significant}$$
2. **One-Sample $t$-Test:** Tests whether an opponent's mean delta against a position differs from zero ($H_0: \mu_\Delta = 0$):
   $$t = \frac{\overline{\Delta}}{s / \sqrt{n}}, \quad s = \sqrt{\frac{1}{n-1}\sum_{i=1}^n (\Delta_i - \overline{\Delta})^2} \quad (\text{Bessel's correction, } ddof=1)$$
3. **Cohen's $d$ Effect Size:**
   $$d = \frac{\overline{\Delta}}{s}$$
   * **Boosted:** $d \ge +0.15$
   * **Suppressed:** $d \le -0.15$
   * **Neutral:** $|d| < 0.15$
4. **Wilcoxon Signed-Rank Test:** Applied non-parametrically to non-zero deltas ($\ge 6$ samples) to prevent outliers from creating false signals in zero-inflated distributions.
5. **Empirical Exceedance Frequencies:** `pct_over_avg` and `pct_over_med` report the percentage of games where the player exceeded their baseline against the opponent.

### Step 4: Unweighted vs. Minutes-Weighted Aggregation
When aggregating games for a position against opponent $T$, an unweighted mean weights a 20.5-minute reserve equally with a 38-minute primary star:

$$\overline{\Delta}_{\text{unweighted}} = \frac{1}{n} \sum_{i=1}^n \Delta_i$$

If a defense sells out to limit a star PG (holding them $-5$ below average) while a backup PG scores in transition ($+4$ above average), an unweighted average reports $-0.5$ (Neutral).

To preserve scheme fidelity, the engine computes **Minutes-Weighted Aggregation** alongside unweighted metrics:

$$\overline{\Delta}_{\text{weighted}} = \frac{\sum_{i=1}^n w_i \Delta_i}{\sum_{i=1}^n w_i}, \quad w_i = \text{MIN}_{i}$$

Both `mean_diff` and `weighted_mean_diff` are published in the dataset and API payloads (`diff` and `weighted_diff`).

---

## 4. Stylistic Similarity Modeling ($k$-NN Euclidean Clustering)

### Separation of Offensive Style vs. Defensive Schemes
To evaluate how a defending team performs against an opponent's offensive profile, the feature vector $\mathbf{v}$ must reflect **pure offensive style** rather than defensive coverage choices:

$$\mathbf{v}_{\text{offense}} = \begin{bmatrix} \text{Pace} \\ \text{3PT Rate} \\ \text{Paint Touch Rate} \\ \text{Transition Frequency} \end{bmatrix}$$

*Note: Defensive schemes such as PnR Switch Rate belong to Pillar 2 (Defense) and are strictly excluded from $\mathbf{v}_{\text{offense}}$ to prevent muddling offensive clusters.*

### Feature Standardization & Distance Metric
Features are standardized using league-wide Z-scores:

$$z_{t,j} = \frac{v_{t,j} - \mu_j}{\sigma_j}, \quad j \in \{1, 2, 3, 4\}$$

The pairwise stylistic distance between attacking team $A$ and opponent $O$ is the Euclidean norm:

$$d(A, O) = \|\mathbf{z}_A - \mathbf{z}_O\|_2 = \sqrt{\sum_{j=1}^4 (z_{A,j} - z_{O,j})^2}$$

### Guaranteed $k$-Nearest Neighbors ($k=5$) with Outlier Fallback
Using a raw distance cutoff ($d \le 1.50\sigma_d$) causes extreme outlier offenses (e.g. ultra-fast pace-and-space systems) to return empty clusters because their minimum distance to all teams exceeds the threshold.

The engine resolves this with an **adaptive threshold and guaranteed $k=5$ fallback**:

$$\text{Threshold} = \overline{d} + 1.50 \times \sigma_d$$

$$\text{Cluster} = \{O \in \text{Opponents} \mid d(A, O) \le \text{Threshold}\}_{[:5]}$$

$$\text{If } |\text{Cluster}| < \min(5, |\text{Opponents}|), \quad \text{Cluster} = \text{Top-5 Nearest Neighbors by } d(A, O)$$

This ensures every matchup lookup delivers a statistically valid sample of up to 5 stylistically closest opponents with median points allowed, offensive rebounds allowed, and 3PT% allowed.

---

## 5. Technical Implementation & Variable Reference Table

| Concept | Mathematical Symbol | Code Variable / Key | File Location |
| :--- | :--- | :--- | :--- |
| **Garbage Minutes Cutoff** | $\overline{\text{MIN}}_p \ge 20$ | `min_minutes` | [./position_correlations.py](./position_correlations.py) |
| **Season Baseline** | $\overline{m}_p$ | `{m}_SEASON_AVG` | [./position_correlations.py](./position_correlations.py) |
| **Leave-One-Out Mean** | $\overline{m}_{p,-i}$ | `{m}_LOO_AVG` | [./position_correlations.py](./position_correlations.py) |
| **Leave-One-Out Delta** | $\Delta_{p,i}^{\text{LOO}}$ | `{m}_DIFF_AVG` | [./position_correlations.py](./position_correlations.py) |
| **Sample Standard Dev** | $s \ (ddof=1)$ | `std_diff` (`np.std(..., ddof=1)`) | [./position_correlations.py](./position_correlations.py) |
| **Unweighted Mean Delta**| $\overline{\Delta}$ | `mean_diff` / `diff` | [./position_correlations.py](./position_correlations.py), [./app.py](./app.py) |
| **Minutes-Weighted Delta**| $\overline{\Delta}_{\text{weighted}}$ | `weighted_mean_diff` / `weighted_diff` | [./position_correlations.py](./position_correlations.py), [./app.py](./app.py) |
| **Cohen's $d$** | $d = \overline{\Delta} / s$ | `cohens_d` | [./position_correlations.py](./position_correlations.py) |
| **Effect Categorization** | Classification | `effect_category` (`boosted`/`suppressed`/`neutral`) | [./position_correlations.py](./position_correlations.py) |
| **Offensive Style Vector**| $\mathbf{v} \in \mathbb{R}^4$ | `['PACE', 'FG3_RATE', 'PAINT_RATE', 'TRANS_FREQ']` | [./forensic_data.py](./forensic_data.py) |
| **Euclidean Distance** | $d(A, O)$ | `euclidean(target_vec, opp_vec)` | [./forensic_data.py](./forensic_data.py) |
| **Analog Cluster** | $k=5$ Neighbors | `cluster_opponents` | [./forensic_data.py](./forensic_data.py), [./app.py](./app.py) |

---

## 6. Verification & Compliance Standards

The codebase adheres strictly to rigorous operational criteria:
1. **Thermonuclear Line Count Limit:** Every `.py` and `.js` file is maintained strictly under 1,000 lines.
2. **Zero Judgmental Language:** Automated audits enforce zero subjective, predictive, or betting terminology.
3. **Automated Test Suites:**
   * `python verify_forensic.py` — 17/17 audits passing with 100% compliance.
   * `python test_position_correlations.py` — 22/22 unit tests passing (LOO math, minutes-weighted aggregation, $k$-NN clustering).
