/**
 * Positional Matchup Lab & Correlation Explorer
 * Displays statistical correlations and allowances (vs player season avg/median).
 */

let positionalData = null;
let currentPosFilter = 'PG';
let currentMetricFilter = 'PTS';
let currentEffectFilter = 'ALL';
let currentSortCol = 'mean_diff';
let currentSortAsc = false;
let positionalSearchTerm = '';

async function loadPositionalTab() {
    window.hasLoadedPositional = true;
    const container = document.getElementById('positional-table-body');
    if (!container) return;

    container.innerHTML = `
        <tr>
            <td colspan="8" class="text-center py-8 text-slate-400">
                <div class="flex items-center justify-center space-x-2">
                    <div class="w-4 h-4 border-2 border-cyan-400 border-t-transparent rounded-full animate-spin"></div>
                    <span>Loading positional matchup dataset...</span>
                </div>
            </td>
        </tr>
    `;

    try {
        const sDate = document.getElementById('input-start-date')?.value || window.startDate;
        const eDate = document.getElementById('input-end-date')?.value || window.endDate;
        const url = (sDate && eDate)
            ? `/api/positional-correlations?start_date=${encodeURIComponent(sDate)}&end_date=${encodeURIComponent(eDate)}`
            : '/api/positional-correlations';
        const res = await fetch(url);
        if (!res.ok) throw new Error('API request failed');
        positionalData = await res.json();
        renderPositionalDashboard();
    } catch (err) {
        console.error('Failed to load positional correlations:', err);
        container.innerHTML = `
            <tr>
                <td colspan="8" class="text-center py-8 text-rose-400">
                    Failed to load positional correlations. Please refresh or run python position_correlations.py.
                </td>
            </tr>
        `;
    }
}

function setPosFilter(pos) {
    currentPosFilter = pos;
    updatePillStyles();
    renderPositionalDashboard();
}

function setMetricFilter(metric) {
    currentMetricFilter = metric;
    updatePillStyles();
    renderPositionalDashboard();
}

function setEffectFilter(effect) {
    currentEffectFilter = effect;
    updatePillStyles();
    renderPositionalDashboard();
}

function onPositionalSearch(term) {
    positionalSearchTerm = term.trim().toUpperCase();
    renderPositionalTable();
}

function sortPositionalTable(col) {
    if (currentSortCol === col) {
        currentSortAsc = !currentSortAsc;
    } else {
        currentSortCol = col;
        currentSortAsc = (col === 'opponent' || col === 'p_val_ttest');
    }
    renderPositionalTable();
}

function updatePillStyles() {
    // Pos pills
    ['PG', 'SG', 'SF', 'PF', 'C', 'Guards', 'Wings', 'Bigs'].forEach(p => {
        const btn = document.getElementById(`pos-btn-${p}`);
        if (btn) {
            btn.className = (currentPosFilter === p)
                ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-cyan-500 text-slate-900 shadow-sm transition'
                : 'px-3 py-1.5 rounded-lg text-xs font-semibold bg-[#21262d] text-slate-300 hover:bg-[#30363d] border border-[#30363d] transition';
        }
    });

    // Metric pills
    ['PTS', 'REB', 'AST', 'FG3M', 'PRA'].forEach(m => {
        const btn = document.getElementById(`metric-btn-${m}`);
        if (btn) {
            btn.className = (currentMetricFilter === m)
                ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-cyan-500 text-slate-900 shadow-sm transition'
                : 'px-3 py-1.5 rounded-lg text-xs font-semibold bg-[#21262d] text-slate-300 hover:bg-[#30363d] border border-[#30363d] transition';
        }
    });

    // Effect filter pills
    ['ALL', 'boosted', 'neutral', 'suppressed'].forEach(e => {
        const btn = document.getElementById(`effect-btn-${e}`);
        if (btn) {
            btn.className = (currentEffectFilter === e)
                ? 'px-2.5 py-1 rounded text-[11px] font-bold bg-slate-200 text-slate-900 transition'
                : 'px-2.5 py-1 rounded text-[11px] font-medium bg-[#21262d] text-slate-400 hover:text-slate-200 border border-[#30363d] transition';
        }
    });
}

function renderPositionalDashboard() {
    if (!positionalData || !positionalData.matchups) return;

    // 1. Render ANOVA banner
    const anovaBlock = document.getElementById('positional-anova-banner');
    if (anovaBlock && positionalData.anova) {
        const anovaInfo = (positionalData.anova[currentPosFilter] && positionalData.anova[currentPosFilter][currentMetricFilter])
            ? positionalData.anova[currentPosFilter][currentMetricFilter]
            : null;

        if (anovaInfo) {
            const isSig = anovaInfo.is_significant_05;
            const pFmt = anovaInfo.p_value < 0.0001 ? anovaInfo.p_value.toExponential(2) : anovaInfo.p_value.toFixed(4);
            const statusClass = isSig ? 'text-emerald-400 bg-emerald-950/40 border-emerald-800/60' : 'text-amber-400 bg-amber-950/40 border-amber-800/60';
            const statusText = isSig ? 'STATISTICALLY SIGNIFICANT MATCHUP DEPENDENCY' : 'MATCHUP RESILIENT / NO UNIFORM DEFENSIVE EFFECT';

            anovaBlock.innerHTML = `
                <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 p-3.5 rounded-xl border ${statusClass}">
                    <div class="flex items-center space-x-3">
                        <div class="w-8 h-8 rounded-lg bg-[#161b22] border border-[#30363d] flex items-center justify-center font-mono font-bold text-xs text-cyan-400">
                            F
                        </div>
                        <div>
                            <div class="text-xs font-bold text-slate-200 flex items-center gap-2">
                                <span>One-Way ANOVA Across 30 Teams (${currentPosFilter} • ${currentMetricFilter})</span>
                                <span class="px-2 py-0.5 rounded text-[10px] font-mono font-bold border ${statusClass}">${statusText}</span>
                            </div>
                            <div class="text-[11px] text-slate-400 mt-0.5">
                                Tests whether opponent defenses create real systematic variance in ${currentPosFilter} ${currentMetricFilter} vs season baselines.
                            </div>
                        </div>
                    </div>
                    <div class="flex items-center space-x-4 text-xs font-mono">
                        <div><span class="text-slate-400 text-[11px]">F-Stat:</span> <strong class="text-slate-200">${anovaInfo.f_stat}</strong></div>
                        <div><span class="text-slate-400 text-[11px]">p-value:</span> <strong class="text-slate-200">${pFmt}</strong></div>
                    </div>
                </div>
            `;
        }
    }

    renderPositionalTable();
    renderCrossPositionCorrelations();
}

function renderPositionalTable() {
    const tbody = document.getElementById('positional-table-body');
    if (!tbody || !positionalData) return;

    let rows = positionalData.matchups.filter(m =>
        m.position.toUpperCase() === currentPosFilter.toUpperCase() &&
        m.metric.toUpperCase() === currentMetricFilter.toUpperCase()
    );

    if (currentEffectFilter !== 'ALL') {
        rows = rows.filter(m => m.effect_category === currentEffectFilter);
    }

    if (positionalSearchTerm) {
        rows = rows.filter(m => m.opponent.includes(positionalSearchTerm));
    }

    // Update Counts Badges
    const allForCat = positionalData.matchups.filter(m =>
        m.position.toUpperCase() === currentPosFilter.toUpperCase() &&
        m.metric.toUpperCase() === currentMetricFilter.toUpperCase()
    );
    const boostedCount = allForCat.filter(m => m.effect_category === 'boosted').length;
    const neutralCount = allForCat.filter(m => m.effect_category === 'neutral').length;
    const suppCount = allForCat.filter(m => m.effect_category === 'suppressed').length;

    const bCountEl = document.getElementById('count-boosted');
    const nCountEl = document.getElementById('count-neutral');
    const sCountEl = document.getElementById('count-suppressed');
    if (bCountEl) bCountEl.textContent = boostedCount;
    if (nCountEl) nCountEl.textContent = neutralCount;
    if (sCountEl) sCountEl.textContent = suppCount;

    // Sorting
    rows.sort((a, b) => {
        let valA = a[currentSortCol];
        let valB = b[currentSortCol];
        if (typeof valA === 'string') {
            return currentSortAsc ? valA.localeCompare(valB) : valB.localeCompare(valA);
        }
        return currentSortAsc ? (valA - valB) : (valB - valA);
    });

    if (rows.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="8" class="text-center py-8 text-slate-400">
                    No matchup data found matching current filters.
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = rows.map((r, idx) => {
        const isBoosted = r.effect_category === 'boosted';
        const isSuppressed = r.effect_category === 'suppressed';
        const badgeClass = isBoosted
            ? 'bg-emerald-950/60 text-emerald-300 border-emerald-800/70'
            : isSuppressed
                ? 'bg-rose-950/60 text-rose-300 border-rose-800/70'
                : 'bg-[#21262d] text-slate-400 border-[#30363d]';
        const badgeLabel = isBoosted ? 'BOOST' : isSuppressed ? 'SUPPRESS' : 'NEUTRAL';

        const meanSign = r.mean_diff > 0 ? '+' : '';
        const medSign = r.median_diff > 0 ? '+' : '';
        const meanClass = r.mean_diff > 0.3 ? 'text-emerald-400 font-bold' : r.mean_diff < -0.3 ? 'text-rose-400 font-bold' : 'text-slate-300';
        const medClass = r.median_diff > 0.3 ? 'text-emerald-400 font-bold' : r.median_diff < -0.3 ? 'text-rose-400 font-bold' : 'text-slate-300';

        // Stars for p-value significance
        let stars = '';
        if (r.p_val_ttest < 0.001) stars = '★★★';
        else if (r.p_val_ttest < 0.01) stars = '★★';
        else if (r.p_val_ttest < 0.05) stars = '★';
        else stars = 'ns';

        const pFmt = r.p_val_ttest < 0.001 ? r.p_val_ttest.toExponential(1) : r.p_val_ttest.toFixed(3);

        return `
            <tr class="hover:bg-[#1f242c] transition border-b border-[#30363d]/40">
                <td class="py-2.5 px-3 font-mono text-center text-slate-400 text-xs">${idx + 1}</td>
                <td class="py-2.5 px-3">
                    <div class="flex items-center space-x-2.5">
                        <span class="w-7 h-7 rounded bg-[#21262d] border border-[#30363d] flex items-center justify-center font-mono font-bold text-xs text-slate-200">
                            ${r.opponent}
                        </span>
                        <span class="text-xs font-semibold text-slate-200">${window.NBA_TEAM_NAMES ? (window.NBA_TEAM_NAMES[r.opponent] || r.opponent) : r.opponent}</span>
                    </div>
                </td>
                <td class="py-2.5 px-3 text-center">
                    <span class="px-2 py-0.5 rounded text-[10px] font-mono font-bold border ${badgeClass}">
                        ${badgeLabel}
                    </span>
                </td>
                <td class="py-2.5 px-3 text-right font-mono ${meanClass}">
                    ${meanSign}${r.mean_diff.toFixed(2)}
                </td>
                <td class="py-2.5 px-3 text-right font-mono ${medClass}">
                    ${medSign}${r.median_diff.toFixed(1)}
                </td>
                <td class="py-2.5 px-3 text-right font-mono text-slate-300">
                    ${r.pct_over_med.toFixed(1)}%
                </td>
                <td class="py-2.5 px-3 text-right font-mono text-slate-300">
                    ${r.cohens_d > 0 ? '+' : ''}${r.cohens_d.toFixed(2)}
                </td>
                <td class="py-2.5 px-3 text-right font-mono text-xs">
                    <span class="${r.p_val_ttest < 0.05 ? 'text-cyan-400 font-bold' : 'text-slate-500'}">${pFmt}</span>
                    <span class="text-[10px] text-amber-400 font-semibold ml-1">${stars}</span>
                </td>
                <td class="py-2.5 px-3 text-center font-mono text-slate-400 text-xs">
                    ${r.sample_size}
                </td>
            </tr>
        `;
    }).join('');
}

function renderCrossPositionCorrelations() {
    const card = document.getElementById('cross-position-matrix-card');
    if (!card || !positionalData || !positionalData.cross_position_correlations) return;

    const matrix = positionalData.cross_position_correlations[currentMetricFilter];
    if (!matrix) {
        card.classList.add('hidden');
        return;
    }
    card.classList.remove('hidden');

    const positions = ['PG', 'SG', 'SF', 'PF', 'C'];
    let html = `
        <div class="overflow-x-auto">
            <table class="w-full text-center text-xs font-mono border-collapse">
                <thead>
                    <tr class="border-b border-[#30363d] text-slate-400">
                        <th class="py-2 px-3 text-left">Pos</th>
                        ${positions.map(p => `<th class="py-2 px-3">${p}</th>`).join('')}
                    </tr>
                </thead>
                <tbody class="divide-y divide-[#30363d]/50">
    `;

    positions.forEach(p1 => {
        html += `<tr><td class="py-2 px-3 text-left font-bold text-slate-200">${p1}</td>`;
        positions.forEach(p2 => {
            const val = matrix[p1] ? matrix[p1][p2] : null;
            if (val === null || val === undefined) {
                html += `<td class="py-2 px-3 text-slate-500">-</td>`;
            } else {
                let colorClass = 'text-slate-400';
                if (val >= 0.6) colorClass = 'text-emerald-400 font-bold bg-emerald-950/20';
                else if (val >= 0.4) colorClass = 'text-cyan-400';
                else if (val <= 0.2) colorClass = 'text-slate-500';
                html += `<td class="py-2 px-3 ${colorClass}">${val.toFixed(2)}</td>`;
            }
        });
        html += `</tr>`;
    });

    html += `</tbody></table></div>`;
    const container = document.getElementById('cross-matrix-content');
    if (container) container.innerHTML = html;
}
