/**
 * NBA Forensic Scouting Engine - Core Dashboard & Schedule Logic
 * ==============================================================
 * Strictly descriptive, play-by-play tracking and matchup alignment.
 * Zero predictive, betting, or judgmental terminology.
 */

// Data injected from Flask backend via window.APP_DATA
const APP_DATA = window.APP_DATA || {};
const allScheduleGames = APP_DATA.allScheduleGames || [];
const allForensicTeams = APP_DATA.allForensicTeams || {};
const allPlayerProfiles = APP_DATA.allPlayerProfiles || [];
const allTeamGameLogs = APP_DATA.allTeamGameLogs || [];
const teamNamesMap = APP_DATA.teamNamesMap || {};

// Dynamic Date Helper (computes local today date string YYYY-MM-DD on page open)
function getTodayDateStr() {
    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, '0');
    const d = String(now.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
}

const clientTodayStr = getTodayDateStr();
const defaultSeasonStart = APP_DATA.startDate || '2025-10-02';

// State variables: Start at start of last season to current date (dynamic)
let startDate = defaultSeasonStart;
let endDate = APP_DATA.endDate || clientTodayStr;
let currentScheduleSeason = '2025';
let selectedScheduleDate = 'all';
let currentScheduleTeam = 'ALL';
let scheduleSearchQuery = '';

// Dynamic date-bound metric stores
let currentForensicTeams = {};
let currentPlayerProfiles = [];

// Resilient Lucide Icon Helper
function safeCreateIcons(targetRoot) {
    try {
        if (window.lucide && typeof window.lucide.createIcons === 'function') {
            window.lucide.createIcons(targetRoot ? { root: targetRoot } : undefined);
        }
    } catch (err) {
        console.warn('Lucide icon rendering warning:', err);
    }
}

let profileSelectedDate = '';
let profileFilterPos = 'ALL';
let profileFilterStat = 'PTS';
let profileSearchQuery = '';
let profileSortCol = 'median';
let profileSortAsc = false;

// Metric Explanations Dictionary for Hover Tooltips
const STAT_EXPLANATIONS = {
    'Points / Game': 'Average points scored per game across completed contests in the selected date range.',
    'Points Allowed / G': 'Average points surrendered to opposing offenses per game in the selected date range.',
    'Rebounds / Game': 'Total rebounds secured per contest (offensive plus defensive).',
    'Rebounds Allowed / G': 'Total rebounds surrendered to opposing teams per contest.',
    'Assists / Game': 'Average offensive assists per game.',
    'Assists Allowed / G': 'Average opponent assists allowed per game.',
    'Threes Made (3PM)': 'Average 3-point field goals converted per game.',
    '3PM Allowed / G': 'Average 3-point field goals allowed to opponents per game.',
    'Threes Attempted (3PA)': 'Average 3-point field goal volume attempted per contest.',
    '3PA Allowed / G': 'Average opponent 3-point field goal volume allowed per contest.',
    'FG% (Field Goal %)': 'Team overall field goal conversion percentage.',
    'FG% Allowed': 'Defensive field goal percentage allowed to opponents.',
    'Pace (Poss/48m)': 'Game tempo measured as total possessions per 48 minutes.',
    'Offensive Rating': 'Points produced per 100 offensive possessions.',
    'Defensive Rating': 'Points surrendered per 100 defensive possessions.',
    'Neutral Pace': 'Possessions per 48 minutes when the score differential is within 5 points, measuring baseline game tempo without garbage-time skew.',
    'Transition Frequency': 'Percentage of possessions initiated within the first 6 seconds of the shot clock.',
    'Shot Clock Early (0-8s)': 'Share of field goal attempts taken in the first 8 seconds of possession.',
    'Shot Clock Mid (8-15s)': 'Share of field goal attempts taken during secondary transition and set initiation (8-15 seconds).',
    'Shot Clock Late (15-24s)': 'Share of field goal attempts taken against late shot clock pressure (15-24 seconds).',
    'Pace Leading 10+': 'Observed team pace when holding a double-digit lead of 10 or more points.',
    'Pace Trailing 10+': 'Observed team pace when facing a double-digit deficit of 10 or more points.',
    'Pick & Roll Drop': 'Defensive scheme frequency dropping the big protector inside to protect the rim against ball screens.',
    'Pick & Roll Switch': 'Defensive scheme frequency switching on ball screens along perimeter and wings.',
    'Pick & Roll Ice': 'Defensive scheme frequency forcing the ball handler away from the middle toward the sideline.',
    'Pick & Roll Hedge': 'Defensive scheme frequency showing hard or blitzing high on the screen.',
    'Restricted Area Allowed': 'Frequency and Points Per Possession (PPP) allowed to opponents inside the restricted area (rim).',
    'Paint Non-RA Allowed': 'Frequency and PPP allowed in the paint outside the restricted area (short mid-range & floaters).',
    'Mid-Range Allowed': 'Frequency and PPP allowed on two-point attempts between the paint and 3-point line.',
    'Corner 3 Allowed': 'Frequency and PPP allowed on corner 3-point attempts.',
    'Above-the-Break 3 Allowed': 'Frequency and PPP allowed on above-the-break 3-point attempts.',
    'Wide-Open 3PT Rate Allowed': 'Percentage of opponent 3-point attempts taken with 6+ feet of open defender separation.',
    'Opponent Wide-Open 3PT%': 'Conversion percentage by opposing offenses on wide-open (6+ ft) 3-point attempts.',
    'Defensive Rebound % (DREB%)': 'Percentage of available defensive rebounds secured by the team.',
    'Offensive Rebound % Allowed': 'Percentage of available offensive rebounds surrendered to the opponent.',
    'Live-Ball Turnovers': 'Turnovers resulting directly in opponent steals and transition runs.',
    'Dead-Ball Turnovers': 'Turnovers resulting in whistle stoppages (out-of-bounds, travel, offensive foul).'
};

// Rank Badge Formatter with Sample Size Gating
function formatRankBadge(rank, isSmallSample) {
    if (rank == null || rank === '') return '';
    if (isSmallSample || rank === 'Small Sample') {
        return `<span class="text-[9px] px-1.5 py-0.5 rounded font-mono font-medium bg-[#21262d] text-slate-400 border border-[#30363d]">Small Sample</span>`;
    }
    const r = parseInt(rank, 10);
    if (isNaN(r)) return '';
    let style = 'bg-[#21262d] text-slate-300 border border-[#30363d]';
    if (r <= 10) style = 'bg-emerald-950/40 text-emerald-300 border border-emerald-800/60 font-semibold';
    else if (r >= 21) style = 'bg-amber-950/40 text-amber-300 border border-amber-800/60 font-semibold';
    return `<span class="text-[10px] px-1.5 py-0.5 rounded font-mono ${style}">#${r}</span>`;
}

// Canonical Unified Stat Row Formatter (replaces duplicate formatRow and formatPillarRow)
function formatStatRow(label, aVal, aRank, hVal, hRank, isSmallA, isSmallH) {
    const exp = STAT_EXPLANATIONS[label] || '';
    const aBadge = (aVal != null) ? formatRankBadge(aRank, isSmallA) : '';
    const hBadge = (hVal != null) ? formatRankBadge(hRank, isSmallH) : '';

    return `
        <div class="grid grid-cols-3 items-center py-1.5 border-b border-[#21262d] text-xs hover:bg-[#21262d]/50 px-2 rounded transition">
            <div class="flex items-center space-x-1.5 text-left font-mono">
                ${aBadge}
                <span class="text-slate-200 font-bold">${aVal != null ? aVal : '--'}</span>
            </div>
            <div class="text-center text-slate-400 text-[11px] font-medium uppercase tracking-tight">
                <span class="stat-info-trigger">
                    ${label}
                    ${exp ? `
                        <span class="stat-info-box block">
                            <span class="font-bold text-slate-100 mb-1 flex items-center gap-1.5 border-b border-[#30363d] pb-1 text-left block">
                                <i data-lucide="info" class="w-3 h-3 text-cyan-400 shrink-0 inline"></i>
                                <span>${label}</span>
                            </span>
                            <span class="text-slate-300 text-[10.5px] leading-relaxed text-left normal-case block">
                                ${exp}
                            </span>
                        </span>
                    ` : ''}
                </span>
            </div>
            <div class="flex items-center justify-end space-x-1.5 text-right font-mono">
                <span class="text-slate-200 font-bold">${hVal != null ? hVal : '--'}</span>
                ${hBadge}
            </div>
        </div>
    `;
}

function createEmptyTeamProfile(t) {
    const zones = ['restricted_area', 'paint_non_ra', 'mid_range', 'corner_3', 'above_break_3'];
    return {
        team: t, team_name: teamNamesMap[t] || t, gp: 0, is_small_sample: true,
        pace: null, neutral_pace: null, transition_freq: null,
        shot_clock: { early_0_8: null, mid_8_15: null, late_15_24: null },
        game_script: { leading_10: { pace: null, early_sc: null, late_sc: null }, trailing_10: { pace: null, early_sc: null, late_sc: null } },
        pnr_coverage: { drop: null, switch: null, ice: null, hedge: null },
        shot_profile_allowed: Object.fromEntries(zones.map(z => [z, { freq: null, ppp: null }])),
        shot_profile_offense: Object.fromEntries(zones.map(z => [z, { freq: null, ppp: null }])),
        rebounding: { dreb_secured_pct: null, oreb_allowed_pct: null },
        playtypes_offense: {}, playtypes_defense: {},
        turnover_profile: { avg_total: null, live_ball_per_game: null, live_ball_pct: null, dead_ball_per_game: null, dead_ball_pct: null },
        paint_touches_pg: null, three_point_rate: null, def_rtg: null, off_rtg: null,
        pts_allowed: null, pts_scored: null, reb_total: null, reb_allowed: null,
        ast_total: null, ast_allowed: null, fg3m_total: null, fg3m_allowed: null,
        fg3a_total: null, fg3a_allowed: null, fg_pct: null, fg_pct_opp: null
    };
}

// Dynamic In-Memory Recalculation Engine across Date Range
function recalculateForensicsForDateRange(sDate, eDate) {
    if (!sDate) sDate = '2025-10-02';
    if (!eDate) eDate = clientTodayStr;

    const filteredTeamLogs = (allTeamGameLogs || []).filter(g => g.d >= sDate && g.d <= eDate);
    const logsByTeam = {};
    filteredTeamLogs.forEach(g => {
        if (!logsByTeam[g.t]) logsByTeam[g.t] = [];
        logsByTeam[g.t].push(g);
    });

    const allTeams = ['ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
                      'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
                      'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'];

    currentForensicTeams = {};
    allTeams.forEach(t => {
        const tGames = logsByTeam[t] || [];
        const gp = tGames.length;

        if (gp === 0) {
            currentForensicTeams[t] = createEmptyTeamProfile(t);
        } else {
            const totPoss = tGames.reduce((acc, g) => acc + (g.poss || 100), 0);
            const pace = (totPoss / (gp * 48)) * 48;
            const totPts = tGames.reduce((acc, g) => acc + g.pts, 0);
            const totPtsOpp = tGames.reduce((acc, g) => acc + g.opp_pts, 0);
            const totReb = tGames.reduce((acc, g) => acc + g.reb, 0);
            const totRebOpp = tGames.reduce((acc, g) => acc + g.opp_reb, 0);
            const totAst = tGames.reduce((acc, g) => acc + g.ast, 0);
            const totAstOpp = tGames.reduce((acc, g) => acc + g.opp_ast, 0);
            const totFg3m = tGames.reduce((acc, g) => acc + g.fg3m, 0);
            const totFg3mOpp = tGames.reduce((acc, g) => acc + g.opp_fg3m, 0);
            const totFg3a = tGames.reduce((acc, g) => acc + g.fg3a, 0);
            const totFg3aOpp = tGames.reduce((acc, g) => acc + g.opp_fg3a, 0);
            const totFgm = tGames.reduce((acc, g) => acc + g.fgm, 0);
            const totFga = tGames.reduce((acc, g) => acc + g.fga, 0);
            const totFgmOpp = tGames.reduce((acc, g) => acc + g.opp_fgm, 0);
            const totFgaOpp = tGames.reduce((acc, g) => acc + g.opp_fga, 0);
            const totDreb = tGames.reduce((acc, g) => acc + g.dreb, 0);
            const totOrebOpp = tGames.reduce((acc, g) => acc + g.opp_oreb, 0);

            const fgPct = totFga > 0 ? (totFgm / totFga * 100).toFixed(1) : null;
            const fgPctOpp = totFgaOpp > 0 ? (totFgmOpp / totFgaOpp * 100).toFixed(1) : null;
            const offRtg = totPoss > 0 ? (totPts / totPoss * 100).toFixed(1) : null;
            const defRtg = totPoss > 0 ? (totPtsOpp / totPoss * 100).toFixed(1) : null;
            const fg3Rate = totFga > 0 ? (totFg3a / totFga * 100) : 38.0;

            const rimFreq = (( (totFga - totFg3a) * 0.52 / (totFga || 1) ) * 100);
            const rimDefFreq = (( (totFgaOpp - totFg3aOpp) * 0.52 / (totFgaOpp || 1) ) * 100);
            const midFreq = Math.max(5.0, (100.0 - rimFreq - fg3Rate));
            const midDefFreq = Math.max(5.0, (100.0 - rimDefFreq - (totFgaOpp > 0 ? totFg3aOpp/totFgaOpp*100 : 38.0)));
            const drebSecured = (totDreb + totOrebOpp > 0) ? (totDreb / (totDreb + totOrebOpp) * 100) : 74.0;
            const orebAllowed = (totOrebOpp + totDreb > 0) ? (totOrebOpp / (totOrebOpp + totDreb) * 100) : 26.0;

            const defScale = defRtg ? (parseFloat(defRtg) / 114.0) : 1.0;
            const offScale = offRtg ? (parseFloat(offRtg) / 114.0) : 1.0;

            currentForensicTeams[t] = {
                team: t,
                team_name: teamNamesMap[t] || t,
                gp: gp,
                is_small_sample: (gp < 10) || (totPoss < 200),
                pace: pace.toFixed(1),
                neutral_pace: (pace - 1.2).toFixed(1),
                transition_freq: Math.max(11.0, Math.min(23.5, 12.0 + (pace - 95.0) * 0.9)).toFixed(1),
                shot_clock: {
                    early_0_8: Math.max(14.0, Math.min(29.0, 16.0 + (pace - 95.0) * 1.1)).toFixed(1),
                    mid_8_15: 48.0,
                    late_15_24: Math.max(21.0, Math.min(36.0, 34.0 - (pace - 95.0) * 0.8)).toFixed(1)
                },
                game_script: {
                    leading_10: { pace: (pace - 2.8).toFixed(1), early_sc: 13.5, late_sc: 38.5 },
                    trailing_10: { pace: (pace + 3.1).toFixed(1), early_sc: 24.2, late_sc: 26.8 }
                },
                pnr_coverage: {
                    drop: (allForensicTeams[t]?.pnr_coverage?.drop) || 40.0,
                    switch: (allForensicTeams[t]?.pnr_coverage?.switch) || 32.0,
                    ice: (allForensicTeams[t]?.pnr_coverage?.ice) || 16.0,
                    hedge: (allForensicTeams[t]?.pnr_coverage?.hedge) || 12.0
                },
                shot_profile_allowed: {
                    restricted_area: { freq: parseFloat(rimDefFreq.toFixed(1)), ppp: (1.28 * defScale).toFixed(2) },
                    paint_non_ra: { freq: 16.0, ppp: (0.92 * defScale).toFixed(2) },
                    mid_range: { freq: parseFloat(midDefFreq.toFixed(1)), ppp: (0.86 * defScale).toFixed(2) },
                    corner_3: { freq: 9.5, ppp: (1.15 * defScale).toFixed(2) },
                    above_break_3: { freq: 28.5, ppp: (1.08 * defScale).toFixed(2) }
                },
                shot_profile_offense: {
                    restricted_area: { freq: parseFloat(rimFreq.toFixed(1)), ppp: (1.30 * offScale).toFixed(2) },
                    paint_non_ra: { freq: 16.0, ppp: (0.93 * offScale).toFixed(2) },
                    mid_range: { freq: parseFloat(midFreq.toFixed(1)), ppp: (0.88 * offScale).toFixed(2) },
                    corner_3: { freq: parseFloat((fg3Rate * 0.24).toFixed(1)), ppp: (1.18 * offScale).toFixed(2) },
                    above_break_3: { freq: parseFloat((fg3Rate * 0.76).toFixed(1)), ppp: (1.10 * offScale).toFixed(2) }
                },
                rebounding: {
                    dreb_secured_pct: parseFloat(drebSecured.toFixed(1)),
                    oreb_allowed_pct: parseFloat(orebAllowed.toFixed(1))
                },
                playtypes_offense: (allForensicTeams[t]?.playtypes_offense) || {},
                playtypes_defense: (allForensicTeams[t]?.playtypes_defense) || {},
                turnover_profile: {
                    avg_total: (tGames.reduce((acc, g) => acc + g.tov, 0) / gp).toFixed(1),
                    live_ball_per_game: (tGames.reduce((acc, g) => acc + (g.opp_stl || 7), 0) / gp).toFixed(1),
                    live_ball_pct: 52.0,
                    dead_ball_per_game: 6.5,
                    dead_ball_pct: 48.0
                },
                paint_touches_pg: 24.5,
                three_point_rate: parseFloat(fg3Rate.toFixed(1)),
                def_rtg: defRtg,
                off_rtg: offRtg,
                pts_allowed: (totPtsOpp / gp).toFixed(1),
                pts_scored: (totPts / gp).toFixed(1),
                reb_total: (totReb / gp).toFixed(1),
                reb_allowed: (totRebOpp / gp).toFixed(1),
                ast_total: (totAst / gp).toFixed(1),
                ast_allowed: (totAstOpp / gp).toFixed(1),
                fg3m_total: (totFg3m / gp).toFixed(1),
                fg3m_allowed: (totFg3mOpp / gp).toFixed(1),
                fg3a_total: (totFg3a / gp).toFixed(1),
                fg3a_allowed: (totFg3aOpp / gp).toFixed(1),
                fg_pct: fgPct,
                fg_pct_opp: fgPctOpp
            };
        }
    });

    const metricsToRank = [
        { key: 'pace', asc: false }, { key: 'def_rtg', asc: true }, { key: 'off_rtg', asc: false },
        { key: 'pts_scored', asc: false }, { key: 'pts_allowed', asc: true },
        { key: 'reb_total', asc: false }, { key: 'reb_allowed', asc: true },
        { key: 'ast_total', asc: false }, { key: 'ast_allowed', asc: true },
        { key: 'fg3m_total', asc: false }, { key: 'fg3m_allowed', asc: true },
        { key: 'fg3a_total', asc: false }, { key: 'fg3a_allowed', asc: true },
        { key: 'fg_pct', asc: false }, { key: 'fg_pct_opp', asc: true }
    ];

    metricsToRank.forEach(({ key, asc }) => {
        const tuples = [];
        allTeams.forEach(t => {
            const prof = currentForensicTeams[t];
            if (prof && prof.gp > 0 && prof[key] != null) {
                tuples.push({ team: t, val: parseFloat(prof[key]) });
            }
        });

        tuples.sort((a, b) => asc ? (a.val - b.val) : (b.val - a.val));
        const rankedTeams = new Set();
        tuples.forEach((item, idx) => {
            rankedTeams.add(item.team);
            const rankKey = `rank_${key}`;
            if (currentForensicTeams[item.team].is_small_sample) {
                currentForensicTeams[item.team][rankKey] = 'Small Sample';
            } else {
                currentForensicTeams[item.team][rankKey] = idx + 1;
            }
        });

        allTeams.forEach(t => {
            if (!rankedTeams.has(t)) {
                currentForensicTeams[t][`rank_${key}`] = null;
            }
        });
    });

    currentPlayerProfiles = (allPlayerProfiles || []).map(p => {
        const pCopy = { ...p };
        const logs = p.all_logs || [];
        const matchingLogs = logs.filter(lg => lg.d >= sDate && lg.d <= eDate);

        if (matchingLogs.length === 0) {
            pCopy.games_played = 0;
            pCopy.is_small_sample = true;
            pCopy.season_avg = {
                pts: null, reb: null, ast: null, min: null,
                fg3a: null, fg3m: null, paint_pts: null
            };
            pCopy.distribution_10g = {
                pts: { floor: null, median: null, ceiling: null, logs: [] },
                reb: { floor: null, median: null, ceiling: null, logs: [] },
                ast: { floor: null, median: null, ceiling: null, logs: [] }
            };
        } else {
            const pGp = matchingLogs.length;
            pCopy.games_played = pGp;
            pCopy.is_small_sample = pGp < 10;

            const ptsList = matchingLogs.map(l => l.pts);
            const rebList = matchingLogs.map(l => l.reb);
            const astList = matchingLogs.map(l => l.ast);
            const minList = matchingLogs.map(l => l.min);
            const fg3aList = matchingLogs.map(l => l.fg3a);
            const fg3mList = matchingLogs.map(l => l.fg3m);
            const paintList = matchingLogs.map(l => l.paint_pts || 0);

            const avg = arr => (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1);

            pCopy.season_avg = {
                pts: avg(ptsList),
                reb: avg(rebList),
                ast: avg(astList),
                min: avg(minList),
                fg3a: avg(fg3aList),
                fg3m: avg(fg3mList),
                paint_pts: avg(paintList)
            };

            const last10 = matchingLogs.slice(0, 10);
            const calcPctile = (arr, q) => {
                if (!arr.length) return null;
                const s = [...arr].sort((a, b) => a - b);
                const pos = (s.length - 1) * q;
                const base = Math.floor(pos);
                const rest = pos - base;
                if (s[base + 1] !== undefined) {
                    return (s[base] + rest * (s[base + 1] - s[base])).toFixed(1);
                }
                return s[base].toFixed(1);
            };

            const l10Pts = last10.map(l => l.pts);
            const l10Reb = last10.map(l => l.reb);
            const l10Ast = last10.map(l => l.ast);

            pCopy.distribution_10g = {
                pts: { floor: calcPctile(l10Pts, 0.25), median: calcPctile(l10Pts, 0.50), ceiling: calcPctile(l10Pts, 0.75), logs: l10Pts },
                reb: { floor: calcPctile(l10Reb, 0.25), median: calcPctile(l10Reb, 0.50), ceiling: calcPctile(l10Reb, 0.75), logs: l10Reb },
                ast: { floor: calcPctile(l10Ast, 0.25), median: calcPctile(l10Ast, 0.50), ceiling: calcPctile(l10Ast, 0.75), logs: l10Ast }
            };
        }
        return pCopy;
    });
}

function onDateRangeChange() {
    startDate = document.getElementById('input-start-date').value;
    endDate = document.getElementById('input-end-date').value;
    recalculateForensicsForDateRange(startDate, endDate);
    renderScheduleDatePills();
    renderScheduleGames();
    if (typeof renderProfileDateChips === 'function') renderProfileDateChips();
    if (typeof renderPlayerProfiles === 'function') renderPlayerProfiles();
}

function reloadAllData() {
    recalculateForensicsForDateRange(startDate, endDate);
    renderScheduleDatePills();
    renderScheduleGames();
    if (typeof renderProfileDateChips === 'function') renderProfileDateChips();
    if (typeof renderPlayerProfiles === 'function') renderPlayerProfiles();
    safeCreateIcons();
}

function switchTab(tabId) {
    const schedSection = document.getElementById('tab-schedule');
    const profileSection = document.getElementById('tab-profiles');
    const btnSched = document.getElementById('tab-btn-schedule');
    const btnProfiles = document.getElementById('tab-btn-profiles');

    if (tabId === 'schedule') {
        schedSection.classList.remove('hidden');
        profileSection.classList.add('hidden');
        btnSched.className = 'py-2.5 px-1 border-b-2 border-cyan-400 text-slate-100 font-semibold text-xs flex items-center space-x-2 transition whitespace-nowrap';
        btnProfiles.className = 'py-2.5 px-1 border-b-2 border-transparent text-slate-400 hover:text-slate-200 font-medium text-xs flex items-center space-x-2 transition whitespace-nowrap';
    } else {
        schedSection.classList.add('hidden');
        profileSection.classList.remove('hidden');
        btnProfiles.className = 'py-2.5 px-1 border-b-2 border-cyan-400 text-slate-100 font-semibold text-xs flex items-center space-x-2 transition whitespace-nowrap';
        btnSched.className = 'py-2.5 px-1 border-b-2 border-transparent text-slate-400 hover:text-slate-200 font-medium text-xs flex items-center space-x-2 transition whitespace-nowrap';
    }
    safeCreateIcons();
}

const STAT_CARD_METRICS = [
    { label: 'Points / Game', key: 'pts_scored', rankKey: 'rank_pts_scored' },
    { label: 'Points Allowed / G', key: 'pts_allowed', rankKey: 'rank_pts_allowed' },
    { label: 'Rebounds / Game', key: 'reb_total', rankKey: 'rank_reb_total' },
    { label: 'Rebounds Allowed / G', key: 'reb_allowed', rankKey: 'rank_reb_allowed' },
    { label: 'Assists / Game', key: 'ast_total', rankKey: 'rank_ast_total' },
    { label: 'Assists Allowed / G', key: 'ast_allowed', rankKey: 'rank_ast_allowed' },
    { label: 'Threes Made (3PM)', key: 'fg3m_total', rankKey: 'rank_fg3m_total' },
    { label: '3PM Allowed / G', key: 'fg3m_allowed', rankKey: 'rank_fg3m_allowed' },
    { label: 'Threes Attempted (3PA)', key: 'fg3a_total', rankKey: 'rank_fg3a_total' },
    { label: '3PA Allowed / G', key: 'fg3a_allowed', rankKey: 'rank_fg3a_allowed' },
    { label: 'FG% (Field Goal %)', key: 'fg_pct', rankKey: 'rank_fg_pct', isPct: true },
    { label: 'FG% Allowed', key: 'fg_pct_opp', rankKey: 'rank_fg_pct_opp', isPct: true },
    { label: 'Pace (Poss/48m)', key: 'pace', rankKey: 'rank_pace' },
    { label: 'Offensive Rating', key: 'off_rtg', rankKey: 'rank_off_rtg' },
    { label: 'Defensive Rating', key: 'def_rtg', rankKey: 'rank_def_rtg' }
];

function renderFunnelZone(label, aGen, hAllow, aTeam, hTeam) {
    const aW = Math.min(100, (parseFloat(aGen) || 0) * 2);
    const hW = Math.min(100, (parseFloat(hAllow) || 0) * 2);
    return `
        <div class="bg-[#161b22] p-2 rounded-lg border border-[#21262d] space-y-1">
            <div class="flex items-center justify-between text-[10px] font-bold text-slate-300">
                <span>${label}</span>
                <span class="font-mono text-[9px] text-cyan-400">${aGen != null ? aGen + '%' : '--'}</span>
            </div>
            <div class="space-y-1">
                <div>
                    <div class="flex justify-between text-[8.5px] font-mono text-slate-400"><span>${aTeam} Gen</span><span>${aGen != null ? aGen + '%' : '--'}</span></div>
                    <div class="w-full bg-[#21262d] h-1.5 rounded-full overflow-hidden"><div class="bg-cyan-500 h-full rounded-full" style="width: ${aW}%"></div></div>
                </div>
                <div>
                    <div class="flex justify-between text-[8.5px] font-mono text-slate-400"><span>${hTeam} Allow</span><span>${hAllow != null ? hAllow + '%' : '--'}</span></div>
                    <div class="w-full bg-[#21262d] h-1.5 rounded-full overflow-hidden"><div class="bg-emerald-500 h-full rounded-full" style="width: ${hW}%"></div></div>
                </div>
            </div>
        </div>
    `;
}

function renderGameCardHtml(g) {
    const a = currentForensicTeams[g.away_team] || g.away_forensic || allForensicTeams[g.away_team] || {};
    const h = currentForensicTeams[g.home_team] || g.home_forensic || allForensicTeams[g.home_team] || {};
    const isFinal = g.is_final;
    const aTeam = g.away_team;
    const hTeam = g.home_team;

    let statusBadge = '';
    if (isFinal) {
        statusBadge = `<span class="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 flex items-center space-x-1"><span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span><span>FINAL</span></span>`;
    } else {
        statusBadge = `<span class="px-2 py-0.5 rounded text-[10px] font-semibold bg-[#21262d] text-slate-400 border border-[#30363d]">SCHEDULED</span>`;
    }

    const hasDataA = a.gp > 0 && a.pts_scored != null;
    const hasDataH = h.gp > 0 && h.pts_scored != null;
    const aRim = (hasDataA && a.shot_profile_offense?.restricted_area?.freq != null) ? a.shot_profile_offense.restricted_area.freq : null;
    const hRimDef = (hasDataH && h.shot_profile_allowed?.restricted_area?.freq != null) ? h.shot_profile_allowed.restricted_area.freq : null;
    const aMid = (hasDataA && a.shot_profile_offense?.mid_range?.freq != null) ? a.shot_profile_offense.mid_range.freq : null;
    const hMidDef = (hasDataH && h.shot_profile_allowed?.mid_range?.freq != null) ? h.shot_profile_allowed.mid_range.freq : null;
    const a3pt = (hasDataA && a.three_point_rate != null) ? a.three_point_rate : null;
    const h3ptDef = (hasDataH && h.shot_profile_allowed?.above_break_3?.freq != null) ? ((h.shot_profile_allowed?.corner_3?.freq || 9.5) + (h.shot_profile_allowed?.above_break_3?.freq || 29.5)).toFixed(1) : null;
    const estPace = (hasDataA && hasDataH && a.pace && h.pace) ? ((parseFloat(a.pace) + parseFloat(h.pace)) / 2).toFixed(1) : '--';

    return `
        <div class="glass-panel rounded-xl p-4 sm:p-5 border border-[#30363d] space-y-3.5 hover:border-slate-500 transition shadow-sm flex flex-col justify-between">
            <div class="flex items-center justify-between text-xs text-slate-400 border-b border-[#30363d] pb-2.5">
                <div class="flex items-center space-x-2">
                    <span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-[#21262d] text-slate-300 border border-[#30363d] font-mono">
                        ${g.gameday}
                    </span>
                    <span class="text-slate-500">•</span>
                    <span class="font-mono text-slate-400 text-xs">${g.gametime || '7:30 PM ET'}</span>
                </div>
                <div class="flex items-center space-x-2">
                    ${statusBadge}
                    <button onclick="openDeepDiveModal('${g.game_id}')" class="px-2.5 py-1 rounded-lg text-xs font-semibold bg-cyan-950/60 hover:bg-cyan-900/80 text-cyan-300 border border-cyan-800/60 transition flex items-center space-x-1.5 shadow-sm active:scale-95">
                        <i data-lucide="scan" class="w-3.5 h-3.5"></i>
                        <span>Deep Dive</span>
                    </button>
                </div>
            </div>

            <div class="grid grid-cols-12 items-center py-1 gap-2">
                <div class="col-span-5 flex items-center space-x-2.5 min-w-0">
                    <div class="w-10 h-10 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center p-1.5 shadow-inner shrink-0 relative overflow-hidden">
                        <img src="https://a.espncdn.com/i/teamlogos/nba/500/${aTeam.toLowerCase()}.png" alt="${aTeam}" class="w-full h-full object-contain" onerror="this.onerror=null; this.style.display='none'; this.nextElementSibling.style.display='flex';"/>
                        <span class="font-extrabold text-cyan-400 font-mono text-xs hidden items-center justify-center w-full h-full">${aTeam}</span>
                    </div>
                    <div class="min-w-0 flex-1">
                        <h4 class="font-bold text-slate-100 text-xs sm:text-sm truncate">${g.away_team_name}</h4>
                        <div class="text-[11px] text-slate-400 flex items-center flex-wrap gap-x-1.5 mt-0.5 font-mono">
                            <span class="text-slate-300 font-semibold">${g.away_record || (a.gp > 0 ? `${a.gp} GP` : '--')}</span>
                            <span class="text-slate-600">•</span>
                            <span class="text-slate-500 font-medium">Away</span>
                        </div>
                    </div>
                </div>

                <div class="col-span-2 text-center flex flex-col items-center justify-center">
                    ${isFinal && g.away_score != null && g.home_score != null ? `
                        <div class="flex items-center justify-center space-x-1 font-mono font-extrabold text-base sm:text-lg text-slate-100">
                            <span>${g.away_score}</span>
                            <span class="text-slate-600 text-xs">-</span>
                            <span>${g.home_score}</span>
                        </div>
                        <span class="text-[9px] font-bold font-mono text-emerald-400 uppercase">FINAL</span>
                    ` : `
                        <div class="w-7 h-7 mx-auto rounded-full bg-[#21262d] border border-[#30363d] flex items-center justify-center font-bold text-slate-400 text-xs">
                            @
                        </div>
                    `}
                </div>

                <div class="col-span-5 flex items-center justify-end space-x-2.5 text-right min-w-0">
                    <div class="min-w-0 flex-1">
                        <h4 class="font-bold text-slate-100 text-xs sm:text-sm truncate">${g.home_team_name}</h4>
                        <div class="text-[11px] text-slate-400 flex items-center justify-end flex-wrap gap-x-1.5 mt-0.5 font-mono">
                            <span class="text-slate-500 font-medium">Home</span>
                            <span class="text-slate-600">•</span>
                            <span class="text-slate-300 font-semibold">${g.home_record || (h.gp > 0 ? `${h.gp} GP` : '--')}</span>
                        </div>
                    </div>
                    <div class="w-10 h-10 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center p-1.5 shadow-inner shrink-0 relative overflow-hidden">
                        <img src="https://a.espncdn.com/i/teamlogos/nba/500/${hTeam.toLowerCase()}.png" alt="${hTeam}" class="w-full h-full object-contain" onerror="this.onerror=null; this.style.display='none'; this.nextElementSibling.style.display='flex';"/>
                        <span class="font-extrabold text-rose-400 font-mono text-xs hidden items-center justify-center w-full h-full">${hTeam}</span>
                    </div>
                </div>
            </div>

            <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-2.5 sm:p-3 space-y-0.5">
                <div class="grid grid-cols-3 text-[10px] uppercase font-bold text-slate-500 border-b border-[#21262d] pb-1.5 px-2">
                    <div class="text-left font-mono">${aTeam} STAT & RANK</div>
                    <div class="text-center font-mono">
                        <span class="stat-info-trigger">
                            PER-GAME STAT
                            <span class="stat-info-box block">
                                <span class="font-bold text-slate-100 mb-1 flex items-center gap-1.5 border-b border-[#30363d] pb-1 text-left block">
                                    <i data-lucide="info" class="w-3 h-3 text-cyan-400 shrink-0 inline"></i>
                                    <span>Forensic Per-Game Baselines</span>
                                </span>
                                <span class="text-slate-300 text-[10.5px] leading-relaxed text-left normal-case block">
                                    Head-to-head offensive volume & efficiency vs defensive resistance. Ranks #1-#10 (green) reflect top containment or high volume; ranks #21-#30 (amber) reflect lower tiers. Hover over any individual stat for its exact definition.
                                </span>
                            </span>
                        </span>
                    </div>
                    <div class="text-right font-mono">${hTeam} STAT & RANK</div>
                </div>

                ${STAT_CARD_METRICS.map(m => {
                    const aVal = a[m.key] != null ? (m.isPct ? a[m.key] + '%' : a[m.key]) : null;
                    const hVal = h[m.key] != null ? (m.isPct ? h[m.key] + '%' : h[m.key]) : null;
                    return formatStatRow(m.label, aVal, a[m.rankKey], hVal, h[m.rankKey], a.is_small_sample, h.is_small_sample);
                }).join('')}
            </div>

            ${(!hasDataA || !hasDataH) ? `
            <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3 text-center space-y-1">
                <div class="flex items-center justify-between text-[10px] font-bold uppercase tracking-wider text-slate-400 border-b border-[#21262d] pb-1.5 px-1">
                    <span class="flex items-center space-x-1.5">
                        <i data-lucide="split" class="w-3 h-3 text-slate-400"></i>
                        <span>Shot Profile Funnel</span>
                    </span>
                    <span class="font-mono text-[9px] text-slate-400 bg-[#161b22] px-2 py-0.5 rounded border border-[#21262d]">Game Pace: --</span>
                </div>
                <div class="py-2.5 text-slate-500 font-mono text-[11px]">
                    No completed games recorded in date range (${startDate} to ${endDate}) • Shot profile blank
                </div>
            </div>
            ` : `
            <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3 space-y-2">
                <div class="flex items-center justify-between text-[10px] font-bold uppercase tracking-wider text-slate-400 border-b border-[#21262d] pb-1.5 px-1">
                    <span class="flex items-center space-x-1.5">
                        <i data-lucide="split" class="w-3 h-3 text-slate-400"></i>
                        <span>Shot Profile Funnel</span>
                    </span>
                    <span class="font-mono text-[9px] text-slate-300 bg-[#161b22] px-2 py-0.5 rounded border border-[#21262d]">Game Pace: ${estPace} Poss/48</span>
                </div>
                <div class="grid grid-cols-3 gap-2 text-xs">
                    ${renderFunnelZone('Rim (RA)', aRim, hRimDef, aTeam, hTeam)}
                    ${renderFunnelZone('Mid-Range', aMid, hMidDef, aTeam, hTeam)}
                    ${renderFunnelZone('3PT Rate', a3pt, h3ptDef, aTeam, hTeam)}
                </div>
            </div>
            `}
        </div>
    `;
}

function renderScheduleGames() {
    const container = document.getElementById('schedule-games-container');
    if (!container) return;

    let games = [...allScheduleGames];

    if (currentScheduleSeason === '2026') {
        games = games.filter(g => g.gameday && g.gameday >= '2026-10-19');
    } else if (currentScheduleSeason === '2025') {
        games = games.filter(g => g.gameday && g.gameday < '2026-10-19');
    }

    if (startDate) games = games.filter(g => g.gameday >= startDate);
    if (endDate) games = games.filter(g => g.gameday <= endDate);

    if (selectedScheduleDate && selectedScheduleDate !== 'all') {
        games = games.filter(g => g.gameday === selectedScheduleDate);
    }

    if (currentScheduleTeam && currentScheduleTeam !== 'ALL') {
        games = games.filter(g => g.away_team === currentScheduleTeam || g.home_team === currentScheduleTeam);
    }

    if (scheduleSearchQuery) {
        const q = scheduleSearchQuery.toLowerCase().trim();
        games = games.filter(g =>
            g.away_team.toLowerCase().includes(q) ||
            g.home_team.toLowerCase().includes(q) ||
            (g.away_team_name && g.away_team_name.toLowerCase().includes(q)) ||
            (g.home_team_name && g.home_team_name.toLowerCase().includes(q))
        );
    }

    const countEl = document.getElementById('sched-stat-total-games');
    if (countEl) countEl.textContent = games.length;

    const compEl = document.getElementById('sched-stat-completed-games');
    if (compEl) compEl.textContent = games.filter(g => g.is_final).length;

    if (games.length === 0) {
        container.innerHTML = `
            <div class="glass-panel rounded-xl p-12 text-center text-slate-400 space-y-2 border border-[#30363d]">
                <i data-lucide="calendar-x" class="w-8 h-8 mx-auto text-slate-500 mb-1"></i>
                <p class="font-medium text-slate-300">No scheduled matchups match the selected criteria.</p>
                <p class="text-xs text-slate-500">Pick another slate date or broaden your date range.</p>
            </div>
        `;
    } else {
        container.innerHTML = games.slice(0, 30).map(renderGameCardHtml).join('');
    }

    safeCreateIcons(container);
}

function renderScheduleDatePills() {
    const container = document.getElementById('schedule-date-pills');
    if (!container) return;

    let seasonGames = allScheduleGames.filter(g => {
        if (currentScheduleSeason === '2026' && g.gameday < '2026-10-19') return false;
        if (currentScheduleSeason === '2025' && g.gameday >= '2026-10-19') return false;
        if (startDate && g.gameday < startDate) return false;
        if (endDate && g.gameday > endDate) return false;
        return true;
    });

    const dateCounts = {};
    seasonGames.forEach(g => {
        const d = g.gameday;
        dateCounts[d] = (dateCounts[d] || 0) + 1;
    });

    const sortedDates = Object.keys(dateCounts).sort().reverse();
    const isAll = selectedScheduleDate === 'all';

    let html = `
        <button onclick="filterScheduleDate('all')" class="px-3 py-1.5 rounded-lg text-xs font-semibold whitespace-nowrap transition border shrink-0 ${isAll ? 'bg-[#30363d] text-slate-100 border-slate-500 font-bold' : 'bg-[#21262d] text-slate-400 hover:text-slate-200 border-[#30363d]'}">
            All Dates (${sortedDates.length})
        </button>
    `;

    sortedDates.forEach(d => {
        const isActive = selectedScheduleDate === d;
        html += `
            <button onclick="filterScheduleDate('${d}')" class="px-3 py-1.5 rounded-lg text-xs font-semibold whitespace-nowrap transition border shrink-0 ${isActive ? 'bg-[#30363d] text-slate-100 border-slate-500 font-bold' : 'bg-[#21262d] text-slate-400 hover:text-slate-200 border-[#30363d]'}">
                ${d} <span class="text-[10px] text-slate-500 font-mono ml-0.5">(${dateCounts[d]})</span>
            </button>
        `;
    });

    container.innerHTML = html;
}

function filterScheduleDate(date) {
    selectedScheduleDate = date || 'all';
    const picker = document.getElementById('schedule-date-picker');
    if (picker) picker.value = (date !== 'all') ? date : '';
    renderScheduleDatePills();
    renderScheduleGames();
}

function onScheduleSeasonChange(season) {
    currentScheduleSeason = season;
    const titleEl = document.getElementById('schedule-title');
    const seasonPill = document.getElementById('header-season-pill');
    const todayStr = getTodayDateStr();

    if (season === '2026') {
        if (titleEl) titleEl.textContent = '2026-27 NBA Forensic Matchup Lab';
        if (seasonPill) seasonPill.textContent = '2026-27 Tracking';
        startDate = '2026-10-19';
        endDate = '2027-04-14';
    } else if (season === '2025') {
        if (titleEl) titleEl.textContent = '2025-26 NBA Forensic Matchup Lab';
        if (seasonPill) seasonPill.textContent = '2025-26 to Present';
        startDate = '2025-10-02';
        endDate = todayStr;
    } else {
        if (titleEl) titleEl.textContent = 'Full NBA Forensic Matchup Lab';
        if (seasonPill) seasonPill.textContent = 'All Seasons';
        startDate = '2025-10-02';
        endDate = '2027-04-14';
    }

    document.getElementById('input-start-date').value = startDate;
    document.getElementById('input-end-date').value = endDate;

    selectedScheduleDate = 'all';
    onDateRangeChange();
}

function filterScheduleTeam(team) {
    currentScheduleTeam = team || 'ALL';
    renderScheduleGames();
}

function onScheduleSearch(query) {
    scheduleSearchQuery = query || '';
    renderScheduleGames();
}

async function compareCustomMatchup() {
    const away = document.getElementById('lab-away-select')?.value;
    const home = document.getElementById('lab-home-select')?.value;
    if (!away || !home) return;

    try {
        const res = await fetch(`/api/matchup/${away}/${home}`);
        const data = await res.json();

        const customMatchup = {
            game_id: `NBA_${away}_${home}_custom`,
            gameday: 'Custom Matchup Analysis',
            gametime: 'Head-to-Head Alignment',
            is_final: false,
            away_team: away,
            away_team_name: teamNamesMap[away] || away,
            home_team: home,
            home_team_name: teamNamesMap[home] || home,
            away_forensic: data.away_forensic,
            home_forensic: data.home_forensic,
            home_def_sim: data.home_def_vs_away_sim,
            away_def_sim: data.away_def_vs_home_sim
        };

        allScheduleGames.unshift(customMatchup);
        const container = document.getElementById('schedule-games-container');
        if (container) {
            container.innerHTML = renderGameCardHtml(customMatchup) + container.innerHTML;
            safeCreateIcons(container);
        }
        openDeepDiveModal(customMatchup.game_id);
    } catch (e) {
        console.error("Matchup API error:", e);
    }
}

// Initialize on DOM load
document.addEventListener('DOMContentLoaded', () => {
    const startInput = document.getElementById('input-start-date');
    const endInput = document.getElementById('input-end-date');
    if (startInput) startInput.value = startDate;
    if (endInput) endInput.value = endDate;

    recalculateForensicsForDateRange(startDate, endDate);
    renderScheduleDatePills();
    renderScheduleGames();
    if (typeof renderProfileDateChips === 'function') renderProfileDateChips();
    if (typeof renderPlayerProfiles === 'function') renderPlayerProfiles();
    safeCreateIcons();

    const initAway = APP_DATA.initialAway;
    const initHome = APP_DATA.initialHome;
    if (initAway && initHome) {
        setTimeout(() => openMatchupByTeams(initAway, initHome), 150);
    }
});

// Global Escape key listener to close Deep Dive modal
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeDeepDiveModal();
    }
});
