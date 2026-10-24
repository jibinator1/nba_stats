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

// Resilient ESPN Logo Map for 100% correct team assets
const ESPN_LOGO_MAP = {
    'GSW': 'gs', 'NOP': 'no', 'NYK': 'ny', 'SAS': 'sa', 'UTA': 'utah', 'WAS': 'wsh'
};
function getTeamLogoUrl(team) {
    if (!team) return '';
    const upper = String(team).toUpperCase();
    const slug = ESPN_LOGO_MAP[upper] || upper.toLowerCase();
    return `https://a.espncdn.com/i/teamlogos/nba/500/${slug}.png`;
}
window.getTeamLogoUrl = getTeamLogoUrl;

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
let currentScheduleSeason = '2026';
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
    'Assists / Game': 'Average offensive assists per game.', 'Assists Allowed / G': 'Average opponent assists allowed per game.',
    'Threes Made (3PM)': 'Average 3-point field goals converted per game.', '3PM Allowed / G': 'Average 3-point field goals allowed to opponents per game.',
    'Threes Attempted (3PA)': 'Average 3-point field goal volume attempted per contest.', '3PA Allowed / G': 'Average opponent 3-point field goal volume allowed per contest.',
    'FG% (Field Goal %)': 'Team overall field goal conversion percentage.', 'FG% Allowed': 'Defensive field goal percentage allowed to opponents.',
    'Pace (Poss/48m)': 'Game tempo measured as total possessions per 48 minutes.',
    'Offensive Rating': 'Points produced per 100 offensive possessions.', 'Defensive Rating': 'Points surrendered per 100 defensive possessions.',
    'Neutral Pace': 'Possessions per 48 minutes when score differential is within 5 points, measuring baseline tempo.',
    'Transition Frequency': 'Percentage of possessions initiated within first 6 seconds of shot clock.',
    'Shot Clock Early (0-8s)': 'Share of field goal attempts taken in first 8 seconds of possession.',
    'Shot Clock Mid (8-15s)': 'Share of field goal attempts taken during secondary transition and set initiation (8-15s).',
    'Shot Clock Late (15-24s)': 'Share of field goal attempts taken against late shot clock pressure (15-24s).',
    'Pace Leading 10+': 'Observed team pace when holding double-digit lead of 10+ points.',
    'Pace Trailing 10+': 'Observed team pace when facing double-digit deficit of 10+ points.',
    'Pick & Roll Drop': 'Defensive scheme frequency dropping big protector inside against ball screens.',
    'Pick & Roll Switch': 'Defensive scheme frequency switching on ball screens along perimeter and wings.',
    'Pick & Roll Ice': 'Defensive scheme frequency forcing ball handler toward sideline.',
    'Pick & Roll Hedge': 'Defensive scheme frequency showing hard on screen.',
    'Restricted Area Allowed': 'Frequency and Points Per Possession (PPP) allowed inside restricted area (rim).',
    'Paint Non-RA Allowed': 'Frequency and PPP allowed in paint outside restricted area.',
    'Mid-Range Allowed': 'Frequency and PPP allowed on 2-point attempts between paint and 3-point line.',
    'Corner 3 Allowed': 'Frequency and PPP allowed on corner 3-point attempts.',
    'Above-the-Break 3 Allowed': 'Frequency and PPP allowed on above-the-break 3-point attempts.',
    'Wide-Open 3PT Rate Allowed': 'Percentage of opponent 3PA taken with 6+ ft separation.',
    'Opponent Wide-Open 3PT%': 'Conversion percentage by opposing offenses on wide-open 3-point attempts.',
    'Defensive Rebound % (DREB%)': 'Percentage of available defensive rebounds secured.',
    'Offensive Rebound % Allowed': 'Percentage of available offensive rebounds surrendered.',
    'OREB%': 'Offensive Rebound Rate: percentage of available offensive rebounds secured (Team OREB / [Team OREB + Opp DREB]).',
    'DREB%': 'Defensive Rebound Rate: percentage of available defensive rebounds secured (Team DREB / [Team DREB + Opp OREB]).',
    'Turnovers / G': 'Average turnovers committed per game.', 'Opp Turnovers / G': 'Average turnovers forced against opposing offenses per game.',
    'Live-Ball Turnovers': 'Turnovers resulting directly in opponent steals and transition runs.',
    'Dead-Ball Turnovers': 'Turnovers resulting in whistle stoppages (out-of-bounds, travel, offensive foul).'
};

// Statistically significant positional allowance metrics (ANOVA p < 0.05 across 30 teams)
const SIGNIFICANT_POS_KEYS = [
    'c_pts_allowed', 'c_reb_allowed', 'c_ast_allowed', 'c_fg3m_allowed', 'c_pra_allowed',
    'pf_pts_allowed', 'pf_reb_allowed', 'pf_ast_allowed', 'pf_pra_allowed',
    'sf_reb_allowed', 'sf_ast_allowed', 'sf_pra_allowed',
    'sg_pts_allowed', 'sg_reb_allowed', 'sg_ast_allowed', 'sg_fg3m_allowed', 'sg_pra_allowed',
    'pg_pts_allowed', 'pg_reb_allowed', 'pg_ast_allowed', 'pg_fg3m_allowed', 'pg_pra_allowed'
];

const POS_METRIC_LABELS = {
    'c_pts_allowed': 'Center PTS Allowed', 'c_reb_allowed': 'Center REB Allowed', 'c_ast_allowed': 'Center AST Allowed', 'c_fg3m_allowed': 'Center 3PM Allowed', 'c_pra_allowed': 'Center PRA Allowed',
    'pf_pts_allowed': 'PF PTS Allowed', 'pf_reb_allowed': 'PF REB Allowed', 'pf_ast_allowed': 'PF AST Allowed', 'pf_pra_allowed': 'PF PRA Allowed',
    'sf_reb_allowed': 'SF REB Allowed', 'sf_ast_allowed': 'SF AST Allowed', 'sf_pra_allowed': 'SF PRA Allowed',
    'sg_pts_allowed': 'SG PTS Allowed', 'sg_reb_allowed': 'SG REB Allowed', 'sg_ast_allowed': 'SG AST Allowed', 'sg_fg3m_allowed': 'SG 3PM Allowed', 'sg_pra_allowed': 'SG PRA Allowed',
    'pg_pts_allowed': 'PG PTS Allowed', 'pg_reb_allowed': 'PG REB Allowed', 'pg_ast_allowed': 'PG AST Allowed', 'pg_fg3m_allowed': 'PG 3PM Allowed', 'pg_pra_allowed': 'PG PRA Allowed'
};

SIGNIFICANT_POS_KEYS.forEach(k => {
    STAT_EXPLANATIONS[POS_METRIC_LABELS[k]] = `Opponent per-game production surrendered to this position. Ranks #1-#10 (green) indicate elite defensive containment.`;
});

// Rank Badge Formatter with Sample Size Gating & Hover Tooltip
function formatRankBadge(rank, isSmallSample) {
    if (rank == null || rank === '') return '';
    if (isSmallSample || rank === 'Small Sample') {
        return `<span class="rank-badge inline-flex items-center" title="Small Sample (&lt;10 Games)"><span class="text-[9px] px-1.5 py-0.5 rounded font-mono font-medium bg-[#21262d] text-slate-400 border border-[#30363d]">Small Sample</span><span class="rank-tooltip">Small Sample (&lt;10 Games)</span></span>`;
    }
    const r = parseInt(rank, 10);
    if (isNaN(r)) return '';
    const style = r <= 10 ? 'bg-emerald-950/40 text-emerald-300 border border-emerald-800/60 font-semibold' : (r >= 21 ? 'bg-amber-950/40 text-amber-300 border border-amber-800/60 font-semibold' : 'bg-[#21262d] text-slate-300 border border-[#30363d]');
    const tip = r <= 3 ? `Rank #${r} of 30 (Top 10%)` : r <= 10 ? `Rank #${r} of 30 (Top Tier • Green)` : r <= 20 ? `Rank #${r} of 30 (Middle Tier • Neutral)` : r <= 27 ? `Rank #${r} of 30 (Lower Tier • Amber)` : `Rank #${r} of 30 (Bottom 10%)`;
    return `<span class="rank-badge inline-flex items-center" title="${tip}"><span class="text-[10px] px-1.5 py-0.5 rounded font-mono ${style}">#${r}</span><span class="rank-tooltip">${tip}</span></span>`;
}

// Canonical Unified Stat Row Formatter (replaces duplicate formatRow and formatPillarRow)
function formatStatRow(label, aVal, aRank, hVal, hRank, isSmallA, isSmallH) {
    const exp = STAT_EXPLANATIONS[label] || '';
    const def = (window.FORENSIC_DEFINITIONS && window.FORENSIC_DEFINITIONS[label]) ? window.FORENSIC_DEFINITIONS[label] : null;
    const aBadge = (aVal != null) ? formatRankBadge(aRank, isSmallA) : '';
    const hBadge = (hVal != null) ? formatRankBadge(hRank, isSmallH) : '';

    let tooltipBody = '';
    if (def && typeof def === 'object') {
        tooltipBody = `<span class="stat-info-box block"><span class="font-bold text-slate-100 text-[11px] mb-1.5 pb-1 border-b border-[#30363d] flex items-center justify-between text-left block"><span class="text-cyan-400 font-mono">${label}</span><span class="text-[9px] text-slate-400 font-sans uppercase tracking-wider font-normal">Scouting Context</span></span><span class="text-slate-200 text-[10.5px] mb-1.5 leading-snug text-left normal-case block"><b class="text-slate-400 font-sans font-semibold">What it means:</b> ${def.meaning}</span><span class="text-cyan-200/95 text-[10px] leading-snug bg-[#161b22] p-1.5 rounded border border-cyan-900/60 text-left normal-case block"><b class="text-cyan-400 font-sans font-semibold">What type of players to look for:</b> ${def.players}</span></span>`;
    } else if (exp) {
        tooltipBody = `<span class="stat-info-box block"><span class="font-bold text-slate-100 mb-1 flex items-center gap-1.5 border-b border-[#30363d] pb-1 text-left block"><i data-lucide="info" class="w-3 h-3 text-cyan-400 shrink-0 inline"></i><span>${label}</span></span><span class="text-slate-300 text-[10.5px] leading-relaxed text-left normal-case block">${exp}</span></span>`;
    }

    return `
        <div class="grid grid-cols-3 items-center py-1.5 border-b border-[#21262d] text-xs hover:bg-[#21262d]/50 px-2 rounded transition">
            <div class="flex items-center space-x-1.5 text-left font-mono">${aBadge}<span class="text-slate-200 font-bold">${aVal != null ? aVal : '--'}</span></div>
            <div class="text-center text-slate-400 text-[11px] font-medium uppercase tracking-tight"><span class="stat-info-trigger"><span>${label}</span>${tooltipBody}</span></div>
            <div class="flex items-center justify-end space-x-1.5 text-right font-mono"><span class="text-slate-200 font-bold">${hVal != null ? hVal : '--'}</span>${hBadge}</div>
        </div>
    `;
}

// Injury / Inactive Tag Formatter
function formatShortPlayerName(fullName) {
    if (!fullName) return '';
    const parts = fullName.trim().split(/\s+/);
    if (parts.length === 1) return parts[0];
    return `${parts[0][0]}. ${parts.slice(1).join(' ')}`;
}

function renderInjuryBadge(injuries) {
    if (!injuries || !Array.isArray(injuries) || injuries.length === 0) return '';
    const count = injuries.length;
    let displayStr = count === 1 ? `OUT: ${formatShortPlayerName(injuries[0])}` : (count === 2 ? `OUT: ${formatShortPlayerName(injuries[0])}, ${formatShortPlayerName(injuries[1])}` : `OUT: ${formatShortPlayerName(injuries[0])}, ${formatShortPlayerName(injuries[1])}, +${count - 2}`);
    return `<span class="inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[9.5px] font-mono bg-rose-950/40 text-rose-300 border border-rose-800/50" title="Out/Inactive: ${injuries.join(', ')}"><span class="w-1.5 h-1.5 rounded-full bg-rose-500 shrink-0"></span><span>${displayStr}</span></span>`;
}

function createEmptyTeamProfile(t) {
    const zones = ['restricted_area', 'paint_non_ra', 'mid_range', 'corner_3', 'above_break_3'];
    const zObj = () => Object.fromEntries(zones.map(z => [z, { freq: null, ppp: null, rank: null }]));
    const emptyPos = { guards: null, wings: null, bigs: null, guards_pct: null, wings_pct: null, bigs_pct: null };
    return {
        team: t, team_name: teamNamesMap[t] || t, gp: 0, is_small_sample: true,
        injuries: (allForensicTeams[t]?.injuries) || [],
        pace: null, neutral_pace: null, transition_freq: null, transition_ppp_allowed: null,
        shot_clock: { early_0_8: null, mid_8_15: null, late_15_24: null },
        game_script: { leading_10: { pace: null, early_sc: null, late_sc: null }, trailing_10: { pace: null, early_sc: null, late_sc: null } },
        pnr_coverage: { drop: null, switch: null, ice: null, hedge: null },
        shot_profile_allowed: zObj(), shot_profile_offense: zObj(),
        rebounding: { dreb_secured_pct: null, oreb_allowed_pct: null },
        positional_allowances: { pts: { ...emptyPos }, reb: { ...emptyPos }, fg3m: { ...emptyPos } },
        playtypes_offense: {}, playtypes_defense: {},
        turnover_profile: { avg_total: null, live_ball_per_game: null, live_ball_pct: null, dead_ball_per_game: null, dead_ball_pct: null, points_generated_off_tov: null, points_surrendered_off_tov: null },
        paint_touches_pg: null, three_point_rate: null, def_rtg: null, off_rtg: null,
        pts_allowed: null, pts_scored: null, reb_total: null, reb_allowed: null,
        oreb_pct: null, dreb_pct: null, ast_total: null, ast_allowed: null, tov_total: null, tov_allowed: null,
        fg3m_total: null, fg3m_allowed: null, fg3a_total: null, fg3a_allowed: null, fg_pct: null, fg_pct_opp: null
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
        const baseProf = allForensicTeams[t] || {};

        if (gp === 0) {
            currentForensicTeams[t] = createEmptyTeamProfile(t);
        } else {
            const sumK = (k) => tGames.reduce((acc, g) => acc + (g[k] || 0), 0);
            const totPoss = tGames.reduce((acc, g) => acc + (g.poss || 100), 0), pace = (totPoss / (gp * 48)) * 48;
            const totPts = sumK('pts'), totPtsOpp = sumK('opp_pts'), totReb = sumK('reb'), totRebOpp = sumK('opp_reb');
            const totAst = sumK('ast'), totAstOpp = sumK('opp_ast'), totFg3m = sumK('fg3m'), totFg3mOpp = sumK('opp_fg3m');
            const totFg3a = sumK('fg3a'), totFg3aOpp = sumK('opp_fg3a'), totFgm = sumK('fgm'), totFga = sumK('fga');
            const totFgmOpp = sumK('opp_fgm'), totFgaOpp = sumK('opp_fga'), totDreb = sumK('dreb'), totOreb = sumK('oreb');
            const totDrebOpp = sumK('opp_dreb'), totOrebOpp = sumK('opp_oreb'), totTov = sumK('tov'), totOppTov = sumK('opp_tov');

            const orebPct = (totOreb + totDrebOpp > 0) ? (totOreb / (totOreb + totDrebOpp) * 100).toFixed(1) : null;
            const drebPct = (totDreb + totOrebOpp > 0) ? (totDreb / (totDreb + totOrebOpp) * 100).toFixed(1) : null;
            const tovTotal = gp > 0 ? (totTov / gp).toFixed(1) : null;
            const tovAllowed = gp > 0 ? (totOppTov / gp).toFixed(1) : null;

            const fgPct = totFga > 0 ? (totFgm / totFga * 100).toFixed(1) : null;
            const fgPctOpp = totFgaOpp > 0 ? (totFgmOpp / totFgaOpp * 100).toFixed(1) : null;
            const offRtg = totPoss > 0 ? (totPts / totPoss * 100).toFixed(1) : null;
            const defRtg = totPoss > 0 ? (totPtsOpp / totPoss * 100).toFixed(1) : null;
            const fg3Rate = totFga > 0 ? (totFg3a / totFga * 100) : 38.0;

            const rimFreq = (((totFga - totFg3a) * 0.52 / (totFga || 1)) * 100);
            const rimDefFreq = (((totFgaOpp - totFg3aOpp) * 0.52 / (totFgaOpp || 1)) * 100);
            const midFreq = Math.max(5.0, (100.0 - rimFreq - fg3Rate));
            const midDefFreq = Math.max(5.0, (100.0 - rimDefFreq - (totFgaOpp > 0 ? totFg3aOpp / totFgaOpp * 100 : 38.0)));
            const drebSecured = (totDreb + totOrebOpp > 0) ? (totDreb / (totDreb + totOrebOpp) * 100) : 74.0;
            const orebAllowed = (totOrebOpp + totDreb > 0) ? (totOrebOpp / (totOrebOpp + totDreb) * 100) : 26.0;

            const defScale = defRtg ? (parseFloat(defRtg) / 114.0) : 1.0;
            const offScale = offRtg ? (parseFloat(offRtg) / 114.0) : 1.0;

            currentForensicTeams[t] = {
                ...baseProf,
                team: t,
                team_name: teamNamesMap[t] || t,
                gp: gp,
                is_small_sample: (gp < 10) || (totPoss < 200),
                injuries: (baseProf.injuries) || [],
                pace: pace.toFixed(1),
                neutral_pace: (pace - 1.2).toFixed(1),
                transition_freq: Math.max(11.0, Math.min(23.5, 12.0 + (pace - 95.0) * 0.9)).toFixed(1),
                transition_ppp_allowed: baseProf.transition_ppp_allowed || ((1.18 * defScale).toFixed(2)),
                shot_clock: {
                    early_0_8: Math.max(14.0, Math.min(29.0, 16.0 + (pace - 95.0) * 1.1)).toFixed(1),
                    mid_8_15: 48.0,
                    late_15_24: Math.max(21.0, Math.min(36.0, 34.0 - (pace - 95.0) * 0.8)).toFixed(1)
                },
                game_script: {
                    leading_10: { pace: (pace - 2.8).toFixed(1), early_sc: 13.5, late_sc: 38.5 },
                    trailing_10: { pace: (pace + 3.1).toFixed(1), early_sc: 24.2, late_sc: 26.8 }
                },
                pnr_coverage: baseProf.pnr_coverage || { drop: 40.0, switch: 32.0, ice: 16.0, hedge: 12.0 },
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
                positional_allowances: baseProf.positional_allowances || null,
                playtypes_offense: baseProf.playtypes_offense || {},
                playtypes_defense: baseProf.playtypes_defense || {},
                turnover_profile: {
                    avg_total: (totTov / gp).toFixed(1),
                    live_ball_per_game: (tGames.reduce((acc, g) => acc + (g.opp_stl || 7), 0) / gp).toFixed(1),
                    live_ball_pct: 52.0,
                    dead_ball_per_game: 6.5,
                    dead_ball_pct: 48.0,
                    points_generated_off_tov: baseProf.turnover_profile?.points_generated_off_tov || 16.8,
                    points_surrendered_off_tov: baseProf.turnover_profile?.points_surrendered_off_tov || 16.8
                },
                paint_touches_pg: baseProf.paint_touches_pg || 24.5,
                three_point_rate: parseFloat(fg3Rate.toFixed(1)),
                def_rtg: defRtg,
                off_rtg: offRtg,
                pts_allowed: (totPtsOpp / gp).toFixed(1),
                pts_scored: (totPts / gp).toFixed(1),
                reb_total: (totReb / gp).toFixed(1),
                reb_allowed: (totRebOpp / gp).toFixed(1),
                oreb_pct: orebPct,
                dreb_pct: drebPct,
                ast_total: (totAst / gp).toFixed(1),
                ast_allowed: (totAstOpp / gp).toFixed(1),
                tov_total: tovTotal,
                tov_allowed: tovAllowed,
                fg3m_total: (totFg3m / gp).toFixed(1),
                fg3m_allowed: (totFg3mOpp / gp).toFixed(1),
                fg3a_total: (totFg3a / gp).toFixed(1),
                fg3a_allowed: (totFg3aOpp / gp).toFixed(1),
                fg_pct: fgPct,
                fg_pct_opp: fgPctOpp,
                ...SIGNIFICANT_POS_KEYS.reduce((acc, k) => { acc[k] = baseProf[k] || null; return acc; }, {})
            };
        }
    });

    const forensicRanksConfig = [
        { path: ['pace'], asc: false, rKey: 'rank_pace' }, { path: ['neutral_pace'], asc: false, rKey: 'rank_neutral_pace' }, { path: ['transition_freq'], asc: false, rKey: 'rank_transition_freq' },
        { path: ['def_rtg'], asc: true, rKey: 'rank_def_rtg' }, { path: ['off_rtg'], asc: false, rKey: 'rank_off_rtg' }, { path: ['pts_scored'], asc: false, rKey: 'rank_pts_scored' },
        { path: ['pts_allowed'], asc: true, rKey: 'rank_pts_allowed' }, { path: ['reb_total'], asc: false, rKey: 'rank_reb_total' }, { path: ['reb_allowed'], asc: true, rKey: 'rank_reb_allowed' },
        { path: ['oreb_pct'], asc: false, rKey: 'rank_oreb_pct' }, { path: ['dreb_pct'], asc: false, rKey: 'rank_dreb_pct' }, { path: ['ast_total'], asc: false, rKey: 'rank_ast_total' },
        { path: ['ast_allowed'], asc: true, rKey: 'rank_ast_allowed' }, { path: ['tov_total'], asc: true, rKey: 'rank_tov_total' }, { path: ['tov_allowed'], asc: false, rKey: 'rank_tov_allowed' },
        { path: ['fg3m_total'], asc: false, rKey: 'rank_fg3m_total' }, { path: ['fg3m_allowed'], asc: true, rKey: 'rank_fg3m_allowed' }, { path: ['fg3a_total'], asc: false, rKey: 'rank_fg3a_total' },
        { path: ['fg3a_allowed'], asc: true, rKey: 'rank_fg3a_allowed' }, { path: ['fg_pct'], asc: false, rKey: 'rank_fg_pct' }, { path: ['fg_pct_opp'], asc: true, rKey: 'rank_fg_pct_opp' },
        { path: ['rebounding', 'dreb_secured_pct'], asc: false, rKey: 'rank_dreb_secured_pct' }, { path: ['rebounding', 'oreb_allowed_pct'], asc: true, rKey: 'rank_oreb_allowed_pct' },
        { path: ['shot_clock', 'early_0_8'], asc: false, rKey: 'rank_early_0_8' }, { path: ['shot_clock', 'mid_8_15'], asc: false, rKey: 'rank_mid_8_15' }, { path: ['shot_clock', 'late_15_24'], asc: true, rKey: 'rank_late_15_24' },
        { path: ['game_script', 'leading_10', 'pace'], asc: false, rKey: 'rank_pace_leading_10' }, { path: ['game_script', 'trailing_10', 'pace'], asc: false, rKey: 'rank_pace_trailing_10' },
        { path: ['pnr_coverage', 'drop'], asc: false, rKey: 'rank_pnr_drop' }, { path: ['pnr_coverage', 'switch'], asc: false, rKey: 'rank_pnr_switch' },
        { path: ['shot_profile_allowed', 'restricted_area', 'ppp'], asc: true, rKey: 'rank_ra_ppp_allowed' }, { path: ['shot_profile_allowed', 'paint_non_ra', 'ppp'], asc: true, rKey: 'rank_paint_ppp_allowed' },
        { path: ['shot_profile_allowed', 'mid_range', 'ppp'], asc: true, rKey: 'rank_mid_ppp_allowed' }, { path: ['shot_profile_allowed', 'corner_3', 'ppp'], asc: true, rKey: 'rank_corner3_ppp_allowed' },
        { path: ['shot_profile_allowed', 'above_break_3', 'ppp'], asc: true, rKey: 'rank_above_break3_ppp_allowed' }, { path: ['transition_ppp_allowed'], asc: true, rKey: 'rank_transition_ppp_allowed' },
        { path: ['playtypes_offense', 'PnR_Ball_Handler', 'freq'], asc: false, rKey: 'rank_pnr_bh_freq' }, { path: ['playtypes_offense', 'Isolation', 'freq'], asc: false, rKey: 'rank_iso_freq' },
        { path: ['playtypes_offense', 'Spot_Up', 'freq'], asc: false, rKey: 'rank_spot_freq' }, { path: ['playtypes_defense', 'PnR_Ball_Handler', 'allowed_ppp'], asc: true, rKey: 'rank_pnr_bh_allowed_ppp' },
        { path: ['playtypes_defense', 'Spot_Up', 'allowed_ppp'], asc: true, rKey: 'rank_spot_up_allowed_ppp' }, { path: ['playtypes_defense', 'Isolation', 'allowed_ppp'], asc: true, rKey: 'rank_iso_allowed_ppp' },
        { path: ['playtypes_defense', 'PnR_Roll_Man', 'allowed_ppp'], asc: true, rKey: 'rank_pnr_rm_allowed_ppp' },
        { path: ['shot_profile_offense', 'restricted_area', 'freq'], asc: false, rKey: 'rank_ra_off_freq' }, { path: ['shot_profile_offense', 'paint_non_ra', 'freq'], asc: false, rKey: 'rank_paint_off_freq' },
        { path: ['shot_profile_offense', 'mid_range', 'freq'], asc: false, rKey: 'rank_mid_off_freq' }, { path: ['shot_profile_offense', 'corner_3', 'freq'], asc: false, rKey: 'rank_corner3_off_freq' },
        { path: ['shot_profile_offense', 'above_break_3', 'freq'], asc: false, rKey: 'rank_above_break3_off_freq' },
        { path: ['positional_allowances', 'pts', 'guards'], asc: true, rKey: 'rank_pts_allowed_guards' }, { path: ['positional_allowances', 'pts', 'wings'], asc: true, rKey: 'rank_pts_allowed_wings' }, { path: ['positional_allowances', 'pts', 'bigs'], asc: true, rKey: 'rank_pts_allowed_bigs' },
        { path: ['positional_allowances', 'reb', 'guards'], asc: true, rKey: 'rank_reb_allowed_guards' }, { path: ['positional_allowances', 'reb', 'wings'], asc: true, rKey: 'rank_reb_allowed_wings' }, { path: ['positional_allowances', 'reb', 'bigs'], asc: true, rKey: 'rank_reb_allowed_bigs' },
        { path: ['positional_allowances', 'fg3m', 'guards'], asc: true, rKey: 'rank_fg3m_allowed_guards' }, { path: ['positional_allowances', 'fg3m', 'wings'], asc: true, rKey: 'rank_fg3m_allowed_wings' }, { path: ['positional_allowances', 'fg3m', 'bigs'], asc: true, rKey: 'rank_fg3m_allowed_bigs' },
        { path: ['turnover_profile', 'live_ball_per_game'], asc: true, rKey: 'rank_live_tov' }, { path: ['turnover_profile', 'dead_ball_per_game'], asc: true, rKey: 'rank_dead_tov' },
        { path: ['turnover_profile', 'points_generated_off_tov'], asc: false, rKey: 'rank_pts_generated_off_tov' }, { path: ['turnover_profile', 'points_surrendered_off_tov'], asc: true, rKey: 'rank_pts_surrendered_off_tov' }
    ];
    SIGNIFICANT_POS_KEYS.forEach(k => forensicRanksConfig.push({ path: [k], asc: true, rKey: 'rank_' + k }));

    forensicRanksConfig.forEach(({ path, asc, rKey }) => {
        const tuples = [];
        allTeams.forEach(t => {
            const prof = currentForensicTeams[t];
            if (!prof || prof.gp === 0) return;
            let val = prof;
            for (const p of path) val = val?.[p];
            if (val != null && !isNaN(parseFloat(val))) tuples.push({ team: t, val: parseFloat(val) });
        });

        tuples.sort((a, b) => asc ? (a.val - b.val) : (b.val - a.val));
        const rankedTeams = new Set();
        let r = 1;
        tuples.forEach((item, idx) => {
            rankedTeams.add(item.team);
            if (idx > 0 && item.val !== tuples[idx - 1].val) r = idx + 1;
            currentForensicTeams[item.team][rKey] = currentForensicTeams[item.team].is_small_sample ? 'Small Sample' : r;
        });

        allTeams.forEach(t => { if (!rankedTeams.has(t)) currentForensicTeams[t][rKey] = null; });
    });

    const zoneMap = {
        restricted_area: { off: 'rank_ra_off_freq', def: 'rank_ra_ppp_allowed' },
        paint_non_ra: { off: 'rank_paint_off_freq', def: 'rank_paint_ppp_allowed' },
        mid_range: { off: 'rank_mid_off_freq', def: 'rank_mid_ppp_allowed' },
        corner_3: { off: 'rank_corner3_off_freq', def: 'rank_corner3_ppp_allowed' },
        above_break_3: { off: 'rank_above_break3_off_freq', def: 'rank_above_break3_ppp_allowed' }
    };
    allTeams.forEach(t => {
        const prof = currentForensicTeams[t];
        if (!prof) return;
        if (prof.playtypes_defense) {
            if (prof.playtypes_defense.PnR_Ball_Handler) prof.playtypes_defense.PnR_Ball_Handler.rank = prof.rank_pnr_bh_allowed_ppp;
            if (prof.playtypes_defense.Spot_Up) prof.playtypes_defense.Spot_Up.rank = prof.rank_spot_up_allowed_ppp;
            if (prof.playtypes_defense.Isolation) prof.playtypes_defense.Isolation.rank = prof.rank_iso_allowed_ppp;
            if (prof.playtypes_defense.PnR_Roll_Man) prof.playtypes_defense.PnR_Roll_Man.rank = prof.rank_pnr_rm_allowed_ppp;
        }
        Object.entries(zoneMap).forEach(([zKey, rKeys]) => {
            if (prof.shot_profile_offense?.[zKey]) prof.shot_profile_offense[zKey].rank = prof[rKeys.off];
            if (prof.shot_profile_allowed?.[zKey]) prof.shot_profile_allowed[zKey].rank = prof[rKeys.def];
        });
        if (prof.positional_allowances) {
            ['pts', 'reb', 'fg3m'].forEach(st => {
                if (prof.positional_allowances[st]) {
                    prof.positional_allowances[st].guards_rank = prof[`rank_${st}_allowed_guards`];
                    prof.positional_allowances[st].wings_rank = prof[`rank_${st}_allowed_wings`];
                    prof.positional_allowances[st].bigs_rank = prof[`rank_${st}_allowed_bigs`];
                }
            });
        }
    });

    currentPlayerProfiles = (allPlayerProfiles || []).map(p => {
        const pCopy = { ...p };
        const logs = p.all_logs || [];
        const matchingLogs = logs.filter(lg => lg.d >= sDate && lg.d <= eDate);

        if (matchingLogs.length === 0) {
            pCopy.games_played = 0;
            pCopy.is_small_sample = true;
            pCopy.season_avg = { pts: null, reb: null, ast: null, min: null, fg3a: null, fg3m: null, paint_pts: null };
            pCopy.distribution_10g = {
                pts: { floor: null, median: null, ceiling: null, logs: [] },
                reb: { floor: null, median: null, ceiling: null, logs: [] },
                ast: { floor: null, median: null, ceiling: null, logs: [] }
            };
        } else {
            const pGp = matchingLogs.length;
            pCopy.games_played = pGp;
            pCopy.is_small_sample = pGp < 10;
            const avg = arr => (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1);
            pCopy.season_avg = {
                pts: avg(matchingLogs.map(l => l.pts)), reb: avg(matchingLogs.map(l => l.reb)),
                ast: avg(matchingLogs.map(l => l.ast)), min: avg(matchingLogs.map(l => l.min)),
                fg3a: avg(matchingLogs.map(l => l.fg3a)), fg3m: avg(matchingLogs.map(l => l.fg3m)),
                paint_pts: avg(matchingLogs.map(l => l.paint_pts || 0))
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

let currentInsightsMatchup = null;

function switchTab(tabId) {
    const schedSection = document.getElementById('tab-schedule');
    const profileSection = document.getElementById('tab-profiles');
    const insightsSection = document.getElementById('tab-insights');
    const positionalSection = document.getElementById('tab-positional');
    const btnSched = document.getElementById('tab-btn-schedule');
    const btnProfiles = document.getElementById('tab-btn-profiles');
    const btnInsights = document.getElementById('tab-btn-insights');
    const btnPositional = document.getElementById('tab-btn-positional');

    const activeClass = 'py-2.5 px-1 border-b-2 border-cyan-400 text-slate-100 font-semibold text-xs flex items-center space-x-2 transition whitespace-nowrap';
    const inactiveClass = 'py-2.5 px-1 border-b-2 border-transparent text-slate-400 hover:text-slate-200 font-medium text-xs flex items-center space-x-2 transition whitespace-nowrap';

    if (schedSection) schedSection.classList.toggle('hidden', tabId !== 'schedule');
    if (profileSection) profileSection.classList.toggle('hidden', tabId !== 'profiles');
    if (insightsSection) insightsSection.classList.toggle('hidden', tabId !== 'insights');
    if (positionalSection) positionalSection.classList.toggle('hidden', tabId !== 'positional');

    if (btnSched) btnSched.className = (tabId === 'schedule') ? activeClass : inactiveClass;
    if (btnProfiles) btnProfiles.className = (tabId === 'profiles') ? activeClass : inactiveClass;
    if (btnInsights) btnInsights.className = (tabId === 'insights') ? activeClass : inactiveClass;
    if (btnPositional) btnPositional.className = (tabId === 'positional') ? activeClass : inactiveClass;

    if (tabId === 'insights' && !currentInsightsMatchup) {
        loadInsightsTab();
    }
    if (tabId === 'positional' && !window.hasLoadedPositional) {
        if (typeof loadPositionalTab === 'function') loadPositionalTab();
    }
    safeCreateIcons();
}

const STAT_CARD_METRICS = [
    { label: 'Points / Game', key: 'pts_scored', rankKey: 'rank_pts_scored' }, { label: 'Points Allowed / G', key: 'pts_allowed', rankKey: 'rank_pts_allowed' },
    { label: 'Rebounds / Game', key: 'reb_total', rankKey: 'rank_reb_total' }, { label: 'Rebounds Allowed / G', key: 'reb_allowed', rankKey: 'rank_reb_allowed' },
    { label: 'OREB%', key: 'oreb_pct', rankKey: 'rank_oreb_pct', isPct: true }, { label: 'DREB%', key: 'dreb_pct', rankKey: 'rank_dreb_pct', isPct: true },
    { label: 'Assists / Game', key: 'ast_total', rankKey: 'rank_ast_total' }, { label: 'Assists Allowed / G', key: 'ast_allowed', rankKey: 'rank_ast_allowed' },
    { label: 'Turnovers / G', key: 'tov_total', rankKey: 'rank_tov_total' }, { label: 'Opp Turnovers / G', key: 'tov_allowed', rankKey: 'rank_tov_allowed' },
    { label: 'Threes Made (3PM)', key: 'fg3m_total', rankKey: 'rank_fg3m_total' }, { label: '3PM Allowed / G', key: 'fg3m_allowed', rankKey: 'rank_fg3m_allowed' },
    { label: 'Threes Attempted (3PA)', key: 'fg3a_total', rankKey: 'rank_fg3a_total' }, { label: '3PA Allowed / G', key: 'fg3a_allowed', rankKey: 'rank_fg3a_allowed' },
    { label: 'FG% (Field Goal %)', key: 'fg_pct', rankKey: 'rank_fg_pct', isPct: true }, { label: 'FG% Allowed', key: 'fg_pct_opp', rankKey: 'rank_fg_pct_opp', isPct: true },
    { label: 'Pace (Poss/48m)', key: 'pace', rankKey: 'rank_pace' }, { label: 'Offensive Rating', key: 'off_rtg', rankKey: 'rank_off_rtg' }, { label: 'Defensive Rating', key: 'def_rtg', rankKey: 'rank_def_rtg' },
    ...SIGNIFICANT_POS_KEYS.map(k => ({ label: POS_METRIC_LABELS[k], key: k, rankKey: 'rank_' + k }))
];

function renderFunnelZone(label, aGen, hAllow, aTeam, hTeam) {
    const aW = Math.min(100, (parseFloat(aGen) || 0) * 2);
    const hW = Math.min(100, (parseFloat(hAllow) || 0) * 2);
    const labelWithTip = (window.formatMetricLabelWithTooltip) 
        ? window.formatMetricLabelWithTooltip(label) 
        : `<span>${label}</span>`;
    return `
        <div class="bg-[#161b22] p-2 rounded-lg border border-[#21262d] space-y-1">
            <div class="flex items-center justify-between text-[10px] font-bold text-slate-300">
                ${labelWithTip}
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

    const aSampleCount = a.gp || 0;
    const aIsSmall = aSampleCount < 10;
    const aSampleClass = aIsSmall ? 'text-amber-400 font-semibold' : 'text-slate-400';
    const aSampleLabel = aIsSmall ? `[Small Sample: n=${aSampleCount}]` : `[n=${aSampleCount} games]`;
    const hSampleCount = h.gp || 0;
    const hIsSmall = hSampleCount < 10;
    const hSampleClass = hIsSmall ? 'text-amber-400 font-semibold' : 'text-slate-400';
    const hSampleLabel = hIsSmall ? `[Small Sample: n=${hSampleCount}]` : `[n=${hSampleCount} games]`;

    const aInjuryBadge = renderInjuryBadge(a.injuries || allForensicTeams[aTeam]?.injuries || []);
    const hInjuryBadge = renderInjuryBadge(h.injuries || allForensicTeams[hTeam]?.injuries || []);
    const statusBadge = isFinal
        ? `<span class="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 flex items-center space-x-1"><span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span><span>FINAL</span></span>`
        : `<span class="px-2 py-0.5 rounded text-[10px] font-semibold bg-[#21262d] text-slate-400 border border-[#30363d]">SCHEDULED</span>`;

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
                    <button onclick="event.stopPropagation(); openMatchupInsights('${aTeam}', '${hTeam}')" class="px-2.5 py-1 rounded-lg text-xs font-semibold bg-cyan-950/60 hover:bg-cyan-900/80 text-cyan-300 border border-cyan-800/60 transition flex items-center space-x-1.5 shadow-sm active:scale-95 cursor-pointer" title="View Matchup Insights">
                        <i data-lucide="brain" class="w-3.5 h-3.5 text-cyan-400"></i>
                        <span>Insights</span>
                    </button>
                </div>
            </div>

            <div onclick="openMatchupInsights('${aTeam}', '${hTeam}')" class="grid grid-cols-12 items-center py-1.5 gap-2 cursor-pointer hover:bg-[#21262d]/40 rounded-lg px-1 transition" title="Click to view Matchup Insights for ${aTeam} @ ${hTeam}">
                <div class="col-span-5 flex items-center space-x-2.5 min-w-0">
                    <div class="w-10 h-10 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center p-1.5 shadow-inner shrink-0 relative overflow-hidden">
                        <img src="${getTeamLogoUrl(aTeam)}" alt="${aTeam}" class="w-full h-full object-contain" onerror="this.onerror=null; this.style.display='none'; this.nextElementSibling.style.display='flex';"/>
                        <span class="font-extrabold text-cyan-400 font-mono text-xs hidden items-center justify-center w-full h-full">${aTeam}</span>
                    </div>
                    <div class="min-w-0 flex-1">
                        <h4 class="font-bold text-slate-100 text-xs sm:text-sm truncate">${g.away_team_name}</h4>
                        <div class="text-[11px] text-slate-400 flex items-center flex-wrap gap-x-1.5 mt-0.5 font-mono">
                            <span class="text-slate-300 font-semibold">${g.away_record || (a.gp > 0 ? `${a.gp} GP` : '--')}</span>
                            <span class="text-slate-600">•</span>
                            <span class="text-slate-500 font-medium">Away</span>
                            <span class="${aSampleClass} text-[10px]">${aSampleLabel}</span>
                            ${aInjuryBadge}
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
                            ${hInjuryBadge}
                            <span class="${hSampleClass} text-[10px]">${hSampleLabel}</span>
                            <span class="text-slate-500 font-medium">Home</span>
                            <span class="text-slate-600">•</span>
                            <span class="text-slate-300 font-semibold">${g.home_record || (h.gp > 0 ? `${h.gp} GP` : '--')}</span>
                        </div>
                    </div>
                    <div class="w-10 h-10 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center p-1.5 shadow-inner shrink-0 relative overflow-hidden">
                        <img src="${getTeamLogoUrl(hTeam)}" alt="${hTeam}" class="w-full h-full object-contain" onerror="this.onerror=null; this.style.display='none'; this.nextElementSibling.style.display='flex';"/>
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

    // Strict 2026-27 Season Schedule Filter
    let games = allScheduleGames.filter(g => g.gameday && g.gameday >= '2026-10-20');

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
                <p class="text-xs text-slate-500">Pick another slate date or reset your team filter.</p>
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

    const seasonGames = allScheduleGames.filter(g => g.gameday && g.gameday >= '2026-10-20');

    const dateCounts = {};
    seasonGames.forEach(g => {
        const d = g.gameday;
        dateCounts[d] = (dateCounts[d] || 0) + 1;
    });

    // Chronological order: Opening Night 2026-10-20 first, then upcoming dates
    const sortedDates = Object.keys(dateCounts).sort();
    const isAll = selectedScheduleDate === 'all';

    let html = `
        <button onclick="filterScheduleDate('all')" class="px-3 py-1.5 rounded-lg text-xs font-semibold whitespace-nowrap transition border shrink-0 ${isAll ? 'bg-cyan-950/60 text-cyan-300 border-cyan-700/60 font-bold' : 'bg-[#21262d] text-slate-400 hover:text-slate-200 border-[#30363d]'}">
            All Dates (${sortedDates.length})
        </button>
    `;

    sortedDates.forEach(d => {
        const isActive = selectedScheduleDate === d;
        html += `
            <button onclick="filterScheduleDate('${d}')" class="px-3 py-1.5 rounded-lg text-xs font-semibold whitespace-nowrap transition border shrink-0 ${isActive ? 'bg-cyan-950/60 text-cyan-300 border-cyan-700/60 font-bold' : 'bg-[#21262d] text-slate-400 hover:text-slate-200 border-[#30363d]'}">
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
    const seasonMap = {
        '2026': { title: '2026-27 NBA Forensic Matchup Lab', pill: '2026-27 Tracking', s: '2026-10-20', e: '2027-04-14' },
        '2025': { title: '2025-26 NBA Forensic Matchup Lab', pill: '2025-26 to Present', s: '2025-10-02', e: getTodayDateStr() }
    };
    const cfg = seasonMap[season] || { title: 'Full NBA Forensic Matchup Lab', pill: 'All Seasons', s: '2025-10-02', e: '2027-04-14' };
    if (titleEl) titleEl.textContent = cfg.title;
    if (seasonPill) seasonPill.textContent = cfg.pill;
    startDate = cfg.s; endDate = cfg.e;
    const startIn = document.getElementById('input-start-date'), endIn = document.getElementById('input-end-date');
    if (startIn) startIn.value = startDate;
    if (endIn) endIn.value = endDate;
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
    openMatchupInsights(away, home);
}

function openMatchupInsights(awayTeam, homeTeam) {
    if (!awayTeam || !homeTeam) return;
    currentInsightsMatchup = {
        away: awayTeam.toUpperCase(),
        home: homeTeam.toUpperCase()
    };
    switchTab('insights');
    populateInsightsMatchupDropdown(currentInsightsMatchup.away, currentInsightsMatchup.home);
    loadMatchupInsights(currentInsightsMatchup.away, currentInsightsMatchup.home);
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function loadInsightsTab() {
    populateInsightsMatchupDropdown(currentInsightsMatchup?.away, currentInsightsMatchup?.home);
    if (!currentInsightsMatchup) {
        const selectEl = document.getElementById('insights-matchup-select');
        if (selectEl && selectEl.value) {
            const parts = selectEl.value.split('_');
            if (parts.length >= 2) {
                currentInsightsMatchup = { away: parts[0], home: parts[1] };
            }
        }
    }
    if (currentInsightsMatchup) {
        loadMatchupInsights(currentInsightsMatchup.away, currentInsightsMatchup.home);
    }
}

function populateInsightsMatchupDropdown(selectedAway = null, selectedHome = null) {
    const selectEl = document.getElementById('insights-matchup-select');
    if (!selectEl) return;

    let games = allScheduleGames.filter(g => g.gameday && g.gameday >= '2026-10-20');
    if (selectedScheduleDate && selectedScheduleDate !== 'all') {
        const dateFiltered = games.filter(g => g.gameday === selectedScheduleDate);
        if (dateFiltered.length > 0) games = dateFiltered;
    }

    if (games.length === 0) {
        games = allScheduleGames.slice(0, 30);
    }

    if (selectedAway && selectedHome) {
        const aU = selectedAway.toUpperCase();
        const hU = selectedHome.toUpperCase();
        const found = games.some(g => (g.away_team || '').toUpperCase() === aU && (g.home_team || '').toUpperCase() === hU);
        if (!found) {
            games.unshift({
                away_team: aU,
                home_team: hU,
                gameday: 'Matchup Lab',
                gametime: 'Custom Alignment'
            });
        }
    }

    const currentVal = selectEl.value;
    let targetKey = (selectedAway && selectedHome) ? `${selectedAway.toUpperCase()}_${selectedHome.toUpperCase()}` : currentVal;
    let optionsHtml = '';

    games.forEach((g, idx) => {
        const key = `${g.away_team}_${g.home_team}`;
        const label = `${g.gameday ? g.gameday + ': ' : ''}${g.away_team} @ ${g.home_team}${g.gametime ? ' (' + g.gametime + ')' : ''}`;
        if (idx === 0 && !targetKey) targetKey = key;
        optionsHtml += `<option value="${key}">${label}</option>`;
    });

    selectEl.innerHTML = optionsHtml;
    if (targetKey) {
        selectEl.value = targetKey;
        const parts = targetKey.split('_');
        currentInsightsMatchup = { away: parts[0], home: parts[1] };
    }
}

function onInsightsMatchupSelectChange(value) {
    if (!value) return;
    const parts = value.split('_');
    if (parts.length >= 2) {
        currentInsightsMatchup = { away: parts[0].toUpperCase(), home: parts[1].toUpperCase() };
        loadMatchupInsights(currentInsightsMatchup.away, currentInsightsMatchup.home);
    }
}

function refreshCurrentInsights() {
    if (!currentInsightsMatchup) {
        loadInsightsTab();
        return;
    }
    loadMatchupInsights(currentInsightsMatchup.away, currentInsightsMatchup.home);
}

function loadMatchupInsights(awayTeam, homeTeam) {
    const container = document.getElementById('insights-view-container');
    if (!container) return;
    const aTeam = awayTeam.toUpperCase(), hTeam = homeTeam.toUpperCase();
    const a = currentForensicTeams[aTeam] || allForensicTeams[aTeam] || {};
    const h = currentForensicTeams[hTeam] || allForensicTeams[hTeam] || {};
    const aPlayers = (currentPlayerProfiles?.length ? currentPlayerProfiles : allPlayerProfiles).filter(p => p.team === aTeam);
    const hPlayers = (currentPlayerProfiles?.length ? currentPlayerProfiles : allPlayerProfiles).filter(p => p.team === hTeam);

    let g = allScheduleGames.find(x => (x.away_team === aTeam && x.home_team === hTeam) || (x.away_team === hTeam && x.home_team === aTeam));
    const baseG = g || {
        game_id: `NBA_${aTeam}_${hTeam}`, gameday: '2026-10-20', gametime: '7:30 PM ET',
        away_team: aTeam, away_team_name: teamNamesMap[aTeam] || aTeam, away_record: (a.gp > 0 ? `${a.gp} GP` : '--'),
        home_team: hTeam, home_team_name: teamNamesMap[hTeam] || hTeam, home_record: (h.gp > 0 ? `${h.gp} GP` : '--'),
        is_final: false, home_def_sim: {}, away_def_sim: {}
    };
    container.innerHTML = generateDeepDiveModalHtml({ ...baseG, away_forensic: a, home_forensic: h, away_players: aPlayers, home_players: hPlayers });
    safeCreateIcons(container);
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
        setTimeout(() => openMatchupInsights(initAway, initHome), 150);
    }
});

// Global Escape key listener to close Deep Dive modal
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeDeepDiveModal();
    }
});
