/**
 * NBA Forensic Scouting Engine - Player Floor Streaks Tab
 * ========================================================
 * Evaluates unbroken consecutive game streaks where player production
 * meets or exceeds a hard floor threshold line (-x% margin off baseline median).
 * Supports Points (PTS), Rebounds (REB), and Assists (AST) with dynamic margin controls.
 * Strictly descriptive, zero predictive/betting terminology.
 */

let floorMargin = 0.30;
let floorFilterStat = 'PTS';
let floorFilterPos = 'ALL';
let floorFilterStreak = '1';
let floorFilterMinGames = 4;
let floorSearchQuery = '';
let floorSortCol = 'floor_streak';
let floorSortAsc = false;
let floorSelectedDate = '';

function computePlayerFloorData(players, statKey, margin, minGames) {
    let rawStat = (statKey || 'PTS').toString().trim().toUpperCase();
    if (rawStat === 'THREES') rawStat = '3PM';
    if (!['PTS', 'REB', 'AST', 'PA', 'PR', 'RA', '3PM'].includes(rawStat)) {
        rawStat = 'PTS';
    }
    const results = [];

    // Map next upcoming game per team from allScheduleGames
    const teamUpcomingMap = {};
    (allScheduleGames || []).forEach(g => {
        const d = g.gameday;
        if (!d) return;
        const h = (g.home_team || '').toUpperCase();
        const a = (g.away_team || '').toUpperCase();
        if (h && !teamUpcomingMap[h]) teamUpcomingMap[h] = { date: d, opponent: a, matchup: `${h} vs ${a}` };
        if (a && !teamUpcomingMap[a]) teamUpcomingMap[a] = { date: d, opponent: h, matchup: `${a} @ ${h}` };
    });

    (players || []).forEach(p => {
        let logs = p.all_logs || [];
        if (startDate && endDate) {
            logs = logs.filter(lg => lg.d >= startDate && lg.d <= endDate);
        } else if (startDate) {
            logs = logs.filter(lg => lg.d >= startDate);
        } else if (endDate) {
            logs = logs.filter(lg => lg.d <= endDate);
        }

        if (logs.length < minGames) return;

        const vals = logs.map(lg => {
            if (rawStat === 'PA') {
                const pts = typeof lg.pts === 'number' ? lg.pts : (parseFloat(lg.pts) || 0);
                const ast = typeof lg.ast === 'number' ? lg.ast : (parseFloat(lg.ast) || 0);
                return pts + ast;
            } else if (rawStat === 'PR') {
                const pts = typeof lg.pts === 'number' ? lg.pts : (parseFloat(lg.pts) || 0);
                const reb = typeof lg.reb === 'number' ? lg.reb : (parseFloat(lg.reb) || 0);
                return pts + reb;
            } else if (rawStat === 'RA') {
                const reb = typeof lg.reb === 'number' ? lg.reb : (parseFloat(lg.reb) || 0);
                const ast = typeof lg.ast === 'number' ? lg.ast : (parseFloat(lg.ast) || 0);
                return reb + ast;
            } else if (rawStat === '3PM') {
                const fg3m = typeof lg.fg3m === 'number' ? lg.fg3m : (parseFloat(lg.fg3m) || 0);
                return fg3m;
            } else {
                const v = lg[rawStat.toLowerCase()];
                return typeof v === 'number' ? v : (parseFloat(v) || 0);
            }
        });

        if (vals.length === 0) return;

        // Baseline median calculated strictly from historical production window
        const sorted = [...vals].sort((a, b) => a - b);
        const n = sorted.length;
        const baselineMedian = n % 2 === 1
            ? sorted[Math.floor(n / 2)]
            : parseFloat(((sorted[n / 2 - 1] + sorted[n / 2]) / 2).toFixed(1));

        // Hard floor threshold line: Median * (1 - margin)
        const floorThreshold = Number(((1.0 - margin) * baselineMedian).toFixed(1));

        // Consecutive unbroken games meeting or exceeding floor threshold from most recent backwards
        let streak = 0;
        if (baselineMedian > 0 && floorThreshold > 0) {
            for (let i = 0; i < vals.length; i++) {
                if (vals[i] >= floorThreshold) {
                    streak++;
                } else {
                    break;
                }
            }
        }

        let tier = 'Breached (0)';
        if (streak >= 8) tier = 'Consistent (8+)';
        else if (streak >= 5) tier = 'Solid Floor (5-7)';
        else if (streak >= 1) tier = 'Developing (1-4)';

        const teamCode = (p.team || '').toUpperCase();
        const upGame = teamUpcomingMap[teamCode] || {};

        results.push({
            player_name: p.player_name || 'Unknown',
            team: teamCode,
            team_name: p.team_name || teamCode,
            position: p.position || '--',
            stat_key: rawStat,
            games_played: logs.length,
            baseline_median: baselineMedian,
            margin_used: margin,
            floor_threshold: floorThreshold,
            floor_streak: streak,
            reliability_tier: tier,
            recent_logs: vals.slice(0, 10),
            matchup: upGame.matchup || '',
            next_date: upGame.date || '',
            next_opponent: upGame.opponent || '',
            raw: p
        });
    });

    return results;
}

function updateFloorKPIs(computedList) {
    const longestEl = document.getElementById('floor-kpi-longest');
    const consistentEl = document.getElementById('floor-kpi-consistent');
    const solidEl = document.getElementById('floor-kpi-solid');
    const devEl = document.getElementById('floor-kpi-developing');

    if (!computedList || computedList.length === 0) {
        if (longestEl) longestEl.innerHTML = '--';
        if (consistentEl) consistentEl.textContent = '0 players';
        if (solidEl) solidEl.textContent = '0 players';
        if (devEl) devEl.textContent = '0 players';
        return;
    }

    let longest = null;
    for (const p of computedList) {
        if ((p.floor_streak || 0) > 0) {
            if (!longest || p.floor_streak > longest.floor_streak ||
                (p.floor_streak === longest.floor_streak && (p.baseline_median || 0) > (longest.baseline_median || 0))) {
                longest = p;
            }
        }
    }

    if (longestEl) {
        if (longest) {
            longestEl.innerHTML = `
                <div class="flex items-center space-x-1.5 truncate">
                    <span class="font-bold text-slate-100">${longest.player_name}</span>
                    <span class="text-slate-400">(${longest.team})</span>
                    <span class="text-cyan-400 font-extrabold">• ${longest.floor_streak} games</span>
                </div>
            `;
        } else {
            longestEl.innerHTML = '<span class="text-slate-500 font-medium">None active (0 games)</span>';
        }
    }

    const consistentCount = computedList.filter(p => p.floor_streak >= 8).length;
    if (consistentEl) consistentEl.textContent = `${consistentCount} players`;

    const solidCount = computedList.filter(p => p.floor_streak >= 5 && p.floor_streak < 8).length;
    if (solidEl) solidEl.textContent = `${solidCount} players`;

    const devCount = computedList.filter(p => p.floor_streak >= 1 && p.floor_streak < 5).length;
    if (devEl) devEl.textContent = `${devCount} players`;
}

function renderFloorDateChips() {
    const container = document.getElementById('floor-date-chips') || document.getElementById('profile-date-chips');
    if (!container) return;

    const dateTeamsMap = {};
    (allScheduleGames || []).forEach(g => {
        const d = g.gameday;
        if (!d || (startDate && d < startDate) || (endDate && d > endDate)) return;
        if (!dateTeamsMap[d]) dateTeamsMap[d] = new Set();
        if (g.home_team) dateTeamsMap[d].add(g.home_team.toUpperCase());
        if (g.away_team) dateTeamsMap[d].add(g.away_team.toUpperCase());
    });

    const dateCounts = {};
    const baseList = (currentPlayerProfiles && currentPlayerProfiles.length) ? currentPlayerProfiles : allPlayerProfiles;
    Object.keys(dateTeamsMap).forEach(d => {
        const teams = dateTeamsMap[d];
        let count = 0;
        baseList.forEach(p => {
            if (teams.has((p.team || '').toUpperCase())) count++;
        });
        if (count > 0) dateCounts[d] = count;
    });

    const sortedDates = Object.keys(dateCounts).sort().reverse().slice(0, 15);

    let html = `
        <button id="btn-floor-all-dates" onclick="clearFloorDateFilter()" class="chip ${!floorSelectedDate ? 'bg-[#30363d] text-slate-100 font-bold' : 'text-slate-400 hover:text-slate-200 bg-[#21262d]'} text-[11px] py-0.5 px-2.5 rounded shrink-0 transition" title="Show all slates">
            All Slates
        </button>
    `;

    sortedDates.forEach(d => {
        const isActive = floorSelectedDate === d;
        html += `
            <button onclick="setFloorDate('${d}')" class="chip ${isActive ? 'bg-[#30363d] text-cyan-300 font-bold border border-cyan-800/60' : 'text-slate-400 hover:text-slate-200 bg-[#21262d]'} text-[11px] py-0.5 px-2 rounded shrink-0 transition">
                ${d} <span class="text-[10px] text-slate-500 font-mono">(${dateCounts[d]})</span>
            </button>
        `;
    });

    container.innerHTML = html;
}

function setFloorDate(d) {
    floorSelectedDate = d;
    const picker = document.getElementById('floor-date-picker') || document.getElementById('profile-date-picker');
    if (picker) picker.value = d;
    renderFloorDateChips();
    renderFloorStreaks();
}

function clearFloorDateFilter() {
    floorSelectedDate = '';
    const picker = document.getElementById('floor-date-picker') || document.getElementById('profile-date-picker');
    if (picker) picker.value = '';
    renderFloorDateChips();
    renderFloorStreaks();
}

function onFloorDateChange(val) {
    floorSelectedDate = val || '';
    renderFloorDateChips();
    renderFloorStreaks();
}

function setFloorMarginFilter(val) {
    floorMargin = parseFloat(val) || 0.30;
    const badge = document.getElementById('floor-margin-badge');
    if (badge) {
        const pct = Math.round(floorMargin * 100);
        badge.textContent = `Hard Line Threshold (-${pct}% Margin)`;
    }
    renderFloorStreaks();
}

function setFloorStatFilter(stat) {
    let sNorm = (stat || 'PTS').toString().trim().toUpperCase();
    if (sNorm === 'THREES') sNorm = '3PM';
    if (!['PTS', 'REB', 'AST', 'PA', 'PR', 'RA', '3PM'].includes(sNorm)) {
        sNorm = 'PTS';
    }
    floorFilterStat = sNorm;
    ['PTS', 'REB', 'AST', 'PA', 'PR', 'RA', '3PM'].forEach(s => {
        const btn = document.getElementById(`floor-filter-stat-${s}`) || document.getElementById(`profile-filter-stat-${s}`);
        if (btn) {
            const isActive = (s === floorFilterStat);
            btn.className = isActive
                ? 'px-2.5 py-1 rounded bg-[#30363d] text-cyan-300 font-bold border border-cyan-800/60 shrink-0 cursor-pointer touch-manipulation select-none'
                : 'px-2.5 py-1 rounded text-slate-400 hover:text-slate-200 font-medium shrink-0 cursor-pointer touch-manipulation select-none';
            btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
        }
    });
    renderFloorStreaks();
}

function setFloorPosFilter(pos) {
    floorFilterPos = (pos || 'ALL').toUpperCase();
    ['ALL', 'PG', 'SG', 'SF', 'PF', 'C'].forEach(p => {
        const btn = document.getElementById(`floor-filter-pos-${p}`) || document.getElementById(`profile-filter-pos-${p}`);
        if (btn) {
            btn.className = (p === floorFilterPos)
                ? 'px-2.5 py-1 rounded bg-[#30363d] text-slate-100 font-semibold'
                : 'px-2.5 py-1 rounded text-slate-400 hover:text-slate-200 font-medium';
        }
    });
    renderFloorStreaks();
}

function setFloorStreakFilter(val) {
    floorFilterStreak = val || '0';
    renderFloorStreaks();
}

function setFloorMinGamesFilter(val) {
    floorFilterMinGames = parseInt(val, 10) || 4;
    renderFloorStreaks();
}

function onFloorSearch(query) {
    floorSearchQuery = query || '';
    renderFloorStreaks();
}

function sortFloorStreaks(col) {
    if (floorSortCol === col) {
        floorSortAsc = !floorSortAsc;
    } else {
        floorSortCol = col;
        floorSortAsc = (col === 'player_name' || col === 'team' || col === 'position');
    }
    renderFloorStreaks();
}

function reloadFloorStreaks() {
    renderFloorDateChips();
    renderFloorStreaks();
    if (typeof safeCreateIcons === 'function') safeCreateIcons();
}

function renderFloorStreaks() {
    const tbody = document.getElementById('floor-streak-table-body') || document.getElementById('player-profile-table-body');
    const countEl = document.getElementById('floor-visible-count') || document.getElementById('profile-visible-count');
    if (!tbody) return;

    const baseList = (currentPlayerProfiles && currentPlayerProfiles.length) ? currentPlayerProfiles : allPlayerProfiles;
    let computed = computePlayerFloorData(baseList, floorFilterStat, floorMargin, floorFilterMinGames);

    updateFloorKPIs(computed);

    // Filter by Game Slate Date
    if (floorSelectedDate) {
        const gamesOnDate = (allScheduleGames || []).filter(g => g.gameday === floorSelectedDate);
        const teamsOnDate = new Set();
        const slateMatchups = {};
        gamesOnDate.forEach(g => {
            const h = (g.home_team || '').toUpperCase();
            const a = (g.away_team || '').toUpperCase();
            if (h) {
                teamsOnDate.add(h);
                slateMatchups[h] = { matchup: `${h} vs ${a}`, opponent: a, date: floorSelectedDate };
            }
            if (a) {
                teamsOnDate.add(a);
                slateMatchups[a] = { matchup: `${a} @ ${h}`, opponent: h, date: floorSelectedDate };
            }
        });

        computed = computed.filter(p => teamsOnDate.has(p.team));
        computed.forEach(p => {
            if (slateMatchups[p.team]) {
                p.matchup = slateMatchups[p.team].matchup;
                p.next_date = slateMatchups[p.team].date;
                p.next_opponent = slateMatchups[p.team].opponent;
            }
        });
    }

    // Filter by Position
    if (floorFilterPos && floorFilterPos !== 'ALL') {
        computed = computed.filter(p => (p.position || '').toUpperCase() === floorFilterPos);
    }

    // Filter by Minimum Streak
    const minStreak = parseInt(floorFilterStreak, 10) || 0;
    if (minStreak > 0) {
        computed = computed.filter(p => (p.floor_streak || 0) >= minStreak);
    }

    // Filter by Search Query
    if (floorSearchQuery) {
        const q = floorSearchQuery.toLowerCase().trim();
        computed = computed.filter(p =>
            (p.player_name || '').toLowerCase().includes(q) ||
            (p.team || '').toLowerCase().includes(q) ||
            (p.team_name || '').toLowerCase().includes(q)
        );
    }

    // Sorting
    computed.sort((a, b) => {
        let vA = a[floorSortCol];
        let vB = b[floorSortCol];

        if (floorSortCol === 'rank') {
            vA = a.floor_streak;
            vB = b.floor_streak;
        }

        if (typeof vA === 'string') vA = vA.toLowerCase();
        if (typeof vB === 'string') vB = vB.toLowerCase();
        if (vA < vB) return floorSortAsc ? -1 : 1;
        if (vA > vB) return floorSortAsc ? 1 : -1;

        if (floorSortCol === 'floor_streak' || floorSortCol === 'rank') {
            const medA = a.baseline_median || 0;
            const medB = b.baseline_median || 0;
            if (medA < medB) return floorSortAsc ? -1 : 1;
            if (medA > medB) return floorSortAsc ? 1 : -1;
            const nameA = a.player_name || '';
            const nameB = b.player_name || '';
            if (nameA < nameB) return -1;
            if (nameA > nameB) return 1;
        }
        return 0;
    });

    if (countEl) countEl.textContent = computed.length;

    if (computed.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="12" class="py-12 text-center text-slate-500">
                    <i data-lucide="inbox" class="w-6 h-6 mx-auto mb-2 text-slate-600"></i>
                    <p class="font-medium text-slate-400">No players match the current filters.</p>
                    <p class="text-[11px] text-slate-500 mt-1">Try lowering the streak threshold or clearing the game slate filter.</p>
                </td>
            </tr>
        `;
        if (typeof safeCreateIcons === 'function') safeCreateIcons(tbody);
        return;
    }

    const marginPct = Math.round(floorMargin * 100);
    const STAT_LABELS = {
        'PTS': 'Points (PTS)',
        'REB': 'Rebounds (REB)',
        'AST': 'Assists (AST)',
        'PA': 'Points + Assists (PA)',
        'PR': 'Points + Rebounds (PR)',
        'RA': 'Rebounds + Assists (RA)',
        '3PM': '3-Pointers Made (3PM)',
        'THREES': '3-Pointers Made (3PM)'
    };
    const statLabel = STAT_LABELS[floorFilterStat] || floorFilterStat;

    tbody.innerHTML = computed.map((p, idx) => {
        const streak = p.floor_streak || 0;
        let tierBadge = '<span class="px-2 py-0.5 rounded text-[10px] font-mono bg-[#161b22] text-amber-500/80 border border-[#30363d]">Breached (0)</span>';
        if (streak >= 8) {
            tierBadge = '<span class="px-2 py-0.5 rounded text-[10px] font-mono bg-emerald-950/80 text-emerald-400 border border-emerald-800 font-bold">Consistent (8+)</span>';
        } else if (streak >= 5) {
            tierBadge = '<span class="px-2 py-0.5 rounded text-[10px] font-mono bg-cyan-950/80 text-cyan-300 border border-cyan-800 font-semibold">Solid Floor (5-7)</span>';
        } else if (streak >= 1) {
            tierBadge = '<span class="px-2 py-0.5 rounded text-[10px] font-mono bg-[#21262d] text-slate-300 border border-[#30363d]">Developing (1-4)</span>';
        }

        const gameLogsHtml = (p.recent_logs || []).map(val => {
            const num = parseFloat(val);
            const meetsFloor = !isNaN(num) && p.floor_threshold > 0 && num >= p.floor_threshold;
            return `<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-mono ${meetsFloor ? 'bg-cyan-950/60 text-cyan-300 border border-cyan-800/80 font-bold' : 'bg-[#21262d] text-slate-400 border border-[#30363d] line-through decoration-red-500/50'}" title="${meetsFloor ? `Met Floor (${val} >= ${p.floor_threshold})` : (p.floor_threshold > 0 ? `Breached Floor (${val} < ${p.floor_threshold})` : `No Baseline Floor (${val})`)}">${val}</span>`;
        }).join(' ');

        const teamLogoUrl = (typeof window.getTeamLogoUrl === 'function')
            ? window.getTeamLogoUrl(p.team)
            : `https://a.espncdn.com/i/teamlogos/nba/500/${(p.team || '').toLowerCase()}.png`;

        return `
            <tr class="hover:bg-[#21262d]/50 transition">
                <td class="text-center font-mono text-slate-500 py-2.5 px-3">${idx + 1}</td>
                <td class="py-2.5 px-3">
                    <div class="font-semibold text-slate-100">${p.player_name}</div>
                    ${p.matchup ? `<div class="text-[10px] text-slate-500 font-mono">${p.matchup}${p.next_date ? ' • ' + p.next_date : ''}</div>` : `<div class="text-[10px] text-slate-500 font-mono">${p.team_name}</div>`}
                </td>
                <td class="text-center font-mono font-bold text-cyan-400 py-2.5 px-3">${p.position}</td>
                <td class="text-center font-mono font-bold text-slate-300 py-2.5 px-3">
                    <div class="flex items-center justify-center space-x-1.5">
                        <img src="${teamLogoUrl}" alt="${p.team}" class="w-4 h-4 object-contain shrink-0" onerror="this.style.display='none';"/>
                        <span>${p.team}</span>
                    </div>
                </td>
                <td class="py-2.5 px-3 text-slate-400 font-medium">${statLabel}</td>
                <td class="text-right font-mono font-bold text-slate-200 py-2.5 px-3">${p.baseline_median != null ? p.baseline_median : '--'}</td>
                <td class="text-center font-mono text-cyan-400 py-2.5 px-3">-${marginPct}%</td>
                <td class="text-right font-mono font-extrabold text-cyan-300 py-2.5 px-3 bg-cyan-950/20">${p.floor_threshold != null ? p.floor_threshold : '--'}</td>
                <td class="text-center font-mono font-bold text-slate-100 py-2.5 px-3">
                    <span class="inline-block px-2.5 py-0.5 rounded bg-[#161b22] border border-[#30363d] ${streak >= 5 ? 'text-cyan-300 border-cyan-800/80 font-extrabold' : ''}">${streak}</span>
                </td>
                <td class="text-center py-2.5 px-3">${tierBadge}</td>
                <td class="text-center font-mono text-slate-400 py-2.5 px-3">${p.games_played}</td>
                <td class="py-2.5 px-3 font-mono">
                    <div class="flex items-center gap-1 flex-nowrap">${gameLogsHtml || '--'}</div>
                </td>
            </tr>
        `;
    }).join('');

    if (typeof safeCreateIcons === 'function') safeCreateIcons(tbody);
}

// Backwards compatibility aliases
const renderPlayerProfiles = renderFloorStreaks;
const setProfilePosFilter = setFloorPosFilter;
const setProfileStatFilter = setFloorStatFilter;
const onProfileSearch = onFloorSearch;
const sortProfiles = sortFloorStreaks;
const renderProfileDateChips = renderFloorDateChips;
const setProfileDate = setFloorDate;
const clearProfileDateFilter = clearFloorDateFilter;
const onProfileDateChange = onFloorDateChange;
