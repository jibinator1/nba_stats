/**
 * NBA Forensic Scouting Engine - Player Usage Profiles Tab
 * ========================================================
 * Manages player usage profile table rendering, sorting, position filtering,
 * slate chips, and search filtering.
 * Strictly descriptive, zero predictive/betting terminology.
 */

function renderProfileDateChips() {
    const container = document.getElementById('profile-date-chips');
    if (!container) return;

    const dateTeamsMap = {};
    allScheduleGames.forEach(g => {
        const d = g.gameday;
        if (!d || (startDate && d < startDate) || (endDate && d > endDate)) return;
        if (!dateTeamsMap[d]) dateTeamsMap[d] = new Set();
        if (g.home_team) dateTeamsMap[d].add(g.home_team.toUpperCase());
        if (g.away_team) dateTeamsMap[d].add(g.away_team.toUpperCase());
    });

    const dateCounts = {};
    Object.keys(dateTeamsMap).forEach(d => {
        const teams = dateTeamsMap[d];
        let count = 0;
        allPlayerProfiles.forEach(p => {
            if (teams.has((p.team || '').toUpperCase())) count++;
        });
        if (count > 0) dateCounts[d] = count;
    });

    const sortedDates = Object.keys(dateCounts).sort().reverse().slice(0, 15);

    let html = `
        <button id="btn-profile-all-dates" onclick="clearProfileDateFilter()" class="chip ${!profileSelectedDate ? 'bg-[#30363d] text-slate-100 font-bold' : 'text-slate-400 hover:text-slate-200 bg-[#21262d]'} text-[11px] py-0.5 px-2.5 rounded shrink-0 transition" title="Show all slates">
            All Slates
        </button>
    `;

    sortedDates.forEach(d => {
        const isActive = profileSelectedDate === d;
        html += `
            <button onclick="setProfileDate('${d}')" class="chip ${isActive ? 'bg-[#30363d] text-slate-100 font-bold' : 'text-slate-400 hover:text-slate-200 bg-[#21262d]'} text-[11px] py-0.5 px-2 rounded shrink-0 transition">
                ${d} <span class="text-[10px] text-slate-500 font-mono">(${dateCounts[d]})</span>
            </button>
        `;
    });

    container.innerHTML = html;
}

function setProfileDate(d) {
    profileSelectedDate = d;
    const picker = document.getElementById('profile-date-picker');
    if (picker) picker.value = d;
    renderProfileDateChips();
    renderPlayerProfiles();
}

function clearProfileDateFilter() {
    profileSelectedDate = '';
    const picker = document.getElementById('profile-date-picker');
    if (picker) picker.value = '';
    renderProfileDateChips();
    renderPlayerProfiles();
}

function onProfileDateChange(val) {
    profileSelectedDate = val || '';
    renderProfileDateChips();
    renderPlayerProfiles();
}

function setProfilePosFilter(pos) {
    profileFilterPos = pos;
    ['ALL', 'PG', 'SG', 'SF', 'PF', 'C'].forEach(p => {
        const btn = document.getElementById(`profile-filter-pos-${p}`);
        if (btn) {
            btn.className = (p === pos) ? 'px-2.5 py-1 rounded bg-[#30363d] text-slate-100 font-semibold' : 'px-2.5 py-1 rounded text-slate-400 hover:text-slate-200 font-medium';
        }
    });
    renderPlayerProfiles();
}

function setProfileStatFilter(stat) {
    profileFilterStat = stat;
    ['PTS', 'REB', 'AST'].forEach(s => {
        const btn = document.getElementById(`profile-filter-stat-${s}`);
        if (btn) {
            btn.className = (s === stat) ? 'px-2.5 py-1 rounded bg-[#30363d] text-slate-100 font-semibold' : 'px-2.5 py-1 rounded text-slate-400 hover:text-slate-200 font-medium';
        }
    });
    renderPlayerProfiles();
}

function onProfileSearch(query) {
    profileSearchQuery = query || '';
    renderPlayerProfiles();
}

function sortProfiles(col) {
    if (profileSortCol === col) {
        profileSortAsc = !profileSortAsc;
    } else {
        profileSortCol = col;
        profileSortAsc = (col === 'player_name' || col === 'team' || col === 'position');
    }
    renderPlayerProfiles();
}

function renderPlayerProfiles() {
    const tbody = document.getElementById('player-profile-table-body');
    const countEl = document.getElementById('profile-visible-count');
    const kpiActive = document.getElementById('profiles-kpi-active');
    const kpiHighUsg = document.getElementById('profiles-kpi-high-usg');
    if (!tbody) return;

    let filtered = [...allPlayerProfiles];

    if (profileSelectedDate) {
        const gamesOnDate = allScheduleGames.filter(g => g.gameday === profileSelectedDate);
        const teamsOnDate = new Set();
        gamesOnDate.forEach(g => {
            if (g.home_team) teamsOnDate.add(g.home_team.toUpperCase());
            if (g.away_team) teamsOnDate.add(g.away_team.toUpperCase());
        });

        filtered = filtered.filter(p => teamsOnDate.has((p.team || '').toUpperCase()));
        if (kpiActive) kpiActive.textContent = `${filtered.length} Players (${profileSelectedDate})`;
    } else {
        if (kpiActive) kpiActive.textContent = 'All Teams (No Slate Filter)';
    }

    if (profileFilterPos && profileFilterPos !== 'ALL') {
        filtered = filtered.filter(p => (p.position || '').toUpperCase() === profileFilterPos);
    }

    if (profileSearchQuery) {
        const q = profileSearchQuery.toLowerCase().trim();
        filtered = filtered.filter(p =>
            (p.player_name || '').toLowerCase().includes(q) ||
            (p.team || '').toLowerCase().includes(q)
        );
    }

    const highUsgCount = filtered.filter(p => (p.context_stats?.usg_pct || 0) >= 28.0).length;
    if (kpiHighUsg) kpiHighUsg.textContent = `${highUsgCount} Players`;

    filtered.sort((a, b) => {
        let vA, vB;
        const statKey = profileFilterStat.toLowerCase();

        if (profileSortCol === 'player_name') { vA = a.player_name; vB = b.player_name; }
        else if (profileSortCol === 'position') { vA = a.position; vB = b.position; }
        else if (profileSortCol === 'team') { vA = a.team; vB = b.team; }
        else if (profileSortCol === 'season_avg') { vA = a.season_avg?.[statKey] || 0; vB = b.season_avg?.[statKey] || 0; }
        else if (profileSortCol === 'floor') { vA = a.distribution_10g?.[statKey]?.floor || 0; vB = b.distribution_10g?.[statKey]?.floor || 0; }
        else if (profileSortCol === 'median') { vA = a.distribution_10g?.[statKey]?.median || 0; vB = b.distribution_10g?.[statKey]?.median || 0; }
        else if (profileSortCol === 'ceiling') { vA = a.distribution_10g?.[statKey]?.ceiling || 0; vB = b.distribution_10g?.[statKey]?.ceiling || 0; }
        else { vA = a.distribution_10g?.[statKey]?.median || 0; vB = b.distribution_10g?.[statKey]?.median || 0; }

        if (typeof vA === 'string') vA = vA.toLowerCase();
        if (typeof vB === 'string') vB = vB.toLowerCase();
        if (vA < vB) return profileSortAsc ? -1 : 1;
        if (vA > vB) return profileSortAsc ? 1 : -1;
        return 0;
    });

    if (countEl) countEl.textContent = filtered.length;

    if (filtered.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="11" class="py-12 text-center text-slate-500">
                    <i data-lucide="inbox" class="w-6 h-6 mx-auto mb-2 text-slate-600"></i>
                    <p class="font-medium text-slate-400">No rotation players match current filters.</p>
                    <p class="text-[11px] text-slate-500 mt-1">Select ALL positions or clear the playing tonight slate filter.</p>
                </td>
            </tr>
        `;
        safeCreateIcons(tbody);
        return;
    }

    const statKey = profileFilterStat.toLowerCase();

    tbody.innerHTML = filtered.map((p, idx) => {
        const dist = p.distribution_10g?.[statKey] || {};
        const sAvg = p.season_avg || {};
        const ctx = p.context_stats || {};
        const sit = p.situational || {};

        const gameLogsHtml = (dist.logs || []).map(val => {
            const num = parseFloat(val);
            const isAboveMedian = !isNaN(num) && num >= (dist.median || 0);
            return `<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-mono ${isAboveMedian ? 'bg-cyan-950/60 text-cyan-300 border border-cyan-800/60 font-semibold' : 'bg-[#21262d] text-slate-300 border border-[#30363d]'}">${val}</span>`;
        }).join(' ');

        return `
            <tr class="hover:bg-[#21262d]/50 transition">
                <td class="text-center font-mono text-slate-500 py-2.5 px-3">${idx + 1}</td>
                <td class="py-2.5 px-3">
                    <div class="font-semibold text-slate-100">${p.player_name}</div>
                    <div class="text-[10px] text-slate-500 font-mono">${p.team_name} • ${p.games_played} GP</div>
                </td>
                <td class="text-center font-mono font-bold text-cyan-400 py-2.5 px-3">${p.position}</td>
                <td class="text-center font-mono font-bold text-slate-300 py-2.5 px-3">
                    <div class="flex items-center justify-center space-x-1.5">
                        <img src="https://a.espncdn.com/i/teamlogos/nba/500/${(p.team || '').toLowerCase()}.png" alt="${p.team}" class="w-4 h-4 object-contain shrink-0" onerror="this.style.display='none';"/>
                        <span>${p.team}</span>
                    </div>
                </td>
                <td class="text-right font-mono text-slate-200 py-2.5 px-3">
                    <span class="font-bold">${sAvg[statKey] != null ? sAvg[statKey] : '--'}</span>
                    <span class="text-[10px] text-slate-500 block">${sAvg.min} MPG</span>
                </td>
                <td class="text-center font-mono font-bold text-slate-300 py-2.5 px-3 bg-[#161b22]/40">${dist.floor != null ? dist.floor : '--'}</td>
                <td class="text-center font-mono font-extrabold text-cyan-300 py-2.5 px-3 bg-cyan-950/20">${dist.median != null ? dist.median : '--'}</td>
                <td class="text-center font-mono font-bold text-slate-300 py-2.5 px-3 bg-[#161b22]/40">${dist.ceiling != null ? dist.ceiling : '--'}</td>
                <td class="text-center font-mono text-slate-300 py-2.5 px-3">
                    <span class="text-[10px] px-1.5 py-0.5 rounded bg-[#21262d] border border-[#30363d]">USG: ${ctx.usg_pct}%</span>
                    <span class="text-[10px] text-slate-500 ml-1">AST: ${ctx.ast_pct}%</span>
                </td>
                <td class="text-center font-mono text-slate-300 py-2.5 px-3">
                    <div class="text-[10px]">Clutch: ${sit.clutch_usg_pct}%</div>
                    <div class="text-[10px] text-slate-500">Paint: ${sit.paint_touches_pg}/G</div>
                </td>
                <td class="py-2.5 px-3 font-mono">
                    <div class="flex items-center gap-1 flex-wrap">${gameLogsHtml || '--'}</div>
                </td>
            </tr>
        `;
    }).join('');

    safeCreateIcons(tbody);
}
