/**
 * NBA Forensic Scouting Engine - Inline Visuals & SVG Telemetry
 * =============================================================
 * Renders Shot Zone Heatmap, Play Type Alignment, and Pace Telemetry Gauge.
 * Strictly descriptive, zero predictive/betting vocabulary.
 */

// Reusable Empty SVG Card Renderer for Zero-Game Date Ranges
function renderEmptySvgCard(title, iconName, detailText) {
    return `
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-4 text-center">
            <div class="flex items-center justify-between text-xs font-bold text-slate-400 mb-2">
                <span class="flex items-center gap-1.5">
                    <i data-lucide="${iconName}" class="w-3.5 h-3.5 text-slate-500"></i>
                    <span>${title}</span>
                </span>
                <span class="text-[10px] text-slate-500 font-mono">Date Range: ${startDate} to ${endDate}</span>
            </div>
            <div class="py-4 text-slate-500 font-mono text-xs">
                ${detailText}
            </div>
        </div>
    `;
}

// Row 1: Shot Zone Alignment Heat Map
function renderShotZoneAlignmentSvg(awayForensic, homeForensic, awayTeam, homeTeam) {
    if (!awayForensic || !awayForensic.gp || !homeForensic || !homeForensic.gp) {
        return renderEmptySvgCard(
            'Shot Zone Alignment (Offense Generation vs. Defense Allowed PPP)',
            'crosshair',
            'No completed games recorded in selected date range • Shot zone alignment blank'
        );
    }

    const zones = [
        { id: 'restricted_area', label: 'Rim (RA)' },
        { id: 'paint_non_ra', label: 'Paint (Non-RA)' },
        { id: 'mid_range', label: 'Mid-Range' },
        { id: 'corner_3', label: 'Corner 3' },
        { id: 'above_break_3', label: 'Above Break 3' }
    ];

    const aOff = awayForensic.shot_profile_offense || {};
    const hDef = homeForensic.shot_profile_allowed || {};

    const formatRank = (r) => {
        if (r != null && r !== '' && r !== 'Small Sample') return `<span class="text-slate-400 font-mono text-[9.5px]">(#${r} of 30)</span>`;
        return '';
    };

    const zoneKeyMap = {
        restricted_area: { off: 'rank_ra_off_freq', def: 'rank_ra_ppp_allowed' },
        paint_non_ra: { off: 'rank_paint_off_freq', def: 'rank_paint_ppp_allowed' },
        mid_range: { off: 'rank_mid_off_freq', def: 'rank_mid_ppp_allowed' },
        corner_3: { off: 'rank_corner3_off_freq', def: 'rank_corner3_ppp_allowed' },
        above_break_3: { off: 'rank_above_break3_off_freq', def: 'rank_above_break3_ppp_allowed' }
    };

    const cardsHtml = zones.map(z => {
        const offFreq = aOff[z.id]?.freq || 20.0;
        const defPpp = hDef[z.id]?.ppp || 1.05;
        const offRank = aOff[z.id]?.rank || awayForensic[zoneKeyMap[z.id]?.off];
        const defRank = hDef[z.id]?.rank || homeForensic[zoneKeyMap[z.id]?.def];

        let borderStyle = 'border-[#334155] bg-[#1e293b]/40';
        let textPppColor = 'text-slate-300';

        if (defPpp <= 0.95) {
            borderStyle = 'border-emerald-700/60 bg-emerald-950/20';
            textPppColor = 'text-emerald-400';
        } else if (defPpp >= 1.15) {
            borderStyle = 'border-rose-700/60 bg-rose-950/20';
            textPppColor = 'text-rose-400';
        }

        const tooltipTrigger = (window.formatMetricLabelWithTooltip)
            ? window.formatMetricLabelWithTooltip(z.label, z.label, 'tooltip-down')
            : `<span class="stat-info-trigger font-mono font-bold text-slate-100"><span>${z.label}</span></span>`;

        const pppFormatted = (window.formatInlinePpp)
            ? window.formatInlinePpp(`${defPpp} PPP`, 'tooltip-down')
            : `${defPpp} PPP`;

        return `
            <div class="flex-1 min-w-[130px] p-3 rounded-xl border ${borderStyle} flex flex-col justify-between space-y-2 hover:border-cyan-400/80 transition shadow-sm">
                <div class="flex items-center justify-between text-xs font-bold font-mono">
                    ${tooltipTrigger}
                </div>
                <div class="space-y-1 text-[11px] font-sans">
                    <div class="text-slate-400 flex items-center justify-between">
                        <span>${awayTeam} Off: <b class="text-slate-100 font-mono">${offFreq}%</b></span>
                        ${formatRank(offRank, awayForensic.is_small_sample)}
                    </div>
                    <div class="text-slate-400 flex items-center justify-between">
                        <span>${homeTeam} Def: <b class="${textPppColor} font-mono">${pppFormatted}</b></span>
                        ${formatRank(defRank, homeForensic.is_small_sample)}
                    </div>
                </div>
            </div>
        `;
    }).join('');

    return `
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3 sm:p-4 space-y-2.5">
            <div class="flex items-center justify-between text-xs font-bold text-slate-300 border-b border-[#21262d] pb-1.5">
                <span class="flex items-center gap-1.5">
                    <i data-lucide="crosshair" class="w-3.5 h-3.5 text-cyan-400"></i>
                    <span>Shot Zone Alignment (Offense Generation vs. Defense Allowed PPP)</span>
                </span>
                <span class="text-[10px] text-slate-400 font-mono">Teal: &le;0.95 PPP | Red: &ge;1.15 PPP</span>
            </div>
            <div class="text-[10.5px] font-mono text-slate-400 uppercase tracking-tight">
                ${awayTeam} ATTEMPTS FREQUENCY (%) VS. ${homeTeam} DEFENSIVE ALLOWANCE (PPP)
            </div>
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2.5">
                ${cardsHtml}
            </div>
        </div>
    `;
}

// Row 2: Play Type Alignment Horizontal Bar Chart
function renderPlayTypeAlignmentSvg(awayForensic, homeForensic, awayTeam, homeTeam) {
    if (!awayForensic || !awayForensic.gp || !homeForensic || !homeForensic.gp) {
        return renderEmptySvgCard(
            `Play Type Alignment (${awayTeam} Generation vs. ${homeTeam} Defense Allowed)`,
            'bar-chart-horizontal',
            'No completed games recorded in selected date range • Play type alignment blank'
        );
    }

    const playtypes = [
        { id: 'PnR_Ball_Handler', label: 'PnR Ball Handler' },
        { id: 'Spot_Up', label: 'Spot-Up' },
        { id: 'Isolation', label: 'Isolation' }
    ];

    const aPt = awayForensic.playtypes_offense || {};
    const hPt = homeForensic.playtypes_defense || {};

    const formatRank = (r) => {
        if (r != null && r !== '' && r !== 'Small Sample') return `<span class="text-slate-400 font-mono text-[9.5px]">(#${r} of 30)</span>`;
        return '';
    };

    const ptKeyMap = {
        PnR_Ball_Handler: { off: 'rank_pnr_bh_freq', def: 'rank_pnr_bh_allowed_ppp' },
        Spot_Up: { off: 'rank_spot_freq', def: 'rank_spot_up_allowed_ppp' },
        Isolation: { off: 'rank_iso_freq', def: 'rank_iso_allowed_ppp' }
    };

    const rowsHtml = playtypes.map(pt => {
        const freq = aPt[pt.id]?.freq || 20.0;
        const allowedPpp = hPt[pt.id]?.allowed_ppp || 0.95;
        const offRank = awayForensic[ptKeyMap[pt.id]?.off];
        const defRank = homeForensic[ptKeyMap[pt.id]?.def] || hPt[pt.id]?.rank;

        let pppColor = 'text-slate-300';
        if (allowedPpp <= 0.92) pppColor = 'text-emerald-400';
        else if (allowedPpp >= 1.08) pppColor = 'text-rose-400';

        const barPct = Math.min(100, (freq / 45.0) * 100);

        const tooltipTrigger = (window.formatMetricLabelWithTooltip)
            ? window.formatMetricLabelWithTooltip(pt.label, pt.label, 'tooltip-down')
            : `<span class="stat-info-trigger font-mono font-bold text-slate-100"><span>${pt.label}</span></span>`;

        const pppFormatted = (window.formatInlinePpp)
            ? window.formatInlinePpp(`${allowedPpp} PPP`, 'tooltip-down')
            : `${allowedPpp} PPP`;

        return `
            <div class="flex items-center justify-between gap-3 p-2.5 rounded-lg bg-[#161b22] border border-[#21262d] text-xs font-mono">
                <div class="w-36 shrink-0 truncate">
                    ${tooltipTrigger}
                </div>
                <div class="flex-1 flex items-center gap-2">
                    <div class="w-full bg-[#0d1117] h-3 rounded-full overflow-hidden border border-[#21262d] flex">
                        <div class="bg-cyan-500 h-full rounded-full transition-all" style="width: ${barPct}%;"></div>
                    </div>
                    <span class="w-28 text-slate-200 font-bold text-[11px] text-right font-mono">${freq}% ${formatRank(offRank, awayForensic.is_small_sample)}</span>
                </div>
                <div class="w-56 text-right text-[11px] text-slate-400 shrink-0 font-sans">
                    ${homeTeam} Allowed: <b class="${pppColor} font-mono">${pppFormatted}</b> ${formatRank(defRank, homeForensic.is_small_sample)}
                </div>
            </div>
        `;
    }).join('');

    const pnrTooltip = (window.formatMetricLabelWithTooltip) ? window.formatMetricLabelWithTooltip('PnR Ball Handler') : 'PnR Ball Handler';
    const isoTooltip = (window.formatMetricLabelWithTooltip) ? window.formatMetricLabelWithTooltip('Isolation') : 'Isolation';

    return `
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3 sm:p-4 space-y-2.5">
            <div class="flex items-center justify-between text-xs font-bold text-slate-300 border-b border-[#21262d] pb-1.5">
                <span class="flex items-center gap-1.5">
                    <i data-lucide="bar-chart-horizontal" class="w-3.5 h-3.5 text-cyan-400"></i>
                    <span>Play Type Alignment (${awayTeam} Generation vs. ${homeTeam} Defense Allowed)</span>
                </span>
                <span class="text-[10px] text-slate-400 font-mono">Primary Schemes</span>
            </div>
            <div class="text-[10.5px] font-mono text-slate-400 uppercase tracking-tight">
                TOP 3 OFFENSIVE PLAY TYPES VS. DEFENSIVE ALLOWANCE
            </div>
            <div class="space-y-2">
                ${rowsHtml}
            </div>
            <div class="flex flex-wrap items-center gap-2 pt-1 text-[11px] font-mono">
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${awayTeam} Primary: ${pnrTooltip} (${(aPt.PnR_Ball_Handler?.freq || 20)}%)]
                </span>
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${homeTeam} Allowed: ${(hPt.PnR_Ball_Handler?.allowed_ppp || 0.91)} PPP on PnR]
                </span>
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${isoTooltip} Frequency: ${(aPt.Isolation?.freq || 8.5)}%]
                </span>
            </div>
        </div>
    `;
}

// Row 3: Pace Alignment Meter (Precision Telemetry Gauge)
function renderPaceAlignmentSvg(awayForensic, homeForensic, awayTeam, homeTeam) {
    if (!awayForensic || !awayForensic.gp || !homeForensic || !homeForensic.gp) {
        return renderEmptySvgCard(
            'Pace Alignment Gauge (Possessions / 48m Game Tempo)',
            'gauge',
            'No completed games recorded in selected date range • Pace gauge blank'
        );
    }

    const aPace = parseFloat(awayForensic.pace) || 100.0;
    const hPace = parseFloat(homeForensic.pace) || 100.0;
    const expPace = ((aPace + hPace) / 2).toFixed(1);

    const minPace = 94.0;
    const maxPace = 108.0;
    const startX = 80;
    const meterW = 650;
    const getX = (val) => startX + Math.max(0, Math.min(meterW, ((val - minPace) / (maxPace - minPace)) * meterW));

    const aX = getX(aPace);
    const hX = getX(hPace);
    const midX = getX(100.0);
    const expX = getX(parseFloat(expPace));

    let ticksHtml = '';
    for (let p = 94; p <= 108; p += 2) {
        const tx = getX(p);
        ticksHtml += `
            <line x1="${tx}" y1="70" x2="${tx}" y2="78" stroke="#475569" stroke-width="1.5"/>
            <text x="${tx}" y="89" fill="#94a3b8" font-size="9.5" text-anchor="middle" font-family="monospace">${p}</text>
        `;
    }

    const isClose = Math.abs(aX - hX) < 55;
    const aHigher = aPace >= hPace;
    const aBadgeY = (isClose && aHigher) ? -46 : -28;
    const hBadgeY = (isClose && !aHigher) ? -46 : -28;
    const aStemH = (isClose && aHigher) ? 30 : 12;
    const hStemH = (isClose && !aHigher) ? 30 : 12;

    return `
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3 space-y-2">
            <div class="flex items-center justify-between text-xs font-bold text-slate-300">
                <span class="flex items-center gap-1.5">
                    <i data-lucide="gauge" class="w-3.5 h-3.5 text-cyan-400"></i>
                    <span>Pace Alignment Gauge (Possessions / 48m Game Tempo)</span>
                </span>
                <span class="text-[10px] text-slate-500 font-mono">League Baseline: 100.0 Poss/48m</span>
            </div>
            <div class="overflow-x-auto touch-scroll">
                <svg viewBox="0 0 815 125" class="w-full min-w-[750px] h-auto">
                    <rect width="815" height="125" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
                    
                    <rect x="${getX(94)}" y="58" width="${getX(98) - getX(94)}" height="12" rx="3" fill="#1e293b" stroke="#334155" stroke-width="0.5"/>
                    <rect x="${getX(98)}" y="58" width="${getX(102) - getX(98)}" height="12" fill="#0f172a" stroke="#0ea5e9" stroke-opacity="0.3" stroke-width="0.5"/>
                    <rect x="${getX(102)}" y="58" width="${getX(108) - getX(102)}" height="12" rx="3" fill="#1e293b" stroke="#334155" stroke-width="0.5"/>
                    
                    <line x1="${midX}" y1="54" x2="${midX}" y2="78" stroke="#38bdf8" stroke-dasharray="2 2" stroke-width="2"/>
                    ${ticksHtml}

                    <text x="${(getX(94) + getX(98)) / 2}" y="106" fill="#64748b" font-size="8.5" text-anchor="middle" font-family="monospace">HALF-COURT</text>
                    <text x="${midX}" y="106" fill="#38bdf8" font-size="8.5" text-anchor="middle" font-family="monospace">LEAGUE BASELINE</text>
                    <text x="${(getX(102) + getX(108)) / 2}" y="106" fill="#64748b" font-size="8.5" text-anchor="middle" font-family="monospace">TRANSITION</text>

                    <!-- Away Team Pace Indicator (Aligned directly above needle) -->
                    <g transform="translate(${aX}, 58)">
                        <polygon points="0,0 -4,-${aStemH} 4,-${aStemH}" fill="#38bdf8"/>
                        <rect x="-27" y="${aBadgeY}" width="54" height="17" rx="3" fill="#0369a1" stroke="#38bdf8" stroke-width="1.2"/>
                        <text x="0" y="${aBadgeY + 12}" fill="#f8fafc" font-size="9" font-weight="bold" text-anchor="middle" font-family="monospace">${awayTeam} ${aPace}</text>
                    </g>

                    <!-- Home Team Pace Indicator (Aligned directly above needle) -->
                    <g transform="translate(${hX}, 58)">
                        <polygon points="0,0 -4,-${hStemH} 4,-${hStemH}" fill="#f43f5e"/>
                        <rect x="-27" y="${hBadgeY}" width="54" height="17" rx="3" fill="#be123c" stroke="#f43f5e" stroke-width="1.2"/>
                        <text x="0" y="${hBadgeY + 12}" fill="#f8fafc" font-size="9" font-weight="bold" text-anchor="middle" font-family="monospace">${homeTeam} ${hPace}</text>
                    </g>

                    <!-- Expected Tempo Marker -->
                    <g transform="translate(${expX}, 64)">
                        <circle cx="0" cy="0" r="4.5" fill="#e2e8f0" stroke="#0f172a" stroke-width="1.5"/>
                    </g>
                </svg>
            </div>
            <div class="flex flex-wrap items-center gap-2 pt-1 text-[11px] font-mono">
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${window.formatMetricLabelWithTooltip ? window.formatMetricLabelWithTooltip('Pace', `${awayTeam} Pace: ${aPace}`) : `${awayTeam} Pace: ${aPace}`} ${awayForensic.rank_pace ? `(#${awayForensic.rank_pace} of 30)` : ''}]
                </span>
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${window.formatMetricLabelWithTooltip ? window.formatMetricLabelWithTooltip('Pace', `${homeTeam} Pace: ${hPace}`) : `${homeTeam} Pace: ${hPace}`} ${homeForensic.rank_pace ? `(#${homeForensic.rank_pace} of 30)` : ''}]
                </span>
                <span class="px-2 py-0.5 rounded bg-cyan-950/40 border border-cyan-800/60 text-cyan-300">
                    [${window.formatMetricLabelWithTooltip ? window.formatMetricLabelWithTooltip('Expected Game Tempo', `Expected Game Tempo: ${expPace} Poss/48m`) : `Expected Game Tempo: ${expPace} Poss/48m`}]
                </span>
            </div>
        </div>
    `;
}
