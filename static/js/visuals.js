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

    let cellsHtml = '';
    const cellW = 150;
    const cellH = 84;

    zones.forEach((z, idx) => {
        const offFreq = aOff[z.id]?.freq || 20.0;
        const defPpp = hDef[z.id]?.ppp || 1.05;

        let fill = '#1e293b';
        let stroke = '#334155';
        let textPppColor = '#94a3b8';

        if (defPpp <= 0.95) {
            fill = 'rgba(16, 185, 129, 0.12)';
            stroke = 'rgba(16, 185, 129, 0.35)';
            textPppColor = '#34d399';
        } else if (defPpp >= 1.15) {
            fill = 'rgba(239, 68, 68, 0.12)';
            stroke = 'rgba(239, 68, 68, 0.35)';
            textPppColor = '#f87171';
        }

        const x = 15 + idx * (cellW + 10);
        cellsHtml += `
            <g transform="translate(${x}, 35)">
                <rect width="${cellW}" height="${cellH}" rx="8" fill="${fill}" stroke="${stroke}" stroke-width="1.2"/>
                <text x="14" y="24" fill="#e2e8f0" font-size="12" font-weight="bold" font-family="monospace">${z.label}</text>
                <text x="14" y="47" fill="#94a3b8" font-size="11" font-family="sans-serif">${awayTeam} Offense: <tspan fill="#f1f5f9" font-weight="bold">${offFreq}%</tspan></text>
                <text x="14" y="68" fill="#94a3b8" font-size="11" font-family="sans-serif">${homeTeam} Defense: <tspan fill="${textPppColor}" font-weight="bold">${defPpp} PPP</tspan></text>
            </g>
        `;
    });

    return `
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3 space-y-2">
            <div class="flex items-center justify-between text-xs font-bold text-slate-300">
                <span class="flex items-center gap-1.5">
                    <i data-lucide="crosshair" class="w-3.5 h-3.5 text-cyan-400"></i>
                    <span>Shot Zone Alignment (Offense Generation vs. Defense Allowed PPP)</span>
                </span>
                <span class="text-[10px] text-slate-500 font-mono">Teal: &le;0.95 PPP | Red: &ge;1.15 PPP</span>
            </div>
            <div class="overflow-x-auto touch-scroll">
                <svg viewBox="0 0 815 135" class="w-full min-w-[750px] h-auto">
                    <rect width="815" height="135" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
                    <text x="15" y="22" fill="#94a3b8" font-size="10.5" font-weight="semibold" font-family="monospace">${awayTeam} ATTEMPTS FREQUENCY (%) VS. ${homeTeam} DEFENSIVE ALLOWANCE (PPP)</text>
                    ${cellsHtml}
                </svg>
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

    let rowsHtml = '';
    const maxBarW = 280;

    playtypes.forEach((pt, idx) => {
        const freq = aPt[pt.id]?.freq || 20.0;
        const allowedPpp = hPt[pt.id]?.allowed_ppp || 0.95;

        const barW = Math.min(maxBarW, (freq / 50.0) * maxBarW);
        const y = 35 + idx * 30;

        let pppColor = '#94a3b8';
        if (allowedPpp <= 0.92) pppColor = '#34d399';
        else if (allowedPpp >= 1.08) pppColor = '#f87171';

        rowsHtml += `
            <g transform="translate(15, ${y})">
                <text x="0" y="14" fill="#e2e8f0" font-size="11" font-weight="bold" font-family="monospace">${pt.label}</text>
                <rect x="150" y="2" width="${maxBarW}" height="16" rx="3" fill="#21262d"/>
                <rect x="150" y="2" width="${barW}" height="16" rx="3" fill="#0284c7"/>
                <text x="${160 + barW}" y="14" fill="#cbd5e1" font-size="10" font-family="monospace">${freq}%</text>
                <text x="490" y="14" fill="#94a3b8" font-size="10" font-family="sans-serif">${homeTeam} Allowed: <tspan fill="${pppColor}" font-weight="bold">${allowedPpp} PPP</tspan></text>
            </g>
        `;
    });

    return `
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3 space-y-2">
            <div class="flex items-center justify-between text-xs font-bold text-slate-300">
                <span class="flex items-center gap-1.5">
                    <i data-lucide="bar-chart-horizontal" class="w-3.5 h-3.5 text-cyan-400"></i>
                    <span>Play Type Alignment (${awayTeam} Generation vs. ${homeTeam} Defense Allowed)</span>
                </span>
                <span class="text-[10px] text-slate-500 font-mono">Primary Schemes</span>
            </div>
            <div class="overflow-x-auto touch-scroll">
                <svg viewBox="0 0 815 130" class="w-full min-w-[750px] h-auto">
                    <rect width="815" height="130" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
                    <text x="15" y="20" fill="#94a3b8" font-size="10" font-weight="semibold" font-family="monospace">TOP 3 OFFENSIVE PLAY TYPES VS. DEFENSIVE ALLOWANCE</text>
                    ${rowsHtml}
                </svg>
            </div>
            <div class="flex flex-wrap items-center gap-2 pt-1 text-[11px] font-mono">
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${awayTeam} Primary Play: PnR Ball Handler (${(aPt.PnR_Ball_Handler?.freq || 20)}%)]
                </span>
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${homeTeam} Allowed: ${(hPt.PnR_Ball_Handler?.allowed_ppp || 0.91)} PPP on PnR]
                </span>
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [Isolation Frequency: ${(aPt.Isolation?.freq || 8.5)}%]
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
                    [${awayTeam} Pace: ${aPace}]
                </span>
                <span class="px-2 py-0.5 rounded bg-[#21262d] border border-[#30363d] text-slate-300">
                    [${homeTeam} Pace: ${hPace}]
                </span>
                <span class="px-2 py-0.5 rounded bg-cyan-950/40 border border-cyan-800/60 text-cyan-300">
                    [Expected Game Tempo: ${expPace} Poss/48m]
                </span>
            </div>
        </div>
    `;
}
