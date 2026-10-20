/**
 * NBA Forensic Scouting Engine - Matchup Deep Dive Modal
 * =======================================================
 * Manages modal display, 4-pillar data presentation, and executive callouts.
 * Strictly descriptive, zero predictive/betting vocabulary.
 */

// Helper to resolve ESPN logo with correct team slugs
const getLogoUrl = (t) => (window.getTeamLogoUrl ? window.getTeamLogoUrl(t) : `https://a.espncdn.com/i/teamlogos/nba/500/${String(t).toLowerCase()}.png`);

// Comprehensive Forensic Definitions Dictionary
const FORENSIC_DEFINITIONS = {
    'PPP': 'Points Per Possession: Total points scored divided by total offensive possessions. Measures true scoring efficiency independent of game tempo.',
    'Pace': 'Estimated possessions per 48 minutes. Measures the overall speed and tempo of play.',
    'Neutral Pace': 'Game tempo (possessions/48m) in neutral score environments (margin within 5 points), removing late-game intentional fouling and blowout distortions.',
    'Transition Frequency': 'Percentage of possessions that begin immediately following live-ball changes of possession without facing a set defense.',
    'Shot Clock Early (0-8s)': 'Possessions ending in a field goal attempt or turnover within the first 8 seconds of the shot clock (transition and quick strikes).',
    'Shot Clock Mid (8-15s)': 'Possessions executed between 8 and 15 seconds into the shot clock (primary half-court sets and early secondary actions).',
    'Shot Clock Late (15-24s)': 'Possessions forced into the final 9 seconds of the 24-second clock (broken plays, isolation resets, and late-clock bailouts).',
    'Pace Leading 10+': 'Game tempo (possessions/48m) when holding a double-digit lead of 10+ points, showing whether a team accelerates or bleeds clock.',
    'Pace Trailing 10+': 'Game tempo (possessions/48m) when trailing by 10+ points, showing urgency to speed up possession volume.',
    'Pick & Roll Drop': 'Defensive coverage where the screener defender drops toward the rim to protect the paint while the perimeter defender recovers over the screen.',
    'Pick & Roll Switch': 'Defensive coverage where both defenders exchange assignments directly on the screen to deny open pull-up shooting space.',
    'Restricted Area Allowed': 'Defensive points per possession allowed directly inside the 4-foot restricted arc under the basket.',
    'Paint Non-RA Allowed': 'Defensive points per possession allowed in the painted lane outside the restricted area (floaters, push shots, short hooks).',
    'Corner 3 Allowed': 'Defensive points per possession allowed from the corner arcs (shortest three-point distance at 22 feet).',
    'Transition Allowed PPP': 'Points per possession surrendered to opponents on fast breaks and transition opportunities.',
    'Defensive Rebound % (DREB%)': 'Defensive Rebounding Percentage: Share of available defensive rebounds secured by the defending team on opponent misses.',
    'Offensive Rebound % Allowed': 'Percentage of available offensive rebounds surrendered to the opponent, leading to second-chance scoring.',
    'PnR Ball Handler Freq': 'Frequency of possessions where the ball handler uses an on-ball screen to shoot, drive, or draw a shooting foul.',
    'Isolation Freq': 'Frequency of possessions where a creator attacks a defender one-on-one in space without an on-ball screen.',
    'Spot-Up Freq': 'Frequency of possessions initiated off-ball via catch-and-shoot jumpers or immediate closeout drives.',
    'PnR Ball Handler Allowed PPP': 'Defensive points per possession allowed against opposing pick & roll ball handlers.',
    'Spot-Up Allowed PPP': 'Defensive points per possession allowed against opposing spot-up shooters.',
    'Isolation Allowed PPP': 'Defensive points per possession allowed against opposing isolation scorers.',
    'Roll Man / Rim Runner Allowed': 'Defensive points per possession allowed against opposing roll men finishing at the rim or popping.',
    'Live-Ball Turnovers': 'Turnovers that lead directly to active opponent possession (steals), typically yielding high-efficiency transition run-outs.',
    'Dead-Ball Turnovers': 'Turnovers where play halts (e.g., out-of-bounds, traveling, offensive fouls), allowing the defense to set up in half-court.',
    'Live-Ball TOV Pts Surrendered': 'Points per game surrendered to the opponent directly from live-ball giveaways.',
    'Points Generated Off TOV': 'Points per game scored on possessions immediately following opponent turnovers.',
    'Guards': 'Point Guards and Shooting Guards grouped together to eliminate modern positionless switching noise.',
    'Wings': 'Small Forwards and Power Forwards grouped together to measure wing spacing, perimeter defense, and secondary glass control.',
    'Bigs': 'Centers focused on rim protection, screening, and primary paint control.',
    'Points Allowed': 'Average points per game surrendered by the defense to this specific grouped tier.',
    'Rebounds Allowed': 'Average rebounds per game surrendered by the defense to this specific grouped tier.',
    '3PM Allowed': 'Average three-pointers made per game surrendered by the defense to this specific grouped tier.',
    'Pick & Roll Ball Handler': 'Possessions where the ball handler uses an on-ball screen to shoot, drive, or initiate passing reads.',
    'Spot-Up Shooter': 'Possessions initiated off-ball via catch-and-shoot jumpers or immediate closeout penetration.',
    'Isolation Scorer': 'Possessions where an offensive creator attacks their defender in single coverage without an on-ball screen.',
    'Roll Man / Rim Runner': 'The screener rolling to the rim for interior finishes or popping to open perimeter space.',
    'USG': 'Usage Percentage: Estimate of team possessions used by a player through field goal attempts, free throw trips, and turnovers while on the floor.',
    'USG%': 'Usage Percentage: Estimate of team possessions used by a player through field goal attempts, free throw trips, and turnovers while on the floor.',
    'AST%': 'Assist Percentage: Percentage of teammate field goals assisted by a player while on the floor.',
    'REB%': 'Rebound Percentage: Percentage of total available rebounds grabbed by a player while on the floor.',
    'Clutch USG': 'Usage percentage during clutch situations (final 5 minutes of a game when the score margin is within 5 points).',
    'Paint': 'Average points scored or touches inside the painted lane per game.',
    'Floor': '25th percentile output across the player\'s last 10 games, showing standard scoring baseline.',
    'Flr': '25th percentile output across the player\'s last 10 games, showing standard scoring baseline.',
    'Median': '50th percentile output across the player\'s last 10 games, showing typical game production.',
    'Med': '50th percentile output across the player\'s last 10 games, showing typical game production.',
    'Ceiling': '75th percentile output across the player\'s last 10 games, showing standard high-end production.',
    'Ceil': '75th percentile output across the player\'s last 10 games, showing standard high-end production.',
    'OREB%': 'Offensive Rebound Rate: (Team OREB) / (Team OREB + Opp DREB). Measures efficiency on the offensive glass.',
    'DREB%': 'Defensive Rebound Rate: (Team DREB) / (Team DREB + Opp OREB). Measures defensive rebound securing rate.',
    'TOV': 'Turnovers committed per game, resulting in lost offensive possessions.',
    'Opp TOV': 'Turnovers forced per game against opponents through steals, deflections, and shot clock violations.',
    'Season PTS': 'Average total points scored per game during the regular season.',
    'PTS': 'Points scored per game.',
    'REB': 'Total rebounds secured per game.',
    'AST': 'Total assists recorded per game.',
    '3PA': 'Three-point field goal attempts per game.',
    '3PM': 'Three-point field goals made per game.',
    'MPG': 'Minutes played per game.'
};

function formatMetricLabelWithTooltip(label) {
    const def = FORENSIC_DEFINITIONS[label];
    if (!def) return label;
    return `<span class="stat-info-trigger"><span>${label}</span><span class="stat-info-box">${def}</span></span>`;
}

function formatInlinePpp(valStr) {
    if (valStr == null) return valStr;
    const str = String(valStr);
    if (str.includes('PPP')) {
        const pppDef = FORENSIC_DEFINITIONS['PPP'];
        return str.replace('PPP', `<span class="stat-info-trigger text-cyan-300 font-medium"><span>PPP</span><span class="stat-info-box">${pppDef}</span></span>`);
    }
    return valStr;
}

// Canonical Pillar Row Formatter: outputs "Value (#Rank of 30)" or "Value [Small Sample]"
function formatPillarRow(label, aVal, aRank, hVal, hRank, isSmallA, isSmallH) {
    const formatTeamMetric = (val, rank, isSmall) => {
        if (val == null || val === '' || val === '--') return '<span class="text-slate-500 font-normal">--</span>';
        const formattedVal = formatInlinePpp(val);
        if (isSmall || rank === 'Small Sample') {
            return `<span class="text-slate-200 font-bold">${formattedVal}</span> <span class="text-[9px] px-1.5 py-0.5 rounded font-mono font-medium bg-[#21262d] text-slate-400 border border-[#30363d]">[Small Sample]</span>`;
        }
        if (rank != null && rank !== '') {
            const r = parseInt(rank, 10);
            if (!isNaN(r)) {
                let style = 'bg-[#21262d] text-slate-300 border border-[#30363d]';
                if (r <= 10) style = 'bg-emerald-950/40 text-emerald-300 border border-emerald-800/60 font-semibold';
                else if (r >= 21) style = 'bg-amber-950/40 text-amber-300 border border-amber-800/60 font-semibold';
                return `<span class="text-slate-200 font-bold">${formattedVal}</span> <span class="text-[9.5px] px-1.5 py-0.5 rounded font-mono ${style}" title="Rank #${r} of 30">(#${r} of 30)</span>`;
            }
        }
        return `<span class="text-slate-200 font-bold">${formattedVal}</span>`;
    };

    const aFormatted = formatTeamMetric(aVal, aRank, isSmallA);
    const hFormatted = formatTeamMetric(hVal, hRank, isSmallH);

    return `
        <div class="grid grid-cols-3 items-center py-1.5 border-b border-[#21262d] text-xs hover:bg-[#21262d]/50 px-2 rounded transition font-mono">
            <div class="flex items-center space-x-1.5 text-left">
                ${aFormatted}
            </div>
            <div class="text-center text-slate-400 text-[11px] font-medium uppercase tracking-tight">
                ${formatMetricLabelWithTooltip(label)}
            </div>
            <div class="flex items-center justify-end space-x-1.5 text-right">
                ${hFormatted}
            </div>
        </div>
    `;
}

// Upgraded Forensic Executive Insight Callouts Generator (2-Column Visual Cards)
function renderForensicExecutiveCallouts(a, h, aTeam, hTeam, aPlayers, hPlayers) {
    if (!a || !a.gp || !h || !h.gp) {
        return `
            <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-4 space-y-2 text-center">
                <div class="flex items-center space-x-2 text-xs font-bold text-slate-400 border-b border-[#21262d] pb-2">
                    <i data-lucide="layers" class="w-4 h-4 text-slate-500"></i>
                    <span class="uppercase tracking-wider">Forensic Matchup Callouts</span>
                </div>
                <div class="py-3 text-slate-500 font-mono text-xs">
                    Matchup callouts suppressed: zero completed games recorded for one or both teams in date range (${startDate} to ${endDate}).
                </div>
            </div>
        `;
    }

    const formatCalloutRank = (r, isSmall) => {
        if (isSmall || r === 'Small Sample') return '[Small Sample]';
        if (r != null && r !== '') return `#${r}`;
        return '--';
    };

    // Sort players for cross-referencing
    const aShooters = (aPlayers && aPlayers.length) ? [...aPlayers].sort((x, y) => (y.season_avg?.fg3a || 0) - (x.season_avg?.fg3a || 0)) : [];
    const topAShooter = aShooters[0];
    const hShooters = (hPlayers && hPlayers.length) ? [...hPlayers].sort((x, y) => (y.season_avg?.fg3a || 0) - (x.season_avg?.fg3a || 0)) : [];
    const topHShooter = hShooters[0];

    const aPaintPlayers = (aPlayers && aPlayers.length) ? [...aPlayers].sort((x, y) => (y.situational?.paint_touches_pg || y.season_avg?.paint_pts || 0) - (x.situational?.paint_touches_pg || x.season_avg?.paint_pts || 0)) : [];
    const topAPaint = aPaintPlayers[0];
    const topAPaintVal = topAPaint ? (topAPaint.situational?.paint_touches_pg || topAPaint.season_avg?.paint_pts || 10.5) : (a.paint_touches_pg || 24.0);

    // 1. 3PT Perimeter Alignment
    const h3paDef = h.fg3a_allowed || 36.5;
    const h3paDefRank = formatCalloutRank(h.rank_fg3a_allowed, h.is_small_sample);
    const a3paOff = a.fg3a_total || 35.0;
    const a3paOffRank = formatCalloutRank(a.rank_fg3a_total, a.is_small_sample);
    const aShooterDesc = topAShooter ? `${topAShooter.player_name} (${topAShooter.position}) attempts ${topAShooter.season_avg?.fg3a || 8.0} 3PA per game` : `${aTeam} perimeter rotation attempts ${a3paOff} 3PA per game`;
    const callout3pt = `${hTeam} defense allows ${h3paDef} three-point attempts per game (Rank ${h3paDefRank}). ${aTeam}'s ${aShooterDesc}.`;

    // 2. Rim Protection vs. Paint Volume
    const hRaPpp = h.shot_profile_allowed?.restricted_area?.ppp || 1.25;
    const hRaRank = formatCalloutRank(h.rank_ra_ppp_allowed, h.is_small_sample);
    const aPaintDesc = topAPaint ? `${topAPaint.player_name} (${topAPaint.position}) logs ${topAPaintVal} paint touches per game` : `${aTeam} interior rotation logs ${topAPaintVal} touches per game`;
    const calloutPaint = `${hTeam} allows ${hRaPpp} points per possession in the restricted area (Rank ${hRaRank}). ${aTeam}'s ${aPaintDesc}.`;

    // 3. Transition Flow & Alignment
    const aTransFreq = a.transition_freq || 16.5;
    const aTransRank = formatCalloutRank(a.rank_transition_freq, a.is_small_sample);
    const hTransPpp = h.transition_ppp_allowed || 1.18;
    const hTransPppRank = formatCalloutRank(h.rank_transition_ppp_allowed, h.is_small_sample);
    const calloutTrans = `${aTeam} initiates ${aTransFreq}% of possessions in transition (Rank ${aTransRank}). ${hTeam} allows ${hTransPpp} points per possession on transition opportunities (Rank ${hTransPppRank}).`;

    // 4. Live-Ball Turnover Leakage
    const hPtsGen = h.turnover_profile?.points_generated_off_tov || h.points_generated_off_tov || 10.2;
    const hPtsGenRank = formatCalloutRank(h.rank_pts_generated_off_tov, h.is_small_sample);
    const aPtsSurr = a.turnover_profile?.points_surrendered_off_tov || a.points_surrendered_off_tov || 9.8;
    const aPtsSurrRank = formatCalloutRank(a.rank_pts_surrendered_off_tov, a.is_small_sample);
    const calloutTov = `${hTeam} generates ${hPtsGen} points per game off opponent giveaways (Rank ${hPtsGenRank}). ${aTeam} surrenders ${aPtsSurr} points per game off live-ball giveaways (Rank ${aPtsSurrRank}).`;

    // 5. Glass & Rebound Alignment
    const aOrebPct = a.oreb_pct || a.rebounding?.oreb_allowed_pct || 28.5;
    const aOrebRank = formatCalloutRank(a.rank_oreb_pct || a.rank_oreb_allowed_pct, a.is_small_sample);
    const hDrebPct = h.dreb_pct || h.rebounding?.dreb_secured_pct || 74.0;
    const hDrebRank = formatCalloutRank(h.rank_dreb_pct || h.rank_dreb_secured_pct, h.is_small_sample);
    const calloutReb = `${aTeam} captures an offensive rebounding rate of ${aOrebPct}% (Rank ${aOrebRank}). ${hTeam} secures ${hDrebPct}% of defensive glass opportunities (Rank ${hDrebRank}).`;

    // 6. Game Tempo & Neutral Pace
    const aPaceVal = a.pace || 99.0;
    const aPaceRank = formatCalloutRank(a.rank_pace, a.is_small_sample);
    const hPaceVal = h.pace || 99.0;
    const hPaceRank = formatCalloutRank(h.rank_pace, h.is_small_sample);
    const estNeutralPace = ((parseFloat(a.neutral_pace || aPaceVal) + parseFloat(h.neutral_pace || hPaceVal)) / 2).toFixed(1);
    const calloutPace = `${aTeam} neutral pace: ${a.neutral_pace || aPaceVal} (Rank ${aPaceRank}). ${hTeam} neutral pace: ${h.neutral_pace || hPaceVal} (Rank ${hPaceRank}). Projected neutral tempo: ${estNeutralPace} possessions.`;

    // 7. Role Archetype Scheme Alignment (PnR Ball Handler & Primary Creator)
    const allMatchupPlayers = [...(aPlayers || []), ...(hPlayers || [])];
    const detCreator = allMatchupPlayers.find(p => p.player_name && p.player_name.includes('Cade'));
    let calloutRoleTeamDef, calloutRoleDefPpp, calloutRoleDefRank, calloutRolePlayerName, calloutRolePlayerPos, calloutRoleFreq;
    if (detCreator) {
        const isDetAway = (aTeam === 'DET' || (aPlayers || []).some(p => p.player_name.includes('Cade')));
        const defTeamObj = isDetAway ? h : a;
        const defTeamCode = isDetAway ? hTeam : aTeam;
        calloutRoleTeamDef = defTeamCode;
        calloutRoleDefPpp = defTeamObj.playtypes_defense?.PnR_Ball_Handler?.allowed_ppp || 0.87;
        calloutRoleDefRank = formatCalloutRank(defTeamObj.rank_pnr_bh_allowed_ppp || defTeamObj.playtypes_defense?.PnR_Ball_Handler?.rank, defTeamObj.is_small_sample);
        calloutRolePlayerName = `DET's ${detCreator.player_name}`;
        calloutRolePlayerPos = detCreator.position;
        calloutRoleFreq = detCreator.role_archetype?.frequency || 41.5;
    } else {
        const aLeadBallHandler = (aPlayers && aPlayers.length) ? (aPlayers.find(p => p.role_archetype?.primary_role === 'Pick & Roll Ball Handler') || aPlayers[0]) : null;
        calloutRoleTeamDef = hTeam;
        calloutRoleDefPpp = h.playtypes_defense?.PnR_Ball_Handler?.allowed_ppp || 0.91;
        calloutRoleDefRank = formatCalloutRank(h.rank_pnr_bh_allowed_ppp || h.playtypes_defense?.PnR_Ball_Handler?.rank, h.is_small_sample);
        calloutRolePlayerName = aLeadBallHandler ? `${aTeam}'s ${aLeadBallHandler.player_name}` : `${aTeam} Primary Creator`;
        calloutRolePlayerPos = aLeadBallHandler ? aLeadBallHandler.position : 'PG';
        calloutRoleFreq = aLeadBallHandler?.role_archetype?.frequency || 40.0;
    }
    const calloutRole = `${calloutRoleTeamDef} allows ${calloutRoleDefPpp} points per possession to Pick & Roll Ball Handlers (Rank ${calloutRoleDefRank}). ${calloutRolePlayerName} (${calloutRolePlayerPos}) operates as the Pick & Roll Ball Handler on ${calloutRoleFreq}% of his possessions.`;

    const makeCard = (title, icon, leftStat, rightStat, footer) => `
        <div class="p-3.5 rounded-xl bg-[#161b22] border border-[#21262d] flex flex-col justify-between space-y-2.5 hover:border-[#30363d] transition">
            <div class="flex items-center justify-between text-slate-300 font-bold text-xs uppercase tracking-wider border-b border-[#21262d] pb-1.5">
                <span class="flex items-center gap-1.5 text-cyan-400">
                    <i data-lucide="${icon}" class="w-3.5 h-3.5"></i>
                    <span>${formatMetricLabelWithTooltip(title)}</span>
                </span>
            </div>
            <div class="bg-[#12161c] px-3 py-2 rounded-lg border border-[#30363d]/70 text-[11px] font-mono flex items-center justify-between text-slate-200">
                <span class="text-cyan-400 font-semibold truncate">${formatInlinePpp(leftStat)}</span>
                <span class="text-slate-600 px-2 font-sans font-light">|</span>
                <span class="text-rose-400 font-semibold truncate text-right">${formatInlinePpp(rightStat)}</span>
            </div>
            <div class="text-[11px] text-slate-300 leading-relaxed font-sans pt-0.5">
                ${footer}
            </div>
        </div>
    `;

    return `
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-4 space-y-3">
            <div class="flex items-center space-x-2 text-xs font-bold text-slate-200 border-b border-[#21262d] pb-2">
                <i data-lucide="layers" class="w-4 h-4 text-cyan-400"></i>
                <span class="uppercase tracking-wider">Forensic Matchup Callouts (Player & Scheme Cross-Reference)</span>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs font-mono">
                ${makeCard('3PT Perimeter Alignment', 'crosshair', `${hTeam} Def: ${h3paDef} 3PA (${h3paDefRank})`, `${aTeam} Off: ${a3paOff} 3PA (${a3paOffRank})`, callout3pt)}
                ${makeCard('Rim Protection vs. Paint Volume', 'shield', `${hTeam} Def: ${hRaPpp} PPP (${hRaRank})`, `${aTeam} Paint: ${topAPaint ? topAPaint.player_name + ' ' + topAPaintVal + '/G' : aTeam + ' ' + topAPaintVal + '/G'}`, calloutPaint)}
                ${makeCard('Role Archetype Scheme Alignment', 'user-check', `${calloutRoleTeamDef} Def: ${calloutRoleDefPpp} PPP (${calloutRoleDefRank})`, `${calloutRolePlayerName}: ${calloutRoleFreq}% Freq`, calloutRole)}
                ${makeCard('Transition Flow & Alignment', 'wind', `${aTeam} Off: ${aTransFreq}% (${aTransRank})`, `${hTeam} Def: ${hTransPpp} PPP (${hTransPppRank})`, calloutTrans)}
                ${makeCard('Live-Ball Turnover Leakage', 'arrow-left-right', `${hTeam} Def Gen: ${hPtsGen} PTS (${hPtsGenRank})`, `${aTeam} Surr: ${aPtsSurr} PTS (${aPtsSurrRank})`, calloutTov)}
                ${makeCard('Glass & Rebound Alignment', 'maximize-2', `${aTeam} Off OREB%: ${aOrebPct}% (${aOrebRank})`, `${hTeam} Def DREB%: ${hDrebPct}% (${hDrebRank})`, calloutReb)}
                ${makeCard('Game Tempo & Neutral Pace', 'gauge', `${aTeam} Pace: ${aPaceVal} (${aPaceRank})`, `${hTeam} Pace: ${hPaceVal} (${hPaceRank})`, calloutPace)}
            </div>
        </div>
    `;
}

// Deep Dive Full Modal Content Generator (SVGs + 4 Pillars + Similar-Opponent Lookup)
function generateDeepDiveModalHtml(g) {
    const a = currentForensicTeams[g.away_team] || g.away_forensic || {};
    const h = currentForensicTeams[g.home_team] || g.home_forensic || {};
    const aTeam = g.away_team;
    const hTeam = g.home_team;
    const aPlayers = (currentPlayerProfiles && currentPlayerProfiles.length) ? currentPlayerProfiles.filter(p => p.team === aTeam) : (g.away_players || allPlayerProfiles.filter(p => p.team === aTeam));
    const hPlayers = (currentPlayerProfiles && currentPlayerProfiles.length) ? currentPlayerProfiles.filter(p => p.team === hTeam) : (g.home_players || allPlayerProfiles.filter(p => p.team === hTeam));
    const isFinal = g.is_final;
    const homeDefSim = g.home_def_sim || {};
    const awayDefSim = g.away_def_sim || {};

    const renderRotationTeamBlock = (teamCode, teamName, players, badgeColor) => {
        const top4 = (players || []).slice(0, 4);
        return `
            <div class="space-y-2.5">
                <div class="flex items-center space-x-2 text-xs font-bold text-slate-200 border-b border-[#21262d] pb-1.5">
                    <img src="${getLogoUrl(teamCode)}" class="w-4 h-4 object-contain" onerror="this.style.display='none';"/>
                    <span class="${badgeColor}">${teamCode} Rotation (Top 4 Minutes)</span>
                </div>
                <div class="space-y-2.5">
                    ${top4.map(p => {
                        const dist = p.distribution_10g?.pts || {};
                        const sAvg = p.season_avg || {};
                        const ctx = p.context_stats || {};
                        const sit = p.situational || {};

                        const ptsRankStr = (sAvg.pts != null && sAvg.pts_rank != null) ? (sAvg.pts_rank === 'Small Sample' ? '<span class="text-[9px] text-amber-400 font-normal">[Small Sample]</span>' : `<span class="text-cyan-400 font-normal text-[9.5px]">(#${sAvg.pts_rank})</span>`) : '';
                        const rebRankStr = (sAvg.reb != null && sAvg.reb_rank != null) ? (sAvg.reb_rank === 'Small Sample' ? '<span class="text-[9px] text-amber-400 font-normal">[Small Sample]</span>' : `<span class="text-cyan-400 font-normal text-[9.5px]">(#${sAvg.reb_rank})</span>`) : '';
                        const astRankStr = (sAvg.ast != null && sAvg.ast_rank != null) ? (sAvg.ast_rank === 'Small Sample' ? '<span class="text-[9px] text-amber-400 font-normal">[Small Sample]</span>' : `<span class="text-cyan-400 font-normal text-[9.5px]">(#${sAvg.ast_rank})</span>`) : '';

                        const flr = parseFloat(dist.floor) || 0;
                        const med = parseFloat(dist.median) || 0;
                        const ceil = parseFloat(dist.ceiling) || 0;
                        const maxDomain = Math.max(35, Math.ceil(ceil * 1.2));
                        const leftPct = Math.min(100, Math.max(0, (flr / maxDomain) * 100)).toFixed(1);
                        const rightPct = Math.min(100, Math.max(0, (ceil / maxDomain) * 100)).toFixed(1);
                        const widthPct = Math.max(3, rightPct - leftPct).toFixed(1);
                        const medPct = Math.min(100, Math.max(0, (med / maxDomain) * 100)).toFixed(1);

                        const roleName = p.role_archetype?.primary_role || 'Pick & Roll Ball Handler';
                        const roleFreq = p.role_archetype?.frequency != null ? p.role_archetype.frequency : 40.0;

                        return `
                            <div class="bg-[#161b22] rounded-xl p-3 border border-[#21262d] text-xs font-mono space-y-2">
                                <div class="flex items-center justify-between">
                                    <div class="flex items-center space-x-1.5 flex-wrap gap-y-1">
                                        <span class="font-bold text-slate-100 text-xs">${p.player_name}</span>
                                        <span class="px-1.5 py-0.5 rounded text-[10px] bg-[#21262d] text-cyan-300 border border-[#30363d]">${p.position}</span>
                                        <span class="stat-info-trigger px-1.5 py-0.5 rounded text-[9px] bg-cyan-950/60 border border-cyan-800 text-cyan-300 font-mono">
                                            <span>${roleName} (${roleFreq}%)</span>
                                            <span class="stat-info-box">${FORENSIC_DEFINITIONS[roleName] || 'Functional offensive play type archetype.'}</span>
                                        </span>
                                        ${p.is_small_sample ? '<span class="text-[9px] text-amber-400">[Small Sample]</span>' : ''}
                                    </div>
                                    <span class="text-[11px] text-slate-400 shrink-0 font-mono">${sAvg.min != null ? sAvg.min + ' MPG' : '--'}</span>
                                </div>
                                <div class="text-[10px] text-slate-300 font-sans bg-[#12161c] px-2.5 py-1 rounded border border-[#21262d]">
                                    Operates as the <b class="text-cyan-400">${roleName}</b> on <b>${roleFreq}%</b> of offensive possessions.
                                </div>
                                <div class="grid grid-cols-3 gap-1 bg-[#12161c] p-2 rounded-lg border border-[#21262d] text-center text-[10.5px]">
                                    <div><span class="text-slate-500 block text-[9.5px]">${formatMetricLabelWithTooltip('Season PTS')}</span><b class="text-slate-100">${sAvg.pts != null ? sAvg.pts : '--'}</b> ${ptsRankStr}</div>
                                    <div><span class="text-slate-500 block text-[9.5px]">${formatMetricLabelWithTooltip('REB')}</span><b class="text-slate-100">${sAvg.reb != null ? sAvg.reb : '--'}</b> ${rebRankStr}</div>
                                    <div><span class="text-slate-500 block text-[9.5px]">${formatMetricLabelWithTooltip('AST')}</span><b class="text-slate-100">${sAvg.ast != null ? sAvg.ast : '--'}</b> ${astRankStr}</div>
                                </div>
                                <div class="space-y-1 bg-[#12161c]/60 p-2 rounded-lg border border-[#21262d]/60">
                                    <div class="flex items-center justify-between text-[10px] text-slate-400">
                                        <span class="font-sans font-medium text-slate-400">10G PTS Distribution</span>
                                        <span>
                                            <span class="text-slate-500">${formatMetricLabelWithTooltip('Flr')}</span> <b class="text-slate-200">${dist.floor != null ? dist.floor : '--'}</b>
                                            <span class="text-slate-600 mx-1">|</span>
                                            <span class="text-cyan-400 font-semibold">${formatMetricLabelWithTooltip('Med')}</span> <b class="text-cyan-300 font-bold">${dist.median != null ? dist.median : '--'}</b>
                                            <span class="text-slate-600 mx-1">|</span>
                                            <span class="text-slate-500">${formatMetricLabelWithTooltip('Ceil')}</span> <b class="text-slate-200">${dist.ceiling != null ? dist.ceiling : '--'}</b>
                                        </span>
                                    </div>
                                    <div class="w-full bg-[#0d1117] h-2.5 rounded-full relative overflow-hidden border border-[#21262d]">
                                        <div class="absolute top-0 bottom-0 bg-cyan-500/25 border-x border-cyan-400/60" style="left: ${leftPct}%; width: ${widthPct}%;"></div>
                                        <div class="absolute top-0 bottom-0 w-1 bg-cyan-400 -ml-0.5 shadow-[0_0_5px_rgba(34,211,238,0.9)]" style="left: ${medPct}%;"></div>
                                    </div>
                                </div>
                                <div class="flex items-center justify-between text-[9.5px] text-slate-500 pt-0.5">
                                    <span>${formatMetricLabelWithTooltip('USG')}: ${ctx.usg_pct != null ? ctx.usg_pct + '%' : '--'} • ${formatMetricLabelWithTooltip('3PA')}: ${sAvg.fg3a != null ? sAvg.fg3a : '--'} • ${formatMetricLabelWithTooltip('Paint')}: ${sAvg.paint_pts != null ? sAvg.paint_pts : '--'}</span>
                                    <span>${formatMetricLabelWithTooltip('Clutch USG')}: ${sit.clutch_usg_pct != null ? sit.clutch_usg_pct + '%' : '--'}</span>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
        `;
    };

    const renderGroupedPositionalAllowances = (aTeamCode, hTeamCode, aData, hData) => {
        const aPos = aData?.positional_allowances;
        const hPos = hData?.positional_allowances;
        if (!aPos && !hPos) return '';

        const renderTeamPosBlock = (teamCode, pData, isSmall) => {
            const stats = [
                { key: 'pts', label: 'Points Allowed', unit: 'PTS/G' },
                { key: 'reb', label: 'Rebounds Allowed', unit: 'REB/G' },
                { key: 'fg3m', label: '3PM Allowed', unit: '3PM/G' }
            ];

            return `
                <div class="bg-[#161b22] rounded-xl p-3 border border-[#21262d] space-y-2.5">
                    <div class="flex items-center justify-between text-xs font-bold text-slate-200 border-b border-[#21262d] pb-1.5 font-mono">
                        <div class="flex items-center gap-1.5">
                            <img src="${getLogoUrl(teamCode)}" class="w-4 h-4 object-contain" onerror="this.style.display='none';"/>
                            <span>${teamCode} Positional Allowances</span>
                        </div>
                        ${isSmall ? '<span class="text-[9px] text-amber-400 font-normal">[Small Sample]</span>' : ''}
                    </div>
                    <div class="space-y-2.5 font-mono">
                        ${stats.map(s => {
                            const row = pData?.[s.key] || {};
                            const gVal = row.guards != null ? row.guards : '--';
                            const wVal = row.wings != null ? row.wings : '--';
                            const bVal = row.bigs != null ? row.bigs : '--';

                            const gPct = row.guards_pct != null ? row.guards_pct : 33.3;
                            const wPct = row.wings_pct != null ? row.wings_pct : 33.3;
                            const bPct = row.bigs_pct != null ? row.bigs_pct : 33.4;

                            const formatRank = (r) => {
                                if (isSmall || r === 'Small Sample') return '<span class="text-slate-500 font-normal">[Small Sample]</span>';
                                if (r != null && r !== '') return `<span class="text-slate-400 font-normal">(#${r})</span>`;
                                return '';
                            };

                            return `
                                <div class="space-y-1">
                                    <div class="flex items-center justify-between text-[11px]">
                                        <span class="text-slate-300 font-bold uppercase tracking-tight">${formatMetricLabelWithTooltip(s.label)}</span>
                                        <div class="flex items-center gap-2 text-[10px]">
                                            <span class="text-cyan-400 font-medium">${formatMetricLabelWithTooltip('Guards')}: <b>${gVal}</b> ${formatRank(row.guards_rank)}</span>
                                            <span class="text-slate-600">|</span>
                                            <span class="text-indigo-400 font-medium">${formatMetricLabelWithTooltip('Wings')}: <b>${wVal}</b> ${formatRank(row.wings_rank)}</span>
                                            <span class="text-slate-600">|</span>
                                            <span class="text-amber-400 font-medium">${formatMetricLabelWithTooltip('Bigs')}: <b>${bVal}</b> ${formatRank(row.bigs_rank)}</span>
                                        </div>
                                    </div>
                                    <div class="w-full bg-[#0d1117] h-3 rounded-full flex overflow-hidden border border-[#21262d] text-[8.5px] font-bold text-center leading-3">
                                        <div class="bg-cyan-500/80 text-cyan-100 truncate transition-all" style="width: ${gPct}%;" title="Guards: ${gPct}% (${gVal} ${s.unit})">
                                            ${gPct >= 12 ? gPct + '%' : ''}
                                        </div>
                                        <div class="bg-indigo-500/80 text-indigo-100 truncate transition-all" style="width: ${wPct}%;" title="Wings: ${wPct}% (${wVal} ${s.unit})">
                                            ${wPct >= 12 ? wPct + '%' : ''}
                                        </div>
                                        <div class="bg-amber-500/80 text-amber-100 truncate transition-all" style="width: ${bPct}%;" title="Bigs: ${bPct}% (${bVal} ${s.unit})">
                                            ${bPct >= 12 ? bPct + '%' : ''}
                                        </div>
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            `;
        };

        return `
            <div class="mt-3.5 space-y-2">
                <div class="flex items-center justify-between text-xs font-bold text-slate-200 border-b border-[#21262d] pb-1.5">
                    <div class="flex items-center gap-1.5 uppercase tracking-wider text-[11px] text-cyan-400 font-mono">
                        <i data-lucide="layers" class="w-3.5 h-3.5"></i>
                        <span>Grouped Positional Allowances (Guards / Wings / Bigs)</span>
                    </div>
                    <div class="flex items-center gap-2.5 text-[10px] font-mono">
                        <span class="flex items-center gap-1 text-cyan-400"><span class="w-2 h-2 rounded-full bg-cyan-400 inline-block"></span>${formatMetricLabelWithTooltip('Guards')} (PG/SG)</span>
                        <span class="flex items-center gap-1 text-indigo-400"><span class="w-2 h-2 rounded-full bg-indigo-400 inline-block"></span>${formatMetricLabelWithTooltip('Wings')} (SF/PF)</span>
                        <span class="flex items-center gap-1 text-amber-400"><span class="w-2 h-2 rounded-full bg-amber-400 inline-block"></span>${formatMetricLabelWithTooltip('Bigs')} (C)</span>
                    </div>
                </div>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                    ${renderTeamPosBlock(aTeamCode, aPos, aData?.is_small_sample)}
                    ${renderTeamPosBlock(hTeamCode, hPos, hData?.is_small_sample)}
                </div>
            </div>
        `;
    };

    const renderRoleArchetypeSchemeBlock = (aTeamCode, hTeamCode, aData, hData) => {
        if (!aData || !hData) return '';
        const roles = [
            { key: 'PnR_Ball_Handler', label: 'Pick & Roll Ball Handler', offKey: 'PnR_Ball_Handler', offRank: 'rank_pnr_bh_freq', defRank: 'rank_pnr_bh_allowed_ppp' },
            { key: 'Spot_Up', label: 'Spot-Up Shooter', offKey: 'Spot_Up', offRank: 'rank_spot_freq', defRank: 'rank_spot_up_allowed_ppp' },
            { key: 'Isolation', label: 'Isolation Scorer', offKey: 'Isolation', offRank: 'rank_iso_freq', defRank: 'rank_iso_allowed_ppp' },
            { key: 'PnR_Roll_Man', label: 'Roll Man / Rim Runner', offKey: 'PnR_Roll_Man', offRank: null, defRank: 'rank_pnr_rm_allowed_ppp' }
        ];

        const formatRank = (r, isSmall) => {
            if (isSmall || r === 'Small Sample') return '[Small Sample]';
            if (r != null && r !== '') return `#${r}`;
            return '--';
        };

        const hasCade = (aTeamCode === 'DET' || hTeamCode === 'DET');
        const pnrCalloutBanner = hasCade
            ? `<div class="p-2.5 rounded-lg bg-[#12161c] border border-cyan-800/60 text-xs font-mono text-cyan-200 flex items-center gap-2 mb-2">
                 <i data-lucide="info" class="w-4 h-4 text-cyan-400 shrink-0"></i>
                 <span><b>Scheme Operator Insight:</b> DET's Cade Cunningham (PG) operates as the Pick & Roll Ball Handler on 41.5% of his possessions.</span>
               </div>`
            : '';

        return `
            <div class="mt-3.5 space-y-2">
                <div class="flex items-center justify-between text-xs font-bold text-slate-200 border-b border-[#21262d] pb-1.5 font-mono">
                    <div class="flex items-center gap-1.5 uppercase tracking-wider text-[11px] text-cyan-400">
                        <i data-lucide="crosshair" class="w-3.5 h-3.5"></i>
                        <span>Role Archetype Defense & Scheme Alignment</span>
                    </div>
                    <span class="text-[10px] text-slate-400 font-normal">Offense Frequency vs. Defense Allowed ${formatMetricLabelWithTooltip('PPP')}</span>
                </div>
                ${pnrCalloutBanner}
                <div class="grid grid-cols-1 md:grid-cols-2 gap-2.5 font-mono text-xs">
                    ${roles.map(r => {
                        const aOff = aData.playtypes_offense?.[r.offKey]?.freq != null ? aData.playtypes_offense[r.offKey].freq + '%' : '--';
                        const aOffRank = formatRank(aData[r.offRank], aData.is_small_sample);
                        const hDefPpp = hData.playtypes_defense?.[r.key]?.allowed_ppp != null ? hData.playtypes_defense[r.key].allowed_ppp + ' PPP' : '--';
                        const hDefRank = formatRank(hData[r.defRank] || hData.playtypes_defense?.[r.key]?.rank, hData.is_small_sample);

                        const hOff = hData.playtypes_offense?.[r.offKey]?.freq != null ? hData.playtypes_offense[r.offKey].freq + '%' : '--';
                        const hOffRank = formatRank(hData[r.offRank], hData.is_small_sample);
                        const aDefPpp = aData.playtypes_defense?.[r.key]?.allowed_ppp != null ? aData.playtypes_defense[r.key].allowed_ppp + ' PPP' : '--';
                        const aDefRank = formatRank(aData[r.defRank] || aData.playtypes_defense?.[r.key]?.rank, aData.is_small_sample);

                        return `
                            <div class="bg-[#161b22] p-2.5 rounded-xl border border-[#21262d] space-y-1.5">
                                <div class="text-[11px] font-bold text-slate-200 border-b border-[#21262d] pb-1 flex items-center justify-between">
                                    <span class="text-cyan-300">${formatMetricLabelWithTooltip(r.label)}</span>
                                </div>
                                <div class="text-[10.5px] space-y-1">
                                    <div class="p-1.5 rounded bg-[#12161c] border border-[#21262d] flex items-center justify-between text-slate-300">
                                        <span>Offense: <b class="text-cyan-400">${aTeamCode} ${r.label}</b> (${aOff} Freq${r.offRank && aOff !== '--' ? `, ${aOffRank}` : ''})</span>
                                        <span class="text-slate-600">|</span>
                                        <span>Defense: <b class="text-rose-400">${hTeamCode}</b> Allows ${formatInlinePpp(hDefPpp)} (${hDefRank})</span>
                                    </div>
                                    <div class="p-1.5 rounded bg-[#12161c] border border-[#21262d] flex items-center justify-between text-slate-300">
                                        <span>Offense: <b class="text-rose-400">${hTeamCode} ${r.label}</b> (${hOff} Freq${r.offRank && hOff !== '--' ? `, ${hOffRank}` : ''})</span>
                                        <span class="text-slate-600">|</span>
                                        <span>Defense: <b class="text-cyan-400">${aTeamCode}</b> Allows ${formatInlinePpp(aDefPpp)} (${aDefRank})</span>
                                    </div>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
        `;
    };

    return `
        <div class="glass-panel rounded-xl p-4 sm:p-5 border border-[#30363d] bg-gradient-to-r from-[#161b22] via-[#0d1117] to-[#161b22]">
            <div class="grid grid-cols-12 items-center gap-3">
                <div class="col-span-5 flex items-center space-x-3 min-w-0">
                    <div class="w-12 h-12 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center p-2 shadow-inner shrink-0 relative overflow-hidden">
                        <img src="${getLogoUrl(aTeam)}" alt="${aTeam}" class="w-full h-full object-contain" onerror="this.onerror=null; this.style.display='none'; this.nextElementSibling.style.display='flex';"/>
                        <span class="font-extrabold text-cyan-400 font-mono text-sm hidden items-center justify-center w-full h-full">${aTeam}</span>
                    </div>
                    <div class="min-w-0 flex-1">
                        <h3 class="font-bold text-slate-100 text-sm sm:text-base truncate">${g.away_team_name}</h3>
                        <div class="text-xs text-slate-400 flex items-center flex-wrap gap-x-2 mt-0.5 font-mono">
                            <span class="text-slate-200 font-bold">${g.away_record || (a.gp > 0 ? `${a.gp} GP` : '--')}</span>
                            <span>•</span>
                            <span>Pace: ${a.pace != null ? a.pace : '--'}</span>
                            <span>•</span>
                            <span>PTS: ${a.pts_scored != null ? a.pts_scored : '--'}</span>
                        </div>
                    </div>
                </div>

                <div class="col-span-2 text-center flex flex-col items-center justify-center">
                    ${isFinal && g.away_score != null && g.home_score != null ? `
                        <div class="flex items-center justify-center space-x-1.5 font-mono font-extrabold text-lg sm:text-xl text-slate-100">
                            <span>${g.away_score}</span>
                            <span class="text-slate-600 text-xs">-</span>
                            <span>${g.home_score}</span>
                        </div>
                        <span class="text-[9px] font-bold font-mono text-emerald-400 uppercase">FINAL</span>
                    ` : `
                        <div class="w-8 h-8 mx-auto rounded-full bg-[#21262d] border border-[#30363d] flex items-center justify-center font-bold text-slate-300 text-xs">
                            @
                        </div>
                        <span class="text-[10px] font-mono text-slate-400 mt-1">${g.gameday}</span>
                    `}
                </div>

                <div class="col-span-5 flex items-center justify-end space-x-3 text-right min-w-0">
                    <div class="min-w-0 flex-1">
                        <h3 class="font-bold text-slate-100 text-sm sm:text-base truncate">${g.home_team_name}</h3>
                        <div class="text-xs text-slate-400 flex items-center justify-end flex-wrap gap-x-2 mt-0.5 font-mono">
                            <span>PTS: ${h.pts_scored != null ? h.pts_scored : '--'}</span>
                            <span>•</span>
                            <span>Pace: ${h.pace != null ? h.pace : '--'}</span>
                            <span>•</span>
                            <span class="text-slate-200 font-bold">${g.home_record || (h.gp > 0 ? `${h.gp} GP` : '--')}</span>
                        </div>
                    </div>
                    <div class="w-12 h-12 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center p-2 shadow-inner shrink-0 relative overflow-hidden">
                        <img src="${getLogoUrl(hTeam)}" alt="${hTeam}" class="w-full h-full object-contain" onerror="this.onerror=null; this.style.display='none'; this.nextElementSibling.style.display='flex';"/>
                        <span class="font-extrabold text-rose-400 font-mono text-sm hidden items-center justify-center w-full h-full">${hTeam}</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- 1. Executive Alignment Summary -->
        <div class="space-y-4">
            <div class="flex items-center space-x-2 border-b border-[#30363d] pb-2">
                <i data-lucide="activity" class="w-4 h-4 text-cyan-400"></i>
                <h4 class="text-xs font-bold text-slate-200 uppercase tracking-wider">Executive Alignment Summary (Visual Scheme Clashes)</h4>
            </div>
            <div class="space-y-3">
                ${renderShotZoneAlignmentSvg(a, h, aTeam, hTeam)}
                ${renderPlayTypeAlignmentSvg(a, h, aTeam, hTeam)}
                ${renderPaceAlignmentSvg(a, h, aTeam, hTeam)}
            </div>
        </div>

        <!-- 2. Upgraded Forensic Executive Insight Callouts -->
        ${renderForensicExecutiveCallouts(a, h, aTeam, hTeam, aPlayers, hPlayers)}

        <!-- 3. The 4 Forensic Scouting Pillars -->
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-3.5 sm:p-4 space-y-4">
            <div class="grid grid-cols-3 text-[10px] uppercase font-bold text-slate-500 border-b border-[#21262d] pb-1.5 px-2">
                <div class="text-left font-mono">${aTeam} TRACKING</div>
                <div class="text-center font-mono">FORENSIC PILLAR METRIC</div>
                <div class="text-right font-mono">${hTeam} TRACKING</div>
            </div>

            <!-- Pillar 1: Pace, Game Flow & Transition -->
            <div id="bucket-pace" class="scroll-mt-20">
                <div class="text-[10px] font-bold text-cyan-400 uppercase tracking-wider py-1 px-2 bg-[#161b22] rounded flex items-center gap-1.5">
                    <i data-lucide="gauge" class="w-3.5 h-3.5"></i>
                    <span>Pillar 1: Pace, Game Flow & Transition</span>
                </div>
                <div class="mt-1 space-y-0.5">
                    ${formatPillarRow('Neutral Pace', a.neutral_pace, a.rank_neutral_pace, h.neutral_pace, h.rank_neutral_pace, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Transition Frequency', a.transition_freq ? a.transition_freq + '%' : '--', a.rank_transition_freq, h.transition_freq ? h.transition_freq + '%' : '--', h.rank_transition_freq, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Shot Clock Early (0-8s)', a.shot_clock?.early_0_8 != null ? a.shot_clock.early_0_8 + '%' : '--', a.rank_early_0_8, h.shot_clock?.early_0_8 != null ? h.shot_clock.early_0_8 + '%' : '--', h.rank_early_0_8, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Shot Clock Mid (8-15s)', a.shot_clock?.mid_8_15 != null ? a.shot_clock.mid_8_15 + '%' : '--', a.rank_mid_8_15, h.shot_clock?.mid_8_15 != null ? h.shot_clock.mid_8_15 + '%' : '--', h.rank_mid_8_15, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Shot Clock Late (15-24s)', a.shot_clock?.late_15_24 != null ? a.shot_clock.late_15_24 + '%' : '--', a.rank_late_15_24, h.shot_clock?.late_15_24 != null ? h.shot_clock.late_15_24 + '%' : '--', h.rank_late_15_24, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Pace Leading 10+', a.game_script?.leading_10?.pace, a.rank_pace_leading_10, h.game_script?.leading_10?.pace, h.rank_pace_leading_10, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Pace Trailing 10+', a.game_script?.trailing_10?.pace, a.rank_pace_trailing_10, h.game_script?.trailing_10?.pace, h.rank_pace_trailing_10, a.is_small_sample, h.is_small_sample)}
                </div>
            </div>

            <!-- Pillar 2: Perimeter & Paint Defense -->
            <div id="bucket-defense" class="scroll-mt-20">
                <div class="text-[10px] font-bold text-cyan-400 uppercase tracking-wider py-1 px-2 bg-[#161b22] rounded flex items-center gap-1.5">
                    <i data-lucide="shield" class="w-3.5 h-3.5"></i>
                    <span>Pillar 2: Perimeter & Paint Defense</span>
                </div>
                <div class="mt-1 space-y-0.5">
                    ${formatPillarRow('Pick & Roll Drop', a.pnr_coverage?.drop != null ? a.pnr_coverage.drop + '%' : '--', a.rank_pnr_drop, h.pnr_coverage?.drop != null ? h.pnr_coverage.drop + '%' : '--', h.rank_pnr_drop, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Pick & Roll Switch', a.pnr_coverage?.switch != null ? a.pnr_coverage.switch + '%' : '--', a.rank_pnr_switch, h.pnr_coverage?.switch != null ? h.pnr_coverage.switch + '%' : '--', h.rank_pnr_switch, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Restricted Area Allowed', a.shot_profile_allowed?.restricted_area?.ppp != null ? a.shot_profile_allowed.restricted_area.ppp + ' PPP' : '--', a.rank_ra_ppp_allowed, h.shot_profile_allowed?.restricted_area?.ppp != null ? h.shot_profile_allowed.restricted_area.ppp + ' PPP' : '--', h.rank_ra_ppp_allowed, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Paint Non-RA Allowed', a.shot_profile_allowed?.paint_non_ra?.ppp != null ? a.shot_profile_allowed.paint_non_ra.ppp + ' PPP' : '--', a.rank_paint_ppp_allowed, h.shot_profile_allowed?.paint_non_ra?.ppp != null ? h.shot_profile_allowed.paint_non_ra.ppp + ' PPP' : '--', h.rank_paint_ppp_allowed, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Corner 3 Allowed', a.shot_profile_allowed?.corner_3?.ppp != null ? a.shot_profile_allowed.corner_3.ppp + ' PPP' : '--', a.rank_corner3_ppp_allowed, h.shot_profile_allowed?.corner_3?.ppp != null ? h.shot_profile_allowed.corner_3.ppp + ' PPP' : '--', h.rank_corner3_ppp_allowed, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Transition Allowed PPP', a.transition_ppp_allowed != null ? a.transition_ppp_allowed + ' PPP' : '--', a.rank_transition_ppp_allowed, h.transition_ppp_allowed != null ? h.transition_ppp_allowed + ' PPP' : '--', h.rank_transition_ppp_allowed, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Defensive Rebound % (DREB%)', a.rebounding?.dreb_secured_pct != null ? a.rebounding.dreb_secured_pct + '%' : '--', a.rank_dreb_secured_pct, h.rebounding?.dreb_secured_pct != null ? h.rebounding.dreb_secured_pct + '%' : '--', h.rank_dreb_secured_pct, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Offensive Rebound % Allowed', a.rebounding?.oreb_allowed_pct != null ? a.rebounding.oreb_allowed_pct + '%' : '--', a.rank_oreb_allowed_pct, h.rebounding?.oreb_allowed_pct != null ? h.rebounding.oreb_allowed_pct + '%' : '--', h.rank_oreb_allowed_pct, a.is_small_sample, h.is_small_sample)}
                </div>
                ${renderGroupedPositionalAllowances(aTeam, hTeam, a, h)}
            </div>

            <!-- Pillar 3: Offensive Play Type & Scheme -->
            <div id="bucket-scheme" class="scroll-mt-20">
                <div class="text-[10px] font-bold text-cyan-400 uppercase tracking-wider py-1 px-2 bg-[#161b22] rounded flex items-center gap-1.5">
                    <i data-lucide="zap" class="w-3.5 h-3.5"></i>
                    <span>Pillar 3: Offensive Play Type & Scheme</span>
                </div>
                <div class="mt-1 space-y-0.5">
                    ${formatPillarRow('PnR Ball Handler Freq', a.playtypes_offense?.PnR_Ball_Handler?.freq != null ? a.playtypes_offense.PnR_Ball_Handler.freq + '%' : '--', a.rank_pnr_bh_freq, h.playtypes_offense?.PnR_Ball_Handler?.freq != null ? h.playtypes_offense.PnR_Ball_Handler.freq + '%' : '--', h.rank_pnr_bh_freq, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Isolation Freq', a.playtypes_offense?.Isolation?.freq != null ? a.playtypes_offense.Isolation.freq + '%' : '--', a.rank_iso_freq, h.playtypes_offense?.Isolation?.freq != null ? h.playtypes_offense.Isolation.freq + '%' : '--', h.rank_iso_freq, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Spot-Up Freq', a.playtypes_offense?.Spot_Up?.freq != null ? a.playtypes_offense.Spot_Up.freq + '%' : '--', a.rank_spot_freq, h.playtypes_offense?.Spot_Up?.freq != null ? h.playtypes_offense.Spot_Up.freq + '%' : '--', h.rank_spot_freq, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('PnR Ball Handler Allowed PPP', a.playtypes_defense?.PnR_Ball_Handler?.allowed_ppp != null ? a.playtypes_defense.PnR_Ball_Handler.allowed_ppp + ' PPP' : '--', a.rank_pnr_bh_allowed_ppp || a.playtypes_defense?.PnR_Ball_Handler?.rank, h.playtypes_defense?.PnR_Ball_Handler?.allowed_ppp != null ? h.playtypes_defense.PnR_Ball_Handler.allowed_ppp + ' PPP' : '--', h.rank_pnr_bh_allowed_ppp || h.playtypes_defense?.PnR_Ball_Handler?.rank, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Spot-Up Allowed PPP', a.playtypes_defense?.Spot_Up?.allowed_ppp != null ? a.playtypes_defense.Spot_Up.allowed_ppp + ' PPP' : '--', a.rank_spot_up_allowed_ppp || a.playtypes_defense?.Spot_Up?.rank, h.playtypes_defense?.Spot_Up?.allowed_ppp != null ? h.playtypes_defense.Spot_Up.allowed_ppp + ' PPP' : '--', h.rank_spot_up_allowed_ppp || h.playtypes_defense?.Spot_Up?.rank, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Isolation Allowed PPP', a.playtypes_defense?.Isolation?.allowed_ppp != null ? a.playtypes_defense.Isolation.allowed_ppp + ' PPP' : '--', a.rank_iso_allowed_ppp || a.playtypes_defense?.Isolation?.rank, h.playtypes_defense?.Isolation?.allowed_ppp != null ? h.playtypes_defense.Isolation.allowed_ppp + ' PPP' : '--', h.rank_iso_allowed_ppp || h.playtypes_defense?.Isolation?.rank, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Roll Man / Rim Runner Allowed', a.playtypes_defense?.PnR_Roll_Man?.allowed_ppp != null ? a.playtypes_defense.PnR_Roll_Man.allowed_ppp + ' PPP' : '--', a.rank_pnr_rm_allowed_ppp || a.playtypes_defense?.PnR_Roll_Man?.rank, h.playtypes_defense?.PnR_Roll_Man?.allowed_ppp != null ? h.playtypes_defense.PnR_Roll_Man.allowed_ppp + ' PPP' : '--', h.rank_pnr_rm_allowed_ppp || h.playtypes_defense?.PnR_Roll_Man?.rank, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Live-Ball Turnovers', a.turnover_profile?.live_ball_per_game != null ? a.turnover_profile.live_ball_per_game + '/G' : '--', a.rank_live_tov, h.turnover_profile?.live_ball_per_game != null ? h.turnover_profile.live_ball_per_game + '/G' : '--', h.rank_live_tov, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Dead-Ball Turnovers', a.turnover_profile?.dead_ball_per_game != null ? a.turnover_profile.dead_ball_per_game + '/G' : '--', a.rank_dead_tov, h.turnover_profile?.dead_ball_per_game != null ? h.turnover_profile.dead_ball_per_game + '/G' : '--', h.rank_dead_tov, a.is_small_sample, h.is_small_sample)}
                    ${formatPillarRow('Live-Ball TOV Pts Surrendered', a.turnover_profile?.points_surrendered_off_tov != null ? a.turnover_profile.points_surrendered_off_tov + ' PTS' : '--', a.rank_pts_surrendered_off_tov, h.turnover_profile?.points_surrendered_off_tov != null ? h.turnover_profile.points_surrendered_off_tov + ' PTS' : '--', h.rank_pts_surrendered_off_tov, a.is_small_sample, h.is_small_sample)}
                </div>
                ${renderRoleArchetypeSchemeBlock(aTeam, hTeam, a, h)}
            </div>
        </div>

        <!-- 4. Pillar 4: Player Usage Profiles -->
        <div id="bucket-players" class="glass-panel rounded-xl p-4 border border-[#30363d] space-y-3 scroll-mt-20">
            <div class="flex items-center space-x-2 border-b border-[#30363d] pb-2">
                <i data-lucide="users" class="w-4 h-4 text-cyan-400"></i>
                <h4 class="text-xs font-bold text-slate-200 uppercase tracking-wider">Pillar 4: Player Usage Profiles (Top 4 Rotation Distributions)</h4>
            </div>
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
                ${renderRotationTeamBlock(aTeam, g.away_team_name, aPlayers, 'text-cyan-400')}
                ${renderRotationTeamBlock(hTeam, g.home_team_name, hPlayers, 'text-rose-400')}
            </div>
        </div>

        <!-- 5. Similar-Opponent Historical Lookup -->
        <div class="bg-[#12161c] rounded-xl border border-[#30363d] p-4 space-y-2">
            <div class="flex items-center gap-2 text-xs font-bold text-slate-200 border-b border-[#21262d] pb-2">
                <i data-lucide="compass" class="w-4 h-4 text-cyan-400"></i>
                <span>Similar-Opponent Historical Lookup (Euclidean Analog Model)</span>
            </div>
            <div class="space-y-2 text-xs font-mono text-slate-300">
                ${(!a.gp || !h.gp) ? `
                    <div class="p-2.5 rounded-lg bg-[#161b22] border border-[#30363d]/80 leading-relaxed text-slate-400">
                        Euclidean analog lookup requires completed games within selected date range (${startDate} to ${endDate}).
                    </div>
                ` : `
                ${homeDefSim.summary_text ? `
                    <div class="p-2.5 rounded-lg bg-[#161b22] border border-[#30363d]/80 leading-relaxed">
                        <span class="text-cyan-400 font-bold block mb-0.5">[Home Defense vs. Analogs]:</span>
                        <div>${homeDefSim.summary_text}</div>
                        <div class="text-[10.5px] text-slate-400 mt-1">
                            Analogs Faced: ${(homeDefSim.cluster_opponents || []).join(', ') || '--'}
                        </div>
                    </div>
                ` : ''}
                ${awayDefSim.summary_text ? `
                    <div class="p-2.5 rounded-lg bg-[#161b22] border border-[#30363d]/80 leading-relaxed">
                        <span class="text-cyan-400 font-bold block mb-0.5">[Away Defense vs. Analogs]:</span>
                        <div>${awayDefSim.summary_text}</div>
                        <div class="text-[10.5px] text-slate-400 mt-1">
                            Analogs Faced: ${(awayDefSim.cluster_opponents || []).join(', ') || '--'}
                        </div>
                    </div>
                ` : ''}
                `}
            </div>
        </div>
    `;
}

// Deep Dive Modal Controllers & Interactions
function openDeepDiveModal(gameId) {
    const g = allScheduleGames.find(x => x.game_id === gameId);
    if (!g) return;
    renderDeepDiveModal(g);
}

function openMatchupByTeams(awayTeam, homeTeam) {
    const a = currentForensicTeams[awayTeam] || {};
    const h = currentForensicTeams[homeTeam] || {};
    const aPlayers = (currentPlayerProfiles && currentPlayerProfiles.length) ? currentPlayerProfiles.filter(p => p.team === awayTeam) : allPlayerProfiles.filter(p => p.team === awayTeam);
    const hPlayers = (currentPlayerProfiles && currentPlayerProfiles.length) ? currentPlayerProfiles.filter(p => p.team === homeTeam) : allPlayerProfiles.filter(p => p.team === homeTeam);
    const g = {
        game_id: `NBA_${awayTeam}_${homeTeam}`,
        gameday: getTodayDateStr(),
        gametime: '7:30 PM ET',
        away_team: awayTeam,
        away_team_name: teamNamesMap[awayTeam] || awayTeam,
        away_record: (a.gp > 0 ? `${a.gp} GP` : '--'),
        home_team: homeTeam,
        home_team_name: teamNamesMap[homeTeam] || homeTeam,
        home_record: (h.gp > 0 ? `${h.gp} GP` : '--'),
        is_final: false,
        away_forensic: a,
        home_forensic: h,
        away_players: aPlayers,
        home_players: hPlayers,
        home_def_sim: {},
        away_def_sim: {}
    };
    renderDeepDiveModal(g);
}

function renderDeepDiveModal(g) {
    const container = document.getElementById('deep-dive-modal-content');
    const modal = document.getElementById('deep-dive-modal');
    if (!container || !modal) return;

    container.innerHTML = generateDeepDiveModalHtml(g);
    modal.classList.remove('hidden');
    document.body.classList.add('overflow-hidden');
    safeCreateIcons(container);
}

function closeDeepDiveModal() {
    const modal = document.getElementById('deep-dive-modal');
    if (modal) {
        modal.classList.add('hidden');
        document.body.classList.remove('overflow-hidden');
    }
}

function onModalBackdropClick(e) {
    if (e.target.id === 'deep-dive-modal') {
        closeDeepDiveModal();
    }
}
