import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

/**
 * Loads ZCTA-County crosswalk data from CSV text
 * Supports two formats:
 * 1. HUD format: ZCTA, County FIPS, County Name, State, etc.
 * 2. Simple format: state_abbr, county_name, ZIP code, total_population
 */

const BATCH_SIZE = 500;
const MAX_RUNTIME_MS = 55000;

Deno.serve(async (req) => {
    const startTime = Date.now();
    
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 401 });
        }

        const body = await req.json();
        const csvText = body?.csvText;
        const filterState = body?.stateAbbr?.toUpperCase();
        
        if (!csvText) {
            return Response.json({ error: 'csvText required' }, { status: 400 });
        }

        console.log(`[0ms] Starting crosswalk load${filterState ? ` for ${filterState}` : ''}`);

        const lines = csvText.trim().split('\n');
        const headerLine = lines[0];
        const headers = headerLine.split(',').map(h => h.trim().toLowerCase());
        
        console.log(`[${Date.now() - startTime}ms] Headers: ${headers.join(', ')}`);
        console.log(`[${Date.now() - startTime}ms] Total rows: ${lines.length - 1}`);

        // Detect format
        const isSimpleFormat = headers.includes('state_abbr') && 
                              headers.includes('county_name') && 
                              headers.includes('zip code');

        console.log(`[${Date.now() - startTime}ms] Format detected: ${isSimpleFormat ? 'Simple (state_abbr, county_name, zip code)' : 'HUD (ZCTA, County FIPS)'}`);

        let colMap;
        if (isSimpleFormat) {
            colMap = {
                stateAbbr: headers.indexOf('state_abbr'),
                countyName: headers.indexOf('county_name'),
                zcta: headers.indexOf('zip code'),
                population: headers.indexOf('total_population')
            };
        } else {
            colMap = {
                zcta: headers.findIndex(h => h.includes('zip') || h.includes('zcta')),
                countyFips: headers.findIndex(h => h.includes('county') && (h.includes('fips') || h.includes('geoid'))),
                countyName: headers.findIndex(h => h.includes('county') && h.includes('name')),
                stateAbbr: headers.findIndex(h => h.includes('state') && (h.includes('abbr') || h.includes('usps'))),
                stateName: headers.findIndex(h => h.includes('state') && h.includes('name'))
            };
        }

        console.log(`[${Date.now() - startTime}ms] Column mapping:`, JSON.stringify(colMap));

        const stateMap = new Map();
        const countyMap = new Map();
        const groupings = [];
        const demographics = [];
        let rowsSkipped = 0;
        let rowsProcessed = 0;

        for (let i = 1; i < lines.length; i++) {
            if (Date.now() - startTime > MAX_RUNTIME_MS - 8000) {
                console.log(`[${Date.now() - startTime}ms] Time limit - stopping at row ${i}`);
                break;
            }

            const values = lines[i].split(',');
            
            if (isSimpleFormat) {
                const stateAbbr = values[colMap.stateAbbr]?.trim()?.toUpperCase();
                const countyName = values[colMap.countyName]?.trim();
                const zcta = values[colMap.zcta]?.trim();
                const population = parseFloat(values[colMap.population]) || 0;

                if (!stateAbbr || !countyName || !zcta) {
                    rowsSkipped++;
                    continue;
                }

                if (filterState && stateAbbr !== filterState) {
                    rowsSkipped++;
                    continue;
                }

                // Add state
                if (!stateMap.has(stateAbbr)) {
                    stateMap.set(stateAbbr, {
                        abbreviation: stateAbbr,
                        name: stateAbbr
                    });
                }

                // Add county (generate FIPS from state + county name hash)
                const countyKey = `${stateAbbr}_${countyName}`;
                const countyFips = countyKey.split('').reduce((hash, char) => 
                    ((hash << 5) - hash + char.charCodeAt(0)) | 0, 0
                ).toString().slice(-5).padStart(5, '0');
                
                if (!countyMap.has(countyKey)) {
                    countyMap.set(countyKey, {
                        name: countyName,
                        state_abbreviation: stateAbbr,
                        state_name: stateAbbr,
                        fips_code: countyFips
                    });
                }

                // Add grouping (dedupe by ZCTA)
                const existingGrouping = groupings.find(g => g.zcta5 === zcta);
                if (!existingGrouping) {
                    groupings.push({
                        zcta5: zcta,
                        county_fips: countyFips,
                        county_name: countyName,
                        state: stateAbbr
                    });
                }

                // Add demographics
                const existingDemo = demographics.find(d => d.zcta5 === zcta);
                if (!existingDemo && population > 0) {
                    demographics.push({
                        zcta5: zcta,
                        acs_year: '2022',
                        state_abbr: stateAbbr,
                        population: Math.round(population),
                        source: 'CSV Upload',
                        source_version: 'user_upload'
                    });
                }

                rowsProcessed++;
            } else {
                // Original HUD format parsing
                const zcta = values[colMap.zcta]?.trim();
                const countyFips = values[colMap.countyFips]?.trim();
                const countyName = colMap.countyName !== -1 ? values[colMap.countyName]?.trim() : '';
                const stateAbbr = values[colMap.stateAbbr]?.trim()?.toUpperCase();
                const stateName = colMap.stateName !== -1 ? values[colMap.stateName]?.trim() : '';

                if (!zcta || !countyFips || !stateAbbr) {
                    rowsSkipped++;
                    continue;
                }

                if (filterState && stateAbbr !== filterState) {
                    rowsSkipped++;
                    continue;
                }

                if (!stateMap.has(stateAbbr)) {
                    stateMap.set(stateAbbr, {
                        abbreviation: stateAbbr,
                        name: stateName || stateAbbr
                    });
                }

                const countyKey = `${countyFips}_${stateAbbr}`;
                if (!countyMap.has(countyKey)) {
                    countyMap.set(countyKey, {
                        name: countyName || `County ${countyFips}`,
                        state_abbreviation: stateAbbr,
                        state_name: stateName || stateAbbr,
                        fips_code: countyFips
                    });
                }

                groupings.push({
                    zcta5: zcta,
                    county_fips: countyFips,
                    county_name: countyName || `County ${countyFips}`,
                    state: stateAbbr
                });

                rowsProcessed++;
            }
        }

        console.log(`[${Date.now() - startTime}ms] Parsed: ${stateMap.size} states, ${countyMap.size} counties, ${groupings.length} groupings, ${demographics.length} demographics (processed ${rowsProcessed}, skipped ${rowsSkipped})`);

        // Insert states
        let statesInserted = 0;
        for (const state of stateMap.values()) {
            try {
                await base44.asServiceRole.entities.State.create(state);
                statesInserted++;
            } catch (error) {
                if (!error.message?.includes('unique') && !error.message?.includes('duplicate')) {
                    console.warn(`State ${state.abbreviation}:`, error.message);
                }
            }
        }
        console.log(`[${Date.now() - startTime}ms] ✓ States: ${statesInserted}`);

        // Insert counties
        const counties = Array.from(countyMap.values());
        let countiesInserted = 0;
        
        for (let i = 0; i < counties.length; i += BATCH_SIZE) {
            const batch = counties.slice(i, i + BATCH_SIZE);
            try {
                await base44.asServiceRole.entities.County.bulkCreate(batch);
                countiesInserted += batch.length;
            } catch (bulkError) {
                for (const county of batch) {
                    try {
                        await base44.asServiceRole.entities.County.create(county);
                        countiesInserted++;
                    } catch (error) {
                        if (!error.message?.includes('unique') && !error.message?.includes('duplicate')) {
                            console.warn(`County ${county.fips_code}:`, error.message);
                        }
                    }
                }
            }
        }
        console.log(`[${Date.now() - startTime}ms] ✓ Counties: ${countiesInserted}`);

        // Insert groupings
        let groupingsInserted = 0;
        for (let i = 0; i < groupings.length; i += BATCH_SIZE) {
            if (Date.now() - startTime > MAX_RUNTIME_MS - 5000) break;
            
            const batch = groupings.slice(i, i + BATCH_SIZE);
            try {
                await base44.asServiceRole.entities.GroupingCounty.bulkCreate(batch);
                groupingsInserted += batch.length;
            } catch (bulkError) {
                for (const grouping of batch) {
                    try {
                        await base44.asServiceRole.entities.GroupingCounty.create(grouping);
                        groupingsInserted++;
                    } catch (error) {
                        if (!error.message?.includes('unique') && !error.message?.includes('duplicate')) {
                            // Skip silently
                        }
                    }
                }
            }
        }
        console.log(`[${Date.now() - startTime}ms] ✓ Groupings: ${groupingsInserted}`);

        // Insert demographics if available
        let demographicsInserted = 0;
        if (demographics.length > 0) {
            for (let i = 0; i < demographics.length; i += BATCH_SIZE) {
                if (Date.now() - startTime > MAX_RUNTIME_MS - 3000) break;
                
                const batch = demographics.slice(i, i + BATCH_SIZE);
                try {
                    await base44.asServiceRole.entities.ZipDemographics.bulkCreate(batch);
                    demographicsInserted += batch.length;
                } catch (bulkError) {
                    for (const demo of batch) {
                        try {
                            await base44.asServiceRole.entities.ZipDemographics.create(demo);
                            demographicsInserted++;
                        } catch (error) {
                            if (!error.message?.includes('unique') && !error.message?.includes('duplicate')) {
                                // Skip silently
                            }
                        }
                    }
                }
            }
            console.log(`[${Date.now() - startTime}ms] ✓ Demographics: ${demographicsInserted}`);
        }

        // Update StateDataStatus for each state
        if (filterState) {
            try {
                const existingStatus = await base44.asServiceRole.entities.StateDataStatus.filter({
                    state_abbr: filterState
                });

                const statusData = {
                    state_abbr: filterState,
                    state_name: filterState,
                    demographics_count: demographicsInserted,
                    demographics_complete: demographicsInserted > 0,
                    last_demographics_update: new Date().toISOString()
                };

                if (existingStatus && existingStatus.length > 0) {
                    await base44.asServiceRole.entities.StateDataStatus.update(
                        existingStatus[0].id,
                        statusData
                    );
                } else {
                    await base44.asServiceRole.entities.StateDataStatus.create(statusData);
                }
                
                console.log(`[${Date.now() - startTime}ms] ✓ Updated StateDataStatus for ${filterState}`);
            } catch (statusError) {
                console.warn(`Failed to update StateDataStatus:`, statusError.message);
            }
        }

        const elapsed = Date.now() - startTime;
        const summary = filterState 
            ? `✅ ${filterState}: ${countiesInserted} counties, ${groupingsInserted} mappings` + (demographicsInserted > 0 ? `, ${demographicsInserted} demographics` : '')
            : `✅ Loaded: ${statesInserted} states, ${countiesInserted} counties, ${groupingsInserted} mappings` + (demographicsInserted > 0 ? `, ${demographicsInserted} demographics` : '');

        console.log(`[${elapsed}ms] COMPLETE! ${summary}`);

        return Response.json({
            success: true,
            message: summary,
            stats: {
                filterState: filterState || 'all',
                states: statesInserted,
                counties: countiesInserted,
                groupings: groupingsInserted,
                demographics: demographicsInserted,
                timeMs: elapsed
            }
        });

    } catch (error) {
        const elapsed = Date.now() - startTime;
        console.error(`[${elapsed}ms] ERROR:`, error);
        return Response.json({ 
            success: false, 
            error: error.message,
            timeMs: elapsed
        }, { status: 500 });
    }
});