import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

/**
 * Returns hierarchical data: State → Counties → ZCTAs with population totals
 * Uses GroupingCounty to get real county names from the loaded crosswalk data
 */

Deno.serve(async (req) => {
    const startTime = Date.now();
    
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
        const state_abbr = body?.state_abbr;
        
        if (!state_abbr) {
            return Response.json({ error: 'state_abbr required' }, { status: 400 });
        }

        console.log(`[getStateHierarchy] ${state_abbr} - Starting`);

        // Get county groupings from the crosswalk data (this should be fast)
        const countyGroupings = await Promise.race([
            base44.asServiceRole.entities.GroupingCounty.filter({
                state: state_abbr.toUpperCase()
            }),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error('County groupings timeout')), 15000)
            )
        ]).catch((error) => {
            console.log(`[getStateHierarchy] ${state_abbr} - County groupings failed:`, error.message);
            return [];
        });

        console.log(`[getStateHierarchy] ${state_abbr} - Found ${countyGroupings?.length || 0} county groupings`);

        // If no groupings, return early - this state hasn't had crosswalk data loaded
        if (!countyGroupings || countyGroupings.length === 0) {
            return Response.json({
                state_abbr: state_abbr.toUpperCase(),
                total_population: 0,
                total_zctas: 0,
                total_with_demographics: 0,
                counties: [],
                message: 'No county data loaded yet. Upload ZCTA-County CSV via Data Upload first.'
            });
        }

        // Get demographics for this state (with error handling for large datasets)
        const demographics = await Promise.race([
            base44.asServiceRole.entities.ZipDemographics.filter({
                state_abbr: state_abbr.toUpperCase()
            }),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Demographics query timeout')), 15000)
            )
        ]).catch((error) => {
            console.log(`[getStateHierarchy] ${state_abbr} - Demographics query failed:`, error.message);
            return [];
        });

        console.log(`[getStateHierarchy] ${state_abbr} - Found ${demographics?.length || 0} demographics records`);

        // Build county map using the crosswalk data
        const countyMap = new Map();
        
        for (const grouping of countyGroupings) {
            const countyFips = grouping.county_fips;
            const countyName = grouping.county_name;
            const zcta = grouping.zcta5;
            
            if (!countyMap.has(countyFips)) {
                countyMap.set(countyFips, {
                    fips: countyFips,
                    name: countyName,
                    population: 0,
                    zcta_count: 0,
                    has_demographics: false,
                    zctas: new Set()
                });
            }
            
            const county = countyMap.get(countyFips);
            county.zctas.add(zcta);
            county.zcta_count = county.zctas.size;
            
            // Add population from demographics if available
            if (demographics && demographics.length > 0) {
                const demo = demographics.find(d => d.zcta5 === zcta);
                if (demo) {
                    const pop = demo.population || 0;
                    county.population += pop;
                    if (pop > 0) county.has_demographics = true;
                }
            }
        }

        // Convert to array and remove the zctas Set
        const counties = Array.from(countyMap.values()).map(county => ({
            fips: county.fips,
            name: county.name,
            population: county.population,
            zcta_count: county.zcta_count,
            has_demographics: county.has_demographics
        }));

        const totalPopulation = counties.reduce((sum, county) => sum + county.population, 0);
        const totalZctas = new Set(countyGroupings.map(g => g.zcta5)).size;

        const elapsed = Date.now() - startTime;
        console.log(`[getStateHierarchy] ${state_abbr} - Complete in ${elapsed}ms. Pop: ${totalPopulation}, Counties: ${counties.length}, ZCTAs: ${totalZctas}`);

        return Response.json({
            state_abbr: state_abbr.toUpperCase(),
            total_population: totalPopulation,
            total_zctas: totalZctas,
            total_with_demographics: demographics?.length || 0,
            counties: counties.sort((a, b) => b.population - a.population).slice(0, 200)
        });

    } catch (error) {
        const elapsed = Date.now() - startTime;
        console.error(`[getStateHierarchy] Error after ${elapsed}ms:`, error.message);
        
        return Response.json({ 
            error: 'Failed to load hierarchy data',
            details: error.message,
            hint: 'Upload ZCTA-County CSV via Data Upload to populate county mappings'
        }, { status: 500 });
    }
});