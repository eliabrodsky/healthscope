import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

/**
 * Offline build function: Generates county-level data packs from ZctaBoundary + ZipDemographics
 * Run this periodically (quarterly) to rebuild versioned packs
 */

Deno.serve(async (req) => {
    const startTime = Date.now();
    
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 401 });
        }

        const { county_fips, state_abbr } = await req.json();
        
        console.log(`[0ms] Building county pack for ${county_fips || state_abbr || 'ALL'}`);

        // Fetch all boundaries for this county/state
        const filter = county_fips 
            ? { county_fips } 
            : state_abbr 
            ? { state_abbr: state_abbr.toUpperCase() }
            : {};
        
        const boundaries = await base44.asServiceRole.entities.ZctaBoundary.list();
        const filteredBoundaries = boundaries.filter(b => {
            if (county_fips) return b.county_fips === county_fips;
            if (state_abbr) return b.state_abbr === state_abbr.toUpperCase();
            return true;
        });

        console.log(`[${Date.now() - startTime}ms] Found ${filteredBoundaries.length} boundaries`);

        // Fetch demographics for these ZCTAs
        const zctas = filteredBoundaries.map(b => b.zcta5);
        const demographics = await base44.asServiceRole.entities.ZipDemographics.list();
        const filteredDemographics = demographics.filter(d => zctas.includes(d.zcta5));

        console.log(`[${Date.now() - startTime}ms] Found ${filteredDemographics.length} demographics`);

        // Build index (for bbox queries)
        const index = filteredBoundaries.map(b => {
            const geometry = b.geometry;
            const coords = geometry.type === 'Polygon' 
                ? geometry.coordinates[0] 
                : geometry.coordinates[0][0];
            
            const lons = coords.map(c => c[0]);
            const lats = coords.map(c => c[1]);
            
            return {
                z: b.zcta5,
                bbox: [
                    Math.min(...lons),
                    Math.min(...lats),
                    Math.max(...lons),
                    Math.max(...lats)
                ],
                centroid: [
                    (Math.min(...lons) + Math.max(...lons)) / 2,
                    (Math.min(...lats) + Math.max(...lats)) / 2
                ]
            };
        });

        // Build stats
        const stats = {};
        for (const demo of filteredDemographics) {
            stats[demo.zcta5] = {
                pop: demo.population || 0,
                income: demo.median_income || null,
                poverty: demo.poverty_rate || null,
                uninsured: demo.uninsured_rate || null
            };
        }

        // Build membership (simplified - assume 100% for now)
        const membership = {};
        for (const b of filteredBoundaries) {
            membership[b.zcta5] = [{
                county_fips: county_fips || 'UNKNOWN',
                pct: 1.0
            }];
        }

        const elapsed = Date.now() - startTime;
        
        return Response.json({
            success: true,
            version: 'v2025-01',
            generated_at: new Date().toISOString(),
            elapsed_ms: elapsed,
            pack: {
                index,
                stats,
                membership
            }
        });

    } catch (error) {
        console.error('Error building county pack:', error);
        return Response.json({ 
            success: false, 
            error: error.message 
        }, { status: 500 });
    }
});