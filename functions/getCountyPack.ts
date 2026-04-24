import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const url = new URL(req.url);
        const zctas = url.searchParams.get('zctas')?.split(',') || [];
        
        if (zctas.length === 0) {
            return Response.json({ error: 'ZCTA codes required' }, { status: 400 });
        }

        // FIX: Fetch only the requested boundaries using filter
        const boundaries = await base44.asServiceRole.entities.ZctaBoundary.filter({
            zcta5: { '$in': zctas }
        });

        // FIX: Fetch only requested demographics
        const demographics = await base44.asServiceRole.entities.ZipDemographics.filter({
            zcta5: { '$in': zctas }
        });

        // Build response
        const features = boundaries.map(b => {
            const demo = demographics.find(d => d.zcta5 === b.zcta5);
            
            return {
                type: 'Feature',
                geometry: b.geometry,
                properties: {
                    zcta5: b.zcta5,
                    state_abbr: b.state_abbr,
                    population: demo?.population || 0,
                    median_income: demo?.median_income || null,
                    poverty_rate: demo?.poverty_rate || null,
                    uninsured_rate: demo?.uninsured_rate || null
                }
            };
        });

        return Response.json({
            type: 'FeatureCollection',
            features,
            version: 'v2025-01',
            count: features.length,
            requested: zctas.length
        }, {
            headers: {
                'Cache-Control': 'public, max-age=3600',
                'Content-Type': 'application/json'
            }
        });

    } catch (error) {
        console.error('Error getting county pack:', error);
        return Response.json({ 
            error: error.message 
        }, { status: 500 });
    }
});