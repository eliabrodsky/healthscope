import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
    const startTime = Date.now();
    
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
        const { stateAbbr, stateName, assessmentId } = body;
        
        const zctaCodes = body.zctaCodes || body.zipCodes;

        if (!zctaCodes || zctaCodes.length === 0) {
            return Response.json({ 
                success: false,
                error: 'ZCTA codes are required' 
            }, { status: 400 });
        }

        console.log(`[0ms] Pre-processing ${zctaCodes.length} ZCTAs from DATABASE`);

        // Query each ZCTA individually to avoid large queries
        console.log(`[${Date.now() - startTime}ms] Fetching ZCTA boundaries individually...`);
        
        const boundaries = [];
        const BATCH_SIZE = 20; // Process 20 at a time concurrently
        
        for (let i = 0; i < zctaCodes.length; i += BATCH_SIZE) {
            const batch = zctaCodes.slice(i, i + BATCH_SIZE);
            
            // Fetch boundaries concurrently for this batch
            const batchPromises = batch.map(async (zcta) => {
                try {
                    const result = await base44.asServiceRole.entities.ZctaBoundary.filter({ 
                        zcta5: zcta 
                    });
                    return result;
                } catch (error) {
                    console.warn(`Failed to fetch ZCTA ${zcta}:`, error.message);
                    return [];
                }
            });
            
            const batchResults = await Promise.all(batchPromises);
            
            // Flatten results
            for (const result of batchResults) {
                if (Array.isArray(result) && result.length > 0) {
                    boundaries.push(result[0]); // Take first match
                }
            }
            
            console.log(`[${Date.now() - startTime}ms] Batch ${Math.floor(i / BATCH_SIZE) + 1}: Found ${batchResults.filter(r => r.length > 0).length} of ${batch.length}`);
        }

        console.log(`[${Date.now() - startTime}ms] Total found: ${boundaries.length} boundaries`);

        if (boundaries.length === 0) {
            return Response.json({
                success: false,
                error: `No boundaries found for the specified ZCTA codes. The states containing these ZCTAs may not have boundary data loaded yet.`,
                hint: 'Load state boundaries via Admin → Data Loader first.'
            }, { status: 404 });
        }

        // Create GeoJSON from database records
        const trimmedGeoJSON = {
            type: 'FeatureCollection',
            features: boundaries.map(boundary => ({
                type: 'Feature',
                geometry: boundary.geometry,
                properties: {
                    ZCTA5CE10: boundary.zcta5,
                    ALAND10: boundary.area_sqkm ? boundary.area_sqkm * 1000000 : null,
                    state_abbr: boundary.state_abbr,
                    state_name: boundary.state_name || 'Unknown'
                }
            }))
        };

        console.log(`[${Date.now() - startTime}ms] Created GeoJSON with ${trimmedGeoJSON.features.length} features`);

        const foundZctas = boundaries.map(b => b.zcta5);
        const missingZctas = zctaCodes.filter(z => !foundZctas.includes(z));

        // If assessment ID provided, save to assessment
        if (assessmentId) {
            try {
                console.log(`[${Date.now() - startTime}ms] Saving to assessment ${assessmentId}...`);
                
                const assessment = await base44.asServiceRole.entities.Assessment.get(assessmentId);
                
                await base44.asServiceRole.entities.Assessment.update(assessmentId, {
                    processed_data: {
                        ...assessment.processed_data,
                        trimmed_geojson: trimmedGeoJSON,
                        boundaries_loaded: true,
                        geojson_metadata: {
                            created_at: new Date().toISOString(),
                            state: stateName || 'Multiple',
                            state_abbr: stateAbbr || 'MULTI',
                            zcta_count: foundZctas.length,
                            source: 'Database (ZctaBoundary entity)',
                            zcta_codes_found: foundZctas,
                            zcta_codes_missing: missingZctas
                        }
                    }
                });
                
                console.log(`[${Date.now() - startTime}ms] Saved to assessment!`);
            } catch (saveError) {
                console.error('Failed to save to assessment:', saveError);
            }
        }

        const elapsed = Date.now() - startTime;
        console.log(`[${elapsed}ms] Pre-processing complete!`);

        return Response.json({
            success: true,
            message: missingZctas.length > 0
                ? `Loaded ${foundZctas.length} of ${zctaCodes.length} boundaries. ${missingZctas.length} ZCTAs not found.`
                : `Successfully loaded all ${foundZctas.length} ZCTA boundaries!`,
            data: {
                geojson: trimmedGeoJSON,
                feature_count: foundZctas.length,
                processing_time_ms: elapsed,
                zcta_codes_found: foundZctas,
                zcta_codes_missing: missingZctas
            }
        });

    } catch (error) {
        console.error('Error in preprocessBoundaries:', error);
        console.error('Stack:', error.stack);
        
        return Response.json({ 
            success: false, 
            error: error.message || 'Failed to load boundaries from database'
        }, { status: 500 });
    }
});