import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

// Primary: GitHub repository with state-specific files
const GITHUB_BASE_URL = "https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master";

const MAX_RUNTIME_MS = 40000;
const DOWNLOAD_TIMEOUT_MS = 15000;
const HEARTBEAT_INTERVAL_MS = 5000;
const BATCH_SIZE = 250;
const MIN_BUFFER_MS = 3000;

async function resetStatusToIdle(base44, statusRecord, errorMsg = null) {
    if (!statusRecord?.id) return;
    
    try {
        const updateData = {
            loading_status: 'idle',
            last_boundary_update: new Date().toISOString()
        };
        
        if (errorMsg) {
            updateData.last_error = errorMsg;
        }
        
        await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, updateData);
        console.log(`✓ Reset status to idle${errorMsg ? ` (error: ${errorMsg})` : ''}`);
    } catch (e) {
        console.warn('Failed to reset status:', e.message);
    }
}

async function updateHeartbeat(base44, statusRecord, cursor, processed, zctaCache) {
    if (!statusRecord?.id) return;
    
    try {
        const updateData = {
            boundary_count: processed,
            resume_cursor: cursor,
            last_boundary_update: new Date().toISOString(),
            loading_status: 'loading_boundaries'
        };
        
        if (zctaCache && zctaCache.length > 0) {
            updateData.zcta_cache = zctaCache;
            updateData.zcta_cache_count = zctaCache.length;
        }
        
        await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, updateData);
        console.log(`💓 Heartbeat: ${processed} processed, cursor: ${cursor}, cache: ${zctaCache?.length || 0} ZCTAs`);
    } catch (e) {
        console.warn('Heartbeat failed:', e.message);
    }
}

async function bulkUpsertBoundaries(base44, batch) {
    const inserted = [];
    const skipped = [];
    
    try {
        await base44.asServiceRole.entities.ZctaBoundary.bulkCreate(batch);
        inserted.push(...batch);
    } catch (bulkError) {
        console.warn('Bulk insert failed, trying individual inserts:', bulkError.message);
        
        for (const item of batch) {
            try {
                await base44.asServiceRole.entities.ZctaBoundary.create(item);
                inserted.push(item);
            } catch (individualError) {
                if (individualError.message?.includes('unique') || 
                    individualError.message?.includes('duplicate')) {
                    skipped.push(item.zcta5);
                } else {
                    console.warn(`⚠️ Failed to insert ${item.zcta5}:`, individualError.message);
                }
            }
        }
    }
    
    return { inserted: inserted.length, skipped: skipped.length };
}

async function fetchGeoJSONFromGitHub(stateAbbr, stateName, startTime) {
    const stateNameLower = stateName.toLowerCase().replace(/ /g, '_');
    const stateAbbrLower = stateAbbr.toLowerCase();
    const stateAbbrUpper = stateAbbr.toUpperCase();
    
    // Try many URL patterns
    const urlPatterns = [
        `${GITHUB_BASE_URL}/${stateAbbrLower}_${stateNameLower}_zip_codes_geo.min.json`,
        `${GITHUB_BASE_URL}/${stateNameLower}_zip_codes_geo.min.json`,
        `${GITHUB_BASE_URL}/${stateAbbrUpper}_${stateNameLower}_zip_codes_geo.min.json`,
        `${GITHUB_BASE_URL}/${stateAbbrLower}_zip_codes_geo.min.json`,
        `${GITHUB_BASE_URL}/${stateAbbrUpper}_zip_codes_geo.min.json`,
        `${GITHUB_BASE_URL}/${stateAbbrLower}_${stateNameLower}_zip_codes_geo.json`,
        `${GITHUB_BASE_URL}/${stateNameLower}_zip_codes_geo.json`,
    ];
    
    for (const [index, url] of urlPatterns.entries()) {
        console.log(`[${Date.now() - startTime}ms] Try ${index + 1}/${urlPatterns.length}: ${url.split('/').pop()}`);
        
        try {
            const response = await Promise.race([
                fetch(url, {
                    headers: {
                        'User-Agent': 'HealthScope/1.0',
                        'Accept': 'application/json'
                    }
                }),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('timeout')), DOWNLOAD_TIMEOUT_MS)
                )
            ]);

            if (response.ok) {
                console.log(`[${Date.now() - startTime}ms] ✓ Found at: ${url.split('/').pop()}`);
                const geojsonData = await response.json();
                return { success: true, data: geojsonData, url };
            }
            
            if (response.status !== 404) {
                console.warn(`[${Date.now() - startTime}ms] HTTP ${response.status}`);
            }
            
        } catch (error) {
            if (!error.message.includes('timeout')) {
                console.warn(`[${Date.now() - startTime}ms] Error: ${error.message}`);
            }
        }
    }
    
    return { 
        success: false, 
        error: `No GeoJSON file found for ${stateName} in GitHub repository`
    };
}

Deno.serve(async (req) => {
    const startTime = Date.now();
    let statusRecord = null;
    let base44;
    let lastHeartbeat = startTime;
    
    try {
        base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
        const { stateAbbr, stateName } = body || {};

        if (!stateAbbr || !stateName) {
            return Response.json({ 
                success: false,
                error: 'State abbreviation and name are required' 
            }, { status: 400 });
        }

        console.log(`[0ms] ========== Loading ${stateName} (${stateAbbr}) ==========`);

        // Get or create status record
        const statusRecords = await base44.asServiceRole.entities.StateDataStatus.filter({ 
            state_abbr: stateAbbr 
        });

        statusRecord = statusRecords?.[0];
        
        if (!statusRecord) {
            console.log(`Creating new status record for ${stateAbbr}`);
            statusRecord = await base44.asServiceRole.entities.StateDataStatus.create({
                state_abbr: stateAbbr,
                state_name: stateName,
                boundary_count: 0,
                boundaries_complete: false,
                loading_status: 'loading_boundaries',
                resume_cursor: 0,
                zcta_cache: [],
                zcta_cache_count: 0,
                zcta_cache_ready: false,
                last_boundary_update: new Date().toISOString()
            });
        } else {
            await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
                loading_status: 'loading_boundaries',
                last_boundary_update: new Date().toISOString()
            });
        }

        // Fetch GeoJSON
        console.log(`[${Date.now() - startTime}ms] Fetching from GitHub...`);
        
        const fetchResult = await fetchGeoJSONFromGitHub(stateAbbr, stateName, startTime);
        
        if (!fetchResult.success) {
            console.error(`[${Date.now() - startTime}ms] ✗ Download failed`);
            await resetStatusToIdle(base44, statusRecord, fetchResult.error);
            
            return Response.json({
                success: false,
                error: fetchResult.error,
                note: `The GeoJSON file for ${stateName} may not be available. Try another state or contact support.`
            }, { status: 404 });
        }
        
        const geojsonData = fetchResult.data;

        if (!geojsonData?.features || !Array.isArray(geojsonData.features)) {
            await resetStatusToIdle(base44, statusRecord, 'Invalid GeoJSON structure');
            return Response.json({
                success: false,
                error: 'Invalid GeoJSON structure'
            }, { status: 500 });
        }

        const totalFeatures = geojsonData.features.length;
        console.log(`[${Date.now() - startTime}ms] Found ${totalFeatures} ZCTAs`);

        const resumeCursor = statusRecord.resume_cursor || 0;
        const processedCount = statusRecord.boundary_count || 0;
        
        console.log(`[${Date.now() - startTime}ms] Resuming from cursor: ${resumeCursor}`);

        const zctaCache = new Set(statusRecord.zcta_cache || []);
        
        let currentCursor = resumeCursor;
        let totalInserted = 0;
        let totalSkipped = 0;
        let batch = [];
        const deadline = startTime + MAX_RUNTIME_MS - MIN_BUFFER_MS;

        for (let i = currentCursor; i < geojsonData.features.length; i++) {
            const now = Date.now();
            if (now >= deadline) {
                console.log(`[${now - startTime}ms] ⏱️ Time limit, stopping at ${i}`);
                break;
            }

            const feature = geojsonData.features[i];
            const props = feature.properties;
            const zcta = props?.ZCTA5CE10 || props?.ZCTA5CE20 || props?.ZCTA5 || props?.ZIP || props?.GEOID;
            
            if (!zcta || !feature.geometry) {
                currentCursor++;
                continue;
            }

            const zctaPadded = zcta.toString().padStart(5, '0');
            
            batch.push({
                zcta5: zctaPadded,
                geometry: feature.geometry,
                state_abbr: stateAbbr.toUpperCase(),
                state_name: stateName,
                is_po_box: false,
                area_sqkm: props.ALAND10 ? (props.ALAND10 / 1000000) : 
                           props.ALAND20 ? (props.ALAND20 / 1000000) : null
            });
            
            zctaCache.add(zctaPadded);
            currentCursor++;

            const shouldFlush = batch.length >= BATCH_SIZE || 
                               (now - lastHeartbeat) >= HEARTBEAT_INTERVAL_MS;

            if (shouldFlush && batch.length > 0) {
                const { inserted, skipped } = await bulkUpsertBoundaries(base44, batch);
                totalInserted += inserted;
                totalSkipped += skipped;
                
                const newProcessedCount = processedCount + totalInserted;
                const sortedCache = Array.from(zctaCache).sort();
                
                await updateHeartbeat(base44, statusRecord, currentCursor, newProcessedCount, sortedCache);
                lastHeartbeat = Date.now();
                
                batch = [];
            }
        }

        // Final batch
        if (batch.length > 0) {
            const { inserted, skipped } = await bulkUpsertBoundaries(base44, batch);
            totalInserted += inserted;
            totalSkipped += skipped;
        }

        const finalProcessedCount = processedCount + totalInserted;
        const isComplete = currentCursor >= totalFeatures;
        const sortedCache = Array.from(zctaCache).sort();

        console.log(`[${Date.now() - startTime}ms] ✅ Complete: +${totalInserted} inserted, ${totalSkipped} skipped`);

        const updateData = {
            boundary_count: finalProcessedCount,
            resume_cursor: currentCursor,
            boundaries_complete: isComplete,
            loading_status: 'idle',
            zcta_cache: sortedCache,
            zcta_cache_count: sortedCache.length,
            zcta_cache_ready: isComplete && sortedCache.length > 0,
            last_boundary_update: new Date().toISOString(),
            last_error: isComplete ? null : statusRecord.last_error
        };

        await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, updateData);

        const message = isComplete
            ? `✅ Complete! ${finalProcessedCount} ZCTAs loaded for ${stateName}.`
            : `✓ Processed ${totalInserted} ZCTAs. Click to continue.`;

        return Response.json({
            success: true,
            message: message,
            loaded: totalInserted,
            skipped: totalSkipped,
            total: totalFeatures,
            current: finalProcessedCount,
            cursor: currentCursor,
            isComplete: isComplete,
            cacheSize: sortedCache.length,
            processingTimeMs: Date.now() - startTime,
            source: 'GitHub'
        });

    } catch (error) {
        const elapsed = Date.now() - startTime;
        console.error(`[${elapsed}ms] 💥 ERROR:`, error);
        console.error('Stack:', error.stack);
        
        if (base44 && statusRecord) {
            await resetStatusToIdle(base44, statusRecord, error.message);
        }
        
        return Response.json({ 
            success: false, 
            error: error.message || 'Unknown error',
            hint: 'Check server logs for details',
            processingTimeMs: elapsed
        }, { status: 500 });
    }
});