import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

/**
 * Load demographics from external ZCTA micro-service API
 * Uses: https://getzctas-1038555279570.us-south1.run.app/?zips={comma-separated-zips}
 * 
 * This replaces Census API calls with a single bulk API call
 * Returns population, income, poverty, race, age, education, housing data
 */

const MAX_RUNTIME_MS = 28000;
const SAFETY_BUFFER_MS = 8000;
const API_CHUNK_SIZE = 100;     // ZCTAs per API request
const UPSERT_BATCH = 100;       // Max ZCTAs to process per batch
const WARMING_BUDGET_MS = 15000;
const WARMING_PAGE_SIZE = 50;
const WARMING_TIMEOUT_MS = 20000;
const ZCTA_API_URL = 'https://getzctas-1038555279570.us-south1.run.app';

function now() {
    return Date.now();
}

function estimateEta(processed, total, startedAtMs) {
    if (!processed || processed === 0) return undefined;
    const elapsedSec = (now() - startedAtMs) / 1000;
    return Math.max(0, Math.round((total - processed) * (elapsedSec / processed)));
}

function emitProgress(stateAbbr, processed, total, batchSize, batchMs, apiMs, dbMs, etaSec) {
    const parts = [
        `[${stateAbbr}]`,
        `Demographics`,
        `${processed}/${total}`,
        `batch=${batchSize} (${batchMs}ms)`,
        apiMs != null ? `api=${apiMs}ms` : '',
        dbMs != null ? `db=${dbMs}ms` : '',
        etaSec != null ? `eta≈${etaSec}s` : ''
    ].filter(Boolean);
    console.log(parts.join(' '));
}

function toClientError(error) {
    const msg = error?.message ?? String(error);
    
    if (/MaxTimeMSExpired|operation exceeded time limit/i.test(msg)) {
        return { 
            code: 'DB_TIMEOUT', 
            message: 'Database query timed out. Try again shortly.',
            hint: 'The database is temporarily slow. Click to retry.'
        };
    }
    
    if (/429|rate limit/i.test(msg)) {
        return { 
            code: 'API_RATE_LIMIT', 
            message: 'Census API rate limit reached. Please retry in ~60 seconds.',
            hint: 'Too many requests to Census API. Wait a minute and try again.'
        };
    }
    
    if (/ECONNRESET|ENETUNREACH|fetch failed|timeout|network/i.test(msg)) {
        return { 
            code: 'API_NETWORK', 
            message: 'External API network or timeout issue. Please retry.',
            hint: 'Connection to Census API failed. Check internet and retry.'
        };
    }
    
    return { 
        code: 'UNKNOWN', 
        message: msg,
        hint: 'An unexpected error occurred. Check logs and retry.'
    };
}

async function warmZctaCachePage(base44, stateAbbr, status, timeLeftMs, budgetMs = WARMING_BUDGET_MS) {
    if (timeLeftMs() < 12000) {
        return { progressed: false, done: false, fetched: 0 };
    }

    const start = now();
    let cursor = status.zcta_cache_cursor || null;
    let zctas = new Set(Array.isArray(status.zcta_cache) ? status.zcta_cache : []);
    let fetched = 0;
    let progressed = false;

    console.log(`[WARM ${stateAbbr}] Starting from cursor=${cursor}, existing=${zctas.size}`);

    while ((now() - start) < budgetMs) {
        try {
            const queryFilter = { state_abbr: stateAbbr.toUpperCase() };
            
            console.log(`[WARM ${stateAbbr}] Fetching page: cursor=${cursor}, limit=${WARMING_PAGE_SIZE}`);
            
            const options = {
                fields: ['zcta5'],
                sort: 'zcta5',
                limit: WARMING_PAGE_SIZE
            };
            
            if (cursor) {
                options.after = cursor;
            }
            
            const result = await Promise.race([
                base44.asServiceRole.entities.ZctaBoundary.filter(queryFilter, options),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Warming page timeout')), WARMING_TIMEOUT_MS)
                )
            ]);

            const page = result?.data || result || [];
            const next_cursor = result?.next_cursor || null;

            console.log(`[WARM ${stateAbbr}] Page fetched: ${page.length} records, next_cursor=${next_cursor}`);

            if (!page || page.length === 0) {
                const finalZctas = Array.from(zctas).sort();
                
                console.log(`[WARM ${stateAbbr}] No more records. Finalizing ${finalZctas.length} ZCTAs...`);
                
                await base44.asServiceRole.entities.StateDataStatus.update(status.id, {
                    zcta_cache: finalZctas,
                    zcta_cache_count: finalZctas.length,
                    zcta_cache_cursor: null,
                    zcta_cache_ready: finalZctas.length > 0,
                    last_boundary_update: new Date().toISOString(),
                    last_error: null
                });

                console.log(`[WARM ${stateAbbr}] Complete! Total=${finalZctas.length} ZCTAs`);
                return { progressed, done: true, fetched };
            }

            for (const item of page) {
                if (item?.zcta5 && /^\d{5}$/.test(item.zcta5)) {
                    zctas.add(item.zcta5);
                }
            }

            fetched += page.length;
            progressed = true;

            console.log(`[WARM ${stateAbbr}] Extracted ${page.length} ZCTAs, total now=${zctas.size}`);

            const currentCachedZctas = Array.from(zctas).sort();
            await base44.asServiceRole.entities.StateDataStatus.update(status.id, {
                zcta_cache: currentCachedZctas,
                zcta_cache_count: currentCachedZctas.length,
                zcta_cache_cursor: next_cursor,
                zcta_cache_ready: false,
                last_boundary_update: new Date().toISOString(),
                last_error: `Warming ZCTA list: ${currentCachedZctas.length} collected...`
            });

            console.log(`[WARM ${stateAbbr}] Progress saved: fetched=${fetched} total=${currentCachedZctas.length}, cursor=${next_cursor}`);

            cursor = next_cursor;

            if (!next_cursor) {
                const finalZctas = Array.from(zctas).sort();
                
                console.log(`[WARM ${stateAbbr}] No next_cursor. Finalizing ${finalZctas.length} ZCTAs...`);
                
                await base44.asServiceRole.entities.StateDataStatus.update(status.id, {
                    zcta_cache: finalZctas,
                    zcta_cache_count: finalZctas.length,
                    zcta_cache_cursor: null,
                    zcta_cache_ready: finalZctas.length > 0,
                    last_boundary_update: new Date().toISOString(),
                    last_error: null
                });

                console.log(`[WARM ${stateAbbr}] Complete! Total=${finalZctas.length} ZCTAs`);
                return { progressed: true, done: true, fetched };
            }

        } catch (pageError) {
            console.error(`[WARM ${stateAbbr}] Page fetch failed:`, pageError.message);
            
            if (pageError.message?.includes('timeout')) {
                console.log(`[WARM ${stateAbbr}] Timeout but progressed=${progressed}, fetched=${fetched}`);
                
                if (progressed && fetched > 0) {
                    return { progressed: true, done: false, fetched };
                }
            }
            
            throw pageError;
        }
    }

    console.log(`[WARM ${stateAbbr}] Budget exhausted. Progressed=${progressed} fetched=${fetched}`);
    return { progressed, done: false, fetched };
}

async function fetchZctaData(zips) {
    const url = `${ZCTA_API_URL}/?zips=${zips.join(',')}`;
    
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort('timeout'), 10000);
        
        const response = await fetch(url, { 
            signal: controller.signal,
            headers: {
                'User-Agent': 'HealthScope/1.0'
            }
        });
        
        clearTimeout(timeout);
        
        if (!response.ok) {
            console.warn(`ZCTA API returned ${response.status}`);
            return { ok: false, data: [], status: response.status };
        }
        
        const data = await response.json();
        
        if (!Array.isArray(data)) {
            console.warn(`Invalid response format from ZCTA API`);
            return { ok: false, data: [] };
        }
        
        return { ok: true, data };
    } catch (error) {
        console.warn(`Failed to fetch from ZCTA API: ${error.message}`);
        return { ok: false, data: [], error: error.message };
    }
}

Deno.serve(async (req) => {
    const startTime = now();
    const timeLeft = () => MAX_RUNTIME_MS - (now() - startTime);
    
    let base44;
    let statusRecord;
    
    try {
        base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ 
                success: false, 
                code: 'UNAUTHORIZED',
                error: 'Authentication required' 
            }, { status: 401 });
        }

        const { stateAbbr, stateName } = await req.json();

        if (!stateAbbr || !stateName) {
            return Response.json({ 
                success: false,
                code: 'INVALID_INPUT',
                error: 'State abbreviation and name are required' 
            }, { status: 400 });
        }

        console.log(`[0ms] 🚀 Loading demographics for ${stateName} (${stateAbbr}) via ZCTA micro-service`);
        console.log(`[${now() - startTime}ms] ✓ Using external ZCTA API: ${ZCTA_API_URL}`);

        const statusRecords = await base44.asServiceRole.entities.StateDataStatus.filter({ 
            state_abbr: stateAbbr 
        });
        
        statusRecord = statusRecords?.[0];
        
        if (!statusRecord) {
            return Response.json({
                success: false,
                code: 'STATE_NOT_INITIALIZED',
                error: `No status record found for ${stateName}.`,
                hint: 'Initialize state data first by loading boundaries.'
            }, { status: 404 });
        }

        // SKIP boundary verification if cache is already marked as ready
        if (!statusRecord.zcta_cache_ready || !Array.isArray(statusRecord.zcta_cache) || statusRecord.zcta_cache.length === 0) {
            console.log(`[${now() - startTime}ms] ⚠️ ZCTA cache not ready – will try to warm it`);
            
            // Try warming cache directly without boundary check
            console.log(`[${now() - startTime}ms] Attempting to warm cache...`);
            
            try {
                const warm = await warmZctaCachePage(base44, stateAbbr, statusRecord, timeLeft);
                
                const statusAfter = (await base44.asServiceRole.entities.StateDataStatus.filter({ 
                    state_abbr: stateAbbr 
                }))[0];

                console.log(`[${now() - startTime}ms] Warming result: done=${warm.done} fetched=${warm.fetched} total=${statusAfter.zcta_cache_count || 0}`);

                const totalCollected = statusAfter.zcta_cache_count || 0;
                
                if (warm.done && totalCollected === 0) {
                    // Warming completed but got zero ZCTAs - boundaries not loaded
                    return Response.json({
                        success: false,
                        code: 'BOUNDARIES_NOT_READY',
                        error: `No ZCTA boundaries found for ${stateName}.`,
                        message: `Load boundaries first: Go to Admin → Data Loader → Find ${stateName} → Click "Load Boundaries"`,
                        hint: 'Boundaries must be loaded before demographics can be fetched.',
                        solution: `1. Navigate to Admin → Data Loader\n2. Find "${stateName}" in the table\n3. Click "Load Boundaries" button\n4. Wait for green checkmark (~60 seconds)\n5. Return and try loading demographics again`,
                        details: {
                            state: stateName,
                            state_abbr: stateAbbr
                        }
                    }, { status: 409 });
                }
                
                return Response.json({
                    success: false,
                    code: 'BOUNDARIES_WARMING',
                    message: warm.done
                        ? `✓ ZCTA cache ready: ${totalCollected} ZCTAs. Click to start loading population.`
                        : `Warming ZCTA list... ${totalCollected} ZCTAs collected. ${warm.fetched} added this run.`,
                    warmedThisRun: warm.fetched,
                    totalCollected: totalCollected,
                    isReady: !!statusAfter.zcta_cache_ready,
                    retryAfterSec: warm.done ? 1 : 3,
                    hint: warm.done 
                        ? 'Cache is ready. Click "Load" again to start loading population data.'
                        : 'Building ZCTA list from boundaries. This may take a few tries without database indexes.'
                }, { status: 202 });

            } catch (warmError) {
                console.error(`[${now() - startTime}ms] ❌ Cache warming failed:`, warmError);
                
                if (warmError.message?.includes('timeout')) {
                    await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
                        last_error: `Database timeout during cache warming. Reload boundaries to rebuild cache.`,
                        loading_status: 'idle'
                    });
                    
                    return Response.json({
                        success: false,
                        code: 'DB_TIMEOUT_WARMING',
                        error: `Database timeout while warming ZCTA cache for ${stateName}.`,
                        hint: 'Reload boundaries to rebuild the cache (faster workaround).',
                        solution: `Click "Reload" in the Boundaries column for ${stateName}. This will rebuild the ZCTA cache and skip the slow database query.`,
                        retryAfterSec: 60
                    }, { status: 502 });
                }
                
                const warmClientError = toClientError(warmError);
                
                await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
                    last_error: `${warmClientError.code}: ${warmClientError.message}`,
                    loading_status: 'idle'
                });
                
                return Response.json({
                    success: false,
                    code: warmClientError.code,
                    error: `Cache warming failed: ${warmClientError.message}`,
                    hint: warmClientError.hint
                }, { status: 502 });
            }
        }

        const zctas = statusRecord.zcta_cache;
        const total = statusRecord.zcta_cache_count || zctas.length;
        
        console.log(`[${now() - startTime}ms] ✓ Using cached ZCTA list: ${zctas.length} ZCTAs`);

        let cursor = Number(statusRecord.demographics_cursor || 0);
        
        console.log(`[${now() - startTime}ms] Loading demographics at cursor ${cursor}/${total}`);

        const phaseStartTime = now();
        let processedThisRun = 0;
        let totalApiTime = 0;
        let totalDbTime = 0;
        let hadProgress = false;

        while (timeLeft() > SAFETY_BUFFER_MS && cursor < total) {
            const batchStart = now();
            
            const slice = zctas.slice(cursor, Math.min(cursor + API_CHUNK_SIZE, total));
            
            console.log(`[${now() - startTime}ms] Fetching ${slice.length} ZCTAs from API...`);

            const apiStart = now();
            const result = await fetchZctaData(slice);
            const apiMs = now() - apiStart;
            totalApiTime += apiMs;

            if (!result.ok || result.data.length === 0) {
                console.warn(`[${now() - startTime}ms] ⚠️ API failed or returned no data`);
                if (result.error) {
                    throw new Error(`ZCTA API error: ${result.error}`);
                }
                // Skip this batch but don't fail completely
                cursor += slice.length;
                continue;
            }

            console.log(`[${now() - startTime}ms] Received ${result.data.length} records from API`);

            const toUpsert = [];
            for (const row of result.data) {
                if (!row.zip) continue;
                
                const record = {
                    zcta5: row.zip,
                    acs_year: '2022',
                    state_abbr: row.state_abbr?.toUpperCase() || stateAbbr.toUpperCase(),
                    source: 'ZCTA Micro-service API',
                    source_version: 'api_v1',
                    population: row.pop_total || null,
                    median_income: row.median_hh_income || null,
                    poverty_universe: row.poverty_universe || null,
                    poverty_below: row.poverty_below || null,
                    uninsured_count: null,
                    insured_total: null
                };
                
                toUpsert.push(record);
            }

            const dbStart = now();
            if (toUpsert.length > 0) {
                hadProgress = true;
                try {
                    await base44.asServiceRole.entities.ZipDemographics.bulkCreate(toUpsert);
                } catch (bulkError) {
                    console.warn(`Bulk insert failed, trying individual inserts...`);
                    for (const record of toUpsert) {
                        try {
                            await base44.asServiceRole.entities.ZipDemographics.create(record);
                        } catch (individualError) {
                            if (!individualError.message?.includes('unique') && 
                                !individualError.message?.includes('duplicate')) {
                                console.warn(`Failed to insert ${record.zcta5}: ${individualError.message}`);
                            }
                        }
                    }
                }
            }
            
            const dbMs = now() - dbStart;
            totalDbTime += dbMs;

            processedThisRun += slice.length;
            cursor += slice.length;

            const batchMs = now() - batchStart;
            const etaSec = estimateEta(cursor, total, phaseStartTime);

            emitProgress(stateAbbr, cursor, total, slice.length, batchMs, apiMs, dbMs, etaSec);

            await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
                demographics_cursor: cursor,
                last_demographics_update: new Date().toISOString(),
                loading_status: 'loading_demographics',
                last_error: `Loading population: ${cursor}/${total} ZCTAs (${Math.round(cursor/total*100)}%)`
            });

            if (timeLeft() <= SAFETY_BUFFER_MS) {
                console.log(`[${now() - startTime}ms] ⏱️ Time limit approaching, stopping at ${cursor}/${total}`);
                break;
            }
        }

        if (!hadProgress && processedThisRun === 0) {
            console.error(`[${now() - startTime}ms] ❌ NO_PROGRESS: No data written`);
            
            await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
                loading_status: 'idle',
                last_error: 'NO_PROGRESS: No data written. Likely DB timeout or rate limit.'
            });
            
            return Response.json({
                success: false,
                code: 'NO_PROGRESS',
                error: 'No progress this cycle. Likely DB timeout or rate limit.',
                hint: 'System may be overloaded. Wait 15 seconds and try again.',
                retryAfterSec: 15
            }, { status: 502 });
        }

        const allComplete = cursor >= total;

        if (allComplete) {
            console.log(`[${now() - startTime}ms] 🎉 DEMOGRAPHICS COMPLETE!`);
            
            await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
                demographics_cursor: 0,
                demographics_complete: true,
                demographics_count: total,
                loading_status: 'idle',
                last_demographics_update: new Date().toISOString(),
                last_error: null
            });
        }

        const avgApiLatency = processedThisRun > 0 ? Math.round(totalApiTime / Math.ceil(processedThisRun / API_CHUNK_SIZE)) : 0;
        const avgDbLatency = processedThisRun > 0 ? Math.round(totalDbTime / Math.ceil(processedThisRun / UPSERT_BATCH)) : 0;

        const message = allComplete
            ? `✅ Complete! Demographics loaded for ${stateName} (${total} ZCTAs) from ZCTA API.`
            : `Loading demographics: ${cursor}/${total} ZCTAs (${Math.round(cursor/total*100)}%). Click to continue.`;

        return Response.json({
            success: true,
            message: message,
            dataType: 'Demographics',
            processed: cursor,
            total: total,
            processedThisRun: processedThisRun,
            progress: Math.round((cursor / total) * 100),
            isComplete: allComplete,
            willContinue: !allComplete,
            metrics: {
                timeMs: now() - startTime,
                avgApiLatencyMs: avgApiLatency,
                avgDbLatencyMs: avgDbLatency
            }
        }, { 
            status: allComplete ? 200 : 202 
        });

    } catch (error) {
        const elapsed = now() - startTime;
        console.error(`[${elapsed}ms] 💥 ERROR:`, error);
        console.error('Stack:', error.stack);
        
        const clientError = toClientError(error);
        
        if (base44 && statusRecord?.id) {
            try {
                await base44.asServiceRole.entities.StateDataStatus.update(
                    statusRecord.id,
                    {
                        loading_status: 'idle',
                        last_error: `${clientError.code}: ${clientError.message}`,
                        last_demographics_update: new Date().toISOString()
                    }
                );
            } catch (updateError) {
                console.warn('Failed to update status on error:', updateError.message);
            }
        }
        
        const statusCode = ['DB_TIMEOUT', 'API_RATE_LIMIT', 'API_NETWORK'].includes(clientError.code) ? 502 : 500;
        
        return Response.json({ 
            success: false, 
            code: clientError.code,
            error: clientError.message,
            hint: clientError.hint,
            processingTimeMs: elapsed
        }, { status: statusCode });
    }
});