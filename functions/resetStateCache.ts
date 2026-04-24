import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

/**
 * ADMIN UTILITY: Reset corrupted ZCTA cache for a state
 * 
 * Use this to clean up states with duplicate ZCTAs in their cache
 * or to force a cache rebuild.
 * 
 * This will:
 * - Clear the zcta_cache array
 * - Reset zcta_cache_cursor to null
 * - Set zcta_cache_ready to false
 * - Clear any error messages
 * 
 * The next demographics load will rebuild the cache from scratch.
 */

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user || user.role !== 'admin') {
            return Response.json({ 
                success: false,
                error: 'Admin access required' 
            }, { status: 403 });
        }

        const { stateAbbr, stateName } = await req.json();

        if (!stateAbbr) {
            return Response.json({ 
                success: false,
                error: 'State abbreviation is required' 
            }, { status: 400 });
        }

        console.log(`[RESET] Resetting cache for ${stateName || stateAbbr} (${stateAbbr})`);

        // Find the state status record
        const statusRecords = await base44.asServiceRole.entities.StateDataStatus.filter({ 
            state_abbr: stateAbbr.toUpperCase() 
        });
        
        const statusRecord = statusRecords?.[0];
        
        if (!statusRecord) {
            return Response.json({
                success: false,
                error: `No status record found for ${stateAbbr}`
            }, { status: 404 });
        }

        const oldCacheSize = statusRecord.zcta_cache?.length || 0;
        const hasDuplicates = oldCacheSize > 0 && 
                             oldCacheSize !== new Set(statusRecord.zcta_cache).size;

        // Reset the cache
        await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
            zcta_cache: [],
            zcta_cache_count: 0,
            zcta_cache_cursor: null,
            zcta_cache_ready: false,
            demographics_cursor: 0,
            demographics_phase: null,
            loading_status: 'idle',
            last_error: null,
            last_demographics_update: new Date().toISOString()
        });

        console.log(`[RESET] ✓ Cache reset for ${stateAbbr}`);
        console.log(`[RESET] Old cache size: ${oldCacheSize} (duplicates: ${hasDuplicates ? 'yes' : 'no'})`);

        return Response.json({
            success: true,
            message: `✓ Cache reset for ${stateName || stateAbbr}. Ready for fresh rebuild.`,
            details: {
                state: stateAbbr,
                old_cache_size: oldCacheSize,
                had_duplicates: hasDuplicates,
                boundaries_complete: statusRecord.boundaries_complete,
                boundary_count: statusRecord.boundary_count
            }
        });

    } catch (error) {
        console.error('Error resetting cache:', error);
        
        return Response.json({ 
            success: false, 
            error: error.message || 'Unknown error'
        }, { status: 500 });
    }
});