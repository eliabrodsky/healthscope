import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

/**
 * Load demographics from external ZCTA micro-service API
 * Uses: https://getzctas-1038555279570.us-south1.run.app/?zips={comma-separated-zips}
 * 
 * This is used by the CreateAssessmentWizard for loading demographics in Step 3/4
 */

const ZCTA_API_URL = 'https://getzctas-1038555279570.us-south1.run.app';
const API_CHUNK_SIZE = 150; // Increased from 100 to process more per call
const TIMEOUT_MS = 50000; // 50 second timeout

async function fetchZctaDataFromAPI(zips) {
  const url = `${ZCTA_API_URL}/?zips=${zips.join(',')}`;
  
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    
    const response = await fetch(url, { 
      signal: controller.signal,
      headers: {
        'User-Agent': 'HealthScope/1.0'
      }
    });
    
    clearTimeout(timeout);
    
    if (!response.ok) {
      console.warn(`ZCTA API returned ${response.status}`);
      return { ok: false, data: [] };
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
  const startTime = Date.now();
  
  try {
    const base44 = createClientFromRequest(req);
    
    const user = await base44.auth.me();
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { zctas, state_abbr } = await req.json();

    if (!zctas || !Array.isArray(zctas) || zctas.length === 0) {
      return Response.json({ 
        success: false, 
        error: 'No ZCTAs provided' 
      }, { status: 400 });
    }

    console.log(`Loading demographics for ${zctas.length} ZCTAs via ZCTA API`);

    const allResults = [];
    let processed = 0;
    let apiSuccessCount = 0;

    // Process in chunks
    for (let i = 0; i < zctas.length; i += API_CHUNK_SIZE) {
      // Check timeout
      if (Date.now() - startTime > TIMEOUT_MS) {
        console.log(`Timeout approaching after ${Date.now() - startTime}ms at ${i}/${zctas.length}`);
        break;
      }
      
      const chunk = zctas.slice(i, i + API_CHUNK_SIZE);
      
      console.log(`Fetching chunk ${Math.floor(i / API_CHUNK_SIZE) + 1}: ${chunk.length} ZCTAs`);
      
      const apiResult = await fetchZctaDataFromAPI(chunk);
      
      if (!apiResult.ok || apiResult.data.length === 0) {
        console.warn(`API call failed for chunk starting at ${i}`);
        continue;
      }

      console.log(`Received ${apiResult.data.length} records from API`);
      apiSuccessCount += apiResult.data.length;

      // Transform and prepare batch insert
      const recordsToInsert = [];

      for (const row of apiResult.data) {
        if (!row.zip) continue;

        const censusData = {
          zcta5: row.zip,
          acs_year: '2022',
          state_abbr: row.state_abbr?.toUpperCase() || state_abbr?.toUpperCase() || 'XX',
          source: 'ZCTA Micro-service API',
          source_version: 'api_v1',
          population: row.pop_total || null,
          households: row.housing_units_total || null,
          median_income: row.median_hh_income || null,
          poverty_count: row.poverty_below || null,
          poverty_total: row.poverty_universe || null,
          poverty_rate: (row.poverty_below != null && row.poverty_universe != null && row.poverty_universe > 0) 
            ? Math.round((row.poverty_below / row.poverty_universe * 100) * 10) / 10 
            : null,
          uninsured_count: row.pop_total && row.insurance_universe ? (row.pop_total - row.insurance_universe) : null,
          insured_total: row.insurance_universe || null,
          uninsured_rate: (row.pop_total != null && row.insurance_universe != null && row.pop_total > 0) 
            ? Math.round(((row.pop_total - row.insurance_universe) / row.pop_total * 100) * 10) / 10 
            : null
        };

        recordsToInsert.push(censusData);
      }

      // Bulk insert directly without checking for existing
      if (recordsToInsert.length > 0) {
        try {
          await base44.asServiceRole.entities.ZipDemographics.bulkCreate(recordsToInsert);
          allResults.push(...recordsToInsert);
          processed += recordsToInsert.length;
        } catch (bulkError) {
          // If bulk fails due to duplicates, that's fine - data is already there
          if (bulkError.message?.includes('unique') || bulkError.message?.includes('duplicate')) {
            console.log(`Skipping ${recordsToInsert.length} existing records`);
            processed += recordsToInsert.length;
          } else {
            console.warn('Bulk create error:', bulkError.message);
          }
        }
      }
    }
    
    const elapsed = Date.now() - startTime;
    console.log(`Completed in ${elapsed}ms: ${processed}/${zctas.length} ZCTAs (${apiSuccessCount} from API)`);
    
    return Response.json({
      success: true,
      processed: processed,
      total: zctas.length,
      api_success: apiSuccessCount,
      message: `Loaded demographics for ${processed}/${zctas.length} ZCTAs from ZCTA API`,
      results: allResults,
      processingTimeMs: elapsed
    });

  } catch (error) {
    console.error('Error in loadZctaDemographics:', error);
    return Response.json({ 
      success: false,
      error: error.message 
    }, { status: 500 });
  }
});