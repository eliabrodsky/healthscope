import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const PLACES_ENDPOINT = "https://data.cdc.gov/resource/qnzd-25i4.json";
const ZCTAS_PER_BATCH = 10;
const REQUEST_TIMEOUT_MS = 20000;

function buildPlacesUrl(zctas, measures) {
  const whereClauses = [
    `locationname IN (${zctas.map(z => `'${z}'`).join(",")})`
  ];
  
  if (measures && measures.length > 0) {
    whereClauses.push(`measureid IN (${measures.map(m => `'${m}'`).join(",")})`);
  }

  const params = new URLSearchParams({
    $where: whereClauses.join(" AND "),
    $limit: "50000"
  });

  return `${PLACES_ENDPOINT}?${params.toString()}`;
}

async function fetchWithTimeout(url, timeoutMs = REQUEST_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  
  try {
    const response = await fetch(url, {
      headers: { 'Accept': 'application/json' },
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const text = await response.text();
      console.error(`PLACES API error (${response.status}):`, text);
      throw new Error(`PLACES API returned ${response.status}: ${text.substring(0, 200)}`);
    }
    
    return await response.json();
  } catch (error) {
    clearTimeout(timeoutId);
    throw error;
  }
}

Deno.serve(async (req) => {
  try {
    if (req.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    let body;
    try {
      body = await req.json();
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    const { job_id } = body;

    if (!job_id) {
      return Response.json({ error: 'Missing job_id' }, { status: 400 });
    }

    // Get sync job
    const syncJob = await base44.asServiceRole.entities.CdcSyncJob.get(job_id);
    
    if (!syncJob) {
      return Response.json({ error: 'Job not found' }, { status: 404 });
    }

    if (syncJob.status === 'complete') {
      return Response.json({ 
        success: true, 
        message: 'Job already complete',
        progress: 100,
        is_complete: true
      });
    }

    // Get ZCTAs from sync job (stored during creation)
    const allZctas = syncJob.zctas || [];
    
    console.log(`Processing job ${job_id} with ${allZctas.length} ZCTAs`);
    
    if (allZctas.length === 0) {
      console.error('No ZCTAs found in sync job');
      await base44.asServiceRole.entities.CdcSyncJob.update(job_id, {
        status: 'failed',
        error_message: 'No ZCTAs found in sync job'
      });
      
      return Response.json({
        success: false,
        error: 'No ZCTAs stored in sync job',
        progress: 0,
        is_complete: false
      }, { status: 400 });
    }
    
    const currentBatch = syncJob.current_batch || 0;
    const startIdx = currentBatch * ZCTAS_PER_BATCH;
    const batchZctas = allZctas.slice(startIdx, startIdx + ZCTAS_PER_BATCH);

    console.log(`Processing batch ${currentBatch}: ZCTAs ${startIdx} to ${startIdx + ZCTAS_PER_BATCH} (${batchZctas.length} ZCTAs)`);

    if (batchZctas.length === 0) {
      // Mark job as complete
      await base44.asServiceRole.entities.CdcSyncJob.update(job_id, {
        status: 'complete',
        zctas_completed: syncJob.total_zctas
      });
      
      return Response.json({
        success: true,
        message: 'All chunks processed',
        progress: 100,
        zctas_completed: syncJob.total_zctas,
        is_complete: true
      });
    }

    // Update job status
    await base44.asServiceRole.entities.CdcSyncJob.update(job_id, {
      status: 'in_progress',
      last_chunk_processed: new Date().toISOString()
    });

    // Fetch CDC data for this batch
    const indicators = syncJob.selected_indicators || [];
    const url = buildPlacesUrl(batchZctas, indicators);
    
    console.log("PLACES URL:", url);
    console.log("Fetching for ZCTAs:", batchZctas);
    console.log("Indicators:", indicators);

    let recordsAdded = 0;
    
    try {
      const data = await fetchWithTimeout(url);
      
      console.log(`Received ${data.length} records from PLACES API`);
      if (data.length > 0) {
        console.log("Sample record:", JSON.stringify(data[0], null, 2));
      }
      
      // Bulk insert records
      if (data && data.length > 0) {
        const recordsToInsert = data.map(record => ({
          zcta5: record.locationname,
          state_abbr: record.stateabbr || record.statedesc || '',
          year: record.year?.toString() || '2024',
          measure_id: record.measureid,
          measure: record.measure || '',
          category: record.category || '',
          data_value: parseFloat(record.data_value) || 0,
          source: 'CDC PLACES 2024 Release',
          source_version: '2024-release-1'
        }));
        
        console.log(`Prepared ${recordsToInsert.length} records for insertion`);
        
        // Insert in batches of 50 to avoid overwhelming the database
        for (let i = 0; i < recordsToInsert.length; i += 50) {
          const chunk = recordsToInsert.slice(i, i + 50);
          await base44.asServiceRole.entities.CdcHealthData.bulkCreate(chunk);
          recordsAdded += chunk.length;
          console.log(`Inserted batch ${Math.floor(i/50) + 1}, total records so far: ${recordsAdded}`);
        }
      } else {
        console.warn('No data returned from PLACES API');
      }
    } catch (error) {
      console.error('Error in CDC data processing:', {
        message: error.message,
        stack: error.stack,
        name: error.name
      });
      
      // Increment failed attempts
      await base44.asServiceRole.entities.CdcSyncJob.update(job_id, {
        failed_attempts: (syncJob.failed_attempts || 0) + 1,
        error_message: error.message
      });
      
      throw error;
    }

    // Update progress
    const newZctasCompleted = Math.min(
      syncJob.zctas_completed + batchZctas.length,
      syncJob.total_zctas
    );
    const progress = Math.round((newZctasCompleted / syncJob.total_zctas) * 100);
    const isComplete = newZctasCompleted >= syncJob.total_zctas;

    await base44.asServiceRole.entities.CdcSyncJob.update(job_id, {
      current_batch: currentBatch + 1,
      zctas_completed: newZctasCompleted,
      status: isComplete ? 'complete' : 'in_progress',
      error_message: null
    });

    return Response.json({
      success: true,
      records_added: recordsAdded,
      progress,
      zctas_completed: newZctasCompleted,
      total_zctas: syncJob.total_zctas,
      is_complete: isComplete
    });

  } catch (error) {
    console.error('Error processing CDC chunk:', error);
    
    return Response.json({ 
      error: error.message || 'Failed to process chunk'
    }, { status: 500 });
  }
});