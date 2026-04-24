import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

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

    const { assessment_id, indicators, zctas: providedZctas } = body;

    console.log('startCdcSync called with:', { assessment_id, indicators, zctas_count: providedZctas?.length });

    if (!assessment_id) {
      return Response.json({ error: 'Missing assessment_id' }, { status: 400 });
    }

    if (!indicators || !Array.isArray(indicators) || indicators.length === 0) {
      return Response.json({ error: 'Missing or invalid indicators array' }, { status: 400 });
    }

    // Use ZCTAs provided from frontend, or try to extract from assessment
    let rawZctas = providedZctas;
    
    if (!rawZctas || rawZctas.length === 0) {
      // Fallback: try to get from assessment
      let assessment;
      try {
        assessment = await base44.asServiceRole.entities.Assessment.get(assessment_id);
      } catch (err) {
        console.error('Failed to fetch assessment:', err);
        return Response.json({ error: `Failed to fetch assessment: ${err.message}` }, { status: 400 });
      }

      if (!assessment) {
        return Response.json({ error: 'Assessment not found' }, { status: 404 });
      }

      rawZctas = assessment?.processed_data?.zcta_codes || 
                 assessment?.geography?.zcta_codes || 
                 assessment?.processed_data?.zip_codes || 
                 assessment?.geography?.zip_codes ||
                 (assessment?.processed_data?.census_data ? Object.keys(assessment.processed_data.census_data) : []) ||
                 [];
    }

    // Normalize and dedupe ZCTAs
    const zctas = Array.from(new Set(rawZctas.map(z => z.toString().padStart(5, "0"))));

    console.log(`Found ${zctas.length} unique ZCTAs`);
    console.log('Normalized ZCTAs:', zctas.slice(0, 5));

    if (zctas.length === 0) {
      return Response.json({ 
        error: 'No ZCTAs provided. Please ensure the assessment has geographic data loaded.'
      }, { status: 400 });
    }

    const ZCTAS_PER_BATCH = 10;
    const totalBatches = Math.ceil(zctas.length / ZCTAS_PER_BATCH);

    // Check if sync job already exists
    const existingJobs = await base44.asServiceRole.entities.CdcSyncJob.filter({ 
      assessment_id 
    });

    let syncJob;
    if (existingJobs && existingJobs.length > 0) {
      // Update existing job
      syncJob = existingJobs[0];
      await base44.asServiceRole.entities.CdcSyncJob.update(syncJob.id, {
        status: 'not_started',
        selected_indicators: indicators,
        zctas: zctas,
        total_zctas: zctas.length,
        total_indicators: indicators.length,
        total_batches: totalBatches,
        zctas_completed: 0,
        indicators_completed: 0,
        current_batch: 0,
        failed_attempts: 0
      });
    } else {
      // Create new sync job
      syncJob = await base44.asServiceRole.entities.CdcSyncJob.create({
        assessment_id,
        selected_indicators: indicators,
        zctas: zctas,
        total_zctas: zctas.length,
        total_indicators: indicators.length,
        total_batches: totalBatches,
        status: 'not_started'
      });
    }

    return Response.json({
      success: true,
      job_id: syncJob.id,
      total_zctas: zctas.length,
      total_batches: totalBatches,
      indicators: indicators.length
    });

  } catch (error) {
    console.error('Error starting CDC sync:', error);
    return Response.json({ 
      error: error.message || 'Failed to start CDC sync' 
    }, { status: 500 });
  }
});