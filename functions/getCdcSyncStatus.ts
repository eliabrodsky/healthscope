import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const url = new URL(req.url);
    const assessmentId = url.searchParams.get('assessment_id');

    if (!assessmentId) {
      return Response.json({ error: 'Missing assessment_id' }, { status: 400 });
    }

    // Find sync job for this assessment
    const jobs = await base44.entities.CdcSyncJob.filter({ 
      assessment_id: assessmentId 
    });

    if (!jobs || jobs.length === 0) {
      return Response.json({
        exists: false,
        status: 'not_started'
      });
    }

    const job = jobs[0];
    const progress = job.total_zctas > 0 
      ? Math.round((job.zctas_completed / job.total_zctas) * 100)
      : 0;

    // Get count of loaded CDC data
    const cdcRecords = await base44.entities.CdcHealthData.filter({
      zcta5: { '$in': [] } // Just count all for this assessment's ZCTAs
    });

    return Response.json({
      exists: true,
      job_id: job.id,
      status: job.status,
      progress,
      zctas_completed: job.zctas_completed,
      total_zctas: job.total_zctas,
      indicators: job.selected_indicators,
      total_records: cdcRecords?.length || 0,
      last_update: job.last_chunk_processed,
      error: job.error_message
    });

  } catch (error) {
    console.error('Error getting CDC sync status:', error);
    return Response.json({ 
      error: error.message || 'Failed to get sync status' 
    }, { status: 500 });
  }
});