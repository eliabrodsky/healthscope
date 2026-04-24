import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    
    const user = await base44.auth.me();
    if (!user || user.role !== 'admin') {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { state_abbr, dry_run = true } = await req.json();

    // Fetch all ZipDemographics records
    const filter = state_abbr ? { state_abbr } : {};
    const allRecords = await base44.asServiceRole.entities.ZipDemographics.filter(
      filter,
      null,
      50000
    );

    // Group by composite key: zcta5 + acs_year + state_abbr
    const groups = new Map();
    allRecords.forEach(record => {
      const key = `${record.zcta5}_${record.acs_year}_${record.state_abbr}`;
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(record);
    });

    // Find duplicates
    const duplicateGroups = Array.from(groups.entries())
      .filter(([key, records]) => records.length > 1);

    const recordsToDelete = [];
    const recordsToKeep = [];

    duplicateGroups.forEach(([key, records]) => {
      // Sort by preference: 1) Census source, 2) Most recent updated_date
      records.sort((a, b) => {
        const aIsCensus = a.source?.includes('Census') ? 1 : 0;
        const bIsCensus = b.source?.includes('Census') ? 1 : 0;
        
        if (aIsCensus !== bIsCensus) {
          return bIsCensus - aIsCensus;
        }
        
        const aDate = new Date(a.updated_date || a.created_date);
        const bDate = new Date(b.updated_date || b.created_date);
        return bDate - aDate;
      });

      // Keep the first (best) record, delete the rest
      recordsToKeep.push(records[0]);
      recordsToDelete.push(...records.slice(1));
    });

    let deletedCount = 0;
    if (!dry_run && recordsToDelete.length > 0) {
      // Delete in batches to avoid timeout
      const batchSize = 100;
      for (let i = 0; i < recordsToDelete.length; i += batchSize) {
        const batch = recordsToDelete.slice(i, i + batchSize);
        for (const record of batch) {
          await base44.asServiceRole.entities.ZipDemographics.delete(record.id);
          deletedCount++;
        }
      }
    }

    return Response.json({
      success: true,
      dry_run,
      total_records: allRecords.length,
      duplicate_groups: duplicateGroups.length,
      records_to_delete: recordsToDelete.length,
      records_deleted: deletedCount,
      sample_duplicates: duplicateGroups.slice(0, 5).map(([key, records]) => ({
        key,
        count: records.length,
        sources: records.map(r => r.source)
      }))
    });

  } catch (error) {
    console.error('Error in cleanupDuplicateZipDemographics:', error);
    return Response.json({ 
      success: false,
      error: error.message 
    }, { status: 500 });
  }
});