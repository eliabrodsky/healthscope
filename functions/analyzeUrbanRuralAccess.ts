import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { zip_codes } = await req.json();
    
    if (!zip_codes || !Array.isArray(zip_codes)) {
      return Response.json({ error: 'ZIP codes array required' }, { status: 400 });
    }

    // Fetch demographic data
    const demographics = await base44.asServiceRole.entities.ZipDemographics.filter({
      zcta5: { $in: zip_codes }
    });

    // Fetch organizations
    const organizations = await base44.asServiceRole.entities.Organization.filter({
      zip_code: { $in: zip_codes }
    });

    // Calculate urban/rural classification based on population density
    const analysis = zip_codes.map(zip => {
      const demo = demographics.find(d => d.zcta5 === zip);
      const population = demo?.population || 0;
      const orgs = organizations.filter(o => o.zip_code === zip);
      
      // Simple urban/rural classification (can be enhanced with actual RUCA codes)
      let classification = 'Unknown';
      if (population > 50000) classification = 'Urban';
      else if (population > 10000) classification = 'Suburban';
      else if (population > 2500) classification = 'Rural';
      else classification = 'Frontier';

      const providersPerCapita = population > 0 ? (orgs.length / population) * 10000 : 0;

      return {
        zip_code: zip,
        population,
        classification,
        provider_count: orgs.length,
        providers_per_10k: parseFloat(providersPerCapita.toFixed(2)),
        has_fqhc: orgs.some(o => o.type === 'fqhc'),
        has_hospital: orgs.some(o => o.type === 'hospital'),
        access_level: providersPerCapita > 5 ? 'Good' : providersPerCapita > 2 ? 'Moderate' : 'Low'
      };
    });

    const summary = {
      total_zips: zip_codes.length,
      urban: analysis.filter(a => a.classification === 'Urban').length,
      suburban: analysis.filter(a => a.classification === 'Suburban').length,
      rural: analysis.filter(a => a.classification === 'Rural').length,
      frontier: analysis.filter(a => a.classification === 'Frontier').length,
      avg_providers_per_10k: parseFloat((analysis.reduce((sum, a) => sum + a.providers_per_10k, 0) / analysis.length).toFixed(2)),
      low_access_areas: analysis.filter(a => a.access_level === 'Low').length
    };

    return Response.json({
      success: true,
      analysis,
      summary
    });

  } catch (error) {
    console.error('Error in analyzeUrbanRuralAccess:', error);
    return Response.json({ 
      success: false,
      error: error.message 
    }, { status: 500 });
  }
});