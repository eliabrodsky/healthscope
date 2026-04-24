import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { state_abbr } = await req.json();
    
    if (!state_abbr) {
      return Response.json({ 
        error: 'State abbreviation is required' 
      }, { status: 400 });
    }

    console.log(`Computing summary for state: ${state_abbr}`);

    // Fetch all relevant data for the state
    const [boundaries, demographics, facilities, places] = await Promise.all([
      base44.asServiceRole.entities.ZctaBoundary.filter({ state_abbr }),
      base44.asServiceRole.entities.ZipDemographics.filter({ state_abbr }),
      base44.asServiceRole.entities.Organization.filter({ state: state_abbr }),
      base44.asServiceRole.entities.ZctaPlaceRelationship.filter({ state_abbr })
    ]);

    // Calculate summary statistics
    const totalPopulation = demographics.reduce((sum, d) => sum + (d.population || 0), 0);
    const totalZctas = boundaries.length;
    const totalFacilities = facilities.length;

    // Facilities by type
    const facilitiesByType = facilities.reduce((acc, f) => {
      acc[f.type] = (acc[f.type] || 0) + 1;
      return acc;
    }, {});

    // Count unique cities (excluding counties/parishes)
    const uniqueCities = new Set();
    places.forEach(p => {
      const isCountyOrParish = /(parish|county)$/i.test(p.place_name);
      if (!isCountyOrParish) {
        uniqueCities.add(p.place_geoid);
      }
    });

    // Calculate averages
    const validIncomes = demographics.filter(d => d.median_income > 0);
    const avgMedianIncome = validIncomes.length > 0
      ? validIncomes.reduce((sum, d) => sum + d.median_income, 0) / validIncomes.length
      : 0;

    const validPoverty = demographics.filter(d => d.poverty_rate != null);
    const avgPovertyRate = validPoverty.length > 0
      ? validPoverty.reduce((sum, d) => sum + d.poverty_rate, 0) / validPoverty.length
      : 0;

    const validUninsured = demographics.filter(d => d.uninsured_rate != null);
    const avgUninsuredRate = validUninsured.length > 0
      ? validUninsured.reduce((sum, d) => sum + d.uninsured_rate, 0) / validUninsured.length
      : 0;

    // Get state name from the State entity
    const states = await base44.asServiceRole.entities.State.filter({ abbreviation: state_abbr });
    const stateName = states.length > 0 ? states[0].name : state_abbr;

    const summaryData = {
      state_abbr,
      state_name: stateName,
      total_population: Math.round(totalPopulation),
      total_zctas: totalZctas,
      total_facilities: totalFacilities,
      facilities_by_type: facilitiesByType,
      total_cities: uniqueCities.size,
      avg_median_income: Math.round(avgMedianIncome),
      avg_poverty_rate: Math.round(avgPovertyRate * 100) / 100,
      avg_uninsured_rate: Math.round(avgUninsuredRate * 100) / 100,
      last_updated: new Date().toISOString()
    };

    // Check if summary exists and update, or create new
    const existing = await base44.asServiceRole.entities.StateSummary.filter({ state_abbr });
    
    if (existing.length > 0) {
      await base44.asServiceRole.entities.StateSummary.update(existing[0].id, summaryData);
      console.log(`Updated summary for ${state_abbr}`);
    } else {
      await base44.asServiceRole.entities.StateSummary.create(summaryData);
      console.log(`Created summary for ${state_abbr}`);
    }

    return Response.json({
      success: true,
      summary: summaryData
    });

  } catch (error) {
    console.error('Error computing state summary:', error);
    return Response.json({ 
      error: error.message 
    }, { status: 500 });
  }
});