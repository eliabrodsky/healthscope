import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { assessment_id, cache_types } = await req.json();
    
    if (!assessment_id) {
      return Response.json({ 
        error: 'Assessment ID is required' 
      }, { status: 400 });
    }

    // Default to all cache types if not specified
    const typesToCache = cache_types || ['boundaries', 'facilities', 'cities', 'demographics'];

    console.log(`Caching data for assessment ${assessment_id}: ${typesToCache.join(', ')}`);

    // Fetch the assessment
    const assessments = await base44.asServiceRole.entities.Assessment.filter({ id: assessment_id });
    if (assessments.length === 0) {
      return Response.json({ error: 'Assessment not found' }, { status: 404 });
    }

    const assessment = assessments[0];
    const zctas = assessment.geography?.zcta_codes || assessment.geography?.zip_codes || [];

    if (zctas.length === 0) {
      return Response.json({ 
        error: 'No ZCTAs found in assessment geography' 
      }, { status: 400 });
    }

    const results = [];

    // Cache boundaries (simplified GeoJSON)
    if (typesToCache.includes('boundaries')) {
      const boundaries = await base44.asServiceRole.entities.ZctaBoundary.filter({
        zcta5: { '$in': zctas }
      });

      // Simplify geometries for faster loading (keep only essential data)
      const simplifiedBoundaries = boundaries.map(b => ({
        zcta5: b.zcta5,
        geometry: b.geometry, // In production, you might want to simplify this further
        state_abbr: b.state_abbr
      }));

      await base44.asServiceRole.entities.AssessmentCache.create({
        assessment_id,
        cache_type: 'boundaries',
        data: { boundaries: simplifiedBoundaries },
        zcta_count: boundaries.length,
        last_updated: new Date().toISOString()
      });

      results.push({ type: 'boundaries', count: boundaries.length });
    }

    // Cache facilities
    if (typesToCache.includes('facilities')) {
      const state = assessment.geography?.state;
      let facilities = [];

      if (state) {
        // Get all facilities in the state, then filter by proximity or ZIP
        facilities = await base44.asServiceRole.entities.Organization.filter({ state });
        
        // Filter to only facilities in the assessment ZCTAs or nearby
        facilities = facilities.filter(f => 
          f.zip_code && zctas.includes(f.zip_code.substring(0, 5))
        );
      }

      // Store essential facility data
      const facilityCache = facilities.map(f => ({
        id: f.id,
        name: f.name,
        type: f.type,
        city: f.city,
        state: f.state,
        zip_code: f.zip_code,
        coordinates: f.coordinates,
        patient_volume: f.patient_volume
      }));

      await base44.asServiceRole.entities.AssessmentCache.create({
        assessment_id,
        cache_type: 'facilities',
        data: { facilities: facilityCache },
        zcta_count: zctas.length,
        last_updated: new Date().toISOString()
      });

      results.push({ type: 'facilities', count: facilities.length });
    }

    // Cache cities
    if (typesToCache.includes('cities')) {
      const placeRelationships = await base44.asServiceRole.entities.ZctaPlaceRelationship.filter({
        zcta5: { '$in': zctas }
      });

      // Group by city (excluding counties/parishes)
      const cityMap = new Map();
      placeRelationships.forEach(p => {
        const isCountyOrParish = /(parish|county)$/i.test(p.place_name);
        if (isCountyOrParish) return;

        const cityKey = p.place_geoid;
        if (!cityMap.has(cityKey)) {
          cityMap.set(cityKey, {
            name: p.place_name.replace(/ (city|town|village|CDP)$/i, '').trim(),
            state: p.state_abbr,
            population: p.place_population || 0,
            place_geoid: p.place_geoid,
            zip_codes: [p.zcta5]
          });
        } else {
          cityMap.get(cityKey).zip_codes.push(p.zcta5);
        }
      });

      const cities = Array.from(cityMap.values())
        .filter(c => c.population > 1000)
        .sort((a, b) => b.population - a.population);

      await base44.asServiceRole.entities.AssessmentCache.create({
        assessment_id,
        cache_type: 'cities',
        data: { cities },
        zcta_count: zctas.length,
        last_updated: new Date().toISOString()
      });

      results.push({ type: 'cities', count: cities.length });
    }

    // Cache demographics summary
    if (typesToCache.includes('demographics')) {
      const demographics = await base44.asServiceRole.entities.ZipDemographics.filter({
        zcta5: { '$in': zctas }
      });

      // Calculate summary statistics
      const totalPop = demographics.reduce((sum, d) => sum + (d.population || 0), 0);
      const validIncomes = demographics.filter(d => d.median_income > 0);
      const avgIncome = validIncomes.length > 0
        ? validIncomes.reduce((sum, d) => sum + d.median_income, 0) / validIncomes.length
        : 0;

      const summary = {
        total_population: totalPop,
        avg_median_income: Math.round(avgIncome),
        zcta_count: demographics.length,
        demographics: demographics // Store full data for detailed views
      };

      await base44.asServiceRole.entities.AssessmentCache.create({
        assessment_id,
        cache_type: 'demographics',
        data: summary,
        zcta_count: demographics.length,
        last_updated: new Date().toISOString()
      });

      results.push({ type: 'demographics', count: demographics.length });
    }

    return Response.json({
      success: true,
      cached: results,
      assessment_id
    });

  } catch (error) {
    console.error('Error caching assessment data:', error);
    return Response.json({ 
      error: error.message 
    }, { status: 500 });
  }
});