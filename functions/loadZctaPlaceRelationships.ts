import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

// This function loads the Census ZCTA to Place relationship data
// Uses the official tab20_zcta520_place20_natl.txt file from Census Bureau
// Source: https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.html

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { state_abbr, batch_size = 100 } = await req.json();

    if (!state_abbr) {
      return Response.json({ 
        error: 'state_abbr is required' 
      }, { status: 400 });
    }

    const CENSUS_API_KEY = Deno.env.get('CENSUS_API_KEY');
    if (!CENSUS_API_KEY) {
      return Response.json({ 
        error: 'CENSUS_API_KEY not configured' 
      }, { status: 500 });
    }

    // Step 1: Download the ZCTA-Place relationship file from Census
    // Using the 2020 ZCTA to Place relationship file
    const relationshipUrl = 'https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_place20_natl.txt';
    
    console.log(`Downloading ZCTA-Place relationship file...`);
    const response = await fetch(relationshipUrl);
    
    if (!response.ok) {
      throw new Error(`Failed to download relationship file: ${response.status}`);
    }

    const csvText = await response.text();
    const lines = csvText.split('\n');
    const headers = lines[0].split('|').map(h => h.trim());
    
    // Find column indices
    const zcta5Index = headers.indexOf('GEOID_ZCTA5_20');
    const placeGeoidIndex = headers.indexOf('GEOID_PLACE_20');
    const placeNameIndex = headers.indexOf('NAMELSAD_PLACE_20');
    const areaPartIndex = headers.indexOf('AREALAND_PART');
    const areaZctaIndex = headers.indexOf('AREALAND_ZCTA5_20');
    const areaPlaceIndex = headers.indexOf('AREALAND_PLACE_20');

    if (zcta5Index === -1 || placeGeoidIndex === -1) {
      throw new Error('Required columns not found in relationship file');
    }

    console.log(`Found columns - ZCTA: ${zcta5Index}, Place: ${placeGeoidIndex}, Name: ${placeNameIndex}`);

    // Step 2: Parse and filter for the requested state
    const stateFipsMap = {
      'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06',
      'CO': '08', 'CT': '09', 'DE': '10', 'FL': '12', 'GA': '13',
      'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19',
      'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23', 'MD': '24',
      'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29',
      'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33', 'NJ': '34',
      'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39',
      'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44', 'SC': '45',
      'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50',
      'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55', 'WY': '56'
    };

    const targetStateFips = stateFipsMap[state_abbr.toUpperCase()];
    if (!targetStateFips) {
      return Response.json({ 
        error: 'Invalid state abbreviation' 
      }, { status: 400 });
    }

    console.log(`Processing relationships for state ${state_abbr} (FIPS: ${targetStateFips})...`);

    // First, check if we already have data for this state
    const existingCheck = await base44.asServiceRole.entities.ZctaPlaceRelationship.filter(
      { state_abbr: state_abbr.toUpperCase() },
      null,
      10
    );

    if (existingCheck && existingCheck.length > 0) {
      console.log(`State ${state_abbr} already has ${existingCheck.length}+ relationships in database`);
      return Response.json({
        success: true,
        state: state_abbr,
        relationships_loaded: existingCheck.length,
        message: 'Data already exists for this state'
      });
    }

    // Group relationships by ZCTA to determine primary place
    const zctaRelationships = new Map();

    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;

      const cols = line.split('|').map(c => c.trim());

      // Extract state FIPS from the place GEOID (first 2 digits)
      const placeGeoid = cols[placeGeoidIndex];
      if (!placeGeoid || placeGeoid.length < 2) continue;

      const stateFips = placeGeoid.substring(0, 2);

      if (stateFips !== targetStateFips) continue;

      const zcta5 = cols[zcta5Index];
      const placeName = cols[placeNameIndex];
      const areaPart = parseFloat(cols[areaPartIndex]) || 0;
      const areaZcta = parseFloat(cols[areaZctaIndex]) || 1;
      const areaPlace = parseFloat(cols[areaPlaceIndex]) || 1;

      const shareOfZcta = areaPart / areaZcta;
      const shareOfPlace = areaPart / areaPlace;

      if (!zctaRelationships.has(zcta5)) {
        zctaRelationships.set(zcta5, []);
      }

      zctaRelationships.get(zcta5).push({
        zcta5,
        place_geoid: placeGeoid,
        place_name: placeName,
        state_fips: stateFips,
        state_abbr: state_abbr.toUpperCase(),
        share_of_zcta: shareOfZcta,
        share_of_place: shareOfPlace,
        is_primary: false
      });
    }

    // Determine primary place for each ZCTA (highest share_of_zcta)
    for (const [zcta, places] of zctaRelationships.entries()) {
      places.sort((a, b) => b.share_of_zcta - a.share_of_zcta);
      places[0].is_primary = true;
    }

    // Step 3: Fetch population data for places from Census API
    const uniquePlaces = new Set();
    for (const places of zctaRelationships.values()) {
      for (const place of places) {
        uniquePlaces.add(place.place_geoid);
      }
    }

    console.log(`Fetching population for ${uniquePlaces.size} unique places...`);

    const placePopulations = new Map();
    const placeArray = Array.from(uniquePlaces);
    
    // Fetch in batches to avoid API limits
    for (let i = 0; i < placeArray.length; i += 50) {
      const batch = placeArray.slice(i, i + 50);
      
      try {
        // Fetch population for places using ACS 5-Year
        const placeList = batch.map(p => p.slice(2)).join(','); // Remove state FIPS prefix
        const populationUrl = `https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=place:${placeList}&in=state:${targetStateFips}&key=${CENSUS_API_KEY}`;
        
        const popResponse = await fetch(populationUrl);
        if (popResponse.ok) {
          const popData = await popResponse.json();
          
          // Skip header row
          for (let j = 1; j < popData.length; j++) {
            const [name, population, stateFips, placeFips] = popData[j];
            const fullGeoid = stateFips + placeFips;
            placePopulations.set(fullGeoid, parseInt(population) || 0);
          }
        }
      } catch (error) {
        console.warn(`Failed to fetch population for batch:`, error);
      }
      
      // Rate limit: wait between batches
      if (i + 50 < placeArray.length) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    // Step 4: Save to database
    const allRelationships = [];
    for (const places of zctaRelationships.values()) {
      for (const place of places) {
        // Extract place type from name
        let placeType = 'other';
        if (place.place_name.endsWith(' city')) placeType = 'city';
        else if (place.place_name.endsWith(' town')) placeType = 'town';
        else if (place.place_name.endsWith(' village')) placeType = 'village';
        else if (place.place_name.includes('CDP')) placeType = 'CDP';

        allRelationships.push({
          ...place,
          place_type: placeType,
          place_population: placePopulations.get(place.place_geoid) || 0
        });
      }
    }

    if (allRelationships.length === 0) {
      console.warn(`No relationships found for state ${state_abbr} (FIPS: ${targetStateFips})`);
      return Response.json({
        success: true,
        state: state_abbr,
        relationships_loaded: 0,
        message: `No ZCTA-Place relationships found in Census file for ${state_abbr}`
      });
    }

    console.log(`Saving ${allRelationships.length} relationships to database...`);

    // Bulk insert in batches
    let inserted = 0;
    for (let i = 0; i < allRelationships.length; i += batch_size) {
      const batch = allRelationships.slice(i, i + batch_size);
      
      try {
        await base44.asServiceRole.entities.ZctaPlaceRelationship.bulkCreate(batch);
        inserted += batch.length;
        console.log(`Progress: ${inserted}/${allRelationships.length}`);
      } catch (error) {
        console.error(`Failed to insert batch at ${i}:`, error);
        // Continue with next batch instead of failing completely
      }
    }

    return Response.json({
      success: true,
      state: state_abbr,
      relationships_loaded: inserted,
      unique_zctas: zctaRelationships.size,
      unique_places: uniquePlaces.size,
      places_with_population: placePopulations.size
    });

  } catch (error) {
    console.error('Error loading ZCTA-Place relationships:', error);
    return Response.json({ 
      error: error.message || 'Failed to load relationships' 
    }, { status: 500 });
  }
});