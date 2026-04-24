import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';
import OpenAI from 'npm:openai';

const openai = new OpenAI({
    apiKey: Deno.env.get('OPENAI_API_KEY')
});

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
        const { prompt, assessmentId } = body;

        if (!prompt || !assessmentId) {
            return Response.json({ error: 'Prompt and assessmentId required' }, { status: 400 });
        }

        // Load assessment data
        const assessment = await base44.entities.Assessment.filter({ id: assessmentId });
        if (!assessment || assessment.length === 0) {
            return Response.json({ error: 'Assessment not found' }, { status: 404 });
        }

        const assessmentData = assessment[0];
        const zipCodes = assessmentData.geography?.zcta_codes || [];
        const organizations = assessmentData.organizations || [];
        const stateAbbr = assessmentData.geography?.state;

        console.log('Assessment loaded:', {
            id: assessmentId,
            title: assessmentData.title,
            zipCount: zipCodes.length,
            state: stateAbbr,
            orgsCount: organizations.length
        });

        // Parse the user's intent
        const parseResponse = await openai.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
                {
                    role: 'system',
                    content: `You are a map request parser. Given a user's natural language request about maps or data visualization, extract the intent and parameters.

Available map types:
- competitor_map: Shows FQHC/hospital/clinic locations
- population_map: Shows total population by ZIP (good for "population", "people", "residents")
- children_map: Shows child population under 18 (for "children", "kids", "youth", "pediatric")
- elderly_map: Shows elderly population 65+ (for "elderly", "seniors", "aging", "65+")
- poverty_map: Shows poverty rates by ZIP (for "poverty", "low income", "poor")
- uninsured_map: Shows uninsured rates by ZIP (for "uninsured", "no insurance", "coverage")
- income_map: Shows median income by ZIP (for "income", "wealth", "earnings")
- demographics_overview: General demographic summary

Return JSON with:
{
  "map_type": "competitor_map|population_map|children_map|elderly_map|poverty_map|uninsured_map|income_map|demographics_overview",
  "filters": {
    "facility_types": ["fqhc", "hospital", "clinic"],
    "center_location": "city, state",
    "radius_miles": 10,
    "metric": "population|children|elderly|poverty|uninsured|income"
  },
  "title": "A descriptive title for the map"
}`
                },
                {
                    role: 'user',
                    content: prompt
                }
            ],
            response_format: { type: 'json_object' }
        });

        const mapConfig = JSON.parse(parseResponse.choices[0].message.content);
        console.log('Parsed map config:', mapConfig);

        // Build map data based on type
        let mapData = {
            type: mapConfig.map_type,
            title: mapConfig.title,
            center: null,
            markers: [],
            choropleth: [],
            bounds: null,
            dataLoaded: false
        };

        // Get center coordinates
        let centerLat = assessmentData.geography?.latitude;
        let centerLng = assessmentData.geography?.longitude;
        
        // If no coordinates, geocode from city/state
        if (!centerLat || !centerLng) {
            const city = assessmentData.geography?.city;
            const state = assessmentData.geography?.state;
            if (city && state) {
                try {
                    const geocodeUrl = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(`${city}, ${state}`)}&format=json&limit=1&countrycodes=us`;
                    const geocodeResponse = await fetch(geocodeUrl, {
                        headers: { 'User-Agent': 'HealthScope/1.0' }
                    });
                    const geocodeData = await geocodeResponse.json();
                    if (geocodeData.length > 0) {
                        centerLat = parseFloat(geocodeData[0].lat);
                        centerLng = parseFloat(geocodeData[0].lon);
                    }
                } catch (geoError) {
                    console.error('Geocoding failed:', geoError);
                }
            }
        }
        
        if (centerLat && centerLng) {
            mapData.center = { lat: centerLat, lng: centerLng };
        }

        // Load demographics data - try multiple approaches
        let demographics = [];

        // First try: ZIPs from assessment
        if (zipCodes.length > 0) {
            console.log('Fetching demographics for', zipCodes.length, 'ZIP codes');
            try {
                demographics = await base44.entities.ZipDemographics.filter({
                    zcta5: { $in: zipCodes.slice(0, 100) }  // Limit to first 100
                }, null, 500);
                console.log('Found demographics by ZIP:', demographics.length);
            } catch (err) {
                console.error('ZIP-based demographics fetch failed:', err.message);
            }
        }

        // Second try: State-based lookup
        if (demographics.length === 0 && stateAbbr) {
            console.log('No ZIP demographics found, trying state:', stateAbbr);
            try {
                demographics = await base44.entities.ZipDemographics.filter({
                    state_abbr: stateAbbr
                }, null, 200);
                console.log('Found demographics by state:', demographics.length);
            } catch (err) {
                console.error('State-based demographics fetch failed:', err.message);
            }
        }

        // Third try: Load all available demographics
        if (demographics.length === 0) {
            console.log('No state demographics, loading any available');
            try {
                demographics = await base44.entities.ZipDemographics.list('-population', 100);
                console.log('Found any demographics:', demographics.length);
            } catch (err) {
                console.error('General demographics fetch failed:', err.message);
            }
        }

        // Handle different map types
        if (mapConfig.map_type === 'competitor_map') {
            const facilityTypes = mapConfig.filters?.facility_types || ['fqhc', 'hospital', 'clinic'];
            
            mapData.markers = organizations
                .filter(org => facilityTypes.includes(org.type))
                .map(org => ({
                    id: org.id,
                    name: org.name,
                    type: org.type,
                    lat: org.coordinates?.latitude,
                    lng: org.coordinates?.longitude,
                    address: org.address,
                    city: org.city,
                    state: org.state
                }))
                .filter(m => m.lat && m.lng);
            
            if (mapData.markers.length === 0 && organizations.length > 0) {
                mapData.markers = organizations
                    .filter(org => facilityTypes.includes(org.type))
                    .slice(0, 20)
                    .map(org => ({
                        id: org.id,
                        name: org.name,
                        type: org.type,
                        address: org.address,
                        city: org.city,
                        state: org.state,
                        needsGeocoding: true
                    }));
            }
            
            mapData.dataLoaded = true;
            
        } else if (['population_map', 'children_map', 'elderly_map', 'poverty_map', 'uninsured_map', 'income_map', 'demographics_overview'].includes(mapConfig.map_type)) {
            
            // Create summary stats from actual data
            if (demographics.length > 0) {
                const totalPop = demographics.reduce((sum, d) => sum + (d.population || 0), 0);
                const avgPoverty = demographics.reduce((sum, d) => sum + (d.poverty_rate || 0), 0) / demographics.length;
                const avgUninsured = demographics.reduce((sum, d) => sum + (d.uninsured_rate || 0), 0) / demographics.length;
                const avgIncome = demographics.reduce((sum, d) => sum + (d.median_income || 0), 0) / demographics.length;

                mapData.summaryStats = {
                    total_population: totalPop,
                    avg_poverty_rate: parseFloat(avgPoverty.toFixed(1)),
                    avg_uninsured_rate: parseFloat(avgUninsured.toFixed(1)),
                    avg_median_income: Math.round(avgIncome),
                    zip_count: demographics.length
                };

                // Determine which metric to sort by
                const metricMap = {
                    population_map: 'population',
                    children_map: 'population', // Could add children-specific field if available
                    elderly_map: 'population',
                    poverty_map: 'poverty_rate',
                    uninsured_map: 'uninsured_rate',
                    income_map: 'median_income',
                    demographics_overview: 'population'
                };

                const metric = metricMap[mapConfig.map_type] || 'population';
                const sortedDemos = [...demographics].sort((a, b) => (b[metric] || 0) - (a[metric] || 0));
                
                mapData.topZips = sortedDemos.slice(0, 15).map(d => ({
                    zcta: d.zcta5,
                    population: d.population || 0,
                    poverty_rate: parseFloat((d.poverty_rate || 0).toFixed(1)),
                    uninsured_rate: parseFloat((d.uninsured_rate || 0).toFixed(1)),
                    median_income: d.median_income || 0,
                    households: d.households || 0
                }));

                mapData.metric = metric;
                mapData.dataLoaded = true;
                
                console.log('Map data generated:', {
                    totalPop,
                    avgPoverty: avgPoverty.toFixed(1),
                    topZipsCount: mapData.topZips.length
                });
            } else {
                // No data found - return helpful message
                mapData.summaryStats = {
                    total_population: 0,
                    avg_poverty_rate: 0,
                    avg_uninsured_rate: 0,
                    avg_median_income: 0,
                    zip_count: 0
                };
                mapData.topZips = [];
                mapData.dataLoaded = false;
                mapData.noDataMessage = `No demographic data found for this assessment area. Please load demographics in the Assessment wizard first.`;
            }
        }

        // Add radius from assessment if available
        if (assessmentData.geography?.radius_miles) {
            mapData.radiusMiles = parseFloat(assessmentData.geography.radius_miles);
            mapData.radiusMeters = mapData.radiusMiles * 1609.34;
        }

        return Response.json({
            success: true,
            mapConfig,
            mapData
        });

    } catch (error) {
        console.error('Chat map generation error:', error);
        return Response.json({ error: error.message, stack: error.stack }, { status: 500 });
    }
});