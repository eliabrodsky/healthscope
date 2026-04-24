import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const TIGER_ZCTA_LAYER = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2025/MapServer/2";
const CENSUS_API_BASE = "https://api.census.gov/data";

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ 
                success: false,
                error: 'Unauthorized'
            }, { status: 401 });
        }

        const body = await req.json();
        const { city, state, radiusMiles, year = 2022 } = body;

        if (!city || !state || !radiusMiles) {
            return Response.json({ 
                success: false,
                error: 'City, state, and radiusMiles are required'
            }, { status: 400 });
        }

        // Step 1: Geocode location
        const geocodeUrl = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(`${city}, ${state}`)}&format=json&limit=1&countrycodes=us`;
        const geocodeResponse = await fetch(geocodeUrl, { 
            headers: { 'User-Agent': 'HealthScope/1.0' }
        });

        if (!geocodeResponse.ok) {
            return Response.json({
                success: false,
                error: 'Could not geocode location'
            }, { status: 400 });
        }

        const geocodeData = await geocodeResponse.json();
        
        if (!geocodeData || geocodeData.length === 0) {
            return Response.json({
                success: false,
                error: `Location "${city}, ${state}" not found`
            }, { status: 400 });
        }

        const lat = parseFloat(geocodeData[0].lat);
        const lon = parseFloat(geocodeData[0].lon);

        // Step 2: Get ZCTAs from TIGERweb
        const params = new URLSearchParams({
            f: "json",
            where: "1=1",
            outFields: "ZCTA5",
            geometry: `${lon},${lat}`,
            geometryType: "esriGeometryPoint",
            inSR: "4326",
            spatialRel: "esriSpatialRelIntersects",
            distance: String(radiusMiles),
            units: "esriSRUnit_StatuteMile",
            returnGeometry: "false",
            outSR: "4326"
        });

        const tigerUrl = `${TIGER_ZCTA_LAYER}/query?${params.toString()}`;
        const tigerResponse = await fetch(tigerUrl);

        if (!tigerResponse.ok) {
            throw new Error(`TIGERweb query failed: ${tigerResponse.status}`);
        }

        const tigerData = await tigerResponse.json();
        const zctaCodes = (tigerData.features || [])
            .map(f => f?.attributes?.ZCTA5)
            .filter(z => typeof z === "string")
            .sort();

        if (zctaCodes.length === 0) {
            return Response.json({
                success: false,
                error: `No ZCTAs found within ${radiusMiles} miles of ${city}, ${state}`
            });
        }

        // Step 3: Fetch Census data for ZCTAs
        const censusApiKey = Deno.env.get('CENSUS_API_KEY');
        if (!censusApiKey) {
            return Response.json({
                success: false,
                error: 'Census API key not configured'
            }, { status: 500 });
        }

        // Helper function to clean Census values (handle error codes)
        const cleanCensusValue = (value) => {
            if (value === null || value === undefined) return null;
            const numValue = parseInt(value);
            // Census error codes: -666666666, -999999999, -888888888, -222222222, -333333333, -555555555
            if (isNaN(numValue) || numValue < -1000000) return null;
            if (numValue < 0) return null; // Also reject any negative values
            return numValue;
        };

        const censusData = {};
        const BATCH_SIZE = 50;
        
        for (let i = 0; i < zctaCodes.length; i += BATCH_SIZE) {
            const batch = zctaCodes.slice(i, i + BATCH_SIZE);
            const zctaFilter = batch.join(',');

            // Fetch multiple variables in one call
            const variables = [
                'B01003_001E',  // Total population
                'B19013_001E',  // Median household income
                'B17001_002E',  // Below poverty level
                'B17001_001E',  // Total for poverty calculation
                'B27001_005E',  // Uninsured male under 18
                'B27001_008E',  // Uninsured male 18-34
                'B27001_011E',  // Uninsured male 35-64
                'B27001_014E',  // Uninsured male 65+
                'B27001_033E',  // Uninsured female under 18
                'B27001_036E',  // Uninsured female 18-34
                'B27001_039E',  // Uninsured female 35-64
                'B27001_042E',  // Uninsured female 65+
                'B27001_001E'   // Total for insurance calculation
            ];

            const censusUrl = `${CENSUS_API_BASE}/${year}/acs/acs5?get=${variables.join(',')}&for=zip%20code%20tabulation%20area:${zctaFilter}&key=${censusApiKey}`;

            try {
                const censusResponse = await fetch(censusUrl);
                
                if (censusResponse.ok) {
                    const data = await censusResponse.json();
                    
                    // Skip header row
                    for (let j = 1; j < data.length; j++) {
                        const row = data[j];
                        const zcta = row[row.length - 1]; // Last element is the ZCTA
                        
                        const population = cleanCensusValue(row[0]);
                        const medianIncome = cleanCensusValue(row[1]);
                        const belowPoverty = cleanCensusValue(row[2]);
                        const povertyTotal = cleanCensusValue(row[3]);
                        
                        // Sum all uninsured counts
                        const uninsuredCount = (
                            (cleanCensusValue(row[4]) || 0) +
                            (cleanCensusValue(row[5]) || 0) +
                            (cleanCensusValue(row[6]) || 0) +
                            (cleanCensusValue(row[7]) || 0) +
                            (cleanCensusValue(row[8]) || 0) +
                            (cleanCensusValue(row[9]) || 0) +
                            (cleanCensusValue(row[10]) || 0) +
                            (cleanCensusValue(row[11]) || 0)
                        );
                        const uninsuredTotal = cleanCensusValue(row[12]);
                        
                        const povertyRate = (belowPoverty && povertyTotal && povertyTotal > 0) 
                            ? parseFloat(((belowPoverty / povertyTotal) * 100).toFixed(1))
                            : null;
                            
                        const uninsuredRate = (uninsuredTotal && uninsuredTotal > 0) 
                            ? parseFloat(((uninsuredCount / uninsuredTotal) * 100).toFixed(1))
                            : null;

                        // Only add if we have at least some valid data
                        if (population || medianIncome || povertyRate || uninsuredRate) {
                            censusData[zcta] = {
                                population,
                                medianIncome,
                                povertyRate,
                                uninsuredRate,
                                source: `US Census Bureau ACS 5-Year ${year}`
                            };
                        }
                    }
                }
            } catch (batchError) {
                console.warn(`Failed to fetch batch starting at index ${i}:`, batchError);
            }

            // Rate limit pause
            if (i + BATCH_SIZE < zctaCodes.length) {
                await new Promise(resolve => setTimeout(resolve, 300));
            }
        }

        return Response.json({
            success: true,
            data: censusData,
            zipCodes: zctaCodes,
            center: { latitude: lat, longitude: lon },
            message: `Loaded demographics for ${Object.keys(censusData).length} of ${zctaCodes.length} ZCTAs`,
            note: Object.keys(censusData).length < zctaCodes.length 
                ? `${zctaCodes.length - Object.keys(censusData).length} ZCTAs had no Census data available`
                : null
        });

    } catch (error) {
        console.error('Error in fetchCensusByRadius:', error);
        return Response.json({ 
            success: false, 
            error: error.message || 'Server error'
        }, { status: 500 });
    }
});