import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const TIGER_ZCTA_LAYER = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2025/MapServer/2";

Deno.serve(async (req) => {
    const startTime = Date.now();
    
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ 
                success: false,
                error: 'Unauthorized',
                zipCodes: []
            }, { status: 401 });
        }

        let body;
        try {
            body = await req.json();
        } catch (jsonError) {
            console.error('Failed to parse request body:', jsonError);
            return Response.json({
                success: false,
                error: 'Invalid request body',
                zipCodes: []
            }, { status: 400 });
        }

        const { city, state, radiusMiles } = body;

        if (!city || !state || !radiusMiles) {
            return Response.json({ 
                success: false,
                error: 'City, state, and radiusMiles are required',
                zipCodes: []
            }, { status: 400 });
        }

        // Enforce 100-mile maximum radius
        const MAX_RADIUS_MILES = 100;
        const effectiveRadius = Math.min(parseFloat(radiusMiles), MAX_RADIUS_MILES);
        
        if (parseFloat(radiusMiles) > MAX_RADIUS_MILES) {
            console.log(`Radius ${radiusMiles} exceeds limit, capping at ${MAX_RADIUS_MILES} miles`);
        }

        // Geocode with timeout
        console.log(`Geocoding: ${city}, ${state}`);
        const geocodeUrl = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(`${city}, ${state}`)}&format=json&limit=1&countrycodes=us`;
        const geocodeController = new AbortController();
        const geocodeTimeout = setTimeout(() => geocodeController.abort(), 8000);
        
        let geocodeResponse;
        try {
            geocodeResponse = await fetch(geocodeUrl, { 
                headers: { 'User-Agent': 'HealthScope/1.0' },
                signal: geocodeController.signal
            });
            clearTimeout(geocodeTimeout);
        } catch (geocodeError) {
            clearTimeout(geocodeTimeout);
            console.error('Geocoding failed:', geocodeError);
            throw new Error(`Could not connect to geocoding service: ${geocodeError.message}`);
        }

        if (!geocodeResponse.ok) {
            return Response.json({
                success: false,
                error: 'Could not geocode location',
                zipCodes: []
            }, { status: 400 });
        }

        const geocodeData = await geocodeResponse.json();
        
        if (!geocodeData || geocodeData.length === 0) {
            return Response.json({
                success: false,
                error: `Location "${city}, ${state}" not found`,
                zipCodes: []
            }, { status: 400 });
        }

        const lat = parseFloat(geocodeData[0].lat);
        const lon = parseFloat(geocodeData[0].lon);
        
        console.log(`Searching ${effectiveRadius}mi from ${lat.toFixed(4)}, ${lon.toFixed(4)}`);
        
        // Query TIGERweb for ZCTAs within radius
        const params = new URLSearchParams({
            f: "json",
            where: "1=1",
            outFields: "ZCTA5",
            geometry: `${lon},${lat}`,
            geometryType: "esriGeometryPoint",
            inSR: "4326",
            spatialRel: "esriSpatialRelIntersects",
            distance: String(effectiveRadius),
            units: "esriSRUnit_StatuteMile",
            returnGeometry: "true",
            outSR: "4326",
            geometryPrecision: "6"
        });

        const tigerUrl = `${TIGER_ZCTA_LAYER}/query?${params.toString()}`;
        const tigerController = new AbortController();
        const tigerTimeout = setTimeout(() => tigerController.abort(), 25000);
        
        let tigerResponse;
        try {
            tigerResponse = await fetch(tigerUrl, {
                signal: tigerController.signal
            });
            clearTimeout(tigerTimeout);
        } catch (tigerError) {
            clearTimeout(tigerTimeout);
            console.error('TIGERweb fetch failed:', tigerError);
            throw new Error(`Census TIGERweb service unavailable: ${tigerError.message}`);
        }

        if (!tigerResponse.ok) {
            const text = await tigerResponse.text().catch(() => "");
            throw new Error(`TIGERweb query failed: ${tigerResponse.status} ${text}`);
        }

        const tigerData = await tigerResponse.json();

        // Transform ESRI features to GeoJSON
        const features = (tigerData.features || [])
            .map(f => {
                const zcta5 = f?.attributes?.ZCTA5;
                const geom = f?.geometry;

                if (typeof zcta5 !== "string" || !geom?.rings) return null;

                // ESRI polygon: { rings: [ [ [x,y], [x,y], ... ], ... ] }
                const rings = geom.rings.map(ring =>
                    ring.map(pair => [Number(pair[0]), Number(pair[1])])
                );

                const geometry = rings.length === 1
                    ? {
                        type: "Polygon",
                        coordinates: rings
                    }
                    : {
                        type: "MultiPolygon",
                        coordinates: rings.map(r => [r])
                    };

                return {
                    type: "Feature",
                    properties: { zcta5 },
                    geometry
                };
            })
            .filter(f => f !== null);

        const zctaCodes = features.map(f => f.properties.zcta5).sort();

        const elapsed = Date.now() - startTime;
        console.log(`Found ${zctaCodes.length} ZCTAs in ${elapsed}ms`);

        const radiusNote = parseFloat(radiusMiles) > MAX_RADIUS_MILES 
            ? ` (capped from ${radiusMiles} to ${MAX_RADIUS_MILES} miles)`
            : '';

        return Response.json({
            success: true,
            zipCodes: zctaCodes,
            center: { latitude: lat, longitude: lon },
            radiusUsed: effectiveRadius,
            radiusRequested: parseFloat(radiusMiles),
            radiusCapped: parseFloat(radiusMiles) > MAX_RADIUS_MILES,
            geojson: {
                type: "FeatureCollection",
                features
            },
            message: zctaCodes.length > 0 
                ? `Found ${zctaCodes.length} ZCTAs within ${effectiveRadius} miles${radiusNote}`
                : `No ZCTAs found within ${effectiveRadius} miles.`,
            processingTimeMs: elapsed,
            source: "US Census TIGERweb"
        });

    } catch (error) {
        console.error('Error in findZipCodesInRadius:', error);
        console.error('Stack:', error.stack);
        
        let errorMessage = error.message || 'Server error';
        let statusCode = 500;
        
        if (error.name === 'AbortError' || errorMessage.includes('aborted')) {
            errorMessage = 'Request timeout - try a smaller radius or try again';
            statusCode = 504;
        } else if (errorMessage.includes('TIGERweb')) {
            errorMessage = 'Census TIGERweb service unavailable - try again in a moment';
            statusCode = 502;
        } else if (errorMessage.includes('not found')) {
            statusCode = 404;
        }
        
        return Response.json({ 
            success: false, 
            error: errorMessage,
            zipCodes: [],
            hint: 'Use the next step to load boundaries from database instead'
        }, { status: statusCode });
    }
});