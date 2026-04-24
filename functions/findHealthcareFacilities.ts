
import { createClientFromRequest } from 'npm:@base44/sdk@0.7.1';

// Helper function to add delay between requests
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Helper function to retry with exponential backoff
async function retryWithBackoff(fn, maxRetries = 3, baseDelay = 2000) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            return await fn();
        } catch (error) {
            if (i === maxRetries - 1) throw error;
            
            const waitTime = baseDelay * Math.pow(2, i);
            console.log(`Attempt ${i + 1} failed, retrying in ${waitTime}ms...`);
            await delay(waitTime);
        }
    }
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        
        // Verify authentication
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { latitude, longitude, radiusMiles, facilityTypes } = await req.json();

        if (!latitude || !longitude || !radiusMiles) {
            return Response.json({ 
                error: 'Latitude, longitude, and radiusMiles are required' 
            }, { status: 400 });
        }

        const types = facilityTypes || ['hospital', 'clinic'];

        // Calculate bounding box (limit to max 100 miles to avoid overloading API)
        const limitedRadius = Math.min(radiusMiles, 100);
        const latOffset = limitedRadius / 69.0;
        const lngOffset = limitedRadius / (69.0 * Math.cos(latitude * Math.PI / 180));

        const minLat = latitude - latOffset;
        const maxLat = latitude + latOffset;
        const minLng = longitude - lngOffset;
        const maxLng = longitude + lngOffset;

        const facilities = [];

        // Helper function to calculate distance
        const calculateDistance = (lat1, lon1, lat2, lon2) => {
            const R = 3959; // Earth's radius in miles
            const dLat = (lat2 - lat1) * Math.PI / 180;
            const dLon = (lon2 - lon1) * Math.PI / 180;
            const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                     Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                     Math.sin(dLon / 2) * Math.sin(dLon / 2);
            const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        };

        // Query for hospitals with retry logic
        if (types.includes('hospital')) {
            try {
                const hospitalData = await retryWithBackoff(async () => {
                    const hospitalQuery = `
                    [out:json][timeout:25];
                    (
                      node["amenity"="hospital"](${minLat},${minLng},${maxLat},${maxLng});
                      way["amenity"="hospital"](${minLat},${minLng},${maxLat},${maxLng});
                    );
                    out center;
                    `;

                    const response = await fetch('https://overpass-api.de/api/interpreter', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'User-Agent': 'HealthScope/1.0 (Healthcare Assessment Tool)'
                        },
                        body: `data=${encodeURIComponent(hospitalQuery)}`
                    });

                    if (!response.ok) {
                        if (response.status === 503) {
                            throw new Error('Overpass API temporarily unavailable');
                        }
                        throw new Error(`Overpass API returned status ${response.status}`);
                    }

                    return await response.json();
                });

                // Keep track of hospitals found *before* name filtering
                const initialHospitalsCount = facilities.length;

                for (const element of hospitalData.elements || []) {
                    let lat, lon;
                    if (element.lat && element.lon) {
                        lat = element.lat;
                        lon = element.lon;
                    } else if (element.center) {
                        lat = element.center.lat;
                        lon = element.center.lon;
                    } else {
                        continue;
                    }

                    const distance = calculateDistance(latitude, longitude, lat, lon);
                    
                    if (distance <= radiusMiles) {
                        const tags = element.tags || {};
                        
                        // IMPROVED: Better name extraction with fallback
                        let facilityName = tags.name || 
                                         tags['official_name'] || 
                                         tags['alt_name'] || 
                                         tags['operator'] ||
                                         null;
                        
                        // Skip if no identifiable name
                        if (!facilityName) {
                            console.log(`Skipping hospital without name at ${lat}, ${lon}`);
                            continue;
                        }
                        
                        facilities.push({
                            id: `hospital_${element.id}`,
                            name: facilityName,
                            type: 'hospital',
                            address: tags['addr:full'] || tags['addr:street'] || '',
                            city: tags['addr:city'] || '',
                            state: tags['addr:state'] || '',
                            zip_code: tags['addr:postcode'] || '',
                            latitude: lat,
                            longitude: lon,
                            distance: distance,
                            source: 'OpenStreetMap'
                        });
                    }
                }

                console.log(`Found ${facilities.length - initialHospitalsCount} named hospitals`);
            } catch (error) {
                console.error('Error fetching hospitals:', error);
                // Continue to clinics even if hospitals fail
            }

            // Add delay between requests to be respectful to OSM servers
            await delay(2000);
        }

        // Query for clinics with better categorization
        if (types.includes('clinic')) {
            try {
                const clinicData = await retryWithBackoff(async () => {
                    const clinicQuery = `
                    [out:json][timeout:25];
                    (
                      node["amenity"="clinic"](${minLat},${minLng},${maxLat},${maxLng});
                      node["amenity"="doctors"](${minLat},${minLng},${maxLat},${maxLng});
                      way["amenity"="clinic"](${minLat},${minLng},${maxLat},${maxLng});
                      node["healthcare"="centre"](${minLat},${minLng},${maxLat},${maxLng});
                      way["healthcare"="centre"](${minLat},${minLng},${maxLat},${maxLng});
                    );
                    out center;
                    `;

                    const response = await fetch('https://overpass-api.de/api/interpreter', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'User-Agent': 'HealthScope/1.0 (Healthcare Assessment Tool)'
                        },
                        body: `data=${encodeURIComponent(clinicQuery)}`
                    });

                    if (!response.ok) {
                        if (response.status === 503) {
                            throw new Error('Overpass API temporarily unavailable');
                        }
                        throw new Error(`Overpass API returned status ${response.status}`);
                    }

                    return await response.json();
                });

                const initialClinicsCount = facilities.length;

                for (const element of clinicData.elements || []) {
                    let lat, lon;
                    if (element.lat && element.lon) {
                        lat = element.lat;
                        lon = element.lon;
                    } else if (element.center) {
                        lat = element.center.lat;
                        lon = element.center.lon;
                    } else {
                        continue;
                    }

                    const distance = calculateDistance(latitude, longitude, lat, lon);
                    
                    if (distance <= radiusMiles) {
                        const tags = element.tags || {};
                        
                        let facilityName = tags.name || 
                                         tags['official_name'] || 
                                         tags['alt_name'] || 
                                         tags['operator'] ||
                                         tags['healthcare:name'] ||
                                         null;
                        
                        if (!facilityName) {
                            console.log(`Skipping clinic without name at ${lat}, ${lon}`);
                            continue;
                        }
                        
                        // IMPROVED: Better type detection for FQHCs vs regular clinics
                        let facilityType = 'clinic'; // Default to regular clinic
                        
                        // Check if it's an FQHC based on name patterns
                        const nameLower = facilityName.toLowerCase();
                        const fqhcIndicators = [
                            'fqhc', 'federally qualified', 'community health center',
                            'chc', 'health center', 'community clinic'
                        ];
                        
                        const isFQHC = fqhcIndicators.some(indicator => nameLower.includes(indicator));
                        
                        if (isFQHC) {
                            facilityType = 'fqhc';
                        }
                        
                        facilities.push({
                            id: `clinic_${element.id}`,
                            name: facilityName,
                            type: facilityType, // Now properly set as 'clinic' or 'fqhc'
                            address: tags['addr:full'] || tags['addr:street'] || '',
                            city: tags['addr:city'] || '',
                            state: tags['addr:state'] || '',
                            zip_code: tags['addr:postcode'] || '',
                            latitude: lat,
                            longitude: lon,
                            distance: distance,
                            source: 'OpenStreetMap'
                        });
                    }
                }

                console.log(`Found ${facilities.length - initialClinicsCount} named clinics. Total named facilities: ${facilities.length}`);
            } catch (error) {
                console.error('Error fetching clinics:', error);
                // Continue even if clinics fail
            }
        }

        // Sort by distance
        facilities.sort((a, b) => a.distance - b.distance);

        return Response.json({
            success: true,
            facilities: facilities,
            count: facilities.length,
            message: `Found ${facilities.length} named healthcare facilities within ${radiusMiles} miles`,
            note: radiusMiles > 100 ? `Radius limited to 100 miles to avoid API overload` : null
        });

    } catch (error) {
        console.error('Error finding healthcare facilities:', error);
        
        // Provide user-friendly error messages
        let userMessage = error.message;
        if (error.message.includes('Overpass API temporarily unavailable')) {
            userMessage = 'OpenStreetMap service is temporarily busy. Please try again in a few minutes.';
        } else if (error.message.includes('timeout')) {
            userMessage = 'Request timed out. Try reducing the search radius or try again later.';
        }
        
        return Response.json({ 
            success: false, 
            error: userMessage,
            facilities: [],
            count: 0
        }, { status: error.message.includes('Unauthorized') ? 401 : 500 });
    }
});
