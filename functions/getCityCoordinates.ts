import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { cityName, state } = await req.json();
    
    if (!cityName || !state) {
      return Response.json({ 
        error: 'City name and state are required' 
      }, { status: 400 });
    }

    // Use Nominatim API to geocode the city
    const query = `${cityName}, ${state}, USA`;
    const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1`;
    
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'HealthScope-CommunityHealthApp/1.0'
      }
    });

    if (!response.ok) {
      return Response.json({ 
        error: 'Geocoding service unavailable' 
      }, { status: 500 });
    }

    const results = await response.json();
    
    if (results && results.length > 0) {
      return Response.json({
        success: true,
        coordinates: {
          latitude: parseFloat(results[0].lat),
          longitude: parseFloat(results[0].lon)
        }
      });
    }

    return Response.json({
      success: false,
      error: 'City not found'
    });

  } catch (error) {
    console.error('Error geocoding city:', error);
    return Response.json({ 
      error: error.message 
    }, { status: 500 });
  }
});