import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { cities } = await req.json();
    
    if (!cities || !Array.isArray(cities) || cities.length === 0) {
      return Response.json({ 
        error: 'Cities array is required' 
      }, { status: 400 });
    }

    const results = [];
    
    // Process cities with rate limiting - limit to 10 cities max for speed
    const citiesToProcess = cities.slice(0, 10);
    
    for (const city of citiesToProcess) {
      try {
        const query = `${city.name}, ${city.state}, USA`;
        const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1`;
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000);
        
        const response = await fetch(url, {
          headers: {
            'User-Agent': 'HealthScope-CommunityHealthApp/1.0'
          },
          signal: controller.signal
        });
        
        clearTimeout(timeoutId);

        if (response.ok) {
          const data = await response.json();
          
          if (data && data.length > 0) {
            results.push({
              ...city,
              coordinates: {
                latitude: parseFloat(data[0].lat),
                longitude: parseFloat(data[0].lon)
              },
              success: true
            });
          } else {
            results.push({
              ...city,
              success: false,
              error: 'Not found'
            });
          }
        } else {
          results.push({
            ...city,
            success: false,
            error: `HTTP ${response.status}`
          });
        }
        
        // Rate limit: 1 request per second for Nominatim
        await new Promise(resolve => setTimeout(resolve, 1100));
        
      } catch (error) {
        results.push({
          ...city,
          success: false,
          error: error.message
        });
      }
    }

    return Response.json({
      success: true,
      results: results
    });

  } catch (error) {
    console.error('Error batch geocoding cities:', error);
    return Response.json({ 
      error: error.message 
    }, { status: 500 });
  }
});