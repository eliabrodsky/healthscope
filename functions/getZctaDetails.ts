import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { zip } = await req.json();

    if (!zip) {
      return Response.json({ 
        success: false, 
        error: 'ZIP code is required' 
      }, { status: 400 });
    }

    // Call the external ZCTA micro-service
    const apiUrl = `https://getzcta-1038555279570.us-south1.run.app/getZcta?zip=${encodeURIComponent(zip)}`;
    const response = await fetch(apiUrl);

    if (!response.ok) {
      return Response.json({ 
        success: false, 
        error: `API returned status ${response.status}` 
      }, { status: response.status });
    }

    const data = await response.json();

    if (!data || data.length === 0) {
      return Response.json({ 
        success: false, 
        error: 'No data found for this ZIP code' 
      }, { status: 404 });
    }

    // Return the first (and typically only) ZCTA record
    const zcta = data[0];

    // Calculate derived values for convenience
    const povertyRate = zcta.poverty_universe > 0 
      ? ((zcta.poverty_below / zcta.poverty_universe) * 100).toFixed(1)
      : 0;

    return Response.json({
      success: true,
      data: {
        ...zcta,
        // Add computed fields
        poverty_rate: parseFloat(povertyRate),
        owner_occupied_rate: zcta.housing_units_total > 0
          ? ((zcta.housing_owner_occupied / zcta.housing_units_total) * 100).toFixed(1)
          : 0
      }
    });

  } catch (error) {
    console.error('Error fetching ZCTA details:', error);
    return Response.json({ 
      success: false, 
      error: error.message 
    }, { status: 500 });
  }
});