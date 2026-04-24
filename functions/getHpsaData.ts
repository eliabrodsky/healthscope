import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { zip_codes } = await req.json();
    
    if (!zip_codes || !Array.isArray(zip_codes) || zip_codes.length === 0) {
      return Response.json({ error: 'ZIP codes array required' }, { status: 400 });
    }

    // Fetch HPSA data from HRSA API
    const hpsaData = [];
    
    for (const zip of zip_codes.slice(0, 50)) { // Limit to 50 for performance
      try {
        const url = `https://data.hrsa.gov/api/3/action/datastore_search?resource_id=c9f9f6c7-5c3e-4d5f-9f5e-3f5c3e4d5f9f&filters={"Common ZIP Code":"${zip}"}`;
        
        const response = await fetch(url, {
          headers: {
            'Accept': 'application/json'
          }
        });
        
        if (response.ok) {
          const data = await response.json();
          if (data.result?.records?.length > 0) {
            hpsaData.push(...data.result.records.map(r => ({
              zip_code: zip,
              hpsa_name: r['HPSA Name'],
              hpsa_type: r['HPSA Type'],
              designation_type: r['Designation Type'],
              status: r['HPSA Status'],
              score: r['HPSA Score'],
              source: 'HRSA HPSA API'
            })));
          }
        }
        
        await new Promise(resolve => setTimeout(resolve, 200)); // Rate limiting
      } catch (err) {
        console.warn(`Failed to fetch HPSA for ${zip}:`, err);
      }
    }

    return Response.json({
      success: true,
      hpsa_areas: hpsaData,
      count: hpsaData.length
    });

  } catch (error) {
    console.error('Error in getHpsaData:', error);
    return Response.json({ 
      success: false,
      error: error.message 
    }, { status: 500 });
  }
});