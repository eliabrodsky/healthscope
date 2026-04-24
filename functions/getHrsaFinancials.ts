import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const FQHC_API_BASE = 'https://fqhc-api-1038555279570.us-south1.run.app';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { organization_ids } = await req.json();
    
    if (!organization_ids || !Array.isArray(organization_ids)) {
      return Response.json({ error: 'Organization IDs array required' }, { status: 400 });
    }

    const financialsData = [];
    const year = 2023; // Default to most recent reliable year

    for (const orgId of organization_ids) {
      try {
        // Use the proxy API profile endpoint which contains financials
        const url = `${FQHC_API_BASE}/profile/by-bhcmis?year=${year}&bhcmis_id=${orgId}`;
        
        const response = await fetch(url);
        
        if (response.ok) {
          const data = await response.json();
          const fin = data.financials || {};
          
          // Only add if we found data
          if (data.bhcmis_id) {
            financialsData.push({
              organization_id: orgId,
              total_patients: parseInt(data.total_patients || (data.service_area_zip || []).reduce((acc, r) => acc + (parseInt(r.total_patients) || 0), 0)) || 0,
              total_revenue: parseFloat(fin.total_revenue) || 0,
              federal_grants: parseFloat(fin.federal_grants) || 0,
              medicaid_revenue: parseFloat(fin.medicaid_revenue) || 0,
              medicare_revenue: parseFloat(fin.medicare_revenue) || 0,
              total_operating_costs: parseFloat(fin.total_expenses) || 0,
              // Map available fields, fill missing with 0 or defaults
              medical_visits: 0, // Not available in simple profile
              dental_visits: 0,
              mental_health_visits: 0,
              services: {
                medical: true, // Assume basic services
                dental: true,
                mental_health: true,
                substance_abuse: false,
                vision: false,
                pharmacy: false
              },
              year: data.uds_year || year,
              source: 'FQHC API'
            });
          }
        }
        
        // Simple rate limiting
        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (err) {
        console.warn(`Failed to fetch financials for ${orgId}:`, err);
      }
    }

    return Response.json({
      success: true,
      financials: financialsData,
      count: financialsData.length
    });

  } catch (error) {
    console.error('Error in getHrsaFinancials:', error);
    return Response.json({ 
      success: false, 
      error: error.message 
    }, { status: 500 });
  }
});