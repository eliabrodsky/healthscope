import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { clinics } = await req.json();
    
    if (!clinics || !Array.isArray(clinics)) {
      return Response.json({ error: 'Clinics array required' }, { status: 400 });
    }

    const enrichedData = [];

    for (const clinic of clinics) {
      try {
        // Use AI with web search to find clinic information
        const searchPrompt = `Find information about ${clinic.name} clinic in ${clinic.city}, ${clinic.state}. 
Look for: patient volume, services offered (primary care, urgent care, specialty services), 
operating hours, insurance accepted, and any available patient satisfaction data.`;

        const response = await base44.integrations.Core.InvokeLLM({
          prompt: searchPrompt,
          add_context_from_internet: true,
          response_json_schema: {
            type: "object",
            properties: {
              estimated_annual_patients: { type: "number" },
              services: { type: "array", items: { type: "string" } },
              is_urgent_care: { type: "boolean" },
              accepts_medicaid: { type: "boolean" },
              accepts_medicare: { type: "boolean" },
              hours_per_week: { type: "number" },
              specialties: { type: "array", items: { type: "string" } }
            }
          }
        });

        enrichedData.push({
          organization_id: clinic.id,
          name: clinic.name,
          type: 'clinic',
          estimated_annual_patients: response.estimated_annual_patients || clinic.total_patients || 0,
          services: response.services || ['Primary Care'],
          is_urgent_care: response.is_urgent_care || false,
          insurance: {
            medicaid: response.accepts_medicaid || false,
            medicare: response.accepts_medicare || false
          },
          hours_per_week: response.hours_per_week || 40,
          specialties: response.specialties || [],
          source: 'Web Search + AI Analysis'
        });

        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (err) {
        console.warn(`Failed to enrich ${clinic.name}:`, err);
        enrichedData.push({
          organization_id: clinic.id,
          name: clinic.name,
          type: 'clinic',
          estimated_annual_patients: clinic.total_patients || 0,
          services: ['Primary Care'],
          source: 'Basic Data'
        });
      }
    }

    return Response.json({
      success: true,
      enriched: enrichedData,
      count: enrichedData.length
    });

  } catch (error) {
    console.error('Error in enrichClinicData:', error);
    return Response.json({ 
      success: false,
      error: error.message 
    }, { status: 500 });
  }
});