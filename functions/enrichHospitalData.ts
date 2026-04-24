import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

// HealthData.gov Hospital API endpoint
const HEALTHDATA_GOV_API = 'https://healthdata.gov/api/v3/views/ieks-f4qs/query.json';

async function fetchHospitalsFromHealthDataGov(state) {
    // Build query for hospitals in the state
    const query = `SELECT * WHERE provider_state = '${state.toUpperCase()}' AND (provider_subtype IS NULL OR provider_subtype = 'Short Term' OR provider_subtype = 'Critical Access Hospitals' OR provider_subtype = 'Psychiatric' OR provider_subtype = 'Long Term' OR provider_subtype = 'Rehabilitation' OR provider_subtype = 'Childrens Hospitals')`;
    
    const url = `${HEALTHDATA_GOV_API}?query=${encodeURIComponent(query)}`;
    
    const response = await fetch(url, {
        headers: { 'Accept': 'application/json' }
    });
    
    if (!response.ok) {
        console.warn(`HealthData.gov API returned ${response.status}`);
        return [];
    }
    
    const data = await response.json();
    return data || [];
}

function normalizeHospitalName(name) {
    return name.toLowerCase()
        .replace(/[^\w\s]/g, '')
        .replace(/\s+/g, ' ')
        .replace(/\b(hospital|medical|center|health|system|inc|llc|the)\b/g, '')
        .trim();
}

function findBestMatch(hospitalName, hospitalCity, apiHospitals) {
    const normalizedSearch = normalizeHospitalName(hospitalName);
    const searchWords = normalizedSearch.split(' ').filter(w => w.length > 2);
    
    let bestMatch = null;
    let bestScore = 0;
    
    for (const apiHospital of apiHospitals) {
        const apiName = normalizeHospitalName(apiHospital.provider_name || '');
        const apiCity = (apiHospital.provider_city || '').toLowerCase();
        
        // Calculate word match score
        let score = 0;
        for (const word of searchWords) {
            if (apiName.includes(word)) {
                score += word.length;
            }
        }
        
        // Bonus for city match
        if (hospitalCity && apiCity.includes(hospitalCity.toLowerCase())) {
            score += 10;
        }
        
        if (score > bestScore) {
            bestScore = score;
            bestMatch = apiHospital;
        }
    }
    
    // Require minimum match quality
    return bestScore >= 5 ? bestMatch : null;
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { hospitals, assessmentId } = await req.json();
        
        if (!hospitals || !Array.isArray(hospitals)) {
            return Response.json({ error: 'Hospitals array required' }, { status: 400 });
        }

        const enrichedData = [];
        const errors = [];

        // Group hospitals by state to minimize API calls
        const hospitalsByState = {};
        for (const hospital of hospitals) {
            const state = hospital.state?.toUpperCase() || '';
            if (!hospitalsByState[state]) {
                hospitalsByState[state] = [];
            }
            hospitalsByState[state].push(hospital);
        }

        // Fetch API data for each state
        const stateApiData = {};
        for (const state of Object.keys(hospitalsByState)) {
            if (state) {
                try {
                    console.log(`Fetching HealthData.gov data for state: ${state}`);
                    stateApiData[state] = await fetchHospitalsFromHealthDataGov(state);
                    console.log(`Found ${stateApiData[state].length} hospitals in ${state}`);
                } catch (err) {
                    console.warn(`Failed to fetch data for ${state}:`, err.message);
                    stateApiData[state] = [];
                }
            }
        }

        for (const hospital of hospitals) {
            try {
                const state = hospital.state?.toUpperCase() || '';
                const apiHospitals = stateApiData[state] || [];
                
                // Find matching hospital in API data
                const match = findBestMatch(hospital.name, hospital.city, apiHospitals);
                
                let hospitalData = null;

                if (match) {
                    // Extract data from HealthData.gov response
                    const totalDischarges = parseInt(match.total_discharges) || 0;
                    const avgCoveredCharges = parseFloat(match.average_covered_charges) || 0;
                    const avgTotalPayments = parseFloat(match.average_total_payments) || 0;
                    const avgMedicarePayments = parseFloat(match.average_medicare_payments) || 0;
                    
                    // Estimate annual patients (Medicare discharges * multiplier for total patient population)
                    // Medicare typically represents ~30-40% of hospital patients
                    const estimatedAnnualPatients = Math.round(totalDischarges * 2.8);
                    const estimatedRevenue = Math.round(totalDischarges * avgTotalPayments);

                    hospitalData = {
                        provider_id: match.provider_id,
                        provider_name: match.provider_name,
                        provider_subtype: match.provider_subtype || 'General',
                        annual_patients: estimatedAnnualPatients,
                        total_discharges: totalDischarges,
                        avg_covered_charges: Math.round(avgCoveredCharges),
                        avg_total_payments: Math.round(avgTotalPayments),
                        avg_medicare_payments: Math.round(avgMedicarePayments),
                        revenue: estimatedRevenue,
                        drg_definition: match.drg_definition,
                        source: 'HealthData.gov CMS Medicare'
                    };
                }

                // Fallback to web search if no API match
                if (!hospitalData || hospitalData.annual_patients <= 0) {
                    console.log(`No API match for ${hospital.name}, using web search fallback`);
                    
                    const searchPrompt = `Search for statistics about "${hospital.name}" hospital in ${hospital.city}, ${hospital.state}.
                    
Find:
1. Number of licensed beds
2. Annual patient admissions (inpatient)
3. Annual emergency department visits
4. Annual outpatient visits
5. Total annual revenue if available

Return actual numbers from recent data. If not found, return 0.`;

                    const response = await base44.integrations.Core.InvokeLLM({
                        prompt: searchPrompt,
                        add_context_from_internet: true,
                        response_json_schema: {
                            type: "object",
                            properties: {
                                beds: { type: "number" },
                                annual_admissions: { type: "number" },
                                annual_ed_visits: { type: "number" },
                                annual_outpatient_visits: { type: "number" },
                                annual_revenue: { type: "number" },
                                data_year: { type: "string" }
                            }
                        }
                    });

                    const totalPatients = (response.annual_admissions || 0) + 
                                         (response.annual_ed_visits || 0) + 
                                         (response.annual_outpatient_visits || 0);

                    hospitalData = {
                        beds: response.beds || 0,
                        annual_patients: totalPatients > 0 ? totalPatients : (response.annual_admissions || 0),
                        annual_admissions: response.annual_admissions || 0,
                        annual_ed_visits: response.annual_ed_visits || 0,
                        annual_outpatient_visits: response.annual_outpatient_visits || 0,
                        revenue: response.annual_revenue || 0,
                        data_year: response.data_year || 'Unknown',
                        source: 'Web Search + AI'
                    };

                    await new Promise(resolve => setTimeout(resolve, 500));
                }

                const isValidData = hospitalData.annual_patients > 0;

                if (!isValidData) {
                    errors.push({
                        organization_id: hospital.id,
                        name: hospital.name,
                        error: 'No valid patient data found'
                    });
                }

                enrichedData.push({
                    organization_id: hospital.id,
                    name: hospital.name,
                    type: 'hospital',
                    provider_id: hospitalData.provider_id,
                    provider_subtype: hospitalData.provider_subtype,
                    beds: hospitalData.beds || 0,
                    annual_patients: Math.max(0, hospitalData.annual_patients || 0),
                    total_discharges: hospitalData.total_discharges || 0,
                    annual_admissions: hospitalData.annual_admissions || 0,
                    annual_ed_visits: hospitalData.annual_ed_visits || 0,
                    annual_outpatient_visits: hospitalData.annual_outpatient_visits || 0,
                    avg_covered_charges: hospitalData.avg_covered_charges || 0,
                    avg_total_payments: hospitalData.avg_total_payments || 0,
                    avg_medicare_payments: hospitalData.avg_medicare_payments || 0,
                    revenue: Math.max(0, hospitalData.revenue || 0),
                    services: ['General Medical', 'Emergency Care'],
                    source: hospitalData.source,
                    data_year: hospitalData.data_year,
                    is_valid: isValidData,
                    validation_message: isValidData ? null : 'Data may be incomplete'
                });

            } catch (err) {
                console.warn(`Failed to enrich ${hospital.name}:`, err);
                errors.push({
                    organization_id: hospital.id,
                    name: hospital.name,
                    error: err.message
                });
                
                enrichedData.push({
                    organization_id: hospital.id,
                    name: hospital.name,
                    type: 'hospital',
                    annual_patients: 0,
                    services: ['General Medical'],
                    source: 'Error - No Data',
                    is_valid: false,
                    validation_message: `Error: ${err.message}`
                });
            }
        }

        const validCount = enrichedData.filter(d => d.is_valid).length;
        const invalidCount = enrichedData.filter(d => !d.is_valid).length;

        return Response.json({
            success: true,
            enriched: enrichedData,
            count: enrichedData.length,
            valid_count: validCount,
            invalid_count: invalidCount,
            errors: errors.length > 0 ? errors : undefined,
            warning: invalidCount > 0 
                ? `${invalidCount} hospital(s) have missing data` 
                : undefined
        });

    } catch (error) {
        console.error('Error in enrichHospitalData:', error);
        return Response.json({ 
            success: false,
            error: error.message 
        }, { status: 500 });
    }
});