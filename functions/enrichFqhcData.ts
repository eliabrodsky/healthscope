import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const FQHC_API_BASE = 'https://fqhc-api-1038555279570.us-south1.run.app';

async function fetchFqhcEndpoint(endpoint, params) {
    const url = new URL(`${FQHC_API_BASE}${endpoint}`);
    Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
            url.searchParams.append(key, value);
        }
    });
    
    const response = await fetch(url.toString(), {
        headers: { 'Accept': 'application/json' }
    });
    
    if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
    }
    
    return response.json();
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { fqhcs, year = 2023, years } = await req.json();
        
        // Support multiple years (2021-2024)
        const yearsToFetch = years || [year];
        const primaryYear = yearsToFetch[0];
        
        if (!fqhcs || !Array.isArray(fqhcs)) {
            return Response.json({ error: 'FQHCs array required' }, { status: 400 });
        }

        const enrichedData = [];
        const errors = [];

        for (const fqhc of fqhcs) {
            try {
                // Extract search name from the FQHC name
                const nameParts = fqhc.name.split(/\s+/).filter(p => 
                    p.length > 2 && 
                    !['the', 'and', 'health', 'center', 'centers', 'community', 'inc', 'inc.', 'clinic', 'clinics', 'medical'].includes(p.toLowerCase())
                );
                const searchName = nameParts[0] || fqhc.name.split(' ')[0];

                // 1. Get basic info and find BHCMIS ID (use primary year)
                const infoData = await fetchFqhcEndpoint('/fqhc/info/by-name', { year: primaryYear, name: searchName });
                
                if (!infoData || !infoData.rows || infoData.rows.length === 0) {
                    throw new Error('FQHC not found in database');
                }

                // Find best matching FQHC
                let bestMatch = infoData.rows[0];
                for (const row of infoData.rows) {
                    const rowName = row.health_center_name?.toLowerCase() || '';
                    const searchLower = fqhc.name.toLowerCase();
                    if (rowName.includes(searchLower) || searchLower.includes(rowName.split(' ')[0])) {
                        bestMatch = row;
                        break;
                    }
                }

                const bhcmisId = bestMatch.bhcmis_id;
                
                // 2. Fetch all data using consolidated profile endpoint
                const profileData = await fetchFqhcEndpoint('/profile/by-bhcmis', { year: primaryYear, bhcmis_id: bhcmisId }).catch(() => null);

                if (!profileData) {
                    throw new Error('Failed to load profile data');
                }

                // Process service area data from profile
                let totalPatients = 0;
                let uninsuredPatients = 0;
                let medicaidPatients = 0;
                let medicarePatients = 0;
                let privatePatients = 0;
                const serviceAreaZips = [];

                const saRows = profileData.service_area_zip || [];
                if (saRows.length > 0) {
                    for (const row of saRows) {
                        totalPatients += Number(row.total_patients) || 0;
                        uninsuredPatients += Number(row.none_uninsured_patients) || 0;
                        medicaidPatients += Number(row.medicaid_chip_other_pub_patients) || 0;
                        medicarePatients += Number(row.medicare_patients) || 0;
                        privatePatients += Number(row.private_patients) || 0;

                        if (row.zip_code && row.zip_code !== '-') {
                            serviceAreaZips.push({
                                zip_code: row.zip_code,
                                patients: Number(row.total_patients) || 0,
                                medicaid: Number(row.medicaid_chip_other_pub_patients) || 0,
                                medicare: Number(row.medicare_patients) || 0,
                                uninsured: Number(row.none_uninsured_patients) || 0,
                                private: Number(row.private_patients) || 0
                            });
                        }
                    }
                    serviceAreaZips.sort((a, b) => b.patients - a.patients);
                }

                // Process sites data
                const sites = (profileData.sites || []).map(site => ({
                    site_name: site.site_name,
                    site_type: site.site_type,
                    site_status: site.site_status,
                    address: site.site_street_address,
                    city: site.site_city,
                    state: site.site_state,
                    zip_code: site.site_zip_code,
                    weekly_hours: site.total_weekly_hours_of_operation,
                    location_type: site.location_type,
                    location_setting: site.location_setting
                }));

                // Process financials
                const fin = profileData.financials || {};
                const financials = {
                    total_revenue: Number(fin.total_revenue) || 0,
                    federal_grants: Number(fin.federal_grants) || 0,
                    medicaid_revenue: Number(fin.medicaid_revenue) || 0,
                    medicare_revenue: Number(fin.medicare_revenue) || 0,
                    self_pay_revenue: Number(fin.self_pay_revenue) || 0,
                    total_expenses: Number(fin.total_expenses) || 0,
                    cost_per_patient: Number(fin.cost_per_patient) || 0
                };

                // Process demographics
                const demo = profileData.demographics || {};
                const demographics = {
                    age_0_17: Number(demo.age_0_17) || 0,
                    age_18_64: Number(demo.age_18_64) || 0,
                    age_65_plus: Number(demo.age_65_plus) || 0,
                    hispanic_latino: Number(demo.hispanic_latino) || 0,
                    non_hispanic_white: Number(demo.non_hispanic_white) || 0,
                    non_hispanic_black: Number(demo.non_hispanic_black) || 0,
                    asian: Number(demo.asian) || 0,
                    other_race: Number(demo.other_race) || 0
                };

                // Process workforce
                const wf = profileData.workforce || {};
                const workforce = {
                    total_fte: Number(wf.total_fte) || 0,
                    physician_fte: Number(wf.physician_fte) || 0,
                    np_pa_fte: Number(wf.np_pa_fte) || 0,
                    dental_fte: Number(wf.dental_fte) || 0,
                    behavioral_health_fte: Number(wf.behavioral_health_fte) || 0,
                    enabling_services_fte: Number(wf.enabling_services_fte) || 0
                };

                // Process clinical quality
                const qual = profileData.quality || {};
                const clinicalMetrics = {
                    htn_control_pct: Number(qual.htn_control_pct) || 0,
                    diabetes_poor_control_pct: Number(qual.diabetes_poor_control_pct) || 0,
                    colorectal_screening_pct: Number(qual.colorectal_screening_pct) || 0,
                    depression_screening_pct: Number(qual.depression_screening_pct) || 0,
                    childhood_immunization_pct: Number(qual.childhood_immunization_pct) || 0
                };

                // Validate data
                const isValidData = totalPatients > 0;

                if (!isValidData) {
                    errors.push({
                        organization_id: fqhc.id,
                        name: fqhc.name,
                        error: 'No patient data found',
                        message: `FQHC found but no patient volume data for ${year}`
                    });
                }

                enrichedData.push({
                    organization_id: fqhc.id,
                    name: fqhc.name,
                    type: 'fqhc',
                    bhcmis_id: bhcmisId,
                    grant_number: bestMatch.grant_number,
                    health_center_name: bestMatch.health_center_name,
                    street_address: bestMatch.street_address,
                    city: bestMatch.city,
                    state: bestMatch.state,
                    zip_code: bestMatch.zip_code,
                    urban_rural_flag: bestMatch.urban_rural_flag,
                    
                    // Patient volumes
                    total_patients: totalPatients,
                    uninsured_patients: uninsuredPatients,
                    medicaid_patients: medicaidPatients,
                    medicare_patients: medicarePatients,
                    private_patients: privatePatients,
                    
                    payer_mix: {
                        medicaid: medicaidPatients,
                        medicare: medicarePatients,
                        private: privatePatients,
                        uninsured: uninsuredPatients
                    },
                    
                    // Service area
                    service_area_zips: serviceAreaZips,
                    service_area_count: serviceAreaZips.length,
                    
                    // Sites
                    sites: sites,
                    total_sites: sites.length,
                    
                    // Financials
                    financials: financials,
                    total_revenue: financials?.total_revenue || 0,
                    
                    // Demographics
                    demographics: demographics,
                    
                    // Workforce
                    workforce: workforce,
                    
                    // Clinical quality
                    clinical_metrics: clinicalMetrics,
                    
                    uds_year: primaryYear,
                    source: 'FQHC API (UDS)',
                    is_valid: isValidData,
                    validation_message: isValidData ? null : 'No patient volume data available',
                    years_available: yearsToFetch
                });

                // Rate limiting
                await new Promise(resolve => setTimeout(resolve, 200));

            } catch (err) {
                console.warn(`Failed to enrich ${fqhc.name}:`, err);
                errors.push({
                    organization_id: fqhc.id,
                    name: fqhc.name,
                    error: err.message
                });

                enrichedData.push({
                    organization_id: fqhc.id,
                    name: fqhc.name,
                    type: 'fqhc',
                    total_patients: 0,
                    source: 'Error',
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
                ? `${invalidCount} FQHC(s) have missing data from UDS ${primaryYear}` 
                : undefined,
            years_searched: yearsToFetch
        });

    } catch (error) {
        console.error('Error in enrichFqhcData:', error);
        return Response.json({ 
            success: false,
            error: error.message 
        }, { status: 500 });
    }
});