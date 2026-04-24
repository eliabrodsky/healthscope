import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const FQHC_API_BASE = 'https://fqhc-api-1038555279570.us-south1.run.app';

async function fetchFqhcEndpoint(endpoint, params) {
    const url = new URL(`${FQHC_API_BASE}${endpoint}`);
    Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
            url.searchParams.append(key, String(value));
        }
    });
    
    console.log('Fetching FQHC API:', url.toString());
    
    const response = await fetch(url.toString(), {
        headers: { 'Accept': 'application/json' }
    });
    
    if (!response.ok) {
        const text = await response.text();
        console.error('FQHC API error:', response.status, text);
        throw new Error(`FQHC API returned ${response.status}`);
    }
    
    return response.json();
}

// Aggregate service area rows by BHCMIS ID
function aggregateByBhcmisId(rows) {
    const grouped = {};
    for (const row of rows) {
        const id = row.bhcmis_id;
        if (!grouped[id]) {
            grouped[id] = {
                bhcmis_id: id,
                grant_number: row.grant_number,
                health_center_name: row.health_center_name,
                rows: []
            };
        }
        grouped[id].rows.push(row);
    }
    return Object.values(grouped);
}

// Parse numeric values that may be null or string
function parseNum(val) {
    if (val === null || val === undefined || val === '') return 0;
    const num = Number(val);
    return isNaN(num) ? 0 : num;
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
                    const { query, udsYear, years } = body;

                    if (!query) {
                        return Response.json({ error: 'Search query required' }, { status: 400 });
                    }

                    // Support multiple years or single year - default to 2024
                    const yearsToSearch = years || [udsYear || 2024];
                    const year = yearsToSearch[0]; // Primary year for initial search
        const searchTerm = query.trim();

        // Determine search type
        const isZipCode = /^\d{5}$/.test(searchTerm);
        const isGrantNumber = /^H\d{2}/i.test(searchTerm);

        let fqhcs = [];

        try {
                        // The API uses /service-area/by-name for name search which returns service area data with FQHC info
                        // API returns array directly, not {rows: []}
                        const serviceAreaResponse = await fetchFqhcEndpoint('/service-area/by-name', { 
                            year, 
                            name: searchTerm 
                        });

                        // Handle both array response and {rows: []} response
                        const serviceAreaRows = Array.isArray(serviceAreaResponse) ? serviceAreaResponse : (serviceAreaResponse.rows || []);

                        console.log('Service area search results:', serviceAreaRows.length, 'rows');

                        if (serviceAreaRows.length > 0) {
                            // Group by BHCMIS ID to get unique FQHCs
                            const grouped = aggregateByBhcmisId(serviceAreaRows);
                console.log('Unique FQHCs found:', grouped.length);
                
                for (const group of grouped.slice(0, 10)) {
                    const firstRow = group.rows[0];
                    
                    // Calculate totals from all ZIP code rows - use parseNum to handle null/undefined
                    const totalPatients = group.rows.reduce((sum, r) => sum + parseNum(r.total_patients), 0);
                    const payerMix = {
                        medicaid: group.rows.reduce((sum, r) => sum + parseNum(r.medicaid_chip_other_pub_patients), 0),
                        medicare: group.rows.reduce((sum, r) => sum + parseNum(r.medicare_patients), 0),
                        private: group.rows.reduce((sum, r) => sum + parseNum(r.private_patients), 0),
                        uninsured: group.rows.reduce((sum, r) => sum + parseNum(r.none_uninsured_patients), 0)
                    };
                    
                    console.log(`FQHC ${group.health_center_name}: ${totalPatients} patients from ${group.rows.length} ZIP codes`);
                    
                    const serviceAreaZips = group.rows
                        .filter(r => r.zip_code && r.zip_code !== '-')
                        .map(r => ({
                            zip_code: r.zip_code,
                            total_patients: parseNum(r.total_patients),
                            medicaid: parseNum(r.medicaid_chip_other_pub_patients),
                            medicare: parseNum(r.medicare_patients),
                            private: parseNum(r.private_patients),
                            uninsured: parseNum(r.none_uninsured_patients)
                        }))
                        .sort((a, b) => b.total_patients - a.total_patients);

                    // Use the new consolidated profile endpoint to get all data
                    try {
                        const profileData = await fetchFqhcEndpoint('/profile/by-bhcmis', { year, bhcmis_id: group.bhcmis_id });

                        if (profileData) {
                            // Process service area from profile
                            const saRows = profileData.service_area_zip || [];
                            const totalPatients = saRows.reduce((sum, r) => sum + parseNum(r.total_patients), 0);

                            const payerMix = {
                                medicaid: saRows.reduce((sum, r) => sum + parseNum(r.medicaid_chip_other_pub_patients), 0),
                                medicare: saRows.reduce((sum, r) => sum + parseNum(r.medicare_patients), 0),
                                private: saRows.reduce((sum, r) => sum + parseNum(r.private_patients), 0),
                                uninsured: saRows.reduce((sum, r) => sum + parseNum(r.none_uninsured_patients), 0)
                            };

                            const serviceAreaZips = saRows
                                .filter(r => r.zip_code && r.zip_code !== '-')
                                .map(r => ({
                                    zip_code: r.zip_code,
                                    total_patients: parseNum(r.total_patients),
                                    medicaid: parseNum(r.medicaid_chip_other_pub_patients),
                                    medicare: parseNum(r.medicare_patients),
                                    private: parseNum(r.private_patients),
                                    uninsured: parseNum(r.none_uninsured_patients)
                                }))
                                .sort((a, b) => b.total_patients - a.total_patients);

                            // Process financials
                            const fin = profileData.financials || {};
                            const financials = {
                                total_revenue: parseNum(fin.total_revenue),
                                federal_grants: parseNum(fin.federal_grants),
                                medicaid_revenue: parseNum(fin.medicaid_revenue),
                                medicare_revenue: parseNum(fin.medicare_revenue),
                                total_expenses: parseNum(fin.total_expenses),
                                cost_per_patient: totalPatients > 0 ? parseNum(fin.total_expenses) / totalPatients : 0
                            };

                            // Process sites
                            const sites = (profileData.sites || []).map(site => ({
                                site_name: site.site_name,
                                site_type: site.site_type,
                                address: site.site_street_address,
                                city: site.site_city,
                                state: site.site_state,
                                zip_code: site.site_zip_code,
                                weekly_hours: site.total_weekly_hours_of_operation
                            }));

                            // Process demographics
                            const demo = profileData.demographics || {};
                            const demographics = {
                                age_0_17: parseNum(demo.age_0_17),
                                age_18_64: parseNum(demo.age_18_64),
                                age_65_plus: parseNum(demo.age_65_plus),
                                homeless: parseNum(demo.homeless),
                                migrant: parseNum(demo.migrant)
                            };

                            // Process workforce
                            const wf = profileData.workforce || {};
                            const workforce = {
                                total_fte: parseNum(wf.total_fte),
                                physician_fte: parseNum(wf.physician_fte),
                                np_pa_fte: parseNum(wf.np_pa_fte),
                                dental_fte: parseNum(wf.dental_fte),
                                behavioral_health_fte: parseNum(wf.behavioral_health_fte)
                            };

                            // Process clinical quality
                            const qual = profileData.quality || {};
                            const clinicalMetrics = {
                                htn_control_pct: parseNum(qual.htn_control_pct),
                                diabetes_poor_control_pct: parseNum(qual.diabetes_poor_control_pct),
                                colorectal_screening_pct: parseNum(qual.colorectal_screening_pct),
                                depression_screening_pct: parseNum(qual.depression_screening_pct)
                            };

                            fqhcs.push({
                                bhcmis_id: profileData.bhcmis_id,
                                grant_number: profileData.grant_number,
                                health_center_name: profileData.health_center_name,
                                uds_year: profileData.uds_year,
                                state: profileData.state,
                                urban_rural_flag: profileData.urban_rural_flag,
                                street_address: profileData.grantee?.street_address,
                                city: profileData.grantee?.city,
                                zip_code: profileData.grantee?.zip_code,
                                total_patients: totalPatients,
                                payer_mix: payerMix,
                                service_area_zips: serviceAreaZips,
                                sites: sites,
                                demographics: demographics,
                                workforce: workforce,
                                clinical_metrics: clinicalMetrics,
                                financials: financials
                            });
                        }
                    } catch (profileErr) {
                        console.warn(`Failed to get profile for ${group.bhcmis_id}:`, profileErr.message);
                    }
                    }
                    }
                    } catch (apiError) {
                    console.error('FQHC API search failed:', apiError);
                    return Response.json({ 
                    success: false, 
                    error: apiError.message,
                    fqhcs: [] 
                    });
                    }

        return Response.json({
            success: true,
            fqhcs: fqhcs,
            count: fqhcs.length
        });

    } catch (error) {
        console.error('Error searching FQHCs:', error);
        return Response.json({ 
            error: error.message,
            success: false,
            fqhcs: []
        }, { status: 500 });
    }
});