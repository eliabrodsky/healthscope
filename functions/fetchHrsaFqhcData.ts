import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const FQHC_API_BASE = 'https://fqhc-api-1038555279570.us-south1.run.app';

async function fetchFqhcEndpoint(endpoint, params) {
    const url = new URL(`${FQHC_API_BASE}${endpoint}`);
    Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
            url.searchParams.append(key, String(value));
        }
    });
    
    console.log('Fetching:', url.toString());
    
    const response = await fetch(url.toString(), {
        headers: { 'Accept': 'application/json' }
    });
    
    if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
    }
    
    return response.json();
}

// Parse numeric values that may be null or string
function parseNum(val) {
    if (val === null || val === undefined || val === '') return 0;
    const num = Number(val);
    return isNaN(num) ? 0 : num;
}

// Geocode an address using Nominatim
async function geocodeAddress(address, city, state, zip) {
    try {
        const query = `${address || ''}, ${city}, ${state} ${zip || ''}`.replace(/\s+/g, ' ').trim();
        const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1&countrycodes=us`;
        const response = await fetch(url, {
            headers: { 'User-Agent': 'HealthScope/1.0' }
        });
        if (response.ok) {
            const data = await response.json();
            if (data && data.length > 0) {
                return {
                    latitude: parseFloat(data[0].lat),
                    longitude: parseFloat(data[0].lon)
                };
            }
        }
    } catch (e) {
        console.log(`Geocode failed for ${city}, ${state}: ${e.message}`);
    }
    return null;
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
        const { assessmentId, state, zipCodes, udsYear, years } = body;

        if (!state) {
            return Response.json({ error: 'State is required' }, { status: 400 });
        }

        // Support multiple years (2021-2024)
        const yearsToFetch = years || [udsYear || 2023];
        const year = yearsToFetch[0];
        const fqhcProfiles = [];

        console.log(`Fetching FQHCs for state: ${state}, year: ${year}, assessment: ${assessmentId || 'none'}`);

        const STATE_TO_ABBR = {
            "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
            "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
            "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
            "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
            "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
            "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
            "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
            "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
            "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
            "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
            "District of Columbia": "DC", "Puerto Rico": "PR"
        };

        let searchState = state;
        if (searchState && searchState.length > 2) {
            // Try to title case it just in case: "arkansas" -> "Arkansas"
            const formatted = searchState.charAt(0).toUpperCase() + searchState.slice(1).toLowerCase();
            const entry = Object.entries(STATE_TO_ABBR).find(([k, v]) => k.toLowerCase() === formatted.toLowerCase());
            if (entry) {
                searchState = entry[1];
                console.log(`Converted state "${state}" to abbreviation "${searchState}"`);
            } else {
                console.warn(`Could not find abbreviation for state: ${state}`);
            }
        } else if (searchState) {
            searchState = searchState.toUpperCase();
        }

        let assessmentZips = zipCodes || [];
        
        // Get assessment and its ZIPs if assessmentId provided
        if (assessmentId) {
            const assessment = await base44.asServiceRole.entities.Assessment.filter({ id: assessmentId }, null, 1);
            if (assessment.length > 0) {
                assessmentZips = assessment[0].geography?.zcta_codes || assessmentZips;
            }
        }
        
        console.log(`Assessment has ${assessmentZips.length} ZIPs`);

        let fqhcData = [];
        
        // 1. Priority: Search by ZIP codes (finds FQHCs with *sites* in the area, not just HQs)
        if (assessmentZips.length > 0) {
            console.log(`Searching for FQHC sites in ${assessmentZips.length} ZIP codes...`);
            const batchSize = 20; // Smaller batch for sites
            const uniqueBhcmis = new Set();
            
            for (let i = 0; i < assessmentZips.length; i += batchSize) {
                const batch = assessmentZips.slice(i, i + batchSize);
                try {
                    // Use /sites/by-zips to find any site located in these ZIPs
                    // This catches FQHCs that operate in the area but are HQ'd elsewhere
                    const sitesData = await fetchFqhcEndpoint('/sites/by-zips', {
                        year,
                        zips: batch.join(',')
                    });
                    
                    const rows = Array.isArray(sitesData) ? sitesData : (sitesData.rows || []);
                    
                    if (!rows.length && sitesData) {
                        console.log(`Batch ${i/batchSize + 1} returned object keys:`, Object.keys(sitesData));
                    }

                    if (rows.length > 0) {
                        console.log(`Found ${rows.length} sites in batch ${i/batchSize + 1}`);
                        rows.forEach(row => {
                            if (row.bhcmis_id) {
                                if (!uniqueBhcmis.has(row.bhcmis_id)) {
                                    uniqueBhcmis.add(row.bhcmis_id);
                                    fqhcData.push({
                                        bhcmis_id: row.bhcmis_id,
                                        grant_number: row.grant_number,
                                        health_center_name: row.health_center_name || row.site_name // Fallback
                                    });
                                }
                            }
                        });
                    }
                } catch (e) {
                    console.log(`Site search batch ${i}-${i+batch.length} failed: ${e.message}`);
                }
            }
            console.log(`Identified ${fqhcData.length} unique FQHCs from site locations`);
        }

        // 2. Search by State (always used if zipCodes are not provided or to supplement)
        if (searchState) {
            try {
                console.log(`Fetching state summary for ${searchState}...`);
                const summaryData = await fetchFqhcEndpoint('/summary/by-state', {
                    year,
                    state: searchState
                });
                
                console.log('Summary data type:', typeof summaryData);
                if (!Array.isArray(summaryData)) {
                    console.log('Summary data keys:', Object.keys(summaryData));
                }

                const rows = Array.isArray(summaryData) ? summaryData : (summaryData.top_fqhcs || summaryData.rows || summaryData.data || summaryData.result || []);
                console.log(`Found ${rows.length} rows in summary data`);
                
                let added = 0;
                rows.forEach(row => {
                    if (row.bhcmis_id && !fqhcData.find(f => f.bhcmis_id === row.bhcmis_id)) {
                        fqhcData.push(row);
                        added++;
                    }
                });
                console.log(`Added ${added} FQHCs from state summary. Total unique: ${fqhcData.length}`);
            } catch (e) {
                console.error(`Failed to fetch FQHC summary for state ${searchState}:`, e);
            }
        }
        
        // Get unique BHCMIS IDs from the financials data
        const bhcmisMap = new Map();
        for (const row of fqhcData) {
            if (row.bhcmis_id && !bhcmisMap.has(row.bhcmis_id)) {
                bhcmisMap.set(row.bhcmis_id, {
                    bhcmis_id: row.bhcmis_id,
                    grant_number: row.grant_number,
                    health_center_name: row.health_center_name
                });
            }
        }
        
        console.log(`Found ${bhcmisMap.size} unique FQHCs from search results`);
        
        // Create grantee objects
        const infoData = { rows: Array.from(bhcmisMap.values()) };
        
        if (!infoData.rows || infoData.rows.length === 0) {
            return Response.json({
                success: true,
                count: 0,
                message: `No FQHCs found in ${searchState || state} for ${year}`
            });
        }

        console.log(`Found ${infoData.rows.length} FQHCs to process`);

        // Process in parallel with concurrency limit
        const batchSize = 5;
        for (let i = 0; i < infoData.rows.length; i += batchSize) {
            const batch = infoData.rows.slice(i, i + batchSize);
            await Promise.all(batch.map(async (grantee) => {
                try {
                    // Check if profile already exists
                    const existing = await base44.asServiceRole.entities.FqhcProfile.filter({
                        bhcmis_id: grantee.bhcmis_id,
                        uds_year: year
                    }, null, 1);

                    if (existing.length > 0) {
                        const ex = existing[0];
                        // Check if we need to update missing data (coordinates, financials, sites)
                        const needsUpdate = !ex.coordinates || !ex.financials?.total_revenue || !ex.sites?.length;
                        
                        if (!needsUpdate) {
                            // Only update assessment_id if one is provided
                            if (assessmentId && ex.assessment_id !== assessmentId) {
                                await base44.asServiceRole.entities.FqhcProfile.update(ex.id, {
                                    assessment_id: assessmentId
                                });
                                ex.assessment_id = assessmentId;
                            }
                            fqhcProfiles.push(ex);
                            return;
                        }
                        
                        // Otherwise, continue to fetch missing data and update the record
                        console.log(`Updating ${ex.health_center_name} - missing: coords=${!ex.coordinates}, fin=${!ex.financials?.total_revenue}, sites=${!ex.sites?.length}`);
                    }

                    // Fetch profile details AND summary data (summary has workforce/financials)
                    const [profileData, summaryData] = await Promise.all([
                        fetchFqhcEndpoint('/profile/by-bhcmis', { year, bhcmis_id: grantee.bhcmis_id }).catch(() => null),
                        fetchFqhcEndpoint('/summary/by-bhcmis', { year, bhcmis_id: grantee.bhcmis_id }).catch(() => null)
                    ]);

                    if (!profileData && !summaryData) {
                        console.log(`No data for ${grantee.bhcmis_id}, skipping`);
                        return;
                    }

                    const healthCenterName = summaryData?.health_center_name || profileData?.health_center_name || grantee.health_center_name || 'Unknown';
                    const grantNumber = summaryData?.grant_number || profileData?.grant_number || grantee.grant_number;

                    // Get totals from summary or calculate from profile
                    const saRows = profileData?.service_area_zip || [];
                    const totalPatients = summaryData?.totals?.patients || saRows.reduce((sum, row) => sum + parseNum(row.total_patients), 0);

                    // Use summary payer_mix if available (more accurate)
                    const payerMix = summaryData?.payer_mix || {
                        medicaid: saRows.reduce((sum, row) => sum + parseNum(row.medicaid_chip_other_pub_patients), 0),
                        medicare: saRows.reduce((sum, row) => sum + parseNum(row.medicare_patients), 0),
                        private: saRows.reduce((sum, row) => sum + parseNum(row.private_patients), 0),
                        uninsured: saRows.reduce((sum, row) => sum + parseNum(row.none_uninsured_patients), 0)
                    };

                    const serviceAreaZips = saRows
                        .filter(row => row.zip_code && row.zip_code !== '-')
                        .map(row => ({
                            zip_code: row.zip_code,
                            total_patients: parseNum(row.total_patients),
                            medicaid: parseNum(row.medicaid_chip_other_pub_patients),
                            medicare: parseNum(row.medicare_patients),
                            private: parseNum(row.private_patients),
                            uninsured: parseNum(row.none_uninsured_patients)
                        }));

                    // Get sites from the profile data (from /profile/by-bhcmis response)
                    let sites = [];
                    const sitesFromProfile = profileData?.sites || [];
                    console.log(`Profile has ${sitesFromProfile.length} sites for ${grantee.bhcmis_id}`);
                    
                    // Process and geocode sites (limit to first 15 to avoid timeout)
                    const sitesToProcess = sitesFromProfile.slice(0, 15);
                    for (const site of sitesToProcess) {
                        if (!site.site_name) continue;
                        
                        let siteCoords = null;
                        const siteCity = site.city;
                        const siteState = site.state;
                        const siteAddress = site.street_address || '';
                        const siteZip = site.zip || '';
                        
                        if (siteCity && siteState && siteAddress) {
                            siteCoords = await geocodeAddress(siteAddress, siteCity, siteState, siteZip);
                            // Small delay to avoid rate limiting
                            await new Promise(r => setTimeout(r, 100));
                        }
                        
                        sites.push({
                            site_name: site.site_name,
                            site_type: site.site_type,
                            address: siteAddress,
                            city: siteCity,
                            state: siteState,
                            zip_code: siteZip,
                            location_setting: site.location_setting,
                            weekly_hours: parseNum(site.weekly_hours),
                            coordinates: siteCoords
                        });
                    }
                    
                    // Add remaining sites without geocoding if there are more
                    for (const site of sitesFromProfile.slice(15)) {
                        if (!site.site_name) continue;
                        sites.push({
                            site_name: site.site_name,
                            site_type: site.site_type,
                            address: site.street_address || '',
                            city: site.city,
                            state: site.state,
                            zip_code: site.zip || '',
                            location_setting: site.location_setting,
                            weekly_hours: parseNum(site.weekly_hours),
                            coordinates: null
                        });
                    }
                    console.log(`Processed ${sites.length} sites, geocoded ${sites.filter(s => s.coordinates).length} for ${healthCenterName}`);

                    // Get financials from summary data first (more reliable), then fall back to API
                    let financials = {
                        total_revenue: 0,
                        federal_grants: 0,
                        medicaid_revenue: 0,
                        medicare_revenue: 0,
                        self_pay_revenue: 0,
                        total_expenses: 0,
                        cost_per_patient: 0
                    };
                    
                    // Summary financial data is in summaryData.financial object with T8a_L1_Ca (total costs)
                    if (summaryData?.financial?.amount) {
                        financials.total_expenses = parseNum(summaryData.financial.amount);
                        // Estimate revenue as slightly higher than expenses (typical for FQHCs)
                        financials.total_revenue = financials.total_expenses * 1.05;
                        if (totalPatients > 0) {
                            financials.cost_per_patient = financials.total_expenses / totalPatients;
                        }
                        console.log(`Got financials from summary: expenses=${financials.total_expenses}`);
                    }
                    
                    // Always try to get more detailed financials from the API
                    try {
                        const finData = await fetchFqhcEndpoint('/financials/by-bhcmis', { year, bhcmis_id: grantee.bhcmis_id });
                        const finRows = Array.isArray(finData) ? finData : (finData?.rows || []);
                        
                        if (finRows.length > 0) {
                            console.log(`Found ${finRows.length} financial rows from API for ${grantee.bhcmis_id}`);
                            for (const row of finRows) {
                                const code = (row.metric_code || row.metric_label || '').toLowerCase();
                                const amount = parseNum(row.amount);
                                
                                // Revenue metrics
                                if (code === 't9d_l1_ca' || code === 't9d_l1_cb') {
                                    if (amount > financials.total_revenue) financials.total_revenue = amount;
                                }
                                if (code === 't9e_l1_ca') financials.federal_grants = amount;
                                if (code === 't9d_l3_cb') financials.medicaid_revenue = amount;
                                if (code === 't9d_l6_cb') financials.medicare_revenue = amount;
                                if (code === 't9d_l13_cb') financials.self_pay_revenue = amount;
                                // Expense metrics
                                if (code === 't8a_l1_ca' || code === 't8a_l1_cc') {
                                    if (amount > 0) financials.total_expenses = amount;
                                }
                            }
                            
                            if (totalPatients > 0 && financials.total_expenses > 0) {
                                financials.cost_per_patient = financials.total_expenses / totalPatients;
                            }
                        }
                    } catch (e) {
                        console.log(`Financials API failed for ${grantee.bhcmis_id}: ${e.message}`);
                    }
                    
                    console.log(`Final financials for ${healthCenterName}: revenue=${financials.total_revenue}, expenses=${financials.total_expenses}`);

                    // Demographics - check profile.demographics first
                    const demo = profileData?.demographics || {};
                    // Also calculate age distribution from totals if available
                    const demographics = {
                        age_0_17: parseNum(demo.age_0_17) || parseNum(demo.children),
                        age_18_64: parseNum(demo.age_18_64) || parseNum(demo.adults),
                        age_65_plus: parseNum(demo.age_65_plus) || parseNum(demo.seniors),
                        hispanic_latino: parseNum(demo.hispanic_latino),
                        non_hispanic_white: parseNum(demo.non_hispanic_white),
                        non_hispanic_black: parseNum(demo.non_hispanic_black),
                        asian: parseNum(demo.asian),
                        other_race: parseNum(demo.other_race),
                        homeless: parseNum(demo.homeless),
                        migrant: parseNum(demo.migrant),
                        agricultural_worker: parseNum(demo.agricultural_worker),
                        public_housing: parseNum(demo.public_housing)
                    };
                    console.log(`Demographics for ${healthCenterName}: age_0_17=${demographics.age_0_17}, homeless=${demographics.homeless}`);

                    // Use summary workforce data (summary has it as workforce.total_fte, workforce.physicians_fte, etc.)
                    const summaryWf = summaryData?.workforce || {};
                    const profileWf = profileData?.workforce || {};
                    const workforce = {
                        total_fte: parseNum(summaryWf.total_fte) || parseNum(profileWf.total_fte),
                        physician_fte: parseNum(summaryWf.physicians_fte) || parseNum(profileWf.physician_fte),
                        np_pa_fte: parseNum(summaryWf.np_pa_fte) || parseNum(profileWf.np_pa_fte),
                        dental_fte: parseNum(summaryWf.dental_fte) || parseNum(profileWf.dental_fte),
                        behavioral_health_fte: parseNum(summaryWf.behavioral_health_fte) || parseNum(profileWf.behavioral_health_fte),
                        enabling_services_fte: parseNum(summaryWf.enabling_services_fte) || parseNum(profileWf.enabling_services_fte)
                    };
                    console.log(`Workforce for ${healthCenterName}: total_fte=${workforce.total_fte}, from summary=${!!summaryWf.total_fte}`);

                    const qual = profileData?.quality || {};
                    const clinicalMetrics = {
                        htn_control_pct: parseNum(qual.htn_control_pct),
                        diabetes_poor_control_pct: parseNum(qual.diabetes_poor_control_pct),
                        colorectal_screening_pct: parseNum(qual.colorectal_screening_pct),
                        depression_screening_pct: parseNum(qual.depression_screening_pct),
                        childhood_immunization_pct: parseNum(qual.childhood_immunization_pct)
                    };

                    const granteeInfo = profileData?.grantee || {};
                    const primarySite = sites[0] || {};
                    
                    // Geocode the primary address
                    const addressToGeocode = granteeInfo.street_address || primarySite.address || '';
                    const cityToGeocode = granteeInfo.city || primarySite.city || summaryData?.state || '';
                    const stateToGeocode = profileData?.state || summaryData?.state || granteeInfo.state || state;
                    const zipToGeocode = granteeInfo.zip_code || primarySite.zip_code || '';
                    
                    let coordinates = null;
                    if (cityToGeocode && stateToGeocode) {
                        coordinates = await geocodeAddress(addressToGeocode, cityToGeocode, stateToGeocode, zipToGeocode);
                        if (coordinates) {
                            console.log(`Geocoded ${healthCenterName}: ${coordinates.latitude}, ${coordinates.longitude}`);
                        }
                        // Add small delay to avoid rate limiting
                        await new Promise(r => setTimeout(r, 200));
                    }
                
                    const profile = {
                        bhcmis_id: profileData.bhcmis_id || grantee.bhcmis_id,
                        grant_number: grantNumber,
                        uds_year: year,
                        health_center_name: healthCenterName,
                        street_address: addressToGeocode,
                        city: cityToGeocode,
                        state: stateToGeocode,
                        zip_code: zipToGeocode,
                        urban_rural_flag: profileData.urban_rural_flag,
                        funding_types: [
                            granteeInfo.funding_chc && 'CHC',
                            granteeInfo.funding_mhc && 'MHC',
                            granteeInfo.funding_ho && 'HO',
                            granteeInfo.funding_ph && 'PH'
                        ].filter(Boolean),
                        coordinates: coordinates,
                        total_patients: totalPatients,
                        payer_mix: payerMix,
                        service_area_zips: serviceAreaZips,
                        sites: sites,
                        workforce: workforce,
                        demographics: demographics,
                        clinical_metrics: clinicalMetrics,
                        financials: financials,
                        assessment_id: assessmentId
                    };

                    // Check if we're updating an existing record
                    const existingCheck = await base44.asServiceRole.entities.FqhcProfile.filter({
                        bhcmis_id: grantee.bhcmis_id,
                        uds_year: year
                    }, null, 1);
                    
                    if (existingCheck.length > 0) {
                        // Update existing record
                        const updated = await base44.asServiceRole.entities.FqhcProfile.update(existingCheck[0].id, profile);
                        fqhcProfiles.push(updated);
                        console.log(`Updated ${healthCenterName}`);
                    } else {
                        const created = await base44.asServiceRole.entities.FqhcProfile.create(profile);
                        fqhcProfiles.push(created);
                        console.log(`Created ${healthCenterName}`);
                    }

                } catch (err) {
                    console.warn(`Failed to process ${grantee.bhcmis_id}:`, err.message);
                }
            }));
        }

        return Response.json({
            success: true,
            count: fqhcProfiles.length,
            message: `Loaded ${fqhcProfiles.length} FQHC profiles for ${searchState || state}`
        });

    } catch (error) {
        console.error('Error fetching HRSA FQHC data:', error);
        return Response.json({ error: error.message }, { status: 500 });
    }
});