import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';
import { BigQuery } from 'npm:@google-cloud/bigquery@7.3.0';

// Initialize BigQuery client with credentials from secrets
function getBigQueryClient() {
    const projectId = Deno.env.get('GCP_PROJECT_ID') || 'health-needs-assessment';
    const credsJson = Deno.env.get('GOOGLE_APPLICATION_CREDENTIALS_JSON');
    
    if (!credsJson) {
        throw new Error('GOOGLE_APPLICATION_CREDENTIALS_JSON is not set');
    }
    
    const credentials = JSON.parse(credsJson);
    
    return new BigQuery({
        projectId,
        credentials,
    });
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
        const { year, bhcmis_id } = body;

        if (!year || !bhcmis_id) {
            return Response.json({ 
                error: 'Missing required params: year, bhcmis_id' 
            }, { status: 400 });
        }

        const udsYear = Number(year);
        if (!Number.isFinite(udsYear)) {
            return Response.json({ 
                error: 'year must be a number (e.g. 2023)' 
            }, { status: 400 });
        }

        const bigquery = getBigQueryClient();

        // Query the table function for service area by BHCMIS ID
        const query = `
            SELECT *
            FROM \`health-needs-assessment.HRSA_FQHC.fn_service_area_by_bhcmis\`(
                @uds_year,
                @bhcmis_id
            )
        `;

        const options = {
            query,
            params: {
                uds_year: udsYear,
                bhcmis_id: bhcmis_id,
            },
        };

        const [rows] = await bigquery.query(options);

        // Build zip_patients map for easy consumption
        const zipPatients = {};
        for (const row of rows) {
            if (row.zip_code && row.zip_code !== '-') {
                zipPatients[row.zip_code] = {
                    total_patients: row.total_patients || 0,
                    medicaid: row.medicaid_chip_other_pub_patients || 0,
                    medicare: row.medicare_patients || 0,
                    private: row.private_patients || 0,
                    uninsured: row.none_uninsured_patients || 0
                };
            }
        }

        // Calculate totals
        const totals = rows.reduce((acc, row) => ({
            total_patients: acc.total_patients + (row.total_patients || 0),
            medicaid: acc.medicaid + (row.medicaid_chip_other_pub_patients || 0),
            medicare: acc.medicare + (row.medicare_patients || 0),
            private: acc.private + (row.private_patients || 0),
            uninsured: acc.uninsured + (row.none_uninsured_patients || 0)
        }), { total_patients: 0, medicaid: 0, medicare: 0, private: 0, uninsured: 0 });

        return Response.json({
            success: true,
            uds_year: udsYear,
            bhcmis_id: bhcmis_id,
            grant_number: rows[0]?.grant_number || null,
            zip_count: Object.keys(zipPatients).length,
            totals,
            zip_patients: zipPatients,
            rows // Full detail if needed
        });

    } catch (error) {
        console.error('Error in fqhcServiceArea:', error);
        return Response.json({ 
            error: 'Failed to fetch service area',
            details: error.message 
        }, { status: 500 });
    }
});