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
        const { year, name } = body;

        if (!name) {
            return Response.json({ 
                error: 'Missing required param: name' 
            }, { status: 400 });
        }

        const udsYear = Number(year) || 2023;

        const bigquery = getBigQueryClient();

        // Query the table function for service area by name search
        const query = `
            SELECT *
            FROM \`health-needs-assessment.HRSA_FQHC.fn_service_area_by_name\`(
                @uds_year,
                @name_search
            )
        `;

        const options = {
            query,
            params: {
                uds_year: udsYear,
                name_search: name.toLowerCase(),
            },
        };

        const [rows] = await bigquery.query(options);

        if (!rows || rows.length === 0) {
            return Response.json({
                success: true,
                uds_year: udsYear,
                name_search: name,
                message: 'No FQHC found matching that name',
                rows: []
            });
        }

        // Extract FQHC info from first row
        const fqhcInfo = {
            bhcmis_id: rows[0].bhcmis_id,
            grant_number: rows[0].grant_number,
            health_center_name: rows[0].health_center_name
        };

        // Build zip_patients map
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
            name_search: name,
            fqhc: fqhcInfo,
            zip_count: Object.keys(zipPatients).length,
            totals,
            zip_patients: zipPatients,
            rows
        });

    } catch (error) {
        console.error('Error in fqhcServiceAreaByName:', error);
        return Response.json({ 
            error: 'Failed to fetch service area',
            details: error.message 
        }, { status: 500 });
    }
});