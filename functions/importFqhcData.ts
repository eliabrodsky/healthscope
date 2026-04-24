import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';
import { read, utils } from 'https://cdn.sheetjs.com/xlsx-0.20.1/package/xlsx.mjs';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    
    // Verify authentication
    const user = await base44.auth.me();
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { file_url, sheet_name } = await req.json();

    if (!file_url) {
      return Response.json({ error: 'file_url is required' }, { status: 400 });
    }

    // Fetch the Excel file
    const response = await fetch(file_url);
    if (!response.ok) {
      return Response.json({ error: `Failed to fetch file: ${response.statusText}` }, { status: 500 });
    }

    const arrayBuffer = await response.arrayBuffer();
    
    // Parse with xlsx
    const workbook = read(arrayBuffer, { type: 'array', cellDates: true });
    
    // Use specified sheet or first sheet
    const sheetToUse = sheet_name || workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetToUse];
    
    if (!worksheet) {
      return Response.json({ error: `Sheet "${sheetToUse}" not found` }, { status: 400 });
    }

    // Convert to JSON
    const rows = utils.sheet_to_json(worksheet, { defval: '' });
    
    console.log(`Processing ${rows.length} rows from sheet "${sheetToUse}"`);
    console.log('Sample row:', rows[0]);
    
    // Parse FQHC data
    // Expected columns: Health Center Name, Address, City, State, ZIP Code, Patients (or similar)
    const organizations = [];
    const processedNames = new Set();
    
    for (const row of rows) {
      // Try to find FQHC name (common column names)
      const name = row['Health Center Name'] || 
                   row['Organization Name'] || 
                   row['FQHC Name'] ||
                   row['Grantee Name'] ||
                   row['Center Name'];
      
      if (!name || processedNames.has(name)) continue;
      processedNames.add(name);
      
      // Extract location data
      const address = row['Address'] || row['Street Address'] || '';
      const city = row['City'] || '';
      const state = row['State'] || row['State Abbreviation'] || '';
      const zipCode = row['ZIP Code'] || row['Zip Code'] || row['ZIP'] || '';
      
      // Extract patient data
      const patients = parseInt(row['Total Patients'] || row['Patients'] || row['Patient Count'] || '0');
      
      // Find ZIP code columns for patient distribution
      const zipColumns = Object.keys(row).filter(key => 
        /^\d{5}$/.test(key) || // Direct ZIP code columns
        key.toLowerCase().includes('zip') && key.toLowerCase().includes('patient')
      );
      
      const patientsByZip = {};
      zipColumns.forEach(zipCol => {
        const zipCode = zipCol.match(/\d{5}/)?.[0];
        if (zipCode && row[zipCol]) {
          const count = parseInt(row[zipCol]);
          if (!isNaN(count) && count > 0) {
            patientsByZip[zipCode] = count;
          }
        }
      });
      
      organizations.push({
        name: name.trim(),
        type: 'fqhc',
        address: address.trim(),
        city: city.trim(),
        state: state.trim(),
        zip_code: zipCode.toString().trim(),
        patient_volume: patients || 0,
        services: ['Primary Care', 'Behavioral Health', 'Dental'],
        data_source: 'UDS 2024',
        patient_zip_distribution: patientsByZip
      });
    }
    
    console.log(`Parsed ${organizations.length} unique organizations`);
    
    // Bulk insert organizations
    if (organizations.length > 0) {
      const created = await base44.asServiceRole.entities.Organization.bulkCreate(organizations);
      
      return Response.json({
        success: true,
        count: organizations.length,
        organizations: organizations.slice(0, 5), // Return first 5 as sample
        message: `Successfully imported ${organizations.length} FQHC organizations from ${sheetToUse}`
      });
    } else {
      return Response.json({
        success: false,
        error: 'No valid FQHC data found in the sheet. Please check column names.',
        sampleRow: rows[0]
      }, { status: 400 });
    }

  } catch (error) {
    console.error('Error importing FQHC data:', error);
    return Response.json(
      { 
        success: false,
        error: error.message || 'Failed to import FQHC data',
        details: error.toString()
      },
      { status: 500 }
    );
  }
});