Deno.serve(async (req) => {
  try {
    const { file_url } = await req.json();

    if (!file_url) {
      return Response.json({ error: 'file_url is required' }, { status: 400 });
    }

    console.log('Fetching Excel file from:', file_url);

    // Dynamic import to avoid cold start issues
    const XLSX = await import('https://cdn.sheetjs.com/xlsx-0.20.1/package/xlsx.mjs');

    // Fetch with timeout
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 90000);
    
    const response = await fetch(file_url, { signal: controller.signal });
    clearTimeout(timeout);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch: ${response.status} ${response.statusText}`);
    }

    const arrayBuffer = await response.arrayBuffer();
    const sizeMB = (arrayBuffer.byteLength / (1024 * 1024)).toFixed(1);
    console.log(`File size: ${sizeMB} MB`);
    
    // Parse with minimal memory footprint
    const workbook = XLSX.read(arrayBuffer, { 
      type: 'array',
      sheetRows: 6, // Only first 6 rows (1 header + 5 data)
      bookSheets: true,
      bookProps: true
    });
    
    const sheets = [];
    
    for (const sheetName of workbook.SheetNames) {
      const worksheet = workbook.Sheets[sheetName];
      
      // Estimate total rows from range
      let rowCount = 0;
      if (worksheet['!ref']) {
        const range = XLSX.utils.decode_range(worksheet['!ref']);
        rowCount = range.e.r;
      }
      
      // Get all data as array of arrays
      const allData = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: '' });
      
      if (allData.length === 0) {
        sheets.push({
          name: sheetName,
          rowCount: 0,
          columns: [],
          sampleRows: []
        });
        continue;
      }
      
      // First row = headers
      const headers = allData[0];
      const columns = headers.map(h => String(h || '')).filter(h => h.trim());
      
      // Convert data rows to objects
      const dataRows = allData.slice(1).map(row => {
        const obj = {};
        headers.forEach((header, idx) => {
          if (header) {
            obj[String(header)] = row[idx] != null ? String(row[idx]) : '';
          }
        });
        return obj;
      });
      
      sheets.push({
        name: sheetName,
        rowCount: rowCount,
        columns: columns,
        sampleRows: dataRows
      });
    }

    return Response.json({
      success: true,
      sheets: sheets,
      totalSheets: sheets.length,
      fileSize: `${sizeMB} MB`
    });

  } catch (error) {
    console.error('Excel parsing error:', error);
    return Response.json(
      { 
        success: false,
        error: error.message || 'Failed to parse Excel file',
        stack: error.stack
      },
      { status: 500 }
    );
  }
});