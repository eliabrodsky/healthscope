import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const ACS5_BASE_URL = "https://api.census.gov/data/{year}/acs/acs5";
const ACS5_SUBJECT_URL = "https://api.census.gov/data/{year}/acs/acs5/subject";

function safeCast(value, type) {
  if (value === "" || value === null || value === "null" || value === undefined) {
    return null;
  }
  try {
    if (type === 'int') return parseInt(value);
    if (type === 'float') return parseFloat(value);
    return value;
  } catch {
    return null;
  }
}

async function fetchZctaData(zcta, year = 2023) {
  const apiKey = Deno.env.get("CENSUS_API_KEY");
  
  // 1) Population + Median Income
  const detailedUrl = ACS5_BASE_URL.replace('{year}', year) + 
    `?get=NAME,B01003_001E,B19013_001E&for=zip%20code%20tabulation%20area:${zcta}` +
    (apiKey ? `&key=${apiKey}` : '');
  
  const detResp = await fetch(detailedUrl, { signal: AbortSignal.timeout(20000) });
  if (!detResp.ok) throw new Error(`Census API error: ${detResp.status}`);
  const detJson = await detResp.json();
  
  if (detJson.length < 2) {
    return {
      zcta5: zcta,
      population: null,
      median_income: null,
      poverty_rate: null,
      uninsured_rate: null,
      medicare_rate: null,
      medicaid_rate: null,
      source: `US Census Bureau ACS 5-Year ${year}`,
      acs_year: year.toString()
    };
  }
  
  const detRow = detJson[1];
  const population = safeCast(detRow[1], 'int');
  const medianIncome = safeCast(detRow[2], 'float');
  
  // 2) Poverty Rate %
  const povUrl = ACS5_SUBJECT_URL.replace('{year}', year) +
    `?get=S1701_C03_001E&for=zip%20code%20tabulation%20area:${zcta}` +
    (apiKey ? `&key=${apiKey}` : '');
  
  const povResp = await fetch(povUrl, { signal: AbortSignal.timeout(20000) });
  const povJson = await povResp.json();
  const povertyRate = povJson.length >= 2 ? safeCast(povJson[1][0], 'float') : null;
  
  // 3) Insurance rates
  const insUrl = ACS5_SUBJECT_URL.replace('{year}', year) +
    `?get=S2701_C05_001E,S2701_C04_003E,S2701_C04_002E&for=zip%20code%20tabulation%20area:${zcta}` +
    (apiKey ? `&key=${apiKey}` : '');
  
  let uninsuredRate = null, medicareRate = null, medicaidRate = null;
  try {
    const insResp = await fetch(insUrl, { signal: AbortSignal.timeout(20000) });
    if (insResp.ok) {
      const insJson = await insResp.json();
      if (insJson.length >= 2) {
        const insRow = insJson[1];
        uninsuredRate = safeCast(insRow[0], 'float');
        medicareRate = safeCast(insRow[1], 'float');
        medicaidRate = safeCast(insRow[2], 'float');
      }
    }
  } catch (error) {
    console.warn(`Insurance data unavailable for ${zcta}:`, error.message);
  }
  
  return {
    zcta5: zcta,
    population,
    median_income: medianIncome,
    poverty_rate: povertyRate,
    uninsured_rate: uninsuredRate,
    medicare_rate: medicareRate,
    medicaid_rate: medicaidRate,
    source: `US Census Bureau ACS 5-Year ${year}`,
    acs_year: year.toString()
  };
}

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }
    
    const { zip_codes, year = 2023 } = await req.json();
    
    if (!zip_codes || !Array.isArray(zip_codes) || zip_codes.length === 0) {
      return Response.json({ error: 'zip_codes array required' }, { status: 400 });
    }
    
    const results = {};
    const errors = [];
    
    // Process in chunks to avoid rate limits
    const CHUNK_SIZE = 5;
    for (let i = 0; i < zip_codes.length; i += CHUNK_SIZE) {
      const chunk = zip_codes.slice(i, i + CHUNK_SIZE);
      
      const chunkPromises = chunk.map(async (zip) => {
        try {
          const data = await fetchZctaData(zip, year);
          results[zip] = data;
        } catch (error) {
          errors.push({ zip, error: error.message });
          results[zip] = null;
        }
      });
      
      await Promise.all(chunkPromises);
      
      // Small delay between chunks to respect rate limits
      if (i + CHUNK_SIZE < zip_codes.length) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    
    return Response.json({
      success: true,
      results,
      errors: errors.length > 0 ? errors : undefined,
      fetched_count: Object.values(results).filter(r => r !== null).length,
      total_requested: zip_codes.length
    });
    
  } catch (error) {
    console.error('Error in fetchFreshCensusData:', error);
    return Response.json({ 
      error: error.message,
      success: false 
    }, { status: 500 });
  }
});