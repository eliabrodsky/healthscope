import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const CENSUS_API_BASE = "https://api.census.gov/data";
const YEAR = "2022"; // ACS 5-year
const MAX_RUNTIME_MS = 28000;
const SAFETY_BUFFER_MS = 8000;
const CHUNK_SIZE = 20;

// Demographics variables
const VARS = {
  totalPop: "B01003_001E",
  male: "B01001_002E",
  female: "B01001_026E",
  medianIncome: "B19013_001E",
  povertyTotal: "B17001_001E",
  povertyBelow: "B17001_002E",
  insuranceTotal: "B27001_001E",
  uninsured: "B27001_005E"
};

async function fetchCensusForZctas(zctas, censusApiKey) {
  const chunks = [];
  for (let i = 0; i < zctas.length; i += CHUNK_SIZE) {
    chunks.push(zctas.slice(i, i + CHUNK_SIZE));
  }

  const allVars = Object.values(VARS).join(",");
  const results = [];

  for (const chunk of chunks) {
    const geoList = chunk.map(z => `zip%20code%20tabulation%20area:${z}`).join(',');
    const url = `${CENSUS_API_BASE}/${YEAR}/acs/acs5?get=${allVars}&for=${geoList}`;
    const urlWithKey = censusApiKey ? `${url}&key=${censusApiKey}` : url;

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);

      const response = await fetch(urlWithKey, {
        signal: controller.signal,
        headers: { 'User-Agent': 'HealthScope/1.0' }
      });

      clearTimeout(timeout);

      if (!response.ok) {
        console.warn(`Census API ${response.status} for chunk`);
        continue;
      }

      const arr = await response.json();
      if (!Array.isArray(arr) || arr.length === 0) continue;

      const header = arr.shift();
      const rows = arr.map(vals =>
        Object.fromEntries(header.map((h, j) => [h, vals[j]]))
      );

      results.push(...rows);
    } catch (error) {
      console.warn(`Failed chunk: ${error.message}`);
    }
  }

  return results;
}

Deno.serve(async (req) => {
  const startTime = Date.now();
  const timeLeft = () => MAX_RUNTIME_MS - (startTime - Date.now());

  try {
    const base44 = createClientFromRequest(req);

    const user = await base44.auth.me();
    if (!user) {
      return Response.json({
        success: false,
        error: 'Unauthorized'
      }, { status: 401 });
    }

    const { stateAbbr, stateName } = await req.json();

    if (!stateAbbr || !stateName) {
      return Response.json({
        success: false,
        error: 'State abbreviation and name required'
      }, { status: 400 });
    }

    console.log(`Loading comprehensive demographics for ${stateName}`);

    const censusApiKey = Deno.env.get('CENSUS_API_KEY');

    const statusRecords = await base44.asServiceRole.entities.StateDataStatus.filter({
      state_abbr: stateAbbr
    });

    const statusRecord = statusRecords?.[0];

    if (!statusRecord) {
      return Response.json({
        success: false,
        code: 'STATE_NOT_INITIALIZED',
        error: `No status record for ${stateName}`,
        hint: 'Load boundaries first'
      }, { status: 404 });
    }

    if (!statusRecord.boundaries_complete || !statusRecord.zcta_cache?.length) {
      return Response.json({
        success: false,
        code: 'BOUNDARIES_NOT_READY',
        error: `Boundaries not loaded for ${stateName}`,
        message: `Load boundaries first: Admin → Data Loader → ${stateName} → Load Boundaries`,
        hint: 'Boundaries must be loaded before demographics'
      }, { status: 409 });
    }

    const zctas = statusRecord.zcta_cache;
    const total = zctas.length;
    let cursor = Number(statusRecord.demographics_cursor || 0);

    console.log(`Loading at cursor ${cursor}/${total}`);

    let processedThisRun = 0;

    while (timeLeft() > SAFETY_BUFFER_MS && cursor < total) {
      const slice = zctas.slice(cursor, Math.min(cursor + 100, total));

      const rows = await fetchCensusForZctas(slice, censusApiKey);

      const toUpsert = [];
      for (const row of rows) {
        const zcta = row['zip code tabulation area'];
        if (!zcta) continue;

        const povertyTotal = Number(row[VARS.povertyTotal] || 0);
        const povertyBelow = Number(row[VARS.povertyBelow] || 0);
        const insTotal = Number(row[VARS.insuranceTotal] || 0);
        const uninsured = Number(row[VARS.uninsured] || 0);

        const record = {
          zcta5: zcta,
          acs_year: YEAR,
          state_abbr: stateAbbr.toUpperCase(),
          source: `US Census Bureau ACS 5-Year ${YEAR}`,
          source_version: `acs${YEAR}_5yr_comprehensive`,
          population: Number(row[VARS.totalPop] || 0) || null,
          median_income: Number(row[VARS.medianIncome] || 0) || null,
          poverty_rate: povertyTotal > 0 ? ((povertyBelow / povertyTotal) * 100) : null,
          poverty_count: povertyBelow || null,
          poverty_total: povertyTotal || null,
          uninsured_rate: insTotal > 0 ? ((uninsured / insTotal) * 100) : null,
          uninsured_count: uninsured || null,
          insured_total: insTotal || null
        };

        toUpsert.push(record);
      }

      if (toUpsert.length > 0) {
        try {
          await base44.asServiceRole.entities.ZipDemographics.bulkCreate(toUpsert);
        } catch (bulkError) {
          for (const record of toUpsert) {
            try {
              await base44.asServiceRole.entities.ZipDemographics.create(record);
            } catch (err) {
              if (!err.message?.includes('unique')) {
                console.warn(`Failed ${record.zcta5}: ${err.message}`);
              }
            }
          }
        }
      }

      processedThisRun += slice.length;
      cursor += slice.length;

      await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
        demographics_cursor: cursor,
        last_demographics_update: new Date().toISOString(),
        loading_status: 'loading_demographics',
        last_error: `Loading: ${cursor}/${total} ZCTAs (${Math.round(cursor/total*100)}%)`
      });

      console.log(`[${stateName}] ${cursor}/${total} (${Math.round(cursor/total*100)}%)`);
    }

    const allComplete = cursor >= total;

    if (allComplete) {
      await base44.asServiceRole.entities.StateDataStatus.update(statusRecord.id, {
        demographics_cursor: 0,
        demographics_complete: true,
        demographics_count: total,
        loading_status: 'idle',
        last_demographics_update: new Date().toISOString(),
        last_error: null
      });
    }

    const message = allComplete
      ? `✅ Complete! Loaded comprehensive demographics for ${stateName} (${total} ZCTAs)`
      : `Loading: ${cursor}/${total} ZCTAs (${Math.round(cursor/total*100)}%). Click to continue.`;

    return Response.json({
      success: true,
      message,
      processed: cursor,
      total,
      processedThisRun,
      progress: Math.round((cursor / total) * 100),
      isComplete: allComplete,
      willContinue: !allComplete
    }, {
      status: allComplete ? 200 : 202
    });

  } catch (error) {
    console.error('Error:', error);

    return Response.json({
      success: false,
      error: error.message || 'Server error',
      hint: 'Check logs and retry'
    }, { status: 500 });
  }
});