import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();

    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { assessmentId, zipCodes, state, indicators } = await req.json();

    if (!assessmentId || !zipCodes || zipCodes.length === 0) {
      return Response.json(
        { error: 'assessmentId and zipCodes are required' },
        { status: 400 }
      );
    }

    if (!indicators || indicators.length === 0) {
      return Response.json(
        { error: 'At least one indicator must be selected' },
        { status: 400 }
      );
    }

    console.log(`Loading CDC data for ${zipCodes.length} ZIPs, ${indicators.length} indicators`);

    // Fetch from CDC PLACES API using Socrata with proper batch queries
    const allResults = [];
    const ZIPS_PER_BATCH = 50; // Batch ZIP codes in WHERE IN clause
    const LIMIT_PER_REQUEST = 1000;
    
    const fetchWithRetry = async (url, maxRetries = 3) => {
      for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 30000); // 30s timeout
          
          const response = await fetch(url, {
            headers: {
              'Accept': 'application/json',
              'User-Agent': 'HealthScope-App/1.0'
            },
            signal: controller.signal
          });
          
          clearTimeout(timeout);
          
          if (response.ok) {
            return await response.json();
          }
          
          if (response.status === 429) {
            console.log('Rate limited, waiting 5s...');
            await new Promise(resolve => setTimeout(resolve, 5000));
          }
          
          if (attempt < maxRetries - 1) {
            await new Promise(resolve => setTimeout(resolve, 2000 * (attempt + 1)));
          }
        } catch (error) {
          console.warn(`Fetch attempt ${attempt + 1} failed:`, error.message);
          if (attempt === maxRetries - 1) {
            throw error;
          }
          await new Promise(resolve => setTimeout(resolve, 2000 * (attempt + 1)));
        }
      }
      return [];
    };
    
    // Process ZIPs in batches using WHERE IN clause
    for (let i = 0; i < zipCodes.length; i += ZIPS_PER_BATCH) {
      const zipBatch = zipCodes.slice(i, i + ZIPS_PER_BATCH);
      const zipList = zipBatch.map(z => `'${z}'`).join(',');
      
      // Build WHERE clause for indicators and ZIPs
      const measureList = indicators.map(ind => `'${ind}'`).join(',');
      const whereClause = `locationname IN (${zipList}) AND measureid IN (${measureList})`;
      
      // Fetch with pagination
      let offset = 0;
      let hasMore = true;
      
      while (hasMore) {
        try {
          const url = `https://chronicdata.cdc.gov/resource/cwsq-ngmh.json?$where=${encodeURIComponent(whereClause)}&$limit=${LIMIT_PER_REQUEST}&$offset=${offset}`;
          
          console.log(`Fetching batch ${Math.floor(i / ZIPS_PER_BATCH) + 1}, offset ${offset}...`);
          const data = await fetchWithRetry(url);
          
          if (data && data.length > 0) {
            allResults.push(...data);
            offset += data.length;
            
            // If we got less than the limit, we're done with this batch
            if (data.length < LIMIT_PER_REQUEST) {
              hasMore = false;
            }
          } else {
            hasMore = false;
          }
          
          // Rate limiting: wait 1s between requests
          await new Promise(resolve => setTimeout(resolve, 1000));
          
        } catch (batchError) {
          console.error(`Error fetching batch:`, batchError.message);
          hasMore = false;
        }
      }
      
      console.log(`Processed ${Math.min(i + ZIPS_PER_BATCH, zipCodes.length)}/${zipCodes.length} ZIPs - ${allResults.length} data points so far`);
    }

    console.log(`Received ${allResults.length} total data points from CDC PLACES`);

    // Group by ZIP code
    const zipData = {};
    allResults.forEach(record => {
      const zip = record.locationname;
      if (!zip) return;
      
      if (!zipData[zip]) {
        zipData[zip] = {
          zcta5: zip,
          state_abbr: record.stateabbr,
          year: record.year || '2023',
          indicators: {}
        };
      }
      
      const measureId = record.measureid;
      const value = parseFloat(record.data_value);
      
      if (!isNaN(value)) {
        zipData[zip].indicators[measureId] = {
          value: value,
          label: record.measure,
          category: record.category
        };
      }
    });

    // Save to CdcHealthData entity in bulk
    const recordsToSave = [];
    
    Object.entries(zipData).forEach(([zip, data]) => {
      Object.entries(data.indicators).forEach(([measureId, indicator]) => {
        recordsToSave.push({
          zcta5: zip,
          state_abbr: data.state_abbr,
          year: data.year,
          measure_id: measureId,
          measure: indicator.label,
          category: indicator.category,
          data_value: indicator.value,
          source: 'CDC PLACES',
          source_version: '2024-release-1'
        });
      });
    });

    if (recordsToSave.length > 0) {
      console.log(`Saving ${recordsToSave.length} CDC health records...`);
      
      const SAVE_BATCH = 100;
      for (let i = 0; i < recordsToSave.length; i += SAVE_BATCH) {
        const batch = recordsToSave.slice(i, i + SAVE_BATCH);
        try {
          await base44.asServiceRole.entities.CdcHealthData.bulkCreate(batch);
        } catch (error) {
          console.warn('Error saving batch:', error);
        }
      }
    }

    // Update assessment with CDC data flag
    await base44.asServiceRole.entities.Assessment.update(assessmentId, {
      processed_data: {
        ...((await base44.asServiceRole.entities.Assessment.get(assessmentId)).processed_data || {}),
        cdc_health_needs: zipData,
        cdc_loaded_at: new Date().toISOString(),
        cdc_indicators: indicators
      }
    });

    return Response.json({
      success: true,
      zipCodesWithData: Object.keys(zipData).length,
      totalZipCodes: zipCodes.length,
      indicatorsLoaded: indicators.length,
      dataPointsSaved: recordsToSave.length,
      healthData: zipData
    });

  } catch (error) {
    console.error('Error in loadCdcHealthIndicators:', error);
    return Response.json(
      { error: error.message || 'Failed to load CDC data' },
      { status: 500 }
    );
  }
});