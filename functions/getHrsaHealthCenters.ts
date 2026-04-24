import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const HRSA_TOKEN = Deno.env.get("HRSA_TOKEN");
const BASE_URL = "https://data.hrsa.gov/HDWAPI3_External/api/v1";

async function postForm(path, body) {
  if (!HRSA_TOKEN) {
    throw new Error("HRSA_TOKEN environment variable is required");
  }

  const form = new URLSearchParams(body);

  const res = await fetch(`${BASE_URL}/${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: form.toString(),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HRSA API error ${res.status} ${res.statusText}: ${text}`);
  }

  return await res.json();
}

function parseLatLon(latLonStr) {
  if (!latLonStr) return { latitude: null, longitude: null };
  const [latStr, lonStr] = latLonStr.split(" ");
  return {
    latitude: latStr ? parseFloat(latStr) : null,
    longitude: lonStr ? parseFloat(lonStr) : null
  };
}

function normalizeSite(site) {
  const coords = parseLatLon(site.LAT_LON);
  
  return {
    id: `hrsa_${site.Row_ID}`,
    name: site.SITE_NM,
    type: 'fqhc',
    address: site.SITE_ADDRESS,
    city: site.SITE_CITY,
    state: site.SITE_STATE_ABBR,
    state_name: site.STATE_NM,
    zip_code: site.SITE_ZIP_CD,
    coordinates: coords,
    phone: site.SITE_PHONE_NUM,
    website: site.SITE_URL,
    site_type: site.HCC_TYP_DESC, // e.g., "Service Delivery", "Administrative"
    location_type: site.HCC_LOC_DESC, // e.g., "Permanent"
    distance: site.Distance,
    data_source: 'HRSA Health Center Data',
    raw_data: site
  };
}

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();

    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { method, ...params } = await req.json();

    let sites = [];

    if (method === 'aroundLocation') {
      // GetHealthCentersAroundALocation
      const { latitude, longitude, radiusMiles, minRecs, maxRadius, radiusIncrement } = params;

      if (!latitude || !longitude || !radiusMiles) {
        return Response.json({ 
          error: 'Missing required parameters: latitude, longitude, radiusMiles' 
        }, { status: 400 });
      }

      const body = {
        Token: HRSA_TOKEN,
        Latitude: latitude.toString(),
        Longitude: longitude.toString(),
        Radius: radiusMiles.toString(),
        MinRecs: (minRecs || 0).toString(),
        MaxRadius: (maxRadius || radiusMiles).toString(),
        "Radius Increment": (radiusIncrement || 0).toString(),
      };

      const json = await postForm("GetHealthCentersAroundALocation", body);
      sites = json.HCC || [];

    } else if (method === 'byArea') {
      // GetHealthCentersByArea
      const { stateFips, countyFips, zipCode, inputParams } = params;

      const body = {
        Token: HRSA_TOKEN,
      };

      if (stateFips) body.StateFipsCode = stateFips;
      if (countyFips) body.CountyFipsCode = countyFips;
      if (zipCode) body.ZipCode = zipCode;
      if (inputParams) body.InputParams = inputParams;

      const json = await postForm("GetHealthCentersByArea", body);
      sites = json.HCC || [];

    } else {
      return Response.json({ 
        error: 'Invalid method. Use "aroundLocation" or "byArea"' 
      }, { status: 400 });
    }

    // Normalize and return sites
    const normalizedSites = sites.map(normalizeSite);

    return Response.json({
      success: true,
      count: normalizedSites.length,
      sites: normalizedSites,
      method: method
    });

  } catch (error) {
    console.error('HRSA API Error:', error);
    return Response.json({ 
      error: error.message || 'Failed to fetch HRSA health center data',
      details: error.toString()
    }, { status: 500 });
  }
});