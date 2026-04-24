import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
    const startTime = Date.now();
    
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ 
                success: false,
                error: 'Unauthorized'
            }, { status: 401 });
        }

        const body = await req.json();
        const { organizations, state } = body;
        
        console.log(`[enrichCompetitorLocations] Received ${organizations?.length} organizations`);

        if (!organizations || !Array.isArray(organizations) || organizations.length === 0) {
            return Response.json({ 
                success: false,
                error: 'Organizations array is required'
            }, { status: 400 });
        }

        const enrichedOrgs = [];
        let totalSitesFound = 0;
        const MAX_RUNTIME_MS = 55000; // 55 second limit

        // Process organizations in batches to avoid rate limiting
        for (let i = 0; i < organizations.length; i++) {
            const org = organizations[i];
            const enrichedOrg = { ...org };
            
            // Check timeout
            if (Date.now() - startTime > MAX_RUNTIME_MS) {
                console.log(`[enrichCompetitorLocations] Timeout reached at ${i}/${organizations.length}`);
                // Add remaining orgs without enrichment
                enrichedOrgs.push(...organizations.slice(i).map(o => ({ ...o, sites: o.sites || [] })));
                break;
            }
            
            // Skip if organization already has sites
            if (org.sites && org.sites.length > 0) {
                enrichedOrgs.push(enrichedOrg);
                totalSitesFound += org.sites.length;
                continue;
            }

            // Search for sites using OpenStreetMap Nominatim
            try {
                const searchQuery = `${org.name}, ${state || 'USA'}`;
                const searchUrl = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(searchQuery)}&format=json&limit=5`;
                
                const controller = new AbortController();
                const timeout = setTimeout(() => controller.abort(), 8000);
                
                const searchResponse = await fetch(searchUrl, {
                    headers: { 'User-Agent': 'HealthScope/1.0' },
                    signal: controller.signal
                });
                
                clearTimeout(timeout);

                if (searchResponse.ok) {
                    const results = await searchResponse.json();
                    
                    if (results && results.length > 0) {
                        enrichedOrg.sites = results.map((result) => ({
                            name: result.display_name.split(',')[0] || org.name,
                            address: result.display_name || '',
                            city: org.city || '',
                            state: org.state || state || '',
                            zip_code: '',
                            coordinates: {
                                latitude: parseFloat(result.lat),
                                longitude: parseFloat(result.lon)
                            },
                            type: 'Service Delivery Site'
                        }));
                        
                        totalSitesFound += enrichedOrg.sites.length;
                    } else {
                        enrichedOrg.sites = [];
                    }
                } else {
                    enrichedOrg.sites = [];
                }

                // Rate limiting: wait between requests
                await new Promise(resolve => setTimeout(resolve, 1200));

            } catch (error) {
                console.warn(`Failed to enrich locations for ${org.name}:`, error.message);
                enrichedOrg.sites = [];
            }

            enrichedOrgs.push(enrichedOrg);
        }
        
        console.log(`[enrichCompetitorLocations] Processed ${enrichedOrgs.length}/${organizations.length} orgs in ${Date.now() - startTime}ms`);

        return Response.json({
            success: true,
            organizations: enrichedOrgs,
            totalSitesFound,
            message: `Location enrichment complete`
        });

    } catch (error) {
        console.error('Error in enrichCompetitorLocations:', error);
        return Response.json({ 
            success: false, 
            error: error.message || 'Server error'
        }, { status: 500 });
    }
});