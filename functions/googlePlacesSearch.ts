import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

const GOOGLE_PLACES_API_KEY = Deno.env.get('GOOGLE_PLACES_API_KEY');
const TOKEN_COST_PER_REQUEST = 5;

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const body = await req.json();
        const { 
            searchType, // 'healthcare', 'schools', 'nonprofits', 'general'
            query,
            latitude,
            longitude,
            radiusMiles = 10,
            placeId, // For enrichment/details
            action = 'search' // 'search', 'enrich', 'details'
        } = body;

        if (!GOOGLE_PLACES_API_KEY) {
            return Response.json({ error: 'Google Places API not configured' }, { status: 500 });
        }

        // Check token balance
        const currentBalance = user.token_balance || 0;
        if (currentBalance < TOKEN_COST_PER_REQUEST) {
            return Response.json({ 
                error: 'Insufficient tokens',
                required: TOKEN_COST_PER_REQUEST,
                balance: currentBalance
            }, { status: 402 });
        }

        let results = [];
        let apiCallMade = false;

        if (action === 'details' && placeId) {
            // Get place details for enrichment
            const detailsUrl = `https://maps.googleapis.com/maps/api/place/details/json?place_id=${placeId}&fields=name,formatted_address,formatted_phone_number,website,opening_hours,business_status,geometry,types,rating,user_ratings_total&key=${GOOGLE_PLACES_API_KEY}`;
            
            const response = await fetch(detailsUrl);
            const data = await response.json();
            apiCallMade = true;

            if (data.result) {
                results = [{
                    place_id: placeId,
                    name: data.result.name,
                    address: data.result.formatted_address,
                    phone: data.result.formatted_phone_number,
                    website: data.result.website,
                    is_open: data.result.opening_hours?.open_now,
                    hours: data.result.opening_hours?.weekday_text,
                    business_status: data.result.business_status,
                    coordinates: data.result.geometry?.location,
                    types: data.result.types,
                    rating: data.result.rating,
                    total_ratings: data.result.user_ratings_total
                }];
            }
        } else if (action === 'search' || action === 'enrich') {
            // Build search query based on type
            let searchQuery = query || '';
            const typeKeywords = {
                healthcare: 'hospital OR clinic OR medical center OR doctor OR health center OR FQHC',
                schools: 'school OR university OR college OR education',
                nonprofits: 'nonprofit OR charity OR community organization OR foundation',
                general: ''
            };

            if (searchType && typeKeywords[searchType] && !query) {
                searchQuery = typeKeywords[searchType];
            }

            const radiusMeters = Math.min(radiusMiles * 1609.34, 50000); // Max 50km
            
            // Use Text Search for better results
            const searchUrl = `https://maps.googleapis.com/maps/api/place/textsearch/json?query=${encodeURIComponent(searchQuery)}&location=${latitude},${longitude}&radius=${radiusMeters}&key=${GOOGLE_PLACES_API_KEY}`;
            
            const response = await fetch(searchUrl);
            const data = await response.json();
            apiCallMade = true;

            if (data.status !== 'OK' && data.status !== 'ZERO_RESULTS') {
                console.error('Google Places API Error:', data.status, data.error_message);
                throw new Error(`Google Places API Error: ${data.status} - ${data.error_message || ''}`);
            }

            if (data.results) {
                results = data.results.map(place => ({
                    place_id: place.place_id,
                    name: place.name,
                    address: place.formatted_address,
                    coordinates: place.geometry?.location,
                    types: place.types,
                    rating: place.rating,
                    total_ratings: place.user_ratings_total,
                    business_status: place.business_status,
                    is_open: place.opening_hours?.open_now
                }));
            }
        }

        // Deduct tokens if API call was made
        if (apiCallMade) {
            const newBalance = currentBalance - TOKEN_COST_PER_REQUEST;
            
            await base44.asServiceRole.auth.updateUser(user.id, {
                token_balance: newBalance
            });

            await base44.asServiceRole.entities.TokenTransaction.create({
                user_email: user.email,
                type: 'spend_action',
                amount: -TOKEN_COST_PER_REQUEST,
                balance_after: newBalance,
                description: `Google Places API: ${action} - ${searchType || query || 'general'}`,
                action_type: 'google_places'
            });
        }

        return Response.json({
            success: true,
            results,
            count: results.length,
            tokens_charged: apiCallMade ? TOKEN_COST_PER_REQUEST : 0,
            new_balance: apiCallMade ? (currentBalance - TOKEN_COST_PER_REQUEST) : currentBalance
        });

    } catch (error) {
        console.error('Google Places API error:', error);
        return Response.json({ error: error.message }, { status: 500 });
    }
});