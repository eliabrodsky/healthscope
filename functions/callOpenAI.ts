import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        
        const user = await base44.auth.me();
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { prompt } = await req.json();

        if (!prompt) {
            return Response.json({ 
                error: 'Prompt is required' 
            }, { status: 400 });
        }

        const result = await base44.integrations.Core.InvokeLLM({
            prompt: prompt,
            add_context_from_internet: false,
            response_json_schema: {
                type: "object",
                properties: {
                    latitude: { type: "number" },
                    longitude: { type: "number" },
                    state: { type: "string" },
                    county: { type: "string" },
                    city: { type: "string" },
                    radius_miles: { type: "number" },
                    radius_center: { type: "string" },
                    zcta_codes: {
                        type: "array",
                        items: { type: "string" }
                    }
                }
            }
        });

        return Response.json(result);

    } catch (error) {
        console.error('Error calling OpenAI:', error);
        return Response.json({ 
            error: error.message || 'Failed to process request' 
        }, { status: 500 });
    }
});