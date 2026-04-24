// Cache the Lottie animation in memory
let cachedAnimation = null;
let lastFetch = null;
const CACHE_DURATION = 24 * 60 * 60 * 1000; // 24 hours

Deno.serve(async (req) => {
  try {
    // Check if we have a valid cached version
    if (cachedAnimation && lastFetch && (Date.now() - lastFetch < CACHE_DURATION)) {
      return new Response(cachedAnimation, {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=86400',
          'Access-Control-Allow-Origin': '*'
        }
      });
    }

    // Fetch the animation from Lottie host
    const response = await fetch('https://lottie.host/a379cb11-e304-4bd5-bb83-4defbca39236/ea2jXvxPDk.lottie');
    
    if (!response.ok) {
      throw new Error(`Failed to fetch animation: ${response.status}`);
    }

    const animationData = await response.text();
    
    // Cache the animation
    cachedAnimation = animationData;
    lastFetch = Date.now();

    return new Response(animationData, {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=86400',
        'Access-Control-Allow-Origin': '*'
      }
    });
  } catch (error) {
    console.error('Error serving Lottie animation:', error);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
});