import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

/**
 * Spend tokens for various actions
 * Action types and costs:
 * - assessment: 5 tokens
 * - competitor_analysis: 5 tokens  
 * - regional_analysis: 5 tokens
 * - presentation: 5 tokens
 */

const TOKEN_COSTS = {
  assessment: 5,
  competitor_analysis: 5,
  regional_analysis: 5,
  presentation: 5
};

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { action_type, related_id, description } = await req.json();

    if (!action_type || !TOKEN_COSTS[action_type]) {
      return Response.json({ 
        success: false,
        error: `Invalid action type. Valid types: ${Object.keys(TOKEN_COSTS).join(', ')}`
      }, { status: 400 });
    }

    const cost = TOKEN_COSTS[action_type];
    const currentBalance = user.token_balance || 0;

    if (currentBalance < cost) {
      return Response.json({ 
        success: false,
        error: `Insufficient tokens. You need ${cost} tokens but only have ${currentBalance}.`,
        required: cost,
        balance: currentBalance
      }, { status: 402 });
    }

    const newBalance = currentBalance - cost;

    // Update user balance
    await base44.auth.updateMe({
      token_balance: newBalance
    });

    // Create transaction record
    await base44.asServiceRole.entities.TokenTransaction.create({
      user_email: user.email,
      type: 'spend_action',
      amount: -cost,
      balance_after: newBalance,
      description: description || `${action_type.replace(/_/g, ' ')} created`,
      action_type: action_type,
      related_id: related_id || null
    });

    return Response.json({ 
      success: true, 
      cost: cost,
      newBalance: newBalance,
      message: `Spent ${cost} tokens for ${action_type}`
    });

  } catch (error) {
    console.error('Error spending tokens:', error);
    return Response.json({ 
      error: error.message,
      success: false 
    }, { status: 500 });
  }
});