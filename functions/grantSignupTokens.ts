import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        // Check if user already has a signup grant transaction
        const existingGrant = await base44.asServiceRole.entities.TokenTransaction.filter({
            user_email: user.email,
            type: 'grant_signup'
        });

        if (existingGrant && existingGrant.length > 0) {
            // User already received signup tokens
            return Response.json({ 
                success: true, 
                alreadyGranted: true,
                message: 'Signup tokens already granted'
            });
        }

        // Grant 100 tokens for signup
        const SIGNUP_TOKEN_AMOUNT = 100;
        const currentBalance = user.token_balance || 0;
        const newBalance = currentBalance + SIGNUP_TOKEN_AMOUNT;

        // Update user balance
        await base44.auth.updateMe({
            token_balance: newBalance
        });

        // Create transaction record
        await base44.asServiceRole.entities.TokenTransaction.create({
            user_email: user.email,
            type: 'grant_signup',
            amount: SIGNUP_TOKEN_AMOUNT,
            balance_after: newBalance,
            description: 'Welcome to HealthScope! 100 free tokens to get started'
        });

        return Response.json({ 
            success: true, 
            tokensGranted: SIGNUP_TOKEN_AMOUNT,
            newBalance: newBalance,
            message: 'Signup tokens granted successfully'
        });

    } catch (error) {
        console.error('Error granting signup tokens:', error);
        return Response.json({ 
            error: error.message,
            success: false 
        }, { status: 500 });
    }
});