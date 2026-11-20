export interface ModelRateCard {
  modelName: string;
  provider: 'anthropic' | 'openai' | 'google' | 'deepseek';
  creditsPer1kInputTokens: number;
  creditsPer1kOutputTokens: number;
  creditsPer1kCachedInputTokens: number;
  minimumChargeCredits: number;
  maxStreamHoldCredits: number;
}

export const MODEL_RATE_CARDS: Record<string, ModelRateCard> = {
  'claude-3-7-sonnet': {
    modelName: 'claude-3-7-sonnet',
    provider: 'anthropic',
    creditsPer1kInputTokens: 3,
    creditsPer1kOutputTokens: 15,
    creditsPer1kCachedInputTokens: 1,
    minimumChargeCredits: 1,
    maxStreamHoldCredits: 10000,
  },
  'claude-3-5-haiku': {
    modelName: 'claude-3-5-haiku',
    provider: 'anthropic',
    creditsPer1kInputTokens: 1,
    creditsPer1kOutputTokens: 5,
    creditsPer1kCachedInputTokens: 1,
    minimumChargeCredits: 1,
    maxStreamHoldCredits: 4000,
  },
  'gpt-4o': {
    modelName: 'gpt-4o',
    provider: 'openai',
    creditsPer1kInputTokens: 3,
    creditsPer1kOutputTokens: 10,
    creditsPer1kCachedInputTokens: 2,
    minimumChargeCredits: 1,
    maxStreamHoldCredits: 8000,
  },
  'gemini-2.5-pro': {
    modelName: 'gemini-2.5-pro',
    provider: 'google',
    creditsPer1kInputTokens: 2,
    creditsPer1kOutputTokens: 10,
    creditsPer1kCachedInputTokens: 1,
    minimumChargeCredits: 1,
    maxStreamHoldCredits: 12000,
  },
};

export const PLAN_TIER_DISCOUNT_BPS: Record<string, number> = {
  STARTER: 0, // 0% discount
  GROWTH: 0, // 0% discount
  ENTERPRISE: 1000, // 10% volume discount (1000 bps)
};
