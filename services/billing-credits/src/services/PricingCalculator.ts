import {
  MODEL_RATE_CARDS,
  ModelRateCard,
  PLAN_TIER_DISCOUNT_BPS,
} from '../config/modelRates';
import { PlanTier, PriceQuoteInput, UnsupportedModelError } from '../types/credits';

export interface PriceQuoteResult {
  modelName: string;
  provider: string;
  inputTokens: number;
  outputTokens: number;
  cachedInputTokens: number;
  rawCredits: number;
  discountBps: number;
  billableCredits: number;
}

export class PricingCalculator {
  getRateCard(modelName: string): ModelRateCard {
    const card = MODEL_RATE_CARDS[modelName];
    if (!card) {
      throw new UnsupportedModelError(modelName);
    }
    return card;
  }

  listRateCards(): ModelRateCard[] {
    return Object.values(MODEL_RATE_CARDS);
  }

  /**
   * Calculate integer billable credits from token counts using ceiling integer arithmetic.
   */
  calculateQuote(input: PriceQuoteInput): PriceQuoteResult {
    const card = this.getRateCard(input.modelName);
    const cachedTokens = input.cachedInputTokens ?? 0;
    const planTier: PlanTier = input.planTier ?? 'GROWTH';

    if (input.inputTokens === 0 && input.outputTokens === 0 && cachedTokens === 0) {
      return {
        modelName: card.modelName,
        provider: card.provider,
        inputTokens: 0,
        outputTokens: 0,
        cachedInputTokens: 0,
        rawCredits: 0,
        discountBps: PLAN_TIER_DISCOUNT_BPS[planTier] ?? 0,
        billableCredits: 0,
      };
    }

    const inputCostMilliCredits =
      input.inputTokens * card.creditsPer1kInputTokens +
      input.outputTokens * card.creditsPer1kOutputTokens +
      cachedTokens * card.creditsPer1kCachedInputTokens;

    const rawCredits = Math.max(
      card.minimumChargeCredits,
      Math.ceil(inputCostMilliCredits / 1000)
    );

    const discountBps = PLAN_TIER_DISCOUNT_BPS[planTier] ?? 0;
    const discounted = Math.ceil((rawCredits * (10000 - discountBps)) / 10000);
    const billableCredits = Math.max(card.minimumChargeCredits, discounted);

    return {
      modelName: card.modelName,
      provider: card.provider,
      inputTokens: input.inputTokens,
      outputTokens: input.outputTokens,
      cachedInputTokens: cachedTokens,
      rawCredits,
      discountBps,
      billableCredits,
    };
  }
}

export const pricingCalculator = new PricingCalculator();
