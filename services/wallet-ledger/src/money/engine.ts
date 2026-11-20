import { CurrencyCode, systemAccountsFor } from './currencies';
import { calculateFee, assertValidFeeBps } from './fees';
import { FX_RATE_SCALE, convertMinorUnits } from './fx';
import { MoneyMathError, assertFitsPgBigint } from './integer';

export class AmountTooSmallError extends MoneyMathError {
  public readonly statusCode = 422;
  public readonly code = 'AMOUNT_TOO_SMALL';
  constructor(message: string) {
    super(message);
    this.name = 'AmountTooSmallError';
  }
}

export class UnbalancedJournalError extends MoneyMathError {
  constructor(message: string) {
    super(message);
    this.name = 'UnbalancedJournalError';
  }
}

/** A fully-determined, integer-only price for a transfer. */
export interface TransferQuote {
  sourceCurrency: CurrencyCode;
  destCurrency: CurrencyCode;
  /** Amount sent, in source minor units (before fee). */
  principalMinor: bigint;
  feeBps: number;
  /** Platform fee in source minor units, charged on top of the principal. */
  feeMinor: bigint;
  /** Exactly principal + fee: what the sender is debited. */
  totalDebitMinor: bigint;
  /** Scaled rate used (FX_RATE_SCALE for same-currency). */
  fxRateScaled: bigint;
  /** What the recipient receives, in destination minor units. */
  creditedMinor: bigint;
}

export interface QuoteInput {
  principalMinor: bigint;
  sourceCurrency: CurrencyCode;
  destCurrency: CurrencyCode;
  feeBps: number;
  /** Required when currencies differ; ignored (identity) when equal. */
  fxRateScaled?: bigint;
}

export function quoteTransfer(input: QuoteInput): TransferQuote {
  const { principalMinor, sourceCurrency, destCurrency } = input;
  const feeBps = assertValidFeeBps(input.feeBps);

  if (principalMinor <= 0n) {
    throw new MoneyMathError(`Transfer principal must be positive, got ${principalMinor}`);
  }

  const feeMinor = calculateFee(principalMinor, feeBps);
  const totalDebitMinor = assertFitsPgBigint(principalMinor + feeMinor, 'totalDebit');

  let fxRateScaled = FX_RATE_SCALE;
  let creditedMinor = principalMinor;

  if (sourceCurrency !== destCurrency) {
    if (input.fxRateScaled === undefined) {
      throw new MoneyMathError(`FX rate required for ${sourceCurrency} -> ${destCurrency}`);
    }
    fxRateScaled = input.fxRateScaled;
    creditedMinor = convertMinorUnits(principalMinor, sourceCurrency, destCurrency, fxRateScaled);
  }

  assertFitsPgBigint(creditedMinor, 'credited');
  if (creditedMinor <= 0n) {
    throw new AmountTooSmallError(
      `${principalMinor} ${sourceCurrency} minor units converts to 0 ${destCurrency}; amount too small`
    );
  }

  return {
    sourceCurrency,
    destCurrency,
    principalMinor,
    feeBps,
    feeMinor,
    totalDebitMinor,
    fxRateScaled,
    creditedMinor,
  };
}

/** One ledger line. Negative = DEBIT, positive = CREDIT. Zero is never allowed. */
export interface JournalLine {
  walletId: string;
  currency: CurrencyCode;
  signedAmountMinor: bigint;
}

/**
 * Lines that settle a quote out of `fundingWalletId` (the sender for instant
 * transfers, the escrow account when releasing an AML hold):
 *
 *   source ccy: funding  -total | fee treasury +fee | recipient or FX pool +principal
 *   dest ccy  : FX pool  -credited | recipient +credited          (cross-currency only)
 *
 * Each currency sums to exactly zero; the FX rounding residual is absorbed by
 * the FX pool position, so no value is created or destroyed in either ledger.
 */
export function buildSettlementJournal(
  quote: TransferQuote,
  fundingWalletId: string,
  recipientWalletId: string
): JournalLine[] {
  const src = systemAccountsFor(quote.sourceCurrency);
  const lines: JournalLine[] = [
    { walletId: fundingWalletId, currency: quote.sourceCurrency, signedAmountMinor: -quote.totalDebitMinor },
  ];

  if (quote.feeMinor > 0n) {
    lines.push({ walletId: src.feeTreasury, currency: quote.sourceCurrency, signedAmountMinor: quote.feeMinor });
  }

  if (quote.sourceCurrency === quote.destCurrency) {
    lines.push({ walletId: recipientWalletId, currency: quote.destCurrency, signedAmountMinor: quote.creditedMinor });
  } else {
    const dst = systemAccountsFor(quote.destCurrency);
    lines.push(
      { walletId: src.fxPool, currency: quote.sourceCurrency, signedAmountMinor: quote.principalMinor },
      { walletId: dst.fxPool, currency: quote.destCurrency, signedAmountMinor: -quote.creditedMinor },
      { walletId: recipientWalletId, currency: quote.destCurrency, signedAmountMinor: quote.creditedMinor }
    );
  }

  assertJournalBalanced(lines);
  return lines;
}

/** Simple same-currency movement: `from` -amount, `to` +amount. */
export function buildMovementJournal(
  currency: CurrencyCode,
  fromWalletId: string,
  toWalletId: string,
  amountMinor: bigint
): JournalLine[] {
  const lines: JournalLine[] = [
    { walletId: fromWalletId, currency, signedAmountMinor: -amountMinor },
    { walletId: toWalletId, currency, signedAmountMinor: amountMinor },
  ];
  assertJournalBalanced(lines);
  return lines;
}

/** Every currency in a journal must sum to exactly zero; no zero-amount lines. */
export function assertJournalBalanced(lines: readonly JournalLine[]): void {
  if (lines.length < 2) {
    throw new UnbalancedJournalError('A journal needs at least two lines');
  }
  const totals = new Map<CurrencyCode, bigint>();
  for (const line of lines) {
    if (line.signedAmountMinor === 0n) {
      throw new UnbalancedJournalError(`Zero-amount ledger line for ${line.walletId}`);
    }
    assertFitsPgBigint(line.signedAmountMinor, `ledger line ${line.walletId}`);
    totals.set(line.currency, (totals.get(line.currency) ?? 0n) + line.signedAmountMinor);
  }
  for (const [currency, total] of totals) {
    if (total !== 0n) {
      throw new UnbalancedJournalError(`Journal does not balance in ${currency}: off by ${total}`);
    }
  }
}
