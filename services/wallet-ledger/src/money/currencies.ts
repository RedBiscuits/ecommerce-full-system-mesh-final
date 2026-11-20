import { MoneyMathError } from './integer';

/**
 * Supported currencies and their ISO 4217 minor-unit exponents. Must match the
 * `currencies` table seeded in schema.sql (enforced by a test).
 */
export const CURRENCIES = {
  USD: { code: 'USD', minorUnits: 2 },
  EUR: { code: 'EUR', minorUnits: 2 },
  GBP: { code: 'GBP', minorUnits: 2 },
  JPY: { code: 'JPY', minorUnits: 0 },
  /** Saudi riyal; the minor unit is the halala (1 SAR = 100 halalas). */
  SAR: { code: 'SAR', minorUnits: 2 },
} as const;

export type CurrencyCode = keyof typeof CURRENCIES;

export const SUPPORTED_CURRENCIES = Object.keys(CURRENCIES) as CurrencyCode[];

export class UnsupportedCurrencyError extends MoneyMathError {
  public readonly statusCode = 422;
  public readonly code = 'UNSUPPORTED_CURRENCY';
  constructor(currency: string) {
    super(`Unsupported currency: ${currency}`);
    this.name = 'UnsupportedCurrencyError';
  }
}

export function isSupportedCurrency(code: string): code is CurrencyCode {
  return Object.prototype.hasOwnProperty.call(CURRENCIES, code);
}

export function assertSupportedCurrency(code: string): CurrencyCode {
  if (!isSupportedCurrency(code)) {
    throw new UnsupportedCurrencyError(code);
  }
  return code;
}

export function minorUnitsOf(code: CurrencyCode): number {
  return CURRENCIES[code].minorUnits;
}

/**
 * Per-currency system accounts. Every money movement routes to the account of
 * the matching currency, so each currency's ledger balances independently.
 *
 *  - clearing: mirror of the external banking world (payout settlement leg)
 *  - escrow:   holds AML-review funds (ledger-synced, row-locked)
 *  - feeTreasury: platform fee revenue (ledger-only, never row-locked)
 *  - fxPool:   the platform's FX position in that currency (ledger-only)
 *
 * USD keeps the legacy `wallet_treasury` / `wallet_escrow` IDs for compatibility.
 */
export interface SystemAccounts {
  clearing: string;
  escrow: string;
  feeTreasury: string;
  fxPool: string;
}

const SYSTEM_ACCOUNTS: Record<CurrencyCode, SystemAccounts> = {
  USD: {
    clearing: 'wallet_treasury',
    escrow: 'wallet_escrow',
    feeTreasury: 'wallet_fee_treasury_usd',
    fxPool: 'wallet_fx_pool_usd',
  },
  EUR: {
    clearing: 'wallet_treasury_eur',
    escrow: 'wallet_escrow_eur',
    feeTreasury: 'wallet_fee_treasury_eur',
    fxPool: 'wallet_fx_pool_eur',
  },
  GBP: {
    clearing: 'wallet_treasury_gbp',
    escrow: 'wallet_escrow_gbp',
    feeTreasury: 'wallet_fee_treasury_gbp',
    fxPool: 'wallet_fx_pool_gbp',
  },
  JPY: {
    clearing: 'wallet_treasury_jpy',
    escrow: 'wallet_escrow_jpy',
    feeTreasury: 'wallet_fee_treasury_jpy',
    fxPool: 'wallet_fx_pool_jpy',
  },
  SAR: {
    // For SAR the clearing account also mirrors the payment gateway (HyperPay):
    // captured card payments are debited here as money arriving from outside.
    clearing: 'wallet_treasury_sar',
    escrow: 'wallet_escrow_sar',
    feeTreasury: 'wallet_fee_treasury_sar',
    fxPool: 'wallet_fx_pool_sar',
  },
};

/**
 * Output VAT collected on captured payments and owed to ZATCA, one account per
 * currency that is subject to KSA VAT. It mirrors a liability to an external
 * party, so it is a (ledger-maintained, sharded) CLEARING account.
 */
const VAT_PAYABLE_ACCOUNTS: Partial<Record<CurrencyCode, string>> = {
  SAR: 'wallet_vat_payable_sar',
};

export function vatPayableAccountFor(currency: CurrencyCode): string {
  const account = VAT_PAYABLE_ACCOUNTS[currency];
  if (!account) {
    throw new UnsupportedCurrencyError(`${currency} (no VAT payable account)`);
  }
  return account;
}

export function systemAccountsFor(currency: CurrencyCode): SystemAccounts {
  return SYSTEM_ACCOUNTS[currency];
}
