/**
 * src/utils/money.ts
 *
 * Exact arithmetic on decimal amount strings ("49.90") via integer minor
 * units. Never use floating point for money: 0.1 + 0.2 !== 0.3.
 */

const AMOUNT = /^(\d+)(?:\.(\d{1,2}))?$/;

/** "49.90" → 4990. Throws on anything that is not a non-negative 2-decimal amount. */
export function toMinor(amount: string): number {
  const match = AMOUNT.exec(amount.trim());
  if (!match) throw new Error(`Invalid amount "${amount}"`);
  const [, whole = '0', fraction = ''] = match;
  return Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
}

/** 4990 → "49.90" */
export function fromMinor(minor: number): string {
  const sign = minor < 0 ? '-' : '';
  const abs = Math.abs(minor);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, '0')}`;
}

export function addAmounts(...amounts: string[]): string {
  return fromMinor(amounts.reduce((sum, a) => sum + toMinor(a), 0));
}

export function subtractAmounts(a: string, b: string): string {
  return fromMinor(toMinor(a) - toMinor(b));
}

/** a > b */
export function isGreater(a: string, b: string): boolean {
  return toMinor(a) > toMinor(b);
}

export function isZero(amount: string): boolean {
  return toMinor(amount) === 0;
}
