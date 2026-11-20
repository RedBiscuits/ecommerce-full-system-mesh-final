import { describe, expect, it } from 'vitest';
import {
  AmountTooSmallError,
  CURRENCIES,
  CurrencyCode,
  FX_RATE_SCALE,
  MAX_FX_RATE_SCALED,
  MoneyMathError,
  PG_BIGINT_MAX,
  SUPPORTED_CURRENCIES,
  UnbalancedJournalError,
  UnsupportedCurrencyError,
  assertFitsPgBigint,
  assertJournalBalanced,
  assertSupportedCurrency,
  buildMovementJournal,
  buildSettlementJournal,
  calculateFee,
  convertMinorUnits,
  divRoundHalfEven,
  quoteTransfer,
  systemAccountsFor,
  toBigInt,
  toSafeNumber,
} from '../src/money';

/** Deterministic PRNG (mulberry32) so property tests are reproducible. */
function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randomBigint(rand: () => number, maxExclusive: bigint): bigint {
  // Compose from two 32-bit draws to cover large ranges without floats.
  const hi = BigInt(Math.floor(rand() * 2 ** 32));
  const lo = BigInt(Math.floor(rand() * 2 ** 32));
  return ((hi << 32n) | lo) % maxExclusive;
}

/**
 * Property check for banker's rounding of n/d (d > 0): the result r must be
 * within half a unit of the exact value, and exact ties must land on even.
 * Verified with pure integer comparisons (|2(n - r*d)| <= d).
 */
function assertHalfEvenProperty(n: bigint, d: bigint, r: bigint): void {
  const twiceError = 2n * (n - r * d);
  const absTwiceError = twiceError < 0n ? -twiceError : twiceError;
  expect(absTwiceError <= d).toBe(true);
  if (absTwiceError === d) {
    expect(r % 2n).toBe(0n); // tie -> even
  }
}

describe('integer money primitives', () => {
  describe('divRoundHalfEven (banker\'s rounding)', () => {
    it.each([
      // [numerator, denominator, expected, description]
      [5n, 10n, 0n, '0.5 -> 0 (tie to even)'],
      [15n, 10n, 2n, '1.5 -> 2 (tie to even)'],
      [25n, 10n, 2n, '2.5 -> 2 (tie to even)'],
      [35n, 10n, 4n, '3.5 -> 4 (tie to even)'],
      [-25n, 10n, -2n, '-2.5 -> -2 (symmetric)'],
      [-35n, 10n, -4n, '-3.5 -> -4 (symmetric)'],
      [24n, 10n, 2n, '2.4 -> 2'],
      [26n, 10n, 3n, '2.6 -> 3'],
      [2499999n, 1000000n, 2n, '2.499999 -> 2'],
      [2500001n, 1000000n, 3n, '2.500001 -> 3 (not a tie)'],
      [100n, 10n, 10n, 'exact division'],
      [0n, 7n, 0n, 'zero'],
      [7n, -2n, -4n, 'negative denominator: -3.5 -> -4'],
    ])('%s / %s = %s (%s)', (n, d, expected) => {
      expect(divRoundHalfEven(n, d)).toBe(expected);
    });

    it('rejects division by zero', () => {
      expect(() => divRoundHalfEven(1n, 0n)).toThrow(MoneyMathError);
    });

    it('is exact far beyond the float-safe range', () => {
      // (2^80 + 1) / 2 is a tie; 2^79 is even so it must round down to 2^79.
      const n = 2n ** 80n + 1n;
      expect(divRoundHalfEven(n, 2n)).toBe(2n ** 79n);
    });

    it('satisfies the half-even property on 5,000 seeded random inputs', () => {
      const rand = prng(0xC0FFEE);
      for (let i = 0; i < 5000; i++) {
        const n = randomBigint(rand, 10n ** 15n) * (rand() < 0.3 ? -1n : 1n);
        const d = randomBigint(rand, 10n ** 6n) + 1n;
        assertHalfEvenProperty(n < 0n ? -n : n, d, divRoundHalfEven(n < 0n ? -n : n, d));
        expect(divRoundHalfEven(n, d)).toBe(-divRoundHalfEven(-n, d)); // odd symmetry
      }
    });
  });

  describe('bigint boundary conversions', () => {
    it('normalizes driver values without losing precision', () => {
      expect(toBigInt(42)).toBe(42n);
      expect(toBigInt(42n)).toBe(42n);
      expect(toBigInt('9007199254740993')).toBe(9007199254740993n); // 2^53 + 1
      expect(toBigInt('-17')).toBe(-17n);
    });

    it.each([
      ['fractional number', 1.5],
      ['unsafe number', 2 ** 53 + 2],
      ['decimal string', '10.00'],
      ['garbage string', '12a'],
      ['null', null],
      ['NaN', Number.NaN],
    ])('rejects %s', (_label, value) => {
      expect(() => toBigInt(value)).toThrow(MoneyMathError);
    });

    it('refuses to convert unsafe bigints to JSON numbers', () => {
      expect(toSafeNumber(9007199254740991n)).toBe(Number.MAX_SAFE_INTEGER);
      expect(() => toSafeNumber(9007199254740992n)).toThrow(MoneyMathError);
    });

    it('guards the PostgreSQL BIGINT range', () => {
      expect(assertFitsPgBigint(PG_BIGINT_MAX)).toBe(PG_BIGINT_MAX);
      expect(() => assertFitsPgBigint(PG_BIGINT_MAX + 1n)).toThrow(MoneyMathError);
    });
  });
});

describe('fee engine (basis points)', () => {
  it.each([
    // [amount, bps, expectedFee, description]
    [10000n, 0, 0n, 'zero fee'],
    [10000n, 150, 150n, '1.5% of $100.00 = $1.50'],
    [10000n, 10000, 10000n, '100%'],
    [1n, 1, 0n, '0.0001 cent -> 0'],
    [50n, 100, 0n, '0.5 -> 0 (tie to even)'],
    [150n, 100, 2n, '1.5 -> 2 (tie to even)'],
    [250n, 100, 2n, '2.5 -> 2 (tie to even)'],
    [350n, 100, 4n, '3.5 -> 4 (tie to even)'],
    [151n, 100, 2n, '1.51 -> 2'],
    [149n, 100, 1n, '1.49 -> 1'],
  ])('fee(%s, %s bps) = %s (%s)', (amount, bps, expected) => {
    expect(calculateFee(amount, bps)).toBe(expected);
  });

  it.each([
    ['negative bps', -1],
    ['over 100%', 10001],
    ['fractional bps', 1.5],
    ['NaN', Number.NaN],
  ])('rejects %s', (_label, bps) => {
    expect(() => calculateFee(100n, bps)).toThrow(MoneyMathError);
  });

  it('rejects a negative fee base', () => {
    expect(() => calculateFee(-1n, 100)).toThrow(MoneyMathError);
  });

  it('satisfies the half-even property for random amounts and fee rates', () => {
    const rand = prng(42);
    for (let i = 0; i < 3000; i++) {
      const amount = randomBigint(rand, 10n ** 12n);
      const bps = Math.floor(rand() * 10001);
      assertHalfEvenProperty(amount * BigInt(bps), 10000n, calculateFee(amount, bps));
    }
  });
});

describe('FX engine (scaled integer rates)', () => {
  const rate = (decimal: string): bigint => {
    // Parse a decimal rate string into rate_scaled exactly (test helper, no floats).
    const [whole, frac = ''] = decimal.split('.');
    return BigInt(whole) * FX_RATE_SCALE + BigInt((frac + '00000000').slice(0, 8));
  };

  it.each([
    // [amount, from, to, rate, expected, description]
    [10000n, 'USD', 'EUR', '0.92', 9200n, '$100.00 -> EUR 92.00'],
    [1n, 'USD', 'EUR', '0.92', 1n, '0.92 cent -> 1 cent'],
    [100n, 'USD', 'JPY', '151.25', 151n, '$1.00 -> 151.25 JPY -> 151 (2 -> 0 decimals)'],
    [1000n, 'JPY', 'USD', '0.0066', 660n, '1000 JPY -> $6.60 (0 -> 2 decimals)'],
    [1n, 'JPY', 'USD', '0.0066', 1n, '1 JPY -> 0.66 cent -> 1 cent'],
    [1n, 'EUR', 'USD', '1.5', 2n, '1.5 cents tie -> 2 (even)'],
    [3n, 'EUR', 'USD', '1.5', 4n, '4.5 cents tie -> 4 (even)'],
    [5n, 'EUR', 'USD', '1.5', 8n, '7.5 cents tie -> 8 (even)'],
    [250n, 'USD', 'JPY', '1.0', 2n, '2.5 JPY tie -> 2 (even)'],
    [350n, 'USD', 'JPY', '1.0', 4n, '3.5 JPY tie -> 4 (even)'],
  ] as const)('%s %s -> %s @ %s = %s (%s)', (amount, from, to, r, expected, _description) => {
    expect(convertMinorUnits(amount, from, to, rate(r))).toBe(expected);
  });

  it('is deterministic: identical inputs always produce identical outputs', () => {
    const results = new Set<bigint>();
    for (let i = 0; i < 100; i++) {
      results.add(convertMinorUnits(123456789n, 'GBP', 'JPY', rate('191.5')));
    }
    expect(results.size).toBe(1);
  });

  it.each([
    ['zero rate', 0n],
    ['negative rate', -1n],
    ['absurd rate', MAX_FX_RATE_SCALED + 1n],
  ])('rejects %s', (_label, r) => {
    expect(() => convertMinorUnits(100n, 'USD', 'EUR', r)).toThrow(MoneyMathError);
  });

  it('rejects negative amounts', () => {
    expect(() => convertMinorUnits(-1n, 'USD', 'EUR', rate('0.92'))).toThrow(MoneyMathError);
  });

  it('satisfies the half-even property across every currency pair and random rates', () => {
    const rand = prng(7);
    for (let i = 0; i < 4000; i++) {
      const from = SUPPORTED_CURRENCIES[Math.floor(rand() * SUPPORTED_CURRENCIES.length)];
      const to = SUPPORTED_CURRENCIES[Math.floor(rand() * SUPPORTED_CURRENCIES.length)];
      const amount = randomBigint(rand, 10n ** 13n);
      const r = randomBigint(rand, 10n ** 12n) + 1n;
      const srcScale = 10n ** BigInt(CURRENCIES[from].minorUnits);
      const dstScale = 10n ** BigInt(CURRENCIES[to].minorUnits);
      const result = convertMinorUnits(amount, from, to, r);
      assertHalfEvenProperty(amount * r * dstScale, FX_RATE_SCALE * srcScale, result);
    }
  });
});

describe('transfer quote + journal builder', () => {
  it('same-currency with fee: total = principal + fee exactly, recipient gets principal', () => {
    const quote = quoteTransfer({
      principalMinor: 10000n,
      sourceCurrency: 'USD',
      destCurrency: 'USD',
      feeBps: 150,
    });
    expect(quote).toMatchObject({
      feeMinor: 150n,
      totalDebitMinor: 10150n,
      creditedMinor: 10000n,
      fxRateScaled: FX_RATE_SCALE,
    });

    const journal = buildSettlementJournal(quote, 'wallet_alice', 'wallet_bob');
    expect(journal).toEqual([
      { walletId: 'wallet_alice', currency: 'USD', signedAmountMinor: -10150n },
      { walletId: 'wallet_fee_treasury_usd', currency: 'USD', signedAmountMinor: 150n },
      { walletId: 'wallet_bob', currency: 'USD', signedAmountMinor: 10000n },
    ]);
  });

  it('cross-currency routes through both FX pools and balances each currency independently', () => {
    const quote = quoteTransfer({
      principalMinor: 10000n,
      sourceCurrency: 'USD',
      destCurrency: 'JPY',
      feeBps: 100,
      fxRateScaled: 15_125_000_000n, // 151.25
    });
    expect(quote.creditedMinor).toBe(15125n);
    expect(quote.totalDebitMinor).toBe(10100n);

    const journal = buildSettlementJournal(quote, 'wallet_alice', 'wallet_dave');
    expect(journal).toEqual([
      { walletId: 'wallet_alice', currency: 'USD', signedAmountMinor: -10100n },
      { walletId: 'wallet_fee_treasury_usd', currency: 'USD', signedAmountMinor: 100n },
      { walletId: 'wallet_fx_pool_usd', currency: 'USD', signedAmountMinor: 10000n },
      { walletId: 'wallet_fx_pool_jpy', currency: 'JPY', signedAmountMinor: -15125n },
      { walletId: 'wallet_dave', currency: 'JPY', signedAmountMinor: 15125n },
    ]);
  });

  it('omits the fee line entirely when the fee rounds to zero (no zero-amount ledger rows)', () => {
    const quote = quoteTransfer({ principalMinor: 50n, sourceCurrency: 'USD', destCurrency: 'USD', feeBps: 100 });
    expect(quote.feeMinor).toBe(0n);
    const journal = buildSettlementJournal(quote, 'wallet_alice', 'wallet_bob');
    expect(journal.map((l) => l.walletId)).toEqual(['wallet_alice', 'wallet_bob']);
  });

  it('rejects conversions that would credit zero', () => {
    expect(() =>
      quoteTransfer({
        principalMinor: 1n,
        sourceCurrency: 'JPY',
        destCurrency: 'USD',
        feeBps: 0,
        fxRateScaled: 400_000n, // 0.004 USD per JPY -> 0.4 cent -> 0
      })
    ).toThrow(AmountTooSmallError);
  });

  it('requires a rate for cross-currency quotes and a positive principal', () => {
    expect(() =>
      quoteTransfer({ principalMinor: 100n, sourceCurrency: 'USD', destCurrency: 'EUR', feeBps: 0 })
    ).toThrow(MoneyMathError);
    expect(() =>
      quoteTransfer({ principalMinor: 0n, sourceCurrency: 'USD', destCurrency: 'USD', feeBps: 0 })
    ).toThrow(MoneyMathError);
  });

  it('rejects a total that would overflow PostgreSQL BIGINT', () => {
    expect(() =>
      quoteTransfer({ principalMinor: PG_BIGINT_MAX, sourceCurrency: 'USD', destCurrency: 'USD', feeBps: 1 })
    ).toThrow(MoneyMathError);
  });

  it('property: 3,000 random quotes are exact, balanced per currency, and never lose value', () => {
    const rand = prng(2024);
    for (let i = 0; i < 3000; i++) {
      const src = SUPPORTED_CURRENCIES[Math.floor(rand() * SUPPORTED_CURRENCIES.length)];
      const dst = SUPPORTED_CURRENCIES[Math.floor(rand() * SUPPORTED_CURRENCIES.length)];
      const principal = randomBigint(rand, 10n ** 11n) + 1n;
      const feeBps = Math.floor(rand() * 1001);
      const fxRate = randomBigint(rand, 10n ** 12n) + FX_RATE_SCALE; // >= 1.0 so credit > 0

      const quote = quoteTransfer({
        principalMinor: principal,
        sourceCurrency: src,
        destCurrency: dst,
        feeBps,
        fxRateScaled: fxRate,
      });

      // No remainder loss on the source side: debit is exactly principal + fee.
      expect(quote.totalDebitMinor).toBe(quote.principalMinor + quote.feeMinor);

      const journal = buildSettlementJournal(quote, 'wallet_sender', 'wallet_recipient');
      const perCurrency = new Map<CurrencyCode, bigint>();
      for (const line of journal) {
        perCurrency.set(line.currency, (perCurrency.get(line.currency) ?? 0n) + line.signedAmountMinor);
      }
      for (const total of perCurrency.values()) {
        expect(total).toBe(0n);
      }
    }
  });

  it('journal validator rejects imbalance and zero lines', () => {
    expect(() =>
      assertJournalBalanced([
        { walletId: 'a', currency: 'USD', signedAmountMinor: -100n },
        { walletId: 'b', currency: 'USD', signedAmountMinor: 99n },
      ])
    ).toThrow(UnbalancedJournalError);
    expect(() =>
      assertJournalBalanced([
        { walletId: 'a', currency: 'USD', signedAmountMinor: -100n },
        { walletId: 'b', currency: 'EUR', signedAmountMinor: 100n }, // balanced in sum, not per currency
      ])
    ).toThrow(UnbalancedJournalError);
    expect(() =>
      assertJournalBalanced([
        { walletId: 'a', currency: 'USD', signedAmountMinor: 0n },
        { walletId: 'b', currency: 'USD', signedAmountMinor: 0n },
      ])
    ).toThrow(UnbalancedJournalError);
    expect(buildMovementJournal('EUR', 'x', 'y', 5n)).toHaveLength(2);
  });
});

describe('currency registry', () => {
  it('maps every supported currency to its own system accounts', () => {
    const seen = new Set<string>();
    for (const ccy of SUPPORTED_CURRENCIES) {
      for (const id of Object.values(systemAccountsFor(ccy))) {
        expect(seen.has(id)).toBe(false); // no account shared across currencies
        seen.add(id);
      }
    }
  });

  it('rejects unsupported currencies', () => {
    expect(assertSupportedCurrency('JPY')).toBe('JPY');
    expect(() => assertSupportedCurrency('XYZ')).toThrow(UnsupportedCurrencyError);
    expect(() => assertSupportedCurrency('toString')).toThrow(UnsupportedCurrencyError);
  });
});
