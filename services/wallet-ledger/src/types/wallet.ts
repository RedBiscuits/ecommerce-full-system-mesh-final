import { z } from 'zod';

export type PayoutStatus =
  | 'PROCESSING'
  | 'SETTLED'
  | 'REVERSED'
  | 'RECONCILIATION_REQUIRED';

export type TransferStatus = 'COMPLETED' | 'PENDING_REVIEW' | 'REVERSED';

export type PayoutWebhookOutcome =
  | 'SETTLED'
  | 'REVERSED_AND_REFUNDED'
  | 'RECONCILIATION_REQUIRED'
  | 'DUPLICATE_IGNORED';

export type AccountType = 'USER' | 'ESCROW' | 'FEE_TREASURY' | 'FX_POOL' | 'CLEARING';

export interface WalletAccount {
  walletId: string;
  ownerId: string;
  currency: string;
  accountType: AccountType;
  /** Live balance. May be negative only for FX_POOL and CLEARING accounts. */
  balanceCents: number;
  /** Precalculated running total of the wallet's ledger entries. */
  ledgerBalanceCents: number;
  /** Number of ledger entries applied to this wallet. */
  ledgerVersion: number;
  /** 0 = single balance row; > 0 = balance striped over this many buckets. */
  shardCount: number;
  version: number;
  /** True when balance and ledger disagree: money movement is blocked until reconciled. */
  frozenPendingReconciliation: boolean;
  updatedAt: Date;
}

export const TransferFundsSchema = z
  .object({
    fromWalletId: z.string().min(1),
    toWalletId: z.string().min(1),
    /** Principal in the SENDER wallet's currency minor units. */
    amountCents: z.number().int().positive().safe(),
    /** Optional guard: reject unless the sender wallet is in this currency. */
    currency: z.string().length(3).optional(),
    referenceId: z.string().min(1).optional(),
    /**
     * Optimistic concurrency token for the SENDER wallet. When supplied, the
     * transfer only proceeds if the sender wallet is still at this version.
     */
    expectedVersion: z.number().int().positive().optional(),
  })
  .refine((input) => input.fromWalletId !== input.toWalletId, {
    message: 'Cannot transfer funds to the same wallet',
    path: ['toWalletId'],
  });

export type TransferFundsInput = z.infer<typeof TransferFundsSchema>;

export const TransferReviewSchema = z.object({
  decision: z.enum(['APPROVE', 'REJECT']),
  reviewedBy: z.string().min(1).optional(),
  reviewNote: z.string().optional(),
});

export type TransferReviewInput = z.infer<typeof TransferReviewSchema>;

export const WithdrawFundsSchema = z.object({
  walletId: z.string().min(1),
  amountCents: z.number().int().positive().safe(),
  bankAccountRef: z.string().min(1),
  idempotencyKey: z.string().min(1).optional(),
  /**
   * Optimistic concurrency token. When supplied, the withdrawal only proceeds if the
   * wallet is still at this version (i.e. nothing has changed since the caller read it).
   */
  expectedVersion: z.number().int().positive().optional(),
});

export type WithdrawFundsInput = z.infer<typeof WithdrawFundsSchema>;

export const PayoutWebhookSchema = z.object({
  eventId: z.string().min(1),
  payoutId: z.string().min(1),
  eventType: z.enum(['payout.settled', 'payout.failed']),
  bankReference: z.string().optional(),
  failureReason: z.string().optional(),
});

export type PayoutWebhookInput = z.infer<typeof PayoutWebhookSchema>;

export class InsufficientFundsError extends Error {
  public readonly statusCode = 409;
  constructor(walletId: string, requestedCents: number, availableCents: number) {
    super(
      `Insufficient funds in wallet ${walletId}: requested ${requestedCents}, available ${availableCents}`
    );
    this.name = 'InsufficientFundsError';
  }
}

export class WalletNotFoundError extends Error {
  public readonly statusCode = 404;
  constructor(walletId: string) {
    super(`Wallet ${walletId} not found`);
    this.name = 'WalletNotFoundError';
  }
}

export class WalletVersionConflictError extends Error {
  public readonly statusCode = 409;
  constructor(
    public readonly walletId: string,
    public readonly expectedVersion: number,
    public readonly actualVersion?: number
  ) {
    super(
      actualVersion === undefined
        ? `Wallet ${walletId} was modified concurrently: expected version ${expectedVersion}`
        : `Wallet ${walletId} version conflict: expected version ${expectedVersion}, found ${actualVersion}`
    );
    this.name = 'WalletVersionConflictError';
  }
}

export class PayoutStateError extends Error {
  public readonly statusCode = 409;
  constructor(payoutId: string, message: string) {
    super(`Payout ${payoutId} state error: ${message}`);
    this.name = 'PayoutStateError';
  }
}

/**
 * System accounts (escrow, fee treasury, FX pool, clearing) are internal
 * ledger accounts; they can never be the source of a withdrawal or a party to
 * a user transfer (otherwise e.g. AML-held escrow funds could be withdrawn).
 */
export class SystemAccountNotAllowedError extends Error {
  public readonly statusCode = 422;
  public readonly code = 'SYSTEM_ACCOUNT_NOT_ALLOWED';
  constructor(walletId: string, accountType: string) {
    super(`Wallet ${walletId} is a ${accountType} system account and cannot be used here`);
    this.name = 'SystemAccountNotAllowedError';
  }
}

/**
 * The wallet's balance disagrees with its precalculated ledger total (drift).
 * Detected under the wallet's row lock (or at COMMIT for ledger-maintained
 * accounts), so the verdict is race-free. Money movement is refused until the
 * reconciliation worker repairs the wallet.
 */
export class WalletFrozenError extends Error {
  public readonly statusCode = 409;
  public readonly code = 'WALLET_FROZEN_PENDING_RECONCILIATION';
  constructor(public readonly walletId: string) {
    super(`Wallet ${walletId} is frozen pending reconciliation: its balance disagrees with its ledger`);
    this.name = 'WalletFrozenError';
  }
}

export class CurrencyMismatchError extends Error {
  public readonly statusCode = 422;
  public readonly code = 'CURRENCY_MISMATCH';
  constructor(walletId: string, expected: string, actual: string) {
    super(`Wallet ${walletId} is denominated in ${actual}, not ${expected}`);
    this.name = 'CurrencyMismatchError';
  }
}

export class PayoutNotFoundError extends Error {
  public readonly statusCode = 404;
  constructor(payoutId: string) {
    super(`Payout ${payoutId} not found`);
    this.name = 'PayoutNotFoundError';
  }
}

export class TransferNotFoundError extends Error {
  public readonly statusCode = 404;
  constructor(transferId: string) {
    super(`Transfer ${transferId} not found`);
    this.name = 'TransferNotFoundError';
  }
}

export class TransferStateError extends Error {
  public readonly statusCode = 409;
  constructor(transferId: string, message: string) {
    super(`Transfer ${transferId} state error: ${message}`);
    this.name = 'TransferStateError';
  }
}
