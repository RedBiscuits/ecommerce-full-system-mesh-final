import { FastifyInstance } from 'fastify';
import { ZodError } from 'zod';
import { walletService } from '../services/WalletService';
import { payoutWebhookService } from '../services/PayoutWebhookService';
import { transferSagaService } from '../services/TransferSagaService';
import { ledgerAuditService } from '../services/LedgerAuditService';
import { canReadWallet } from '../auth/principal';
import {
  AmountTooSmallError,
  FxRateUnavailableError,
  UnsupportedCurrencyError,
} from '../money';
import {
  WalletFrozenError,
  SystemAccountNotAllowedError,
  CurrencyMismatchError,
  InsufficientFundsError,
  PayoutNotFoundError,
  PayoutStateError,
  PayoutWebhookSchema,
  TransferFundsSchema,
  TransferNotFoundError,
  TransferReviewSchema,
  TransferStateError,
  WalletNotFoundError,
  WalletVersionConflictError,
  WithdrawFundsSchema,
} from '../types/wallet';

export async function walletRoutes(app: FastifyInstance): Promise<void> {
  app.get<{ Params: { walletId: string } }>(
    '/wallets/:walletId',
    { config: { access: 'authenticated' } },
    async (request, reply) => {
      const wallet = await walletService.getWallet(request.params.walletId);
      // Not yours (and not an admin) is indistinguishable from "doesn't exist":
      // no wallet-ID enumeration oracle (IDOR).
      if (!wallet || !canReadWallet(request.principal, wallet.ownerId)) {
        return reply.status(404).send({ error: 'Wallet not found' });
      }
      return reply.status(200).send(wallet);
    }
  );

  app.post('/wallets/withdraw', { config: { access: 'authenticated' } }, async (request, reply) => {
    try {
      const input = WithdrawFundsSchema.parse(request.body);
      const result = await walletService.withdrawFunds(input, { actor: request.principal });
      return reply.status(201).send(result);
    } catch (err) {
      if (err instanceof ZodError) {
        return reply.status(400).send({ error: 'Validation failed', details: err.errors });
      }
      if (err instanceof WalletFrozenError) {
        return reply.status(409).send({ error: err.message, code: err.code, walletId: err.walletId });
      }
      if (err instanceof WalletNotFoundError) {
        return reply.status(404).send({ error: err.message, code: 'WALLET_NOT_FOUND' });
      }
      if (err instanceof InsufficientFundsError) {
        return reply.status(409).send({ error: err.message, code: 'INSUFFICIENT_FUNDS' });
      }
      if (err instanceof WalletVersionConflictError) {
        return reply.status(409).send({ error: err.message, code: 'WALLET_VERSION_CONFLICT' });
      }
      if (err instanceof SystemAccountNotAllowedError) {
        return reply.status(422).send({ error: err.message, code: err.code });
      }
      throw err;
    }
  });

  app.post('/wallets/transfer', { config: { access: 'authenticated' } }, async (request, reply) => {
    try {
      const input = TransferFundsSchema.parse(request.body);
      const result = await walletService.transferFunds(input, { actor: request.principal });
      // 202: accepted but parked in escrow pending AML review, not yet credited.
      return reply.status(result.status === 'PENDING_REVIEW' ? 202 : 201).send(result);
    } catch (err) {
      if (err instanceof ZodError) {
        return reply.status(400).send({ error: 'Validation failed', details: err.errors });
      }
      if (err instanceof WalletFrozenError) {
        return reply.status(409).send({ error: err.message, code: err.code, walletId: err.walletId });
      }
      if (err instanceof WalletNotFoundError) {
        return reply.status(404).send({ error: err.message, code: 'WALLET_NOT_FOUND' });
      }
      if (err instanceof InsufficientFundsError) {
        return reply.status(409).send({ error: err.message, code: 'INSUFFICIENT_FUNDS' });
      }
      if (err instanceof WalletVersionConflictError) {
        return reply.status(409).send({ error: err.message, code: 'WALLET_VERSION_CONFLICT' });
      }
      if (
        err instanceof SystemAccountNotAllowedError ||
        err instanceof CurrencyMismatchError ||
        err instanceof FxRateUnavailableError ||
        err instanceof AmountTooSmallError ||
        err instanceof UnsupportedCurrencyError
      ) {
        return reply.status(422).send({ error: err.message, code: err.code });
      }
      throw err;
    }
  });

  app.post('/admin/ledger/audit', { config: { access: 'admin' } }, async (request, reply) => {
    const body = (request.body ?? {}) as { publish?: unknown };
    if (body.publish !== undefined && typeof body.publish !== 'boolean') {
      return reply.status(400).send({ error: 'publish must be a boolean' });
    }
    const report = await ledgerAuditService.runAudit({ publish: body.publish ?? true });
    return reply.status(200).send(report);
  });

  app.get<{ Params: { transferId: string } }>(
    '/transfers/:transferId',
    { config: { access: 'authenticated' } },
    async (request, reply) => {
      const transfer = await transferSagaService.getTransfer(request.params.transferId, {
        actor: request.principal,
      });
      if (!transfer) {
        return reply.status(404).send({ error: 'Transfer not found', code: 'TRANSFER_NOT_FOUND' });
      }
      return reply.status(200).send(transfer);
    }
  );

  app.post<{ Params: { transferId: string } }>(
    '/transfers/:transferId/review',
    { config: { access: 'admin' } },
    async (request, reply) => {
      try {
        const input = TransferReviewSchema.parse(request.body);
        // The audit trail records the VERIFIED reviewer, never a body-supplied
        // name (legacy unauthenticated clients keep the body value).
        const reviewedBy =
          request.principal?.kind === 'token' ? request.principal.subject : input.reviewedBy;
        const result = await transferSagaService.reviewTransfer(request.params.transferId, {
          ...input,
          reviewedBy,
        });
        return reply.status(200).send(result);
      } catch (err) {
        if (err instanceof ZodError) {
          return reply.status(400).send({ error: 'Validation failed', details: err.errors });
        }
        if (err instanceof WalletFrozenError) {
          return reply.status(409).send({ error: err.message, code: err.code, walletId: err.walletId });
        }
        if (err instanceof TransferNotFoundError) {
          return reply.status(404).send({ error: err.message, code: 'TRANSFER_NOT_FOUND' });
        }
        if (err instanceof TransferStateError) {
          return reply.status(409).send({ error: err.message, code: 'TRANSFER_STATE_CONFLICT' });
        }
        if (err instanceof WalletNotFoundError) {
          return reply.status(404).send({ error: err.message, code: 'WALLET_NOT_FOUND' });
        }
        throw err;
      }
    }
  );

  app.post('/payouts/webhook', { config: { access: 'payout_webhook' } }, async (request, reply) => {
    try {
      const input = PayoutWebhookSchema.parse(request.body);
      const result = await payoutWebhookService.handlePayoutWebhook(input);
      return reply.status(200).send(result);
    } catch (err) {
      if (err instanceof ZodError) {
        return reply.status(400).send({ error: 'Validation failed', details: err.errors });
      }
      if (err instanceof WalletFrozenError) {
        return reply.status(409).send({ error: err.message, code: err.code, walletId: err.walletId });
      }
      if (err instanceof PayoutNotFoundError) {
        return reply.status(404).send({ error: err.message, code: 'PAYOUT_NOT_FOUND' });
      }
      if (err instanceof PayoutStateError) {
        return reply.status(409).send({ error: err.message, code: 'PAYOUT_STATE_CONFLICT' });
      }
      throw err;
    }
  });
}
