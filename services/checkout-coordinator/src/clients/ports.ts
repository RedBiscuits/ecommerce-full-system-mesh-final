import type { Tender } from '../../../../shared/contracts';
import { call, HttpTransport } from './http';

export interface PlaceCreditHold {
  orderId: string;
  orgId: string;
  credits: number;
  ttlSeconds: number;
  holdKey: string;
}

export interface CreditHoldPlaced {
  holdId: string;
  status: string;
  created: boolean;
  allowanceHalalas: number;
  halalasPerCredit: number;
}

export interface ReleaseReservation {
  orderId: string;
  holdKey: string;
  reason: string;
}

export interface BillingPort {
  placeCreditHold(input: PlaceCreditHold): Promise<CreditHoldPlaced>;
  releaseCreditHold(input: ReleaseReservation): Promise<{ outcome: string }>;
}

export interface TenderHold {
  holdKey: string;
  orderId: string;
  merchantId: string;
  tender: Tender;
  sourceWalletId?: string;
  amountHalalas: number;
  checkoutId: string;
}

export interface TenderHoldPlaced {
  holdId: string;
  status: string;
  created: boolean;
}

export interface WalletPort {
  placeTenderHold(input: TenderHold): Promise<TenderHoldPlaced>;
  releaseTenderHold(input: TenderHold & { reason: string }): Promise<{ outcome: string }>;
}

/** billing-credits over HTTP. */
export function billingHttpPort(transport: HttpTransport): BillingPort {
  return {
    placeCreditHold: (input) =>
      call<CreditHoldPlaced>('billing-credits', transport, {
        method: 'POST',
        path: `/credits/orders/${encodeURIComponent(input.orderId)}/hold`,
        body: { orgId: input.orgId, credits: input.credits, ttlSeconds: input.ttlSeconds, holdKey: input.holdKey },
      }),
    releaseCreditHold: (input) =>
      call<{ outcome: string }>('billing-credits', transport, {
        method: 'POST',
        path: `/credits/orders/${encodeURIComponent(input.orderId)}/hold/release`,
        body: { holdKey: input.holdKey, reason: input.reason },
      }),
  };
}

/** wallet-ledger over HTTP, as the `checkout_coordinator` service principal. */
export function walletHttpPort(transport: HttpTransport, token: () => Promise<string> | string): WalletPort {
  const auth = async () => ({ authorization: `Bearer ${await token()}` });
  return {
    placeTenderHold: async (input) =>
      call<{ holdId: string; status: string; created: boolean }>('wallet-ledger', transport, {
        method: 'POST',
        path: '/payments/holds',
        headers: await auth(),
        body: input,
      }),
    releaseTenderHold: async (input) =>
      call<{ outcome: string }>('wallet-ledger', transport, {
        method: 'POST',
        path: '/payments/holds/release',
        headers: await auth(),
        body: input,
      }),
  };
}
