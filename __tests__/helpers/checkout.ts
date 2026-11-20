import crypto from 'node:crypto';
import { computeOrderTotals, halalasToSar, type OrderLine } from '../../shared/contracts';
import { SIGNATURE_HEADER, sign } from '../../services/hyperpay-gateway/src/security/webhookSignature';
import type { PlatformRuntime } from '../../scripts/platform';

/** OPPWA result codes. */
export const RESULT = {
  success: { code: '000.100.110', description: "Request successfully processed in 'Merchant in Integrator Test Mode'" },
  declined: { code: '800.100.151', description: 'transaction declined (invalid card)' },
  pending: { code: '000.200.000', description: 'transaction pending' },
} as const;

export const DEFAULT_LINES: OrderLine[] = [
  { sku: 'RTR-AX6000', nameEn: 'Wi-Fi 6 Router', nameAr: 'موجه واي فاي 6', quantity: 2, unitPriceHalalas: 10000 },
];

export const MERCHANT_ID = 'txp_riyadh_retail';
export const HALALAS_PER_CREDIT = 100;

export interface CheckoutOptions {
  orderId?: string;
  paymentId?: string;
  lines?: OrderLine[];
  /** Credits of the order's hold (applied as the allowance) — only used to price the payment. */
  credits?: number;
  creditOrgId?: string | null;
  result?: { code: string; description: string };
  /** Override the charged amount (e.g. to simulate a mispriced checkout). */
  amountHalalas?: number;
  customer?: { givenName: string; surname: string; email?: string };
}

export interface Checkout {
  orderId: string;
  paymentId: string;
  amountHalalas: number;
  body: Record<string, unknown>;
}

/** Build an OPPWA payment notification for an order (cart lines + order id + credit org). */
export function buildCheckout(options: CheckoutOptions = {}): Checkout {
  const orderId = options.orderId ?? `ord_${crypto.randomUUID().slice(0, 12)}`;
  const paymentId = options.paymentId ?? `8ac7a4a1${crypto.randomBytes(12).toString('hex')}`;
  const lines = options.lines ?? DEFAULT_LINES;
  const allowance = (options.credits ?? 0) * HALALAS_PER_CREDIT;
  const amountHalalas = options.amountHalalas ?? computeOrderTotals(lines, allowance).totalHalalas;
  const customer = options.customer ?? { givenName: 'Noura', surname: 'Al-Qahtani', email: 'noura@example.sa' };

  const body: Record<string, unknown> = {
    id: paymentId,
    resourcePath: `/v1/checkouts/${paymentId}/payment`,
    paymentType: 'DB',
    paymentBrand: 'MADA',
    amount: halalasToSar(amountHalalas),
    currency: 'SAR',
    merchantTransactionId: orderId,
    result: options.result ?? RESULT.success,
    timestamp: new Date().toISOString(),
    ndc: `${paymentId}.uat01-vm-tx04`,
    customer,
    customParameters: {
      merchantId: MERCHANT_ID,
      ...(options.creditOrgId ? { creditOrgId: options.creditOrgId } : {}),
    },
    cart: {
      items: lines.map((l) => ({
        merchantItemId: l.sku,
        name: l.nameEn,
        nameAr: l.nameAr,
        quantity: String(l.quantity),
        price: halalasToSar(l.unitPriceHalalas),
        ...(l.discountHalalas ? { discount: halalasToSar(l.discountHalalas) } : {}),
        ...(l.vatCategory ? { vatCategory: l.vatCategory } : {}),
        ...(l.vatExemptionCode ? { vatExemptionCode: l.vatExemptionCode } : {}),
      })),
    },
  };
  return { orderId, paymentId, amountHalalas, body };
}

/** POST the notification exactly as HyperPay would: raw JSON bytes signed with HMAC-SHA256. */
export async function postWebhook(
  platform: PlatformRuntime,
  body: unknown,
  options: { secret?: string; signature?: string | null; raw?: string } = {}
) {
  const raw = options.raw ?? JSON.stringify(body);
  const signature =
    options.signature === null
      ? undefined
      : options.signature ?? sign(options.secret ?? platform.webhookSecret, raw, Math.floor(Date.now() / 1000));
  return platform.hyperpay.app.inject({
    method: 'POST',
    url: '/api/webhook/hyperpay',
    headers: { 'content-type': 'application/json', ...(signature ? { [SIGNATURE_HEADER]: signature } : {}) },
    payload: raw,
  });
}

/** Reserve credits for an order through billing's API (what the storefront does at checkout). */
export async function placeCreditHold(platform: PlatformRuntime, orderId: string, orgId: string, credits: number) {
  const res = await platform.billing.app.inject({
    method: 'POST',
    url: `/credits/orders/${orderId}/hold`,
    payload: { orgId, credits },
  });
  if (res.statusCode !== 201 && res.statusCode !== 200) {
    throw new Error(`placing the credit hold failed: ${res.statusCode} ${res.body}`);
  }
  return res.json() as { holdId: string; orgId: string; estimatedCredits: number; created: boolean };
}

export async function spendableCredits(platform: PlatformRuntime, orgId: string): Promise<number> {
  const res = await platform.billing.app.inject({ method: 'GET', url: `/credits/organizations/${orgId}/balance` });
  return (res.json() as { netSpendableCredits: number }).netSpendableCredits;
}
