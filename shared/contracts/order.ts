import { z } from 'zod';

/**
 * Commerce context of a checkout, carried from the payment gateway through
 * every downstream step (event-carried state transfer): billing needs the
 * order id and the credit org, the ledger the merchant and amounts, ZATCA the
 * lines and the customer. All money is integer SAR halalas (1 SAR = 100).
 */
export const VAT_CATEGORIES = ['S', 'Z', 'E', 'O'] as const;
export type OrderVatCategory = (typeof VAT_CATEGORIES)[number];

export const VAT_EXEMPTION_CODES = [
  'VATEX-SA-29',
  'VATEX-SA-29-7',
  'VATEX-SA-30',
  'VATEX-SA-32',
  'VATEX-SA-33',
  'VATEX-SA-34-1',
  'VATEX-SA-EDU',
  'VATEX-SA-HEA',
  'VATEX-SA-OOS',
] as const;

const halalas = z.number().int().nonnegative().safe();

export const OrderLineSchema = z.object({
  sku: z.string().min(1).max(120),
  nameEn: z.string().min(1).max(300),
  nameAr: z.string().min(1).max(300),
  quantity: z.number().int().positive().max(1_000_000),
  /** Unit price excluding VAT. */
  unitPriceHalalas: halalas,
  /** Line-level discount (not the credit allowance, which is document level). */
  discountHalalas: halalas.optional(),
  vatCategory: z.enum(VAT_CATEGORIES).optional(),
  vatExemptionCode: z.enum(VAT_EXEMPTION_CODES).optional(),
});
export type OrderLine = z.infer<typeof OrderLineSchema>;

export const OrderCustomerSchema = z.object({
  name: z.string().min(1).max(200),
  email: z.string().max(320).optional(),
  phone: z.string().max(40).optional(),
});
export type OrderCustomer = z.infer<typeof OrderCustomerSchema>;

export const OrderContextSchema = z.object({
  orderId: z.string().min(1).max(100),
  /** Seller: the ZATCA taxpayer id and the merchant the ledger credits. */
  merchantId: z.string().min(1).max(64),
  /** Billing organization whose credit holds fund part of this order (optional). */
  creditOrgId: z.string().min(1).max(64).nullable(),
  customer: OrderCustomerSchema,
  lines: z.array(OrderLineSchema).min(1).max(500),
  /** Split-tender checkout (checkout coordinator) whose reservations this order settles, if any. */
  checkoutId: z.string().min(1).max(100).optional(),
});
export type OrderContext = z.infer<typeof OrderContextSchema>;

/**
 * How the payable remainder of an order (after the promotional credit) is paid:
 *   CARD    card payment through HyperPay (authorization hold in the ledger)
 *   WALLET  the customer's SAR wallet balance (funds reserved in escrow)
 */
export const TENDERS = ['CARD', 'WALLET'] as const;
export type Tender = (typeof TENDERS)[number];
