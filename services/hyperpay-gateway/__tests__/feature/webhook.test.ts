import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
/**
 * tests/feature/webhook.test.ts
 *
 * POST /api/webhook/hyperpay through Fastify: the signature is verified over
 * the raw request bytes before anything is parsed into the domain; only
 * authentic, valid notifications are recorded (202); replays are 200.
 */
import type { FastifyInstance } from 'fastify';
import { buildHyperPayServer } from '../../src/server';
import { sign, SIGNATURE_HEADER } from '../../src/security/webhookSignature';
import { db } from '../../src/services/jsonDb';
import { outbox, OUTBOX_COLLECTION } from '../../src/services/outbox';
import { PAYMENTS_COLLECTION } from '../../src/services/collections';
import { cleanupTestDb } from '../helpers/testDb';

const SECRET = 'feature-test-webhook-secret';
const body = {
  id: 'pay-feature-1',
  amount: '230.00',
  currency: 'SAR',
  merchantTransactionId: 'ord-feature-1',
  result: { code: '000.100.110', description: 'ok' },
  cart: { items: [{ merchantItemId: 'RTR', name: 'Router', quantity: '2', price: '100.00' }] },
};

describe('POST /api/webhook/hyperpay', () => {
  let app: FastifyInstance;
  const now = () => Math.floor(Date.now() / 1000);
  const post = (raw: string, signature?: string) =>
    app.inject({
      method: 'POST',
      url: '/api/webhook/hyperpay',
      headers: { 'content-type': 'application/json', ...(signature ? { [SIGNATURE_HEADER]: signature } : {}) },
      payload: raw,
    });

  beforeAll(async () => {
    app = buildHyperPayServer({ webhookSecret: SECRET, webhookToleranceSec: 300 });
    await app.ready();
  });

  beforeEach(async () => {
    await db.clear(PAYMENTS_COLLECTION);
    await db.clear(OUTBOX_COLLECTION);
  });

  afterAll(async () => {
    await app.close();
    cleanupTestDb();
  });

  it('records a signed notification (202) and acknowledges its replay (200, same event)', async () => {
    const raw = JSON.stringify(body);
    const first = await post(raw, sign(SECRET, raw, now()));
    expect(first.statusCode).toBe(202);
    expect(first.json()).toMatchObject({ status: 'ACCEPTED', outcome: 'RECORDED', paymentStatus: 'SUCCESS', orderId: 'ord-feature-1' });
    const replay = await post(raw, sign(SECRET, raw, now()));
    expect(replay.statusCode).toBe(200);
    expect(replay.json()).toMatchObject({ status: 'DUPLICATE', eventId: first.json().eventId });
    expect((await outbox.list()).length).toBe(1);
  });

  it.each([
    ['missing', undefined],
    ['malformed', 'garbage'],
    ['mismatch', sign('wrong-secret', JSON.stringify(body), Math.floor(Date.now() / 1000))],
    ['timestamp_out_of_tolerance', sign(SECRET, JSON.stringify(body), Math.floor(Date.now() / 1000) - 3600)],
  ])('rejects a %s signature with 401 and records nothing', async (reason, signature) => {
    const res = await post(JSON.stringify(body), signature);
    expect(res.statusCode).toBe(401);
    expect(res.json()).toEqual({ error: 'INVALID_WEBHOOK_SIGNATURE', reason });
    expect(await db.findAll(PAYMENTS_COLLECTION)).toEqual([]);
  });

  it('verifies the exact bytes: a body altered after signing is rejected', async () => {
    const raw = JSON.stringify(body);
    const res = await post(raw.replace('230.00', '2.30'), sign(SECRET, raw, now()));
    expect(res.statusCode).toBe(401);
  });

  it('rejects signed bodies that are not JSON or not a payment notification with 400', async () => {
    const notJson = '{"id": ';
    expect((await post(notJson, sign(SECRET, notJson, now()))).statusCode).toBe(400);
    const invalid = JSON.stringify({ id: 'x' });
    const res = await post(invalid, sign(SECRET, invalid, now()));
    expect(res.statusCode).toBe(400);
    expect(res.json().error).toBe('INVALID_PAYMENT_NOTIFICATION');
    expect(await db.findAll(PAYMENTS_COLLECTION)).toEqual([]);
  });
});
