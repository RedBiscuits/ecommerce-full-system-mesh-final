import { describe, expect, it } from 'vitest';
import { brokerDriverFromEnv, ensureRabbitMqDirectExchangeSemantics, resolveAmqpConnect } from '../messaging';

/**
 * The in-process broker must route like RabbitMQ, or the sandbox would lose
 * messages a real deployment delivers.
 */
describe('in-process broker fidelity', () => {
  it('a DIRECT exchange delivers a copy to EVERY queue bound with the routing key (AMQP 0-9-1)', async () => {
    const connect = resolveAmqpConnect('fake');
    const conn = await connect(`amqp://fidelity-${process.pid}`);
    try {
      const ch = await conn.createConfirmChannel();
      await ch.assertExchange('payments', 'direct', { durable: true });
      for (const q of ['results', 'events', 'billing']) {
        await ch.assertQueue(q, { durable: true });
        await ch.bindQueue(q, 'payments', 'payment.result');
      }
      await ch.assertQueue('refunds', { durable: true });
      await ch.bindQueue('refunds', 'payments', 'refund.processed');

      for (let i = 0; i < 3; i++) ch.publish('payments', 'payment.result', Buffer.from(String(i)), {});
      await ch.waitForConfirms();

      for (const q of ['results', 'events', 'billing']) {
        expect((await ch.checkQueue(q)).messageCount).toBe(3);
      }
      expect((await ch.checkQueue('refunds')).messageCount).toBe(0);
    } finally {
      await conn.close();
    }
  });

  it('the fix is idempotent', () => {
    ensureRabbitMqDirectExchangeSemantics();
    ensureRabbitMqDirectExchangeSemantics();
  });

  it('selects the driver from the environment', () => {
    expect(brokerDriverFromEnv({})).toBe('fake');
    expect(brokerDriverFromEnv({ PLATFORM_BROKER_DRIVER: 'amqp' })).toBe('amqp');
    expect(brokerDriverFromEnv({ EVENT_BUS_DRIVER: 'amqp' })).toBe('amqp');
    expect(() => brokerDriverFromEnv({ PLATFORM_BROKER_DRIVER: 'kafka' })).toThrow(/fake' or 'amqp/);
  });
});
