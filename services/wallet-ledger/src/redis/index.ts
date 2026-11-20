import RedisMock from 'ioredis-mock';
import type Redis from 'ioredis';

export const redis: Redis = new RedisMock() as unknown as Redis;

export function walletLockRedisKey(walletId: string): string {
  return `wallet:lock:${walletId}`;
}

export function payoutWebhookRedisKey(eventId: string): string {
  return `wallet:webhook:${eventId}`;
}

export async function flushRedis(): Promise<void> {
  await redis.flushdb();
}

export async function closeRedis(): Promise<void> {
  // No-op for embedded in-memory Redis instance during test teardown
}
