import RedisMock from 'ioredis-mock';
import type Redis from 'ioredis';

export const redis: Redis = new RedisMock() as unknown as Redis;

export function holdRedisKey(holdId: string): string {
  return `credits:hold:${holdId}`;
}

export function balanceCacheKey(orgId: string): string {
  return `credits:balance:${orgId}`;
}

export async function flushRedis(): Promise<void> {
  await redis.flushdb();
}

export async function closeRedis(): Promise<void> {
  // No-op for in-memory cache instance
}
