/**
 * src/locking/redisClient.ts
 *
 * The Redis connections behind the lock manager, configured to fail fast:
 *
 *   - `enableOfflineQueue: false` — while a node is disconnected a command
 *     fails immediately instead of queueing until it comes back. A lock
 *     request that waits is a charge that waits; the worker would rather
 *     defer the item and try again with jitter.
 *   - `maxRetriesPerRequest: 0` — ioredis does not silently re-send a
 *     command on a fresh connection (a re-sent SET could grant a lock we had
 *     already given up on).
 *   - `commandTimeout` — a node that stops answering is treated as down.
 *   - `retryStrategy` — reconnect forever with exponential backoff; an
 *     outage is an operational event, not a reason to stop.
 *   - every node has an `error` listener (an unhandled one would crash the
 *     process) and reports state changes for the admin API.
 *
 * `lockManager` is the process-wide manager; with no Redis configured it is
 * disabled (`enabled: false`) and everything that needs exclusion stays off.
 */

import Redis from 'ioredis';
import { config } from '../config/env';
import { RedisLockManager } from './redisLockManager';

export function createRedisNode(url: string, label: string): Redis {
  const { connectTimeoutMs, commandTimeoutMs, retryDelayMs, maxRetryDelayMs } = config.redis;
  const node = new Redis(url, {
    lazyConnect: true,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 0,
    connectTimeout: connectTimeoutMs,
    commandTimeout: commandTimeoutMs,
    retryStrategy: (attempt): number => Math.min(retryDelayMs * 2 ** (attempt - 1), maxRetryDelayMs),
  });
  node.on('error', (err: Error) => console.error(`[Redis:${label}] ${err.message}`));
  node.on('ready', () => console.log(`[Redis:${label}] ready`));
  node.on('end', () => console.warn(`[Redis:${label}] connection ended`));
  node.on('reconnecting', (delay: number) => console.warn(`[Redis:${label}] reconnecting in ${delay}ms`));
  return node;
}

export const redisNodes: Redis[] = config.redis.nodes.map((url, i) => createRedisNode(url, `node-${i}`));

/** Connect every node; a node that is down keeps retrying in the background. Never throws. */
export async function connectRedis(): Promise<void> {
  await Promise.all(
    redisNodes.map((node, i) =>
      node.connect().catch((err: Error) => console.warn(`[Redis:node-${i}] initial connect failed (${err.message}) — retrying in the background`)),
    ),
  );
}

/** Close every node cleanly (shutdown). */
export async function disconnectRedis(): Promise<void> {
  await Promise.all(redisNodes.map((node) => node.quit().catch(() => node.disconnect())));
}

export const lockManager = new RedisLockManager({
  nodes: redisNodes,
  ttlMs: config.locks.ttlMs,
  driftFactor: config.locks.driftFactor,
  ...(config.locks.watchdogIntervalMs > 0 ? { watchdogIntervalMs: config.locks.watchdogIntervalMs } : {}),
});
