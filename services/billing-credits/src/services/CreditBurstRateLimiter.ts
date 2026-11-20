import crypto from 'crypto';
import type Redis from 'ioredis';
import { config } from '../config/env';
import { redis } from '../redis';
import { PlanTier, RateLimitExceededError } from '../types/credits';

/**
 * Sliding-window credit burst limiter.
 *
 * Each org has a sorted set of the credit "spends" admitted during the last window (score =
 * admission time in ms, member = `<id>|<credits>`). One Lua script atomically evicts entries that
 * slid out of the window, sums what is left and admits the request only if `used + cost <= limit`.
 *
 * - Sliding, not fixed: every entry leaves the window exactly `windowMs` after its own admission,
 *   and the key's expiry is re-derived from the window contents on every call
 *   (it expires when its newest entry slides out; it is deleted once the window is empty) rather than
 *   being reset to a flat TTL.
 * - One clock: time comes from the Redis server (TIME), so every app instance evaluates the same
 *   window edges regardless of local clock skew (an explicit clock can be injected for tests).
 * - Zero-cost idempotent replays: members are keyed by the idempotency key, so a replay of an
 *   already-admitted request is admitted again without being counted twice; callers also skip the
 *   limiter for replays they can detect up front and `release` admissions that fail or turn out
 *   to be replays.
 * - Fail open: any Redis error or a call slower than `redisTimeoutMs` admits the request.
 */
const ADMIT_SCRIPT = `
local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local member = ARGV[4]
local now = tonumber(ARGV[5])
if now < 0 then
  local t = redis.call('TIME')
  -- Float arithmetic: exact for ms timestamps, and no integer overflow on 32-bit Lua engines.
  now = tonumber(t[1]) * 1000.0 + math.floor(tonumber(t[2]) / 1000)
end
now = math.floor(now)

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

local function slide()
  local newest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
  if #newest == 0 then
    redis.call('DEL', key)
  else
    -- Relative to the script's clock: the key lives exactly until its newest entry slides out.
    redis.call('PEXPIRE', key, math.floor(tonumber(newest[2]) + window - now))
  end
end

if redis.call('ZSCORE', key, member) then
  slide()
  return {1, -1, 0, 0}
end

local entries = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
local used = 0
for i = 1, #entries, 2 do
  used = used + tonumber(string.match(entries[i], '|(%d+)$'))
end

if used + cost > limit then
  local needed = used + cost - limit
  local freed = 0
  local retry = window
  for i = 1, #entries, 2 do
    freed = freed + tonumber(string.match(entries[i], '|(%d+)$'))
    if freed >= needed then
      retry = math.floor(tonumber(entries[i + 1]) + window - now)
      break
    end
  end
  slide()
  return {0, used, retry, 0}
end

redis.call('ZADD', key, now, member)
slide()
return {1, used + cost, 0, 1}
`;

export interface BurstAdmission {
  allowed: boolean;
  /** True when this call added the spend to the window (and so may later `release` it). */
  counted: boolean;
  failedOpen: boolean;
  member: string;
  usedCredits: number;
  limitCredits: number;
  retryAfterMs: number;
}

export interface CreditBurstRateLimiterOptions {
  windowMs: number;
  redisTimeoutMs: number;
  enabled: boolean;
  /** Clock override in ms (tests). By default the Redis server clock is used. */
  now?: () => number;
}

export const burstKey = (orgId: string): string => `credits:burst:${orgId}`;

export class CreditBurstRateLimiter {
  private readonly opts: CreditBurstRateLimiterOptions;

  constructor(
    private readonly client: Redis = redis,
    options: Partial<CreditBurstRateLimiterOptions> = {}
  ) {
    this.opts = {
      windowMs: config.creditBurst.windowMs,
      redisTimeoutMs: config.creditBurst.redisTimeoutMs,
      enabled: config.creditBurst.enabled,
      ...options,
    };
  }

  limitFor(planTier: PlanTier, override: number | null): number {
    return override ?? config.creditBurst.limitsByTier[planTier] ?? config.creditBurst.limitsByTier.GROWTH;
  }

  /** Try to admit a spend of `credits`. `dedupeKey` makes retries of the same request count once. */
  async admit(
    orgId: string,
    credits: number,
    limit: number,
    dedupeKey?: string
  ): Promise<BurstAdmission> {
    const member = `${dedupeKey ?? crypto.randomUUID()}|${credits}`;
    const base = { member, limitCredits: limit };
    if (!this.opts.enabled || credits <= 0) {
      return { ...base, allowed: true, counted: false, failedOpen: false, usedCredits: 0, retryAfterMs: 0 };
    }

    try {
      const raw = (await this.withTimeout(
        this.client.eval(
          ADMIT_SCRIPT,
          1,
          burstKey(orgId),
          this.opts.windowMs,
          limit,
          credits,
          member,
          this.opts.now ? this.opts.now() : -1
        )
      )) as [number, number, number, number];
      const [allowed, used, retryAfterMs, counted] = raw.map(Number);
      return {
        ...base,
        allowed: allowed === 1,
        counted: counted === 1,
        failedOpen: false,
        usedCredits: Math.max(0, used),
        retryAfterMs: Math.max(0, retryAfterMs),
      };
    } catch (err) {
      console.warn(`[CreditBurstRateLimiter] failing open for ${orgId}: ${(err as Error).message}`);
      return { ...base, allowed: true, counted: false, failedOpen: true, usedCredits: 0, retryAfterMs: 0 };
    }
  }

  /** Admit or throw 429. */
  async admitOrThrow(
    orgId: string,
    credits: number,
    limit: number,
    dedupeKey?: string
  ): Promise<BurstAdmission> {
    const admission = await this.admit(orgId, credits, limit, dedupeKey);
    if (!admission.allowed) {
      throw new RateLimitExceededError(
        orgId,
        credits,
        limit,
        Math.max(1, Math.ceil(admission.retryAfterMs / 1000))
      );
    }
    return admission;
  }

  /** Give back an admitted spend (the operation failed or was a free idempotent replay). */
  async release(orgId: string, admission: BurstAdmission): Promise<void> {
    if (!admission.counted) return;
    try {
      await this.withTimeout(this.client.zrem(burstKey(orgId), admission.member));
    } catch (err) {
      console.warn(`[CreditBurstRateLimiter] release failed for ${orgId}: ${(err as Error).message}`);
    }
  }

  private withTimeout<T>(promise: Promise<T>): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`redis timed out after ${this.opts.redisTimeoutMs}ms`)),
        this.opts.redisTimeoutMs
      );
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
  }
}

export const creditBurstRateLimiter = new CreditBurstRateLimiter();
