/**
 * src/locking/redisLockManager.ts
 *
 * Distributed mutual exclusion over Redis, Redlock style.
 *
 *   acquire(resource)
 *     ├─ one unique owner token per acquisition (random 128 bits)
 *     ├─ on EVERY node, atomically (Lua): set the key to the token with a
 *     │  TTL unless someone else holds it
 *     ├─ quorum (N/2 + 1) must have granted it, and the time that took —
 *     │  plus a clock-drift allowance — must leave the lock some validity
 *     └─ otherwise release what was granted and report contention
 *
 *   held lock
 *     ├─ watchdog: every TTL/3 the TTL is pushed out again on every node,
 *     │  but only where the key still carries OUR token (Lua compare-and-
 *     │  pexpire). Losing quorum — key expired, Redis restarted, network
 *     │  gone — marks the lock LOST: `isValid()` turns false, `signal`
 *     │  aborts, `lost` is emitted. Work that has not reached the
 *     │  side-effect yet must check before it does.
 *     └─ release(): Lua compare-and-delete on every node. A token that is
 *        not ours is never deleted — nobody can release a lock they do not
 *        hold, and a lock that expired and was re-acquired by someone else is
 *        left alone. Idempotent, swallows Redis errors (the TTL is the
 *        fallback), always stops the watchdog.
 *
 * Redis failure modes:
 *   - a node that errors or times out counts as "not granted" (Redlock);
 *     with all nodes failing `acquire()` throws `LockUnavailableError`
 *     (distinguishable from contention, which resolves `null`) — callers
 *     fail closed and retry later, they never proceed without the lock;
 *   - `isAvailable()` answers whether a quorum of nodes is connected, so a
 *     caller can back off instead of hammering a dead Redis;
 *   - a lock lost mid-work is reported, not hidden (see above).
 *
 * Caveats (inherent to Redlock, documented in the README): no fencing token
 * is handed to the protected resource, so a process paused for longer than
 * the TTL that has already *started* its side-effect cannot be stopped; a
 * Redis node restarting without persistence forgets locks it held.
 */

import crypto from 'crypto';
import { EventEmitter } from 'events';
import type { Redis, Result } from 'ioredis';

// ─── Lua scripts (atomic on the server) ───────────────────────────────────────

/**
 * Take the lock unless another token holds it. Re-applying OUR token refreshes
 * the TTL, so a retried acquire whose reply was lost on the wire still succeeds.
 * Returns 1 when held by us afterwards, 0 when held by someone else.
 */
export const ACQUIRE_SCRIPT = `
local current = redis.call("get", KEYS[1])
if current == false then
  redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
  return 1
end
if current == ARGV[1] then
  redis.call("pexpire", KEYS[1], ARGV[2])
  return 1
end
return 0
`;

/** Push the TTL out, only if the key still carries our token. 1 = extended, 0 = not ours / gone. */
export const EXTEND_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`;

/** Delete the key, only if it still carries our token. 1 = deleted, 0 = not ours / gone. */
export const RELEASE_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`;

/** The scripts as ioredis custom commands (EVALSHA with automatic EVAL fallback). */
declare module 'ioredis' {
  interface RedisCommander<Context> {
    lockAcquire(key: string, token: string, ttlMs: number): Result<number, Context>;
    lockExtend(key: string, token: string, ttlMs: number): Result<number, Context>;
    lockRelease(key: string, token: string): Result<number, Context>;
  }
}

// ─── Types ────────────────────────────────────────────────────────────────────

export interface RedisLockManagerOptions {
  /** Independent Redis masters. One is fine; 3 or 5 give Redlock quorum. */
  nodes: Redis[];
  /** Default lock lifetime without the watchdog. */
  ttlMs: number;
  /** Clock-drift allowance as a fraction of the TTL (Redlock: 0.01). */
  driftFactor?: number;
  /** Watchdog heartbeat. Default: a third of the TTL. */
  watchdogIntervalMs?: number;
  /** Identifies this process in owner tokens (for diagnostics only). */
  ownerPrefix?: string;
  /** Injectable clock. */
  now?: () => number;
}

export interface AcquireOptions {
  ttlMs?: number;
  /** Run the watchdog while the lock is held. Default true. */
  watchdog?: boolean;
}

export type LockLostReason = 'expired' | 'quorum-lost' | 'released-elsewhere';

export interface Lock {
  readonly resource: string;
  /** The owner token — what the Lua scripts compare against. */
  readonly token: string;
  readonly acquiredAt: number;
  /** Epoch ms until which the lock is guaranteed (updated by the watchdog). */
  readonly validUntil: number;
  /** Still ours, as far as we know, and not yet expired. */
  isValid(): boolean;
  /** Aborted when the lock is lost or released. */
  readonly signal: AbortSignal;
  /** Extend once, by `ttlMs` (default: the acquire TTL). Resolves false when the lock is no longer ours. */
  extend(ttlMs?: number): Promise<boolean>;
  /** Release on every node where we still hold it. Idempotent; never throws. */
  release(): Promise<ReleaseResult>;
}

export interface ReleaseResult {
  /** Nodes that deleted our token. */
  released: number;
  /** Nodes that answered "not yours / gone". */
  notHeld: number;
  /** Nodes that could not be reached. */
  errors: number;
}

export interface LockManagerMetrics {
  enabled: boolean;
  nodes: Array<{ index: number; status: string }>;
  quorum: number;
  available: boolean;
  /** Locks currently held by this manager. */
  held: number;
  totals: {
    acquired: number;
    contended: number;
    unavailable: number;
    extended: number;
    lost: number;
    released: number;
  };
}

export interface LockManagerEvents {
  acquired: [resource: string, token: string];
  released: [resource: string, token: string, result: ReleaseResult];
  lost: [resource: string, token: string, reason: LockLostReason];
  unavailable: [resource: string, error: Error];
}

/** The lock store could not be reached (no node granted, every node errored). */
export class LockUnavailableError extends Error {
  public readonly resource: string;
  constructor(resource: string, cause: string) {
    super(`Lock store unavailable for "${resource}": ${cause}`);
    this.name = 'LockUnavailableError';
    this.resource = resource;
  }
}

/** `withLock` could not get the lock because someone else holds it. */
export class LockContentionError extends Error {
  public readonly resource: string;
  constructor(resource: string) {
    super(`Lock "${resource}" is held by another worker`);
    this.name = 'LockContentionError';
    this.resource = resource;
  }
}

type NodeAnswer = 'granted' | 'denied' | 'error';

/** What a held lock needs from its manager. */
interface LockContext {
  now(): number;
  quorum: number;
  heartbeatMs: number;
  extend(resource: string, token: string, ttlMs: number): Promise<{ granted: number; validUntil: number }>;
  release(resource: string, token: string): Promise<ReleaseResult>;
  onLost(lock: HeldLock, reason: LockLostReason): void;
  onReleased(lock: HeldLock, result: ReleaseResult): void;
}

// ─── Manager ──────────────────────────────────────────────────────────────────

export class RedisLockManager extends EventEmitter<LockManagerEvents> {
  private readonly nodes: Redis[];
  private readonly ttlMs: number;
  private readonly driftFactor: number;
  private readonly watchdogIntervalMs: number;
  private readonly ownerPrefix: string;
  private readonly now: () => number;
  private readonly held = new Map<string, HeldLock>();
  private readonly totals = { acquired: 0, contended: 0, unavailable: 0, extended: 0, lost: 0, released: 0 };

  constructor(options: RedisLockManagerOptions) {
    super();
    this.nodes = options.nodes;
    this.ttlMs = options.ttlMs;
    this.driftFactor = options.driftFactor ?? 0.01;
    this.watchdogIntervalMs = options.watchdogIntervalMs ?? Math.max(1, Math.floor(options.ttlMs / 3));
    this.ownerPrefix = options.ownerPrefix ?? `${process.pid}`;
    this.now = options.now ?? Date.now;
    for (const node of this.nodes) {
      node.defineCommand('lockAcquire', { numberOfKeys: 1, lua: ACQUIRE_SCRIPT });
      node.defineCommand('lockExtend', { numberOfKeys: 1, lua: EXTEND_SCRIPT });
      node.defineCommand('lockRelease', { numberOfKeys: 1, lua: RELEASE_SCRIPT });
    }
    this.context = {
      now: (): number => this.now(),
      quorum: this.quorum,
      heartbeatMs: this.watchdogIntervalMs,
      extend: (resource, token, ttlMs): Promise<{ granted: number; validUntil: number }> => this.extendOnNodes(resource, token, ttlMs),
      release: (resource, token): Promise<ReleaseResult> => this.releaseOnNodes(resource, token),
      onLost: (lock, reason): void => {
        this.held.delete(lock.token);
        this.totals.lost += 1;
        console.warn(`[Locks] LOST ${lock.resource} (${reason}) token=${lock.token}`);
        this.emit('lost', lock.resource, lock.token, reason);
      },
      onReleased: (lock, result): void => {
        this.held.delete(lock.token);
        this.totals.released += 1;
        this.emit('released', lock.resource, lock.token, result);
      },
    };
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  get enabled(): boolean {
    return this.nodes.length > 0;
  }

  get quorum(): number {
    return Math.floor(this.nodes.length / 2) + 1;
  }

  /** A quorum of nodes is connected — worth trying to lock. */
  isAvailable(): boolean {
    return this.enabled && this.nodes.filter((n) => n.status === 'ready').length >= this.quorum;
  }

  /**
   * Try once. Resolves the lock, or `null` when another owner holds it.
   * Throws `LockUnavailableError` when the store cannot be reached.
   */
  async acquire(resource: string, options: AcquireOptions = {}): Promise<Lock | null> {
    if (!this.enabled) throw new LockUnavailableError(resource, 'no Redis nodes configured');
    const ttlMs = options.ttlMs ?? this.ttlMs;
    const token = `${this.ownerPrefix}:${crypto.randomBytes(16).toString('hex')}`;

    const started = this.now();
    const answers = await Promise.all(this.nodes.map((node) => this.run(node.lockAcquire(resource, token, ttlMs))));
    const validUntil = started + ttlMs - this.drift(ttlMs);
    const granted = answers.filter((a) => a === 'granted').length;

    if (granted >= this.quorum && validUntil > this.now()) {
      const lock = new HeldLock(this.context, resource, token, ttlMs, started, validUntil, options.watchdog ?? true);
      this.held.set(token, lock);
      this.totals.acquired += 1;
      this.emit('acquired', resource, token);
      return lock;
    }

    // Not ours: give back whatever single nodes granted, so a partial grant
    // cannot block the next claimant for a whole TTL.
    await this.releaseOnNodes(resource, token);
    const unreachable = answers.filter((a) => a === 'error').length;
    if (this.nodes.length - unreachable < this.quorum) {
      // Too few nodes answered for anyone to hold a quorum: the store is the problem, not a competitor.
      this.totals.unavailable += 1;
      const error = new LockUnavailableError(resource, `${unreachable} of ${this.nodes.length} node(s) unreachable`);
      this.emit('unavailable', resource, error);
      throw error;
    }
    this.totals.contended += 1;
    return null;
  }

  /** Acquire, run, and release in `finally` — the only way work should hold a lock. */
  async withLock<T>(resource: string, work: (lock: Lock) => Promise<T>, options: AcquireOptions = {}): Promise<T> {
    const lock = await this.acquire(resource, options);
    if (!lock) throw new LockContentionError(resource);
    try {
      return await work(lock);
    } finally {
      await lock.release();
    }
  }

  getMetrics(): LockManagerMetrics {
    return {
      enabled: this.enabled,
      nodes: this.nodes.map((n, index) => ({ index, status: n.status })),
      quorum: this.quorum,
      available: this.isAvailable(),
      held: this.held.size,
      totals: { ...this.totals },
    };
  }

  /** Release every lock this manager still holds (shutdown). */
  async releaseAll(): Promise<void> {
    await Promise.all([...this.held.values()].map((lock) => lock.release()));
  }

  // ── Internals ──────────────────────────────────────────────────────────────

  private readonly context: LockContext;

  private async extendOnNodes(resource: string, token: string, ttlMs: number): Promise<{ granted: number; validUntil: number }> {
    const started = this.now();
    const answers = await Promise.all(this.nodes.map((node) => this.run(node.lockExtend(resource, token, ttlMs))));
    const granted = answers.filter((a) => a === 'granted').length;
    if (granted >= this.quorum) this.totals.extended += 1;
    return { granted, validUntil: started + ttlMs - this.drift(ttlMs) };
  }

  private async releaseOnNodes(resource: string, token: string): Promise<ReleaseResult> {
    const answers = await Promise.all(this.nodes.map((node) => this.run(node.lockRelease(resource, token))));
    return {
      released: answers.filter((a) => a === 'granted').length,
      notHeld: answers.filter((a) => a === 'denied').length,
      errors: answers.filter((a) => a === 'error').length,
    };
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private drift(ttlMs: number): number {
    return Math.round(ttlMs * this.driftFactor) + 2;
  }

  /** Turn a node's reply (or failure) into a Redlock answer; errors never propagate. */
  private async run(command: Promise<number>): Promise<NodeAnswer> {
    try {
      return (await command) === 1 ? 'granted' : 'denied';
    } catch (err) {
      console.warn(`[Locks] node error: ${err instanceof Error ? err.message : String(err)}`);
      return 'error';
    }
  }
}

// ─── A held lock ──────────────────────────────────────────────────────────────

class HeldLock implements Lock {
  public readonly signal: AbortSignal;
  private readonly controller = new AbortController();
  private validUntilMs: number;
  private lost = false;
  private released = false;
  private releasing: Promise<ReleaseResult> | null = null;
  private timer: NodeJS.Timeout | null = null;
  private extending = false;

  constructor(
    private readonly context: LockContext,
    public readonly resource: string,
    public readonly token: string,
    private readonly ttlMs: number,
    public readonly acquiredAt: number,
    validUntil: number,
    watchdog: boolean,
  ) {
    this.validUntilMs = validUntil;
    this.signal = this.controller.signal;
    if (watchdog) this.startWatchdog();
  }

  get validUntil(): number {
    return this.validUntilMs;
  }

  isValid(): boolean {
    if (this.lost || this.released) return false;
    if (this.context.now() >= this.validUntilMs) {
      this.markLost('expired');
      return false;
    }
    return true;
  }

  async extend(ttlMs = this.ttlMs): Promise<boolean> {
    if (!this.isValid()) return false;
    const { granted, validUntil } = await this.context.extend(this.resource, this.token, ttlMs);
    if (this.released || this.lost) return false;
    if (granted < this.context.quorum) {
      this.markLost('quorum-lost');
      return false;
    }
    this.validUntilMs = validUntil;
    return true;
  }

  release(): Promise<ReleaseResult> {
    if (this.releasing) return this.releasing;
    this.releasing = this.doRelease();
    return this.releasing;
  }

  private async doRelease(): Promise<ReleaseResult> {
    this.stopWatchdog();
    const wasLost = this.lost;
    this.released = true;
    this.controller.abort();

    let result: ReleaseResult;
    try {
      result = await this.context.release(this.resource, this.token);
    } catch (err) {
      // Cannot happen (node errors are absorbed), but a release must never throw.
      console.warn(`[Locks] release of ${this.resource} failed: ${err instanceof Error ? err.message : String(err)}`);
      result = { released: 0, notHeld: 0, errors: 1 };
    }
    if (!wasLost) this.context.onReleased(this, result);
    return result;
  }

  private startWatchdog(): void {
    this.timer = setInterval(() => {
      if (this.extending) return; // a slow extension is still in flight
      this.extending = true;
      void this.extend().finally(() => {
        this.extending = false;
      });
    }, this.context.heartbeatMs);
    this.timer.unref();
  }

  private stopWatchdog(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private markLost(reason: LockLostReason): void {
    if (this.lost || this.released) return;
    this.lost = true;
    this.stopWatchdog();
    this.controller.abort();
    this.context.onLost(this, reason);
  }
}
