import { EventEmitter } from 'events';
import type { ChannelModel } from 'amqplib';

/** Opens one raw broker connection (amqplib or the in-process fake). */
export type OpenConnection = () => Promise<ChannelModel>;

/**
 * Builds the complete session (topology, channels, consumers) on a freshly
 * opened connection. The connection is only published to callers after this
 * resolves; if it throws, the connection is closed and a reconnect scheduled.
 */
export type SessionSetup = (connection: ChannelModel) => Promise<void>;

export interface ReconnectPolicy {
  /** Delay before the first reconnect attempt (ms). */
  initialDelayMs: number;
  /** Upper bound for the exponential backoff (ms). */
  maxDelayMs: number;
  /** Backoff multiplier per attempt. */
  factor: number;
  /** +/- jitter ratio in [0, 1] to avoid reconnect stampedes. */
  jitter: number;
  /** Give up after this many consecutive failed attempts (default: never). */
  maxAttempts: number;
}

export const DEFAULT_RECONNECT_POLICY: ReconnectPolicy = {
  initialDelayMs: 100,
  maxDelayMs: 30_000,
  factor: 2,
  jitter: 0.2,
  maxAttempts: Number.POSITIVE_INFINITY,
};

interface Waiter {
  resolve: (connection: ChannelModel) => void;
  reject: (err: Error) => void;
}

/** Wrap a waiter so its abort listener is always detached once it settles. */
function settleOnce(
  resolve: (connection: ChannelModel) => void,
  reject: (err: Error) => void,
  detach: () => void
): Waiter {
  return {
    resolve: (c) => {
      detach();
      resolve(c);
    },
    reject: (e) => {
      detach();
      reject(e);
    },
  };
}

/**
 * A broker connection that heals itself, with ATOMIC session swaps:
 *
 *  - Single-flight: concurrent demand shares one connection attempt.
 *  - Atomic: a new connection becomes visible (`current()`, waiters, the
 *    'connected' event) only after `setup` has rebuilt the WHOLE session on it.
 *    A failure part-way closes that connection; no caller ever sees a
 *    half-built session.
 *  - Event-driven: callers await `waitForConnection()`, resolved by the
 *    connect event itself; nothing polls.
 *  - Backoff: the only time-based element is the reconnect schedule (there is
 *    no signal for "the broker is back"). It is a cancellable AbortSignal
 *    timer that triggers the next attempt, not a sleep anyone awaits; close()
 *    cancels it.
 *
 * Events: 'connected' (connection), 'disconnected' (err),
 * 'reconnect-scheduled' ({ attempt, delayMs, error }), 'reconnect-failed' (err),
 * 'connection-error' (err).
 */
export class RecoveringConnection extends EventEmitter {
  private connection: ChannelModel | null = null;
  private connecting: Promise<void> | null = null;
  private readonly stopController = new AbortController();
  private failedAttempts = 0;
  private waiters: Waiter[] = [];
  private failure: Error | null = null;
  private readonly policy: ReconnectPolicy;

  constructor(
    private readonly open: OpenConnection,
    private readonly setup: SessionSetup,
    policy: Partial<ReconnectPolicy> = {}
  ) {
    super();
    this.policy = { ...DEFAULT_RECONNECT_POLICY, ...policy };
  }

  get stopped(): boolean {
    return this.stopController.signal.aborted;
  }

  /** The live connection, or null while (re)connecting. */
  current(): ChannelModel | null {
    return this.connection;
  }

  /** Start connecting (idempotent) and resolve once a session is live. */
  start(signal?: AbortSignal): Promise<ChannelModel> {
    const wait = this.waitForConnection(signal);
    void this.connect();
    return wait;
  }

  /** Resolve with the live connection as soon as one exists. Event-driven. */
  waitForConnection(signal?: AbortSignal): Promise<ChannelModel> {
    if (this.connection) return Promise.resolve(this.connection);
    if (this.stopped) return Promise.reject(this.failure ?? new Error('Connection closed'));

    return new Promise<ChannelModel>((resolve, reject) => {
      if (signal?.aborted) {
        reject(abortReason(signal));
        return;
      }
      const onAbort = () => {
        this.waiters = this.waiters.filter((w) => w !== waiter);
        reject(abortReason(signal!));
      };
      const waiter = settleOnce(resolve, reject, () => signal?.removeEventListener('abort', onAbort));
      signal?.addEventListener('abort', onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }

  async close(): Promise<void> {
    if (this.stopped) return;
    this.failure = new Error('Connection closed');
    this.stopController.abort(); // cancels any scheduled reconnect
    this.rejectWaiters(this.failure);
    await this.connecting?.catch(() => undefined);
    const connection = this.connection;
    this.connection = null;
    if (connection) await closeQuietly(connection);
  }

  private connect(): Promise<void> {
    if (this.stopped || this.connection) return Promise.resolve();
    if (this.connecting) return this.connecting;

    this.connecting = (async () => {
      let candidate: ChannelModel | undefined;
      try {
        candidate = await this.open();
        const opened = candidate;
        // Must always have an 'error' listener: an unhandled 'error' event
        // would crash the process.
        opened.on('error', (err: Error) => this.emit('connection-error', err));
        // Watch for 'close' from the very start: a connection that dies while
        // setup is still running must never be published as live.
        let closedEarly = false;
        opened.once('close', (err?: Error) => {
          if (this.connection === opened) this.onLiveConnectionClosed(opened, err);
          else closedEarly = true;
        });
        await this.setup(opened);
        if (closedEarly) throw new Error('Connection closed during session setup');
        if (this.stopped) {
          await closeQuietly(opened);
          return;
        }
        this.publish(opened);
      } catch (err) {
        if (candidate) await closeQuietly(candidate);
        this.scheduleReconnect(toError(err));
      } finally {
        this.connecting = null;
      }
    })();
    return this.connecting;
  }

  private onLiveConnectionClosed(connection: ChannelModel, err?: Error): void {
    if (this.connection !== connection) return;
    this.connection = null;
    if (this.stopped) return;
    const error = toError(err ?? new Error('Connection closed by broker'));
    this.emit('disconnected', error);
    this.scheduleReconnect(error);
  }

  /** Atomic swap: only a fully set-up connection ever becomes visible. */
  private publish(connection: ChannelModel): void {
    this.connection = connection;
    this.failedAttempts = 0;
    const waiters = this.waiters;
    this.waiters = [];
    for (const w of waiters) w.resolve(connection);
    this.emit('connected', connection);
  }

  private scheduleReconnect(error: Error): void {
    if (this.stopped) return;
    this.failedAttempts++;
    if (this.failedAttempts > this.policy.maxAttempts) {
      this.failure = error;
      this.stopController.abort();
      this.rejectWaiters(error);
      this.emit('reconnect-failed', error);
      return;
    }

    const delayMs = this.backoffDelay(this.failedAttempts);
    this.emit('reconnect-scheduled', { attempt: this.failedAttempts, delayMs, error });

    // A cancellable timer that TRIGGERS the next attempt (nobody awaits it).
    const fire = AbortSignal.any([AbortSignal.timeout(delayMs), this.stopController.signal]);
    fire.addEventListener(
      'abort',
      () => {
        if (!this.stopped) void this.connect();
      },
      { once: true }
    );
  }

  private backoffDelay(attempt: number): number {
    const { initialDelayMs, maxDelayMs, factor, jitter } = this.policy;
    const base = Math.min(maxDelayMs, initialDelayMs * factor ** (attempt - 1));
    const spread = base * jitter * (Math.random() * 2 - 1);
    return Math.max(0, Math.round(base + spread));
  }

  private rejectWaiters(err: Error): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const w of waiters) w.reject(err);
  }
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error('Aborted');
}

async function closeQuietly(connection: ChannelModel): Promise<void> {
  try {
    await connection.close();
  } catch {
    // already closed / broken: nothing to do
  }
}
