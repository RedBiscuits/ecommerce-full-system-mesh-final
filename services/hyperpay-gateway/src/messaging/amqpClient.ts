/**
 * src/messaging/amqpClient.ts
 *
 * Owns the RabbitMQ connection for the lifetime of the process.
 *
 * Responsibilities:
 *   - connect with exponential backoff (retries forever while running —
 *     a broker outage is an operational event, not a reason to give up);
 *   - make the broker topology match (`ensureTopology`) before use;
 *   - open the working channel, apply prefetch, and run `onReady` so the
 *     owner can attach its consumer — on *every* (re)connect;
 *   - detect loss of the connection *or* the channel and recover by
 *     reconnecting, re-running the full sequence above;
 *   - shut down cleanly on `stop()`: tear down the consumer, close the
 *     channel and connection, and never reconnect afterwards;
 *   - publish the current connection (or `null`) to whoever needs it via
 *     `onConnectionChange`, and emit lifecycle events for logging.
 *
 * State machine:
 *
 *   idle ──start()──► connecting ──ok──► connected ──close──► reconnecting ─┐
 *                         ▲                                                 │
 *                         └────────────────────── ok ◄──────────────────────┘
 *   any ──stop()──► stopped  (terminal)
 */

import { EventEmitter } from 'events';
import amqplib from 'amqplib';
import { openChannel, safeClose } from './amqpUtils';

// ─── Types ────────────────────────────────────────────────────────────────────

export interface AmqpConnection {
  connection: amqplib.ChannelModel;
  channel: amqplib.Channel;
}

export type AmqpClientState = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'stopped';

/** Returned by `onReady`; invoked before the connection is closed on `stop()`. */
export type Teardown = () => Promise<void>;

export interface AmqpClientOptions {
  url: string;
  /** Base delay between attempts; doubles on each consecutive failure. */
  retryDelayMs: number;
  /** Upper bound for the backoff delay. */
  maxRetryDelayMs: number;
  /** Consumer prefetch applied to the working channel. */
  prefetch: number;
  /** Reconcile broker topology on a scratch channel before the working channel is opened. */
  ensureTopology: (connection: amqplib.ChannelModel) => Promise<void>;
  /** Attach consumers. Runs after every successful (re)connect. May return a teardown. */
  onReady?: (conn: AmqpConnection) => Promise<Teardown | void>;
  /** Observe the live connection (`null` while disconnected). */
  onConnectionChange?: (conn: AmqpConnection | null) => void;
  /** Injectable for tests; defaults to amqplib.connect. */
  connect?: (url: string) => Promise<amqplib.ChannelModel>;
}

export interface RetryInfo {
  attempt: number;
  delayMs: number;
  error: Error;
}

export interface AmqpClientEvents {
  connected: [conn: AmqpConnection];
  disconnected: [reason: string];
  retry: [info: RetryInfo];
  stopped: [];
}

// ─── Client ───────────────────────────────────────────────────────────────────

export class AmqpClient extends EventEmitter<AmqpClientEvents> {
  private readonly options: AmqpClientOptions;
  private readonly connectImpl: (url: string) => Promise<amqplib.ChannelModel>;

  private state: AmqpClientState = 'idle';
  private current: AmqpConnection | null = null;
  private teardown: Teardown | null = null;
  private consecutiveFailures = 0;
  /** Resolves the in-progress backoff sleep early (used by stop()). */
  private wake: (() => void) | null = null;

  constructor(options: AmqpClientOptions) {
    super();
    this.options = options;
    this.connectImpl = options.connect ?? ((url): Promise<amqplib.ChannelModel> => amqplib.connect(url));
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  getState(): AmqpClientState {
    return this.state;
  }

  /** The live connection, or `null` while disconnected/reconnecting/stopped. */
  getConnection(): AmqpConnection | null {
    return this.current;
  }

  /**
   * Connect (retrying until it works) and resolve once connected.
   * Resolves without connecting if `stop()` is called in the meantime.
   */
  async start(): Promise<void> {
    if (this.state !== 'idle') {
      throw new Error(`AmqpClient.start() called in state "${this.state}"`);
    }
    this.state = 'connecting';
    await this.connectLoop();
  }

  /** Graceful shutdown. Idempotent. No reconnect will ever follow. */
  async stop(): Promise<void> {
    if (this.state === 'stopped') return;
    this.state = 'stopped';
    this.wake?.();

    const conn = this.current;
    this.current = null;
    this.options.onConnectionChange?.(null);

    if (conn) {
      await this.runTeardown();
      await safeClose(conn.channel);
      await safeClose(conn.connection);
    }
    this.emit('stopped');
  }

  // ── Connection sequence ────────────────────────────────────────────────────

  /** Keep trying until connected or stopped. Never rejects. */
  private async connectLoop(): Promise<void> {
    while (this.state === 'connecting' || this.state === 'reconnecting') {
      try {
        await this.openConnection();
        return;
      } catch (err) {
        this.consecutiveFailures += 1;
        const error = err instanceof Error ? err : new Error(String(err));
        const delayMs = this.backoffDelay(this.consecutiveFailures);
        console.warn(
          `[AMQP] Connect attempt ${this.consecutiveFailures} failed (${error.message}) — retrying in ${delayMs}ms…`,
        );
        this.emit('retry', { attempt: this.consecutiveFailures, delayMs, error });
        await this.sleep(delayMs);
      }
    }
  }

  private async openConnection(): Promise<void> {
    console.log(`[AMQP] Connecting to ${this.options.url}…`);
    const connection = await this.connectImpl(this.options.url);

    // Attach the error listener immediately: an unhandled 'error' event throws.
    connection.on('error', (err: Error) => {
      console.error(`[AMQP] Connection error: ${err.message}`);
    });

    let conn: AmqpConnection | null = null;
    try {
      await this.options.ensureTopology(connection);

      const channel = await openChannel(connection, 'consumer');
      await channel.prefetch(this.options.prefetch);
      conn = { connection, channel };

      // Stop may have been requested while we were busy — don't come up.
      if (this.state === 'stopped') throw new Error('client stopped during connect');

      this.teardown = (await this.options.onReady?.(conn)) ?? null;
    } catch (err) {
      this.teardown = null;
      await safeClose(connection);
      throw err;
    }

    this.current = conn;
    this.consecutiveFailures = 0;
    this.state = 'connected';

    connection.on('close', () => this.handleConnectionClosed(conn, 'connection closed'));
    conn.channel.on('close', () => this.handleChannelClosed(conn));

    console.log('[AMQP] Connected; topology asserted; consumer attached.');
    this.options.onConnectionChange?.(conn);
    this.emit('connected', conn);
  }

  // ── Failure handling ───────────────────────────────────────────────────────

  private handleConnectionClosed(conn: AmqpConnection, reason: string): void {
    if (this.current !== conn) return; // stale (already replaced or stopped)

    this.current = null;
    this.teardown = null; // consumer died with the channel; nothing to cancel
    this.options.onConnectionChange?.(null);
    this.emit('disconnected', reason);

    if (this.state === 'stopped') return;
    console.warn(`[AMQP] ${reason} — reconnecting…`);
    this.state = 'reconnecting';
    void this.connectLoop();
  }

  /**
   * The working channel closed while the connection is still up (e.g. the
   * broker closed it after a protocol error). Rather than rebuild a channel
   * in place, recycle the whole connection: closing it fires the connection
   * 'close' handler, which drives the normal reconnect path.
   */
  private handleChannelClosed(conn: AmqpConnection): void {
    if (this.current !== conn || this.state !== 'connected') return;
    console.warn('[AMQP] Consumer channel closed unexpectedly — recycling connection…');
    void safeClose(conn.connection);
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private async runTeardown(): Promise<void> {
    const teardown = this.teardown;
    this.teardown = null;
    if (!teardown) return;
    try {
      await teardown();
    } catch (err) {
      console.warn('[AMQP] Teardown failed during stop:', err);
    }
  }

  private backoffDelay(failures: number): number {
    const exp = this.options.retryDelayMs * 2 ** (failures - 1);
    return Math.min(exp, this.options.maxRetryDelayMs);
  }

  /**
   * Sleep that `stop()` can cut short. Unref'd: the reconnect loop must
   * never be what keeps the process alive (server.ts's HTTP listener is).
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wake = null;
        resolve();
      }, ms);
      timer.unref();
      this.wake = (): void => {
        clearTimeout(timer);
        this.wake = null;
        resolve();
      };
    });
  }
}
