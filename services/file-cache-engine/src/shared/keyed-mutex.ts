import { LockTimeoutError } from '../resilience/errors';

export interface RunExclusiveOptions {
  /**
   * Give up waiting after this long and reject with {@link LockTimeoutError}.
   * Guards against deadlocks (e.g. a task that re-enters its own key) and
   * unbounded queues behind a stuck task. Default: wait indefinitely.
   */
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
}

/**
 * Per-key asynchronous mutex.
 *
 * Serialises tasks that share a key while letting tasks for different keys run
 * concurrently. Used to give cache mutations *isolation* semantics: two writes
 * (or a write and a delete) for the same entry never interleave.
 *
 * Fairness is FIFO per key. A waiter that times out or aborts leaves the
 * queue without disturbing the others. Memory is reclaimed automatically:
 * once the last queued task for a key finishes, its bookkeeping is dropped.
 */
export class KeyedMutex {
  readonly #tails = new Map<string, Promise<void>>();

  /**
   * Runs `task` once every previously scheduled task for `key` has settled.
   * The returned promise mirrors the outcome of `task`.
   */
  async runExclusive<T>(
    key: string,
    task: () => Promise<T>,
    options: RunExclusiveOptions = {},
  ): Promise<T> {
    const previous = this.#tails.get(key) ?? Promise.resolve();
    const { promise: current, resolve: release } =
      Promise.withResolvers<void>();
    const tail = previous.then(() => current);
    this.#tails.set(key, tail);

    const cleanup = (): void => {
      release();
      if (this.#tails.get(key) === tail) {
        this.#tails.delete(key);
      }
    };

    try {
      await this.#waitFor(previous, key, options);
    } catch (error) {
      // Never ran: release our slot immediately so later waiters proceed.
      cleanup();
      throw error;
    }

    try {
      return await task();
    } finally {
      cleanup();
    }
  }

  /**
   * Whether `key` has a running or queued task. Checking this and then
   * calling `runExclusive` in the same synchronous step is race-free, which
   * allows "skip if busy" without a try-lock.
   */
  isLocked(key: string): boolean {
    return this.#tails.has(key);
  }

  /** Number of keys that currently have at least one pending task. */
  get size(): number {
    return this.#tails.size;
  }

  async #waitFor(
    previous: Promise<void>,
    key: string,
    options: RunExclusiveOptions,
  ): Promise<void> {
    const { timeoutMs, signal } = options;
    if (timeoutMs === undefined && signal === undefined) {
      return previous;
    }
    signal?.throwIfAborted();

    const { promise: interrupted, reject } = Promise.withResolvers<never>();
    const timer =
      timeoutMs === undefined
        ? undefined
        : setTimeout(
            () => reject(new LockTimeoutError(key, timeoutMs)),
            timeoutMs,
          );
    const onAbort = (): void => reject(toError(signal?.reason));
    signal?.addEventListener('abort', onAbort, { once: true });
    try {
      await Promise.race([previous, interrupted]);
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    }
  }
}

function toError(reason: unknown): Error {
  return reason instanceof Error
    ? reason
    : new DOMException('The operation was aborted.', 'AbortError');
}
