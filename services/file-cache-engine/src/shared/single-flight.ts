/**
 * Request coalescing ("single flight").
 *
 * When several callers ask for the same key while a task for that key is
 * already running, they all share the in-flight promise instead of starting
 * duplicate work. This is the primary defence against cache stampedes: when a
 * hot entry expires, exactly one loader runs and exactly one write happens.
 */
export class SingleFlight<T> {
  readonly #inFlight = new Map<string, Promise<T>>();

  /**
   * Executes `task` for `key`, or joins an existing execution for that key.
   * Rejections propagate to every joined caller.
   */
  run(key: string, task: () => Promise<T>): Promise<T> {
    const existing = this.#inFlight.get(key);
    if (existing !== undefined) {
      return existing;
    }

    const flight = (async (): Promise<T> => task())().finally(() => {
      if (this.#inFlight.get(key) === flight) {
        this.#inFlight.delete(key);
      }
    });
    this.#inFlight.set(key, flight);
    return flight;
  }

  /** Whether a task for `key` is currently executing. */
  isInFlight(key: string): boolean {
    return this.#inFlight.has(key);
  }

  /** Number of distinct keys with a task in progress. */
  get size(): number {
    return this.#inFlight.size;
  }

  /** Resolves once every task currently in flight has settled. */
  async drain(): Promise<void> {
    await Promise.allSettled(this.#inFlight.values());
  }
}
