/**
 * Fixed-capacity FIFO queue over a preallocated circular array.
 *
 * `push` and `shift` are O(1) and never allocate. A full buffer rejects the
 * push (returns `false`) instead of growing, so the caller decides what
 * backpressure means.
 */
export class RingBuffer<T> {
  readonly #slots: Array<T | undefined>;
  #head = 0;
  #size = 0;

  constructor(capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) {
      throw new RangeError('capacity must be a positive safe integer');
    }
    this.#slots = Array.from({ length: capacity }, () => undefined);
  }

  get capacity(): number {
    return this.#slots.length;
  }

  get size(): number {
    return this.#size;
  }

  get isFull(): boolean {
    return this.#size === this.#slots.length;
  }

  /** Appends `item` at the tail. Returns `false` (and drops nothing) if full. */
  push(item: T): boolean {
    if (this.isFull) {
      return false;
    }
    this.#slots[(this.#head + this.#size) % this.#slots.length] = item;
    this.#size += 1;
    return true;
  }

  /** Removes and returns the head, or `undefined` when empty. */
  shift(): T | undefined {
    if (this.#size === 0) {
      return undefined;
    }
    const item = this.#slots[this.#head];
    this.#slots[this.#head] = undefined; // don't pin the item in memory
    this.#head = (this.#head + 1) % this.#slots.length;
    this.#size -= 1;
    return item;
  }

  /** Yields the queued items from head to tail without removing them. */
  *[Symbol.iterator](): IterableIterator<T> {
    for (let i = 0; i < this.#size; i++) {
      yield this.#slots[(this.#head + i) % this.#slots.length] as T;
    }
  }

  clear(): void {
    this.#slots.fill(undefined);
    this.#head = 0;
    this.#size = 0;
  }
}
