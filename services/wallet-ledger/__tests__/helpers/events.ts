import { on } from 'events';
import type { EventEmitter } from 'events';

/**
 * Event-driven waiting for tests: resolve with the first `event` payload that
 * matches `predicate`. The deadline is an AbortSignal, never a sleep/poll.
 */
export async function waitForEvent<T = unknown>(
  emitter: EventEmitter,
  event: string,
  predicate: (payload: T) => boolean = () => true,
  timeoutMs = 5000
): Promise<T> {
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    for await (const [payload] of on(emitter, event, { signal })) {
      if (predicate(payload as T)) return payload as T;
    }
  } catch (err) {
    if (signal.aborted) throw new Error(`Timed out after ${timeoutMs}ms waiting for '${event}'`, { cause: err });
    throw err;
  }
  throw new Error(`Emitter closed while waiting for '${event}'`);
}

/** Collect `count` matching payloads of `event` (event-driven, with a deadline). */
export async function collectEvents<T = unknown>(
  emitter: EventEmitter,
  event: string,
  count: number,
  predicate: (payload: T) => boolean = () => true,
  timeoutMs = 5000
): Promise<T[]> {
  const out: T[] = [];
  if (count <= 0) return out;
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    for await (const [payload] of on(emitter, event, { signal })) {
      if (predicate(payload as T)) out.push(payload as T);
      if (out.length >= count) return out;
    }
  } catch (err) {
    if (signal.aborted) {
      throw new Error(`Timed out after ${timeoutMs}ms: saw ${out.length}/${count} '${event}' events`, {
        cause: err,
      });
    }
    throw err;
  }
  return out;
}

/** A promise you resolve by hand (used to gate handlers deterministically). */
export function deferred<T = void>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
