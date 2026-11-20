/**
 * Resolves after `ms` milliseconds, or rejects with the abort reason if
 * `signal` fires first.
 *
 * The timer keeps the event loop alive by default, because a caller awaiting
 * a sleep is doing real work. Pass `unref: true` only for opportunistic
 * background scheduling that must never block process exit.
 */
export function sleep(
  ms: number,
  options: { signal?: AbortSignal; unref?: boolean } = {},
): Promise<void> {
  const { signal, unref = false } = options;
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(toAbortError(signal.reason));
      return;
    }

    const onAbort = (): void => {
      clearTimeout(timer);
      reject(toAbortError(signal?.reason));
    };

    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    if (unref) {
      timer.unref();
    }

    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

function toAbortError(reason: unknown): Error {
  if (reason instanceof Error) {
    return reason;
  }
  return new DOMException('The operation was aborted.', 'AbortError');
}
