import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  DEFAULT_RETRY_POLICY,
  backoffDelay,
  withRetry,
} from '../../../src/resilience/retry';
import { errno } from '../../helpers/faulty-store';

describe('withRetry', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('retries transient failures with backoff and eventually succeeds', async () => {
    const task = vi
      .fn<(attempt: number) => Promise<string>>()
      .mockRejectedValueOnce(errno('EMFILE'))
      .mockRejectedValueOnce(errno('EBUSY'))
      .mockResolvedValue('ok');
    const onRetry = vi.fn<(e: unknown, a: number, d: number) => void>();

    const pending = withRetry(task, { jitterRatio: 0 }, { onRetry });
    await vi.advanceTimersByTimeAsync(20 + 40);
    await expect(pending).resolves.toBe('ok');
    expect(task).toHaveBeenCalledTimes(3);
    expect(
      onRetry.mock.calls.map(([, attempt, delay]) => [attempt, delay]),
    ).toEqual([
      [1, 20],
      [2, 40],
    ]);
  });

  it('gives up after maxAttempts and rethrows the last error unchanged', async () => {
    const last = errno('EAGAIN', 'still busy');
    const task = vi.fn<() => Promise<never>>().mockRejectedValue(last);
    const outcome = withRetry(task, { maxAttempts: 3, jitterRatio: 0 }).then(
      () => 'resolved',
      (error: unknown) => error,
    );
    await vi.advanceTimersByTimeAsync(1_000);
    await expect(outcome).resolves.toBe(last);
    expect(task).toHaveBeenCalledTimes(3);
  });

  it('does not retry non-transient failures', async () => {
    const task = vi
      .fn<() => Promise<never>>()
      .mockRejectedValue(errno('EROFS'));
    await expect(withRetry(task)).rejects.toMatchObject({ code: 'EROFS' });
    expect(task).toHaveBeenCalledTimes(1);
  });

  it('honours a custom shouldRetry predicate', async () => {
    const task = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(new Error('flaky'))
      .mockResolvedValue('ok');
    const pending = withRetry(task, {
      shouldRetry: (error) =>
        error instanceof Error && error.message === 'flaky',
      jitterRatio: 0,
    });
    await vi.advanceTimersByTimeAsync(20);
    await expect(pending).resolves.toBe('ok');
  });

  it('stops when aborted during backoff', async () => {
    const controller = new AbortController();
    const task = vi
      .fn<() => Promise<never>>()
      .mockRejectedValue(errno('EMFILE'));
    const outcome = withRetry(
      task,
      { jitterRatio: 0 },
      { signal: controller.signal },
    ).then(
      () => 'resolved',
      (error: unknown) => error,
    );
    await vi.advanceTimersByTimeAsync(5);
    controller.abort(new Error('shutting down'));
    await expect(outcome).resolves.toMatchObject({ message: 'shutting down' });
    expect(task).toHaveBeenCalledTimes(1);
  });

  it('computes exponential delays capped at maxDelayMs with bounded jitter', () => {
    const policy = {
      ...DEFAULT_RETRY_POLICY,
      baseDelayMs: 100,
      maxDelayMs: 350,
      jitterRatio: 0.5,
    };
    expect(backoffDelay(policy, 1, () => 0.5)).toBe(100);
    expect(backoffDelay(policy, 2, () => 0.5)).toBe(200);
    expect(backoffDelay(policy, 3, () => 0.5)).toBe(350);
    expect(backoffDelay(policy, 1, () => 0)).toBe(50);
    expect(backoffDelay(policy, 1, () => 1)).toBe(150);
  });
});
