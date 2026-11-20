import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { sleep } from '../../../src/shared/sleep';

describe('sleep', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('resolves after the given delay', async () => {
    let done = false;
    void sleep(100).then(() => (done = true));
    await vi.advanceTimersByTimeAsync(99);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(done).toBe(true);
  });

  it('rejects with the abort reason when aborted mid-sleep', async () => {
    const controller = new AbortController();
    const pending = sleep(1_000, { signal: controller.signal });
    controller.abort(new Error('stop'));
    await expect(pending).rejects.toThrow('stop');
  });

  it('rejects immediately when the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(
      sleep(1_000, { signal: controller.signal }),
    ).rejects.toMatchObject({ name: 'AbortError' });
  });

  it('keeps the event loop alive unless unref is requested', () => {
    const spy = vi.spyOn(globalThis, 'setTimeout');
    void sleep(10);
    void sleep(10, { unref: true });
    const [kept, released] = spy.mock.results.map((r) => r.value);
    expect(kept?.hasRef()).toBe(true);
    expect(released?.hasRef()).toBe(false);
  });
});
