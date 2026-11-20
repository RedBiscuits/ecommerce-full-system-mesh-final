/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/* eslint-disable @typescript-eslint/no-unused-vars */
/**
 * tests/helpers/waitFor.ts
 *
 * Poll until a condition holds (for asynchronous, push-driven flows such as
 * the fake broker delivering to a consumer).
 */

export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  { timeoutMs = 2000, intervalMs = 5, label = 'condition' }: { timeoutMs?: number; intervalMs?: number; label?: string } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await condition()) return;
    if (Date.now() > deadline) throw new Error(`waitFor: timed out after ${timeoutMs}ms waiting for ${label}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/** Let pending macrotasks (setImmediate / resolved promises) run. */
export function flush(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
