import { describe, expect, it } from 'vitest';

import {
  FrequencySketch,
  MAX_FREQUENCY,
  MAX_SKETCH_CAPACITY,
  counterAt,
  halveCounters,
  incrementCounter,
} from '../../../src/cache/tiny-lfu';

const LANES = [0, 1, 2, 3] as const;

/** Scheduler that holds the reset until the test runs it. */
function manualScheduler(): {
  schedule: (reset: () => void) => void;
  queued: Array<() => void>;
  runAll: () => void;
} {
  const queued: Array<() => void> = [];
  return {
    queued,
    schedule: (reset): void => {
      queued.push(reset);
    },
    runAll: (): void => {
      for (const reset of queued.splice(0)) {
        reset();
      }
    },
  };
}

function hit(sketch: FrequencySketch, key: string, times: number): void {
  for (let i = 0; i < times; i++) {
    sketch.increment(key);
  }
}

describe('16-bit block arithmetic (exhaustive)', () => {
  it('increments exactly one lane, saturating at 15 without carrying into neighbours', () => {
    const violations: string[] = [];
    for (let block = 0; block <= 0xffff; block++) {
      for (const lane of LANES) {
        const next = incrementCounter(block, lane);
        const expected = Math.min(counterAt(block, lane) + 1, MAX_FREQUENCY);
        if (next < 0 || next > 0xffff) {
          violations.push(`${block}/${lane}: overflowed 16 bits (${next})`);
        }
        if (counterAt(next, lane) !== expected) {
          violations.push(`${block}/${lane}: counter not ${expected}`);
        }
        for (const other of LANES) {
          if (
            other !== lane &&
            counterAt(next, other) !== counterAt(block, other)
          ) {
            violations.push(`${block}/${lane}: leaked into lane ${other}`);
          }
        }
      }
    }
    expect(violations).toEqual([]);
  });

  it('halves every lane independently (floor), with no bit bleeding between lanes', () => {
    const violations: string[] = [];
    for (let block = 0; block <= 0xffff; block++) {
      const halved = halveCounters(block);
      for (const lane of LANES) {
        if (counterAt(halved, lane) !== counterAt(block, lane) >>> 1) {
          violations.push(`${block}/${lane}`);
        }
      }
    }
    expect(violations).toEqual([]);
  });
});

describe('FrequencySketch', () => {
  const inline = (reset: () => void): void => reset();

  it('counts accesses and saturates at 15', () => {
    const sketch = new FrequencySketch({ capacity: 64, seed: 1 });
    expect(sketch.frequency('a')).toBe(0);
    hit(sketch, 'a', 3);
    expect(sketch.frequency('a')).toBe(3);
    hit(sketch, 'a', 100);
    expect(sketch.frequency('a')).toBe(MAX_FREQUENCY);
  });

  it('keeps a hammered key from inflating unrelated keys', () => {
    const sketch = new FrequencySketch({
      capacity: 1_024,
      seed: 7,
      sampleSize: 1e9,
    });
    hit(sketch, 'hot', 10_000);
    let inflated = 0;
    for (let i = 0; i < 1_000; i++) {
      if (sketch.frequency(`cold-${i}`) !== 0) {
        inflated += 1;
      }
    }
    // Count-min: a cold key reads non-zero only if *all four* of its
    // counters collide with the hot key's, which is vanishingly rare.
    expect(inflated).toBe(0);
  });

  it('ranks re-read keys above a one-pass scan (scan resistance)', () => {
    const sketch = new FrequencySketch({ capacity: 1_000, seed: 42 });
    const hot = Array.from({ length: 50 }, (_, i) => `hot-${i}`);
    for (let round = 0; round < 5; round++) {
      for (const key of hot) {
        sketch.increment(key);
      }
      // A scan of never-repeated keys interleaved with the real workload.
      for (let i = 0; i < 400; i++) {
        sketch.increment(`scan-${round}-${i}`);
      }
    }
    const scanMax = Math.max(
      ...Array.from({ length: 400 }, (_, i) => sketch.frequency(`scan-0-${i}`)),
    );
    const hotMin = Math.min(...hot.map((key) => sketch.frequency(key)));
    expect(scanMax).toBeLessThan(hotMin);
  });

  it('queues halving once sampleSize increments are recorded, and halves on run', () => {
    const scheduler = manualScheduler();
    const sketch = new FrequencySketch({
      capacity: 64,
      sampleSize: 10,
      seed: 3,
      scheduleReset: scheduler.schedule,
    });
    hit(sketch, 'a', 9);
    expect(sketch.isResetPending).toBe(false);
    expect(scheduler.queued).toHaveLength(0);

    sketch.increment('b'); // 10th recorded increment fills the sample
    expect(sketch.isResetPending).toBe(true);
    expect(scheduler.queued).toHaveLength(1);
    // Nothing is halved on the caller's path.
    expect(sketch.frequency('a')).toBe(9);

    // Further increments while queued do not queue a second reset.
    hit(sketch, 'c', 5);
    expect(scheduler.queued).toHaveLength(1);
    expect(sketch.size).toBe(15);

    scheduler.runAll();
    expect(sketch.isResetPending).toBe(false);
    expect(sketch.frequency('a')).toBe(4); // floor(9 / 2)
    expect(sketch.frequency('b')).toBe(0); // floor(1 / 2)
    expect(sketch.frequency('c')).toBe(2); // floor(5 / 2)
    expect(sketch.size).toBeLessThanOrEqual(Math.floor(15 / 2));
  });

  it('ages out yesterday’s hot key in favour of today’s', () => {
    const sketch = new FrequencySketch({
      capacity: 64,
      sampleSize: 100,
      seed: 11,
      scheduleReset: inline,
    });
    hit(sketch, 'old', MAX_FREQUENCY);
    // 30 rounds over 10 new keys: fills the sample (and halves) 3 times.
    for (let round = 0; round < 30; round++) {
      for (let i = 0; i < 10; i++) {
        sketch.increment(`new-${i}`);
      }
    }
    const old = sketch.frequency('old');
    expect(old).toBeLessThanOrEqual(1); // 15 -> 7 -> 3 -> 1
    expect(sketch.frequency('new-0')).toBeGreaterThan(old);
  });

  it('does not count increments of a fully saturated key towards the sample', () => {
    const sketch = new FrequencySketch({
      capacity: 64,
      sampleSize: 1_000,
      seed: 5,
    });
    hit(sketch, 'a', MAX_FREQUENCY);
    const size = sketch.size;
    hit(sketch, 'a', 500);
    expect(sketch.size).toBe(size);
    expect(sketch.isResetPending).toBe(false);
  });

  it('defers halving to the next event-loop turn by default', async () => {
    const sketch = new FrequencySketch({
      capacity: 16,
      sampleSize: 8,
      seed: 9,
    });
    hit(sketch, 'a', 8);
    expect(sketch.isResetPending).toBe(true);
    expect(sketch.frequency('a')).toBe(8);

    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(sketch.isResetPending).toBe(false);
    expect(sketch.frequency('a')).toBe(4);
  });

  it('clear() zeroes counters and cancels a queued halving', () => {
    const scheduler = manualScheduler();
    const sketch = new FrequencySketch({
      capacity: 16,
      sampleSize: 4,
      seed: 2,
      scheduleReset: scheduler.schedule,
    });
    hit(sketch, 'a', 4);
    expect(sketch.isResetPending).toBe(true);

    sketch.clear();
    expect(sketch.isResetPending).toBe(false);
    expect(sketch.size).toBe(0);
    expect(sketch.frequency('a')).toBe(0);

    hit(sketch, 'b', 2);
    scheduler.runAll(); // the stale reset must be a no-op
    expect(sketch.frequency('b')).toBe(2);
  });

  it('re-queues the reset if more than a full sample arrived while queued', () => {
    const scheduler = manualScheduler();
    const sketch = new FrequencySketch({
      capacity: 256,
      sampleSize: 10,
      seed: 4,
      scheduleReset: scheduler.schedule,
    });
    // Even counts: halving loses nothing to truncation, so size just halves.
    for (let i = 0; i < 20; i++) {
      hit(sketch, `k-${i}`, 2);
    }
    expect(sketch.size).toBe(40);
    scheduler.runAll();
    expect(sketch.size).toBe(20);
    expect(sketch.isResetPending).toBe(true);
    // Now every counter is 1 (odd) and halves to 0: the sample empties.
    scheduler.runAll();
    expect(sketch.size).toBe(0);
    expect(sketch.frequency('k-0')).toBe(0);
    expect(sketch.isResetPending).toBe(false);
  });

  it('retries scheduling on a later increment if the scheduler throws', () => {
    let fail = true;
    let resets = 0;
    const sketch = new FrequencySketch({
      capacity: 16,
      sampleSize: 2,
      seed: 6,
      scheduleReset: (reset): void => {
        if (fail) {
          throw new Error('queue full');
        }
        resets += 1;
        reset();
      },
    });
    sketch.increment('a');
    expect(() => sketch.increment('b')).toThrow('queue full');
    expect(sketch.isResetPending).toBe(false);

    fail = false;
    sketch.increment('c');
    expect(resets).toBe(1);
  });

  it('defaults sampleSize to 10 × capacity', () => {
    expect(new FrequencySketch({ capacity: 100 }).sampleSize).toBe(1_000);
  });

  it('rejects invalid options', () => {
    for (const capacity of [0, -1, 1.5, Number.NaN, MAX_SKETCH_CAPACITY + 1]) {
      expect(() => new FrequencySketch({ capacity })).toThrow(RangeError);
    }
    for (const sampleSize of [0, -5, 2.5, Number.POSITIVE_INFINITY]) {
      expect(() => new FrequencySketch({ capacity: 8, sampleSize })).toThrow(
        RangeError,
      );
    }
  });
});
