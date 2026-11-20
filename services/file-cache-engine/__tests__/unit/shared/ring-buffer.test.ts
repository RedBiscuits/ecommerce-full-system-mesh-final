import { describe, expect, it } from 'vitest';

import { RingBuffer } from '../../../src/shared/ring-buffer';

describe('RingBuffer', () => {
  it('is FIFO', () => {
    const ring = new RingBuffer<number>(3);
    ring.push(1);
    ring.push(2);
    ring.push(3);
    expect([ring.shift(), ring.shift(), ring.shift()]).toEqual([1, 2, 3]);
    expect(ring.shift()).toBeUndefined();
  });

  it('rejects a push when full instead of growing or overwriting', () => {
    const ring = new RingBuffer<string>(2);
    expect(ring.push('a')).toBe(true);
    expect(ring.push('b')).toBe(true);
    expect(ring.isFull).toBe(true);
    expect(ring.push('c')).toBe(false);
    expect([...ring]).toEqual(['a', 'b']);
    expect(ring.size).toBe(2);
  });

  it('wraps around the end of its storage', () => {
    const ring = new RingBuffer<number>(3);
    const out: number[] = [];
    for (let i = 0; i < 10; i++) {
      ring.push(i);
      if (ring.size === 3) {
        out.push(ring.shift() ?? -1);
      }
    }
    expect(out).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect([...ring]).toEqual([8, 9]);
  });

  it('iterates head to tail without consuming', () => {
    const ring = new RingBuffer<number>(4);
    ring.push(1);
    ring.push(2);
    ring.shift();
    ring.push(3);
    expect([...ring]).toEqual([2, 3]);
    expect(ring.size).toBe(2);
  });

  it('clear() empties it and it stays usable', () => {
    const ring = new RingBuffer<number>(2);
    ring.push(1);
    ring.push(2);
    ring.clear();
    expect(ring.size).toBe(0);
    expect(ring.shift()).toBeUndefined();
    expect(ring.push(3)).toBe(true);
    expect([...ring]).toEqual([3]);
  });

  it('validates its capacity', () => {
    for (const capacity of [0, -1, 1.5, Number.NaN]) {
      expect(() => new RingBuffer(capacity)).toThrow(RangeError);
    }
  });
});
