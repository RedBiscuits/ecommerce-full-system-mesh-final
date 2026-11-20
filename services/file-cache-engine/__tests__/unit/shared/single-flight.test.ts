import { describe, expect, it } from 'vitest';

import { SingleFlight } from '../../../src/shared/single-flight';

describe('SingleFlight', () => {
  it('coalesces concurrent calls for the same key into one execution', async () => {
    const flight = new SingleFlight<number>();
    let calls = 0;
    const { promise, resolve } = Promise.withResolvers<number>();
    const task = (): Promise<number> => {
      calls += 1;
      return promise;
    };

    const results = Promise.all([
      flight.run('k', task),
      flight.run('k', task),
      flight.run('k', task),
    ]);
    expect(flight.isInFlight('k')).toBe(true);
    resolve(7);

    await expect(results).resolves.toEqual([7, 7, 7]);
    expect(calls).toBe(1);
    expect(flight.isInFlight('k')).toBe(false);
    expect(flight.size).toBe(0);
  });

  it('runs again once the previous flight has settled', async () => {
    const flight = new SingleFlight<number>();
    let calls = 0;
    const task = async (): Promise<number> => ++calls;

    await flight.run('k', task);
    await flight.run('k', task);
    expect(calls).toBe(2);
  });

  it('propagates rejections to every joined caller and clears the slot', async () => {
    const flight = new SingleFlight<never>();
    const task = async (): Promise<never> => {
      throw new Error('boom');
    };

    const a = flight.run('k', task);
    const b = flight.run('k', task);
    await expect(a).rejects.toThrow('boom');
    await expect(b).rejects.toThrow('boom');
    expect(flight.isInFlight('k')).toBe(false);
  });

  it('does not let a synchronous throw escape the promise', async () => {
    const flight = new SingleFlight<number>();
    const task = (): Promise<number> => {
      throw new Error('sync boom');
    };
    await expect(flight.run('k', task)).rejects.toThrow('sync boom');
  });
});
