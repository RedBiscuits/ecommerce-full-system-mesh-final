import type { ServerResponse } from 'node:http';
import { Readable } from 'node:stream';

import { afterEach, describe, expect, it } from 'vitest';

import type { CaptureOutcome } from '../../../src/http/response-capture';
import { captureResponse } from '../../../src/http/response-capture';
import { startServer, type TestServer } from '../../helpers/http-server';

describe('captureResponse', () => {
  let server: TestServer | undefined;

  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  async function run(
    handler: (res: ServerResponse) => void,
    maxBytes = 1024,
    fetchInit?: RequestInit,
  ): Promise<{ outcome: CaptureOutcome; response: Response | undefined }> {
    let outcome: Promise<CaptureOutcome> | undefined;
    server = await startServer([], (_req, res) => {
      outcome = captureResponse(res, { maxBytes });
      handler(res);
    });
    const response = await fetch(server.url, fetchInit).catch(() => undefined);
    if (outcome === undefined) {
      throw new Error('handler did not run');
    }
    return { outcome: await outcome, response };
  }

  it('captures status, headers and a chunked body while streaming it', async () => {
    const { outcome, response } = await run((res) => {
      res.statusCode = 201;
      res.setHeader('x-a', '1');
      res.write('hello ');
      res.write(Buffer.from('wor'));
      res.end(new Uint8Array([0x6c, 0x64]));
    });
    expect(outcome).toEqual({
      kind: 'complete',
      response: {
        status: 201,
        headers: expect.objectContaining({ 'x-a': '1' }),
        body: Buffer.from('hello world'),
      },
    });
    expect(response?.status).toBe(201);
    await expect(response?.text()).resolves.toBe('hello world');
  });

  it('captures headers and status passed to writeHead (both signatures)', async () => {
    const { outcome } = await run((res) => {
      res.setHeader('x-set', 'a');
      res.writeHead(202, 'Accepted', { 'x-head': 'b' });
      res.end();
    });
    expect(outcome).toMatchObject({
      kind: 'complete',
      response: { status: 202, headers: { 'x-set': 'a', 'x-head': 'b' } },
    });
    await server?.close();
    const second = await run((res) => {
      res.writeHead(204, { 'x-only': 'c' });
      res.end();
    });
    expect(second.outcome).toMatchObject({
      kind: 'complete',
      response: { status: 204, headers: { 'x-only': 'c' } },
    });
  });

  it('honours string encodings and end callbacks', async () => {
    let callbackRan = false;
    const { outcome } = await run((res) => {
      res.write('68656c6c6f', 'hex');
      res.end(() => {
        callbackRan = true;
      });
    });
    expect(outcome).toMatchObject({
      kind: 'complete',
      response: { body: Buffer.from('hello') },
    });
    expect(callbackRan).toBe(true);
  });

  it('works with piped streams', async () => {
    const { outcome } = await run((res) => {
      Readable.from(['a', 'b', 'c']).pipe(res);
    });
    expect(outcome).toMatchObject({
      kind: 'complete',
      response: { body: Buffer.from('abc') },
    });
  });

  it('stops buffering oversized bodies but lets them stream through', async () => {
    const { outcome, response } = await run((res) => {
      res.write('x'.repeat(600));
      res.end('y'.repeat(600));
    }, 1000);
    expect(outcome).toEqual({ kind: 'oversized' });
    expect((await response?.text())?.length).toBe(1200);
  });

  it('reports aborted when the client disconnects mid-response', async () => {
    const controller = new AbortController();
    const { outcome } = await run(
      (res) => {
        res.write('partial');
        setTimeout(() => controller.abort(), 20);
        res.on('close', () => {
          if (!res.writableEnded) {
            res.destroy();
          }
        });
      },
      1024,
      { signal: controller.signal },
    );
    expect(outcome).toEqual({ kind: 'aborted' });
  });

  it('restores the original methods once settled', async () => {
    let res: ServerResponse | undefined;
    server = await startServer([], (_req, response) => {
      res = response;
      void captureResponse(response, { maxBytes: 10 });
      response.end('ok');
    });
    await fetch(server.url);
    expect(res).toBeDefined();
    expect(res?.write).toBe(Object.getPrototypeOf(res).write);
    expect(res?.end).toBe(Object.getPrototypeOf(res).end);
    expect(res?.writeHead).toBe(Object.getPrototypeOf(res).writeHead);
  });
});
