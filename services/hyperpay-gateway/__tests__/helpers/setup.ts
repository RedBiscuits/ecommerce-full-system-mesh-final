import os from 'os';
import path from 'path';
import crypto from 'crypto';
import { beforeEach, afterAll } from 'vitest';
import { gatewayBreaker } from '../../src/services/gatewayBreaker';

process.env['DB_PATH'] = path.join(
  os.tmpdir(),
  'hyperpay-test-' + crypto.randomUUID() + '.json'
);
process.env['NODE_ENV'] = 'test';

beforeEach(() => {
  process.env['NODE_ENV'] = 'test';
  gatewayBreaker.reset('test isolation');
});

afterAll(() => {
  gatewayBreaker.stop();
});