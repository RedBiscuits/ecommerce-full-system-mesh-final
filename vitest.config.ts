import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['__tests__/**/*.test.ts', 'shared/**/__tests__/**/*.test.ts', 'services/**/__tests__/**/*.test.ts'],
    globalSetup: ['__tests__/setup/postgres.global.ts'],
    setupFiles: [
      'services/billing-credits/__tests__/helpers/setup.ts',
      'services/hyperpay-gateway/__tests__/helpers/setup.ts',
    ],
    // Platform tests boot five services (four embedded PostgreSQL instances) in one process.
    testTimeout: 60000,
    hookTimeout: 60000,
    fileParallelism: false,
  },
});
