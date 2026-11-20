import { afterEach } from 'vitest';
import { autoRechargeService } from '../../src/services/AutoRechargeService';

// Auto-recharge relay runs are kicked asynchronously after commits; never let one bleed into the
// next test's database reset.
afterEach(async () => {
  await autoRechargeService.whenOutboxIdle();
});
