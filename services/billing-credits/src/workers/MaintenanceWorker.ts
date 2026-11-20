import { autoRechargeService } from '../services/AutoRechargeService';
import { CreditLedgerService, creditLedgerService } from '../services/CreditLedgerService';
import { StreamHoldService, streamHoldService } from '../services/StreamHoldService';

export class MaintenanceWorker {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(
    private readonly ledger: CreditLedgerService = creditLedgerService,
    private readonly holds: StreamHoldService = streamHoldService,
    private readonly intervalMs: number = 10000
  ) {}

  async runCycle(): Promise<{
    expiredLotsCount: number;
    expiredCreditsTotal: number;
    releasedHoldsCount: number;
    releasedHoldCredits: number;
    discrepanciesLogged: number;
    unbackedHoldCredits: number;
    rechargeOrdersCreated: number;
    rollupDeltasCompacted: number;
  }> {
    // Release abandoned holds first so their stale reservations are not reported as
    // EXPIRED_DURING_ACTIVE_HOLD discrepancies by the grant expiry sweep.
    const holdsResult = await this.holds.releaseExpiredHolds();
    const grantsResult = await this.ledger.expireStaleGrants();
    // Relay auto-recharge intents that no post-commit kick picked up (or that failed).
    const outbox = await autoRechargeService.processOutbox();
    const rollups = await this.ledger.compactUsageRollups();
    return {
      rechargeOrdersCreated: outbox.ordersCreated,
      rollupDeltasCompacted: rollups.deltasCompacted,
      expiredLotsCount: grantsResult.expiredLotsCount,
      expiredCreditsTotal: grantsResult.expiredCreditsTotal,
      releasedHoldsCount: holdsResult.releasedCount,
      releasedHoldCredits: holdsResult.releasedCredits,
      discrepanciesLogged: grantsResult.discrepanciesLogged,
      unbackedHoldCredits: grantsResult.unbackedHoldCredits,
    };
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(async () => {
      if (this.running) return;
      this.running = true;
      try {
        await this.runCycle();
      } catch (err) {
        console.error('[MaintenanceWorker] Maintenance sweep failed:', err);
      } finally {
        this.running = false;
      }
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

export const maintenanceWorker = new MaintenanceWorker();
