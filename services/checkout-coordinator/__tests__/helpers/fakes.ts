import { DownstreamError } from '../../src/clients/http';
import type { BillingPort, CreditHoldPlaced, PlaceCreditHold, ReleaseReservation, TenderHold, TenderHoldPlaced, WalletPort } from '../../src/clients/ports';

type Fault = { kind: 'REJECTED' | 'UNAVAILABLE'; code: string; status: number; times: number };

/**
 * In-memory stand-ins with the downstream semantics the coordinator relies on: one hold per key,
 * replays return the same hold, a released (or pre-released: tombstone) key never places again.
 * Faults are injected per operation.
 */
abstract class FakeService {
  readonly calls: string[] = [];
  private readonly faults = new Map<string, Fault[]>();
  /** Runs after a successful operation committed downstream (e.g. to simulate a crash right after). */
  after: Partial<Record<string, () => void>> = {};

  constructor(private readonly service: DownstreamError['service']) {}

  fail(op: string, kind: Fault['kind'], code: string, times = 1, status = kind === 'REJECTED' ? 409 : 503): void {
    this.faults.set(op, [...(this.faults.get(op) ?? []), { kind, code, status, times }]);
  }

  protected enter(op: string): void {
    this.calls.push(op);
    const queue = this.faults.get(op);
    const fault = queue?.[0];
    if (!fault) return;
    fault.times -= 1;
    if (fault.times <= 0) queue.shift();
    throw new DownstreamError(this.service, fault.kind, fault.status, fault.code, `${this.service} ${op}: ${fault.code}`);
  }

  protected leave(op: string): void {
    this.after[op]?.();
  }
}

export class FakeBilling extends FakeService implements BillingPort {
  readonly holds = new Map<string, { holdId: string; orderId: string; credits: number; status: 'ACTIVE' | 'RELEASED' | 'SETTLED' | 'CANCELLED' }>();
  reservedCredits = 0;
  halalasPerCredit = 100;

  constructor() {
    super('billing-credits');
  }

  async placeCreditHold(input: PlaceCreditHold): Promise<CreditHoldPlaced> {
    this.enter('place');
    const existing = this.holds.get(input.holdKey);
    if (existing?.status === 'CANCELLED' || existing?.status === 'RELEASED') {
      throw new DownstreamError('billing-credits', 'REJECTED', 409, 'HOLD_CANCELLED', 'reservation cancelled');
    }
    const hold =
      existing ??
      (() => {
        const created = { holdId: `hold_${this.holds.size + 1}`, orderId: input.orderId, credits: input.credits, status: 'ACTIVE' as const };
        this.holds.set(input.holdKey, created);
        this.reservedCredits += input.credits;
        return created;
      })();
    this.leave('place');
    return {
      holdId: hold.holdId,
      status: hold.status,
      created: !existing,
      halalasPerCredit: this.halalasPerCredit,
      allowanceHalalas: hold.credits * this.halalasPerCredit,
    };
  }

  async releaseCreditHold(input: ReleaseReservation): Promise<{ outcome: string }> {
    this.enter('release');
    const hold = this.holds.get(input.holdKey);
    let outcome = 'ALREADY_RELEASED';
    if (!hold) {
      this.holds.set(input.holdKey, { holdId: '', orderId: input.orderId, credits: 0, status: 'CANCELLED' });
      outcome = 'TOMBSTONED';
    } else if (hold.status === 'SETTLED') {
      throw new DownstreamError('billing-credits', 'REJECTED', 409, 'HOLD_STATE_CONFLICT', 'Cannot release: the order credits were already captured');
    } else if (hold.status === 'ACTIVE') {
      hold.status = 'RELEASED';
      this.reservedCredits -= hold.credits;
      outcome = 'RELEASED';
    }
    this.leave('release');
    return { outcome };
  }

  active(): number {
    return [...this.holds.values()].filter((h) => h.status === 'ACTIVE').length;
  }
}

export class FakeWallet extends FakeService implements WalletPort {
  readonly holds = new Map<string, { holdId: string; amount: number; tender: string; status: 'HELD' | 'RELEASED' | 'CAPTURED' }>();
  balances = new Map<string, number>([['wallet_noura_sar', 50000]]);
  escrow = 0;

  constructor() {
    super('wallet-ledger');
  }

  async placeTenderHold(input: TenderHold): Promise<TenderHoldPlaced> {
    this.enter('place');
    const existing = this.holds.get(input.holdKey);
    if (existing?.status === 'RELEASED') throw new DownstreamError('wallet-ledger', 'REJECTED', 409, 'HOLD_RELEASED', 'released');
    if (existing) {
      this.leave('place');
      return { holdId: existing.holdId, status: existing.status, created: false };
    }
    if (input.tender === 'WALLET') {
      const balance = this.balances.get(input.sourceWalletId as string);
      if (balance === undefined) throw new DownstreamError('wallet-ledger', 'REJECTED', 404, 'WALLET_NOT_FOUND', 'no wallet');
      if (balance < input.amountHalalas) throw new DownstreamError('wallet-ledger', 'REJECTED', 409, 'INSUFFICIENT_FUNDS', 'insufficient funds');
      this.balances.set(input.sourceWalletId as string, balance - input.amountHalalas);
      this.escrow += input.amountHalalas;
    }
    const hold = { holdId: `ph_${this.holds.size + 1}`, amount: input.amountHalalas, tender: input.tender, status: 'HELD' as const };
    this.holds.set(input.holdKey, hold);
    this.leave('place');
    return { holdId: hold.holdId, status: 'HELD', created: true };
  }

  async releaseTenderHold(input: TenderHold & { reason: string }): Promise<{ outcome: string }> {
    this.enter('release');
    const hold = this.holds.get(input.holdKey);
    let outcome = 'ALREADY_RELEASED';
    if (!hold) {
      this.holds.set(input.holdKey, { holdId: 'tombstone', amount: input.amountHalalas, tender: input.tender, status: 'RELEASED' });
      outcome = 'TOMBSTONED';
    } else if (hold.status === 'CAPTURED') {
      throw new DownstreamError('wallet-ledger', 'REJECTED', 409, 'HOLD_ALREADY_CAPTURED', 'captured');
    } else if (hold.status === 'HELD') {
      hold.status = 'RELEASED';
      if (hold.tender === 'WALLET') {
        this.escrow -= hold.amount;
        this.balances.set(input.sourceWalletId as string, (this.balances.get(input.sourceWalletId as string) ?? 0) + hold.amount);
      }
      outcome = 'RELEASED';
    }
    this.leave('release');
    return { outcome };
  }

  held(): number {
    return [...this.holds.values()].filter((h) => h.status === 'HELD').length;
  }
}
