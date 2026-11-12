// Run admission per tenant (0048 §2.3): Rust's `concurrency` permits, held in
// one object per tenant. Leases carry their own expiry, so a run that dies
// without releasing stops counting by itself.
import { DurableObject } from "cloudflare:workers";

interface RunLease { until: number }

export class RunSlots extends DurableObject {
  async acquire(request: { executionId: string; until: number; concurrency: number }): Promise<boolean> {
    const now = Date.now();
    const leases = await this.ctx.storage.list<RunLease>();
    const expired = [...leases].filter(([, lease]) => lease.until <= now).map(([key]) => key);
    if (expired.length > 0) await this.ctx.storage.delete(expired);
    for (const key of expired) leases.delete(key);
    if (leases.has(request.executionId)) return true;
    if (leases.size >= request.concurrency) return false;
    await this.ctx.storage.put<RunLease>(request.executionId, { until: request.until });
    return true;
  }

  async release(executionId: string): Promise<void> {
    await this.ctx.storage.delete(executionId);
  }
}
