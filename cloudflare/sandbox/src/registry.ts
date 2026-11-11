// Deployment-wide sandbox policy: Rust's `max_total` and `max_per_account`
// (RT/sandbox.rs:238-248) and the run admission semaphore (`concurrency`,
// sandbox.rs:357-363). Rust kept both in one process; here one object holds
// them for the deployment. State is in key-value storage so an eviction
// cannot reset a count; every entry carries its own expiry, so a workspace
// or a run that dies without saying so stops counting on its own.
import { DurableObject } from "cloudflare:workers";

interface SandboxEntry { scope: string; expiresAt: number }
interface RunLease { until: number }

const SANDBOX = "sandbox:";
const RUN = "run:";

export class SandboxRegistry extends DurableObject {
  private async live<T extends { expiresAt?: number; until?: number }>(prefix: string, now: number): Promise<Map<string, T>> {
    const entries = await this.ctx.storage.list<T>({ prefix });
    const expired: string[] = [];
    for (const [key, value] of entries) {
      if ((value.expiresAt ?? value.until ?? 0) <= now) expired.push(key);
    }
    for (const key of expired) entries.delete(key);
    if (expired.length > 0) await this.ctx.storage.delete(expired);
    return entries;
  }

  /** `create`'s two limits, then the entry. Answers Rust's error text, or null when admitted. */
  async admit(request: { sandboxId: string; scope: string; expiresAt: number; maxTotal: number; maxPerAccount: number }): Promise<string | null> {
    const live = await this.live<SandboxEntry>(SANDBOX, Date.now());
    if (live.size >= request.maxTotal) return "runtime sandbox limit reached";
    let owned = 0;
    for (const entry of live.values()) if (entry.scope === request.scope) owned++;
    if (owned >= request.maxPerAccount) return "sandbox limit reached for this account and tenant";
    await this.ctx.storage.put<SandboxEntry>(SANDBOX + request.sandboxId, { scope: request.scope, expiresAt: request.expiresAt });
    return null;
  }

  async forget(sandboxId: string): Promise<void> {
    await this.ctx.storage.delete(SANDBOX + sandboxId);
  }

  /** One run slot, held until `release` or `until`. */
  async acquire(request: { executionId: string; until: number; concurrency: number }): Promise<boolean> {
    const live = await this.live<RunLease>(RUN, Date.now());
    if (live.has(RUN + request.executionId)) return true;
    if (live.size >= request.concurrency) return false;
    await this.ctx.storage.put<RunLease>(RUN + request.executionId, { until: request.until });
    return true;
  }

  async release(executionId: string): Promise<void> {
    await this.ctx.storage.delete(RUN + executionId);
  }

  /** Test and operator view. */
  async counts(): Promise<{ sandboxes: number; runs: number }> {
    const now = Date.now();
    return { sandboxes: (await this.live(SANDBOX, now)).size, runs: (await this.live(RUN, now)).size };
  }
}
