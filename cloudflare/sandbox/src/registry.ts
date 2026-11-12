// Deployment-wide sandbox counts: Rust's `max_total` and `max_per_account`
// (RT/sandbox.rs:238-248). Rust kept them in one process; here one object holds
// them for the deployment. Run admission (`concurrency`) is per tenant, in the
// tenant's Container (0048 §2.3). State is in key-value storage so an eviction
// cannot reset a count; every entry carries its own expiry, so a workspace that
// dies without saying so stops counting on its own.
import { DurableObject } from "cloudflare:workers";

interface SandboxEntry { scope: string; expiresAt: number }

const SANDBOX = "sandbox:";

export class SandboxRegistry extends DurableObject {
  private async live(now: number): Promise<Map<string, SandboxEntry>> {
    const entries = await this.ctx.storage.list<SandboxEntry>({ prefix: SANDBOX });
    const expired: string[] = [];
    for (const [key, value] of entries) {
      if (value.expiresAt <= now) expired.push(key);
    }
    for (const key of expired) entries.delete(key);
    if (expired.length > 0) await this.ctx.storage.delete(expired);
    return entries;
  }

  /** `create`'s two limits, then the entry. Answers Rust's error text, or null when admitted. */
  async admit(request: { sandboxId: string; scope: string; expiresAt: number; maxTotal: number; maxPerAccount: number }): Promise<string | null> {
    const live = await this.live(Date.now());
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

  /** Test and operator view. */
  async counts(): Promise<{ sandboxes: number }> {
    return { sandboxes: (await this.live(Date.now())).size };
  }
}
