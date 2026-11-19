// X13g (0094, approved 2026-11-19, Alternative B): the per-tenant ActionHost.
//
// A socket's interactive agent Action (`action.call` of a `profile: "agent"` Action, X13's inline path)
// runs here instead of in a stateless ActionWorker invocation. On the platform a stateless Actions
// isolate was almost never reused between calls (dev, 2026-11-19: 8/8 rewrites paid the cold module
// evaluation, 238-297 ms CPU), while Rust's module host is one resident isolate per project, evaluated
// at activation. This object lives in the Actions Worker script, so Actions keep that script's CPU limit
// (300 s), bindings and capability checks, and never share the tenant's deterministic event loop.
//
// It stays resident only while its tenant has live subscribers, under the tenant's own no-expiry rule
// (BUG-9, `SubscriberKeepAlive`): the tenant sends `hold` when it gains its first subscriber (which also
// evaluates the module now, as Rust's host does at activation) and `release` when the last one goes.
// While held, one pending timer keeps the object from hibernating; every check it asks the tenant
// whether it still wants the host, so a tenant that went away without `release` frees it too.
//
// Outbox and scheduled Actions keep their Workflow hand-off. Every run stays durable in the tenant
// (`startCall` / `completeCall`, X13/X13b); this object holds no Action state of its own.
import { DurableObject } from "cloudflare:workers";
import { KEEPALIVE_CHECK_MS } from "../../kernel/delivery/keepalive";
import { prewarm, serveCall, type ActionEnv, type DirectCall } from "./worker.js";
import type { ActionReport } from "./runtime.js";

export interface HoldRequest { v: 1; projectId: string; tenantId: string; /** X13g review: newer wins; absent = legacy, always applied. */ generation?: number }
export interface ReleaseRequest { v: 1; generation?: number }
export interface HoldReply { v: 1; held: boolean; warm: boolean }

interface TenantWant { actionHostWanted(): Promise<boolean> }

export class ActionHost extends DurableObject<ActionEnv> {
  private held: HoldRequest | undefined;
  /** The newest hold/release generation applied: an older (reordered) one is ignored. */
  private generation = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  /** Test and operator view: module evaluations requested by prewarm (not by calls). */
  private prewarms = 0;

  /** Same as `ActionWorker.call`, in this resident isolate. */
  async call(input: DirectCall): Promise<ActionReport | { pending: true; id: string }> {
    // A call can arrive before `hold` (or after an eviction lost it): the next check asks the tenant.
    if (!this.held) this.held = { v: 1, projectId: input.session.projectId, tenantId: input.session.tenantId };
    this.arm();
    return serveCall(this.env, (work) => this.ctx.waitUntil(work), input);
  }

  /** The tenant gained a live subscriber: stay resident, and evaluate the module now. */
  async hold(request: HoldRequest): Promise<HoldReply> {
    if (this.stale(request.generation)) return { v: 1, held: this.held !== undefined, warm: false };
    this.held = request;
    this.arm();
    let warm = false;
    try { this.prewarms++; warm = prewarm(this.env, request.tenantId, request.projectId); }
    catch (error) { console.error("action host: prewarm failed", { error: String((error as Error)?.message ?? error).slice(0, 200) }); }
    return { v: 1, held: true, warm };
  }

  /** The tenant's last subscriber went: let the object hibernate. */
  async release(request?: ReleaseRequest): Promise<{ v: 1; held: boolean }> {
    if (this.stale(request?.generation)) return { v: 1, held: this.held !== undefined };
    return this.letGo();
  }

  /** True for a reordered hold/release older than one already applied; records a newer one. */
  private stale(generation: number | undefined): boolean {
    if (generation === undefined) return false;
    if (generation <= this.generation) return true;
    this.generation = generation;
    return false;
  }

  private letGo(): { v: 1; held: false } {
    this.held = undefined;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    return { v: 1, held: false };
  }

  /** Test seam: whether a timer holds the object, and how often prewarm ran. */
  async hostState(): Promise<{ held: boolean; holding: boolean; prewarms: number }> {
    return { held: this.held !== undefined, holding: this.timer !== undefined, prewarms: this.prewarms };
  }

  /** One pending timer while held (the platform does not hibernate an object with a pending timer). */
  private arm(): void {
    if (this.timer !== undefined || !this.held) return;
    this.timer = setTimeout(() => { this.timer = undefined; void this.check(); }, checkMs(this.env));
  }

  /** Still wanted? Ask the tenant; an unreachable or older tenant lets the host go. */
  private async check(): Promise<void> {
    const held = this.held;
    if (!held) return;
    let wanted = false;
    try {
      const tenant = (this.env.TENANT as unknown as { getByName(name: string): TenantWant }).getByName(held.tenantId);
      wanted = await tenant.actionHostWanted();
    } catch { wanted = false; }
    if (this.held !== held) return;
    if (wanted) this.arm(); else this.letGo();
  }
}

function checkMs(env: ActionEnv): number {
  const raw = (env as { GONVEX_ACTION_HOST_CHECK_MS?: string }).GONVEX_ACTION_HOST_CHECK_MS;
  const n = raw === undefined || raw === "" ? NaN : Number(raw);
  return Number.isInteger(n) && n > 0 && n <= 10 * 60_000 ? n : KEEPALIVE_CHECK_MS;
}
