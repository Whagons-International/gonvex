/**
 * Per-address budgets across isolates (contract-changes 0040).
 *
 * Rust keeps three process-local maps per client address
 * (`PublicLimiter`, public_functions.rs:81-176): pending sockets, public
 * sockets and the call window. On Cloudflare a client's sockets land on
 * different gateway objects (a shard of the connection id) and its upgrades
 * on different Worker isolates, so each address bucket gets one Durable
 * Object, `addr:<bucket>`, that holds those three counters for it. Every
 * isolate and gateway asks the same object, so a limit trips at exactly the
 * configured count however the sockets are spread.
 *
 * - **Memory only** (design rule: nothing ephemeral in SQLite). If the object
 *   is evicted while idle, its counters restart at zero, which can only admit,
 *   never refuse, a client. Slots are held per connection id, so a release
 *   that arrives after a restart is a no-op.
 * - **Leaked slots.** A slot is released when its socket closes, and a pending
 *   slot also when the socket authenticates. A gateway that is reset loses its
 *   sockets without running its close handlers; their slots would then stay
 *   held. So when a bucket is at its limit, the object asks each gateway
 *   holding its older slots which of those connections are still open
 *   (`liveConnections`), drops the rest, and decides again. Refusal is the
 *   only path that pays for this, at most once per 10 s per bucket.
 */
import { DurableObject } from "cloudflare:workers";
import { allowCalls, type CallWindow, newWindow } from "./window";

export const PENDING_LIMIT_ERROR = "pending connection limit exceeded";
export const PUBLIC_LIMIT_ERROR = "public connection limit exceeded";

/** A slot this young is never audited: its upgrade may not have reached the gateway yet. */
export const AUDIT_GRACE_MS = 10_000;
/** At most one audit per kind per this interval (refusals under attack stay cheap). */
export const AUDIT_INTERVAL_MS = 10_000;

export type SlotKind = "pending" | "public";

export interface AdmitRequest {
  v: 1;
  connectionId: string;
  /** The gateway object that will hold the socket (asked during an audit). */
  gateway: string;
  publicTransport: boolean;
  pendingLimit: number;
  publicLimit: number;
}
export type AdmitResult = { ok: true } | { ok: false; error: typeof PENDING_LIMIT_ERROR | typeof PUBLIC_LIMIT_ERROR };

export interface ReleaseRequest { v: 1; connectionId: string; kinds: SlotKind[] }
export interface AllowRequest { v: 1; cost: number; limit: number }

interface Slot { gateway: string; at: number }

/** What the object asks a gateway during an audit. */
export interface LiveConnections {
  liveConnections(connectionIds: string[]): Promise<string[]>;
}

/** The counters of one address bucket. Plain, so tests drive it without an object. */
export class AddressBudget {
  readonly slots: Record<SlotKind, Map<string, Slot>> = { pending: new Map(), public: new Map() };
  private window: CallWindow | null = null;
  private audited: Record<SlotKind, number> = { pending: -Infinity, public: -Infinity };

  constructor(private readonly gateway: (name: string) => LiveConnections, private readonly now: () => number = Date.now) {}

  /**
   * `admit_pending`, then `admit` for the public transport (lib.rs:1590-1617):
   * a refused public slot leaves no pending slot behind.
   */
  async admit(request: AdmitRequest): Promise<AdmitResult> {
    if (!await this.room("pending", request.pendingLimit)) return { ok: false, error: PENDING_LIMIT_ERROR };
    if (request.publicTransport && !await this.room("public", request.publicLimit)) return { ok: false, error: PUBLIC_LIMIT_ERROR };
    // Decided again after the awaits above, with no await in between: two admits never share the last slot.
    if (!this.fits("pending", request.pendingLimit) || (request.publicTransport && !this.fits("public", request.publicLimit))) {
      return this.admit(request);
    }
    const slot = { gateway: request.gateway, at: this.now() };
    this.slots.pending.set(request.connectionId, slot);
    if (request.publicTransport) this.slots.public.set(request.connectionId, slot);
    return { ok: true };
  }

  release(request: ReleaseRequest): void {
    for (const kind of request.kinds) this.slots[kind].delete(request.connectionId);
  }

  /** The per-address half of `PublicLimiter::allow` (the window starts at the first call, as `windows.entry(ip).or_default()`). */
  allow(request: AllowRequest): boolean {
    const now = this.now();
    this.window ??= newWindow(now);
    return allowCalls(this.window, request.cost, request.limit, now);
  }

  count(kind: SlotKind): number { return this.slots[kind].size; }

  private fits(kind: SlotKind, limit: number): boolean {
    return limit > 0 && this.slots[kind].size < limit;
  }

  /** Room for one more slot, auditing leaked slots first when full. */
  private async room(kind: SlotKind, limit: number): Promise<boolean> {
    if (this.fits(kind, limit)) return true;
    if (limit <= 0) return false;
    await this.audit(kind);
    return this.fits(kind, limit);
  }

  private async audit(kind: SlotKind): Promise<void> {
    const now = this.now();
    if (now - this.audited[kind] < AUDIT_INTERVAL_MS) return;
    this.audited[kind] = now;
    const byGateway = new Map<string, string[]>();
    for (const [id, slot] of this.slots[kind]) {
      if (now - slot.at < AUDIT_GRACE_MS) continue;
      let ids = byGateway.get(slot.gateway);
      if (!ids) byGateway.set(slot.gateway, ids = []);
      ids.push(id);
    }
    await Promise.all([...byGateway].map(async ([gateway, ids]) => {
      let live: Set<string>;
      try {
        live = new Set(await this.gateway(gateway).liveConnections(ids));
      } catch (error) {
        // An unreachable gateway keeps its slots: refusing is safe, freeing a live socket's slot is not.
        console.error(`public limiter: audit of ${gateway} failed`, error);
        return;
      }
      for (const id of ids) {
        if (live.has(id)) continue;
        this.slots.pending.delete(id);
        this.slots.public.delete(id);
      }
    }));
  }
}

export interface PublicLimiterEnv {
  GATEWAY: DurableObjectNamespace<any>;
}

/** The limiter object of one address bucket, bound as `PUBLIC_LIMITER` and named `limiterName(bucket)`. */
export class PublicLimiterObject extends DurableObject<PublicLimiterEnv> {
  private readonly budget: AddressBudget;

  constructor(ctx: DurableObjectState, env: PublicLimiterEnv) {
    super(ctx, env);
    const gateways = env.GATEWAY as unknown as { getByName(name: string): LiveConnections };
    this.budget = new AddressBudget((name) => gateways.getByName(name));
  }

  async admit(request: AdmitRequest): Promise<AdmitResult> { return this.budget.admit(request); }
  async release(request: ReleaseRequest): Promise<void> { this.budget.release(request); }
  async allow(request: AllowRequest): Promise<boolean> { return this.budget.allow(request); }
  /** Test and operator seam: held slots per kind. */
  async counts(): Promise<Record<SlotKind, number>> { return { pending: this.budget.count("pending"), public: this.budget.count("public") }; }
}

export const limiterName = (bucket: string) => `addr:${bucket}`;

/** The RPC shape of `PublicLimiterObject` (stubs are typed through this, not the class). */
export interface PublicLimiterRpc {
  admit(request: AdmitRequest): Promise<AdmitResult>;
  release(request: ReleaseRequest): Promise<void>;
  allow(request: AllowRequest): Promise<boolean>;
  counts(): Promise<Record<SlotKind, number>>;
}

export function limiterFor(namespace: DurableObjectNamespace<any>, bucket: string): PublicLimiterRpc {
  return (namespace as unknown as { getByName(name: string): PublicLimiterRpc }).getByName(limiterName(bucket));
}
