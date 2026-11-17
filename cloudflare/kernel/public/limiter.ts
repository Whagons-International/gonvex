/**
 * Per-address budgets across isolates (contract-changes 0042, accepted in 0045).
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
 * - **Slots are persisted** (0045): one small KV entry per held connection,
 *   written when a slot is taken or freed and read back when the object
 *   wakes, so an evicted limiter cannot reset the 20-socket and 100-pending
 *   caps while public sockets hibernate. At most `pending` entries per bucket
 *   (100 by default); a bucket with no socket stores nothing.
 * - **The call window stays in memory.** It is 60 s of ephemeral state (design
 *   rule: nothing ephemeral in SQLite), and an idle object only restarts it.
 * - **Leaked slots.** A slot is released when its socket closes, and a pending
 *   slot also when the socket authenticates. A gateway that is reset loses its
 *   sockets without running its close handlers; their slots would then stay
 *   held. So when a bucket is at its limit, the object asks each gateway
 *   holding its older slots which of those connections are still open
 *   (`liveConnections`), drops the rest, and decides again (at most once per
 *   10 s per kind). An hourly alarm runs the same audit over every stored slot
 *   while any remain, so a leak in a bucket that never fills is not kept forever.
 */
import { DurableObject } from "cloudflare:workers";
import { allowCalls, type CallWindow, newWindow } from "./window";

export const PENDING_LIMIT_ERROR = "pending connection limit exceeded";
export const PUBLIC_LIMIT_ERROR = "public connection limit exceeded";
export const ADMISSION_EXPIRED_ERROR = "connection admission expired";

/** A slot this young is never audited: its upgrade may not have reached the gateway yet. */
export const AUDIT_GRACE_MS = 10_000;
/** At most one audit per kind per this interval (refusals under attack stay cheap). */
export const AUDIT_INTERVAL_MS = 10_000;
/** While slots are stored, the object audits all of them this often (alarm). */
export const SWEEP_INTERVAL_MS = 60 * 60_000;

export type SlotKind = "pending" | "public";

export interface AdmitRequest {
  v: 1;
  connectionId: string;
  /** The gateway object that will hold the socket (asked during an audit). */
  gateway: string;
  publicTransport: boolean;
  pendingLimit: number;
  publicLimit: number;
  /** Router deadline: a delayed audit/RPC must not install slots for an abandoned upgrade. */
  expiresAt?: number;
}
export type AdmitResult = { ok: true } | { ok: false; error: typeof PENDING_LIMIT_ERROR | typeof PUBLIC_LIMIT_ERROR | typeof ADMISSION_EXPIRED_ERROR };

export interface ReleaseRequest { v: 1; connectionId: string; kinds: SlotKind[] }
export interface AllowRequest { v: 1; cost: number; limit: number }

interface Slot { gateway: string; at: number }

/** One held connection as stored: its gateway, when it was admitted, and the kinds it still holds. */
export interface StoredSlot { gateway: string; at: number; kinds: SlotKind[] }

/** Where the slots live across evictions: the object's KV storage, or memory in unit tests. */
export interface SlotStore {
  load(): Iterable<[string, StoredSlot]>;
  put(connectionId: string, slot: StoredSlot): void;
  delete(connectionId: string): void;
}

export class MemorySlotStore implements SlotStore {
  readonly entries = new Map<string, StoredSlot>();
  load() { return [...this.entries].map(([k, v]) => [k, { ...v, kinds: [...v.kinds] }] as [string, StoredSlot]); }
  put(id: string, slot: StoredSlot) { this.entries.set(id, { ...slot, kinds: [...slot.kinds] }); }
  delete(id: string) { this.entries.delete(id); }
}

const SLOT_PREFIX = "slot:";

/** The object's synchronous KV storage (SQLite-backed): one entry per held connection. */
export function kvSlotStore(kv: SyncKvStorage): SlotStore {
  return {
    *load() { for (const [key, value] of kv.list<StoredSlot>({ prefix: SLOT_PREFIX })) yield [key.slice(SLOT_PREFIX.length), value]; },
    put: (id, slot) => { kv.put(SLOT_PREFIX + id, slot); },
    delete: (id) => { kv.delete(SLOT_PREFIX + id); },
  };
}

/** What the object asks a gateway during an audit. */
export interface LiveConnections {
  liveConnections(connectionIds: string[]): Promise<string[]>;
}

/** The counters of one address bucket. Plain, so tests drive it without an object. */
export class AddressBudget {
  readonly slots: Record<SlotKind, Map<string, Slot>> = { pending: new Map(), public: new Map() };
  private window: CallWindow | null = null;
  private audited: Record<SlotKind, number> = { pending: -Infinity, public: -Infinity };

  constructor(
    private readonly gateway: (name: string) => LiveConnections,
    private readonly now: () => number = Date.now,
    private readonly store: SlotStore = new MemorySlotStore(),
  ) {
    for (const [id, stored] of store.load()) {
      const slot = { gateway: stored.gateway, at: stored.at };
      for (const kind of stored.kinds) this.slots[kind].set(id, slot);
    }
  }

  /** Whether any slot is held (the object keeps its sweep alarm while one is). */
  get holding(): boolean { return this.slots.pending.size > 0 || this.slots.public.size > 0; }

  /** Writes one connection's entry as it now stands (deleted when it holds nothing). */
  private save(id: string): void {
    const slot = this.slots.pending.get(id) ?? this.slots.public.get(id);
    const kinds = (["pending", "public"] as const).filter(k => this.slots[k].has(id));
    if (!slot || !kinds.length) this.store.delete(id);
    else this.store.put(id, { gateway: slot.gateway, at: slot.at, kinds });
  }

  /**
   * `admit_pending`, then `admit` for the public transport (lib.rs:1590-1617):
   * a refused public slot leaves no pending slot behind.
   */
  async admit(request: AdmitRequest): Promise<AdmitResult> {
    const expired = () => request.expiresAt !== undefined && (!Number.isFinite(request.expiresAt) || this.now() >= request.expiresAt);
    if (expired()) return { ok: false, error: ADMISSION_EXPIRED_ERROR };
    if (!await this.room("pending", request.pendingLimit)) return { ok: false, error: PENDING_LIMIT_ERROR };
    if (expired()) return { ok: false, error: ADMISSION_EXPIRED_ERROR };
    if (request.publicTransport && !await this.room("public", request.publicLimit)) return { ok: false, error: PUBLIC_LIMIT_ERROR };
    if (expired()) return { ok: false, error: ADMISSION_EXPIRED_ERROR };
    // Decided again after the awaits above, with no await in between: two admits never share the last slot.
    if (!this.fits("pending", request.pendingLimit) || (request.publicTransport && !this.fits("public", request.publicLimit))) {
      return this.admit(request);
    }
    const slot = { gateway: request.gateway, at: this.now() };
    this.slots.pending.set(request.connectionId, slot);
    if (request.publicTransport) this.slots.public.set(request.connectionId, slot);
    this.save(request.connectionId);
    return { ok: true };
  }

  release(request: ReleaseRequest): void {
    let changed = false;
    for (const kind of request.kinds) changed = this.slots[kind].delete(request.connectionId) || changed;
    if (changed) this.save(request.connectionId);
  }

  /** The alarm's sweep: audits every slot past the grace, whatever the bucket's fill. */
  async sweep(): Promise<void> {
    this.audited = { pending: -Infinity, public: -Infinity };
    await this.audit("pending");
    await this.audit("public");
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
        this.store.delete(id);
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
    this.budget = new AddressBudget((name) => gateways.getByName(name), Date.now, kvSlotStore(ctx.storage.kv));
  }

  async admit(request: AdmitRequest): Promise<AdmitResult> {
    const result = await this.budget.admit(request);
    if (result.ok) await this.armSweep();
    return result;
  }
  async release(request: ReleaseRequest): Promise<void> { this.budget.release(request); }
  async allow(request: AllowRequest): Promise<boolean> { return this.budget.allow(request); }
  /** Test and operator seam: held slots per kind. */
  async counts(): Promise<Record<SlotKind, number>> { return { pending: this.budget.count("pending"), public: this.budget.count("public") }; }

  override async alarm(): Promise<void> {
    await this.budget.sweep();
    if (this.budget.holding) await this.ctx.storage.setAlarm(Date.now() + SWEEP_INTERVAL_MS);
  }

  /** One alarm while any slot is held; never moved once set. */
  private async armSweep(): Promise<void> {
    if ((await this.ctx.storage.getAlarm()) === null) await this.ctx.storage.setAlarm(Date.now() + SWEEP_INTERVAL_MS);
  }
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
