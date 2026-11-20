// The two placements of a socket's holder (brief: one interface, two
// placements). `GatewayOutlet` sends to gateway objects over RPC; `LocalOutlet`
// hands payloads to sockets in this object. The engine cannot tell them apart.
//
// Payloads to one holder go one at a time, in order: Workers RPC gives no
// order across separate calls, and a socket must see a snapshot before the
// deltas that follow it. A payload stays under the 32 MiB RPC argument limit
// because snapshot pages are posted chunk by chunk (0020 §6, 0022 §4.4).

import type { DeliveryOutlet } from "./engine.ts";
import { initialRefBytes, validRecoveryInitialDelivery, initialDeliveryOnly, initialFailureOnly, type InitialRequestRef, type ReplicaDelivery, type ReplicaReceipt } from "./protocol.ts";
import { span, spanTimingOn } from "./spans.ts";

const pressureCounts = { capacityRejections: 0, unsupportedRecoveries: 0 };
export const DELIVERY_CAPACITY_CODE = "GONVEX_DELIVERY_CAPACITY";
export class DeliveryCapacityError extends Error {
  readonly code = DELIVERY_CAPACITY_CODE;
  constructor() { super("ordered delivery capacity exhausted; whole-holder recovery required"); pressureCounts.capacityRejections++; }
}
export function isDeliveryCapacityError(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === DELIVERY_CAPACITY_CODE;
}
const HOST_PENDING_BYTES = 8 * 1024 * 1024;
const NORMAL_BYTES = 16 * 1024 * 1024;
const RECOVERY_BYTES = 8 * 1024 * 1024;
const MAX_HOSTS = 32 * 64 + 8; // finite automatic fleet plus the legacy pool during deployment skew
const MAX_PENDING = 256;
export const MAX_RPCS = 8;
/**
 * X17: a `sendFirst` payload this small is admitted beyond the holder's and the isolate's byte budgets (a first load's
 * snapshot pages fill them, which is exactly when a live query reply has to overtake them). The count cap and a
 * holder in recovery still refuse it.
 */
export const FIRST_BYTES = 1024 * 1024;
/**
 * X14: payloads waiting for one holder go in ONE ordered RPC (`deliverReplicaBatch`), up to these bounds. Every
 * delivery RPC is held by the gateway's output gate until its socket-recovery write is durable (~20-30 ms on dev),
 * and the channel carries one RPC at a time, so one RPC per snapshot page made a cold load's ~60-90 opens a serial
 * chain of ~30 ms round trips (X14 report). The batch is applied in order on the holder, item by item, exactly as the
 * single RPCs were; it only removes the per-page round trip and output-gate wait.
 */
export const BATCH_MAX_ITEMS = 64;
export const BATCH_MAX_BYTES = 8 * 1024 * 1024;
/** X14 review: a holder that refused the batch method (deployment skew) is tried again with a batch after this. */
export const SINGLE_ONLY_TTL_MS = 60_000;
export class DeliveryRecoveryUnsupportedError extends Error {
  readonly code = "GONVEX_DELIVERY_RECOVERY_UNSUPPORTED";
  constructor() { super("holder did not acknowledge whole-holder recovery"); pressureCounts.unsupportedRecoveries++; }
}
// Shared by EVERY outlet/tenant in this module's Worker isolate. These are estimated byte credits,
// including retained text plus serialization/copy allowance, not a guarantee about whole-isolate RAM.
const credits = { normal: 0, recovery: 0, count: 0, running: 0, hosts: 0 };
/**
 * BUG-4b: a holder whose next payload is over the budget (legal only alone in the isolate) claims the isolate while
 * its own channel is empty. While the claim stands, `admits` tells every other holder to wait (cooperative engines
 * defer instead of sending), so the isolate's credits drain and the large payload goes; `send` admission itself is
 * unchanged, so nothing is refused for it. No timer: the claim ends when the claimant is admitted, when its engine
 * stops waiting for that holder (`unclaim`), or when its outlet is gone.
 */
let claim: { owner: WeakRef<object>; host: string } | undefined;
/**
 * #550 round 3: outlets whose work waits for an isolate RPC slot, in arrival order, with when each last checked in.
 * Turn order only: an outlet starts its RPCs itself, from its own object's context (`pump`), never another outlet's.
 * A waiter that has not checked in for `WAITER_LIVE_MS` (its object was reset) is skipped and dropped.
 */
const waiters = new Map<OrderedOutlet, number>();
/**
 * #550 follow-up: work-conserving handoff. When a settling outlet would leave a freed slot idle for a waiter ahead
 * (that waiter can only start from its own context, at its next poll), it grants that waiter a reservation and uses
 * the slot itself now. The waiter claims the reservation at its next poll and starts one RPC even with every slot
 * taken. At most `MAX_RPCS` reservations are outstanding, so `credits.running` is at most 2 x `MAX_RPCS` (overshoot
 * <= `MAX_RPCS`: with X = running - MAX_RPCS and R = reservations, X + R <= MAX_RPCS always holds); a
 * reservation not claimed within `WAITER_LIVE_MS` (its object was reset, or its timer ran that late) lapses.
 */
const reservations = new Map<OrderedOutlet, number>();
const reservationCounts = { reservationsGranted: 0, reservationsClaimed: 0, reservationsLapsed: 0 };
/** Drops `outlet`'s reservation unclaimed (its object was reset, its work moved on, or it was claimed too late). */
function lapse(outlet: OrderedOutlet): void { if (reservations.delete(outlet)) reservationCounts.reservationsLapsed++; }
/** A blocked outlet's own timer re-checks for a free slot this often (another object's settlement cannot wake it). */
export const WAIT_POLL_MS = 5;
const WAITER_LIVE_MS = 250;
/** A wake timer this far past its deadline is treated as lost and re-armed on the outlet's next own call. */
export const WAKE_SLACK_MS = 1_000;
let dropWakeTimers = false;
/** Test seam: wake timers are recorded but never set (a timer lost with the context that armed it). */
export function dropWakeTimersForTests(on: boolean): void { dropWakeTimers = on; }
const owners = new Set<WeakRef<OrderedOutlet>>();
/**
 * 04-hang (dev, 2026-11-20): the RPC slots and byte credits above outlive the object that took them. When the platform
 * resets a tenant object (CPU limit, `ctx.abort`, an internal reset) and keeps the isolate, the reset object's
 * delivery RPCs never settle in it: their continuations belong to its dead I/O context, so `run`'s `finally` never
 * returns the slot. With `MAX_RPCS` slots taken that way, no outlet in the isolate starts another RPC: opens keep
 * running and queueing, nothing is delivered, and only a new isolate (a redeploy) clears it.
 *
 * An RPC belongs to the context that STARTED it, not to the outlet it serves. So (#550 round 3) an outlet starts its
 * RPCs only from its own object's contexts (`pump`: its enqueue, the settlement of an RPC it started, its own wake
 * timer). An isolate-global scheduler that started outlet A's RPC from object B's settlement would tie A's holder
 * channel to B's context: a reset of B would leave A's queue `running` for good, and the stale rule below returns a
 * slot, not a channel. The isolate-wide parts are turn order (`waiters`) and bookkeeping (the sweep), never a dispatch.
 *
 * Two reclaims, both isolate-local bookkeeping (neither changes what a live holder receives or in which order):
 * - The same object again (deterministic): one Durable Object id has at most one live instance, so a new outlet for
 *   that id means the previous instance is gone. Its slots, credits and queues are returned at once (`retire`).
 * - Any other (bounded): an RPC that has held a slot for `RPC_SLOT_STALE_MS` stops counting against `MAX_RPCS`, and
 *   the credits of its batch and of everything queued behind it on that holder are returned (none of it can move until
 *   that RPC settles; a reset object that never comes back would otherwise hold them forever). The isolate's
 *   `normal`/`count`/`recovery` totals are then recounted from live outlets (`WeakRef`s, as `hosts` is). Its holder's
 *   channel stays ordered: that holder still waits for the RPC.
 * Both are deterministic: an outlet holding a slot is strongly reachable (`holding`) until the slot is returned, so GC
 * cannot hide a reset instance's slots from either reclaim; and an outlet whose work waits for a slot keeps one wake
 * timer of its own (`WAIT_POLL_MS`, or the next stale deadline), so the sweep runs then even if nothing else happens.
 * A timer more than `WAKE_SLACK_MS` overdue is treated as lost and replaced on the outlet's next own call (`heal`).
 * Cost (measured, #550 round 4; accepted by the lead for #550): a single tenant pays nothing (its own settlement
 * pumps it). With tenants contending for slots, a slot freed by one tenant's settlement stays idle until the next
 * tenant in line wakes, and its timer fires late under load (avg 28 ms past the 5 ms deadline at 16 tenants, 63 ms at
 * 32), so each handoff costs one idle slot: +15% makespan at 4 contending tenants, +75% at 16, +114% at 32
 * (workerd, 45 pages per tenant, 25 ms RPCs). On the dead-object wedge path every blocked tenant polls (~200/s) until
 * the 60 s stale bound, and its object cannot hibernate meanwhile. Polling CPU stays small (1.3-3.1% of a core for
 * 1-200 blocked tenants). A resolver wake is not an alternative: a continuation of a promise another object resolves
 * runs in that object's context (probed).
 * Work-conserving handoff (follow-up PR): a settling outlet that would leave its freed slot idle for a waiter ahead
 * grants that waiter a reservation and uses the slot itself; the waiter claims it from its own poll even with every
 * slot taken (`reservations`, at most `MAX_RPCS`, so `running` is at most 2 x `MAX_RPCS`; unclaimed ones lapse after
 * `WAITER_LIVE_MS`). Contention benchmark (`test/outlet-contention.bench.ts`): 4 tenants 662 -> 625 ms, 16 tenants
 * 5,433 -> 4,044 ms, 32 tenants 17,644 -> 12,820 ms. What remains: a settler with nothing queued cannot use its
 * slot, which then idles until the next waiter polls (dispatch stays in each owner's own context).
 * Cost when a stale RPC was only slow and settles later: what was queued behind it at that moment goes out uncounted,
 * at most one holder backlog (`HOST_PENDING_BYTES`; `sendFirst` items up to `FIRST_BYTES` each, capped by
 * `MAX_PENDING`) per stale RPC; the engine's holder-stall recovery (`HOLDER_STALL_MS` < the stale bound) discards and
 * fences what that holder had counted after it. Cumulatively the uncounted bytes are about `HOST_PENDING_BYTES` x (stale
 * RPCs not yet settled), a number that grows by at most `MAX_RPCS` per `RPC_SLOT_STALE_MS` and is capped only by
 * `MAX_HOSTS`. The isolate may also run more than `MAX_RPCS` RPCs, at most one per holder.
 * Each reclaim logs one warn line (`delivery: outlet of a reset object retired`, `delivery: stale RPC slots returned`,
 * counts only) and counts it in `deliveryPressure()`. The sweep runs in `pump` once the oldest held slot's deadline
 * has passed, and by then whenever work is blocked on slots; a delivery RPC stuck past the bound below the cap is
 * logged at the next enqueue or settlement in the isolate.
 */
export const RPC_SLOT_STALE_MS = 60_000;
let slotStaleMs = RPC_SLOT_STALE_MS;
/** Test seam: the stale-slot bound for this isolate; returns the previous value. */
export function setRpcSlotStaleMs(ms: number): number { const previous = slotStaleMs; slotStaleMs = ms; nextStaleCheck = 0; return previous; }
const byObject = new Map<string, WeakRef<OrderedOutlet>>();
const reclaimCounts = { retiredOutlets: 0, retiredSlots: 0, staleSlots: 0 };
/** When the oldest held slot can first be stale: `pump` sweeps only from then (not on every call of a burst). */
let nextStaleCheck = Number.POSITIVE_INFINITY;
/**
 * #550 re-review: every outlet with an RPC slot not yet returned, held STRONGLY. A reset instance's outlet is otherwise
 * referenced only by RPC promises of its dead I/O context; were V8 to collect it, no reclaim could find its slots
 * (`owners` and `byObject` are WeakRefs). Both reclaims and the stale sweep find slot holders here.
 */
const holding = new Set<OrderedOutlet>();
/** `byObject` entries kept before dead ones are pruned (one short string per Durable Object id the isolate saw). */
const BY_OBJECT_PRUNE_AT = 1_024;
/** Bounded isolate totals, never a per-tenant/member series. Byte values are credits, not heap measurements. */
export function deliveryPressure() { return { ...credits, ...pressureCounts, ...reclaimCounts, holding: holding.size, reservations: reservations.size, ...reservationCounts }; }

interface Pending {
  payload: ReplicaDelivery;
  bytes: number;
  recovery: boolean;
  /** Its credits were returned already: on completion, or (BUG-4b) when its RPC stopped progressing. */
  released?: boolean;
  /** X14 review: the engine reset its holder for a stall (`abandon`): its work is given up, not just its credits. */
  abandoned?: boolean;
  enqueued: number;
  /** X17: sent ahead of the ordinary payloads waiting for this holder (`sendFirst`). */
  first?: boolean;
  resolve(value: ReplicaReceipt | undefined): void;
  reject(error: unknown): void;
}
interface Queue {
  pending: Pending[];
  running: boolean;
  /** The first item of the RPC in flight. */
  current?: Pending | undefined;
  /** X14: every item of the RPC in flight (one, or a batch), in order. */
  inflight: Pending[];
  /** BUG-4b: ticks of an isolate claim that found this channel's RPC in flight and no completion since the last. */
  stuckTicks: number;
  stuckSeen: number;
  bytes: number;
  rpcMs: number;
  /** 04-hang: when the RPC in flight took its isolate slot, and whether that slot was already given back. */
  startedAt: number;
  slotReturned: boolean;
  recoveryRequired: boolean;
  recovery: Promise<ReplicaReceipt | undefined> | undefined;
  idle: Promise<void>;
  drained(): void;
}

/** X14: one item's outcome in a `deliverReplicaBatch` reply, in request order. A failed item keeps its error's
 * message, name and code (the engine and its tests tell capacity and protocol errors apart by them). */
export type ReplicaBatchResult = { readonly receipt: ReplicaReceipt } | { readonly error: string; readonly name?: string; readonly code?: string };

/** The Error a failed batch item rejects with: the holder's message, name and code. */
export function batchItemError(result: { error: string; name?: string; code?: string }): Error {
  const error = new Error(result.error) as Error & { code?: string };
  if (result.name) error.name = result.name;
  if (result.code !== undefined) error.code = result.code;
  return error;
}

export interface ReplicaHolderRpc {
  deliverReplica(tenantId: string, payload: ReplicaDelivery): Promise<ReplicaReceipt>;
  /** X14: the payloads applied in order, each as its own delivery RPC would be; one result per payload. */
  deliverReplicaBatch?(tenantId: string, payloads: ReplicaDelivery[]): Promise<ReplicaBatchResult[]>;
  deliverInitialFailures?(tenantId: string, payload: ReplicaDelivery): Promise<ReplicaReceipt>;
  deliverReplicaInitial?(tenantId:string,payload:ReplicaDelivery):Promise<ReplicaReceipt>;
}

abstract class OrderedOutlet implements DeliveryOutlet {
  private readonly queues = new Map<string, Queue>();
  /** BUG-4b: RPCs completed per holder, kept across the lifetimes of its queue (a drained queue is dropped). */
  private readonly completed = new Map<string, number>();
  private readonly owner = new WeakRef(this);
  /** 04-hang: a newer instance of this outlet's object exists; this one's bookkeeping was returned (`retire`). */
  private retired = false;
  /** RPC slots this outlet holds (taken, not yet returned); while any, `holding` keeps it reachable. */
  private heldSlots = 0;
  /** #550 round 3: this outlet's one wake timer while its work waits for a slot, and when it is due. */
  private wakeTimer: unknown;
  private wakeAt = 0;

  /** `object`: the owning Durable Object's id (one live instance per id); a newer outlet for it retires this one. */
  constructor(object?: string) {
    if (object === undefined) return;
    if (byObject.size >= BY_OBJECT_PRUNE_AT) OrderedOutlet.pruneObjects();
    const previous = byObject.get(object)?.deref();
    if (previous && previous !== this) previous.retire();
    byObject.set(object, this.owner);
  }

  /**
   * 04-hang: the instance that built this outlet was reset. Nothing it queued or sent can reach a socket through it
   * any more (the new instance serves the holders under a new `instance`), so its RPC slots, byte credits and host
   * entries are returned and it leaves the scheduler. A late settlement of one of its RPCs returns nothing twice.
   */
  private retire(): void {
    if (this.retired) return;
    this.retired = true;
    let slots = 0;
    for (const queue of this.queues.values()) {
      for (const item of [...queue.inflight, ...queue.pending]) this.release(queue, item);
      queue.pending = [];
      queue.recoveryRequired = false;
      if (queue.running && this.returnSlot(queue)) slots++;
    }
    credits.hosts = Math.max(0, credits.hosts - this.queues.size);
    this.queues.clear();
    waiters.delete(this); lapse(this);
    holding.delete(this); this.heldSlots = 0;
    owners.delete(this.owner);
    if (claim?.owner.deref() === this) claim = undefined;
    reclaimCounts.retiredOutlets++;
    reclaimCounts.retiredSlots += slots;
    console.warn("delivery: outlet of a reset object retired", { slots, running: credits.running, count: credits.count });
  }

  /**
   * 04-hang: RPCs that held an isolate slot past the stale bound stop counting against `MAX_RPCS`; the credits of their
   * batch and of the payloads queued behind them are returned, and the byte/count totals are recounted from live
   * outlets. Also sets when the next check is due (the oldest remaining slot's deadline).
   */
  private static expireStaleSlots(now: number): void {
    let stale = 0, next = Number.POSITIVE_INFINITY;
    for (const outlet of [...holding]) {
      for (const queue of outlet.queues.values()) {
        if (!queue.running || queue.slotReturned) continue;
        if (now - queue.startedAt < slotStaleMs) { next = Math.min(next, queue.startedAt + slotStaleMs); continue; }
        outlet.returnSlot(queue); stale++;
        for (const item of [...queue.inflight, ...queue.pending]) outlet.release(queue, item);
      }
    }
    nextStaleCheck = next;
    // #550 round 4: `waiters` holds outlets strongly; a dead waiter behind every live one would stay referenced (with its
    // queued payloads) until some pump walked past it. The sweep drops them too.
    for (const [outlet, seen] of waiters) if (outlet.retired || now - seen > WAITER_LIVE_MS || !outlet.waiting()) waiters.delete(outlet);
    for (const [outlet, at] of reservations) if (outlet.retired || now - at > WAITER_LIVE_MS || !outlet.waiting()) lapse(outlet);
    if (!stale) return;
    OrderedOutlet.recount();
    reclaimCounts.staleSlots += stale;
    console.warn("delivery: stale RPC slots returned", { slots: stale, running: credits.running, count: credits.count });
  }

  /** Takes one isolate RPC slot for `queue`'s RPC; the outlet stays strongly reachable until every slot it took is returned. */
  private takeSlot(queue: Queue): void {
    credits.running++; queue.slotReturned = false;
    this.heldSlots++; holding.add(this);
  }

  /** Returns `queue`'s slot, once: false when the stale rule or retirement already returned it. */
  private returnSlot(queue: Queue): boolean {
    if (queue.slotReturned) return false;
    queue.slotReturned = true; credits.running--;
    if (--this.heldSlots <= 0) { this.heldSlots = 0; holding.delete(this); }
    return true;
  }

  /** Whether a holder of this outlet has a payload waiting for an RPC slot (not behind its own RPC in flight). */
  private waiting(): boolean {
    for (const queue of this.queues.values()) if (queue.pending.length && !queue.running) return true;
    return false;
  }

  /**
   * #550 round 3: starts this outlet's RPCs, from its OWN object's context only: its enqueue microtask, the settlement
   * of an RPC it started (which resumes in the context that started it), or its own wake timer. An RPC started from
   * another object's context belongs to that context: were that object reset, the RPC would never settle and this
   * live outlet's holder channel would stay `running` for good. So the isolate-wide part is turn order (`waiters`)
   * and bookkeeping (the stale sweep), never a dispatch: one outlet never starts another outlet's RPC.
   *
   * Fairness as before: one dispatch per turn, then the outlet goes to the back of the line. An outlet whose work
   * still waits for a slot polls with its own timer (`WAIT_POLL_MS`, and the next stale deadline).
   */
  private pump(): void {
    if (this.retired) return;
    const now = Date.now();
    if (now >= nextStaleCheck) OrderedOutlet.expireStaleSlots(now);
    while (this.waiting()) {
      waiters.set(this, now); // keeps its place (Map order), refreshes its liveness
      // A reservation another outlet granted this one: claimed here, in this outlet's own context, cap or not.
      const reserved = reservations.get(this);
      if (reserved !== undefined) {
        // `running < 2 x MAX_RPCS` always holds here (X + R <= MAX_RPCS); the check is a backstop, not a limit in use.
        if (now - reserved <= WAITER_LIVE_MS && credits.running < 2 * MAX_RPCS) {
          reservations.delete(this); reservationCounts.reservationsClaimed++;
          this.dispatchTurn(now); continue;
        }
        lapse(this);
      }
      const free = MAX_RPCS - credits.running;
      if (free <= 0) break;
      // Live waiters ahead that hold no reservation yet (a reserved one already has its slot coming).
      let first: OrderedOutlet | undefined, ahead = 0;
      for (const [outlet, seen] of waiters) {
        if (outlet === this) break;
        if (outlet.retired || now - seen > WAITER_LIVE_MS || !outlet.waiting()) { waiters.delete(outlet); lapse(outlet); continue; }
        if (reservations.has(outlet)) continue;
        first ??= outlet; ahead++;
      }
      if (ahead >= free) {
        // Work-conserving: the slot would idle until that waiter's own timer fires. Reserve it one, use this slot now.
        if (!first || reservations.size >= MAX_RPCS) break;
        reservations.set(first, now); reservationCounts.reservationsGranted++;
      }
      this.dispatchTurn(now);
    }
    if (!this.waiting()) { waiters.delete(this); lapse(this); this.disarm(); return; }
    this.arm(now);
  }

  /** One dispatch, then the back of the line if this outlet still waits (fairness: one RPC per turn). */
  private dispatchTurn(now: number): void {
    this.startOne();
    waiters.delete(this); // one dispatch per turn: back of the line if it still waits
    if (this.waiting()) waiters.set(this, now);
  }

  /** One wake timer per outlet, armed from this outlet's own context (only `pump` arms it). */
  private arm(now: number): void {
    const due = Math.min(now + WAIT_POLL_MS, Number.isFinite(nextStaleCheck) ? Math.max(now + 1, nextStaleCheck) : Number.POSITIVE_INFINITY);
    if (this.wakeTimer !== undefined && this.wakeAt <= due) return;
    this.disarm();
    this.wakeAt = due;
    this.wakeTimer = dropWakeTimers ? "dropped" : setTimeout(() => { this.wakeTimer = undefined; this.pump(); }, Math.max(1, due - now));
  }

  private disarm(): void {
    if (this.wakeTimer !== undefined && this.wakeTimer !== "dropped") clearTimeout(this.wakeTimer as ReturnType<typeof setTimeout>);
    this.wakeTimer = undefined;
  }

  /**
   * #550 round 3: self-healing. Every call the owning object makes on this outlet checks that waiting work has a live
   * wake timer; one missing, or more than `WAKE_SLACK_MS` past its deadline (lost with the context that armed it), is
   * replaced by a pump from this call's context, which is this outlet's own.
   */
  private heal(): void {
    if (this.retired || !this.waiting()) return;
    if (this.wakeTimer !== undefined && Date.now() <= this.wakeAt + WAKE_SLACK_MS) return;
    this.wakeTimer = undefined;
    void Promise.resolve().then(() => this.pump());
  }

  /** The isolate's byte and item totals from the unreleased payloads of live outlets (an outlet gone to GC drops out). */
  private static recount(): void {
    let normal = 0, recovery = 0, count = 0, hosts = 0;
    for (const ref of owners) {
      const outlet = ref.deref();
      if (!outlet) { owners.delete(ref); continue; }
      hosts += outlet.queues.size;
      for (const queue of outlet.queues.values()) {
        let bytes = 0;
        for (const item of [...queue.inflight, ...queue.pending]) {
          if (item.released) continue;
          if (item.recovery) recovery += item.bytes;
          else { bytes += item.bytes; count++; }
        }
        queue.bytes = bytes; normal += bytes;
      }
    }
    Object.assign(credits, { normal, recovery, count, hosts });
  }

  private static pruneObjects(): void {
    for (const [object, ref] of byObject) if (!ref.deref()) byObject.delete(object);
  }

  private sweepOwners(): void {
    let hosts = 0;
    for (const owner of owners) {
      const outlet = owner.deref();
      if (outlet) hosts += outlet.queues.size;
      else owners.delete(owner);
    }
    credits.hosts = hosts;
    OrderedOutlet.pruneObjects();
  }

  protected abstract deliver(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt>;
  /** X14: whether this placement can send `host` a batch at all (decided before items are taken off its queue). */
  protected batchable(_host: string): boolean { return false; }
  /** X14: several payloads in one ordered call; only called when `batchable(host)`. */
  protected deliverBatch(_host: string, _payloads: ReplicaDelivery[]): Promise<ReplicaBatchResult[]> { return Promise.reject(new Error("replica batches are not supported by this placement")); }
  /** X14: holders that answered a batch with workerd's missing-method error (deployment skew), until when they get single RPCs. */
  private readonly singleOnly = new Map<string, number>();
  private batching(host: string): boolean {
    const until = this.singleOnly.get(host);
    if (until !== undefined && until <= Date.now()) this.singleOnly.delete(host);
    return until === undefined || until <= Date.now() ? this.batchable(host) : false;
  }
  abstract hosts(): Promise<string[]>;

  private queue(host: string): Queue {
    let queue = this.queues.get(host);
    if (!queue) {
      if (credits.hosts >= MAX_HOSTS) this.sweepOwners();
      if (credits.hosts >= MAX_HOSTS) throw new DeliveryCapacityError();
      credits.hosts++;
      owners.add(this.owner);
      let drained!: () => void;
      const idle = new Promise<void>(resolve => { drained = resolve; });
      this.queues.set(host, (queue = { pending: [], running: false, stuckTicks: 0, stuckSeen: -1, bytes: 0, rpcMs: 0,
        startedAt: 0, slotReturned: false, recoveryRequired: false, recovery: undefined, idle, drained, inflight: [] }));
    }
    return queue;
  }

  send(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined> {
    this.heal();
    if (!validRecoveryInitialDelivery(payload)) return Promise.reject(new Error("invalid recovery initial offer"));
    if (payload.initialFailure && !initialFailureOnly(payload)) return Promise.reject(new Error("invalid initial-failure-only delivery"));
    if(Object.values(payload.sockets).some(work=>work.initialScope||work.initialCompletions?.length)&&!initialDeliveryOnly(payload))return Promise.reject(new Error("invalid initial-only delivery"));
    let queue: Queue;
    try { queue = this.queue(host); } catch (error) { return Promise.reject(error); }
    const bytes = retainedBytes(payload);
    if (!OrderedOutlet.fits(queue, bytes)) {
      queue.recoveryRequired = true;
      this.settleEmpty(host, queue);
      return Promise.reject(new DeliveryCapacityError());
    }
    const sent = this.enqueue(host, queue, payload, bytes, false);
    this.unclaim(host); // a claimant's payload is enqueued: its claim ends (r4609198128)
    return sent;
  }

  /**
   * X17: `send`, placed ahead of every ordinary payload still waiting for `host` (never ahead of the RPC in flight, a
   * recovery barrier, or an earlier `sendFirst` payload). The same admission and credits as `send`.
   */
  sendFirst(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined> {
    this.heal();
    if (!validRecoveryInitialDelivery(payload)) return Promise.reject(new Error("invalid recovery initial offer"));
    if (payload.initialFailure && !initialFailureOnly(payload)) return Promise.reject(new Error("invalid initial-failure-only delivery"));
    if(Object.values(payload.sockets).some(work=>work.initialScope||work.initialCompletions?.length)&&!initialDeliveryOnly(payload))return Promise.reject(new Error("invalid initial-only delivery"));
    let queue: Queue;
    try { queue = this.queue(host); } catch (error) { return Promise.reject(error); }
    const bytes = retainedBytes(payload);
    if (!OrderedOutlet.fitsFirst(queue, bytes)) {
      queue.recoveryRequired = true;
      this.settleEmpty(host, queue);
      return Promise.reject(new DeliveryCapacityError());
    }
    let at = 0;
    while (at < queue.pending.length && (queue.pending[at]!.first || queue.pending[at]!.recovery)) at++;
    const sent = this.enqueue(host, queue, payload, bytes, false, at);
    this.unclaim(host);
    return sent;
  }

  /**
   * A legal large singleton is allowed when it is the only retained normal delivery. Never impose
   * the pending budget as a smaller wire/frame limit. Its real isolate peak remains a V04 gate.
   */
  private static fits(queue: Queue | undefined, bytes: number): boolean {
    if (queue?.recoveryRequired || credits.count >= MAX_PENDING) return false;
    // The recovery reserve is its own bounded budget (`RECOVERY_BYTES`): a recovery barrier queued behind a stalled
    // RPC must not hold back the singleton forever (BUG-4b r4609198131). Peak: the singleton plus that reserve.
    const singleton = credits.normal === 0 && !queue?.running && !queue?.pending.length;
    return singleton || ((queue?.bytes ?? 0) + bytes <= HOST_PENDING_BYTES && credits.normal + bytes <= NORMAL_BYTES);
  }

  /** X17: `fits` for a `sendFirst` payload: within `FIRST_BYTES` it needs no byte room. */
  private static fitsFirst(queue: Queue | undefined, bytes: number): boolean {
    if (bytes <= FIRST_BYTES) return !queue?.recoveryRequired && credits.count < MAX_PENDING;
    return OrderedOutlet.fits(queue, bytes);
  }

  /** X17: whether `sendFirst` would accept `payload` now (no side effects). */
  admitsFirst(host: string, payload: ReplicaDelivery): boolean {
    // A holder that claimed the quiet isolate for a large payload (BUG-4b) still goes before anyone else.
    if (claim && claim.owner.deref() && !(claim.owner.deref() === this && claim.host === host)) return false;
    return OrderedOutlet.fitsFirst(this.queues.get(host), retainedBytes(payload));
  }

  /** BUG-4: whether `send` would accept `payload` now (no side effects); the engine waits for room instead of tripping recovery. */
  admits(host: string, payload: ReplicaDelivery): boolean {
    this.heal();
    const queue = this.queues.get(host), bytes = retainedBytes(payload);
    const mine = claim?.owner.deref() === this && claim.host === host;
    if (claim && !claim.owner.deref()) claim = undefined;
    // Its turn: the claim stays until `send` enqueues it, so nobody takes the quiet isolate in between (r4609198128).
    if (OrderedOutlet.fits(queue, bytes)) return !claim || mine;
    // BUG-4b: over the budget with its own channel empty, it can only wait for the isolate to be quiet: claim it.
    const oversize = bytes > HOST_PENDING_BYTES || bytes > NORMAL_BYTES;
    if (oversize && !queue?.recoveryRequired && !queue?.running && !queue?.pending.length && (!claim || mine)) claim = { owner: this.owner, host };
    return false;
  }

  /** BUG-4b: this holder no longer waits for room (caught up, reset or gone): its claim, if any, ends. */
  unclaim(host: string): void {
    if (claim?.owner.deref() === this && claim.host === host) claim = undefined;
  }

  /** BUG-4b: RPCs this holder's channel has completed, monotonic across its queue's lifetimes (r4609198111). */
  progress(host: string): number {
    return this.completed.get(host) ?? 0;
  }

  /**
   * BUG-4b (r4609198131): one stall tick of the claim `host` holds. A claim never waits on a stalled RPC: any channel
   * in the isolate whose RPC has been in flight for `threshold` such ticks with no completion has its credits
   * returned (the RPC itself may still settle; its credits are not returned twice). True when that freed some.
   */
  claimTick(host: string, threshold: number): boolean {
    if (claim?.owner.deref() !== this || claim.host !== host) return false;
    let freed = false;
    for (const ref of owners) {
      const outlet = ref.deref();
      if (!outlet) continue;
      for (const [name, queue] of outlet.queues) {
        const current = queue.current;
        if (!current || current.recovery || queue.inflight.every(item => item.released)) { queue.stuckTicks = 0; continue; }
        const done = outlet.progress(name);
        if (done === queue.stuckSeen) queue.stuckTicks += 1;
        else { queue.stuckTicks = 1; queue.stuckSeen = done; }
        if (queue.stuckTicks > threshold) { for (const item of queue.inflight) outlet.release(queue, item); freed = true; }
      }
    }
    return freed;
  }

  /** BUG-4b: the engine reset `host` for a stall: its in-flight RPC's credits are returned now, not when it settles. */
  abandon(host: string): void {
    const queue = this.queues.get(host);
    if (queue?.current && !queue.current.recovery) for (const item of queue.inflight) { item.abandoned = true; this.release(queue, item); }
  }

  /**
   * One whole-holder recovery barrier on the SAME channel. Admission is synchronous; promise settles
   * at RPC completion. Repeated calls coalesce to that barrier, so payload must reset ALL holder state,
   * including live queries and failed/pending opens. Engine suppresses new normal work until completion.
   * RPC failure keeps recoveryRequired; caller can explicitly retry reset, never a background retry.
   */
  reset(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined> {
    if (!validRecoveryInitialDelivery(payload)) return Promise.reject(new Error("invalid recovery initial offer"));
    let queue: Queue;
    try { queue = this.queue(host); } catch (error) { return Promise.reject(error); }
    if (queue.recovery) return queue.recovery;
    queue.recoveryRequired = true;
    this.discard(host);
    const bytes = retainedBytes(payload);
    if (credits.recovery + bytes > RECOVERY_BYTES) return Promise.reject(new DeliveryCapacityError());
    const recovery = this.enqueue(host, queue, payload, bytes, true);
    queue.recovery = recovery;
    return recovery;
  }

  private enqueue(host: string, queue: Queue, payload: ReplicaDelivery, bytes: number, recovery: boolean, at?: number): Promise<ReplicaReceipt | undefined> {
    if (!queue.running && !queue.pending.length) {
      queue.idle = new Promise<void>(resolve => { queue.drained = resolve; });
    }
    const next = new Promise<ReplicaReceipt | undefined>((resolve, reject) => {
      const item: Pending = { payload, bytes, recovery, enqueued: Date.now(), resolve, reject, ...(at === undefined ? {} : { first: true }) };
      if (at === undefined) queue.pending.push(item); else queue.pending.splice(at, 0, item);
    });
    if (recovery) credits.recovery += bytes;
    else { queue.bytes += bytes; credits.normal += bytes; credits.count++; }
    // Begin in a microtask so reset publishes its coalescing promise before a local RPC can finish.
    void Promise.resolve().then(() => this.pump());
    return next;
  }

  private startOne(): void {
    for (const [host, queue] of this.queues) {
      if (queue.running) continue;
      const item = queue.pending.shift();
      if (!item) continue;
      const items = [item];
      // X14: ordinary payloads queued behind it for the same holder ride the same RPC (never a recovery barrier).
      if (!item.recovery && queue.pending.length && !queue.pending[0]!.recovery && this.batching(host)) {
        let bytes = item.bytes;
        while (items.length < BATCH_MAX_ITEMS && queue.pending.length && !queue.pending[0]!.recovery
          && bytes + queue.pending[0]!.bytes <= BATCH_MAX_BYTES) {
          const next = queue.pending.shift()!;
          bytes += next.bytes; items.push(next);
        }
      }
      queue.running = true; this.takeSlot(queue); queue.current = item; queue.inflight = items;
      queue.startedAt = Date.now();
      nextStaleCheck = Math.min(nextStaleCheck, queue.startedAt + slotStaleMs);
      void this.run(host, queue, items);
      return;
    }
  }

  private async run(host: string, queue: Queue, items: Pending[]): Promise<void> {
    const started = Date.now();
    const pressure = (item: Pending) => ({ ...item.payload, pressure: { queueAgeMs: started - item.enqueued, rpcMs: queue.rpcMs, ...(spanTimingOn() ? { sentAt: started } : {}) } });
    let requeue = false;
    try {
      if (items.length === 1) {
        try { this.settle(items[0]!, queue, await this.deliver(host, pressure(items[0]!))); }
        catch (error) { items[0]!.reject(error); }
      } else {
        let results: ReplicaBatchResult[];
        try { results = await this.deliverBatch(host, items.map(pressure)); }
        catch (error) {
          if (!batchUnsupported(error)) throw error;
          // Deployment skew: the old holder has no batch method, so nothing was applied. Resend singly, in order,
          // and try a batch again after SINGLE_ONLY_TTL_MS (the holder is likely updated by then).
          this.singleOnly.set(host, Date.now() + SINGLE_ONLY_TTL_MS);
          requeue = true;
          return;
        }
        if (!Array.isArray(results) || results.length !== items.length) throw new Error("invalid replica batch receipt");
        items.forEach((item, index) => {
          const result = results[index]!;
          if ("error" in result) { item.reject(batchItemError(result)); return; }
          try { this.settle(item, queue, result.receipt); } catch (error) { item.reject(error); }
        });
      }
    } catch (error) { for (const item of items) item.reject(error); }
    finally {
      queue.rpcMs = Date.now() - started;
      // X13i: tenant -> holder delivery: wait behind this holder's earlier payload, then the RPC (dev spans only).
      // X14: one line per RPC; a batch reports its oldest item's wait and how many payloads it carried.
      span("tenant.delivery", { queueAgeMs: started - items[0]!.enqueued, rpcMs: queue.rpcMs, items: items.length });
      queue.current = undefined;
      queue.inflight = [];
      if (this.retired) {
        // 04-hang: a late settlement of a retired outlet (its instance was reset) returns its slot at most once and
        // nothing else: it never requeues, rotates or re-enters the scheduler with items whose credits were returned.
        queue.running = false;
        this.returnSlot(queue);
        for (const item of items) this.release(queue, item);
      } else {
        if (requeue) {
          // Items the engine meanwhile gave up on (a recovery barrier was set or is queued, or `abandon` reset the
          // holder for a stall) are settled as discarded, as `discard` settles pending work. Items whose credits a
          // claim tick only returned early (BUG-4b, no engine reset) are still owed: they go back, in order.
          const discarded = queue.recoveryRequired || queue.recovery !== undefined;
          const back: Pending[] = [];
          for (const item of items) {
            if (discarded || item.abandoned) { this.release(queue, item); item.resolve(undefined); }
            else back.push(item);
          }
          queue.pending.unshift(...back);
        } else {
          this.completed.set(host, (this.completed.get(host) ?? 0) + 1);
          for (const item of items) {
            this.release(queue, item);
            if (item.recovery) queue.recovery = undefined;
          }
        }
        queue.running = false;
        this.returnSlot(queue);
        queue.slotReturned = false;
        if (!queue.pending.length) {
          this.settleEmpty(host, queue);
        } else {
          // Rotate a busy holder behind other ready holders; no cross-holder ordering requirement.
          this.queues.delete(host); this.queues.set(host, queue);
        }
      }
      // This settlement resumes in the context that started the RPC, which is this outlet's own (`pump`).
      this.pump();
    }
  }

  /** One item's receipt: the checks a single delivery RPC's receipt gets. Throws to reject the item. */
  private settle(item: Pending, queue: Queue, receipt: ReplicaReceipt): void {
    if (item.payload.initialFailure && receipt.initialFailureAck !== 1) throw new Error("GONVEX_DELIVERY_INITIAL_FAILURE_UNSUPPORTED");
    if (item.recovery) {
      if (receipt.recoveryAck !== 1) throw new DeliveryRecoveryUnsupportedError();
      queue.recoveryRequired = false;
    }
    item.resolve(receipt);
  }

  private release(queue: Queue, item: Pending): void {
    if (item.released) return;
    item.released = true;
    if (item.recovery) credits.recovery -= item.bytes;
    else { queue.bytes -= item.bytes; credits.normal -= item.bytes; credits.count--; }
  }
  /** Empty blocked queues retain their recovery fence, but have no outstanding RPC to await. */
  private settleEmpty(host: string, queue: Queue): void {
    if (queue.running || queue.pending.length) return;
    queue.drained();
    if (!queue.recoveryRequired && this.queues.get(host) === queue) {
      this.queues.delete(host); credits.hosts--;
      if (!this.queues.size) owners.delete(this.owner);
    }
    if (!this.waiting()) waiters.delete(this);
  }
  queued(host: string): number {
    this.heal();
    const queue = this.queues.get(host);
    // Items, not RPCs: a batch in flight counts every payload it carries (#453/#473 deferral bound).
    return queue ? queue.pending.length + (queue.running ? Math.max(1, queue.inflight.length) : 0) : 0;
  }
  backlogged(host: string): boolean {
    const queue = this.queues.get(host);
    return Boolean(queue?.recoveryRequired || (queue?.bytes ?? 0) >= HOST_PENDING_BYTES || credits.normal >= NORMAL_BYTES);
  }
  discard(host: string): void {
    const queue = this.queues.get(host);
    if (!queue) return;
    const keep: Pending[] = [];
    for (const item of queue.pending) {
      if (item.recovery) keep.push(item);
      else { this.release(queue, item); item.resolve(undefined); }
    }
    queue.pending = keep;
    this.settleEmpty(host, queue);
  }
  async idle(host: string): Promise<void> { this.heal(); await this.queues.get(host)?.idle; }
}

/** Gateway placement (0016 G0-1): one RPC per payload to the gateway holding the sockets. */
export class GatewayOutlet extends OrderedOutlet {
  constructor(
    private readonly tenantId: string,
    private readonly gateway: (name: string) => ReplicaHolderRpc,
    private readonly registered: () => Promise<string[]>,
    /** 04-hang: the owning Durable Object's id; a later outlet for the same id retires this one (`OrderedOutlet`). */
    object?: string,
  ) {
    super(object);
  }

  protected deliver(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt> {
    // Never expose a new marker to an old ordinary receiver: the absent
    // additive RPC fails before it can apply even an empty envelope/instance.
    if (payload.initialFailure) {
      const receiver = this.gateway(host);
      if (typeof receiver.deliverInitialFailures !== "function") return Promise.reject(new Error("GONVEX_DELIVERY_INITIAL_FAILURE_UNSUPPORTED"));
      return receiver.deliverInitialFailures(this.tenantId, payload);
    }
    if(Object.values(payload.sockets).some(work=>work.initialScope||work.initialCompletions?.length)){
      const receiver=this.gateway(host);
      if(typeof receiver.deliverReplicaInitial!=="function")return Promise.reject(new Error("GONVEX_INITIAL_DELIVERY_UNSUPPORTED"));
      return receiver.deliverReplicaInitial(this.tenantId,payload);
    }
    return this.gateway(host).deliverReplica(this.tenantId, payload);
  }

  protected override batchable(host: string): boolean {
    return typeof this.gateway(host).deliverReplicaBatch === "function";
  }

  protected override deliverBatch(host: string, payloads: ReplicaDelivery[]): Promise<ReplicaBatchResult[]> {
    return this.gateway(host).deliverReplicaBatch!(this.tenantId, payloads);
  }

  hosts(): Promise<string[]> {
    return this.registered();
  }
}

/** Tenant placement: the sockets live in this object; `deliver` is a local call. */
export class LocalOutlet extends OrderedOutlet {
  constructor(private readonly local: (host: string, payload: ReplicaDelivery) => ReplicaReceipt, private readonly names: () => string[],
    private readonly failures?: (host: string, payload: ReplicaDelivery) => ReplicaReceipt | Promise<ReplicaReceipt>,
    private readonly initials?: (host:string,payload:ReplicaDelivery)=>ReplicaReceipt|Promise<ReplicaReceipt>) {
    super();
  }

  protected async deliver(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt> {
    if (payload.initialFailure) {
      if (!this.failures) throw new Error("GONVEX_DELIVERY_INITIAL_FAILURE_UNSUPPORTED");
      return this.failures(host, payload);
    }
    if(Object.values(payload.sockets).some(work=>work.initialScope||work.initialCompletions?.length)){
      if(!this.initials)throw new Error("GONVEX_INITIAL_DELIVERY_UNSUPPORTED");
      return this.initials(host,payload);
    }
    return this.local(host, payload);
  }

  async hosts(): Promise<string[]> {
    return this.names();
  }
}

/** X14: the holder's Worker version predates `deliverReplicaBatch` (Workers RPC: the receiver does not implement it). */
export function batchUnsupported(error: unknown): boolean {
  const message = String((error as Error | undefined)?.message ?? error);
  // workerd's wording for a method the receiving class does not define. workerd 1.20261023 says
  // 'The RPC receiver does not implement "deliverReplicaBatch".'; other versions add "the method".
  return /does not implement (the method )?"deliverReplicaBatch"/.test(message);
}

/** Conservative text/copy credits (UTF16 plus two worst-case UTF8 copies), no second JSON encoding of the delivery payload. */
function retainedBytes(payload: ReplicaDelivery): number {
  let bytes = 256 + payload.instance.length * 8;
  if (payload.initialFailure) {
    bytes += 256 + payload.initialFailure.reason.length * 8;
    for (const [id, refs] of Object.entries(payload.initialFailure.requests)) {
      bytes += 128 + id.length * 8;
      for (const ref of refs) bytes += initialRefBytes(ref);
    }
  }
  if (payload.recovery) bytes += payload.recovery.reason.length * 8 + 64;
  for (const [id,refs] of Object.entries(payload.recoveryInitials ?? {})) {
    bytes += 128 + id.length * 8;
    for (const ref of refs) bytes += 512 + initialRefBytes(ref);
  }
  if (payload.membership) {
    bytes += 128;
    for (const id of payload.membership.members) bytes += id.length * 8 + 64;
    for (const id of payload.membership.accounts) bytes += id.length * 8 + 64;
  }
  for (const text of payload.shared) bytes += text.length * 8 + 64;
  for (const [id, work] of Object.entries(payload.sockets)) {
    bytes += id.length * 8 + 256;
    if(work.initialScope)bytes+=256+initialRefBytes(work.initialScope);
    if(work.recoveryInitials)bytes+=64;
    for(const ref of work.recoveryInitials ?? [])bytes+=512+initialRefBytes(ref);
    for (const op of work.ops) {
      bytes += 64;
      for (const value of op) if (typeof value === "string") bytes += value.length * 8;

    }
    for (const completion of work.initialCompletions ?? []) bytes += 512 + initialRefBytes(completion.request); // request + bounded status receipt/copies
    for (const [id, path] of Object.entries(work.opened ?? {})) bytes += (id.length + path.length) * 8 + 128;
    for (const id of work.removed ?? []) bytes += id.length * 8 + 64;
  }
  return bytes;
}
