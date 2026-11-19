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
const MAX_RPCS = 8;
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
const ready = new Set<OrderedOutlet>();
const owners = new Set<WeakRef<OrderedOutlet>>();
/** Bounded isolate totals, never a per-tenant/member series. Byte values are credits, not heap measurements. */
export function deliveryPressure() { return { ...credits, ...pressureCounts }; }

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

  private sweepOwners(): void {
    let hosts = 0;
    for (const owner of owners) {
      const outlet = owner.deref();
      if (outlet) hosts += outlet.queues.size;
      else owners.delete(owner);
    }
    credits.hosts = hosts;
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
        recoveryRequired: false, recovery: undefined, idle, drained, inflight: [] }));
    }
    return queue;
  }

  send(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined> {
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

  /** BUG-4: whether `send` would accept `payload` now (no side effects); the engine waits for room instead of tripping recovery. */
  admits(host: string, payload: ReplicaDelivery): boolean {
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
    ready.add(this);
    void Promise.resolve().then(() => OrderedOutlet.schedule());
    return next;
  }

  private static schedule(): void {
    while (credits.running < MAX_RPCS && ready.size) {
      const outlet = ready.values().next().value!;
      ready.delete(outlet);
      outlet.startOne();
      // One dispatch per owner per turn: a busy tenant cannot monopolize shared RPC credits.
      if ([...outlet.queues.values()].some(q => q.pending.length && !q.running)) ready.add(outlet);
    }
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
      queue.running = true; credits.running++; queue.current = item; queue.inflight = items;
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
      queue.running = false; credits.running--;
      if (!queue.pending.length) {
        this.settleEmpty(host, queue);
      } else {
        // Rotate a busy holder behind other ready holders; no cross-holder ordering requirement.
        this.queues.delete(host); this.queues.set(host, queue);
      }
      // A different tenant may be waiting for the isolate's RPC or oversize credit.
      if ([...this.queues.values()].some(q => q.pending.length && !q.running)) ready.add(this);
      OrderedOutlet.schedule();
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
    if (![...this.queues.values()].some(q => q.pending.length && !q.running)) ready.delete(this);
  }
  queued(host: string): number {
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
  async idle(host: string): Promise<void> { await this.queues.get(host)?.idle; }
}

/** Gateway placement (0016 G0-1): one RPC per payload to the gateway holding the sockets. */
export class GatewayOutlet extends OrderedOutlet {
  constructor(
    private readonly tenantId: string,
    private readonly gateway: (name: string) => ReplicaHolderRpc,
    private readonly registered: () => Promise<string[]>,
  ) {
    super();
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
