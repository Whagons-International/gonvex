// The two placements of a socket's holder (brief: one interface, two
// placements). `GatewayOutlet` sends to gateway objects over RPC; `LocalOutlet`
// hands payloads to sockets in this object. The engine cannot tell them apart.
//
// Payloads to one holder go one at a time, in order: Workers RPC gives no
// order across separate calls, and a socket must see a snapshot before the
// deltas that follow it. A payload stays under the 32 MiB RPC argument limit
// because snapshot pages are posted chunk by chunk (0020 §6, 0022 §4.4).

import type { DeliveryOutlet } from "./engine.ts";
import { initialDeliveryOnly, initialFailureOnly, type InitialRequestRef, type ReplicaDelivery, type ReplicaReceipt } from "./protocol.ts";

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
export class DeliveryRecoveryUnsupportedError extends Error {
  readonly code = "GONVEX_DELIVERY_RECOVERY_UNSUPPORTED";
  constructor() { super("holder did not acknowledge whole-holder recovery"); pressureCounts.unsupportedRecoveries++; }
}
// Shared by EVERY outlet/tenant in this module's Worker isolate. These are estimated byte credits,
// including retained text plus serialization/copy allowance, not a guarantee about whole-isolate RAM.
const credits = { normal: 0, recovery: 0, count: 0, running: 0, hosts: 0 };
const ready = new Set<OrderedOutlet>();
const owners = new Set<WeakRef<OrderedOutlet>>();
/** Bounded isolate totals, never a per-tenant/member series. Byte values are credits, not heap measurements. */
export function deliveryPressure() { return { ...credits, ...pressureCounts }; }

interface Pending {
  payload: ReplicaDelivery;
  bytes: number;
  recovery: boolean;
  enqueued: number;
  resolve(value: ReplicaReceipt | undefined): void;
  reject(error: unknown): void;
}
interface Queue {
  pending: Pending[];
  running: boolean;
  bytes: number;
  rpcMs: number;
  recoveryRequired: boolean;
  recovery: Promise<ReplicaReceipt | undefined> | undefined;
  idle: Promise<void>;
  drained(): void;
}

export interface ReplicaHolderRpc {
  deliverReplica(tenantId: string, payload: ReplicaDelivery): Promise<ReplicaReceipt>;
  deliverInitialFailures?(tenantId: string, payload: ReplicaDelivery): Promise<ReplicaReceipt>;
  deliverReplicaInitial?(tenantId:string,payload:ReplicaDelivery):Promise<ReplicaReceipt>;
}

abstract class OrderedOutlet implements DeliveryOutlet {
  private readonly queues = new Map<string, Queue>();
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
      this.queues.set(host, (queue = { pending: [], running: false, bytes: 0, rpcMs: 0,
        recoveryRequired: false, recovery: undefined, idle, drained }));
    }
    return queue;
  }

  send(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined> {
    if (payload.initialFailure && !initialFailureOnly(payload)) return Promise.reject(new Error("invalid initial-failure-only delivery"));
    if(Object.values(payload.sockets).some(work=>work.initialScope||work.initialCompletions?.length)&&!initialDeliveryOnly(payload))return Promise.reject(new Error("invalid initial-only delivery"));
    let queue: Queue;
    try { queue = this.queue(host); } catch (error) { return Promise.reject(error); }
    const bytes = retainedBytes(payload);
    // A legal large singleton is allowed when it is the only retained normal delivery. Never impose
    // the pending budget as a smaller wire/frame limit. Its real isolate peak remains a V04 gate.
    const singleton = credits.normal === 0 && credits.recovery === 0 && !queue.running && !queue.pending.length;
    if (queue.recoveryRequired || credits.count >= MAX_PENDING || (!singleton
      && (queue.bytes + bytes > HOST_PENDING_BYTES || credits.normal + bytes > NORMAL_BYTES))) {
      queue.recoveryRequired = true;
      this.settleEmpty(host, queue);
      return Promise.reject(new DeliveryCapacityError());
    }
    return this.enqueue(host, queue, payload, bytes, false);
  }

  /**
   * One whole-holder recovery barrier on the SAME channel. Admission is synchronous; promise settles
   * at RPC completion. Repeated calls coalesce to that barrier, so payload must reset ALL holder state,
   * including live queries and failed/pending opens. Engine suppresses new normal work until completion.
   * RPC failure keeps recoveryRequired; caller can explicitly retry reset, never a background retry.
   */
  reset(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined> {
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

  private enqueue(host: string, queue: Queue, payload: ReplicaDelivery, bytes: number, recovery: boolean): Promise<ReplicaReceipt | undefined> {
    if (!queue.running && !queue.pending.length) {
      queue.idle = new Promise<void>(resolve => { queue.drained = resolve; });
    }
    const next = new Promise<ReplicaReceipt | undefined>((resolve, reject) => {
      queue.pending.push({ payload, bytes, recovery, enqueued: Date.now(), resolve, reject });
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
      queue.running = true; credits.running++;
      void this.run(host, queue, item);
      return;
    }
  }

  private async run(host: string, queue: Queue, item: Pending): Promise<void> {
    const started = Date.now();
    try {
      const receipt = await this.deliver(host, { ...item.payload,
        pressure: { queueAgeMs: started - item.enqueued, rpcMs: queue.rpcMs } });
      if (item.payload.initialFailure && receipt.initialFailureAck !== 1) throw new Error("GONVEX_DELIVERY_INITIAL_FAILURE_UNSUPPORTED");
      if (item.recovery) {
        if (receipt.recoveryAck !== 1) throw new DeliveryRecoveryUnsupportedError();
        queue.recoveryRequired = false;
      }
      item.resolve(receipt);
    } catch (error) { item.reject(error); }
    finally {
      queue.rpcMs = Date.now() - started;
      this.release(queue, item);
      if (item.recovery) queue.recovery = undefined;
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

  private release(queue: Queue, item: Pending): void {
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
    return queue ? queue.pending.length + Number(queue.running) : 0;
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
  if (payload.membership) {
    bytes += 128;
    for (const id of payload.membership.members) bytes += id.length * 8 + 64;
    for (const id of payload.membership.accounts) bytes += id.length * 8 + 64;
  }
  for (const text of payload.shared) bytes += text.length * 8 + 64;
  for (const [id, work] of Object.entries(payload.sockets)) {
    bytes += id.length * 8 + 256;
    if(work.initialScope)bytes+=256+initialRefBytes(work.initialScope);
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

/** Object keys, array slots, string/copy and safe-integer metadata allowance. */
function initialRefBytes(ref: InitialRequestRef): number {
  return 1024 + (ref.kind.length + ref.id.length + ref.path.length) * 8;
}
