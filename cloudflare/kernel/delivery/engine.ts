// Replica delivery in the tenant object: per-connection subscriptions, open
// queues, the commit feed and watermarks (lib.rs socket loop 1840-2420,
// 3585-3692, 4188-4300). Placement-neutral: frames leave through a
// `DeliveryOutlet`, one ordered channel per socket holder (a gateway object by
// default, or this object's own sockets), and the holder's `SocketDelivery`
// turns them into frames or `replica.batch` envelopes.
//
// Work runs one task at a time under the tenant's invocation queue
// (`serial`), so an open's reads and a commit's reads each see one state. A
// commit is read from the change log after the delivery's own cursor; commits
// that land while a task runs coalesce into the next wake, as Rust's feed
// task does. Nothing here is written to SQLite (brief: nothing ephemeral).

import { type RecoveryDelivery, type RecoveryReceipt, type ScopedSocketWork } from "./recovery";
import type { LiveQueries } from "../query/live.ts";
import type { ClientFrame } from "../session/decode.ts";
import type { Negotiated } from "../session/capabilities.ts";
import type { SocketView, TenantSession } from "../session/ports.ts";
import type { StorageSession } from "../storage/index.ts";
import { EvalMemo } from "../visibility/class.ts";
import type { JsonValue } from "../visibility/json.ts";
import { VisibilityRegistry } from "../visibility/tenant.ts";
import type { ReplicaModule } from "./definition.ts";
import { applyTransaction, EventShare, TransactionReads, type FeedFrame } from "./feed.ts";
import { compactTransaction, deltaBody, deltaFrame, deltaTail, errorFrame, needHashesFrame, readyFrame, resetFrame, transactionFrame, watermarkFrame } from "./frames.ts";
import { CollapsedChanges, readClock, readTransactions, type FeedTransaction } from "./log.ts";
import { REPLICA_DELIVERY_VERSION, type ReplicaDelivery, type ReplicaReceipt, type SocketOp, type SocketWork } from "./protocol.ts";
import type { ScanProgress } from "./scan.ts";
import { openReplica, sqlOf, type OpenRequest, type Subscription } from "./subscription.ts";

/** `REPLICA_OPEN_BATCH_LIMIT`, and the legacy paged-subscription limit per socket. */
export const OPEN_BATCH_LIMIT = 256;
/** Opens waiting in one tenant object (Rust: process-wide admission, `GONVEX_REPLICA_OPEN_MAX_WAITING`). */
export const MAX_WAITING_OPENS = 4096;
/** One open request: `json_len * 2 + 1024 <= 32 MiB` (replica_open_queue.rs `MAX_BYTES`). */
export const OPEN_REQUEST_MAX_BYTES = 32 * 1024 * 1024;
/** Payloads queued to one holder before its sockets are reset `change-feed-lagged`. */
export const MAX_QUEUED_PAYLOADS = 256;
/**
 * Characters of text in one payload (at most 3 UTF-8 bytes each, so 24 MiB): Workers RPC refuses an argument over
 * 32 MiB (0020 §6, 0022 §4.4). A larger delivery goes as several payloads, in order.
 */
export const MAX_PAYLOAD_CHARS = 8 << 20;

const opChars = (op: SocketOp): number => op.reduce<number>((n, part) => n + (typeof part === "string" ? part.length : 8), 0);

/**
 * Splits a payload under `MAX_PAYLOAD_CHARS`: whole sockets first, then one socket's ops across consecutive parts.
 * Each part carries only the shared texts it references; the watermark, membership and a socket's `opened`/`removed`
 * ride in the last part that touches it. One op larger than the limit still goes alone (the holder refuses a frame
 * over the platform's outbound limit, 0018).
 */
export function splitPayload(payload: ReplicaDelivery, limit = MAX_PAYLOAD_CHARS): ReplicaDelivery[] {
  const size = (ops: readonly SocketOp[]) => ops.reduce((n, op) => n + opChars(op) + (op[0] === "i" ? payload.shared[op[1]]!.length : op[0] === "d" ? payload.shared[op[3]]!.length : 0), 0);
  const total = Object.values(payload.sockets).reduce((n, work) => n + size(work.ops), 0);
  if (total <= limit) return [payload];
  type Part = { sockets: Record<string, SocketWork>; chars: number };
  const parts: Part[] = [{ sockets: {}, chars: 0 }];
  for (const [id, work] of Object.entries(payload.sockets)) {
    let current: SocketOp[] = [];
    for (const op of work.ops) {
      const chars = size([op]);
      let part = parts[parts.length - 1]!;
      if (part.chars + chars > limit && (part.chars > 0 || current.length)) {
        if (current.length) part.sockets[id] = { ops: current, subscriptionEpoch: (work as ScopedSocketWork).subscriptionEpoch } as ScopedSocketWork;
        parts.push((part = { sockets: {}, chars: 0 }));
        current = [];
      }
      current.push(op);
      part.chars += chars;
    }
    const { ops: _ops, ...rest } = work;
    parts[parts.length - 1]!.sockets[id] = { ops: current, ...rest };
  }
  return parts.map((part, index) => {
    const shared: string[] = [];
    const at = new Map<number, number>();
    const ref = (i: number) => { let j = at.get(i); if (j === undefined) { j = shared.length; shared.push(payload.shared[i]!); at.set(i, j); } return j; };
    const sockets: Record<string, SocketWork> = {};
    for (const [id, work] of Object.entries(part.sockets)) {
      sockets[id] = { ...work, ops: work.ops.map((op): SocketOp => (op[0] === "i" ? ["i", ref(op[1])] : op[0] === "d" ? ["d", op[1], op[2], ref(op[3]), op[4]] : op)) };
    }
    const last = index === parts.length - 1;
    return {
      v: payload.v, instance: payload.instance, ...(payload.scopeFence ? { scopeFence: payload.scopeFence } : {}), shared, sockets,
      ...(last && payload.watermark !== undefined ? { watermark: payload.watermark } : {}),
      ...(last && payload.membership ? { membership: payload.membership } : {}),
    };
  });
}

/** Where a connection's frames go: one ordered channel per holder. */
export interface DeliveryOutlet {
  /** Queues `payload` for `host` after everything queued before; resolves with the holder's receipt. */
  send(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined>;
  /** Payloads queued for `host` and not yet accepted. */
  queued(host: string): number;
  /** X01: holder-specific byte/count pressure; the reset still uses this engine's ordered channel. */
  backlogged?(host: string): boolean;
  /** X01: same ordered channel, constant-size recovery reserve; an ordinary receipt is never an ack. */
  reset?(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined>;
  /** Drops what is queued for `host` (lag), keeping order for what comes next. */
  discard(host: string): void;
  /** Resolves once everything queued for `host` was accepted. */
  idle(host: string): Promise<void>;
  /** Every holder of this tenant's sockets (a commit's watermark goes to each). */
  hosts(): Promise<string[]>;
}

interface Connection {
  readonly id: string;
  host: string;
  session: TenantSession;
  negotiated: Negotiated;
  scope: string;
  visibilityScope: string;
  /** The session epoch the tenant last saw (re-auth without preservation, discard). */
  epoch: number;
  readonly subscriptions: Map<string, Subscription>;
  readonly pending: OpenRequest[];
  /** A subscription opened since the last bootstrap watermark (`opening_cohort_dirty`). */
  openingCohort: boolean;
  /** `sent_replica_revision`. */
  sentRevision: number;
  /** Stream token per subscription id (default `~<identity>`, suffixed on a collision). */
  readonly tokens: Map<string, string>;
}

export interface DeliveryOptions {
  readonly tenantId: string;
  /** Runs `fn` after every invocation already queued on the object (K07's `serial`). */
  readonly serial: <T>(fn: () => Promise<T>) => Promise<T>;
  /** One synchronous body in one Query invocation (K01). */
  readonly read: <T>(body: (db: StorageSession) => T) => Promise<T>;
  readonly module: () => ReplicaModule | undefined;
  readonly outlet: DeliveryOutlet;
  readonly registry?: VisibilityRegistry;
  /** After each committed transaction is delivered (K19 `operatorDataChanged`). */
  readonly delivered?: (revision: number) => void;
  /** K22: live queries share the commit snapshot and this ordered outlet. */
  readonly live?: LiveQueries | undefined;
}

export interface DeliveryStats {
  opens: number;
  commits: number;
  payloads: number;
  statements: number;
  rowsRead: number;
  resets: number;
  /** Reads of the commit feed (clock, change log, affected rows), across every connection. */
  commitStatements: number;
  commitRowsRead: number;
}

const byId = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
interface HostBarrier { pending: boolean; retryAt: number; catchup: boolean }
interface DeferredQuery { readonly socket: SocketView; readonly id: string; readonly snapshot: (db: StorageSession) => string }

/** FNV-1a 64 of path and arguments: the stream identity (any per-socket-unique token works, A.4 row 2). */
export function streamIdentity(path: string, argsText: string): string {
  let h = 0xcbf29ce484222325n;
  for (const ch of `${path}\u0000${argsText}`) {
    h ^= BigInt(ch.codePointAt(0)!);
    h = (h * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return h.toString(16).padStart(16, "0");
}

export class ReplicaDeliveryEngine {
  readonly instance = crypto.randomUUID();
  readonly registry: VisibilityRegistry;
  readonly stats: DeliveryStats = { opens: 0, commits: 0, payloads: 0, statements: 0, rowsRead: 0, resets: 0, commitStatements: 0, commitRowsRead: 0 };
  private readonly connections = new Map<string, Connection>();
  /** Reset fences also cover a request already in flight when its connection was dropped. */
  private readonly epochs = new Map<string, number>();
  private delivered: { epoch: string; revision: number } | undefined;
  private wanted = 0;
  private running: Promise<void> | undefined;
  /** Highest revision whose payload each holder accepted. */
  private readonly accepted = new Map<string, number>();
  private readonly recovering = new Map<string, HostBarrier>();
  private readonly fencing = new Map<string, HostBarrier>();
  private readonly deferredQueries = new Map<string, DeferredQuery[]>();
  private waitingOpens = 0;
  private lastOpened: string | undefined;

  constructor(private readonly options: DeliveryOptions) {
    this.registry = options.registry ?? new VisibilityRegistry();
  }

  // ---- frames from sockets ---------------------------------------------------------------

  /** `replica.open`, `replica.openMany`, `replica.close` of a socket with a tenant session. Frames go out through the outlet. */
  dispatch(socket: SocketView, frame: ClientFrame): string[] | undefined {
    if (frame.type !== "replica.open" && frame.type !== "replica.openMany" && frame.type !== "replica.close") return undefined;
    const session = socket.tenant;
    if (!session) return [];
    const connection = this.connection(socket, session);
    if (!connection) return [];
    this.retryBarriers(connection.host);
    if (frame.type === "replica.close") {
      this.removePending(connection, frame.id);
      this.remove(connection, frame.id);
      return [];
    }
    const opens = frame.type === "replica.open" ? [frame] : frame.opens;
    const refused: string[] = [];
    connection.openingCohort = true;
    for (const raw of opens) {
      const request: OpenRequest = {
        id: raw.id, path: raw.path, args: raw.args as JsonValue, keys: raw.keys, hashes: raw.hashes, fullIntegrity: raw.fullIntegrity,
        ...(raw.cursor ? { cursor: raw.cursor } : {}), ...(raw.digest !== undefined ? { digest: raw.digest } : {}),
      };
      this.removePending(connection, request.id);
      this.remove(connection, request.id);
      if (JSON.stringify(request).length * 2 + 1024 > OPEN_REQUEST_MAX_BYTES) {
        refused.push(errorFrame(request.id, request.path, "individual replica open request is too large"));
        continue;
      }
      if (this.waitingOpens >= MAX_WAITING_OPENS || connection.pending.length >= OPEN_BATCH_LIMIT) {
        refused.push(errorFrame(request.id, request.path, "too many pending replica opens"));
        continue;
      }
      connection.pending.push(request);
      this.waitingOpens += 1;
    }
    if (refused.length) this.post(connection, { ops: refused.map((text) => ["f", text] as const) });
    this.kick();
    return [];
  }

  /** Live-query-only sockets also participate in the single commit channel. */
  observe(socket: SocketView, notify = true): boolean {
    if (socket.tenant) {
      const connection = this.connection(socket, socket.tenant);
      if (!connection) return false;
      if (notify) this.post(connection, { ops: [] });
      return true;
    }
    return false;
  }

  /** Called while the assembled tenant still owns the snapshot's invocation turn. */
  queryReply(socket: SocketView, replies: readonly string[]): void {
    if (!socket.tenant) return;
    const connection = this.connection(socket, socket.tenant);
    if (!connection) return;
    const computed = this.options.live?.computedRevisions(connection.id) ?? [];
    this.post(connection, { ops: replies.map((text) => ["f", text] as const), ...(computed.length ? { watermark: Math.min(...computed) } : {}) });
  }

  /** Initial LQ snapshots and their enqueue share the reducer/commit serialization turn. */
  querySubscribe(socket: SocketView, snapshot: (db: StorageSession) => string, recovery = false): Promise<string[]> {
    return this.options.serial(async () => {
      if (socket.tenant && !this.observe(socket, false)) return [];
      if (!recovery && this.paused(socket.host)) {
        const pending = this.deferredQueries.get(socket.connectionId) ?? [];
        // This stores only the semantic request/plan, never a retained row result.
        pending.push({ socket, id: (socket as SocketView & { queryId?: string }).queryId ?? "", snapshot });
        this.deferredQueries.set(socket.connectionId, pending);
        this.retryBarriers(socket.host);
        return [];
      }
      const replies = await this.options.read((db) => [snapshot(db)]);
      if (recovery) return replies;
      this.queryReply(socket, replies);
      return [];
    });
  }

  queryUnsubscribe(socket: SocketView, id: string): boolean {
    if ((socket.subscriptionEpoch ?? 0) < (this.epochs.get(socket.connectionId) ?? 0)) return false;
    const pending = this.deferredQueries.get(socket.connectionId);
    if (pending) this.deferredQueries.set(socket.connectionId, pending.filter((query) => query.id !== id));
    this.options.live?.unsubscribe(socket.connectionId, id);
    return true;
  }

  /** The socket's session changed (re-auth without preservation, discard, close): its subscriptions go. */
  reset(connectionId: string, epoch: number): void {
    if (epoch < (this.epochs.get(connectionId) ?? 0)) return;
    this.epochs.set(connectionId, epoch);
    this.deferredQueries.delete(connectionId);
    const connection = this.connections.get(connectionId);
    if (!connection || epoch < connection.epoch) return;
    this.options.live?.clear(connectionId);
    this.drop(connection);
    this.accepted.delete(connectionId);
    this.connections.delete(connectionId);
  }

  /**
   * K07's own-membership hand-off (lib.rs `membership_changed_frames`), on the same ordered channel as the commits
   * (0048 §1a): the holder discards the Member's sessions and sends `auth.error membership-changed`, then the watermark.
   */
  membershipChanged(host: string, memberId: string, revision: number): void {
    for (const connection of [...this.connections.values()]) {
      if (connection.session.member.id !== memberId || revision <= connection.session.admissionRevision) continue;
      this.drop(connection);
      this.connections.delete(connection.id);
    }
    void this.sendOne(host, this.payload([], {}, undefined, { revision, members: [memberId], accounts: [] }), 0);
  }

  /** K07's call watermark rule: undefined without subscriptions, else the revision this socket's holder accepted. */
  subscriptionFloor(socket: SocketView): number | undefined {
    const connection = this.connections.get(socket.connectionId);
    if (connection && connection.epoch !== (socket.subscriptionEpoch ?? 0)) return undefined;
    if (!connection || (!connection.subscriptions.size && !connection.pending.length && !this.options.live?.subscriptionIds(connection.id).length)) return undefined;
    if (this.recovering.has(connection.host) || this.fencing.has(connection.host)) return 0;
    return Math.min(this.accepted.get(connection.id) ?? 0, ...(this.options.live?.computedRevisions(connection.id) ?? []));
  }

  /** Subscriptions of a connection (tests). */
  subscriptions(connectionId: string): ReadonlyMap<string, Subscription> {
    return this.connections.get(connectionId)?.subscriptions ?? new Map();
  }

  /**
   * The object woke: the delivery's cursor starts at the clock. Earlier commits
   * reached the sockets of the previous instance; holders reset what that
   * instance served when they see this one (`ReplicaDelivery.instance`).
   */
  start(clock: { epoch: string; revision: number } | undefined): void {
    if (clock && this.delivered === undefined) this.delivered = { epoch: clock.epoch, revision: clock.revision };
  }

  /** A commit was published (K03 `ChangeWake`): deliver everything up to `revision`. */
  wake(revision: number): void {
    for (const host of new Set([...this.recovering.keys(), ...this.fencing.keys()])) this.retryBarriers(host);
    this.wanted = Math.max(this.wanted, revision);
    this.kick();
  }

  /** Resolves when no work is queued or running (tests and drains). */
  async settled(): Promise<void> {
    while (this.running) await this.running;
  }

  // ---- connections -----------------------------------------------------------------------

  private connection(socket: SocketView, session: TenantSession): Connection | undefined {
    const epoch = (socket as SocketView & { subscriptionEpoch?: number }).subscriptionEpoch ?? 0;
    if (epoch < (this.epochs.get(socket.connectionId) ?? 0)) return undefined;
    this.epochs.set(socket.connectionId, epoch);
    let connection = this.connections.get(socket.connectionId);
    if (connection && epoch > connection.epoch) {
      this.drop(connection);
      this.accepted.delete(connection.id);
      connection = undefined;
    }
    const replica = (socket as SocketView & { replica?: { scope: string; visibilityScope: string } | null }).replica ?? null;
    if (!connection) {
      this.accepted.delete(socket.connectionId);
      connection = {
        id: socket.connectionId, host: socket.host, session, negotiated: socket.negotiated, epoch,
        scope: replica?.scope ?? "", visibilityScope: replica?.visibilityScope ?? "",
        subscriptions: new Map(), pending: [], openingCohort: false, sentRevision: 0, tokens: new Map(),
      };
      this.connections.set(socket.connectionId, connection);
    } else {
      connection.host = socket.host;
      connection.session = session;
      connection.negotiated = socket.negotiated;
      if (replica) { connection.scope = replica.scope; connection.visibilityScope = replica.visibilityScope; }
    }
    return connection;
  }

  private drop(connection: Connection, clearLive = true): void {
    for (const subscription of connection.subscriptions.values()) subscription.mask.clear();
    connection.subscriptions.clear();
    if (clearLive) this.options.live?.clear(connection.id);
    this.waitingOpens -= connection.pending.length;
    connection.pending.length = 0;
    connection.tokens.clear();
    connection.openingCohort = false;
  }

  private removePending(connection: Connection, id: string): void {
    const before = connection.pending.length;
    for (let i = connection.pending.length - 1; i >= 0; i--) if (connection.pending[i]!.id === id) connection.pending.splice(i, 1);
    this.waitingOpens -= before - connection.pending.length;
  }

  private remove(connection: Connection, id: string): void {
    const subscription = connection.subscriptions.get(id);
    if (!subscription) return;
    subscription.mask.clear();
    connection.subscriptions.delete(id);
    connection.tokens.delete(id);
  }

  /** The stream token of a subscription on its socket (one token per subscription; a duplicate collection gets `.n`). */
  private token(connection: Connection, subscription: Subscription): string {
    const existing = connection.tokens.get(subscription.id);
    if (existing) return existing;
    const base = `~${streamIdentity(subscription.path, subscription.argsText)}`;
    const taken = new Set([...connection.tokens].filter(([id]) => connection.subscriptions.has(id)).map(([, token]) => token));
    let token = base;
    for (let n = 2; taken.has(token); n++) token = `${base}.${n}`;
    connection.tokens.set(subscription.id, token);
    return token;
  }

  // ---- the task loop ---------------------------------------------------------------------

  private kick(): void {
    if (this.running) return;
    this.running = this.run().finally(() => { this.running = undefined; });
  }

  private async run(): Promise<void> {
    for (;;) {
      if (this.delivered === undefined || this.wanted > this.delivered.revision) {
        const more = await this.options.serial(() => this.deliverCommits());
        if (more) continue;
      }
      const next = this.nextOpen();
      if (next) {
        await this.options.serial(() => this.processOpen(next.connection, next.request));
        continue;
      }
      if (this.delivered !== undefined && this.wanted <= this.delivered.revision) return;
    }
  }

  /** Round robin over connections with pending opens (one open per turn, as Rust pages one subscription per turn). */
  private nextOpen(): { connection: Connection; request: OpenRequest } | undefined {
    const ids = [...this.connections.keys()].filter((id) => { const c = this.connections.get(id)!; return c.pending.length && !this.paused(c.host); }).sort(byId);
    if (!ids.length) return undefined;
    const id = ids.find((candidate) => this.lastOpened === undefined || candidate > this.lastOpened) ?? ids[0]!;
    this.lastOpened = id;
    const connection = this.connections.get(id)!;
    const request = connection.pending.shift()!;
    this.waitingOpens -= 1;
    return { connection, request };
  }

  private async processOpen(connection: Connection, request: OpenRequest): Promise<void> {
    if (this.connections.get(connection.id) !== connection) return;
    // Every commit up to now is delivered before the snapshot is read, so the subscription joins at the clock.
    await this.deliverCommits();
    const ops: SocketOp[] = [];
    const flush = () => {
      if (!ops.length) return;
      this.post(connection, { ops: ops.splice(0) });
    };
    const progress: ScanProgress = { statements: 0, rowsRead: 0 };
    const subscription = await openReplica(request, {
      session: connection.session,
      module: this.options.module(),
      sumNegotiated: connection.negotiated.setDigest,
      paged: connection.negotiated.snapshotPages && !connection.negotiated.nativeRows,
      scope: connection.scope,
      visibilityScope: connection.visibilityScope,
      registry: this.registry,
      read: this.options.read,
      emit: (frames) => { for (const text of frames) ops.push(["f", text]); },
      drain: async () => { flush(); await this.options.outlet.idle(connection.host); },
      progress,
    });
    this.stats.opens += 1;
    this.stats.statements += progress.statements;
    this.stats.rowsRead += progress.rowsRead;
    if (this.connections.get(connection.id) !== connection) return;
    const opened: Record<string, string> = {};
    if (subscription) {
      // The connection may have closed or reopened the id while this open ran.
      connection.subscriptions.set(subscription.id, subscription);
      connection.tokens.delete(subscription.id);
      opened[subscription.id] = subscription.path;
    }
    // Mixed database epochs cannot share a numeric certificate: Rust ends the connection, and the client's
    // reconnect re-admits every subscription in the current epoch (lib.rs 1859-1866).
    if (!connection.pending.length && connection.openingCohort && new Set([...connection.subscriptions.values()].map((s) => s.cursor.epoch).concat(this.options.live?.epochs(connection.id) ?? [])).size > 1) {
      ops.push(["x"]);
      this.post(connection, { ops: ops.splice(0) });
      this.drop(connection);
      this.connections.delete(connection.id);
      return;
    }
    // Bootstrap watermark (`bootstrap_watermark_after_sent_snapshots`): once the cohort's snapshots are all out.
    if (!connection.pending.length && connection.openingCohort) {
      const revisions = [...connection.subscriptions.values()].map((s) => s.cursor.revision);
      if (revisions.length) {
        const revision = Math.min(...revisions);
        if (revision >= connection.sentRevision) {
          ops.push(["f", watermarkFrame(revision)]);
          connection.sentRevision = revision;
          connection.openingCohort = false;
        }
      }
    }
    this.post(connection, { ops: ops.splice(0), ...(subscription ? { opened } : { removed: [request.id] }) });
  }

  /** Reads and delivers every committed transaction after the delivery's cursor. True when more arrived meanwhile. */
  private async deliverCommits(): Promise<boolean> {
    const target = this.wanted;
    try { this.knownHosts = await this.options.outlet.hosts(); } catch (error) { console.error("delivery: holder list failed", error); }
    const work = await this.options.read((db) => {
      const before = { statements: db.stats.statements, rowsRead: db.stats.rowsRead };
      try {
        return this.commitWork(db);
      } finally {
        this.stats.commitStatements += db.stats.statements - before.statements;
        this.stats.commitRowsRead += db.stats.rowsRead - before.rowsRead;
      }
    });
    await this.sendAll(work.payloads);
    this.wanted = Math.max(this.wanted, work.clock.revision);
    return this.wanted > target;
  }

  /** The synchronous part of `deliverCommits`, inside one Query invocation. */
  private commitWork(db: StorageSession): { payloads: Array<[string, ReplicaDelivery, number]>; clock: { revision: number } } {
    {
      const clock = readClock(sqlOf(db));
      if (this.delivered === undefined) {
        // First wake of this instance: nothing before the clock is owed to anyone it knows.
        this.delivered = { epoch: clock.epoch, revision: clock.revision };
        return { payloads: [] as Array<[string, ReplicaDelivery, number]>, clock };
      }
      if (clock.revision < this.delivered.revision || clock.retained > this.delivered.revision) {
        // FeedEvent::Reset: the log no longer holds what follows the delivery's cursor (pruned, or the
        // database was replaced by one at an earlier revision); nothing can say what each client holds.
        const payloads = this.resetAll("change-feed-reset", this.options.live?.applyFeedSnapshot(db, { kind: "reset" }));
        this.delivered = { epoch: clock.epoch, revision: clock.revision };
        return { payloads, clock };
      }
      // A new epoch with the log intact (an import that kept the revisions) is delivered as Rust's feed delivers it:
      // transactions carry the new epoch, subscriptions keep their cursor's (lib.rs, feed task).
      const payloads: Array<[string, ReplicaDelivery, number]> = [];
      const transactions = readTransactions(sqlOf(db), clock.epoch, this.delivered.revision, clock.revision);
      let previous = this.delivered.revision;
      for (const transaction of transactions) {
        this.registry.apply(transaction, previous);
        previous = transaction.revision;
        payloads.push(...this.deliverTransaction(db, transaction));
      }
      this.delivered = { epoch: clock.epoch, revision: clock.revision };
      return { payloads, clock };
    }
  }

  /** Every subscription of every connection gets `replica.reset` (lib.rs 2367-2386, replica.rs 1340-1355). */
  private resetAll(reason: string, live?: ReadonlyMap<string, string[]>): Array<[string, ReplicaDelivery, number]> {
    const byHost = new Map<string, Record<string, SocketWork>>();
    for (const connection of this.connections.values()) {
      const ops: SocketOp[] = [...(live?.get(connection.id) ?? []).map((text): SocketOp => ["f", text]), ...[...connection.subscriptions.values()].sort((a, b) => byId(a.id, b.id)).map((s) => ["f", resetFrame(s.id, s.path, reason)] as const)];
      const removed = [...connection.subscriptions.keys()];
      this.stats.resets += removed.length;
      this.drop(connection, false);
      connection.sentRevision = 0;
      if (!ops.length) continue;
      let sockets = byHost.get(connection.host);
      if (!sockets) byHost.set(connection.host, (sockets = {}));
      sockets[connection.id] = { ops, removed, subscriptionEpoch: connection.epoch } as ScopedSocketWork;
    }
    return [...byHost].map(([host, sockets]) => [host, this.payload([], sockets), 0]);
  }

  /** One committed transaction for every connection: frames per socket, shared texts once. */
  private deliverTransaction(db: StorageSession, transaction: FeedTransaction): Array<[string, ReplicaDelivery, number]> {
    this.stats.commits += 1;
    // Ruling 0048(a): evaluate first, then prepend frames to each socket's replica work.
    const live = this.options.live?.applyFeedSnapshot(db, {
      kind: "transaction", epoch: transaction.epoch, revision: transaction.revision,
      changes: transaction.changes.map((change) => ({ ...change,
        oldValue: change.oldValue && typeof change.oldValue === "object" && !Array.isArray(change.oldValue) ? change.oldValue : null,
        newValue: change.newValue && typeof change.newValue === "object" && !Array.isArray(change.newValue) ? change.newValue : null,
      })),
    });
    const collapsed = new CollapsedChanges(transaction.changes);
    const shared: string[] = [];
    const index = new Map<string, number>();
    const share = (text: string) => {
      let at = index.get(text);
      if (at === undefined) { at = shared.length; shared.push(text); index.set(text, at); }
      return at;
    };
    const membership = membershipChanges(transaction);
    // One set of reads per transaction, shared by every subscription of every connection (no SQL per subscriber).
    const reads = new TransactionReads({
      sql: db,
      kinds: (table) => db.catalogOf(table).columns,
      clock: () => { const c = readClock(sqlOf(db)); return { revision: c.revision, retained: c.retained }; },
    });
    const eventShare = new EventShare(new EvalMemo());
    const byHost = new Map<string, Record<string, SocketWork>>();
    const lagged = new Set<string>();
    for (const host of new Set([...this.connections.values()].map((c) => c.host))) {
      if (this.options.outlet.queued(host) >= MAX_QUEUED_PAYLOADS || this.options.outlet.backlogged?.(host)) lagged.add(host);
    }
    for (const connection of [...this.connections.values()].sort((a, b) => byId(a.id, b.id))) {
      const session = connection.session;
      if (membership && transaction.revision > session.admissionRevision
        && (membership.members.includes(session.member.id) || membership.accounts.includes(session.identity.account.id))) {
        // The holder discards the session and sends `auth.error membership-changed` then the watermark.
        this.drop(connection);
        this.connections.delete(connection.id);
        continue;
      }
      if (lagged.has(connection.host)) {
        this.requestRecovery(connection.host);
        continue;
      }
      const queryFrames = live?.get(connection.id) ?? [];
      const { frames, removed } = applyTransaction(connection.subscriptions, transaction, collapsed, reads, eventShare, this.options.tenantId);
      for (const subscription of removed) {
        subscription.mask.clear();
        connection.tokens.delete(subscription.id);
      }
      connection.sentRevision = Math.max(connection.sentRevision, transaction.revision);
      const ops: SocketOp[] = [...queryFrames.map((text): SocketOp => ["f", text]), ...frames.map((frame) => this.op(connection, frame, share))];
      let sockets = byHost.get(connection.host);
      if (!sockets) byHost.set(connection.host, (sockets = {}));
      const watermark = Math.min(transaction.revision, ...(this.options.live?.computedRevisions(connection.id) ?? []));
      sockets[connection.id] = { ops, watermark, subscriptionEpoch: connection.epoch, ...(removed.length ? { removed: removed.map((s) => s.id) } : {}) } as ScopedSocketWork;
    }

    // A commit's watermark reaches every holder, including those with no work this time; each gets only its texts.
    const out: Array<[string, ReplicaDelivery, number]> = [];
    const hosts = new Set([...byHost.keys(), ...this.knownHosts]);
    for (const host of hosts) {
      const sockets = byHost.get(host) ?? {};
      out.push([host, this.payload(Object.keys(sockets).length ? shared : [], sockets, transaction.revision, membership && { revision: transaction.revision, ...membership }), transaction.revision]);
    }
    return out;
  }

  /** Holders registered with the tenant (refreshed before each send round). */
  private knownHosts: string[] = [];

  private op(connection: Connection, frame: FeedFrame, share: (text: string) => number): SocketOp {
    const batch = connection.negotiated.batchFrames;
    switch (frame.kind) {
      case "transaction":
        return batch ? ["i", share(transactionFrame(frame, false))] : ["f", connection.negotiated.noOldValues ? compactTransaction(frame) : transactionFrame(frame, true)];
      case "delta": {
        const s = frame.subscription;
        const delta = { path: s.path, cursor: s.cursor, upserts: frame.upserts, deleted: frame.deleted };
        if (!batch) return ["f", deltaFrame(s.id, { ...delta, digest: s.digest.digest(), truncated: s.window.truncated })];
        const token = this.token(connection, s);
        return ["d", s.id, token, share(deltaBody(token, delta)), deltaTail(s.digest.digest(), s.window.truncated)];
      }
      case "ready": {
        const s = frame.subscription;
        if (!batch) return ["f", readyFrame(s.id, s.cursor, s.digest.digest(), s.definition, s.path, s.window.truncated)];
        const token = this.token(connection, s);
        return ["r", s.id, token, readyFrame(token, s.cursor, s.digest.digest(), s.definition, s.path, s.window.truncated)];
      }
      case "needHashes":
        return ["f", needHashesFrame(frame.id, frame.path)];
    }
  }

  private payload(shared: readonly string[], sockets: Record<string, SocketWork>, watermark?: number, membership?: ReplicaDelivery["membership"]): ReplicaDelivery {
    return {
      v: REPLICA_DELIVERY_VERSION, instance: this.instance, scopeFence: 1, shared, sockets,
      ...(watermark !== undefined ? { watermark } : {}), ...(membership ? { membership } : {}),
    };
  }

  /** Work for one socket outside a commit (opens, refusals). */
  private post(connection: Connection, work: SocketWork): void {
    void this.send(connection.host, this.payload([], { [connection.id]: { ...work, subscriptionEpoch: connection.epoch } as ScopedSocketWork }), 0);
  }

  private async sendAll(payloads: Array<[string, ReplicaDelivery, number]>): Promise<void> {
    for (const [host, payload, revision] of payloads) void this.send(host, payload, revision);
  }

  private send(host: string, payload: ReplicaDelivery, revision: number): Promise<void> {
    if (this.paused(host)) {
      // Auth invalidation must not disappear behind a capacity/restart barrier.
      if (payload.membership) return this.sendOne(host, this.payload([], {}, undefined, payload.membership), 0, true);
      return Promise.resolve();
    }
    const parts = splitPayload(payload);
    for (const part of parts.slice(0, -1)) void this.sendOne(host, part, 0);
    return this.sendOne(host, parts[parts.length - 1]!, revision);
  }

  private accept(host: string, payload: ReplicaDelivery, receipt: ReplicaReceipt, revision: number): void {
    if (this.paused(host) || Object.keys(receipt.scopeRequired ?? {}).length) return;
    const missing = new Set(receipt.missing ?? []);
    for (const connection of this.connections.values()) {
      if (connection.host !== host) continue;
      if (missing.has(connection.id)) { this.drop(connection); this.connections.delete(connection.id); continue; }
      const work = payload.sockets[connection.id] as ScopedSocketWork | undefined;
      if (!work || work.subscriptionEpoch !== connection.epoch) continue;
      const watermark = work.watermark;
      if (watermark !== undefined) this.accepted.set(connection.id, Math.max(this.accepted.get(connection.id) ?? 0, watermark));
    }
    if (revision) this.options.delivered?.(revision);
  }

  private paused(host: string): boolean {
    return [this.recovering.get(host), this.fencing.get(host)].some((state) => state && !state.catchup);
  }

  private retryBarriers(host: string): void {
    const recovery = this.recovering.get(host), fence = this.fencing.get(host);
    if (recovery && !recovery.pending && Date.now() >= recovery.retryAt) this.requestRecovery(host);
    else if (fence && !fence.pending && Date.now() >= fence.retryAt) this.requestScopeFence(host);
  }

  private dropHost(host: string): void {
    for (const connection of this.connections.values()) if (connection.host === host) {
      this.stats.resets += connection.subscriptions.size;
      this.drop(connection);
      this.accepted.delete(connection.id);
    }
  }

  /** The challenge's authoritative epochs stamp EMPTY work only. */
  private requestScopeFence(host: string, supplied?: Readonly<Record<string, number>>): void {
    if (this.recovering.has(host) || this.fencing.get(host)?.pending) return;
    const state: HostBarrier = { pending: true, retryAt: 0, catchup: false };
    this.fencing.set(host, state);
    void this.options.serial(async () => {
      this.dropHost(host);
      this.options.outlet.discard(host);
      return { challenge: supplied ? Promise.resolve({ scopeRequired: supplied }) : this.options.outlet.send(host, this.payload([], {})) };
    }).then(async ({ challenge }) => {
      const challenged = await challenge;
      const scopes = challenged?.scopeRequired;
      if (!scopes) throw new Error("delivery scope challenge was not understood");
      const sockets: Record<string, SocketWork> = {};
      for (const [id, epoch] of Object.entries(scopes)) {
        if (!Number.isSafeInteger(epoch) || epoch < 0) throw new Error("invalid delivery scope challenge");
        sockets[id] = { ops: [], subscriptionEpoch: epoch } as ScopedSocketWork;
      }
      const receipt = await this.options.outlet.send(host, this.payload([], sockets));
      if (!receipt || Object.keys(receipt.scopeRequired ?? {}).length) throw new Error("delivery scope fence was not applied");
      const missing = new Set(receipt.missing ?? []);
      for (const [id, epoch] of Object.entries(scopes)) if (!missing.has(id) && receipt.scopeAck?.[id] !== epoch) throw new Error("delivery scope fence was not acknowledged");
      await this.finishBarrier(host, state, this.fencing);
    }).catch((error) => {
      if (this.fencing.get(host) !== state) return;
      state.pending = false; state.catchup = false; state.retryAt = Date.now() + 1000;
      console.error(`delivery: scope fence for ${host} requires a fresh event`, error);
    });
  }

  /** Catch-up is enqueued in one invocation turn; later normal sends stay behind its RPC acknowledgment. */
  private async finishBarrier(host: string, state: HostBarrier, owners: Map<string, HostBarrier>): Promise<void> {
    const catchup = await this.options.serial(async () => {
      if (owners.get(host) !== state) throw new Error("delivery barrier superseded");
      const result = await this.options.read((db) => {
        const ids = new Set([...this.connections.values()].filter((c) => c.host === host).map((c) => c.id));
        const frames = this.options.live?.applyFeedSnapshot(db, { kind: "reset" }, undefined, ids);
        const clock = readClock(sqlOf(db));
        const sockets: Record<string, SocketWork> = {};
        for (const id of ids) {
          const computed = this.options.live?.computedRevisions(id) ?? [];
          sockets[id] = { ops: (frames?.get(id) ?? []).map((text) => ["f", text] as const), subscriptionEpoch: this.connections.get(id)!.epoch, watermark: computed.length ? Math.min(...computed) : 0 } as ScopedSocketWork;
        }
        // Catch-up concerns only the freshly authorized known connections.
        // A global zero watermark would challenge unrelated quiet sockets.
        return { payload: Object.keys(sockets).length ? this.payload([], sockets) : undefined, revision: clock.revision };
      });
      // All actual receivers already acknowledged the real reset/rebuild.
      // With no remaining recipient/work, no receipt or progress is invented.
      if (!result.payload) return { ...result, receipt: undefined };
      const receipt = this.options.outlet.send(host, result.payload);
      // The outlet runs exactly one RPC per host: normals may now enqueue,
      // but cannot be delivered before catch-up was acknowledged. Floors stay
      // zero until the barrier's receipt resolves and ownership is cleared.
      state.catchup = true;
      for (const [id, queries] of [...this.deferredQueries]) {
        const own = queries.filter((query) => query.socket.host === host);
        if (!own.length) continue;
        this.deferredQueries.delete(id);
        for (const query of own) {
          if ((query.socket.subscriptionEpoch ?? 0) !== this.epochs.get(id)) continue;
          const replies = await this.options.read((db) => [query.snapshot(db)]);
          this.queryReply(query.socket, replies);
        }
      }
      return { ...result, receipt };
    });
    if (!catchup.payload) {
      if (owners.get(host) === state) { owners.delete(host); this.kick(); }
      return;
    }
    const accepted = await catchup.receipt;
    if (!accepted || Object.keys(accepted.scopeRequired ?? {}).length) throw new Error("delivery catch-up was discarded or unscoped");
    if (owners.get(host) !== state) return;
    owners.delete(host);
    this.accept(host, catchup.payload, accepted, catchup.revision);
    this.kick();
  }

  private requestRecovery(host: string): void {
    const existing = this.recovering.get(host);
    if (existing?.pending) return;
    const state: HostBarrier = { pending: true, retryAt: 0, catchup: false };
    this.fencing.delete(host);
    this.recovering.set(host, state); // Suppress immediately, before the serialized reset job can run.
    void this.options.serial(async () => {
      this.dropHost(host);
      const payload: RecoveryDelivery = { ...this.payload([], {}), recovery: { reason: "change-feed-lagged" } };
      if (this.options.outlet.reset) return { recovery: this.options.outlet.reset(host, payload) };
      this.options.outlet.discard(host);
      return { recovery: this.options.outlet.send(host, payload) };
    }).then(async ({ recovery }) => {
      const receipt = await recovery as RecoveryReceipt | undefined;
      if (receipt?.recoveryAck !== 1) throw new Error("GONVEX_DELIVERY_RECOVERY_UNSUPPORTED");
      await this.finishBarrier(host, state, this.recovering);
    }).catch((error) => {
      if (!this.recovering.has(host)) this.recovering.set(host, state);
      state.pending = false;
      state.catchup = false;
      state.retryAt = Date.now() + 1000;
      console.error(`delivery: recovery of ${host} requires a fresh event`, error);
    });
  }

  private sendOne(host: string, payload: ReplicaDelivery, revision: number, membershipOnly = false): Promise<void> {
    if (!membershipOnly && this.paused(host)) return Promise.resolve();
    this.stats.payloads += 1;
    return this.options.outlet.send(host, payload).then((receipt) => {
      // X01 settles discarded pending work with undefined; it certifies no delivery.
      if (Object.keys(receipt?.scopeRequired ?? {}).length) this.requestScopeFence(host, receipt!.scopeRequired);
      else if (receipt) this.accept(host, payload, receipt, revision);
    }, (error) => {
      if ((error as { code?: unknown })?.code === "GONVEX_DELIVERY_CAPACITY") this.requestRecovery(host);
      else { console.error(`delivery: payload to ${host} failed`, error); this.requestRecovery(host); }
    });
  }

}

/** lib.rs `membership_affects`: a `members` change in the access columns, by Member id and account id. */
function membershipChanges(transaction: FeedTransaction): { members: string[]; accounts: string[] } | undefined {
  const members = new Set<string>(), accounts = new Set<string>();
  for (const change of transaction.changes) {
    if (change.table !== "members") continue;
    if (!change.changedColumns.some((c) => c === "account_id" || c === "status" || c === "role" || c === "permissions" || c === "membership_revision")) continue;
    for (const row of [change.oldValue, change.newValue]) {
      if (!row || typeof row !== "object" || Array.isArray(row)) continue;
      if (typeof row.id === "string") members.add(row.id);
      if (typeof row.account_id === "string") accounts.add(row.account_id);
    }
  }
  return members.size || accounts.size ? { members: [...members], accounts: [...accounts] } : undefined;
}
