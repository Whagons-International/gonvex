/**
 * Live Queries: `query.subscribe`, `query.subscribeMany`, `query.unsubscribe`
 * and the reruns after each commit, with the Rust runtime's frames
 * (`live_query.rs` `open_live_query`, `apply_live_query_feed_event`,
 * `resume_message`; `lib.rs` 3068-3259).
 *
 * Placement-neutral: it holds each socket's subscriptions by connection id
 * and returns frames; the object that owns the sockets (the tenant object
 * today, a gateway through delivery) sends them. Live query frames for a
 * commit go out before that commit's replica frames and watermark, as in
 * Rust (`lib.rs` 2203-2210), so delivery calls `applyFeed` first.
 *
 * What Rust does and this keeps:
 * - A subscription reruns its whole plan and sends `query.result` (reason
 *   `change`, or `reconnect` after a feed reset) only when the result or the
 *   epoch changed. Rust never sends `query.patch`, `query.pagePatch` or
 *   `query.objectPatch` (0012 R19), so neither does the kernel.
 * - `subscriptionRevision` is `{epoch, sequence}` of the snapshot the result
 *   was computed at; `windowRevision` is the SHA-256 of the result. A
 *   subscribe whose `windowRevision` equals the fresh one gets
 *   `query.progress` instead of the result (resume).
 * - Re-subscribing an id replaces it; a failed subscribe leaves the old one.
 *   Frames of one commit are in subscription-id order (Rust's `BTreeMap`).
 *
 * What differs in mechanism only: relevance. Rust skips reruns for changes a
 * member cannot observe (v2 masks). Here a rerun is skipped when no change
 * touches a table the plan reads (plan, relations, index projection, search
 * documents, visibility inputs), refined by K09's `changeConcerns` when it is
 * present. Skipping more or fewer reruns changes no frame, because an
 * unchanged result sends nothing. Subscriptions of one account with equal
 * path and arguments share one execution per commit, and their frames share
 * one encoding of the result.
 */
import type { StorageSession, TenantStorage } from "../storage";
import type { TenantSession } from "../session/ports";
import { UNAUTHENTICATED } from "../session/protocol";
import { executePlan, type Snapshot } from "./execute";
import type { FeedEvent } from "./feed";
import { liveResult, liveResultTail, progress, queryError, windowRevision } from "./frames";
import { rustDebug } from "./json";
import type { QueryModule } from "./module";
import { byteOrder, LiveQueryError, projectionTables, relationTables, validatePlan, type LiveQueryPlan } from "./plan";
import type { LogChange, QueryVisibility, ResolvedVisibility, VisibilityPlan } from "./visibility";

/** `query.subscribeMany` limit (`lib.rs:3155-3169`). */
export const SUBSCRIBE_MANY_LIMIT = 256;
export const SUBSCRIBE_MANY_ERROR = "query batch cannot contain more than 256 subscribes";

export interface SubscribeRequest {
  readonly id: string;
  readonly path: string;
  readonly args: unknown;
  readonly scope?: string;
  readonly windowRevision?: string;
}

/** Runs `body` in one read-only snapshot (a K01 Query invocation). The tenant object serializes these with its other invocations. */
export type SnapshotRunner = <T>(body: (db: StorageSession) => T) => Promise<T>;

/** The runner over K01: one `TenantStorage.invoke("query", …)`; a failure rejects with the body's error. */
export function snapshotRunner(storage: TenantStorage): SnapshotRunner {
  return async <T>(body: (db: StorageSession) => T): Promise<T> => {
    const result = await storage.invoke("query", async (db) => body(db));
    if (!result.ok) throw result.error;
    return result.value;
  };
}

export interface LiveQueryOptions {
  readonly visibility: QueryVisibility;
  /** The project's active module, or null when none is loaded. */
  readonly module: (projectId: string) => QueryModule | null;
  readonly run: SnapshotRunner;
}

interface Subscription {
  readonly id: string;
  readonly path: string;
  readonly args: unknown;
  readonly argsKey: string;
  readonly plan: LiveQueryPlan;
  readonly visibilityPlan: VisibilityPlan;
  readonly relatedPlans: ReadonlyMap<string, VisibilityPlan>;
  /** Tables whose writes can change the result. */
  readonly tables: ReadonlySet<string>;
  /** Tables read without a mask (index projection, search documents): always relevant. */
  readonly projection: ReadonlySet<string>;
  resolved: ResolvedVisibility;
  related: Map<string, ResolvedVisibility>;
  requiredRevision: number;
  computedRevision: number;
  epoch: string;
  lastText: string;
}

interface Connection {
  session: TenantSession;
  readonly subscriptions: Map<string, Subscription>;
  /** A subscription opened since delivery last asked (`opening_cohort_dirty`): a bootstrap watermark is due. */
  openingCohort: boolean;
}

export interface FeedStats {
  /** Subscriptions whose plan reran. */
  reruns: number;
  /** Plan executions (reruns sharing one execution count once). */
  executions: number;
  /** Subscriptions certified without a rerun. */
  skipped: number;
  statements: number;
  rowsRead: number;
}

/** Stable JSON of the arguments (sorted keys) for sharing executions. */
function argsKey(args: unknown): string {
  const sort = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(sort);
    if (value && typeof value === "object") {
      return Object.fromEntries(Object.keys(value).sort(byteOrder).map((key) => [key, sort((value as Record<string, unknown>)[key])]));
    }
    return value;
  };
  return JSON.stringify(sort(args)) ?? "null";
}

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

export class LiveQueries {
  private readonly connections = new Map<string, Connection>();

  constructor(private readonly options: LiveQueryOptions) {}

  // ---- frames --------------------------------------------------------------------

  /** `query.subscribe` from a socket; `tenant` is null without a tenant session. Control-scope frames are K06's and never reach here. */
  async subscribe(connectionId: string, tenant: TenantSession | null, request: SubscribeRequest): Promise<string> {
    if (!tenant) return queryError(request.id, request.path, UNAUTHENTICATED.subscribe);
    const [frame] = await this.options.run((db) => [this.open(db, connectionId, tenant, request)]);
    return frame!;
  }

  /** `query.subscribeMany`: one frame per entry, in order, in one snapshot. */
  async subscribeMany(connectionId: string, tenant: TenantSession | null, requests: readonly SubscribeRequest[]): Promise<string[]> {
    if (requests.length > SUBSCRIBE_MANY_LIMIT) return [queryError("", undefined, SUBSCRIBE_MANY_ERROR)];
    if (!tenant) return requests.map((request) => queryError(request.id, request.path, UNAUTHENTICATED.subscribe));
    if (!requests.length) return [];
    return this.options.run((db) => requests.map((request) => this.open(db, connectionId, tenant, request)));
  }

  /** `query.unsubscribe`: no reply. */
  unsubscribe(connectionId: string, id: string): void {
    this.connections.get(connectionId)?.subscriptions.delete(id);
  }

  // ---- socket lifecycle ----------------------------------------------------------

  /** Drops every live query of a socket: close, re-auth without `subscriptionsPreserved`, membership change, `system.reload`, feed lag. */
  clear(connectionId: string): void {
    this.connections.delete(connectionId);
  }

  /** A re-auth that preserved subscriptions: the same member under a new session record. */
  rebind(connectionId: string, tenant: TenantSession): void {
    const connection = this.connections.get(connectionId);
    if (connection) connection.session = tenant;
  }

  /** Computed revisions of a socket's live queries (input to the bootstrap and call watermarks). */
  computedRevisions(connectionId: string): number[] {
    return [...this.connections.get(connectionId)?.subscriptions.values() ?? []].map((s) => s.computedRevision);
  }

  /** Epochs of a socket's live queries (Rust closes a socket whose subscriptions span two epochs). */
  epochs(connectionId: string): string[] {
    return [...this.connections.get(connectionId)?.subscriptions.values() ?? []].map((s) => s.epoch);
  }

  /** Reads and clears the opening-cohort flag. */
  takeOpeningCohort(connectionId: string): boolean {
    const connection = this.connections.get(connectionId);
    if (!connection?.openingCohort) return false;
    connection.openingCohort = false;
    return true;
  }

  subscriptionIds(connectionId: string): string[] {
    return [...this.connections.get(connectionId)?.subscriptions.keys() ?? []].sort(byteOrder);
  }

  get size(): number {
    let n = 0;
    for (const connection of this.connections.values()) n += connection.subscriptions.size;
    return n;
  }

  // ---- open ------------------------------------------------------------------------

  /** `open_live_query` plus `resume_message`, inside one snapshot. Returns the reply frame. */
  private open(db: StorageSession, connectionId: string, tenant: TenantSession, request: SubscribeRequest): string {
    const { id, path } = request;
    let opened: { subscription: Subscription; snapshot: Snapshot };
    try {
      opened = this.prepare(db, tenant, request);
    } catch (error) {
      return queryError(id, path, errorText(error));
    }
    const { subscription, snapshot } = opened;
    let connection = this.connections.get(connectionId);
    if (!connection) {
      connection = { session: tenant, subscriptions: new Map(), openingCohort: false };
      this.connections.set(connectionId, connection);
    }
    connection.session = tenant;
    connection.subscriptions.set(id, subscription);
    connection.openingCohort = true;
    const revision = { epoch: snapshot.epoch, sequence: snapshot.revision };
    const window = windowRevision(snapshot.text);
    if (request.windowRevision !== undefined && request.windowRevision === window) return progress(id, path, revision);
    return liveResult(id, liveResultTail(path, snapshot.text, "initial", revision, window));
  }

  private prepare(db: StorageSession, tenant: TenantSession, request: SubscribeRequest): { subscription: Subscription; snapshot: Snapshot } {
    const { path } = request;
    if (tenant.anonymous) throw LiveQueryError.invalid(UNAUTHENTICATED.subscribe);
    const module = this.options.module(tenant.identity.projectId);
    if (!module) throw LiveQueryError.invalid("project module is not loaded");
    const fn = module.functions.get(path);
    if (!fn || fn.kind !== "query" || fn.delivery !== "live") throw LiveQueryError.invalid(`live query ${rustDebug(path)} is not registered`);
    if (fn.internal) throw LiveQueryError.invalid(`internal query ${rustDebug(path)} cannot be subscribed by a client`);
    const plan = fn.liveQueryPlan;
    if (!plan) throw LiveQueryError.invalid(`live query ${rustDebug(path)} requires a structured live query plan`);
    validatePlan(plan);
    const visibilityPlan = module.visibility.get(plan.table);
    if (!visibilityPlan) throw LiveQueryError.visibilityMissing(plan.table);
    const snapshot = this.execute(db, tenant, module, plan, visibilityPlan, request.args);
    const relatedPlans = new Map<string, VisibilityPlan>();
    for (const table of relationTables(plan.where)) {
      const related = module.visibility.get(table);
      if (related) relatedPlans.set(table, related);
    }
    const projection = projectionTables(plan);
    const tables = new Set<string>([plan.table, ...relatedPlans.keys(), ...projection]);
    for (const vplan of [visibilityPlan, ...relatedPlans.values()]) {
      tables.add(vplan.table);
      for (const table of this.options.visibility.dependencyTables(vplan, module.visibility)) tables.add(table);
    }
    return {
      snapshot,
      subscription: {
        id: request.id,
        path,
        args: request.args,
        argsKey: argsKey(request.args),
        plan,
        visibilityPlan,
        relatedPlans,
        tables,
        projection,
        resolved: snapshot.resolved,
        related: new Map(snapshot.related),
        requiredRevision: snapshot.revision,
        computedRevision: snapshot.revision,
        epoch: snapshot.epoch,
        lastText: snapshot.text,
      },
    };
  }

  private execute(db: StorageSession, session: TenantSession, module: QueryModule, plan: LiveQueryPlan, visibilityPlan: VisibilityPlan, args: unknown): Snapshot {
    return executePlan({ db, session, module, visibility: this.options.visibility }, plan, visibilityPlan, args);
  }

  // ---- feed ---------------------------------------------------------------------------

  /** Whether `change` can alter what `subscription` returns. */
  private relevant(subscription: Subscription, change: LogChange): boolean {
    if (!subscription.tables.has(change.table)) return false;
    const concerns = this.options.visibility.changeConcerns;
    if (!concerns || subscription.visibilityPlan.version !== 2 || subscription.projection.has(change.table)) return true;
    if (concerns.call(this.options.visibility, subscription.visibilityPlan, subscription.resolved, change)) return true;
    for (const [table, plan] of subscription.relatedPlans) {
      if (plan.version !== 2) {
        if (change.table === table || this.options.visibility.dependencyTables(plan, new Map()).has(change.table)) return true;
        continue;
      }
      const resolved = subscription.related.get(table);
      if (!resolved || concerns.call(this.options.visibility, plan, resolved, change)) return true;
    }
    return false;
  }

  /**
   * `apply_live_query_feed_event` for every socket: reruns what the commit
   * can affect, in one snapshot, and returns each socket's frames in
   * subscription-id order. Sockets without frames are absent.
   */
  async applyFeed(event: FeedEvent, stats?: FeedStats): Promise<Map<string, string[]>> {
    const reset = event.kind === "reset";
    const revision = event.kind === "transaction" ? event.revision : 0;
    const changes = event.kind === "transaction" ? event.changes : [];
    const due: Array<{ connection: Connection; connectionId: string; subscription: Subscription }> = [];
    for (const [connectionId, connection] of this.connections) {
      for (const id of [...connection.subscriptions.keys()].sort(byteOrder)) {
        const subscription = connection.subscriptions.get(id)!;
        if (!reset && revision <= subscription.computedRevision) continue;
        if (!reset && !changes.some((change) => this.relevant(subscription, change))) {
          // Certify the unchanged result, but never past a rerun that failed to cover a relevant revision.
          if (subscription.computedRevision >= subscription.requiredRevision) subscription.computedRevision = revision;
          if (stats) stats.skipped++;
          continue;
        }
        if (!reset) subscription.requiredRevision = Math.max(subscription.requiredRevision, revision);
        due.push({ connection, connectionId, subscription });
      }
    }
    const out = new Map<string, string[]>();
    if (!due.length) return out;
    await this.options.run((db) => {
      // One execution per (account, path, args) in this snapshot; one encoding per reason.
      const shared = new Map<string, { snapshot?: Snapshot; error?: string; tail?: string }>();
      for (const { connection, connectionId, subscription } of due) {
        const session = connection.session;
        const key = `${session.identity.account.id}\u0000${subscription.path}\u0000${subscription.argsKey}`;
        let entry = shared.get(key);
        if (!entry) {
          entry = {};
          try {
            const module = this.options.module(session.identity.projectId);
            if (!module) throw LiveQueryError.invalid("project module is not loaded");
            entry.snapshot = this.execute(db, session, module, subscription.plan, subscription.visibilityPlan, subscription.args);
            if (stats) { stats.executions++; stats.statements += entry.snapshot.stats.statements; stats.rowsRead += entry.snapshot.stats.rowsRead; }
          } catch (error) {
            entry.error = errorText(error);
          }
          shared.set(key, entry);
        }
        if (stats) stats.reruns++;
        const frames = out.get(connectionId) ?? [];
        if (entry.error !== undefined) {
          frames.push(queryError(subscription.id, subscription.path, entry.error));
          out.set(connectionId, frames);
          continue;
        }
        const snapshot = entry.snapshot!;
        if (snapshot.revision < subscription.requiredRevision) continue;
        const changed = snapshot.text !== subscription.lastText || snapshot.epoch !== subscription.epoch;
        subscription.resolved = snapshot.resolved;
        subscription.related = new Map(snapshot.related);
        subscription.computedRevision = snapshot.revision;
        subscription.epoch = snapshot.epoch;
        if (!changed) continue;
        subscription.lastText = snapshot.text;
        entry.tail ??= liveResultTail(subscription.path, snapshot.text, reset ? "reconnect" : "change", { epoch: snapshot.epoch, sequence: snapshot.revision });
        frames.push(liveResult(subscription.id, entry.tail));
        out.set(connectionId, frames);
      }
    });
    return out;
  }
}
