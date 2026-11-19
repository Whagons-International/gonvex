/**
 * The control object: one SQLite-backed Durable Object holding the 36-table
 * Control Plane schema (rust/crates/store/src/control_schema.sql, unchanged).
 *
 * RPC surface, used by the object that owns a socket (K05's gateway), by
 * tenant objects (membership projection) and by the router:
 *
 *   authenticate          the `auth` frame (C.3.8)
 *   query / write         `control.*` calls (C.0, C.1)
 *   checkSubscribe        live Control Query admission
 *   serviceCall           service-principal Control Reducers (C.1 rows 61-62)
 *   validSessionHashes, validateImpersonation, activeMemberAccounts, memberById
 *                         the 30 s revalidation round (C.3.8 item 10)
 *   projectMemberships    membership projection from tenant objects (C.4 item 6)
 *   connectionState       live sockets per tenant and support presence
 *   watch / unwatch       ControlChanged notification of socket owners
 *   telemetry*            K15: error envelopes, error support sessions and performance events
 *                         (kernel/telemetry/store.ts); the hourly alarm applies their retention
 *   fetch                 native OAuth routes `/auth/*` (C.3.5)
 *
 * Every RPC returns plain JSON; errors come back as `Outcome` values because
 * thrown errors lose their class across RPC.
 */
import { setSpanTiming, spanTimingFromEnv } from "../../kernel/delivery/spans";
import { DurableObject } from "cloudflare:workers";
import schema from "../../../rust/crates/store/src/control_schema.sql";
import { type AuthOutcome, type AuthRequest, authenticate, parseServicePrincipals } from "./auth";
import type { ControlConfig, Ctx } from "./context";
import * as dev from "./dev";
import {
  activeMemberAccounts as activeMemberAccountsOf,
  authenticateSession,
  loadSessionIdentity,
  resolvePublicTenant,
  memberById as memberByIdOf,
  validSessionHashes as validSessionHashesOf,
  validateImpersonationSession,
} from "./directory";
import { asControlError } from "./errors";
import { type LiveConnections, checkSubscribable, executeControlQuery, executeControlWrite, projectMemberships as project, pruneIdempotency } from "./functions";
import { handleNativeAuth } from "./native-auth";
import { ServiceError, serviceControlCall } from "./service";
import { Sql } from "./sql";
import type { TenantPort } from "./tenant-port";
import type { ControlConnection, Cost, Member, Outcome, ServiceGrant, ServicePrincipalConfig, TenantRoute } from "./types";
import type { PerformanceRow, PreparedErrorEvent } from "../../kernel/telemetry/envelope";
import { type ErrorSessionInput, TelemetryStore } from "../../kernel/telemetry/store";

export interface ControlEnv {
  /** Tenant objects, `idFromName(tenantId)`. */
  TENANT: DurableObjectNamespace;
  GONVEX_DASHBOARD_SESSION_SECRET?: string;
  GONVEX_SERVICE_PRINCIPALS?: string;
  /** Dev/test only: enables fixture-only operator hooks (service-principal override). */
  GONVEX_ASSEMBLY_FIXTURE_KEY?: string;
  GONVEX_AUTH_URL?: string;
  GONVEX_PUBLIC_URL?: string;
  GONVEX_GOOGLE_CLIENT_ID?: string;
  GONVEX_GOOGLE_CLIENT_SECRET?: string;
  GONVEX_RUNTIME_VERSION?: string;
  GONVEX_ENV?: string;
  GONVEX_TEST_AUTH_EMULATOR?: string;
  FIREBASE_AUTH_EMULATOR_HOST?: string;
  GONVEX_TEST_GOOGLE_TOKEN_URL?: string;
  GONVEX_TEST_IDENTITY_TOOLKIT_URL?: string;
  [binding: string]: unknown;
}

/** Socket owners receive `controlChanged(projectId)` (RuntimeEvent::ControlChanged). */
export interface ControlWatcher {
  controlChanged(projectId: string): Promise<void>;
  presenceChanged?(projectId: string): Promise<void>;
}

export interface ConnectionState {
  projectId: string;
  tenantId: string;
  authenticated: boolean;
  /** `support_<connectionId>` once a support heartbeat registered it. */
  supportSessionId?: string;
  /** K15: the connection id once `error.register` registered it (`register_error_session`). */
  errorSessionId?: string;
}

const loopback = (host: string | undefined): boolean => {
  if (!host) return false;
  const name = host.replace(/:\d+$/, "").replace(/^\[|\]$/g, "");
  return name === "127.0.0.1" || name === "::1" || name === "localhost";
};

/** X03, dev/test only: conformance's `configureServicePrincipals` on a deployed Worker (no redeploy). */
export const FIXTURE_PRINCIPALS_KEY = "fixture/service-principals";
/** The environment's principals plus the fixture's; a fixture entry replaces an environment entry with its id. */
export function withFixturePrincipals(base: ServicePrincipalConfig[], fixture: string | undefined): ServicePrincipalConfig[] {
  if (!fixture) return base;
  const extra = parseServicePrincipals(fixture), ids = new Set(extra.map((p) => p.id));
  return [...base.filter((p) => !ids.has(p.id)), ...extra];
}

export function configFromEnv(env: ControlEnv): ControlConfig {
  const test = env.GONVEX_ENV === "test";
  return {
    ...(env.GONVEX_DASHBOARD_SESSION_SECRET ? { controlSecret: env.GONVEX_DASHBOARD_SESSION_SECRET } : {}),
    servicePrincipals: parseServicePrincipals(env.GONVEX_SERVICE_PRINCIPALS),
    ...((env.GONVEX_AUTH_URL || env.GONVEX_PUBLIC_URL) ? { authPublicUrl: (env.GONVEX_AUTH_URL || env.GONVEX_PUBLIC_URL)! } : {}),
    ...(env.GONVEX_GOOGLE_CLIENT_ID ? { googleClientId: env.GONVEX_GOOGLE_CLIENT_ID } : {}),
    ...(env.GONVEX_GOOGLE_CLIENT_SECRET ? { googleClientSecret: env.GONVEX_GOOGLE_CLIENT_SECRET } : {}),
    runtimeVersion: env.GONVEX_RUNTIME_VERSION || "development",
    ...(test && env.GONVEX_TEST_AUTH_EMULATOR === "true" && loopback(env.FIREBASE_AUTH_EMULATOR_HOST)
      ? { authEmulator: { host: env.FIREBASE_AUTH_EMULATOR_HOST! } }
      : {}),
    ...(test && env.GONVEX_TEST_GOOGLE_TOKEN_URL ? { googleTokenUrl: env.GONVEX_TEST_GOOGLE_TOKEN_URL } : {}),
    ...(test && env.GONVEX_TEST_IDENTITY_TOOLKIT_URL ? { identityToolkitUrl: env.GONVEX_TEST_IDENTITY_TOOLKIT_URL } : {}),
  };
}

const PRUNE_EVERY_MS = 60 * 60 * 1000;
/** The next hourly prune (shared with the operator Control, K19). */
const PRUNE_DUE_KEY = "k19/prune-due";
/** X17e: a watcher is unregistered only after this many failed notifications in a row (~85 s of backoff). */
export const NOTICE_ATTEMPTS = 6;
/** X17e: first retry delay of a failed notification; each further retry waits 4x longer. */
export const NOTICE_RETRY_MS = 250;
type NoticeKind = "control" | "presence";
/** `attempts`: consecutive failed control notices (they unregister the watcher); `presenceAttempts`: presence ones (never do). */
interface PendingNotice { binding: string; name: string; projects: Map<string, NoticeKind>; attempts: number; presenceAttempts?: number; dueAt: number }

export class ControlObject extends DurableObject<ControlEnv> implements LiveConnections {
  readonly sql: Sql;
  readonly config: ControlConfig;
  private telemetryStore: TelemetryStore | undefined;
  private readonly connections = new Map<string, ConnectionState>();
  private readonly watchers = new Map<string, Map<string, { binding: string; name: string }>>();

  constructor(state: DurableObjectState, env: ControlEnv) {
    super(state, env);
    // X13i/X14: opt-in time-only spans (GONVEX_SPAN_TIMING=1), e.g. the per-part sign-in timings (`control.auth`).
    if (spanTimingFromEnv(env)) setSpanTiming(true);
    this.sql = new Sql(state.storage);
    this.config = configFromEnv(env);
    if (env.GONVEX_ASSEMBLY_FIXTURE_KEY) this.config.servicePrincipals = withFixturePrincipals(this.config.servicePrincipals, state.storage.kv.get<string>(FIXTURE_PRINCIPALS_KEY));
    state.blockConcurrencyWhile(async () => {
      this.sql.script(schema);
      if ((await state.storage.getAlarm()) === null) await state.storage.setAlarm(Date.now() + PRUNE_EVERY_MS);
    });
  }

  /** X03 dev/test fixture: replace the fixture service principals (validated like the environment's) and apply them now. */
  async fixtureServicePrincipals(principals: string): Promise<{ count: number } | { error: string }> {
    if (!(this.env as ControlEnv).GONVEX_ASSEMBLY_FIXTURE_KEY) return { error: "assembly fixture control disabled" };
    try { parseServicePrincipals(principals); } catch (error) { return { error: (error as Error).message }; }
    this.ctx.storage.kv.put(FIXTURE_PRINCIPALS_KEY, principals);
    this.config.servicePrincipals = withFixturePrincipals(configFromEnv(this.env as ControlEnv).servicePrincipals, principals);
    return { count: this.config.servicePrincipals.length };
  }

  /** K15: error envelopes, performance events and their retention (created on first use). */
  get telemetry(): TelemetryStore {
    return (this.telemetryStore ??= new TelemetryStore(this.sql));
  }

  /** A fresh per-call context. */
  private context(): Ctx {
    const env = this.env;
    return {
      sql: this.sql,
      config: this.config,
      tenant: (route: TenantRoute) => env.TENANT.getByName(route.tenantId) as unknown as TenantPort,
      controlChanged: (projectId: string) => this.notify(projectId),
      fetch: (input, init) => fetch(input, init),
      tenantCalls: 0,
    };
  }

  /** Rows read and written and tenant RPCs of the last `query` / `write` (tests and the PR's numbers). */
  lastCost: Cost = { rowsRead: 0, rowsWritten: 0, tenantCalls: 0 };

  private async run<T>(fn: (ctx: Ctx) => Promise<T> | T): Promise<Outcome<T>> {
    const ctx = this.context();
    const before = this.sql.mark();
    try {
      return { ok: true, value: await fn(ctx) };
    } catch (error) {
      const failure = asControlError(error);
      return { ok: false, error: failure.message, errorClass: failure.errorClass };
    } finally {
      const after = this.sql.mark();
      this.lastCost = { rowsRead: after.read - before.read, rowsWritten: after.written - before.written, tenantCalls: ctx.tenantCalls };
    }
  }

  // --------------------------------------------------------------- auth

  async authenticate(request: AuthRequest): Promise<AuthOutcome> {
    return authenticate(this.context(), request);
  }

  // ------------------------------------------------------------ control.*

  async query(connection: ControlConnection, path: string, args: unknown): Promise<Outcome> {
    return this.run((ctx) => executeControlQuery(ctx, this, connection, path, args));
  }

  async write(connection: ControlConnection, path: string, args: unknown, idempotencyKey: string, kind: "reducer" | "action"): Promise<Outcome> {
    const outcome = await this.run((ctx) => executeControlWrite(ctx, this, connection, path, args, idempotencyKey, kind));
    // `notify_control_changed` after any successful Control write (lib.rs:4036).
    if (outcome.ok) this.notify(connection.projectId);
    return outcome;
  }

  async checkSubscribe(connection: ControlConnection, path: string): Promise<Outcome<null>> {
    return this.run(() => {
      checkSubscribable(connection, path);
      return null;
    });
  }

  async serviceCall(grant: ServiceGrant, path: string, args: unknown): Promise<Outcome> {
    try {
      return { ok: true, value: await serviceControlCall(this.context(), grant, path, args) };
    } catch (error) {
      const message = error instanceof ServiceError || error instanceof Error ? error.message : String(error);
      return { ok: false, error: message, errorClass: "rejected" };
    }
  }

  // --------------------------------------------------------- revalidation

  async validSessionHashes(projectId: string, tokenHashes: string[]): Promise<string[]> {
    return validSessionHashesOf(this.context(), projectId, tokenHashes);
  }

  async validateImpersonation(token: string, projectId: string, tenantId: string, connectionId: string): Promise<Outcome<null>> {
    return this.run(async (ctx) => {
      await validateImpersonationSession(ctx, token, projectId, tenantId, connectionId);
      return null;
    });
  }

  /** `revalidate_connection` (lib.rs:3938): one socket's 30 s check. */
  async revalidate(connection: ControlConnection): Promise<boolean> {
    const ctx = this.context();
    try {
      const session = connection.tenant;
      if (session && session.identity.account.id === "") {
        resolvePublicTenant(ctx, connection.projectId, session.route.tenantId);
        return true;
      }
      const project = connection.projectId || null;
      if (connection.impersonationId) {
        await validateImpersonationSession(ctx, connection.authToken, project ?? "", session?.route.tenantId ?? "", connection.connectionId);
      } else if (session) {
        await authenticateSession(ctx, connection.authToken, project, session.route.tenantId);
      } else {
        loadSessionIdentity(ctx, connection.authToken, project);
      }
      return true;
    } catch {
      return false;
    }
  }

  async activeMemberAccounts(projectId: string, tenantId: string, accountIds: string[]): Promise<Outcome<string[]>> {
    return this.run((ctx) => activeMemberAccountsOf(ctx, projectId, tenantId, accountIds));
  }

  async memberById(route: TenantRoute, memberId: string): Promise<Outcome<Member | null>> {
    return this.run((ctx) => memberByIdOf(ctx, route, memberId));
  }

  // ------------------------------------------------- membership projection

  /**
   * Object-to-object membership projection: a tenant object pushes its
   * outbox rows after commit; the revision-guarded upsert makes a retry safe.
   */
  async projectMemberships(projectId: string, tenantId: string, rows: Array<{ accountId: string; memberId: string; status: string; membershipRevision: number }>): Promise<number> {
    const applied = project(this.context(), tenantId, rows);
    if (applied > 0) this.notify(projectId);
    return applied;
  }

  // ------------------------------------------------- live connections

  async connectionState(connectionId: string, state: ConnectionState | null): Promise<void> {
    const before = this.connections.get(connectionId);
    // K15: a report from presence does not know the error session; keep it.
    if (state) this.connections.set(connectionId, before?.errorSessionId && !state.errorSessionId ? { ...state, errorSessionId: before.errorSessionId } : state);
    else {
      this.connections.delete(connectionId);
      // A closed connection's support sessions are last seen now (presence `disconnect` flush).
      for (const id of [before?.supportSessionId, before?.errorSessionId]) {
        if (!id) continue;
        this.sql.tx(() => this.sql.run("UPDATE gonvex_support_sessions SET last_seen_at=CAST(unixepoch('subsec')*1000 AS INTEGER) WHERE id=$1", id));
      }
    }
    if (before?.projectId !== state?.projectId || before?.tenantId !== state?.tenantId || before?.authenticated !== state?.authenticated) {
      for (const project of new Set([before?.projectId, state?.projectId])) if (project) this.notifyPresence(project);
    }
  }

  tenantCounts(projectId: string): Map<string, number> {
    const counts = new Map<string, number>();
    for (const state of this.connections.values()) {
      if (state.projectId === projectId && state.authenticated && state.tenantId) counts.set(state.tenantId, (counts.get(state.tenantId) ?? 0) + 1);
    }
    return counts;
  }

  supportLiveIds(projectId: string): Set<string> {
    const ids = new Set<string>();
    for (const state of this.connections.values()) {
      if (state.projectId !== projectId) continue;
      if (state.supportSessionId) ids.add(state.supportSessionId);
      if (state.errorSessionId) ids.add(state.errorSessionId);
    }
    return ids;
  }

  // ------------------------------------------------------------ telemetry (K15)

  /**
   * `register_error_session`'s upsert (the socket owner answers repeats from
   * memory). The session is live until the socket closes, then last seen.
   */
  async telemetryRegisterSession(input: ErrorSessionInput): Promise<boolean> {
    try {
      this.telemetry.registerErrorSession(input);
    } catch (error) {
      console.error("telemetry: error session upsert failed", error);
      return false;
    }
    // Live only while presence v2 is on (`support_register`); otherwise no close is ever reported for it.
    if (input.live === true) {
      const state = this.connections.get(input.connectionId);
      this.connections.set(input.connectionId, { ...(state ?? { projectId: input.projectId, tenantId: input.tenantId, authenticated: true }), errorSessionId: input.connectionId });
    }
    this.notifySupport(input.projectId);
    return true;
  }

  /** `persist_event` for one envelope's events; null when the store failed (`error store unavailable`). */
  async telemetryRecordErrors(projectId: string, events: PreparedErrorEvent[]): Promise<boolean[] | null> {
    let inserted: boolean[];
    try {
      inserted = this.telemetry.recordErrors(projectId, events);
    } catch (error) {
      console.error("telemetry: error events not stored", error);
      return null;
    }
    if (inserted.some(Boolean)) this.notifySupport(projectId);
    return inserted;
  }

  /** `capture_performance_event` rows (the bounded window the dashboard routes read). */
  async telemetryRecordPerformance(projectId: string, rows: PerformanceRow[]): Promise<number> {
    return this.telemetry.recordPerformance(projectId, rows);
  }

  /** Every live support session id, any project (the telemetry prune keeps them). */
  private liveSupportIds(): Set<string> {
    const ids = new Set<string>();
    for (const state of this.connections.values()) {
      if (state.supportSessionId) ids.add(state.supportSessionId);
      if (state.errorSessionId) ids.add(state.errorSessionId);
    }
    return ids;
  }

  protected watchersFor(projectId: string) {
    let set = this.watchers.get(projectId);
    if (!set) {
      const stored = this.ctx.storage.kv.get<Array<{binding:string;name:string}>>(`control/watchers/${projectId}`) ?? [];
      set = new Map(stored.map(w => [`${w.binding}:${w.name}`, w]));
      this.watchers.set(projectId, set);
    }
    return set;
  }

  /** `id` is the watcher object's id string in the `binding` namespace. */
  async watch(projectId: string, binding: string, id: string): Promise<void> {
    const set = this.watchersFor(projectId);
    if (set.has(`${binding}:${id}`)) return;
    set.set(`${binding}:${id}`, { binding, name: id });
    this.ctx.storage.kv.put(`control/watchers/${projectId}`, [...set.values()]);
  }

  async unwatch(projectId: string, binding: string, id: string): Promise<void> {
    const set = this.watchersFor(projectId); set.delete(`${binding}:${id}`);
    this.ctx.storage.kv.put(`control/watchers/${projectId}`, [...set.values()]);
  }

  /**
   * X17: gateways (watcher keys) whose sessions read `control.support.*` for a project. Only those queries read what
   * the error-session/telemetry writes and the live-connection counts change, so those notifications go to these
   * gateways only (and to every non-gateway watcher, as before). A gateway that receives a notification it has no use
   * for still pays for it: the notification is a peer request, so its next outgoing call waits for a fresh event (X02
   * CallOrigin, 30-70 ms on dev), and every sign-in `auth` changed the connection state, so it notified every gateway.
   * Kept in memory and in an unconfirmed KV copy; a gateway re-registers when its sessions run the query again.
   */
  private readonly supportInterest = new Map<string, Set<string>>();
  private supportInterestFor(projectId: string): Set<string> {
    let set = this.supportInterest.get(projectId);
    if (!set) {
      set = new Set(this.ctx.storage.kv.get<string[]>(`control/support-interest/${projectId}`) ?? []);
      this.supportInterest.set(projectId, set);
    }
    return set;
  }
  /** A session of gateway `host` (its object name) ran a `control.support.*` query of `projectId`. */
  protected noteSupportInterest(projectId: string, host: string): void {
    const namespace = this.env.GATEWAY as DurableObjectNamespace | undefined;
    if (!namespace || !projectId || !host) return;
    let key: string;
    try { key = `GATEWAY:${namespace.idFromName(host).toString()}`; } catch { return; }
    const set = this.supportInterestFor(projectId);
    if (set.has(key)) return;
    set.add(key);
    while (set.size > 256) set.delete(set.values().next().value!);
    void this.ctx.storage.put(`control/support-interest/${projectId}`, [...set], { allowUnconfirmed: true }).catch(() => {});
  }
  /** Watchers of `projectId` that need a support-scoped change: every non-gateway watcher, and gateways that read support queries. */
  private supportWatchers(projectId: string) {
    const interest = this.supportInterestFor(projectId);
    return [...this.watchersFor(projectId).values()].filter(({ binding, name }) => binding !== "GATEWAY" || interest.has(`${binding}:${name}`));
  }

  /** X17: a change only `control.support.*` queries can observe (error sessions, error events). */
  private notifySupport(projectId: string): void {
    for (const { binding, name } of this.supportWatchers(projectId)) this.enqueueNotice(binding, name, projectId, "control");
  }

  private notify(projectId: string): void {
    for (const { binding, name } of this.watchersFor(projectId).values()) this.enqueueNotice(binding, name, projectId, "control");
  }

  private notifyPresence(projectId: string): void {
    // X17: only `control.support.listTenants` reads the live counts this refreshes.
    // K22's real gateway implements this additive hook; package fixture socket owners keep the ControlChanged contract.
    for (const { binding, name } of this.supportWatchers(projectId)) if (binding === "GATEWAY") this.enqueueNotice(binding, name, projectId, "presence");
  }

  /**
   * X17e (#529 review): notifications leave Control only from its own alarm. An outgoing call counts one hop below
   * the object's NEWEST incoming request (kernel/session/placement/origin.ts), and the write that notifies can commit
   * while another gateway's refresh query is Control's newest request, so a notification sent in place had no depth
   * bound. Sent synchronously at the start of the alarm (a depth-0 platform event that is then the newest request),
   * every `controlChanged`/`presenceChanged` arrives at depth 1, which lets the gateway treat it as a bounded notice
   * and launch its refreshes at once (gateway call at 2, Control query at 3) instead of waiting for its own alarm.
   * Notices are coalesced per watcher and project; a failed send is retried with backoff instead of unregistering
   * the watcher, which is dropped only after `NOTICE_ATTEMPTS` failures in a row. The outbox is in memory: a Control
   * reset drops queued notices, as the old in-place sends were lost; a gateway refreshes all its sessions when its
   * alarm sees a new Control instance (`refreshControlWatches`).
   */
  private readonly outbox = new Map<string, PendingNotice>();
  private noticeAlarmAt: number | undefined;

  private enqueueNotice(binding: string, name: string, projectId: string, kind: NoticeKind): void {
    if (!this.env[binding]) return;
    const key = `${binding}:${name}`;
    let pending = this.outbox.get(key);
    if (!pending) this.outbox.set(key, pending = { binding, name, projects: new Map(), attempts: 0, dueAt: 0 });
    // A control change refreshes every Control query, presence ones included.
    if (pending.projects.get(projectId) !== "control") pending.projects.set(projectId, kind);
    pending.dueAt = Date.now();
    this.armNotices(pending.dueAt);
  }

  private armNotices(at: number): void {
    if (this.noticeAlarmAt !== undefined && this.noticeAlarmAt <= at) return;
    this.noticeAlarmAt = at;
    // Never later than a duty already due earlier (prune, operator socket ticks; #532 re-review): setAlarm replaces it.
    void this.ctx.storage.setAlarm(Math.min(at, this.nextAlarmAt()), { allowUnconfirmed: true }).catch((error) => console.error("control: notice alarm failed", error));
  }

  /** Sends every due notice now. Call only synchronously at the start of the alarm (see `outbox`). */
  private sendNotices(now: number): Promise<unknown> {
    const sends: Promise<unknown>[] = [];
    for (const [key, pending] of [...this.outbox]) {
      if (pending.dueAt > now) continue;
      this.outbox.delete(key);
      const namespace = this.env[pending.binding] as DurableObjectNamespace | undefined;
      if (!namespace) continue;
      let stub: Required<ControlWatcher>;
      try { stub = namespace.get(namespace.idFromString(pending.name)) as unknown as Required<ControlWatcher>; }
      catch (error) { console.error("control: watcher id is invalid", { watcher: key, error: String(error) }); continue; }
      for (const [projectId, kind] of pending.projects) {
        const call = kind === "presence" ? stub.presenceChanged(projectId) : stub.controlChanged(projectId);
        sends.push(Promise.resolve(call).catch((error) => this.noticeFailed(key, pending, projectId, kind, error)));
      }
    }
    return Promise.all(sends);
  }

  private noticeFailed(key: string, pending: PendingNotice, projectId: string, kind: NoticeKind, error: unknown): void {
    // #532 review: presence failures (e.g. a gateway build without `presenceChanged` during a rollout) are counted apart
    // and never unregister the watcher; only consecutive failed control notices do.
    const presence = kind === "presence";
    const attempts = (presence ? pending.presenceAttempts ?? 0 : pending.attempts) + 1;
    if (attempts >= NOTICE_ATTEMPTS) {
      if (presence) { console.error("control: presence notice failed; giving up", { watcher: key, attempts, error: String(error) }); return; }
      console.error("control: watcher dropped after failed notifications", { watcher: key, attempts, error: String(error) });
      this.watchers.get(projectId)?.delete(key);
      this.ctx.storage.kv.put(`control/watchers/${projectId}`, [...this.watchersFor(projectId).values()]);
      return;
    }
    console.error("control: notification failed; retrying", { watcher: key, kind, attempts, error: String(error) });
    let retry = this.outbox.get(key);
    if (!retry) this.outbox.set(key, retry = { binding: pending.binding, name: pending.name, projects: new Map(), attempts: 0, dueAt: 0 });
    if (retry.projects.get(projectId) !== "control") retry.projects.set(projectId, kind);
    if (presence) retry.presenceAttempts = Math.max(retry.presenceAttempts ?? 0, attempts);
    else retry.attempts = Math.max(retry.attempts, attempts);
    const at = Date.now() + NOTICE_RETRY_MS * 4 ** (attempts - 1);
    retry.dueAt = retry.dueAt ? Math.min(retry.dueAt, at) : at;
    this.armNotices(retry.dueAt);
  }

  /** The next prune or pending notice, whichever is first. Subclasses add their own deadlines. */
  protected nextAlarmAt(): number {
    let at = this.ctx.storage.kv.get<number>(PRUNE_DUE_KEY) ?? Date.now() + PRUNE_EVERY_MS;
    for (const pending of this.outbox.values()) at = Math.min(at, pending.dueAt);
    return at;
  }

  /**
   * Arms the object's single alarm for `nextAlarmAt`, never later than it already is (unless that time passed). The
   * deadline is read after the `getAlarm` wait, so a notice enqueued meanwhile is included or arms its own alarm later.
   */
  async armAlarm(): Promise<void> {
    const current = await this.ctx.storage.getAlarm();
    const next = this.nextAlarmAt();
    if (this.outbox.size) this.noticeAlarmAt = Math.min(next, current ?? next);
    if (current === null || current > next || current < Date.now()) await this.ctx.storage.setAlarm(next);
  }

  // --------------------------------------------------------------- HTTP

  override async fetch(request: Request): Promise<Response> {
    const response = await handleNativeAuth(this.context(), request);
    return response ?? Response.json({ error: "not found" }, { status: 404 });
  }

  // -------------------------------------------------------------- alarm

  /**
   * X17e: pending notices first, synchronously (see `outbox`), then `alarmWork`, then the next alarm. Subclasses
   * extend `alarmWork` and `nextAlarmAt`, not this method.
   */
  override async alarm(): Promise<void> {
    this.noticeAlarmAt = undefined;
    const now = Date.now();
    // Started now (depth 1), not awaited (#532 review): one slow watcher must not hold back the next batch, the prune
    // or the operator's socket ticks. A failed send arms its own retry (`noticeFailed`).
    this.ctx.waitUntil(this.sendNotices(now));
    try {
      await this.alarmWork(now);
    } finally {
      await this.armAlarm();
    }
  }

  /** Kernel: `gonvex_control_idempotency` rows expire after 24 h (0012 R9), pruned in pages, hourly. */
  protected async alarmWork(now: number): Promise<void> {
    const due = this.ctx.storage.kv.get<number>(PRUNE_DUE_KEY) ?? now;
    if (now < due) return;
    const ctx = this.context();
    for (let page = 0; page < 20; page++) {
      if (pruneIdempotency(ctx, 500) < 500) break;
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    // K15: telemetry retention and caps (contract-changes 0041).
    try {
      await this.telemetry.prune(this.liveSupportIds());
    } catch (error) {
      console.error("telemetry: prune failed", error);
    }
    this.ctx.storage.kv.put(PRUNE_DUE_KEY, now + PRUNE_EVERY_MS);
  }

  // ------------------------------------------------- seeding (tests only)

  async devCreateProject(input: { name: string; databaseMode?: string; ownerEmail?: string; authMode?: string; id?: string }) {
    return dev.createProject(this.context(), input);
  }
  async devSaveManifest(projectId: string, manifest: Record<string, unknown>) {
    return dev.saveManifest(this.context(), projectId, manifest);
  }
  async devCreateTenant(projectId: string, tenantId: string, name: string) {
    return dev.createTenant(this.context(), projectId, tenantId, name);
  }
  async devE2eMember(input: { projectId: string; tenantId?: string; email: string; password: string; name?: string; accountOnly?: boolean; role?: string; permissions?: Record<string, unknown> }) {
    return dev.e2eMember(this.context(), input);
  }
  async devE2eMemberRead(input: { projectId: string; tenantId: string; accountId?: string; email?: string; memberId?: string }) {
    return dev.e2eMemberRead(this.context(), input);
  }
  async devProjectMember(projectId: string, input: { email: string; name?: string; role?: string }) {
    return dev.upsertProjectMember(this.context(), projectId, input);
  }
  async devGoogleAuth(projectId: string, input: { redirectUri: string; signupMode?: string }) {
    return this.run((ctx) => dev.putGoogleAuth(ctx, projectId, input));
  }
  async devProjectKey(projectId: string): Promise<string | null> {
    return (this.sql.value<string>("SELECT project_key FROM gonvex_runtime_projects WHERE id=$1", projectId) as string | undefined) ?? null;
  }
}
