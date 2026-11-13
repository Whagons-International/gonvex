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
import type { ControlConnection, Cost, Member, Outcome, ServiceGrant, TenantRoute } from "./types";
import type { PerformanceRow, PreparedErrorEvent } from "../../kernel/telemetry/envelope";
import { type ErrorSessionInput, TelemetryStore } from "../../kernel/telemetry/store";

export interface ControlEnv {
  /** Tenant objects, `idFromName(tenantId)`. */
  TENANT: DurableObjectNamespace;
  GONVEX_DASHBOARD_SESSION_SECRET?: string;
  GONVEX_SERVICE_PRINCIPALS?: string;
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

export class ControlObject extends DurableObject<ControlEnv> implements LiveConnections {
  readonly sql: Sql;
  readonly config: ControlConfig;
  private telemetryStore: TelemetryStore | undefined;
  private readonly connections = new Map<string, ConnectionState>();
  private readonly watchers = new Map<string, Map<string, { binding: string; name: string }>>();

  constructor(state: DurableObjectState, env: ControlEnv) {
    super(state, env);
    this.sql = new Sql(state.storage);
    this.config = configFromEnv(env);
    state.blockConcurrencyWhile(async () => {
      this.sql.script(schema);
      if ((await state.storage.getAlarm()) === null) await state.storage.setAlarm(Date.now() + PRUNE_EVERY_MS);
    });
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
    this.notify(input.projectId);
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
    if (inserted.some(Boolean)) this.notify(projectId);
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

  /** `id` is the watcher object's id string in the `binding` namespace. */
  async watch(projectId: string, binding: string, id: string): Promise<void> {
    const set = this.watchers.get(projectId) ?? new Map();
    set.set(`${binding}:${id}`, { binding, name: id });
    this.watchers.set(projectId, set);
  }

  async unwatch(projectId: string, binding: string, id: string): Promise<void> {
    this.watchers.get(projectId)?.delete(`${binding}:${id}`);
  }

  private notify(projectId: string): void {
    const set = this.watchers.get(projectId);
    if (!set) return;
    for (const { binding, name } of set.values()) {
      const namespace = this.env[binding] as DurableObjectNamespace | undefined;
      if (!namespace) continue;
      const stub = namespace.get(namespace.idFromString(name)) as unknown as ControlWatcher;
      this.ctx.waitUntil(stub.controlChanged(projectId).catch(() => this.watchers.get(projectId)?.delete(`${binding}:${name}`)));
    }
  }

  private notifyPresence(projectId: string): void {
    for (const { binding, name } of this.watchers.get(projectId)?.values() ?? []) {
      // K22's real gateway implements this additive hook; package fixture
      // socket owners retain their existing ControlChanged contract.
      if (binding !== "GATEWAY") continue;
      const namespace = this.env[binding] as DurableObjectNamespace | undefined;
      if (!namespace) continue;
      const stub = namespace.get(namespace.idFromString(name)) as unknown as Required<ControlWatcher>;
      this.ctx.waitUntil(stub.presenceChanged(projectId).catch((error) => console.error("control: presence refresh failed", error)));
    }
  }

  // --------------------------------------------------------------- HTTP

  override async fetch(request: Request): Promise<Response> {
    const response = await handleNativeAuth(this.context(), request);
    return response ?? Response.json({ error: "not found" }, { status: 404 });
  }

  // -------------------------------------------------------------- alarm

  /** Kernel: `gonvex_control_idempotency` rows expire after 24 h (0012 R9), pruned in pages. */
  override async alarm(): Promise<void> {
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
    await this.ctx.storage.setAlarm(Date.now() + PRUNE_EVERY_MS);
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
  async devE2eMember(input: { projectId: string; tenantId?: string; email: string; password: string; name?: string; accountOnly?: boolean }) {
    return dev.e2eMember(this.context(), input);
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
