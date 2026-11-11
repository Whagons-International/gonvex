/**
 * Operator sockets on the control object, with the hibernation API.
 *
 * - `/dev/dashboard/ws` (rust/crates/runtime/src/dashboard.rs): `session.ready`,
 *   `auth` → `auth.result{system:"dashboard"}` / `auth.error`,
 *   `query.subscribe{path:"dashboard.read",args:{resource,project?,tenant?,projectKey?}}`
 *   → `query.result{reason:"initial"|"change"}` / `query.error`, `query.unsubscribe`.
 *   Each read runs the operator route in-process with the socket's token, so
 *   authorization is the route's own. It re-reads after any successful /dev
 *   write, after a tenant reports a data change, and every 30 s (3 s for logs
 *   and errors); a result is sent only when it changed.
 * - `/dev/metrics/stream`: the token in `Sec-WebSocket-Protocol:
 *   gonvex-dashboard-auth.<token>` (echoed), `{type:"metrics",metrics}` every
 *   2 s, re-authorized every tick.
 * - `/dev/logs/stream`: `?key=` is the admin key or the project key;
 *   `{type:"ready"}`, a replay of the last 200 rows, then new rows every 500 ms.
 *
 * Per-socket state lives in attachments (≤ 16 KiB, 0016 G0-9), previous
 * results as a SHA-256 of their JSON. The ticks run on the object's alarm.
 */
import { createHash } from "node:crypto";
import { num, str } from "../src/sql";
import { bearer, type OperatorAuth } from "./auth";
import { error, json } from "./http";
import { handleOperatorRoute, logJson, metricsSnapshot, type Op } from "./routes";

export const DASHBOARD_TAG = "k19-dashboard";
export const METRICS_TAG = "k19-metrics";
export const LOGS_TAG = "k19-logs";

const MAX_SUBSCRIPTIONS = 64;
const MAX_MESSAGE_BYTES = 64 * 1024;
const MAX_RESULT_BYTES = 8 * 1024 * 1024;
const RECONCILE_MS = 30_000;
const TELEMETRY_RECONCILE_MS = 3_000;
const METRICS_MS = 2_000;
const LOGS_MS = 500;

interface Read {
  resource: string;
  project: string;
  tenant: string;
  projectKey: string;
}

interface Subscription {
  read: Read;
  /** SHA-256 of the last result sent, or "" before the first. */
  hash: string;
  next: number;
}

interface DashboardState {
  kind: "dashboard";
  token: string;
  subs: Record<string, Subscription>;
}

interface MetricsState {
  kind: "metrics";
  project: string;
  token: string;
  next: number;
}

interface LogsState {
  kind: "logs";
  project: string;
  lastTime: number | null;
  lastId: string;
}

type State = DashboardState | MetricsState | LogsState;

export interface SocketHost {
  readonly ctx: DurableObjectState;
  operatorContext(): Op;
  operatorAuth(): OperatorAuth;
  armAlarm(): Promise<void>;
  readonly operatorConfig: { adminKey: string | undefined; devSyncKey: string | undefined };
}

/** dashboard.rs `allowed_resource`: relative, read-only operator resources only. */
export function allowedResource(resource: string): boolean {
  if (resource.length > 16_384) return false;
  const path = resource.split("?")[0] ?? "";
  if (path.includes("%") || path.includes("..") || path.includes("\\") || resource.includes("#")) return false;
  if (["/dev/projects", "/dev/tenants", "/dev/manifest", "/dev/data/tables", "/dev/storage/files", "/dev/auth/notifications", "/dev/auth/tokens", "/dev/logs", "/dev/errors/groups"].includes(path)) return true;
  const parts = path.split("/");
  if (parts.length === 5 && parts[0] === "" && parts[1] === "dev" && parts[2] === "projects" && parts[3] !== "" && parts[4] === "members") return true;
  if (parts.length === 6 && parts[0] === "" && parts[1] === "dev" && parts[2] === "data" && parts[3] === "tables" && parts[4] !== "" && parts[5] === "rows") return true;
  return false;
}

const hashOf = (value: string) => createHash("sha256").update(value).digest("hex");

/** serde `Read` with `deny_unknown_fields`: `resource` required, the rest strings. */
function parseRead(args: unknown): Read | undefined {
  if (!args || typeof args !== "object" || Array.isArray(args)) return undefined;
  const a = args as Record<string, unknown>;
  for (const key of Object.keys(a)) if (!["resource", "project", "tenant", "projectKey"].includes(key)) return undefined;
  if (typeof a.resource !== "string") return undefined;
  for (const key of ["project", "tenant", "projectKey"]) if (a[key] !== undefined && typeof a[key] !== "string") return undefined;
  return { resource: a.resource, project: (a.project as string) ?? "", tenant: (a.tenant as string) ?? "", projectKey: (a.projectKey as string) ?? "" };
}

export class OperatorSockets {
  constructor(private readonly host: SocketHost) {}

  private get state(): DurableObjectState {
    return this.host.ctx;
  }

  private attachment(ws: WebSocket): State | undefined {
    return (ws.deserializeAttachment() ?? undefined) as State | undefined;
  }

  private send(ws: WebSocket, value: unknown): void {
    try {
      ws.send(JSON.stringify(value));
    } catch {
      // A closed socket is dropped on its close event.
    }
  }

  // --------------------------------------------------------------- accept

  acceptDashboard(request: Request): Response {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("Connection header did not include 'upgrade'", { status: 400 });
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    this.state.acceptWebSocket(server, [DASHBOARD_TAG]);
    server.serializeAttachment({ kind: "dashboard", token: "", subs: {} } satisfies DashboardState);
    this.send(server, { type: "session.ready", capabilities: { protocolVersion: 2 } });
    return new Response(null, { status: 101, webSocket: client });
  }

  async acceptMetrics(request: Request, url: URL): Promise<Response> {
    // axum extracts WebSocketUpgrade before the handler body runs.
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("Connection header did not include 'upgrade'", { status: 400 });
    const project = (url.searchParams.get("project") ?? "").trim() || (request.headers.get("x-gonvex-project-id") ?? "").trim();
    if (project === "") return error(400, "project is required");
    const protocol = (request.headers.get("sec-websocket-protocol") ?? "")
      .split(",")
      .map((value) => value.trim())
      .find((value) => value.startsWith("gonvex-dashboard-auth."));
    const token = protocol ? protocol.slice("gonvex-dashboard-auth.".length) : "";
    try {
      await this.authorizeMetrics(token, project);
    } catch (cause) {
      const failure = cause as { status?: number; message?: string; extra?: Record<string, unknown> };
      return error(failure.status ?? 401, failure.message ?? "dashboard sign-in or personal access token is required", failure.extra ?? {});
    }
    const pair = new WebSocketPair();
    this.state.acceptWebSocket(pair[1], [METRICS_TAG]);
    pair[1].serializeAttachment({ kind: "metrics", project, token, next: Date.now() + METRICS_MS } satisfies MetricsState);
    await this.host.armAlarm();
    return new Response(null, { status: 101, webSocket: pair[0], headers: protocol ? { "sec-websocket-protocol": protocol } : {} });
  }

  private async authorizeMetrics(token: string, project: string): Promise<void> {
    const headers = new Headers(token ? { authorization: `Bearer ${token}` } : {});
    const auth = this.host.operatorAuth();
    const actor = await auth.authorize(headers, "projects:read");
    if (!auth.canAccessProject(actor, project)) throw Object.assign(new Error("project access is required"), { status: 403 });
  }

  async acceptLogs(request: Request, url: URL): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("Connection header did not include 'upgrade'", { status: 400 });
    const project = (url.searchParams.get("project") ?? "").trim();
    if (project === "") return error(400, "project id is required");
    const key = (url.searchParams.get("key") ?? "").trim();
    const admin = this.host.operatorConfig.adminKey ?? "";
    const auth = this.host.operatorAuth();
    const allowed = (admin !== "" && admin === key) || auth.projectAcceptsSyncKey(project, key, undefined);
    if (!allowed) return error(401, "invalid Gonvex sync key");
    const replay = !["0", "false", "no", "off"].includes((url.searchParams.get("replay") ?? "1").trim().toLowerCase());
    const pair = new WebSocketPair();
    const server = pair[1];
    this.state.acceptWebSocket(server, [LOGS_TAG]);
    this.send(server, { type: "ready" });
    const state: LogsState = { kind: "logs", project, lastTime: null, lastId: "" };
    const op = this.host.operatorContext();
    if (replay) {
      const rows = op.ctx.sql
        .all(
          `SELECT kind,path,outcome,error,client_duration_ms,reason,tenant_id,account_id,created_at,event_id
           FROM gonvex_performance_events WHERE project_id=$1 ORDER BY created_at DESC,event_id DESC LIMIT 200`,
          project,
        )
        .reverse();
      for (const row of rows) {
        this.send(server, { type: "log", log: logJson(project, row) });
        state.lastTime = num(row.created_at);
        state.lastId = str(row.event_id);
      }
    } else {
      // Rust starts the cursor at the first poll, which reads nothing: only rows from now on.
      const latest = op.ctx.sql.get("SELECT created_at,event_id FROM gonvex_performance_events WHERE project_id=$1 ORDER BY created_at DESC,event_id DESC LIMIT 1", project);
      if (latest) {
        state.lastTime = num(latest.created_at);
        state.lastId = str(latest.event_id);
      } else {
        state.lastTime = 0;
      }
    }
    server.serializeAttachment(state);
    await this.host.armAlarm();
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  // ------------------------------------------------------------- messages

  async message(ws: WebSocket, raw: string | ArrayBuffer): Promise<void> {
    const state = this.attachment(ws);
    if (!state || state.kind !== "dashboard") return; // metrics and logs sockets ignore client frames
    if (typeof raw !== "string") return; // dashboard.rs skips non-text frames
    if (new TextEncoder().encode(raw).length > MAX_MESSAGE_BYTES) {
      ws.close(1009, "message too big");
      return;
    }
    let frame: Record<string, unknown>;
    try {
      frame = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      ws.close(1000, "");
      return;
    }
    const id = typeof frame.id === "string" ? frame.id : "";
    switch (frame.type) {
      case "auth": {
        state.subs = {};
        state.token = "";
        const candidate = typeof frame.token === "string" ? frame.token : "";
        let accepted = false;
        try {
          await this.host.operatorAuth().authorize(new Headers(candidate ? { authorization: `Bearer ${candidate}` } : {}), "");
          accepted = true;
        } catch {
          accepted = false;
        }
        if (accepted) {
          state.token = candidate;
          ws.serializeAttachment(state);
          this.send(ws, { type: "auth.result", id, result: { system: "dashboard" } });
        } else {
          ws.serializeAttachment(state);
          this.send(ws, { type: "auth.error", id, error: "dashboard authentication required" });
        }
        return;
      }
      case "query.subscribe": {
        delete state.subs[id];
        const read = frame.path === "dashboard.read" ? parseRead(frame.args) : undefined;
        if (!read || !allowedResource(read.resource) || Object.keys(state.subs).length >= MAX_SUBSCRIPTIONS) {
          ws.serializeAttachment(state);
          this.send(ws, { type: "query.error", id, error: "unsupported dashboard subscription or subscription limit reached" });
          return;
        }
        const subscription: Subscription = { read, hash: "", next: Date.now() + this.reconcileMs(read) };
        state.subs[id] = subscription;
        ws.serializeAttachment(state);
        await this.runRead(ws, state, id, subscription);
        await this.host.armAlarm();
        return;
      }
      case "query.unsubscribe": {
        delete state.subs[id];
        ws.serializeAttachment(state);
        return;
      }
      default:
        ws.close(1000, "");
    }
  }

  private reconcileMs(read: Read): number {
    return read.resource.startsWith("/dev/logs") || read.resource.startsWith("/dev/errors/") ? TELEMETRY_RECONCILE_MS : RECONCILE_MS;
  }

  /** dashboard.rs `snapshot`: the route in-process, with the socket's credentials. */
  private async snapshot(token: string, read: Read): Promise<{ ok: true; value: unknown; text: string } | { ok: false; error: string }> {
    const headers = new Headers();
    if (read.project) headers.set("x-gonvex-project-id", read.project);
    if (read.tenant) headers.set("x-gonvex-tenant-id", read.tenant);
    if (read.projectKey) headers.set("x-gonvex-key", read.projectKey);
    if (token) headers.set("authorization", `Bearer ${token}`);
    let response: Response;
    try {
      response = await handleOperatorRoute(this.host.operatorContext(), new Request(`http://dashboard.internal${read.resource}`, { method: "GET", headers }));
    } catch (cause) {
      return { ok: false, error: (cause as Error).message };
    }
    const text = await response.text();
    if (text.length > MAX_RESULT_BYTES) return { ok: false, error: "dashboard result exceeds limit" };
    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch {
      return { ok: false, error: "invalid dashboard response" };
    }
    if (response.status < 200 || response.status >= 300) {
      const message = value && typeof value === "object" && typeof (value as { error?: unknown }).error === "string" ? (value as { error: string }).error : "dashboard read denied";
      return { ok: false, error: message };
    }
    return { ok: true, value, text };
  }

  private async runRead(ws: WebSocket, state: DashboardState, id: string, subscription: Subscription): Promise<void> {
    const result = await this.snapshot(state.token, subscription.read);
    // The socket may have re-authenticated or unsubscribed meanwhile.
    const current = this.attachment(ws) as DashboardState | undefined;
    if (!current || current.kind !== "dashboard" || current.subs[id]?.read.resource !== subscription.read.resource || current.token !== state.token) return;
    if (!result.ok) {
      // Rust ends the watch after an error.
      delete current.subs[id];
      ws.serializeAttachment(current);
      this.send(ws, { type: "query.error", id, error: result.error });
      return;
    }
    const hash = hashOf(result.text);
    const sub = current.subs[id]!;
    sub.next = Date.now() + this.reconcileMs(sub.read);
    if (sub.hash !== hash) {
      const reason = sub.hash === "" ? "initial" : "change";
      sub.hash = hash;
      ws.serializeAttachment(current);
      this.send(ws, { type: "query.result", id, result: result.value, reason });
    } else {
      ws.serializeAttachment(current);
    }
  }

  /** Re-runs dashboard reads after a /dev write (all of them) or a tenant data change (matching data reads). */
  async refresh(trigger: { reason: "write" } | { reason: "data"; projectId: string; tenantId: string }): Promise<void> {
    for (const ws of this.state.getWebSockets(DASHBOARD_TAG)) {
      const state = this.attachment(ws);
      if (!state || state.kind !== "dashboard") continue;
      for (const [id, subscription] of Object.entries(state.subs)) {
        if (trigger.reason === "data") {
          const read = subscription.read;
          const tenant = read.tenant === "" ? read.project : read.tenant;
          if (!read.resource.startsWith("/dev/data/") || read.project !== trigger.projectId || tenant !== trigger.tenantId) continue;
        }
        await this.runRead(ws, this.attachment(ws) as DashboardState, id, subscription);
      }
    }
  }

  // ---------------------------------------------------------------- ticks

  nextDue(): number | undefined {
    let due: number | undefined;
    const at = (time: number) => {
      due = due === undefined ? time : Math.min(due, time);
    };
    for (const ws of this.state.getWebSockets()) {
      const state = this.attachment(ws);
      if (!state) continue;
      if (state.kind === "logs") at(Date.now() + LOGS_MS);
      else if (state.kind === "metrics") at(state.next);
      else for (const sub of Object.values(state.subs)) at(sub.next);
    }
    return due;
  }

  async tick(now: number): Promise<void> {
    const op = this.host.operatorContext();
    for (const ws of this.state.getWebSockets()) {
      const state = this.attachment(ws);
      if (!state) continue;
      if (state.kind === "logs") this.pollLogs(ws, state, op);
      else if (state.kind === "metrics" && state.next <= now) await this.pushMetrics(ws, state, op);
      else if (state.kind === "dashboard") {
        for (const [id, sub] of Object.entries(state.subs)) if (sub.next <= now) await this.runRead(ws, this.attachment(ws) as DashboardState, id, sub);
      }
    }
  }

  private pollLogs(ws: WebSocket, state: LogsState, op: Op): void {
    if (state.lastTime === null) return;
    const rows = op.ctx.sql.all(
      `SELECT kind,path,outcome,error,client_duration_ms,reason,tenant_id,account_id,created_at,event_id
       FROM gonvex_performance_events WHERE project_id=$1
         AND (created_at>$2 OR (created_at=$2 AND event_id>$3))
       ORDER BY created_at,event_id LIMIT 200`,
      state.project,
      state.lastTime,
      state.lastId,
    );
    if (rows.length === 0) return;
    for (const row of rows) {
      this.send(ws, { type: "log", log: logJson(state.project, row) });
      state.lastTime = num(row.created_at);
      state.lastId = str(row.event_id);
    }
    ws.serializeAttachment(state);
  }

  private async pushMetrics(ws: WebSocket, state: MetricsState, op: Op): Promise<void> {
    try {
      await this.authorizeMetrics(state.token, state.project);
    } catch {
      ws.close(1000, "");
      return;
    }
    this.send(ws, { type: "metrics", metrics: metricsSnapshot(op, state.project) });
    state.next = Date.now() + METRICS_MS;
    ws.serializeAttachment(state);
  }
}

export { bearer, json };
