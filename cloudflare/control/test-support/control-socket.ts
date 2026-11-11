/**
 * A minimal socket owner for the control object: one object per connection,
 * serving `auth` and `scope:"control"` frames exactly as the reference socket
 * loop does (lib.rs: session.ready 1774-1800, auth 2786-2995, control calls
 * 3002-3560, revalidation 2052-2092, auth deadline 2040-2045).
 *
 * It exists so the control object can be driven by the real SDK and diffed
 * against Rust before K05's gateway objects land. Tenant-scope frames get a
 * clear error. K05 replaces this with the gateway placement (0016 G0-1).
 */
import { DurableObject } from "cloudflare:workers";
import type { AuthOutcome } from "../src/auth";
import type { ControlObject } from "../src/control-object";
import type { ControlConnection, Outcome, ServiceGrant } from "../src/types";
import { emptyConnection } from "../src/types";

interface SocketEnv {
  CONTROL: DurableObjectNamespace<ControlObject>;
  GONVEX_RUNTIME_VERSION?: string;
  /** Test override for the 30 s revalidation interval. */
  GONVEX_TEST_REVALIDATION_MS?: string;
  /** Test override for the 10 s auth deadline. */
  GONVEX_TEST_AUTH_DEADLINE_MS?: string;
}

interface Subscription {
  id: string;
  path: string;
  args: unknown;
  last: string;
}

type Frame = Record<string, unknown> & { type?: unknown };

const SUPPORT_HEARTBEAT = "control.support.heartbeat";

/** Rust's `session.ready` capabilities, without the three of 0012 R20. */
const READY_CAPABILITIES = {
  replicaNoOldValues: 1,
  replicaSnapshotPages: 1,
  replicaResume: 1,
  queryResume: 1,
  preserveSubscriptions: 1,
  telemetryBatch: 1,
  protocolVersion: 2,
  replicaBatch: 1,
  replicaIntegrity: 1,
  queryBatch: 1,
  queryResultBatch: 1,
  reducerBatch: 1,
  replicaWatermark: 1,
  reducerReceiptLookup: 1,
  controlWatermark: 1,
  replicaBatchFrames: 1,
  presence: 1,
};

const sameJson = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);

export class ControlSocketObject extends DurableObject<SocketEnv> {
  private socket: WebSocket | null = null;
  private connection: ControlConnection = emptyConnection();
  private connectionId = "";
  private service: ServiceGrant | null = null;
  private pending = true;
  private tenantSession = false;
  private readonly subscriptions = new Map<string, Subscription>();
  private watching = "";
  private support: string | null = null;
  private queue: Promise<void> = Promise.resolve();
  private deadline: ReturnType<typeof setTimeout> | null = null;
  private revalidation: ReturnType<typeof setInterval> | null = null;

  private get control(): DurableObjectStub<ControlObject> {
    return this.env.CONTROL.getByName("control");
  }

  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("expected a WebSocket upgrade", { status: 426 });
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    server.accept();
    this.socket = server;
    this.connectionId = crypto.randomUUID();
    this.connection = { ...emptyConnection(), connectionId: this.connectionId };
    server.addEventListener("message", (event) => this.enqueue(() => this.onMessage(event.data)));
    server.addEventListener("close", () => this.enqueue(() => this.onClose()));
    server.addEventListener("error", () => this.enqueue(() => this.onClose()));
    this.send({ type: "session.ready", capabilities: { ...READY_CAPABILITIES, runtimeVersion: this.env.GONVEX_RUNTIME_VERSION || "development" } });
    const deadline = Number(this.env.GONVEX_TEST_AUTH_DEADLINE_MS) || 10_000;
    this.deadline = setTimeout(() => {
      if (this.pending) this.socket?.close(1008, "authentication deadline exceeded");
    }, deadline);
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Frames and events are handled one at a time, as the reference socket loop does. */
  private enqueue(task: () => Promise<void>): void {
    this.queue = this.queue.then(task).catch(() => {});
  }

  private send(message: unknown): void {
    try {
      this.socket?.send(JSON.stringify(message));
    } catch {
      /* closed */
    }
  }

  private async onClose(): Promise<void> {
    if (!this.socket) return;
    this.socket = null;
    if (this.deadline) clearTimeout(this.deadline);
    if (this.revalidation) clearInterval(this.revalidation);
    await this.setWatch("");
    await this.control.connectionState(this.connectionId, null);
  }

  private async setWatch(projectId: string): Promise<void> {
    const name = this.ctx.id.toString();
    if (this.watching && this.watching !== projectId) await this.control.unwatch(this.watching, "CONTROL_SOCKETS", name);
    if (projectId && this.watching !== projectId) await this.control.watch(projectId, "CONTROL_SOCKETS", name);
    this.watching = projectId;
  }

  private async reportState(): Promise<void> {
    const authenticated = this.connection.identity !== null || this.connection.tenant !== null;
    await this.control.connectionState(this.connectionId, {
      projectId: this.connection.projectId,
      tenantId: this.connection.tenant?.route.tenantId ?? "",
      authenticated,
      ...(this.support ? { supportSessionId: this.support } : {}),
    });
  }

  /** `RuntimeEvent::ControlChanged` for this socket's project. */
  async controlChanged(projectId: string): Promise<void> {
    this.enqueue(async () => {
      if (projectId === this.connection.projectId) for (const message of await this.refresh("control-change")) this.send(message);
    });
  }

  /** `refresh_control_queries` (control.rs:1978). */
  private async refresh(reason: string): Promise<unknown[]> {
    const messages: unknown[] = [];
    for (const subscription of this.subscriptions.values()) {
      if (reason === "presence-change" && subscription.path !== "control.support.listTenants") continue;
      const outcome = ((await this.control.query(this.connection, subscription.path, subscription.args)) as Outcome);
      if (outcome.ok) {
        const encoded = JSON.stringify(outcome.value);
        if (encoded !== subscription.last) {
          subscription.last = encoded;
          messages.push({ type: "query.result", id: subscription.id, path: subscription.path, result: outcome.value, reason });
        }
      } else {
        messages.push({ type: "query.error", id: subscription.id, path: subscription.path, error: outcome.error });
      }
    }
    return messages;
  }

  private async onMessage(data: unknown): Promise<void> {
    if (typeof data !== "string") return;
    let frame: Frame;
    try {
      frame = JSON.parse(data);
    } catch {
      this.socket?.close(1003, "invalid frame");
      return;
    }
    if (!frame || typeof frame !== "object") return;
    if (this.service && frame.type !== "auth") return this.serviceFrame(frame);
    switch (frame.type) {
      case "auth":
        return this.auth(frame);
      case "query.call":
        return this.queryCall(frame);
      case "query.subscribe":
        return this.subscribe(frame);
      case "query.subscribeMany":
        for (const item of (Array.isArray(frame.subscribes) ? frame.subscribes : []) as Frame[]) await this.subscribe(item);
        return;
      case "query.unsubscribe":
        this.subscriptions.delete(String(frame.id));
        return;
      case "reducer.call":
        return this.reducer(frame);
      case "reducer.callMany": {
        const calls = (Array.isArray(frame.calls) ? frame.calls : []) as Frame[];
        if (calls.length > 256) {
          this.send({ type: "reducer.error", id: "", error: "reducer batch cannot contain more than 256 calls", class: "rejected", retryable: false });
          return;
        }
        for (const call of calls) await this.reducer(call);
        return;
      }
      case "action.call":
        return this.action(frame);
      default:
        return;
    }
  }

  private async auth(frame: Frame): Promise<void> {
    const controlOnly = frame.controlOnly === true;
    const capabilities = (frame.capabilities ?? {}) as Record<string, unknown>;
    // `control_auth_tenant`: a control-only socket keeps its tenant only with tenantControl=1.
    let tenant = typeof frame.tenant === "string" ? frame.tenant : null;
    if (controlOnly) tenant = capabilities.tenantControl === 1 && tenant && tenant.trim() ? tenant : null;
    const outcome: AuthOutcome = await this.control.authenticate({
      id: String(frame.id ?? ""),
      token: typeof frame.token === "string" ? frame.token : null,
      project: typeof frame.project === "string" ? frame.project : null,
      tenant,
      controlOnly,
      public: frame.public === true,
      connectionId: this.connectionId,
    });
    this.service = outcome.service ?? null;
    if (this.service) this.pending = false;
    const next = { ...outcome.connection, connectionId: this.connectionId };
    if (next.identity || next.tenant) this.pending = false;
    const message = outcome.message as Record<string, unknown>;
    if (controlOnly && message.type === "auth.result") {
      const result = { ...(message.result as Record<string, unknown>) };
      delete result.replica;
      delete result.localIdentity;
      message.result = result;
    }
    const preserved =
      !controlOnly &&
      this.tenantSession &&
      capabilities.preserveSubscriptions === 1 &&
      this.connection.tenant !== null &&
      next.tenant !== null &&
      this.connection.tenant.identity.account.id !== "" &&
      this.connection.projectId === next.projectId &&
      sameJson(this.connection.identity, next.identity) &&
      this.connection.impersonationId === next.impersonationId &&
      sameJson(this.connection.tenant, next.tenant);
    if (preserved && message.type === "auth.result") message.result = { ...(message.result as object), subscriptionsPreserved: true };
    if (!preserved) this.subscriptions.clear();
    this.tenantSession = !controlOnly && next.tenant !== null;
    this.connection = next;
    this.support = null;
    await this.reportState();
    await this.setWatch(this.subscriptions.size ? next.projectId : "");
    if (this.revalidation) clearInterval(this.revalidation);
    this.revalidation = null;
    if (next.authToken) {
      const interval = Number(this.env.GONVEX_TEST_REVALIDATION_MS) || 30_000;
      this.revalidation = setInterval(() => this.enqueue(() => this.revalidate()), interval);
    }
    this.send(message);
  }

  /** The 30 s revalidation round for this socket (C.3.8 item 10). */
  private async revalidate(): Promise<void> {
    if (!this.connection.authToken) return;
    if (await this.control.revalidate(this.connection)) return;
    if (this.revalidation) clearInterval(this.revalidation);
    this.revalidation = null;
    this.subscriptions.clear();
    this.tenantSession = false;
    this.connection = { ...emptyConnection(), connectionId: this.connectionId };
    this.support = null;
    await this.reportState();
    await this.setWatch("");
    this.send({ type: "auth.error", code: "transient", id: "session-expired", error: "authentication expired or was revoked" });
  }

  private isControl(frame: Frame): boolean {
    return frame.scope === "control";
  }

  private async queryCall(frame: Frame): Promise<void> {
    const id = String(frame.id ?? "");
    const path = String(frame.path ?? "");
    if (!this.isControl(frame)) {
      this.send({ type: "query.error", id, path, error: this.tenantSession ? "tenant functions are not served by the control test front" : "authenticate with an active tenant before calling a Query" });
      return;
    }
    const outcome = ((await this.control.query(this.connection, path, frame.args ?? {})) as Outcome);
    this.send(outcome.ok ? { type: "query.result", id, path, result: outcome.value, reason: "initial" } : { type: "query.error", id, path, error: outcome.error });
  }

  private async subscribe(frame: Frame): Promise<void> {
    const id = String(frame.id ?? "");
    const path = String(frame.path ?? "");
    if (!this.isControl(frame)) {
      this.send({ type: "query.error", id, path, error: this.tenantSession ? "tenant functions are not served by the control test front" : "authenticate with an active tenant before subscribing to a Live Query" });
      return;
    }
    const check = ((await this.control.checkSubscribe(this.connection, path)) as Outcome);
    const outcome = check.ok ? ((await this.control.query(this.connection, path, frame.args ?? {})) as Outcome) : check;
    if (!outcome.ok) {
      this.send({ type: "query.error", id, path, error: outcome.error });
      return;
    }
    this.subscriptions.set(id, { id, path, args: frame.args ?? {}, last: JSON.stringify(outcome.value) });
    await this.setWatch(this.connection.projectId);
    this.send({ type: "query.result", id, path, result: outcome.value, reason: "initial" });
  }

  /** `ordered_control_completion`: refreshed subscriptions, the response, then `control.watermark`. */
  private async complete(response: Record<string, unknown>, succeeded: boolean, refresh = true): Promise<void> {
    if (!succeeded) {
      this.send(response);
      return;
    }
    const updates = refresh ? await this.refresh("control-change") : [];
    for (const update of updates) this.send(update);
    this.send(response);
    this.send({ type: "control.watermark", id: response.id });
  }

  private async reducer(frame: Frame): Promise<void> {
    const id = String(frame.id ?? "");
    const path = String(frame.path ?? "");
    if (!this.isControl(frame)) {
      this.send({
        type: "reducer.error",
        id,
        path,
        error: this.tenantSession ? "tenant functions are not served by the control test front" : "authenticate with an active tenant before calling a Reducer",
        class: this.tenantSession ? "rejected" : "unauthenticated",
        retryable: !this.tenantSession,
      });
      return;
    }
    // Support heartbeats with unchanged attribution are answered from memory (lib.rs:4899-4935).
    const attribution =
      path === SUPPORT_HEARTBEAT && this.connection.identity
        ? JSON.stringify([this.connection.projectId, this.connection.tenant?.route.tenantId ?? "", this.connection.identity.account.id, ...heartbeatFields(frame.args)])
        : null;
    if (attribution && this.supportAttribution === attribution && this.support) {
      await this.complete({ type: "reducer.result", id, path, result: { sessionId: this.support }, originCommandId: id }, true, false);
      return;
    }
    const outcome: Outcome = ((await this.control.write(this.connection, path, frame.args ?? {}, String(frame.idempotencyKey ?? ""), "reducer")) as Outcome);
    if (outcome.ok && attribution) {
      this.support = `support_${this.connectionId}`;
      this.supportAttribution = attribution;
      await this.reportState();
    }
    await this.complete(
      outcome.ok
        ? { type: "reducer.result", id, path, result: outcome.value, originCommandId: id }
        : { type: "reducer.error", id, path, error: outcome.error, class: outcome.errorClass, retryable: outcome.errorClass === "transient" || outcome.errorClass === "unauthenticated" },
      outcome.ok,
    );
  }

  private supportAttribution: string | null = null;

  private async action(frame: Frame): Promise<void> {
    const id = String(frame.id ?? "");
    const path = String(frame.path ?? "");
    if (!this.isControl(frame)) {
      this.send({ type: "action.error", id, path, error: this.tenantSession ? "tenant functions are not served by the control test front" : "authenticate with an active tenant before calling an Action" });
      return;
    }
    const outcome = ((await this.control.write(this.connection, path, frame.args ?? {}, String(frame.idempotencyKey ?? ""), "action")) as Outcome);
    await this.complete(outcome.ok ? { type: "action.result", id, path, result: outcome.value } : { type: "action.error", id, path, error: outcome.error }, outcome.ok);
  }

  /** `handle_service_frame` (service_principal.rs:359-470) for the control-scope part. */
  private async serviceFrame(frame: Frame): Promise<void> {
    const id = String(frame.id ?? "");
    const path = String(frame.path ?? "");
    const control = frame.scope === "control";
    switch (frame.type) {
      case "reducer.call": {
        const outcome: Outcome = control
          ? ((await this.control.serviceCall(this.service!, path, frame.args ?? {})) as Outcome)
          : this.service!.functions.includes(path)
            ? { ok: false, error: "tenant functions are not served by the control test front", errorClass: "rejected" }
            : { ok: false, error: `function ${JSON.stringify(path)} is not allowed for this service principal`, errorClass: "rejected" };
        this.send(outcome.ok ? { type: "reducer.result", id, path, result: outcome.value, originCommandId: id } : { type: "reducer.error", id, path, error: outcome.error });
        return;
      }
      case "action.call":
        this.send({ type: "action.error", id, path, error: control ? "service principals cannot call Control Plane Actions" : "tenant functions are not served by the control test front" });
        return;
      case "query.call":
        this.send({ type: "query.error", id, path, error: control ? "service principals cannot call Control Plane Queries" : "tenant functions are not served by the control test front" });
        return;
      case "query.subscribe":
        this.send({ type: "query.error", id, path, error: "service principals cannot subscribe" });
        return;
      case "replica.open":
        this.send({ type: "replica.error", id, path, error: "service principals cannot open replicas" });
        return;
      case "query.subscribeMany":
      case "replica.openMany":
      case "reducer.callMany":
        this.send({ type: "reducer.error", id: "", error: "batched frames are not available to service principals", class: "rejected", retryable: false });
        return;
      default:
        return;
    }
  }
}

function heartbeatFields(args: unknown): [string, string] {
  const object = args && typeof args === "object" ? (args as Record<string, unknown>) : {};
  const field = (key: string) => (typeof object[key] === "string" ? (object[key] as string).trim() : "");
  return [field("release"), field("environment")];
}
