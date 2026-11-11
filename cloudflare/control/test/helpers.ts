/** Test helpers: the test worker's seeding routes and a frame-level socket client. */
import { exports } from "cloudflare:workers";

export const ADMIN = "test-admin-key";
export const PASSWORD = "control-password-1";
const BASE = "https://control.gonvex.test";

export function worker(path: string, init?: RequestInit): Promise<Response> {
  return exports.default.fetch(new Request(`${BASE}${path}`, init));
}

export async function dev<T = Record<string, unknown>>(method: string, path: string, body?: unknown): Promise<T> {
  const response = await worker(path, {
    method,
    headers: { authorization: `Bearer ${ADMIN}`, ...(body ? { "content-type": "application/json" } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const json = (await response.json()) as T;
  if (!response.ok) throw new Error(`${method} ${path} -> ${response.status}: ${JSON.stringify(json)}`);
  return json;
}

let counter = 0;
export const unique = (prefix: string): string => `${prefix}-${Date.now().toString(36)}-${(counter++).toString(36)}`;

export interface Seeded {
  project: string;
  tenants: Record<string, string>;
  members: Record<string, { email: string; accountId: string; memberIds: Record<string, string> }>;
}

/** A project with a deployed manifest, tenants and password Members (as the conformance runner seeds). */
export async function seed(tenants: Record<string, string[]>, { projectId, manifest }: { projectId?: string; manifest?: Record<string, unknown> } = {}): Promise<Seeded> {
  const created = await dev<{ project: { id: string } }>("POST", "/dev/projects", { name: "test", databaseMode: "multiTenant", ...(projectId ? { id: projectId } : {}) });
  const project = created.project.id;
  await dev("POST", "/dev/sync", { project, module: { hash: "artifact-1" }, functions: [], ...(manifest ?? {}) });
  const out: Seeded = { project, tenants: {}, members: {} };
  for (const [name, members] of Object.entries(tenants)) {
    const id = unique(`tenant-${name}`);
    await dev("POST", "/dev/tenants", { projectId: project, id, name: `Tenant ${name.toUpperCase()}` });
    out.tenants[name] = id;
    for (const member of members) {
      const email = `${member}-${project.slice(0, 8)}@control.test`;
      const r = await dev<{ accountId: string; memberId: string }>("POST", "/dev/internal/e2e/members", { projectId: project, tenantId: id, email, password: PASSWORD, name: member });
      out.members[member] ??= { email, accountId: r.accountId, memberIds: {} };
      out.members[member]!.memberIds[name] = r.memberId;
    }
  }
  return out;
}

type Frame = Record<string, unknown>;

/** A WebSocket to the control socket owner with an inbox. */
export class Client {
  private inbox: Frame[] = [];
  private waiters: Array<() => void> = [];
  closed: { code: number; reason: string } | null = null;
  private constructor(private readonly socket: WebSocket) {
    socket.addEventListener("message", (event) => {
      this.inbox.push(JSON.parse(String(event.data)) as Frame);
      this.wake();
    });
    socket.addEventListener("close", (event) => {
      this.closed = { code: event.code, reason: event.reason };
      this.wake();
    });
  }

  static async open(): Promise<Client> {
    const response = await worker("/ws", { headers: { upgrade: "websocket" } });
    const socket = response.webSocket;
    if (!socket) throw new Error(`expected a WebSocket, got ${response.status}`);
    socket.accept();
    return new Client(socket);
  }

  private wake(): void {
    for (const waiter of this.waiters.splice(0)) waiter();
  }

  send(frame: Frame): void {
    this.socket.send(JSON.stringify(frame));
  }

  async next(match: (frame: Frame) => boolean, timeoutMs = 5000): Promise<Frame> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const index = this.inbox.findIndex(match);
      if (index >= 0) return this.inbox.splice(index, 1)[0]!;
      if (this.closed) throw new Error(`socket closed (${this.closed.code} ${this.closed.reason})`);
      const left = deadline - Date.now();
      if (left <= 0) throw new Error("timed out waiting for a frame");
      await Promise.race([new Promise<void>((resolve) => this.waiters.push(resolve)), new Promise((resolve) => setTimeout(resolve, left))]);
    }
  }

  async waitClosed(timeoutMs = 5000): Promise<{ code: number; reason: string }> {
    const deadline = Date.now() + timeoutMs;
    while (!this.closed) {
      if (Date.now() > deadline) throw new Error("socket did not close");
      await new Promise<void>((resolve) => {
        this.waiters.push(resolve);
        setTimeout(resolve, 50);
      });
    }
    return this.closed;
  }

  async auth(fields: Frame): Promise<Frame> {
    const id = crypto.randomUUID();
    this.send({ type: "auth", id, clientContract: 1, ...fields });
    return this.next((f) => (f.type === "auth.result" || f.type === "auth.error") && f.id === id);
  }

  async call(type: "query.call" | "reducer.call" | "action.call", path: string, args: unknown = {}, key?: string | null): Promise<Frame> {
    const id = crypto.randomUUID();
    const frame: Frame = { type, id, path, args, scope: "control" };
    if (type !== "query.call" && key !== null) frame.idempotencyKey = key ?? id;
    this.send(frame);
    const kind = type.split(".")[0];
    return this.next((f) => (f.type === `${kind}.result` || f.type === `${kind}.error`) && f.id === id);
  }

  close(): void {
    try {
      this.socket.close(1000, "done");
    } catch {
      /* closed */
    }
  }
}

/** Logs in on a fresh control-only socket; returns the socket and the session. */
export async function login(project: string, email: string, password = PASSWORD): Promise<{ client: Client; session: Record<string, unknown> }> {
  const client = await Client.open();
  await client.auth({ project, controlOnly: true });
  const reply = await client.call("action.call", "control.auth.passwordLogin", { email, password });
  if (reply.type !== "action.result") throw new Error(`login failed: ${JSON.stringify(reply)}`);
  return { client, session: reply.result as Record<string, unknown> };
}
