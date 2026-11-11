// A raw protocol client over SELF (the test Worker's /ws), for the specs.
import { SELF } from "cloudflare:test";

export const ADMIN = "k07-test-key";
type Frame = Record<string, any>;

export async function stub(method: "GET" | "POST", route: string, body?: unknown): Promise<any> {
  const response = await SELF.fetch(`http://k07.test/_stub/session/${route}`, {
    method, headers: { authorization: `Bearer ${ADMIN}`, ...(body ? { "content-type": "application/json" } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${method} ${route} -> ${response.status}: ${text}`);
  return JSON.parse(text);
}

export async function prune(tenant: string, now: number): Promise<any> {
  const response = await SELF.fetch("http://k07.test/_stub/reducer/prune", {
    method: "POST", headers: { authorization: `Bearer ${ADMIN}`, "content-type": "application/json" }, body: JSON.stringify({ tenant, now }),
  });
  return response.json();
}

export class Client {
  readonly frames: Frame[] = [];
  private waiters: Array<{ match: (f: Frame) => boolean; resolve: (f: Frame) => void }> = [];
  private seen = 0;
  closed = false;
  private constructor(private readonly socket: WebSocket) {
    socket.addEventListener("message", (event) => {
      const frame = JSON.parse(String(event.data)) as Frame;
      this.frames.push(frame);
      this.flush();
    });
    socket.addEventListener("close", () => { this.closed = true; });
  }

  static async connect(): Promise<Client> {
    const response = await SELF.fetch("http://k07.test/ws", { headers: { upgrade: "websocket" } });
    const socket = response.webSocket;
    if (!socket) throw new Error(`no websocket: ${response.status}`);
    socket.accept();
    const client = new Client(socket);
    await client.next((f) => f.type === "session.ready");
    return client;
  }

  private flush(): void {
    for (const waiter of [...this.waiters]) {
      const index = this.frames.findIndex((f, i) => i >= this.seen && waiter.match(f));
      if (index >= 0) {
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        waiter.resolve(this.frames[index]!);
      }
    }
  }

  send(frame: Frame): void { this.socket.send(JSON.stringify(frame)); }

  /** The next frame (from now on) matching `match`. */
  next(match: (f: Frame) => boolean, ms = 10_000): Promise<Frame> {
    const index = this.frames.findIndex((f, i) => i >= this.seen && match(f));
    if (index >= 0) return Promise.resolve(this.frames[index]!);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`no matching frame within ${ms} ms; got ${JSON.stringify(this.frames.slice(this.seen))}`)), ms);
      this.waiters.push({ match, resolve: (f) => { clearTimeout(timer); resolve(f); } });
    });
  }

  /** Forget frames received so far (for `after` assertions). */
  mark(): void { this.seen = this.frames.length; }
  since(): Frame[] { return this.frames.slice(this.seen); }

  async login(project: string, email: string, password: string): Promise<string> {
    this.send({ type: "auth", id: "login-auth", project, controlOnly: true, capabilities: {} });
    await this.next((f) => f.id === "login-auth");
    this.send({ type: "action.call", id: "login", scope: "control", path: "control.auth.passwordLogin", args: { email, password }, idempotencyKey: `login-${email}-${Math.random()}` });
    const result = await this.next((f) => f.id === "login" && (f.type === "action.result" || f.type === "action.error"));
    if (result.type !== "action.result") throw new Error(JSON.stringify(result));
    return result.result.accessToken as string;
  }

  async auth(project: string, tenant: string, token: string, id = "tenant-auth", clientContract?: number): Promise<Frame> {
    this.send({ type: "auth", id, project, tenant, token, capabilities: {}, ...(clientContract === undefined ? {} : { clientContract }) });
    const frame = await this.next((f) => f.id === id && (f.type === "auth.result" || f.type === "auth.error"));
    if (frame.type !== "auth.result") throw new Error(JSON.stringify(frame));
    return frame.result;
  }

  /** One `reducer.call`; resolves with the reply (`reducer.result` or `reducer.error`). */
  async call(id: string, path: string, args: unknown, extra: Frame = {}): Promise<Frame> {
    this.send({ type: "reducer.call", id, path, args, ...extra });
    return this.next((f) => f.id === id && (f.type === "reducer.result" || f.type === "reducer.error"));
  }

  async query(id: string, path: string, args: unknown): Promise<Frame> {
    this.send({ type: "query.call", id, path, args });
    return this.next((f) => f.id === id && (f.type === "query.result" || f.type === "query.error"));
  }

  close(): void { this.socket.close(1000, "done"); }
}

/** A project with the fixture deployed, one tenant and Members. */
export async function seed(name: string, manifest: Record<string, unknown>, members: string[] = ["alice"]) {
  const { projectId } = await stub("POST", "projects", { name });
  await stub("POST", "sync", { ...manifest, project: projectId });
  const tenant = `${name}-a`;
  await stub("POST", "tenants", { projectId, id: tenant, name: tenant });
  const accounts: Record<string, { accountId: string; memberId: string; email: string }> = {};
  for (const member of members) {
    const email = `${member}@k07.invalid`;
    accounts[member] = { ...(await stub("POST", "members", { projectId, tenantId: tenant, email, password: "k07-password", name: member })), email };
  }
  return { projectId, tenant, accounts };
}

/** A client logged in as `member` and authenticated in `tenant`. */
export async function member(projectId: string, tenant: string, email: string, clientContract?: number): Promise<{ client: Client; token: string; auth: Frame }> {
  const client = await Client.connect();
  const token = await client.login(projectId, email, "k07-password");
  const auth = await client.auth(projectId, tenant, token, "tenant-auth", clientContract);
  return { client, token, auth };
}

export async function numbers(tenant: string): Promise<any> {
  const response = await SELF.fetch(`http://k07.test/_stub/reducer/numbers?tenant=${encodeURIComponent(tenant)}`, { headers: { authorization: `Bearer ${ADMIN}` } });
  return response.json();
}
