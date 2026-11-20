// X17o: a Reducer call that arrives while the socket's Action is in flight is forwarded to the tenant at once,
// marked with that Action, and keeps its place in the socket's order. Synthetic ports.
import { describe, expect, it } from "vitest";
import { Session } from "../session";
import { FORWARD_REFUSED } from "../forward";
import { CLOSE } from "../protocol";
import type { ClientFrame } from "../decode";
import type { ModuleInfo, Ports, SocketView, TenantSession } from "../ports";

function tenant(name: string, id = "t"): TenantSession {
  return {
    identity: { projectId: "p", account: { id: name, email: "", emailVerified: false, name, avatarUrl: "", provider: "" } },
    route: { projectId: "p", tenantId: id, databaseUrl: `sqlite:///${id}.db` },
    member: { id: `m-${name}`, accountId: name, status: "active", displayName: name, avatarUrl: "", role: "member", permissions: {}, membershipRevision: 1 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}
type Call = { socket: SocketView; frame: ClientFrame; release(out?: string[] | Error): void; settled: boolean };

function fixture() {
  const sent: string[] = [], calls: Call[] = [];
  const reply = (frame: ClientFrame) => frame.type === "action.call"
    ? [JSON.stringify({ type: "action.result", id: frame.id, path: frame.path, result: null })]
    : [JSON.stringify({ type: "reducer.result", id: (frame as { id: string }).id, result: null })];
  const ports: Ports = {
    control: {
      project: async () => ({ ok: true, value: null }),
      loadSessionIdentity: async (name) => ({ ok: true, value: tenant(name).identity }),
      authenticateSession: async (name, _project, id) => ({ ok: true, value: tenant(name, id) }),
      authenticateImpersonation: async () => ({ ok: false, kind: "InvalidSession", message: "invalid" }),
      resolvePublicTenant: async () => ({ ok: false, kind: "Other", message: "disabled" }),
      revalidate: async (items) => items.map(() => true), dispatch: async () => [],
    },
    modules: { module: async () => ({ artifactHash: "v1", clientContract: null }) as ModuleInfo, epoch: async () => "db-epoch" },
    tenants: {
      // Every tenant call is held until the test releases it (in-flight Actions, forwarded calls).
      dispatch: (socket, frame) => new Promise<string[]>((resolve, reject) => {
        const call: Call = { socket, frame, settled: false, release: (out) => { call.settled = true; if (out instanceof Error) reject(out); else resolve(out ?? reply(frame)); } };
        calls.push(call);
      }),
      connectionReset: async () => {},
    },
  };
  const session = Session.open({
    ports, host: "holder", connectionId: "c", publicTransport: false,
    ready: { runtimeVersion: "test", presence: false },
    sink: { send: (text) => { sent.push(text); }, close: () => {} },
    now: () => 1_000_000,
    timers: { later: () => () => {} },
  });
  const tick = async () => { for (let i = 0; i < 40; i++) await Promise.resolve(); };
  const types = () => sent.map((text) => JSON.parse(text) as { type: string; id?: string }).filter((f) => f.type === "action.result" || f.type === "reducer.result" || f.type === "query.result").map((f) => `${f.type}:${f.id}`);
  const send = (frame: Record<string, unknown>) => session.receive(JSON.stringify(frame));
  const auth = async () => { await send({ type: "auth", id: "login", project: "p", tenant: "t", token: "alice" }); };
  const action = (id: string) => send({ type: "action.call", id, path: "assistant.rewriteText", args: {} });
  const create = (id: string, extra: Record<string, unknown> = { idempotencyKey: `k-${id}` }) => send({ type: "reducer.call", id, path: "tasks.create", args: {}, ...extra });
  const of = (id: string) => calls.filter((c) => (c.frame as { id?: string }).id === id);
  return { session, sent, calls, tick, types, send, auth, action, create, of };
}

describe("X17o a Reducer call behind an in-flight Action is forwarded at once", () => {
  it("dispatches it immediately, marked with the Action, and replies in the socket's order", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    expect(f.of("c1")).toHaveLength(1); // sent while the Action is out
    expect(f.of("c1")[0]!.socket.afterAction).toBe("a1");
    f.of("c1")[0]!.release(); await f.tick(); // the tenant answered the forwarded call first
    expect(f.types()).toEqual([]); // nothing is sent before the Action's own result
    f.of("a1")[0]!.release(); await a; await c;
    expect(f.types()).toEqual(["action.result:a1", "reducer.result:c1"]);
  });

  it("a refused forward is dispatched again in order, after the Action (reordered calls, restarted tenant, pending Action)", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    f.of("c1")[0]!.release([FORWARD_REFUSED]); await f.tick();
    expect(f.of("c1")).toHaveLength(1); // not before the Action's turn ends
    f.of("a1")[0]!.release(); await a; await f.tick();
    expect(f.of("c1")).toHaveLength(2);
    expect(f.of("c1")[1]!.socket.afterAction).toBeUndefined();
    f.of("c1")[1]!.release(); await c;
    expect(f.types()).toEqual(["action.result:a1", "reducer.result:c1"]);
    expect(f.sent.some((text) => text.includes("forwardRefused"))).toBe(false);
  });

  it("a failed forward (lost tenant call) is dispatched again; the idempotency key makes a repeat return the first result", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    f.of("c1")[0]!.release(new Error("tenant reset")); await f.tick();
    f.of("a1")[0]!.release(); await a; await f.tick();
    expect(f.of("c1")).toHaveLength(2);
    expect((f.of("c1")[1]!.frame as { idempotencyKey?: string }).idempotencyKey).toBe("k-c1");
    f.of("c1")[1]!.release(); await c;
    expect(f.types()).toEqual(["action.result:a1", "reducer.result:c1"]);
  });

  it("a later frame queues behind the forwarded call as before (reducer after reducer, query after reducer)", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c1 = f.create("c1"); await f.tick();
    const c2 = f.create("c2"); await f.tick();
    const q = f.send({ type: "query.call", id: "q1", path: "tasks.list", args: {} }); await f.tick();
    expect(f.of("c1")).toHaveLength(1);
    expect(f.of("c2")).toHaveLength(0); // the second call is not forwarded: the forwarded one is ahead of it
    expect(f.of("q1")).toHaveLength(0);
    f.of("c1")[0]!.release(); f.of("a1")[0]!.release(); await a; await c1; await f.tick();
    expect(f.of("c2")).toHaveLength(1);
    f.of("c2")[0]!.release(); await c2; await f.tick();
    f.of("q1")[0]!.release([JSON.stringify({ type: "query.result", id: "q1", result: null })]); await q;
    expect(f.types()).toEqual(["action.result:a1", "reducer.result:c1", "reducer.result:c2", "query.result:q1"]);
  });

  it("is not forwarded without an idempotency key, behind another queued frame, for a Control call, or after a module reload", async () => {
    const f = fixture(); await f.auth();
    let a = f.action("a1"); await f.tick();
    const c = f.create("c1", {}); await f.tick();
    expect(f.of("c1")).toHaveLength(0);
    f.of("a1")[0]!.release(); await a; await f.tick(); f.of("c1")[0]!.release(); await c;

    a = f.action("a2"); await f.tick();
    const q = f.send({ type: "query.call", id: "q2", path: "tasks.list", args: {} }); await f.tick();
    const c2 = f.create("c2"); await f.tick();
    expect(f.of("c2")).toHaveLength(0); // a query is queued ahead of it
    f.of("a2")[0]!.release(); await a; await f.tick(); f.of("q2")[0]!.release(); await q; await f.tick(); f.of("c2")[0]!.release(); await c2;

    a = f.action("a3"); await f.tick();
    const c3 = f.create("c3", { idempotencyKey: "k3", scope: "control" }); await f.tick();
    expect(f.of("c3")).toHaveLength(0);
    f.of("a3")[0]!.release(); await a; await c3;

    void f.session.moduleReloaded({ artifactHash: "v2", clientContract: null, publicationId: "pub-2" }); await f.tick();
    a = f.action("a4"); await f.tick();
    // The reload cleared the contract check, and the Action's frame re-checked it: forwarding resumes.
    const c4 = f.create("c4"); await f.tick();
    expect(f.of("c4")).toHaveLength(1);
    f.of("c4")[0]!.release(); f.of("a4")[0]!.release(); await a; await c4;
  });

  it("is not forwarded behind a reducer or a query in flight (only an Action)", async () => {
    const f = fixture(); await f.auth();
    const c0 = f.create("c0"); await f.tick();
    const c1 = f.create("c1"); await f.tick();
    expect(f.of("c1")).toHaveLength(0);
    f.of("c0")[0]!.release(); await c0; await f.tick(); f.of("c1")[0]!.release(); await c1;
    expect(f.types()).toEqual(["reducer.result:c0", "reducer.result:c1"]);
  });

  it("a socket that closes while the forward is out sends nothing for it", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    f.session.close(CLOSE.ended);
    f.of("c1")[0]!.release(); f.of("a1")[0]!.release(); await a; await c;
    expect(f.types().filter((t) => t.startsWith("reducer"))).toEqual([]);
  });
});
