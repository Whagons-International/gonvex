// X17o: a Reducer call that arrives while the socket's Action is in flight is forwarded to the tenant at once,
// marked with that Action, and keeps its place in the socket's order. Synthetic ports.
import { describe, expect, it } from "vitest";
import { ModuleCache, Session } from "../session";
import { DEFAULT_PRESENCE_CONFIG } from "../../presence/hub";
import { REPLICA_DELIVERY_VERSION } from "../../delivery/protocol";
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

function fixture(opts: { legacyBeat?: string } = {}) {
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
  const modules = new ModuleCache(ports);
  const session = Session.open({
    ports, host: "holder", connectionId: "c", publicTransport: false, modules,
    ...(opts.legacyBeat ? { presence: { ...DEFAULT_PRESENCE_CONFIG, enabled: true, legacyBeatPaths: [opts.legacyBeat] } } : {}),
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
  const record = (session as unknown as { record: Record<string, unknown> & { tenant: TenantSession } }).record;
  return { session, sent, calls, tick, types, send, auth, action, create, of, modules, record };
}

describe("X17o a Reducer call behind an in-flight Action is forwarded at once", () => {
  it("dispatches it immediately, marked with the Action, and replies in the socket's order", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    expect(f.of("c1")).toHaveLength(1); // sent while the Action is out
    expect(f.of("c1")[0]!.socket.afterAction).toBe("a1");
    expect(f.of("c1")[0]!.socket.forwardContract).toBeNull(); // the contract its frame was checked against (none)
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

  // ---- review: one test per guard (each fails without its guard) ----
  const notForwarded = async (f: ReturnType<typeof fixture>, label: string) => {
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    expect(f.of("c1"), label).toHaveLength(0);
    f.of("a1")[0]!.release(); await a; await f.tick(); f.of("c1")[0]?.release(); await c;
  };
  it("guard: not forwarded on the public transport", async () => {
    const f = fixture(); await f.auth(); f.record.publicTransport = true; await notForwarded(f, "public");
  });
  it("guard: not forwarded under public admission", async () => {
    const f = fixture(); await f.auth(); f.record.admission = { checkedAt: 1_000_000, revoked: false }; await notForwarded(f, "admission");
  });
  it("guard: not forwarded for an anonymous tenant session", async () => {
    const f = fixture(); await f.auth(); f.record.tenant = { ...f.record.tenant, anonymous: true }; await notForwarded(f, "anonymous");
  });
  it("guard: not forwarded when presence may answer the call in the gateway (legacy beat path)", async () => {
    const f = fixture({ legacyBeat: "tasks.create" }); await f.auth(); await notForwarded(f, "presence");
  });
  it("guard: a forwarded call whose turn finds a client update required sends nothing and closes", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    expect(f.of("c1")).toHaveLength(1);
    f.modules.set("p", { artifactHash: "v2", clientContract: 7 } as ModuleInfo); // a publication changed the contract
    f.of("c1")[0]!.release(); f.of("a1")[0]!.release(); await a; await c;
    expect(f.types()).toEqual(["action.result:a1"]);
    expect(f.sent.some((text) => text.includes('"client.updateRequired"'))).toBe(true);
  });
  it("guard: a forward that fails after the socket closed is not dispatched again", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    f.of("a1")[0]!.release(); await a; await f.tick(); // c1's turn awaits its forward
    f.session.close(CLOSE.ended);
    f.of("c1")[0]!.release(new Error("socket gone")); await c; await f.tick();
    expect(f.of("c1")).toHaveLength(1);
  });

  // ---- review item 5: the forwarded call's rows never precede the Action's result ----
  it("holds the forwarded call's commit rows (delivered first) until the Action's result was sent", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    // The tenant ran c1 as soon as a1's dispatch returned; its delivery RPC reaches the holder before a1's reply.
    const tx = JSON.stringify({ type: "replica.transaction", cursor: { epoch: "e", revision: 9 }, originCommandId: "c1", changes: [] });
    const watermark = JSON.stringify({ type: "replica.watermark", revision: 9 });
    const epoch = (f.record.subscriptionEpoch as number | undefined) ?? 0;
    f.session.replicaDelivery({ v: REPLICA_DELIVERY_VERSION, instance: (f.record.replicaInstance as string | undefined) ?? "i", scopeFence: 1, shared: [], sockets: {} } as never, { ops: [["f", tx], ["f", watermark]], subscriptionEpoch: epoch, originCommandId: "c1" } as never);
    // An unrelated earlier frame (the Action's own tool commit) is not held.
    expect(f.sent.some((text) => text.includes('"originCommandId":"c1"'))).toBe(false);
    f.of("a1")[0]!.release(); await a;
    const order = f.sent.map((text) => (text.includes('"originCommandId":"c1"') ? "rows:c1" : text.includes('"replica.watermark"') ? "watermark" : text.includes('"action.result"') ? "action.result" : null)).filter(Boolean);
    expect(order).toEqual(["action.result", "rows:c1", "watermark"]);
    f.of("c1")[0]!.release(); await c;
  });

  it("frames before the forwarded call's commit (the Action's own tool rows) are not held", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    const tool = JSON.stringify({ type: "replica.transaction", cursor: { epoch: "e", revision: 8 }, originCommandId: "tool-1", changes: [] });
    const epoch = (f.record.subscriptionEpoch as number | undefined) ?? 0;
    f.session.replicaDelivery({ v: REPLICA_DELIVERY_VERSION, instance: (f.record.replicaInstance as string | undefined) ?? "i", scopeFence: 1, shared: [], sockets: {} } as never, { ops: [["f", tool]], subscriptionEpoch: epoch, originCommandId: "tool-1" } as never);
    expect(f.sent.some((text) => text.includes('"tool-1"'))).toBe(true); // sent at once, before the Action's result
    f.of("a1")[0]!.release(); f.of("c1")[0]!.release(); await a; await c;
  });
});
