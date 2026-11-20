// #549 re-review (2304b9e9): gaps in the delta-ordering hold and the contract check. Fixture copied from forward.test.ts.
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
  const auth = async (capabilities?: Record<string, number>) => { await send({ type: "auth", id: "login", project: "p", tenant: "t", token: "alice", ...(capabilities ? { capabilities } : {}) }); };
  const action = (id: string) => send({ type: "action.call", id, path: "assistant.rewriteText", args: {} });
  const create = (id: string, extra: Record<string, unknown> = { idempotencyKey: `k-${id}` }) => send({ type: "reducer.call", id, path: "tasks.create", args: {}, ...extra });
  const of = (id: string) => calls.filter((c) => (c.frame as { id?: string }).id === id);
  const record = (session as unknown as { record: Record<string, unknown> & { tenant: TenantSession } }).record;
  return { session, sent, calls, tick, types, send, auth, action, create, of, modules, record };
}



const epochOf = (f: ReturnType<typeof fixture>) => (f.record.subscriptionEpoch as number | undefined) ?? 0;
const instanceOf = (f: ReturnType<typeof fixture>) => (f.record.replicaInstance as string | undefined) ?? "i";
// Adapted (structural fix): the tenant engine puts each commit's origin on the socket's work (`originCommandId`).
const deliver = (f: ReturnType<typeof fixture>, shared: string[], ops: unknown[], origin: string) =>
  f.session.replicaDelivery({ v: REPLICA_DELIVERY_VERSION, instance: instanceOf(f), scopeFence: 1, shared, sockets: {} } as never, { ops, subscriptionEpoch: epochOf(f), originCommandId: origin } as never);
const label = (text: string) => text.includes('"originCommandId":"c1"') || text.includes('"c1-row"') ? "rows:c1"
  : text.includes('"tool-1"') ? "rows:tool" : text.includes('"action.result"') ? "action.result" : text.includes('"reducer.result"') ? "reducer.result" : null;
const order = (f: ReturnType<typeof fixture>) => f.sent.map(label).filter(Boolean);

describe("#549 re-review: the forwarded call's output never precedes the Action's result", () => {
  it("R1 batched socket: the forwarded commit sitting in the open replica.batch is not flushed ahead of action.result", async () => {
    const f = fixture(); await f.auth({ replicaBatchFrames: 1 });
    expect((f.record as unknown as { negotiated: { batchFrames: boolean } }).negotiated.batchFrames).toBe(true);
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    expect(f.of("c1")).toHaveLength(1);
    // The tenant ran c1 when a1's dispatch returned; its delivery reaches the holder first and opens a batch (50 ms tick).
    const tx = JSON.stringify({ type: "replica.transaction", cursor: { epoch: "e", revision: 9 }, originCommandId: "c1", changes: [] });
    deliver(f, [tx], [["i", 0]], "c1");
    expect(order(f)).toEqual([]); // still in the open batch
    f.of("a1")[0]!.release(); await a; // send(action.result) flushes the open batch first, while the Action-reply bypass is on
    expect(order(f)).toEqual(["action.result", "rows:c1"]);
    f.of("c1")[0]!.release(); await c;
  });

  it("R2 batched socket: the Action's tool rows and the forwarded commit in one open batch keep tool rows before, c1 rows after action.result", async () => {
    const f = fixture(); await f.auth({ replicaBatchFrames: 1 });
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    const tool = JSON.stringify({ type: "replica.transaction", cursor: { epoch: "e", revision: 8 }, originCommandId: "tool-1", changes: [] });
    const tx = JSON.stringify({ type: "replica.transaction", cursor: { epoch: "e", revision: 9 }, originCommandId: "c1", changes: [] });
    deliver(f, [tool], [["i", 0]], "tool-1");
    deliver(f, [tx], [["i", 0]], "c1");
    f.of("a1")[0]!.release(); await a;
    expect(order(f)).toEqual(["rows:tool", "action.result", "rows:c1"]);
    f.of("c1")[0]!.release(); await c;
  });

  it("R3 a live query re-run for the forwarded commit (ops before its transaction frame) is not sent ahead of action.result", async () => {
    const f = fixture(); await f.auth();
    (f.record as unknown as { liveQueries: Record<string, unknown> }).liveQueries = { q1: { type: "query.subscribe", id: "q1", path: "tasks.live", args: {} } };
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    // engine.ts: a commit's ops are [...live query frames, transaction, deltas...]; the live result shows c1's row.
    const live = JSON.stringify({ type: "query.result", id: "q1", path: "tasks.live", result: [{ id: "c1-row" }] });
    const tx = JSON.stringify({ type: "replica.transaction", cursor: { epoch: "e", revision: 9 }, originCommandId: "c1", changes: [] });
    deliver(f, [], [["f", live], ["f", tx]], "c1");
    f.of("a1")[0]!.release(); await a;
    expect(order(f)[0]).toBe("action.result");
    f.of("c1")[0]!.release(); await c;
  });

  it("R4 a delta-only commit (no transaction frame: replica.delta carries originCommandIds: []) is not sent ahead of action.result", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    const delta = '{"type":"replica.delta","id":"r1","cursor":{"epoch":"e","revision":9},"deleted":[],"digest":"d","originCommandIds":[],"path":"tasks","truncated":false,"upserts":[{"id":"c1-row"}]}';
    deliver(f, [], [["f", delta]], "c1");
    f.of("a1")[0]!.release(); await a;
    expect(order(f)[0]).toBe("action.result");
    f.of("c1")[0]!.release(); await c;
  });
});

describe("#549 re-review: a publication while the forward is out commits nothing (fix a)", () => {
  it("R5 (fix a): the forward carries the checked contract; the tenant refuses after a publication, the client gets client.updateRequired, nothing is dispatched again", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    expect(f.of("c1")[0]!.socket.forwardContract).toBeNull(); // checked against the module's contract (none)
    // host.moduleReloaded: cache set and the socket's moduleReloaded queued behind c1, synchronously.
    f.modules.set("p", { artifactHash: "v2", clientContract: 7 } as ModuleInfo);
    const reload = f.session.moduleReloaded({ artifactHash: "v2", clientContract: 7, publicationId: "pub-2" } as ModuleInfo);
    // The tenant's active contract (7) is not the forwarded one (none): it answers FORWARD_REFUSED, nothing committed.
    f.of("c1")[0]!.release([FORWARD_REFUSED]); f.of("a1")[0]!.release(); await a; await c; await reload;
    expect(f.types()).toEqual(["action.result:a1"]);
    expect(f.sent.some((text) => text.includes('"client.updateRequired"'))).toBe(true);
    expect(f.of("c1")).toHaveLength(1); // as today: c1 is never dispatched after the update is required
  });
});

describe("#549 re-review D: the held output is bounded", () => {
  it("closes the socket when the held output passes the hold limit", async () => {
    const f = fixture(); await f.auth();
    const a = f.action("a1"); await f.tick();
    const c = f.create("c1"); await f.tick();
    const big = JSON.stringify({ type: "replica.delta", id: "r1", upserts: [{ id: "c1-row", body: "x".repeat(9 * 1024 * 1024) }] });
    deliver(f, [], [["f", big]], "c1");
    expect(f.session.closed).toBe(true);
    f.of("a1")[0]!.release(); f.of("c1")[0]!.release(); await a; await c;
  });
});
