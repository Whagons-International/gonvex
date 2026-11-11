/**
 * The socket side through a real `Session` with fake ports: the in-memory
 * answers of `call_reducer_with_presence` (support heartbeat repeats, legacy
 * beats, legacy transitions), what reaches the tenant object and in which
 * order, and how deliveries reach the socket.
 */
import { describe, expect, it } from "vitest";
import { NOTHING_NEGOTIATED, negotiate } from "../../session/capabilities";
import type { ClientFrame } from "../../session/decode";
import type { Ports, SocketView, TenantSession } from "../../session/ports";
import { NO_CONNECTION } from "../../session/ports";
import { Session, type SessionRecord } from "../../session/session";
import { DEFAULT_PRESENCE_CONFIG } from "../hub";

const ROUTE = { projectId: "p1", tenantId: "t1", databaseUrl: "" };
const ACCOUNT = { id: "acc-1", email: "", emailVerified: false, name: "", avatarUrl: "", provider: "" };

function tenantSession(over: Partial<TenantSession> = {}): TenantSession {
  return {
    identity: { projectId: "p1", account: ACCOUNT }, route: ROUTE,
    member: { id: "m1", accountId: "acc-1", status: "active", displayName: "", avatarUrl: "", role: "admin", permissions: null, membershipRevision: 1 },
    admissionRevision: 3, delegation: null, developerSession: null, anonymous: false, ...over,
  };
}

type Call = { port: string; frame?: ClientFrame; args?: unknown[] };

function harness(options: { presenceCap?: boolean; batchFrames?: boolean; tenant?: TenantSession; controlReply?: (f: ClientFrame) => string[] } = {}) {
  const calls: Call[] = [];
  const sent: string[] = [];
  const tenant = options.tenant ?? tenantSession();
  const ports: Ports = {
    control: {
      project: async () => ({ ok: true, value: null }),
      loadSessionIdentity: async () => { throw new Error("unused"); },
      authenticateSession: async () => ({ ok: true, value: tenant }),
      authenticateImpersonation: async () => { throw new Error("unused"); },
      resolvePublicTenant: async () => { throw new Error("unused"); },
      revalidate: async (items) => items.map(() => true),
      dispatch: async (_s: SocketView, frame: ClientFrame) => { calls.push({ port: "control", frame }); return options.controlReply?.(frame) ?? []; },
      connectionState: async (...args: unknown[]) => { calls.push({ port: "connectionState", args }); },
    },
    modules: { module: async () => ({ artifactHash: "h", clientContract: null }), epoch: async () => "e" },
    tenants: {
      dispatch: async (_s: SocketView, frame: ClientFrame) => {
        calls.push({ port: "tenant", frame });
        if (frame.type === "reducer.call") return [JSON.stringify({ type: "reducer.result", id: frame.id, path: frame.path, result: null, originCommandId: frame.id, committedRevision: 9 })];
        return [];
      },
      presenceLeave: async (...args: unknown[]) => { await new Promise(r => setTimeout(r, 20)); calls.push({ port: "leave", args }); },
    },
  };
  const record: SessionRecord = {
    v: 1, connectionId: "conn-1", openedAt: 0, publicTransport: false, pendingUntil: null,
    connection: { ...NO_CONNECTION, projectId: "p1", identity: { projectId: "p1", account: ACCOUNT }, tenant, authToken: "tok" },
    tenant, clientContract: null,
    negotiated: options.presenceCap || options.batchFrames ? negotiate({ presence: options.presenceCap ? 1 : 0, replicaBatchFrames: options.batchFrames ? 1 : 0 } as never, true) : NOTHING_NEGOTIATED,
    publicScope: null, revalidateAt: null, sentRevision: 3, livenessAt: 0,
  };
  const session = Session.restore(record, {
    ports, ready: { runtimeVersion: "dev", presence: true }, host: "gw-1",
    presence: { ...DEFAULT_PRESENCE_CONFIG, legacyBeatPaths: ["presence.beat"], legacyTransitionPaths: ["presence.transition"] },
    sink: { send: (t) => sent.push(t), close: () => {} },
  });
  const send = (frame: Record<string, unknown>) => session.receive(JSON.stringify(frame));
  const take = () => sent.splice(0).map(t => JSON.parse(t));
  const presenceCalls = () => calls.filter(c => c.port === "leave" || (c.port === "tenant" && c.frame!.type.startsWith("presence."))).map(c => c.port === "leave" ? "leave" : `${c.frame!.type}${"state" in c.frame! ? ` ${c.frame.state}` : ""}`);
  return { session, record, calls, send, take, presenceCalls };
}

describe("socket presence", () => {
  it("answers a legacy beat in memory once a watermark was sent, registering the caller once", async () => {
    const h = harness();
    await h.send({ type: "reducer.call", id: "b1", path: "presence.beat", args: {} });
    expect(h.calls.map(c => c.port)).toEqual(["tenant"]); // no watermark yet: the Reducer runs
    h.take();
    await h.session.deliver([], 5);
    await h.send({ type: "reducer.call", id: "b2", path: "presence.beat", args: {}, trace: { clientSentAtMs: 1 } });
    const [result, watermark] = h.take();
    expect(result).toMatchObject({ type: "reducer.result", id: "b2", path: "presence.beat", originCommandId: "b2", committedRevision: 5, trace: { clientSentAtMs: 1 } });
    expect(Object.keys(result)).toEqual(["type", "id", "path", "result", "originCommandId", "committedRevision", "trace"]);
    expect(typeof result.result).toBe("number");
    expect(watermark).toEqual({ type: "replica.watermark", revision: 5 });
    await h.send({ type: "reducer.call", id: "b3", path: "presence.beat", args: {} });
    expect(h.presenceCalls()).toEqual(["presence.set active"]);
    // A reported idle state is kept: beats never override it.
    await h.send({ type: "presence.set", state: "idle" });
    await h.send({ type: "reducer.call", id: "b4", path: "presence.beat", args: {} });
    expect(h.presenceCalls()).toEqual(["presence.set active", "presence.set idle"]);
    // `reducer.callMany` items take the same path.
    h.take();
    await h.send({ type: "reducer.callMany", calls: [{ id: "b5", path: "presence.beat", args: {} }, { id: "x", path: "other", args: {} }] });
    expect(h.take().map(f => f.type)).toEqual(["reducer.result", "replica.watermark", "reducer.result"]);
  });

  it("never short-circuits a beat for a delegated session or a control call", async () => {
    const h = harness({ tenant: tenantSession({ delegation: { actor: "x" } }) });
    await h.session.deliver([], 5);
    await h.send({ type: "reducer.call", id: "b", path: "presence.beat", args: {} });
    await h.send({ type: "reducer.call", id: "c", path: "presence.beat", args: {}, scope: "control" });
    expect(h.calls.map(c => c.port)).toEqual(["tenant", "control"]);
  });

  it("answers a repeated support heartbeat from memory until its attribution changes", async () => {
    const h = harness({ controlReply: (f) => f.type === "reducer.call" ? [JSON.stringify({ type: "reducer.result", id: f.id, path: f.path, result: { sessionId: "support_conn-1" }, originCommandId: f.id }), JSON.stringify({ type: "control.watermark", id: f.id })] : [] });
    const beat = (id: string, release = "r1") => h.send({ type: "reducer.call", id, path: "control.support.heartbeat", scope: "control", args: { release: ` ${release} `, environment: "prod" } });
    await beat("h1");
    expect(h.record.support).toEqual({ sessionId: "support_conn-1", project: "p1", tenant: "t1", account: "acc-1", release: "r1", environment: "prod" });
    expect(h.calls.find(c => c.port === "connectionState")?.args).toEqual(["conn-1", { projectId: "p1", tenantId: "t1", authenticated: true, supportSessionId: "support_conn-1" }]);
    h.take();
    await beat("h2");
    expect(h.take()).toEqual([
      { type: "reducer.result", id: "h2", path: "control.support.heartbeat", result: { sessionId: "support_conn-1" }, originCommandId: "h2" },
      { type: "control.watermark", id: "h2" },
    ]);
    expect(h.calls.filter(c => c.port === "control")).toHaveLength(1);
    await beat("h3", "r2");
    expect(h.calls.filter(c => c.port === "control")).toHaveLength(2);
    // Closing reports the support session gone (K06 writes its last-seen time).
    await h.session.closedByPeer();
    expect(h.calls.at(-1)).toEqual({ port: "connectionState", args: ["conn-1", null] });
  });

  it("mirrors a successful legacy transition into the feed only for clients without presence v2", async () => {
    const old = harness();
    await old.send({ type: "reducer.call", id: "t", path: "presence.transition", args: { state: "Gone" } });
    await old.send({ type: "reducer.call", id: "t2", path: "presence.transition", args: { state: "idle" } });
    expect(old.presenceCalls()).toEqual(["presence.set idle"]); // "gone" while absent changes nothing
    const v2 = harness({ presenceCap: true });
    await v2.send({ type: "reducer.call", id: "t", path: "presence.transition", args: { state: "idle" } });
    expect(v2.presenceCalls()).toEqual([]);
  });

  it("sends tenant RPCs in order: a leave on re-auth precedes the re-announced state", async () => {
    const h = harness();
    await h.send({ type: "presence.set", state: "active" });
    await h.send({ type: "presence.set", state: "active" }); // repeat: no RPC
    await h.send({ type: "presence.set", state: "busy" }); // invalid: ignored
    await h.send({ type: "auth", id: "a2", token: "tok", tenant: "t1", capabilities: { presence: 1 } });
    await h.send({ type: "presence.set", state: "active" });
    expect(h.presenceCalls()).toEqual(["presence.set active", "leave", "presence.set active"]);
  });

  it("delivers snapshots, patches and resyncs only to the subscription the socket holds", async () => {
    const h = harness({ batchFrames: true });
    await h.send({ type: "presence.subscribe", id: "s1" });
    h.take();
    const patch = '{"type":"presence.patch","serverTime":1,"changes":[]}';
    await h.session.presenceDelivery({ v: 1, tenantId: "t1", projectId: "p1", patch });
    expect(h.take()).toEqual([]); // before its snapshot: dropped
    await h.session.presenceDelivery({ v: 1, tenantId: "t1", projectId: "p1", snapshots: [{ connectionId: "other", id: "s1", frame: "{}" }, { connectionId: "conn-1", id: "old", frame: "{}" }] });
    expect(h.take()).toEqual([]);
    const snapshot = '{"type":"presence.snapshot","id":"s1","serverTime":1,"members":[]}';
    await h.session.presenceDelivery({ v: 1, tenantId: "t1", projectId: "p1", snapshots: [{ connectionId: "conn-1", id: "s1", frame: snapshot }], patch });
    const [first, second] = h.take();
    expect(first).toEqual(JSON.parse(snapshot)); // the subscribe answer is sent bare, as Rust does
    // `from` is the last watermark sent in a batch (A.4 row 1, delivery.rs `last_watermark`): none yet.
    expect(second).toEqual({ type: "replica.batch", tenant: "t1", class: expect.stringMatching(/^[0-9a-f]{16}$/), from: 0, items: [JSON.parse(patch)] });
    await h.session.presenceDelivery({ v: 1, tenantId: "t1", projectId: "p1", resync: { serverTime: 2, members: [{ memberId: "m1", state: "idle", since: 1 }] } });
    expect(h.take()[0].items[0]).toEqual({ type: "presence.snapshot", id: "s1", serverTime: 2, members: [{ memberId: "m1", state: "idle", since: 1 }] });
    expect(h.session.presence.report()).toEqual({ connectionId: "conn-1", projectId: "p1", memberId: "m1", state: null, subscriptionId: "s1" });
    await h.send({ type: "presence.unsubscribe" });
    await h.session.presenceDelivery({ v: 1, tenantId: "t1", projectId: "p1", patch });
    expect(h.take()).toEqual([]);
    expect(h.session.presence.report()).toBeNull();
  });

  it("refuses subscribe for ineligible sessions without a tenant call", async () => {
    const h = harness({ tenant: tenantSession({ member: { ...tenantSession().member, id: "_gonvex_service:svc" } }) });
    await h.send({ type: "presence.subscribe", id: "s" });
    await h.send({ type: "presence.set", state: "active" });
    expect(h.take()).toEqual([{ type: "presence.error", id: "s", error: "presence requires an authenticated tenant member session" }]);
    expect(h.presenceCalls()).toEqual([]);
  });
});
