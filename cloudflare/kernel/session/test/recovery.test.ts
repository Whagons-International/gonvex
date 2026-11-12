// Deterministic Session wiring tests. The ports below are synthetic controls;
// deployed auth/visibility and CPU enforcement require the assembled matrix.
import { describe, expect, it } from "vitest";
import { Session } from "../session";
import type { Ports, TenantSession } from "../ports";
import type { ReplicaDelivery } from "../../delivery/protocol";
import type { RecoveryDelivery, RecoverySocketView, ScopedSocketWork } from "../../delivery/recovery";

function tenant(name: string, id = "t"): TenantSession {
  return {
    identity: { projectId: "p", account: { id: name, email: "", emailVerified: false, name, avatarUrl: "", provider: "" } },
    route: { projectId: "p", tenantId: id, databaseUrl: `sqlite:///${id}.db` },
    member: { id: `m-${name}`, accountId: name, status: "active", displayName: name, avatarUrl: "", role: "member", permissions: {}, membershipRevision: 1 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}
function fixture() {
  const sent: string[] = [], closes: Array<{ code: number; reason: string }> = [];
  const dispatches: RecoverySocketView[] = [];
  const ports: Ports = {
    control: {
      project: async () => ({ ok: true, value: null }),
      loadSessionIdentity: async (name) => ({ ok: true, value: tenant(name).identity }),
      authenticateSession: async (name, _project, id) => ({ ok: true, value: tenant(name, id) }),
      authenticateImpersonation: async () => ({ ok: false, kind: "InvalidSession", message: "invalid" }),
      resolvePublicTenant: async () => ({ ok: false, kind: "Other", message: "disabled" }),
      revalidate: async (items) => items.map(() => true), dispatch: async () => [],
    },
    modules: { module: async () => null, epoch: async () => "db-epoch" },
    tenants: { dispatch: async (socket) => { dispatches.push(socket as RecoverySocketView); return []; }, connectionReset: async () => {} },
  };
  const session = Session.open({ ports, host: "holder", connectionId: "c", publicTransport: false,
    ready: { runtimeVersion: "test", presence: false }, sink: { send: (text) => sent.push(text), close: (frame) => closes.push(frame) },
    timers: { later: () => () => {} },
  });
  const auth = (name = "alice", id = "t") => session.receive(JSON.stringify({ type: "auth", id: "auth", project: "p", tenant: id, token: name }));
  const payload = (epoch: number, instance = "I", text = '{"type":"private-data"}', watermark = 5): ReplicaDelivery => ({
    v: 1, instance, shared: [], watermark,
    sockets: { c: { subscriptionEpoch: epoch, ops: [["f", text]], watermark } as ScopedSocketWork },
  });
  const deliver = (p: ReplicaDelivery) => session.replicaDelivery(p, p.sockets.c);
  return { session, ports, sent, closes, dispatches, auth, payload, deliver };
}

describe("K22 session delivery scope", () => {
  it("counts ControlOnly tenantControl connections without granting tenant-data dispatch", async () => {
    const f = fixture();
    const reported: unknown[] = [];
    f.ports.control.connectionState = async (_id, state) => { reported.push(state); };
    await f.session.receive('{"type":"auth","id":"control","token":"alice","project":"p","tenant":"t","controlOnly":true,"capabilities":{"tenantControl":1}}');
    expect(f.session.record.tenant).toBeNull();
    expect(reported.at(-1)).toMatchObject({ projectId: "p", tenantId: "t", authenticated: true });
    await f.session.receive('{"type":"query.call","id":"private","path":"items.private","args":{}}');
    expect(f.dispatches).toEqual([]);
    expect(JSON.parse(f.sent.at(-1)!)).toMatchObject({ type: "query.error", id: "private" });
  });

  it("drops an already queued live result after unsubscribe at the holder", async () => {
    const f = fixture(); await f.auth();
    await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{}}');
    await f.session.receive('{"type":"query.unsubscribe","id":"q"}');
    const before = f.sent.length;
    f.deliver(f.payload(f.session.record.subscriptionEpoch!, "I", '{"type":"query.result","id":"q","result":["late"]}'));
    expect(f.sent.slice(before).some((text) => text.includes('"late"'))).toBe(false);
  });

  it("rejects delayed work before instance, data or watermark changes across Member, tenant, reauth and deploy resets", async () => {
    const f = fixture();
    await f.auth();
    const initial = f.session.record.subscriptionEpoch!;
    f.deliver(f.payload(initial));
    expect(f.session.record.replicaInstance).toBe("I");
    await f.auth("bob");
    const afterMember = JSON.stringify(f.session.record);
    const sent = f.sent.length;
    f.deliver(f.payload(initial, "old-member", "private-alice", 999));
    expect(JSON.stringify(f.session.record)).toBe(afterMember);
    expect(f.sent).toHaveLength(sent);
    for (const reset of [() => f.auth("bob", "t2"), () => f.auth("bob", "t2"), () => f.session.moduleReloaded(null)]) {
      const before = f.session.record.subscriptionEpoch!;
      await reset();
      const record = JSON.stringify(f.session.record), count = f.sent.length;
      f.deliver(f.payload(before, "stale", "stale-private", 1000));
      expect(JSON.stringify(f.session.record)).toBe(record);
      expect(f.sent).toHaveLength(count);
    }
    f.deliver(f.payload(f.session.record.subscriptionEpoch!, "current", '{"type":"current"}', 7));
    expect(f.sent.at(-2)).toBe('{"type":"current"}');
    expect(f.session.record.watermark).toBe(7);
  });

  it("fails closed for missing-epoch scoped work; ignores same-instance unscoped watermarks", async () => {
    const f = fixture(); await f.auth();
    f.deliver(f.payload(f.session.record.subscriptionEpoch!));
    f.session.replicaDelivery({ v: 1, instance: "I", shared: [], sockets: {}, watermark: 999 }, undefined);
    expect(f.session.record.watermark).toBe(5);
    expect(f.closes).toEqual([]);
    f.session.replicaDelivery({ v: 1, instance: "I", shared: [], sockets: {} }, { ops: [["f", "private-old-sender"]] });
    expect(f.closes).toEqual([{ code: 1012, reason: "delivery scope requires reconnect" }]);
    expect(f.sent).not.toContain("private-old-sender");
    const g = fixture(); await g.auth(); g.deliver(g.payload(g.session.record.subscriptionEpoch!));
    g.session.replicaDelivery({ v: 1, instance: "restarted", shared: [], sockets: {}, watermark: 999 }, undefined);
    expect(g.closes[0]?.code).toBe(1012);
  });

  it("retains ordered pending LQs only after success, and a failed replacement preserves the prior request", async () => {
    const f = fixture(); await f.auth();
    const subscribe = (args: unknown) => f.session.receive(JSON.stringify({ type: "query.subscribe", id: "q", path: "items.live", args }));
    await subscribe({ owner: "alice" });
    expect(f.session.record.liveQueries?.q).toBeUndefined();
    const hash = "a".repeat(64);
    f.deliver(f.payload(f.session.record.subscriptionEpoch!, "I", `{"type":"query.result","id":"q","result":[],"windowRevision":"${hash}"}`));
    expect(f.session.record.liveQueries?.q?.args).toEqual({ owner: "alice" });
    expect(f.session.record.pendingLiveQueries?.q).toBeUndefined();
    await subscribe({ invalid: true });
    f.deliver(f.payload(f.session.record.subscriptionEpoch!, "I", '{"type":"query.error","id":"q","error":"invalid"}'));
    expect(f.session.record.liveQueries?.q?.args).toEqual({ owner: "alice" });
    await f.auth("bob");
    expect(f.session.record.liveQueries).toEqual({});
  });

  it("keeps same-id initial replies correlated when multiple replacements precede delivery", async () => {
    const f = fixture(); await f.auth();
    for (const args of [{ first: true }, { invalid: true }]) await f.session.receive(JSON.stringify({ type: "query.subscribe", id: "q", path: "items.live", args }));
    f.deliver(f.payload(f.session.record.subscriptionEpoch!, "I", '{"type":"query.result","id":"q","result":[]}'));
    expect(f.session.record.liveQueries?.q?.args).toEqual({ first: true });
    expect(f.session.record.pendingLiveQueries?.q).toHaveLength(1);
    f.deliver(f.payload(f.session.record.subscriptionEpoch!, "I", '{"type":"query.error","id":"q","error":"invalid"}'));
    expect(f.session.record.liveQueries?.q?.args).toEqual({ first: true });
    expect(f.session.record.pendingLiveQueries?.q).toBeUndefined();
  });

  it("recovery waits for real LQ replies, includes pending opens, and rejects stale scope or a refused rebuild", async () => {
    const f = fixture(); await f.auth();
    await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{}}');
    f.session.delivery.opening("pending-replica", "items.all");
    const recovery: RecoveryDelivery = { v: 1, instance: "new", shared: [], sockets: {}, recovery: { reason: "change-feed-lagged" } };
    f.ports.tenants.dispatch = async (socket) => { f.dispatches.push(socket as RecoverySocketView); return ['{"type":"query.error","id":"q","error":"visibility unavailable"}']; };
    await expect(f.session.recoverReplica("t", recovery)).rejects.toThrow("live query recovery was refused");
    expect(f.sent.some((text) => text.includes('"id":"pending-replica"') && text.includes('"reason":"change-feed-lagged"'))).toBe(true);
    expect(f.dispatches.at(-1)?.queryRecovery).toBe(true);
    // Queue an auth before recovery: capturing the old epoch must not reset the new Member.
    const auth = f.auth("bob"), stale = f.session.recoverReplica("t", recovery);
    await auth;
    await expect(stale).rejects.toThrow("delivery recovery scope changed");
  });
});
