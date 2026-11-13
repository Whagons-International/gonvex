// Deterministic Session wiring tests. The ports below are synthetic controls;
// deployed auth/visibility and CPU enforcement require the assembled matrix.
import { describe, expect, it } from "vitest";
import { Session } from "../session";
import type { Ports, TenantSession } from "../ports";
import { initialCompletion, type InitialRequestRef, type ReplicaDelivery } from "../../delivery/protocol";
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
  const fail={send:false};
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
    ready: { runtimeVersion: "test", presence: false }, sink: { send: (text) => { if(fail.send)throw new Error("sink unavailable"); sent.push(text); }, close: (frame) => closes.push(frame) },
    timers: { later: () => () => {} },
  });
  const auth = (name = "alice", id = "t") => session.receive(JSON.stringify({ type: "auth", id: "auth", project: "p", tenant: id, token: name }));
  const payload = (epoch: number, instance = "I", text = '{"type":"private-data"}', watermark = 5): ReplicaDelivery => ({
    v: 1, instance, shared: [], watermark,
    sockets: { c: { subscriptionEpoch: epoch, ops: [["f", text]], watermark } as ScopedSocketWork },
  });
  const deliver = (p: ReplicaDelivery) => session.replicaDelivery(p, p.sockets.c);
  const complete = (text: string, ref = Object.values(session.record.initialRequests ?? {})[0]!) => {
    const p = payload(session.record.subscriptionEpoch!, "I", text);
    const work = p.sockets.c!;
    const completion = initialCompletion(ref, work.ops[0]!, 0);
    if (!completion) throw new Error("fixture frame is not a terminal initial operation");
    deliver({ ...p, sockets: { c: { ...work, initialCompletions: [completion] } } });
  };
  return { session, ports, sent, closes, dispatches, auth, payload, deliver, complete,fail };
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
    f.complete(`{"type":"query.result","id":"q","path":"items.live","reason":"initial","result":[],"windowRevision":"${hash}"}`);
    expect(f.session.record.liveQueries?.q?.args).toEqual({ owner: "alice" });
    expect(f.session.record.pendingLiveQueries?.q).toBeUndefined();
    await subscribe({ invalid: true });
    f.complete('{"type":"query.error","id":"q","path":"items.live","error":"invalid"}');
    expect(f.session.record.liveQueries?.q?.args).toEqual({ owner: "alice" });
    await f.auth("bob");
    expect(f.session.record.liveQueries).toEqual({});
  });

  it("keeps same-id initial replies correlated when multiple replacements precede delivery", async () => {
    const f = fixture(); await f.auth();
    for (const args of [{ first: true }, { invalid: true }]) await f.session.receive(JSON.stringify({ type: "query.subscribe", id: "q", path: "items.live", args }));
    f.complete('{"type":"query.result","id":"q","path":"items.live","reason":"initial","result":[]}');
    expect(f.session.record.liveQueries?.q?.args).toEqual({ first: true });
    expect(f.session.record.pendingLiveQueries?.q).toHaveLength(1);
    f.complete('{"type":"query.error","id":"q","path":"items.live","error":"invalid"}');
    expect(f.session.record.liveQueries?.q?.args).toEqual({ first: true });
    expect(f.session.record.pendingLiveQueries?.q).toBeUndefined();
  });

  it("N5 old A change/progress cannot accept B; refused B retains A through hibernation", async () => {
    const f = fixture(); await f.auth();
    const subscribe = (args: unknown) => f.session.receive(JSON.stringify({ type: "query.subscribe", id: "q", path: "items.live", args }));
    await subscribe({ owner: "A" });
    f.complete('{"type":"query.result","id":"q","path":"items.live","reason":"initial","result":["A"]}');
    await subscribe({ owner: "B" });
    const ref = Object.values(f.session.record.initialRequests!)[0]!;
    for (const text of ['{"type":"query.result","id":"q","path":"items.live","reason":"change","result":["A changed"]}', '{"type":"query.progress","id":"q","path":"items.live","throughRevision":{"epoch":"e","sequence":5}}']) f.deliver(f.payload(f.session.record.subscriptionEpoch!, "I", text));
    expect(f.session.record.liveQueries?.q?.args).toEqual({ owner: "A" });
    expect(f.session.record.pendingLiveQueries?.q).toHaveLength(1);
    f.complete('{"type":"query.error","id":"q","path":"items.live","error":"B refused"}', ref);
    const restored = Session.restore(JSON.parse(JSON.stringify(f.session.record)), {
      ports: f.ports, host: "holder", sink: { send: () => {}, close: () => {} }, ready: { runtimeVersion: "test", presence: false }, timers: { later: () => () => {} },
    });
    expect(restored.record.liveQueries?.q?.args).toEqual({ owner: "A" });
    expect(restored.record.pendingLiveQueries?.q).toBeUndefined();
    expect(restored.record.initialRequests).toEqual({});
  });

  it("an actual no-change completion accepts B while arbitrary progress does not", async () => {
    const f = fixture(); await f.auth();
    await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{"owner":"B"},"windowRevision":"known"}');
    const progress = '{"type":"query.progress","id":"q","path":"items.live","throughRevision":{"epoch":"e","sequence":2}}';
    f.deliver(f.payload(f.session.record.subscriptionEpoch!, "I", progress));
    expect(f.session.record.liveQueries?.q).toBeUndefined();
    f.complete(progress);
    expect(f.session.record.liveQueries?.q?.args).toEqual({ owner: "B" });
    expect(Object.keys(f.session.record.initialRequests!)).toHaveLength(0);
  });

  it("failure processing is generation scoped, idempotent, sink-accepted, and never certifies revisions", async () => {
    const f = fixture(); await f.auth();
    await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{}}');
    const old = Object.values(f.session.record.initialRequests!)[0]!;
    const before = { watermark: f.session.record.watermark, instance: f.session.record.replicaInstance, revision: f.session.record.sentRevision };
    await f.session.failReplicaInitials("t", [old]);
    expect(JSON.parse(f.sent.at(-1)!)).toMatchObject({ type: "query.error", id: "q", path: "items.live" });
    const sent = f.sent.length;
    await f.session.failReplicaInitials("t", [old]);
    expect(f.sent).toHaveLength(sent);
    expect({ watermark: f.session.record.watermark, instance: f.session.record.replicaInstance, revision: f.session.record.sentRevision }).toEqual(before);
    await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{"new":true}}');
    await f.session.failReplicaInitials("t", [old]);
    expect(f.session.record.pendingLiveQueries?.q).toHaveLength(1);
    await f.session.receive('{"type":"query.unsubscribe","id":"q"}');
    await f.session.failReplicaInitials("t", [old]);
    expect(f.session.record.pendingLiveQueries?.q).toBeUndefined();
    expect(f.closes).toEqual([]);
  });

  it("0070 returns only offered exact terminal sink facts; unrebuilt/unknown/omitted refs grant no authority",async()=>{
    const f=fixture();await f.auth();
    for(const id of ['q1','q2'])await f.session.receive(JSON.stringify({type:'query.subscribe',id,path:'items.live',args:{id}}));
    await f.session.receive(JSON.stringify({type:'replica.open',id:'r',path:'items.all',args:{}}));
    const refs=Object.values(f.session.record.initialRequests!);
    const q1=refs.find(r=>r.id==='q1')!,replica=refs.find(r=>r.id==='r')!;
    f.ports.tenants.dispatch=async(_view,frame)=>frame.type==='query.subscribe'?[JSON.stringify({type:'query.progress',id:frame.id,path:frame.path,throughRevision:{epoch:'e',sequence:999}})]:[];
    const watermark=f.session.record.watermark,sentRevision=f.session.record.sentRevision;
    const facts=await f.session.recoverReplica('t',{v:1,instance:'recovered',shared:[],sockets:{},recovery:{reason:'change-feed-lagged'},recoveryInitials:{c:[q1,replica,{...q1,generation:777,id:'unknown'}]}});
    expect(facts).toEqual([{generation:q1.generation,status:'enqueued'}]);
    expect(f.session.record.liveQueries?.q1?.args).toEqual({id:'q1'});expect(f.session.record.liveQueries?.q2?.args).toEqual({id:'q2'});
    expect(f.session.record.initialRequests?.[String(replica.generation)]).toEqual(replica);
    expect(f.session.record.watermark).toBe(watermark);expect(f.session.record.sentRevision).toBe(sentRevision);
  });

  it("0070 sink failure yields no completion fact and preserves the exact pending book for a fresh retry",async()=>{
    const f=fixture();await f.auth();await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{}}');
    const ref=Object.values(f.session.record.initialRequests!)[0]!;
    f.ports.tenants.dispatch=async(_view,frame)=>frame.type==='query.subscribe'?[JSON.stringify({type:'query.result',id:frame.id,path:frame.path,reason:'initial',result:[]})]:[];
    const payload:ReplicaDelivery={v:1,instance:'recovered',shared:[],sockets:{},recovery:{reason:'change-feed-lagged'},recoveryInitials:{c:[ref]}};
    f.fail.send=true;await expect(f.session.recoverReplica('t',payload)).rejects.toThrow('sink unavailable');
    expect(f.session.record.initialRequests?.[String(ref.generation)]).toEqual(ref);expect(f.session.record.pendingLiveQueries?.q).toHaveLength(1);
    f.fail.send=false;expect(await f.session.recoverReplica('t',payload)).toEqual([{generation:ref.generation,status:'enqueued'}]);
    expect(f.session.record.initialRequests?.[String(ref.generation)]).toBeUndefined();
  });

  it("0070 empty bootstrap retains original refs and ordinary old ACKs contain no terminal facts",async()=>{
    const f=fixture();await f.auth();await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{"value":"B"}}');
    const ref=Object.values(f.session.record.initialRequests!)[0]!;
    f.ports.tenants.dispatch=async(_view,frame)=>frame.type==='query.subscribe'?[JSON.stringify({type:'query.progress',id:frame.id,path:frame.path,throughRevision:{epoch:'e',sequence:2}})]:[];
    const work={ops:[] as const,subscriptionEpoch:ref.epoch,recoveryInitials:[ref]};
    expect(await f.session.bootstrapReplica('t',{v:1,scopeFence:1,instance:'new',shared:[],sockets:{c:work}},work)).toEqual([{generation:ref.generation,status:'enqueued'}]);
    expect(f.session.record.liveQueries?.q?.args).toEqual({value:'B'});
    expect(await f.session.recoverReplica('t',{v:1,instance:'newer',shared:[],sockets:{},recovery:{reason:'change-feed-lagged'}})).toBeUndefined();
  });

  it("recovery waits for real LQ replies, includes pending opens, and rejects stale scope or a refused rebuild", async () => {
    const f = fixture(); await f.auth();
    await f.session.receive('{"type":"query.subscribe","id":"q","path":"items.live","args":{}}');
    f.session.delivery.opening("pending-replica", "items.all");
    const recovery: RecoveryDelivery = { v: 1, instance: "new", shared: [], sockets: {}, recovery: { reason: "change-feed-lagged" } };
    f.ports.tenants.dispatch = async (socket, frame) => {
      f.dispatches.push(socket as RecoverySocketView);
      if (frame.type === "replica.openMany" && !frame.opens.length) return [];
      return ['{"type":"query.error","id":"q","path":"items.live","error":"visibility unavailable"}'];
    };
    await expect(f.session.recoverReplica("t", recovery)).rejects.toThrow("live query recovery was refused");
    expect(f.sent.some((text) => text.includes('"id":"pending-replica"') && text.includes('"reason":"change-feed-lagged"'))).toBe(true);
    expect(f.dispatches.at(-1)?.queryRecovery).toBe(true);
    // Queue an auth before recovery: capturing the old epoch must not reset the new Member.
    const auth = f.auth("bob"), stale = f.session.recoverReplica("t", recovery);
    await auth;
    await expect(stale).rejects.toThrow("delivery recovery scope changed");
  });
});
