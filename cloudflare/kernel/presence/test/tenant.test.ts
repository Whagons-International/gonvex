/**
 * `TenantPresence` with fake gateways: the ordered per-gateway channel
 * (snapshot before patch, merging behind an in-flight delivery, resync past
 * the pending limit or after a failed delivery), the rebuild after a memory
 * loss, the sweep of a gateway that lost its sockets, and the alarm for
 * grace ends. Timing uses a short window and grace on real timers.
 */
import { describe, expect, it } from "vitest";
import { DEFAULT_PRESENCE_CONFIG, type ConnectionReport, type PresenceConfig } from "../hub";
import { type PresenceDelivery, type PresenceGatewayLink, TenantPresence, resyncFrame } from "../tenant";
import type { PresenceSocketView } from "../socket";
import type { TenantSession } from "../../session/ports";

const TENANT = "t1", PROJECT = "p1";
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function session(memberId: string, over: Partial<TenantSession> = {}): TenantSession {
  return {
    identity: { projectId: PROJECT, account: { id: `acc-${memberId}`, email: "", emailVerified: false, name: "", avatarUrl: "", provider: "" } },
    route: { projectId: PROJECT, tenantId: TENANT, databaseUrl: "" },
    member: { id: memberId, accountId: `acc-${memberId}`, status: "active", displayName: "", avatarUrl: "", role: "admin", permissions: null, membershipRevision: 1 },
    admissionRevision: 1, delegation: null, developerSession: null, anonymous: false, ...over,
  };
}
const socket = (connectionId: string, memberId: string, host = "gw-a"): PresenceSocketView => ({ connectionId, host, tenant: session(memberId) });

/** A gateway that records deliveries, can hold them (in flight), fail, and report connections. */
class FakeGateway implements PresenceGatewayLink {
  deliveries: PresenceDelivery[] = [];
  reports: ConnectionReport[] = [];
  hold: Promise<void> | null = null;
  fail = false;
  async presence(delivery: PresenceDelivery) {
    if (this.hold) await this.hold;
    if (this.fail) throw new Error("gateway unavailable");
    this.deliveries.push(delivery);
    return { v: 1, subscribers: 0 };
  }
  async presenceConnections(): Promise<ConnectionReport[]> { return this.reports; }
  /** What one subscribed socket with subscription `id` would have shown, in order. */
  frames(id: string, wasLive = false): string[] {
    // As the gateway does: patches reach a socket only once its snapshot did (`PresenceRecord.live`).
    const out: string[] = [];
    let live = wasLive;
    for (const d of this.deliveries) {
      for (const s of d.snapshots ?? []) if (s.id === id) { out.push(s.frame); live = true; }
      if (d.resync) { out.push(resyncFrame(id, d.resync)); live = true; } else if (d.patch && live) out.push(d.patch);
    }
    return out;
  }
}

function setup(over: Partial<PresenceConfig> = {}, extra: { lostPending?: boolean; gateways?: string[] } = {}) {
  const gateways = new Map<string, FakeGateway>();
  const gw = (name: string) => { let g = gateways.get(name); if (!g) gateways.set(name, g = new FakeGateway()); return g; };
  const alarms: number[] = [];
  const presence = new TenantPresence({
    tenantId: TENANT,
    config: { ...DEFAULT_PRESENCE_CONFIG, tickMs: 40, offlineGraceMs: 300, ...over },
    gateway: gw,
    registeredGateways: async () => extra.gateways ?? [...gateways.keys()],
    armAlarm: (at) => alarms.push(at),
    ...(extra.lostPending === undefined ? {} : { lostPending: async () => extra.lostPending! }),
  });
  return { presence, gw, alarms };
}

const types = (frames: string[]) => frames.map(f => { const m = JSON.parse(f); return m.type === "presence.patch" ? m.changes.map((c: any) => `${c.memberId}:${c.state}`).join(",") : `snapshot[${m.members.map((c: any) => `${c.memberId}:${c.state}`).join(",")}]`; });

describe("TenantPresence", () => {
  it("answers subscribe on the channel, then publishes the leading edge at once", async () => {
    const { presence, gw } = setup();
    expect(await presence.frame(socket("v", "viewer"), { type: "presence.subscribe", id: "s1" })).toEqual([]);
    await presence.frame(socket("c1", "m1"), { type: "presence.set", state: "active" });
    await presence.flushed();
    expect(types(gw("gw-a").frames("s1"))).toEqual(["snapshot[]", "m1:active"]);
    // One delivery per publication per gateway: the patch is encoded once by the tenant.
    expect(gw("gw-a").deliveries.at(-1)!.patch).toContain('"presence.patch"');
  });

  it("refuses ineligible sessions as Rust does", async () => {
    const { presence } = setup();
    const refuse = (tenant: TenantSession | null) => presence.frame({ connectionId: "x", host: "gw-a", tenant }, { type: "presence.subscribe", id: "s" });
    const error = '{"type":"presence.error","id":"s","error":"presence requires an authenticated tenant member session"}';
    expect(await refuse(null)).toEqual([error]);
    expect(await refuse(session("", {}))).toEqual([error]);
    expect(await refuse(session("_gonvex_service:svc"))).toEqual([error]);
    expect(await refuse(session("m1", { delegation: { by: "x" } }))).toEqual([error]);
    expect(await refuse(session("m1", { anonymous: true }))).toEqual([error]);
    const off = setup({ enabled: false }).presence;
    expect(await off.frame(socket("x", "m1"), { type: "presence.subscribe", id: "s" })).toEqual(['{"type":"presence.error","id":"s","error":"presence v2 is disabled"}']);
  });

  it("coalesces inside the window and delivers to every gateway with a subscriber once", async () => {
    const { presence, gw } = setup();
    await presence.frame(socket("v1", "viewer", "gw-a"), { type: "presence.subscribe", id: "a" });
    await presence.frame(socket("v2", "viewer", "gw-b"), { type: "presence.subscribe", id: "b" });
    await presence.frame(socket("v3", "viewer", "gw-b"), { type: "presence.subscribe", id: "b2" });
    await presence.frame(socket("c1", "m1"), { type: "presence.set", state: "active" });
    await presence.frame(socket("c1", "m1"), { type: "presence.set", state: "idle" });
    await presence.frame(socket("c2", "m2"), { type: "presence.set", state: "active" });
    await sleep(80);
    await presence.flushed();
    expect(types(gw("gw-a").frames("a"))).toEqual(["snapshot[]", "m1:active", "m1:idle,m2:active"]);
    expect(types(gw("gw-b").frames("b"))).toEqual(["snapshot[]", "m1:active", "m1:idle,m2:active"]);
    expect(gw("gw-b").deliveries.filter(d => d.patch).length).toBe(2);
  });

  it("merges publications queued behind an in-flight delivery, and a later snapshot still precedes them", async () => {
    const { presence, gw } = setup({ tickMs: 1 });
    await presence.frame(socket("v1", "viewer"), { type: "presence.subscribe", id: "s1" });
    await presence.flushed();
    let release!: () => void;
    gw("gw-a").hold = new Promise<void>(r => { release = r; });
    await presence.frame(socket("c1", "m1"), { type: "presence.set", state: "active" }); // in flight, held
    for (const state of ["idle", "active", "idle"]) {
      await sleep(3);
      await presence.frame(socket("c1", "m1"), { type: "presence.set", state });
    }
    await sleep(3);
    await presence.frame(socket("v2", "viewer"), { type: "presence.subscribe", id: "s2" });
    gw("gw-a").hold = null;
    release();
    await sleep(5);
    await presence.flushed();
    expect(types(gw("gw-a").frames("s1"))).toEqual(["snapshot[]", "m1:active", "m1:idle"]);
    expect(types(gw("gw-a").frames("s2"))).toEqual(["snapshot[m1:idle]", "m1:idle"]);
    expect(presence.stats.merged).toBe(2);
  });

  it("resynchronizes a gateway past the pending limit and after a failed delivery", async () => {
    const { presence, gw } = setup({ tickMs: 1, maxPendingBatches: 2 });
    await presence.frame(socket("v1", "viewer"), { type: "presence.subscribe", id: "s1" });
    await presence.flushed();
    let release!: () => void;
    gw("gw-a").hold = new Promise<void>(r => { release = r; });
    for (const [i, state] of ["active", "idle", "active", "idle", "active"].entries()) {
      await presence.frame(socket(`c${i}`, `m${i}`), { type: "presence.set", state });
      await sleep(3);
    }
    gw("gw-a").hold = null;
    release();
    await sleep(5);
    await presence.flushed();
    const last = gw("gw-a").deliveries.at(-1)!;
    expect(last.resync?.members.map(m => m.memberId)).toEqual(["m0", "m1", "m2", "m3", "m4"]);
    expect(last.patch).toBeUndefined();

    gw("gw-a").fail = true;
    await presence.frame(socket("c9", "m9"), { type: "presence.set", state: "idle" });
    await presence.flushed();
    const before = presence.stats.deliveries;
    await sleep(3);
    await presence.frame(socket("c9", "m9"), { type: "presence.set", state: "active" });
    await presence.flushed();
    // No retry storm: nothing is attempted inside the retry delay.
    expect(presence.stats.deliveries).toBe(before);
    gw("gw-a").fail = false;
    await sleep(1_100);
    await presence.flushed();
    expect(gw("gw-a").deliveries.at(-1)!.resync?.members.find(m => m.memberId === "m9")?.state).toBe("active");
  });

  it("a disconnect publishes offline after the grace and puts the grace end on the alarm", async () => {
    const { presence, gw, alarms } = setup({ offlineGraceMs: 1_200 });
    await presence.frame(socket("v", "viewer"), { type: "presence.subscribe", id: "s" });
    await presence.frame(socket("c1", "m1"), { type: "presence.set", state: "active" });
    const left = Date.now();
    await presence.leave("c1");
    expect(alarms.length).toBe(1);
    expect(alarms[0]! - left).toBeGreaterThanOrEqual(1_150);
    await sleep(600);
    await presence.flushed();
    expect(types(gw("gw-a").frames("s"))).toEqual(["snapshot[]", "m1:active"]);
    await sleep(800);
    await presence.flushed();
    expect(types(gw("gw-a").frames("s"))).toEqual(["snapshot[]", "m1:active", "m1:offline"]);
    // The alarm firing late is harmless (at-least-once).
    await presence.alarm();
    await presence.flushed();
    expect(gw("gw-a").frames("s")).toHaveLength(3);
  });

  it("rebuilds from the gateways after a memory loss without a snapshot when nothing was pending", async () => {
    const { presence, gw } = setup({}, { lostPending: false, gateways: ["gw-a", "gw-b"] });
    gw("gw-a").reports = [
      { connectionId: "c1", projectId: PROJECT, memberId: "m1", state: "active", subscriptionId: "s1", since: 10 },
      { connectionId: "trigger", projectId: PROJECT, memberId: "m3", state: "idle", subscriptionId: null },
    ];
    gw("gw-b").reports = [{ connectionId: "c2", projectId: PROJECT, memberId: "m2", state: "idle", subscriptionId: null, since: 20 }];
    // The triggering socket's own event applies after the rebuild, so it is published.
    await presence.frame(socket("trigger", "m3"), { type: "presence.set", state: "idle" });
    await presence.flushed();
    expect(presence.stats.rebuilds).toBe(1);
    expect(await presence.published(PROJECT)).toEqual([
      { memberId: "m1", state: "active", since: 10 }, { memberId: "m2", state: "idle", since: 20 }, { memberId: "m3", state: "idle", since: expect.any(Number) },
    ]);
    expect(types(gw("gw-a").frames("s1", true))).toEqual(["m3:idle"]);
  });

  it("resynchronizes subscribers when the memory loss may have dropped a pending grace", async () => {
    const { presence, gw } = setup({}, { lostPending: true, gateways: ["gw-a"] });
    gw("gw-a").reports = [{ connectionId: "c1", projectId: PROJECT, memberId: "m1", state: "active", subscriptionId: "s1" }];
    await presence.frame(socket("c9", "m9"), { type: "presence.set", state: "active" });
    await presence.flushed();
    expect(types(gw("gw-a").frames("s1"))[0]).toBe("snapshot[m1:active]");
    // An alarm that fires on a cold object counts as a lost grace too.
    const cold = setup({}, { gateways: ["gw-a"] });
    cold.gw("gw-a").reports = [{ connectionId: "c1", projectId: PROJECT, memberId: "m1", state: "idle", subscriptionId: "s1" }];
    await cold.presence.alarm();
    await cold.presence.flushed();
    expect(types(cold.gw("gw-a").frames("s1"))).toEqual(["snapshot[m1:idle]"]);
  });

  it("a leave or an explicit offline that wakes a cold object still publishes the Member's offline", async () => {
    // The closed socket is no longer on its gateway; its previous state comes with the leave.
    const leave = setup({ offlineGraceMs: 50 }, { gateways: ["gw-a"] });
    leave.gw("gw-a").reports = [{ connectionId: "v", projectId: PROJECT, memberId: "viewer", state: null, subscriptionId: "s" }];
    await leave.presence.leave("c1", "gw-a", { connectionId: "c1", projectId: PROJECT, memberId: "m1", state: "active", subscriptionId: null, since: 5 });
    await sleep(120);
    await leave.presence.flushed();
    expect(types(leave.gw("gw-a").frames("s", true))).toEqual(["m1:offline"]);
    // `presence.set offline`: the gateway's record already shows no state, the previous one is active.
    const off = setup({}, { gateways: ["gw-a"] });
    off.gw("gw-a").reports = [{ connectionId: "v", projectId: PROJECT, memberId: "viewer", state: null, subscriptionId: "s" }];
    await off.presence.frame({ ...socket("c1", "m1"), presencePrevious: { connectionId: "c1", projectId: PROJECT, memberId: "m1", state: "active", subscriptionId: null } }, { type: "presence.set", state: "offline" });
    await off.presence.flushed();
    expect(types(off.gw("gw-a").frames("s", true))).toEqual(["m1:offline"]);
  });

  it("sweeps connections a restarted gateway no longer holds", async () => {
    const { presence, gw } = setup({ offlineGraceMs: 50 });
    await presence.frame(socket("v", "viewer", "gw-b"), { type: "presence.subscribe", id: "s" });
    await presence.frame(socket("c1", "m1", "gw-a"), { type: "presence.set", state: "active" });
    gw("gw-b").reports = [{ connectionId: "v", projectId: PROJECT, memberId: "viewer", state: null, subscriptionId: "s" }];
    gw("gw-a").reports = []; // gw-a restarted: its socket is gone without a close event
    await sleep(5);
    await presence.alarm(); // the alarm (or the first event a minute later) sweeps
    await sleep(100);
    await presence.flushed();
    expect(presence.stats.swept).toBe(1);
    expect(types(gw("gw-b").frames("s"))).toEqual(["snapshot[]", "m1:active", "m1:offline"]);
  });
});
