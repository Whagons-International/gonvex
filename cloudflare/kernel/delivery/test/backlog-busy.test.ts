// #554 (flag `GONVEX_TENANT_BACKLOG_UNITS`, default off): a tenant bounds the opens it queues. Opens beyond the budget
// come back to a gateway that advertised `acceptsBusy` as one busy reply; nothing changes with the flag off or for a
// gateway that does not advertise it. The engine's invocation queue never runs here, so queued opens stay pending.
import { describe, expect, it, vi } from "vitest";
import type { Negotiated } from "../../session/capabilities.ts";
import type { ClientFrame } from "../../session/decode.ts";
import type { SocketView, TenantSession } from "../../session/ports.ts";
import { ReplicaDeliveryEngine } from "../engine.ts";
import { LocalOutlet } from "../outlet.ts";
import { BUSY_FRAME_TYPE, busyReply } from "../protocol.ts";

const NEGOTIATED: Negotiated = { snapshotPages: true, noOldValues: true, fieldPatches: false, batchFrames: false, nativeRows: false, setDigest: false, telemetryBatch: false, presence: false };
const tenant: TenantSession = {
  identity: { projectId: "p", account: { id: "acct-a", email: "", emailVerified: false, name: "a", avatarUrl: "", provider: "" } },
  route: { projectId: "p", tenantId: "t", databaseUrl: "sqlite:///t.db" },
  member: { id: "m-a", accountId: "acct-a", status: "active", displayName: "a", avatarUrl: "", role: "admin", permissions: {}, membershipRevision: 0 },
  admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
};
const socket = (id: string, acceptsBusy: boolean, busyRetry = false): SocketView => ({
  connectionId: id, connection: { projectId: "p", identity: tenant.identity, tenant, impersonationId: "", authToken: "" },
  tenant, negotiated: NEGOTIATED, publicTransport: false, host: "h1", replica: { scope: "s", visibilityScope: "v" }, subscriptionEpoch: 0,
  ...(acceptsBusy ? { acceptsBusy: true } : {}), ...(busyRetry ? { busyRetry: true } : {}),
} as SocketView);
const openMany = (n: number, prefix = "o"): ClientFrame => ({ type: "replica.openMany", opens: Array.from({ length: n }, (_, i) => ({ id: `${prefix}${i}`, path: "items.all", args: {} })) } as ClientFrame);
const engine = (backlogUnits?: number, backlogged = false) => {
  const outlet = new LocalOutlet(() => ({ v: 1, sockets: 0 }), () => []);
  if (backlogged) outlet.backlogged = () => true;
  return new ReplicaDeliveryEngine({
    tenantId: "t", serial: () => new Promise(() => {}), read: async () => { throw new Error("not used"); }, module: () => undefined,
    outlet, ...(backlogUnits === undefined ? {} : { backlogUnits }),
  });
};
/** Engine internals the tests set up directly (a holder in recovery or deferred, a path's measured cost). */
type Internals = { recovering: Map<string, unknown>; behind: Map<string, unknown>; openRows: Map<string, number>; requestRecovery(host: string): void };
const internals = (e: ReplicaDeliveryEngine) => e as unknown as Internals;
const open = (id: string, path = "items.all"): ClientFrame => ({ type: "replica.open", id, path, args: {} } as ClientFrame);

describe("#554 tenant backlog bound", () => {
  it("is off by default: every open is queued, no busy reply", () => {
    const e = engine();
    expect(e.dispatch(socket("c1", true), openMany(20))).toEqual([]);
    expect(e.backlogUnits()).toBe(40); // 20 opens x (1 + 100 default rows / 100)
  });

  it("queues opens up to the budget and answers the rest busy to a gateway that advertised it", () => {
    const e = engine(10);
    const out = e.dispatch(socket("c1", true), openMany(20))!;
    const busy = busyReply(out)!;
    expect(busy.type).toBe(BUSY_FRAME_TYPE);
    expect(busy.ids).toEqual(Array.from({ length: 15 }, (_, i) => `o${i + 5}`));
    expect(busy.retryAfterMs).toBeGreaterThanOrEqual(50);
    expect(busy.retryAfterMs).toBeLessThanOrEqual(1_000);
    expect(e.backlogUnits()).toBe(10);
    // Another socket while the backlog is full: everything busy.
    expect(busyReply(e.dispatch(socket("c2", true), openMany(3, "p"))!)!.ids).toEqual(["p0", "p1", "p2"]);
    expect(e.busyRefusals).toBe(18);
  });

  it("never answers busy to a gateway that did not advertise it (deployment skew, or the gateway's last attempt)", () => {
    const e = engine(10);
    expect(e.dispatch(socket("c1", false), openMany(20))).toEqual([]);
    expect(e.backlogUnits()).toBe(40);
    expect(e.busyRefusals).toBe(0);
  });

  it("admits an open on an idle tenant however heavy its path's estimate (#557 R1)", () => {
    const e = engine(10);
    internals(e).openRows.set("items.heavy", 2_000); // 21 units, over the whole budget
    expect(e.dispatch(socket("c1", true), open("h0", "items.heavy"))).toEqual([]);
    expect(e.backlogUnits()).toBe(21);
    // With that backlog queued, the next heavy open waits its turn.
    expect(busyReply(e.dispatch(socket("c2", true), open("h1", "items.heavy"))!)!.ids).toEqual(["h1"]);
  });

  it("refuses every open of a frame for a holder under pressure: recovering, deferred, or backlogged (#557 R3, E1)", () => {
    const paused = engine(1_000);
    internals(paused).recovering.set("h1", { pending: true, retryAt: 0, catchup: false });
    expect(busyReply(paused.dispatch(socket("c1", true), openMany(2))!)!.ids).toEqual(["o0", "o1"]);
    const behind = engine(1_000);
    internals(behind).behind.set("h1", { from: 0, outbox: [], waiting: true, attempts: 0 });
    expect(busyReply(behind.dispatch(socket("c1", true), openMany(2))!)!.ids).toEqual(["o0", "o1"]);
    const backlogged = engine(1_000, true);
    expect(busyReply(backlogged.dispatch(socket("c1", true), openMany(2))!)!.ids).toEqual(["o0", "o1"]);
    expect(backlogged.backlogUnits()).toBe(0);
  });

  it("gives one recovery-barrier allowance per client frame: busy resends (busyRetry) request no recovery (#557 R2)", () => {
    const e = engine(10);
    internals(e).recovering.set("h1", { pending: false, retryAt: 0, catchup: false }); // its last attempt failed
    const requested = vi.spyOn(internals(e), "requestRecovery").mockImplementation(() => {});
    // The client's frame, then the gateway's resends: two busy attempts and the unconditional fallthrough.
    busyReply(e.dispatch(socket("c1", true), openMany(1))!);
    e.dispatch(socket("c1", true, true), openMany(1));
    e.dispatch(socket("c1", true, true), openMany(1));
    e.dispatch(socket("c1", false, true), openMany(1));
    expect(requested).toHaveBeenCalledTimes(1);
  });
});
