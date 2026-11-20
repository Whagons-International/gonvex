// #554 (flag `GONVEX_TENANT_BACKLOG_UNITS`, default off): a tenant bounds the opens it queues. Opens beyond the budget
// come back to a gateway that advertised `acceptsBusy` as one busy reply; nothing changes with the flag off or for a
// gateway that does not advertise it. The engine's invocation queue never runs here, so queued opens stay pending.
import { describe, expect, it } from "vitest";
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
const socket = (id: string, acceptsBusy: boolean): SocketView => ({
  connectionId: id, connection: { projectId: "p", identity: tenant.identity, tenant, impersonationId: "", authToken: "" },
  tenant, negotiated: NEGOTIATED, publicTransport: false, host: "h1", replica: { scope: "s", visibilityScope: "v" }, subscriptionEpoch: 0,
  ...(acceptsBusy ? { acceptsBusy: true } : {}),
} as SocketView);
const openMany = (n: number, prefix = "o"): ClientFrame => ({ type: "replica.openMany", opens: Array.from({ length: n }, (_, i) => ({ id: `${prefix}${i}`, path: "items.all", args: {} })) } as ClientFrame);
const engine = (backlogUnits?: number) => new ReplicaDeliveryEngine({
  tenantId: "t", serial: () => new Promise(() => {}), read: async () => { throw new Error("not used"); }, module: () => undefined,
  outlet: new LocalOutlet(() => ({ v: 1, sockets: 0 }), () => []), ...(backlogUnits === undefined ? {} : { backlogUnits }),
});

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
});
