// #554: the gateway absorbs a tenant's busy reply. Refused opens wait in the socket's ordered queue and go again with
// their initial refs; the client never sees the busy reply; after BUSY_MAX_ATTEMPTS they go without `acceptsBusy`.
// Synthetic ports and a deterministic clock (as stall.test.ts).
import { describe, expect, it } from "vitest";
import { Session } from "../session";
import type { Ports, SocketView, TenantSession } from "../ports";
import { BUSY_FRAME_TYPE, BUSY_MAX_ATTEMPTS } from "../../delivery/protocol";

function tenant(name: string, id = "t"): TenantSession {
  return {
    identity: { projectId: "p", account: { id: name, email: "", emailVerified: false, name, avatarUrl: "", provider: "" } },
    route: { projectId: "p", tenantId: id, databaseUrl: `sqlite:///${id}.db` },
    member: { id: `m-${name}`, accountId: name, status: "active", displayName: name, avatarUrl: "", role: "member", permissions: {}, membershipRevision: 1 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}

type Call = { type: string; ids: string[]; acceptsBusy: boolean; initials: string[] };
function fixture(busyRounds: number) {
  let clock = 1_000_000, round = 0;
  const sent: string[] = [], calls: Call[] = [];
  const timers: Array<{ at: number; fn: () => void; cancelled: boolean }> = [];
  const ports: Ports = {
    control: {
      project: async () => ({ ok: true, value: null }),
      loadSessionIdentity: async (name) => ({ ok: true, value: tenant(name).identity }),
      authenticateSession: async (name, _project, id) => ({ ok: true, value: tenant(name, id) }),
      authenticateImpersonation: async () => ({ ok: false, kind: "InvalidSession", message: "invalid" }),
      resolvePublicTenant: async () => ({ ok: false, kind: "Other", message: "disabled" }),
      revalidate: async (items) => items.map(() => true), dispatch: async () => [],
    },
    modules: { module: async () => ({ artifactHash: "v1", clientContract: null }), epoch: async () => "db-epoch" },
    tenants: {
      // A tenant with a full backlog: it queues the first open of a frame (none of a single one) and answers the rest busy,
      // `busyRounds` times.
      dispatch: async (socket: SocketView, frame) => {
        const opens = frame.type === "replica.openMany" ? frame.opens : frame.type === "replica.open" ? [frame] : [];
        const acceptsBusy = (socket as SocketView & { acceptsBusy?: boolean }).acceptsBusy === true;
        if (opens.length) calls.push({ type: frame.type, ids: opens.map((o) => o.id), acceptsBusy, initials: (socket.initialRequests ?? []).map((r) => r.id) });
        if (!opens.length) return frame.type === "reducer.call" ? [JSON.stringify({ type: "reducer.result", id: frame.id, result: null })] : [];
        if (!acceptsBusy || round++ >= busyRounds) return [];
        const refused = opens.length > 1 ? opens.slice(1) : opens;
        return [JSON.stringify({ type: BUSY_FRAME_TYPE, ids: refused.map((o) => o.id), retryAfterMs: 100 })];
      },
      connectionReset: async () => {},
    },
  };
  const session = Session.open({
    ports, host: "holder", connectionId: "c", publicTransport: false,
    ready: { runtimeVersion: "test", presence: false },
    sink: { send: (text) => { sent.push(text); }, close: () => {} },
    now: () => clock,
    timers: { later: (ms, fn) => { const t = { at: clock + ms, fn, cancelled: false }; timers.push(t); return () => { t.cancelled = true; }; } },
  });
  const advance = async (ms: number) => {
    for (let step = 0; step < ms; step += 10) {
      clock += 10;
      for (const t of timers.filter((t) => !t.cancelled && t.at <= clock)) { t.cancelled = true; t.fn(); }
      for (let i = 0; i < 20; i++) await Promise.resolve();
    }
  };
  return { session, sent, calls, advance };
}

describe("#554 the gateway absorbs a tenant's busy reply", () => {
  it("resends the refused opens with their initial refs, in the socket's order, and never shows busy to the client", async () => {
    const f = fixture(1);
    await f.session.receive(JSON.stringify({ type: "auth", id: "a1", project: "p", tenant: "t", token: "alice" }));
    const opened = f.session.receive(JSON.stringify({ type: "replica.openMany", opens: ["x", "y", "z"].map((id) => ({ id, path: "items.all", args: {} })) }));
    // A later frame of the same socket waits behind the held opens (order kept).
    const later = f.session.receive(JSON.stringify({ type: "reducer.call", id: "r1", path: "tasks.create", args: {} }));
    for (let i = 0; i < 20; i++) await Promise.resolve();
    expect(f.calls).toEqual([{ type: "replica.openMany", ids: ["x", "y", "z"], acceptsBusy: true, initials: ["x", "y", "z"] }]);
    expect(f.sent.some((t) => t.includes(BUSY_FRAME_TYPE))).toBe(false);
    expect(f.sent.some((t) => t.includes('"id":"r1"'))).toBe(false);
    await f.advance(200);
    await opened; await later;
    expect(f.calls[1]).toEqual({ type: "replica.openMany", ids: ["y", "z"], acceptsBusy: true, initials: ["y", "z"] });
    expect(f.calls).toHaveLength(2);
    expect(f.sent.some((t) => t.includes(BUSY_FRAME_TYPE))).toBe(false);
    expect(f.sent.some((t) => t.includes('"id":"r1"'))).toBe(true);
  });

  it(`stops advertising acceptsBusy after ${BUSY_MAX_ATTEMPTS} attempts, so the tenant must queue them`, async () => {
    const f = fixture(1_000);
    await f.session.receive(JSON.stringify({ type: "auth", id: "a1", project: "p", tenant: "t", token: "alice" }));
    const opened = f.session.receive(JSON.stringify({ type: "replica.openMany", opens: ["x", "y", "z"].map((id) => ({ id, path: "items.all", args: {} })) }));
    await f.advance(2_000);
    await opened;
    expect(f.calls.map((c) => c.acceptsBusy)).toEqual([...Array(BUSY_MAX_ATTEMPTS).fill(true), false]);
    expect(f.calls.at(-1)!.ids).toEqual(["z"]); // each busy round queued its first open; the last goes unconditionally
    expect(f.sent.some((t) => t.includes(BUSY_FRAME_TYPE))).toBe(false);
  });
});
