// X15: an openMany reply completes many initial requests in one synchronous loop. Each completion used to
// rewrite the socket's whole recovery record (O(subscriptions) per write, O(n^2) per reply); the loop now runs
// inside the host's `batchPersists`, so the host writes once, in the same turn as the frames.
import { describe, expect, it } from "vitest";
import { Session, type SessionHooks } from "../session";
import type { Ports, TenantSession } from "../ports";

function tenant(name: string): TenantSession {
  return {
    identity: { projectId: "p", account: { id: name, email: "", emailVerified: false, name, avatarUrl: "", provider: "" } },
    route: { projectId: "p", tenantId: "t", databaseUrl: "sqlite:///t.db" },
    member: { id: `m-${name}`, accountId: name, status: "active", displayName: name, avatarUrl: "", role: "member", permissions: {}, membershipRevision: 1 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}

function fixture(replies: (opens: Array<{ id: string; path: string }>) => string[], hooks: SessionHooks) {
  const sent: string[] = [];
  const ports: Ports = {
    control: {
      project: async () => ({ ok: true, value: null }),
      loadSessionIdentity: async (name) => ({ ok: true, value: tenant(name).identity }),
      authenticateSession: async (name) => ({ ok: true, value: tenant(name) }),
      authenticateImpersonation: async () => ({ ok: false, kind: "InvalidSession", message: "invalid" }),
      resolvePublicTenant: async () => ({ ok: false, kind: "Other", message: "disabled" }),
      revalidate: async (items) => items.map(() => true), dispatch: async () => [],
    },
    modules: { module: async () => null, epoch: async () => "db-epoch" },
    tenants: {
      dispatch: async (_socket, frame) => frame.type === "replica.openMany" ? replies(frame.opens) : [],
      connectionReset: async () => {},
    },
  };
  const session = Session.open({ ports, host: "holder", connectionId: "c", publicTransport: false,
    ready: { runtimeVersion: "test", presence: false }, sink: { send: (text) => sent.push(text), close: () => {} },
    timers: { later: () => () => {} }, hooks });
  return { session, sent };
}

const ready = (open: { id: string; path: string }) => [
  JSON.stringify({ type: "replica.snapshotPage", id: open.id, path: open.path, rows: [] }),
  JSON.stringify({ type: "replica.ready", id: open.id, path: open.path }),
];

describe("X15 recovery writes per openMany reply", () => {
  it("completes every open of one reply inside a single batchPersists scope", async () => {
    const events: string[] = [];
    let depth = 0;
    const hooks: SessionHooks = {
      persist: () => events.push(depth ? "persist-in-scope" : "persist"),
      batchPersists: (fn) => { depth++; events.push("scope"); try { return fn(); } finally { depth--; events.push("end"); } },
    };
    const f = fixture((opens) => opens.flatMap(ready), hooks);
    await f.session.receive(JSON.stringify({ type: "auth", id: "auth", project: "p", tenant: "t", token: "alice" }));
    events.length = 0;
    const opens = Array.from({ length: 5 }, (_, i) => ({ id: `r${i}`, path: `items.list${i}` }));
    await f.session.receive(JSON.stringify({ type: "replica.openMany", opens }));
    // Five completions, all inside one scope; nothing persisted outside it before the scope ended.
    const scope = events.indexOf("scope"), end = events.indexOf("end");
    expect(scope).toBeGreaterThanOrEqual(0);
    expect(events.slice(scope + 1, end).filter((e) => e === "persist-in-scope")).toHaveLength(5);
    expect(events.filter((e) => e === "scope")).toHaveLength(1);
    expect(f.session.record.initialRequests ?? {}).toEqual({});
    expect(Object.keys(f.session.record.replicas ?? {}).sort()).toEqual(opens.map((o) => o.id).sort());
    expect(f.sent.filter((t) => t.includes('"replica.ready"'))).toHaveLength(5);
  });

  it("still persists each completion when the host has no batching hook (older hosts, tests)", async () => {
    let persists = 0;
    const f = fixture((opens) => opens.flatMap(ready), { persist: () => { persists++; } });
    await f.session.receive(JSON.stringify({ type: "auth", id: "auth", project: "p", tenant: "t", token: "alice" }));
    persists = 0;
    await f.session.receive(JSON.stringify({ type: "replica.openMany", opens: [{ id: "a", path: "x.a" }, { id: "b", path: "x.b" }] }));
    expect(persists).toBeGreaterThanOrEqual(2);
    expect(f.session.record.initialRequests ?? {}).toEqual({});
  });
});
