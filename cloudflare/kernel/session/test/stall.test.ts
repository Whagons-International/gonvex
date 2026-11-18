// X07 BUG-5: an application socket went silent across a publication (dev, 2026-11-18 01:01-01:12 UTC).
// The server sent no auth.result and no close while the SDK re-sent `auth` every 10 s and its queued
// create hung, until a page reload. A port call that never settled held the socket's ordered queue,
// and every later frame, alarm task and delivery waited behind it. Synthetic ports; deterministic clock.
import { describe, expect, it } from "vitest";
import { Session } from "../session";
import { CLOSE, LIMITS } from "../protocol";
import type { Ports, TenantSession } from "../ports";

function tenant(name: string, id = "t"): TenantSession {
  return {
    identity: { projectId: "p", account: { id: name, email: "", emailVerified: false, name, avatarUrl: "", provider: "" } },
    route: { projectId: "p", tenantId: id, databaseUrl: `sqlite:///${id}.db` },
    member: { id: `m-${name}`, accountId: name, status: "active", displayName: name, avatarUrl: "", role: "member", permissions: {}, membershipRevision: 1 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}

function fixture() {
  let clock = 1_000_000;
  const sent: string[] = [], closes: Array<{ code: number; reason: string }> = [];
  const timers: Array<{ at: number; fn: () => void; cancelled: boolean }> = [];
  let tenantAnswers = true;
  const dispatched: string[] = [];
  const held: Array<(override?: string[]) => void> = [];
  let hold = false;
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
      // A tenant being replaced by a publication: the call is accepted and never answered.
      dispatch: (_socket, frame) => {
        const id = (frame as { id: string }).id;
        dispatched.push(id);
        const reply = [JSON.stringify({ type: "reducer.result", id, result: null })];
        if (hold) return new Promise<string[]>((resolve) => { held.push((override) => resolve(override ?? reply)); });
        return tenantAnswers ? Promise.resolve(reply) : new Promise<string[]>(() => {});
      },
      connectionReset: async () => {},
    },
  };
  const session = Session.open({
    ports, host: "holder", connectionId: "c", publicTransport: false,
    ready: { runtimeVersion: "test", presence: false },
    sink: { send: (text) => { sent.push(text); }, close: (frame) => { closes.push(frame); } },
    now: () => clock,
    timers: { later: (ms, fn) => { const t = { at: clock + ms, fn, cancelled: false }; timers.push(t); return () => { t.cancelled = true; }; } },
  });
  const frames = () => sent.map((text) => JSON.parse(text) as { type: string; id?: string });
  const auth = (id: string) => session.receive(JSON.stringify({ type: "auth", id, project: "p", tenant: "t", token: "alice" }));
  const create = (id: string) => session.receive(JSON.stringify({ type: "reducer.call", id, path: "tasks.create", args: {} }));
  const advance = (ms: number) => {
    clock += ms;
    for (const t of timers.filter((t) => !t.cancelled && t.at <= clock)) { t.cancelled = true; t.fn(); }
  };
  const tick = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
  const settled = async (promise: Promise<unknown>) => {
    let done = false;
    void promise.then(() => { done = true; }, () => { done = true; });
    for (let i = 0; i < 20; i++) await Promise.resolve();
    return done;
  };
  return { session, sent, closes, frames, auth, create, advance, settled, tick, dispatched, held, holdTenant: () => { hold = true; }, stopTenant: () => { tenantAnswers = false; }, now: () => clock };
}

describe("X07 BUG-5 a socket never goes silent across a publication", () => {
  it("closes with a reconnect code once a never-settling port call holds the queue, and releases what waited", async () => {
    const f = fixture();
    await f.auth("login");
    expect(f.frames().at(-1)).toMatchObject({ type: "auth.result", id: "login" });
    await f.create("before");
    expect(f.frames().at(-1)).toMatchObject({ type: "reducer.result", id: "before" });

    // The publication: the socket learns of the new module, then the tenant is replaced mid-call.
    await f.session.moduleReloaded({ artifactHash: "v2", clientContract: null, publicationId: "pub-2" });
    expect(f.frames().at(-1)).toMatchObject({ type: "system.reload" });
    f.stopTenant();
    const queuedCreate = f.create("queued-create");
    await f.tick();

    // The SDK's auth watchdog re-sends `auth` every 10 s on the same open socket.
    const resent: Array<Promise<void>> = [];
    for (let n = 1; n * 10_000 < LIMITS.stallMs; n++) {
      f.advance(10_000);
      resent.push(f.auth(`reauth-${n}`));
      await f.tick();
    }
    // Below the bound nothing is decided yet: no reply and no close (a slow call is not a dead one).
    expect(f.frames().some((frame) => frame.type === "auth.result" && frame.id?.startsWith("reauth"))).toBe(false);
    expect(f.closes).toEqual([]);

    // Before X07 the socket stayed open and silent here for good (dev: ~10 minutes, until a reload).
    f.advance(LIMITS.stallMs);
    expect(f.closes).toEqual([CLOSE.stalled]);
    expect(f.session.closed).toBe(true);
    // The hung create and every queued auth settle, so no frame handler, alarm or delivery stays parked.
    expect(await f.settled(queuedCreate)).toBe(true);
    for (const frame of resent) expect(await f.settled(frame)).toBe(true);
    expect(await f.settled(f.session.liveness(f.now()))).toBe(true);
    expect(f.closes).toHaveLength(1);
  });

  it("a frame that arrives after the bound closes the socket at once, without the in-memory timer", async () => {
    const f = fixture();
    await f.auth("login");
    f.stopTenant();
    const hung = f.create("hung");
    await f.tick();
    f.session["stallTimer"]?.();
    f.session["stallTimer"] = undefined;
    f.advance(LIMITS.stallMs);
    expect(f.closes).toEqual([]);
    const late = f.auth("late");
    expect(f.closes).toEqual([CLOSE.stalled]);
    expect(await f.settled(late)).toBe(true);
    expect(await f.settled(hung)).toBe(true);
  });

  it("the host alarm check closes a stuck queue and schedules itself for the bound", async () => {
    const f = fixture();
    await f.auth("login");
    f.stopTenant();
    const started = f.now();
    void f.create("hung");
    await f.tick();
    expect(f.session.nextWake()).toBeLessThanOrEqual(started + LIMITS.stallMs);
    expect(f.session.checkStall(started + LIMITS.stallMs - 1)).toBe(false);
    expect(f.session.checkStall(started + LIMITS.stallMs)).toBe(true);
    expect(f.closes).toEqual([CLOSE.stalled]);
  });

  it("work that finishes within the bound never closes the socket", async () => {
    const f = fixture();
    await f.auth("login");
    f.advance(LIMITS.stallMs * 3);
    await f.create("later");
    expect(f.frames().at(-1)).toMatchObject({ type: "reducer.result", id: "later" });
    expect(f.session.checkStall(f.now() + LIMITS.stallMs * 2)).toBe(false);
    expect(f.closes).toEqual([]);
  });

  it("review r4608168740: work released by a stall starts no further port call and sends nothing", async () => {
    const f = fixture();
    await f.auth("login");
    f.holdTenant();
    const batch = f.session.receive(JSON.stringify({ type: "reducer.callMany", calls: [
      { id: "first", path: "tasks.create", args: {} }, { id: "second", path: "tasks.create", args: {} },
    ] }));
    await f.tick();
    expect(f.dispatched).toEqual(["first"]);
    f.advance(LIMITS.stallMs);
    expect(f.closes).toEqual([CLOSE.stalled]);
    expect(await f.settled(batch)).toBe(true);
    const before = f.sent.length;
    // The tenant finally answers the first write: the abandoned handler must stop here.
    f.held.shift()!();
    await f.tick();
    expect(f.dispatched).toEqual(["first"]);
    expect(f.sent.length).toBe(before);
  });

  it("review r4608168745: settled queue items leave no stall handler behind", async () => {
    const f = fixture();
    await f.auth("login");
    for (let i = 0; i < 2_000; i++) {
      await f.create(`c${i}`);
      await f.session.liveness(f.now());
    }
    expect(f.session.pendingStallHandlers()).toBe(0);
    const inflight = Array.from({ length: 50 }, (_, i) => f.create(`q${i}`));
    expect(f.session.pendingStallHandlers()).toBeLessThanOrEqual(50 + 1);
    await Promise.all(inflight);
    expect(f.session.pendingStallHandlers()).toBe(0);
  });

  for (const resumes of [true, false]) {
    it(`review r4608249098: an Action reply waiting on a stalled socket settles (dispatch ${resumes ? "completes after" : "never completes after"} the close)`, async () => {
      const f = fixture();
      await f.auth("login");
      f.holdTenant();
      const call = f.session.receive(JSON.stringify({ type: "action.call", id: "act", path: "tasks.generate", args: {} }));
      await f.tick();
      expect(f.dispatched).toEqual(["act"]);
      // The tenant's Action finishes and its reply RPC reaches this socket while the dispatch is still held.
      const generation = f.session.record.actionGenerations?.act;
      const reply = f.session.actionFinished("act", "tasks.generate", f.session.record.subscriptionEpoch ?? 0,
        JSON.stringify({ type: "action.result", id: "act", result: null }), generation);
      await f.tick();
      f.advance(LIMITS.stallMs);
      expect(f.closes).toEqual([CLOSE.stalled]);
      if (resumes) f.held.shift()!([]);
      await f.tick();
      expect(await f.settled(reply)).toBe(true);
      await expect(reply).resolves.toBe("obsolete");
      expect(await f.settled(call)).toBe(true);
    });
  }
});
