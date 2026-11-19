/**
 * X02: a gateway's outgoing calls never inherit a tenant RPC's subrequest depth.
 *
 * The first block reproduces the 2026-11-17 dev failure on a model of the platform
 * rule (workerd `IoContext::getCurrentIncomingRequest()` = the newest alive incoming
 * request; a finished request stays the front until a newer one arrives): a client
 * opening subscriptions one frame at a time, a tenant that answers each dispatch with
 * an initial-delivery RPC, and the gateway's serialized frame queue. Without the
 * gate the depth ratchets with every frame until the platform refuses the call.
 */
import { describe, expect, it } from "vitest";
import { CallOrigin, FRESH_ENTRIES, gatedEnv, gatedStub } from "../placement/origin";

const LIMIT = 16;
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

/** One object under the platform rule. */
class ModelObject {
  private alive: Array<{ depth: number; running: boolean }> = [];
  maxOutgoing = 0;

  /** The platform delivers a request at `depth`; the handler runs while it is alive. */
  async deliver<T>(depth: number, handler: () => Promise<T>): Promise<T> {
    // A newer request ends the drain of every finished one.
    this.alive = this.alive.filter((request) => request.running);
    const request = { depth, running: true };
    this.alive.push(request);
    try { return await handler(); }
    finally {
      request.running = false;
      // Only the newest finished request stays (draining) until a newer one arrives.
      if (this.alive[this.alive.length - 1] !== request) this.alive = this.alive.filter((r) => r !== request);
    }
  }

  /** Depth of a call made now: one below the newest alive request. */
  outgoing(): number {
    const front = this.alive[this.alive.length - 1];
    if (!front) throw new Error("no current incoming request");
    const depth = front.depth + 1;
    if (depth > LIMIT) throw new Error("Subrequest depth limit exceeded");
    this.maxOutgoing = Math.max(this.maxOutgoing, depth);
    return depth;
  }
}

interface Session { gateway: ModelObject; tenant: ModelObject; origin?: CallOrigin; failures: string[] }

function world(gated: boolean): Session {
  const gateway = new ModelObject(), tenant = new ModelObject();
  const session: Session = { gateway, tenant, failures: [] };
  if (gated) {
    // The wake is the gateway's alarm: a fresh platform event.
    session.origin = new CallOrigin(() => { setTimeout(() => void freshEvent(session, async () => {}), 0); });
  }
  return session;
}

/** A platform event on the gateway that starts a new chain (client frame, alarm). */
function freshEvent<T>(s: Session, handler: () => Promise<T>): Promise<T> {
  return s.gateway.deliver(0, async () => {
    const entry = s.origin?.enter("fresh");
    try { return await handler(); } finally { if (entry) s.origin!.exit(entry); }
  });
}

/** The tenant's initial delivery RPC to the gateway, sent from the tenant's current context. */
function deliverInitial(s: Session): void {
  let depth: number;
  try { depth = s.tenant.outgoing(); } catch (error) { s.failures.push(String(error)); return; }
  void tick().then(() => s.gateway.deliver(depth, async () => {
    const entry = s.origin?.enter("peer");
    try { await tick(); } finally { if (entry) s.origin!.exit(entry); }
  }));
}

/** gateway -> tenant dispatch (replica.open): the tenant enqueues the initial page, then replies. */
function dispatch(s: Session): Promise<void> {
  const send = () => {
    const depth = s.gateway.outgoing();
    return tick().then(() => s.tenant.deliver(depth, async () => {
      deliverInitial(s);
      await tick();
    }));
  };
  const stub = gatedStub({ dispatch: send }, s.origin ?? new CallOrigin(() => {}));
  return s.origin ? stub.dispatch() : send();
}

/** A client that opens `frames` subscriptions at once; the gateway handles them in order (Session.serial). */
async function openMany(s: Session, frames: number): Promise<void> {
  let queue = Promise.resolve();
  const handled: Array<Promise<void>> = [];
  for (let i = 0; i < frames; i++) {
    handled.push(freshEvent(s, () => {
      const run = async () => { try { await dispatch(s); } catch (error) { s.failures.push(String(error)); } };
      const next = queue.then(run);
      queue = next;
      return next;
    }));
  }
  await Promise.all(handled);
  for (let i = 0; i < 10; i++) await tick();
}

describe("X02 subrequest depth (platform model)", () => {
  it("reproduces the ratchet without the gate: the depth grows with every frame until the platform refuses", async () => {
    const s = world(false);
    await openMany(s, 40);
    expect(s.failures.length).toBeGreaterThan(0);
    expect(s.failures[0]).toContain("Subrequest depth limit exceeded");
  });

  it("keeps every gateway call one hop below a fresh event, however many frames and deliveries", async () => {
    const s = world(true);
    await openMany(s, 200);
    expect(s.failures).toEqual([]);
    expect(s.gateway.maxOutgoing).toBe(1);
    expect(s.tenant.maxOutgoing).toBe(2);
    expect(s.origin!.stats().waiting).toBe(0);
    // The tenant's deliveries were the newest request for most frames: those calls waited for a fresh event.
    expect(s.origin!.stats().deferred).toBeGreaterThan(0);
  });
});

describe("X02 CallOrigin front model", () => {
  it("lets a call start at once only while a fresh event can be the platform's front", () => {
    const origin = new CallOrigin(() => {});
    expect(origin.safe()).toBe(false); // nothing seen yet: unknown front
    const frame = origin.enter("fresh");
    expect(origin.safe()).toBe(true);
    const delivery = origin.enter("peer");
    expect(origin.safe()).toBe(false); // the newer tenant RPC is the front
    origin.exit(delivery);
    expect(origin.safe()).toBe(false); // finished, but still the front until a newer request
    const next = origin.enter("fresh");
    expect(origin.safe()).toBe(true);
    origin.exit(next);
    expect(origin.safe()).toBe(true); // newest finished request is fresh; the older one is still running
    origin.exit(frame);
    expect(origin.safe()).toBe(true);
  });

  it("an older running tenant RPC cannot be the front once a newer fresh event is running", () => {
    const origin = new CallOrigin(() => {});
    origin.enter("peer");
    expect(origin.safe()).toBe(false);
    origin.enter("fresh");
    expect(origin.safe()).toBe(true);
  });

  it("holds calls in order until a fresh event and wakes once", async () => {
    let wakes = 0;
    const origin = new CallOrigin(() => { wakes++; });
    origin.enter("peer");
    const order: number[] = [];
    const a = origin.run(() => { order.push(1); return 1; });
    const b = origin.run(() => { order.push(2); return 2; });
    await tick();
    expect(order).toEqual([]);
    expect(wakes).toBe(1);
    expect(origin.stats()).toMatchObject({ immediate: 0, deferred: 2, waiting: 2, expired: 0, oldest: { label: "call" } });
    origin.enter("fresh");
    expect(order).toEqual([1, 2]); // launched synchronously inside the fresh event's turn
    expect(await Promise.all([a, b])).toEqual([1, 2]);
    // A call made while others wait keeps its place behind them.
    origin.enter("peer");
    const c = origin.run(() => 3);
    origin.enter("fresh");
    expect(await c).toBe(3);
    expect(wakes).toBe(2);
  });

  it("propagates a refused call to its caller", async () => {
    const origin = new CallOrigin(() => {});
    origin.enter("fresh");
    await expect(origin.run(() => { throw new Error("refused"); })).rejects.toThrow("refused");
    await expect(origin.run(() => Promise.reject(new Error("remote")))).rejects.toThrow("remote");
  });
});

describe("X02 gatedEnv", () => {
  it("gates an RPC-enabled service binding, which answers every member name with a callable", async () => {
    const calls: string[] = [];
    // Like a workerd RPC Fetcher: any property, `fetch`, `idFromName` and `get` included, is a remote method.
    const service = new Proxy({}, { get: (_t, prop) => typeof prop === "symbol" || prop === "then" ? undefined : (...args: unknown[]) => { calls.push(String(prop)); return Promise.resolve(args.length); } });
    const namespace = { idFromName: (name: string) => ({ name }), get: () => ({ ping: () => { calls.push("ns.ping"); return Promise.resolve(0); } }) };
    const origin = new CallOrigin(() => {});
    const env = gatedEnv({ SERVICE: service, TENANT: namespace, NAME: "plain" }, origin);
    origin.enter("peer");
    const rpc = (env.SERVICE as unknown as { report(n: number): Promise<number> }).report(1);
    const viaNamespace = (env.TENANT.get() as { ping(): Promise<number> }).ping();
    expect(env.TENANT.idFromName("t")).toEqual({ name: "t" }); // id helpers stay local and synchronous
    await tick();
    expect(calls).toEqual([]);
    expect(origin.stats().waiting).toBe(2);
    origin.enter("fresh");
    expect(calls).toEqual(["report", "ns.ping"]);
    expect(await rpc).toBe(1);
    expect(await viaNamespace).toBe(0);
    expect(env.NAME).toBe("plain");
  });
});

describe("X07b BUG-5 on -12: a deferred gateway call never strands its socket", () => {
  function clock() {
    let now = 0;
    const timers: Array<{ at: number; fn: () => void; off: boolean }> = [];
    const later = (ms: number, fn: () => void) => { const t = { at: now + ms, fn, off: false }; timers.push(t); return () => { t.off = true; }; };
    const advance = (ms: number) => {
      const end = now + ms;
      for (;;) {
        const next = timers.filter((t) => !t.off && t.at <= end).sort((a, b) => a.at - b.at)[0];
        if (!next) break;
        now = next.at; next.off = true; next.fn();
      }
      now = end;
    };
    return { now: () => now, later, advance, armed: () => timers.filter((t) => !t.off).length };
  }

  it("a call made inside a tenant reply RPC launches on a re-wake when the first wake is lost", async () => {
    const c = clock();
    let wakes = 0;
    // Dev 02:28:55: the reducer call after an Action reply was made while the tenant's
    // `settleActionReply` RPC was the newest request, so it had to wait for a fresh event.
    const origin: CallOrigin = new CallOrigin(() => {
      wakes++;
      if (wakes === 1) return; // the first wake alarm is lost
      const alarm = origin.enter("fresh"); origin.exit(alarm);
    }, { now: c.now, later: c.later });
    const reply = origin.enter("peer");
    let launched = 0;
    const call = origin.run(() => { launched++; return "dispatched"; }, "dispatch");
    origin.exit(reply);
    await tick();
    expect(launched).toBe(0);
    expect(origin.stats()).toMatchObject({ waiting: 1, oldest: { label: "dispatch" } });
    // Before X07b nothing else re-armed the wake: the call waited forever and the socket went silent.
    c.advance(1_000);
    await expect(call).resolves.toBe("dispatched");
    expect(launched).toBe(1);
    expect(origin.stats().waiting).toBe(0);
    expect(c.armed()).toBe(0);
  });

  it("with no fresh event at all the call fails after the deadline, naming the method", async () => {
    const c = clock();
    let wakes = 0;
    const origin = new CallOrigin(() => { wakes++; }, { now: c.now, later: c.later });
    const tenant = gatedStub({ dispatch: async () => ["never"] }, origin);
    const reply = origin.enter("peer");
    const call = tenant.dispatch();
    origin.exit(reply);
    let error: unknown;
    void call.catch((e) => { error = e; });
    c.advance(9_999);
    await tick();
    expect(error).toBeUndefined();
    expect(wakes).toBeGreaterThanOrEqual(9);
    c.advance(1);
    await tick();
    expect(String((error as Error)?.message)).toMatch(/gateway call dispatch waited 10000 ms/);
    expect(origin.stats()).toMatchObject({ waiting: 0, expired: 1 });
  });
});

describe("X17e Control notifications start a bounded chain", () => {
  it("controlChanged and presenceChanged are fresh entries; tenant deliveries stay peers", () => {
    expect(FRESH_ENTRIES.has("controlChanged")).toBe(true);
    expect(FRESH_ENTRIES.has("presenceChanged")).toBe(true);
    for (const peer of ["deliverReplica", "deliverReplicaBatch", "deliverReplicaInitial", "deliverInitialFailures", "capacity", "drain"]) expect(FRESH_ENTRIES.has(peer)).toBe(false);
  });
});
