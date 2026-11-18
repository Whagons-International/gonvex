// Replica delivery in a real Durable Object (workerd, SQLite): snapshot pages
// and the two-pass scan of a large window, the feed's per-socket work with
// shared texts, digests, the socket side's batches and instance resets.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { CaptureState, ChangeWake, directSql, installChangeFeed, invokeCaptured } from "../../capture/index.ts";
import { sha256Hex } from "../../capture/digest.ts";
import type { Negotiated } from "../../session/capabilities.ts";
import type { SocketView, TenantSession } from "../../session/ports.ts";
import { TenantStorage } from "../../storage";
import { parsePlan } from "../../visibility/plan.ts";
import { ReplicaBatcher } from "../batcher.ts";
import { Aggregate, stableJson } from "../canonical.ts";
import { parseDefinition, type ReplicaModule } from "../definition.ts";
import { ReplicaDeliveryEngine, STALL_TICKS, type DeliveryClock } from "../engine.ts";
import { LocalOutlet } from "../outlet.ts";
import type { ReplicaDelivery, ReplicaReceipt } from "../protocol.ts";
import type { ScopedSocketWork } from "../recovery.ts";
import { SocketDelivery, type TrackedReplicas } from "../socket.ts";
import { LiveQueries, snapshotRunner, kernelVisibility, moduleFromManifest } from "../../query/index";
import { VisibilityRegistry } from "../../visibility";
import { PAGE_BYTES, PAGE_ROWS } from "../subscription.ts";
import { keepAliveCheck, KEEPALIVE_CHECK_MS, SubscriberKeepAlive, type KeepAliveClock } from "../keepalive.ts";

let objects = 0;

const NEGOTIATED: Negotiated = { snapshotPages: true, noOldValues: true, fieldPatches: false, batchFrames: false, nativeRows: false, setDigest: false, telemetryBatch: false, presence: false };

function session(member: string): TenantSession {
  return {
    identity: { projectId: "p", account: { id: `acct-${member}`, email: "", emailVerified: false, name: member, avatarUrl: "", provider: "" } },
    route: { projectId: "p", tenantId: "t", databaseUrl: "sqlite:///t.db" },
    member: { id: `m-${member}`, accountId: `acct-${member}`, status: "active", displayName: member, avatarUrl: "", role: "admin", permissions: {}, membershipRevision: 0 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}

function socket(id: string, negotiated: Partial<Negotiated> = {}, host = "h1"): SocketView {
  const tenant = session("alice");
  return {
    connectionId: id, connection: { projectId: "p", identity: tenant.identity, tenant, impersonationId: "", authToken: "" },
    tenant, negotiated: { ...NEGOTIATED, ...negotiated }, publicTransport: false, host,
    replica: { scope: "scope-1", visibilityScope: "vscope-1" }, subscriptionEpoch: 0,
  };
}

const MODULE: ReplicaModule = {
  projectId: "p",
  artifactHash: "h",
  functions: new Map([
    ["items.all", { path: "items.all", kind: "query", delivery: "replica", replica: parseDefinition({ table: "items", key: "id", columns: ["id", "rank", "label"], orderBy: "rank", orderDirection: "asc", mode: "eager", maxRows: 100000 }) }],
    ["items.top", { path: "items.top", kind: "query", delivery: "replica", replica: parseDefinition({ table: "items", key: "id", columns: ["id", "rank", "label"], orderBy: "rank", orderDirection: "asc", mode: "progressive", maxRows: 2 }) }],
  ]),
  visibility: new Map([["items", parsePlan({ table: "items", key: "id", sets: {}, where: { operator: "public" } })]]),
};

interface Harness {
  engine: ReplicaDeliveryEngine;
  live: LiveQueries;
  payloads: Array<[string, ReplicaDelivery]>;
  receiver: { deliver?: (host: string, payload: ReplicaDelivery) => ReplicaReceipt | Promise<ReplicaReceipt> };
  delivered: number[];
  /**
   * Tenant invocations that overlapped another (`an invocation is already running`) and engine reads outside the
   * invocation queue. Production's `serial` is the reducers' queue (delivery/object.ts), so both must stay empty.
   */
  conflicts: string[];
  /** `settle: false` returns once the commit is published, while delivery may still be running (a concurrent writer). */
  commit(statements: Array<[string, unknown[]]>, settle?: boolean): Promise<number>;
}

async function withEngine<T>(body: (harness: Harness) => Promise<T>, module: ReplicaModule = MODULE, composeLive = false, activity?: () => void, stallMs?: number, hosts: string[] = ["h1"], clock?: DeliveryClock): Promise<T> {
  const stub = env.HELLO.getByName(`k10-${++objects}-${Math.random()}`);
  return runInDurableObject(stub, async (_instance, state) => {
    const sql = directSql(state.storage.sql);
    state.storage.transactionSync(() => {
      installChangeFeed(sql);
      sql.run("CREATE TABLE members (id TEXT PRIMARY KEY, account_id TEXT, status TEXT, role TEXT, permissions JSON)");
      sql.run("CREATE TABLE items (id TEXT PRIMARY KEY, rank INTEGER, label TEXT)");
      sql.run("CREATE INDEX items_rank ON items (rank)");
      sql.run("CREATE TABLE tasks (id TEXT PRIMARY KEY, state TEXT)");
      sql.run("CREATE TABLE tnotes (id TEXT PRIMARY KEY, task_id TEXT, body TEXT)");
      sql.run("CREATE INDEX tnotes_task ON tnotes (task_id)");
      sql.run("CREATE TABLE apiKeys (_id TEXT PRIMARY KEY, _creationTime REAL, name TEXT, keyHash TEXT, keyHint TEXT, scopes JSON, isActive INTEGER, lastUsedAt REAL, expiresAt REAL, revokedAt REAL, createdByUserId TEXT)");
      installChangeFeed(sql);
      sql.run("INSERT INTO members VALUES ('m-alice', 'acct-alice', 'active', 'admin', '{}')");
    });
    const storage = new TenantStorage(state.storage);
    const context = { state: new CaptureState(), wake: new ChangeWake() };
    let queue: Promise<unknown> = Promise.resolve();
    const payloads: Array<[string, ReplicaDelivery]> = [];
    const receiver: Harness["receiver"] = {};
    const delivered: number[] = [];
    const liveModule = moduleFromManifest("p", {
      functions: { "items.live": { kind: "query", delivery: "live", dependencies: { liveQueryPlan: {
        table: "items", key: "id", columns: ["id", "rank", "label"],
      } } } },
      visibility: { items: { table: "items", key: "id", where: { operator: "public" } } },
    });
    // One invocation queue for the engine and the reducers, as the tenant object has (`kernel.reducers.serial`).
    const conflicts: string[] = [];
    let inQueue = false;
    const serial = <R>(fn: () => Promise<R>): Promise<R> => {
      const run = async () => { inQueue = true; try { return await fn(); } finally { inQueue = false; } };
      const next = queue.then(run, run); queue = next.catch(() => undefined); return next;
    };
    const invoke = async <R>(fn: () => Promise<R>): Promise<R> => {
      try { return await fn(); } catch (error) { if (String(error).includes("already running")) conflicts.push(String(error)); throw error; }
    };
    const live = new LiveQueries({ run: (body) => serial(() => snapshotRunner(storage)(body)), module: () => liveModule, visibility: kernelVisibility(new VisibilityRegistry()) });
    const receive = (host:string,payload:ReplicaDelivery) => { payloads.push([host,payload]); return (receiver.deliver?.(host,payload) ?? {v:1,sockets:Object.keys(payload.sockets).length}) as ReplicaReceipt; };
    const engine = new ReplicaDeliveryEngine({
      ...(composeLive ? { live } : {}),
      tenantId: "t",
      delivered: (revision) => delivered.push(revision),
      serial,
      read: async (fn) => {
        if (!inQueue) conflicts.push(`engine read outside the invocation queue: ${new Error().stack}`);
        const r = await invoke(() => storage.invoke("query", async (s) => fn(s)));
        if (!r.ok) throw r.error;
        return r.value;
      },
      module: () => module,
      outlet: new LocalOutlet(receive, () => hosts, undefined, receive),
      ...(activity ? { activity } : {}),
      ...(stallMs !== undefined ? { stallMs } : {}),
      ...(clock ? { clock } : {}),
    });
    let commands = 0;
    return body({
      engine, live, payloads, receiver, delivered, conflicts,
      async commit(statements, settle = true) {
        const commandId = `c${++commands}`;
        const result = await serial(() => invoke(() => invokeCaptured(storage, "reducer", { commandId }, async (s) => {
          for (const [text, parameters] of statements) s.run(text, parameters);
        }, context)));
        if (!result.ok) throw result.error;
        const revision = result.notice?.revision ?? 0;
        engine.wake(revision);
        if (settle) await engine.settled();
        return revision;
      },
    });
  });
}

const frames = (payloads: Array<[string, ReplicaDelivery]>, id: string) =>
  payloads.flatMap(([, p]) => (p.sockets[id]?.ops ?? []).filter((op) => op[0] === "f").map((op) => JSON.parse(op[1] as string)));

const label = (n: number, size: number) => `${String(n).padStart(6, "0")} ${"x".repeat(size)}`;

describe("snapshots", () => {
  it("pages a window under 256 KiB and 512 rows, digest on the last page only", async () => {
    await withEngine(async ({ engine, payloads, commit }) => {
      const rows: Array<[string, unknown[]]> = [];
      for (let i = 0; i < 3000; i++) rows.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(5, "0")}`, i, label(i, 300)]]);
      await commit(rows);
      engine.dispatch(socket("s1"), { type: "replica.open", id: "sub", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      const got = frames(payloads, "s1");
      const pages = got.filter((f) => f.type === "replica.snapshotPage");
      expect(pages.length).toBeGreaterThan(3);
      const encoder = new TextEncoder();
      for (const [index, page] of pages.entries()) {
        expect(page.sequence).toBe(index);
        expect(page.last).toBe(index === pages.length - 1);
        expect(page.result.length).toBeLessThanOrEqual(PAGE_ROWS);
        expect(encoder.encode(JSON.stringify(page)).length).toBeLessThanOrEqual(PAGE_BYTES);
        expect("digest" in page).toBe(page.last);
        expect(page.snapshotId).toBe(pages[0].snapshotId);
      }
      const all = pages.flatMap((p) => p.result);
      expect(all.length).toBe(3000);
      expect(all.map((r: { rank: number }) => r.rank)).toEqual([...Array(3000).keys()]);
      const aggregate = new Aggregate("xor1");
      for (const row of all) aggregate.add(sha256Hex(stableJson(row)));
      expect(pages.at(-1).digest).toBe(aggregate.digest());
      const ready = got.find((f) => f.type === "replica.ready");
      expect(ready.digest).toBe(aggregate.digest());
      expect(got.at(-1)).toEqual({ type: "replica.watermark", revision: ready.cursor.revision });
    });
  });

  it("scans a window over 4 MiB twice instead of keeping it, with identical pages", { timeout: 60_000 }, async () => {
    await withEngine(async ({ engine, payloads, commit }) => {
      for (let batch = 0; batch < 3; batch++) {
        const rows: Array<[string, unknown[]]> = [];
        for (let i = batch * 3000; i < (batch + 1) * 3000; i++) rows.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(5, "0")}`, i, label(i, 600)]]);
        await commit(rows);
      }
      engine.dispatch(socket("s1"), { type: "replica.open", id: "sub", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      const pages = frames(payloads, "s1").filter((f) => f.type === "replica.snapshotPage");
      const all = pages.flatMap((p) => p.result);
      expect(all.length).toBe(9000);
      expect(new Set(all.map((r: { id: string }) => r.id)).size).toBe(9000);
      // More than RETAIN_BYTES of rows: posted in several payloads (chunks), not one.
      expect(payloads.filter(([, p]) => p.sockets.s1?.ops.some((op) => op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.snapshotPage"'))).length).toBeGreaterThan(1);
      // Two passes: one for the digest and boundary, one to page (256 rows per statement).
      expect(engine.stats.statements).toBe(2 * Math.ceil(9000 / 256));
    });
  });

  it("truncates by maxRows and keeps the boundary; an entering row asks for hashes", async () => {
    await withEngine(async ({ engine, payloads, commit }) => {
      await commit([1, 2, 3, 4].map((i) => ["INSERT INTO items VALUES ($1, $2, $3)", [`i${i}`, i * 10, `row ${i}`]] as [string, unknown[]]));
      engine.dispatch(socket("s1"), { type: "replica.open", id: "top", path: "items.top", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      const opened = frames(payloads, "s1");
      expect(opened.find((f) => f.type === "replica.ready").truncated).toBe(true);
      expect(opened.filter((f) => f.type === "replica.snapshotPage").flatMap((p) => p.result).map((r: { id: string }) => r.id)).toEqual(["i1", "i2"]);
      payloads.length = 0;
      const outside = await commit([["UPDATE items SET label = 'outside' WHERE id = 'i4'", []]]);
      // Nothing for this socket but the commit's watermark, which every socket of the holder gets.
      expect(frames(payloads, "s1")).toEqual([]);
      expect(payloads.map(([, p]) => p.watermark)).toEqual([outside]);
      payloads.length = 0;
      const inside = await commit([["UPDATE items SET label = 'inside' WHERE id = 'i2'", []]]);
      expect(frames(payloads, "s1").map((f) => f.type)).toEqual(["replica.transaction", "replica.delta", "replica.ready"]);
      expect(payloads[0]![1].sockets.s1!.watermark).toBe(inside);
      payloads.length = 0;
      await commit([["INSERT INTO items VALUES ('i0', 1, 'first')", []]]);
      expect(frames(payloads, "s1").map((f) => f.type)).toEqual(["replica.needHashes"]);
      expect(engine.subscriptions("s1").has("top")).toBe(false);
    });
  });
});

describe("feed", () => {
  it("encodes each shared text once for sockets of one class and gives each socket its own tail", async () => {
    await withEngine(async ({ engine, payloads, commit }) => {
      await commit([["INSERT INTO items VALUES ('a', 1, 'a')", []]]);
      for (const id of ["b1", "b2"]) engine.dispatch(socket(id, { batchFrames: true }), { type: "replica.open", id: `sub-${id}`, path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      engine.dispatch(socket("plain"), { type: "replica.open", id: "sub-plain", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      payloads.length = 0;
      const revision = await commit([["INSERT INTO items VALUES ('b', 2, 'b')", []], ["UPDATE items SET label = 'a2' WHERE id = 'a'", []]]);
      const [, payload] = payloads.find(([, p]) => p.sockets.b1)!;
      // One transaction item and one delta body, shared by both batch sockets.
      expect(payload.shared.length).toBe(2);
      for (const id of ["b1", "b2"]) {
        const ops = payload.sockets[id]!.ops;
        expect(ops.map((op) => op[0])).toEqual(["i", "d", "r"]);
        expect(payload.sockets[id]!.watermark).toBe(revision);
        const delta = ops[1] as readonly ["d", string, string, number, string];
        expect(JSON.parse(payload.shared[delta[3]]! + delta[4]).upserts.map((r: { id: string }) => r.id)).toEqual(["a", "b"]);
        expect(delta[2]).toMatch(/^~[0-9a-f]{16}$/);
      }
      const plain = payload.sockets.plain!.ops.map((op) => JSON.parse(op[1] as string));
      expect(plain.map((f) => f.type)).toEqual(["replica.transaction", "replica.delta", "replica.ready"]);
      expect(plain[1].id).toBe("sub-plain");
    });
  });

  it("gives K07 a floor only while a socket has subscriptions, and only what its holder accepted", async () => {
    await withEngine(async ({ engine, commit }) => {
      // The seeded Member row is captured with the first commit (a principal change); open after it.
      await commit([["INSERT INTO items VALUES ('z', 0, 'z')", []]]);
      expect(engine.subscriptionFloor(socket("s1"))).toBeUndefined();
      engine.dispatch(socket("s1"), { type: "replica.open", id: "sub", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      const revision = await commit([["INSERT INTO items VALUES ('a', 1, 'a')", []]]);
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(engine.subscriptionFloor(socket("s1"))).toBe(revision);
      engine.dispatch(socket("s1"), { type: "replica.close", id: "sub" });
      expect(engine.subscriptionFloor(socket("s1"))).toBeUndefined();
    });
  });

  it("sends the own-membership hand-off on the commit channel and drops that Member's subscriptions (0048 §1a)", async () => {
    await withEngine(async ({ engine, payloads }) => {
      engine.dispatch(socket("s1"), { type: "replica.open", id: "sub", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      payloads.length = 0;
      engine.membershipChanged("h1", "m-alice", 5);
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(payloads).toEqual([["h1", expect.objectContaining({ membership: { revision: 5, members: ["m-alice"], accounts: [] }, sockets: {} })]]);
      expect(engine.subscriptions("s1").size).toBe(0);
    });
  });

  it("drops a connection's state on a newer subscription epoch", async () => {
    await withEngine(async ({ engine }) => {
      engine.dispatch(socket("s1"), { type: "replica.open", id: "sub", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      expect(engine.subscriptions("s1").size).toBe(1);
      engine.reset("s1", -1);
      expect(engine.subscriptions("s1").size).toBe(1);
      engine.reset("s1", 0);
      expect(engine.subscriptions("s1").size).toBe(0);
    });
  });
});

describe("digests", () => {
  it("sum128-v1 matches the protocol crate's vector, and xor1 parity holds", () => {
    const high = `${"ff".repeat(16)}${"00".repeat(16)}`;
    const low = `${"00".repeat(15)}01${"00".repeat(15)}01`;
    const sum = new Aggregate("sum128-v1");
    sum.add(high);
    sum.add(low);
    expect(sum.digest()).toBe(`sum128-v1:${"00".repeat(16)}`);
    sum.remove(high);
    expect(sum.legacy().digest()).toBe(`xor1:${low}`);
    sum.remove(low);
    expect(sum.digest()).toBe(`sum128-v1:${"0".repeat(32)}`);
    // A removal below zero rows changes nothing (ReplicaSetDigest refuses it).
    sum.remove(low);
    expect(sum.legacy().digest()).toBe(`xor1:${"0".repeat(64)}`);
  });

  it("stable JSON matches the SDK's canonical form", () => {
    expect(stableJson({ b: 1, a: [true, null, "x "], "é": 1.5, A: { z: 0, y: -0 } })).toBe('{"A":{"y":0,"z":0},"a":[true,null,"x\\u2028"],"b":1,"é":1.5}');
  });
});

describe("socket side", () => {
  function host(batch: boolean) {
    const sent: string[] = [];
    const timers: Array<() => void> = [];
    let now = 0, watermark = 0, instance = "";
    const tracked: TrackedReplicas = {};
    let delivery!: SocketDelivery;
    const h = {
      send: (text: string) => { if (delivery.batcher.hasOpen) delivery.batcher.flush(); sent.push(text); },
      sendRaw: (text: string) => sent.push(text),
      end: () => sent.push("<end>"),
      envelope: () => ({ tenant: "t", class: "0000000000000001" }),
      now: () => now,
      later: (_ms: number, fn: () => void) => { timers.push(fn); return () => { const i = timers.indexOf(fn); if (i >= 0) timers.splice(i, 1); }; },
      batchFrames: () => batch,
      watermark: () => watermark,
      setWatermark: (r: number) => { watermark = r; },
      tracked: () => tracked,
      instance: () => instance,
      setInstance: (i: string) => { instance = i; },
      trackedChanged: () => {},
    };
    delivery = new SocketDelivery(h);
    delivery.authenticated(batch);
    return { delivery, sent, timers, tick: (ms: number) => { now += ms; }, fire: () => timers.splice(0).forEach((fn) => fn()), tracked };
  }

  it("batches items with stream bindings, sends at the tick, and at once when a call waits", () => {
    const { delivery, sent, timers, fire } = host(true);
    const payload: ReplicaDelivery = { v: 1, instance: "A", shared: ['{"type":"replica.transaction"}', '{"type":"replica.delta","id":"~tok"'], sockets: {}, watermark: 7 };
    delivery.apply(payload, { ops: [["i", 0], ["d", "sub", "~tok", 1, ',"digest":"d","truncated":false}'], ["r", "sub", "~tok", '{"type":"replica.ready","id":"~tok"}']], watermark: 7 });
    expect(sent).toEqual([]);
    expect(timers.length).toBe(1);
    fire();
    expect(sent.length).toBe(1);
    const batch = JSON.parse(sent[0]!);
    expect(batch.items.map((i: { type: string }) => i.type)).toEqual(["replica.transaction", "replica.stream", "replica.delta", "replica.ready"]);
    expect(batch.watermark).toBe(7);
    // A call waiting for revision 8 makes the batch carrying it go at once; a binding is sent only once.
    delivery.batcher.urgent(8);
    delivery.apply({ ...payload, watermark: 8 }, { ops: [["d", "sub", "~tok", 1, ',"digest":"e","truncated":false}']], watermark: 8 });
    expect(sent.length).toBe(2);
    const second = JSON.parse(sent[1]!);
    expect(second.from).toBe(7);
    expect(second.items.map((i: { type: string }) => i.type)).toEqual(["replica.delta"]);
    // Watermarks never go down.
    delivery.apply({ ...payload, watermark: 3 }, undefined);
    expect(timers.length).toBe(0);
  });

  it("a plain socket gets frames and a watermark per commit", () => {
    const { delivery, sent } = host(false);
    delivery.apply({ v: 1, instance: "A", shared: [], sockets: {}, watermark: 4 }, { ops: [["f", '{"type":"replica.delta"}']], watermark: 5 });
    expect(sent).toEqual(['{"type":"replica.delta"}', '{"type":"replica.watermark","revision":5}']);
  });

  it("resets the collections of a tenant instance that is gone", () => {
    const { delivery, sent, tracked } = host(false);
    delivery.opening("s1", "items.all");
    delivery.apply({ v: 1, instance: "A", shared: [], sockets: {} }, { ops: [], opened: { s1: "items.all" } });
    delivery.opening("s2", "items.top");
    delivery.apply({ v: 1, instance: "B", shared: [], sockets: {}, watermark: 9 }, undefined);
    expect(sent).toEqual([
      '{"type":"replica.reset","id":"s1","path":"items.all","reason":"change-feed-reset"}',
      '{"type":"replica.reset","id":"s2","path":"items.top","reason":"change-feed-reset"}',
      '{"type":"replica.watermark","revision":9}',
    ]);
    expect(tracked).toEqual({});
  });

  it("presence rides in a batch that holds rows, and goes at once otherwise", () => {
    const sent: string[] = [];
    const batcher = new ReplicaBatcher({ sendRaw: (t) => sent.push(t), envelope: () => ({ tenant: "t", class: "c" }), now: () => 0, later: () => () => {} });
    batcher.presence('{"type":"presence.patch"}');
    expect(sent.length).toBe(1);
    batcher.item('{"type":"replica.transaction"}');
    batcher.presence('{"type":"presence.patch"}');
    expect(sent.length).toBe(1);
    batcher.flush();
    expect(JSON.parse(sent[1]!).items.length).toBe(2);
  });
});

describe("payload limits", () => {
  it("splits a payload under the RPC cap, keeping each socket's order and the watermark last", async () => {
    const { splitPayload } = await import("../engine.ts");
    const big = "x".repeat(400);
    const payload: ReplicaDelivery = {
      v: 1, instance: "A", shared: [big, `{"type":"replica.delta","id":"~t"${big}`], watermark: 9,
      sockets: {
        a: { ops: [["i", 0], ["d", "s", "~t", 1, "}"], ["f", big], ["f", big]], watermark: 9, opened: { s: "p" }, subscriptionEpoch: 7 } as ScopedSocketWork,
        b: { ops: [["i", 0], ["f", big]], watermark: 9, subscriptionEpoch: 8 } as ScopedSocketWork,
      },
    };
    const parts = splitPayload(payload, 1000);
    expect(parts.length).toBeGreaterThan(2);
    const opsOf = (id: string) => parts.flatMap((p) => (p.sockets[id]?.ops ?? []).map((op) => (op[0] === "i" ? ["i", p.shared[op[1]]] : op[0] === "d" ? ["d", p.shared[op[3]]] : op)));
    expect(opsOf("a")).toEqual([["i", big], ["d", payload.shared[1]], ["f", big], ["f", big]]);
    expect(opsOf("b")).toEqual([["i", big], ["f", big]]);
    expect(parts.slice(0, -1).every((p) => p.watermark === undefined)).toBe(true);
    expect(parts.at(-1)!.watermark).toBe(9);
    expect(parts.filter((p) => p.sockets.a?.opened).length).toBe(1);
    for (const part of parts) for (const [id, work] of Object.entries(part.sockets)) expect((work as ScopedSocketWork).subscriptionEpoch).toBe(id === "a" ? 7 : 8);
    expect(splitPayload({ ...payload, sockets: {} }, 1000)).toHaveLength(1);
  });
});

const MASKED: ReplicaModule = {
  projectId: "p",
  artifactHash: "h2",
  functions: new Map([
    ["notes.all", { path: "notes.all", kind: "query", delivery: "replica", replica: parseDefinition({ table: "tnotes", key: "id", columns: ["id", "task_id", "body"], orderBy: "id", orderDirection: "asc", mode: "eager", maxRows: 10000 }) }],
  ]),
  visibility: new Map([
    ["tasks", parsePlan({ version: 2, table: "tasks", key: "id", sets: {}, where: { operator: "eq", column: "state", value: { literal: "open" } } })],
    ["tnotes", parsePlan({ version: 2, table: "tnotes", key: "id", sets: {}, where: { operator: "inherits", column: "task_id", table: "tasks", key: "id" } })],
  ]),
};

describe("design rule: no SQL per subscriber", () => {
  // A parent's flip makes every subscriber re-decide its children (`affected_rows`). The read happens once per
  // transaction, whatever the number of subscribers (0048 §1).
  const commitReads = async (subscribers: number) => withEngine(async ({ engine, payloads, commit }) => {
    const notes: Array<[string, unknown[]]> = [["INSERT INTO tasks VALUES ('t1', 'open')", []]];
    for (let i = 0; i < 50; i++) notes.push(["INSERT INTO tnotes VALUES ($1, 't1', $2)", [`n${String(i).padStart(2, "0")}`, `note ${i}`]]);
    await commit(notes);
    for (let i = 0; i < subscribers; i++) engine.dispatch(socket(`s${i}`), { type: "replica.open", id: `sub${i}`, path: "notes.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
    await engine.settled();
    payloads.length = 0;
    const before = { statements: engine.stats.commitStatements, rowsRead: engine.stats.commitRowsRead };
    await commit([["UPDATE tasks SET state = 'closed' WHERE id = 't1'", []]]);
    const deltas = payloads.flatMap(([, p]) => Object.values(p.sockets).flatMap((w) => w.ops))
      .filter((op) => op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.delta"'));
    expect(deltas.length).toBe(subscribers);
    expect(JSON.parse(deltas[0]![1] as string).deleted).toHaveLength(50);
    return { statements: engine.stats.commitStatements - before.statements, rowsRead: engine.stats.commitRowsRead - before.rowsRead };
  }, MASKED);

  it("reads the same rows per commit for 1, 10 and 100 subscribers", { timeout: 60_000 }, async () => {
    const one = await commitReads(1), ten = await commitReads(10), hundred = await commitReads(100);
    console.log(`K10 commit reads (statements / rows read): 1 subscriber ${one.statements} / ${one.rowsRead}, 10: ${ten.statements} / ${ten.rowsRead}, 100: ${hundred.statements} / ${hundred.rowsRead}`);
    expect(ten).toEqual(one);
    expect(hundred).toEqual(one);
  });
});


describe("K22 ordered query and replica commits", () => {
  // Receiver controls below test the engine/barrier wiring; actual holder
  // scope validation and rebuilding are covered separately on Gateway/Session.
  it("0061 uses challenge epochs only for empty work and keeps deferred query replies behind acknowledged catch-up", async () => {
    await withEngine(async ({ engine, live, payloads, receiver, delivered, commit }) => {
      await commit([["INSERT INTO items VALUES ('i1', 1, 'before')", []]]);
      let acknowledge!: (receipt: ReplicaReceipt) => void;
      let fence: ReplicaDelivery | undefined;
      receiver.deliver = (_host, payload) => {
        if (!Object.keys(payload.sockets).length && payload.watermark! > 0) return { v: 1, sockets: 1, scopeRequired: { restored: 7 } };
        if (Object.values(payload.sockets).some((work) => !work.ops.length && work.watermark === undefined)) {
          fence = payload;
          return new Promise((resolve) => { acknowledge = resolve; });
        }
        return { v: 1, sockets: Object.keys(payload.sockets).length };
      };
      await commit([["UPDATE items SET label='after' WHERE id='i1'", []]]);
      for (let i = 0; i < 50 && !fence; i++) await new Promise((resolve) => setTimeout(resolve, 1));
      expect(fence).toBeDefined();
      expect(fence!.shared).toEqual([]);
      expect(fence!.sockets.restored!.ops).toEqual([]);
      expect((fence!.sockets.restored as ScopedSocketWork).subscriptionEpoch).toBe(7);
      expect(delivered).toEqual([]); // Challenge receipt is not commit acceptance.
      const view = { ...socket("late"), subscriptionEpoch: 7, queryId: "live" };
      let executed = false;
      await engine.querySubscribe(view, (db) => { executed = true; return live.subscribeSnapshot(db, view.connectionId, view.tenant, { id: "live", path: "items.live", args: {} }); });
      expect(executed).toBe(false);
      expect(engine.subscriptionFloor(view)).toBe(0); // Unacknowledged holder fence also blocks this not-yet-installed query.
      acknowledge({ v: 1, sockets: 1, scopeAck: { restored: 7 } });
      for (let i = 0; i < 100 && !executed; i++) await new Promise((resolve) => setTimeout(resolve, 1));
      expect(executed).toBe(true);
      await engine.settled();
      const query = payloads.findIndex(([, payload]) => payload.sockets.late?.ops.some((op) => op[0] === "f" && op[1].includes('"type":"query.result"')));
      expect(query).toBeGreaterThan(payloads.findIndex(([, payload]) => payload === fence));
      expect(JSON.stringify(payloads[query]![1])).toContain("after");
    }, MODULE, true);
  });

  it("does not accept an older holder's ordinary receipt as an empty-scope fence acknowledgment", async () => {
    await withEngine(async ({ engine, live, receiver, delivered, commit }) => {
      await commit([["INSERT INTO items VALUES ('i1', 1, 'before')", []]]);
      let fences = 0;
      receiver.deliver = (_host, payload) => {
        if (!Object.keys(payload.sockets).length && payload.watermark! > 0) return { v: 1, sockets: 1, scopeRequired: { restored: 4 } };
        fences++; return { v: 1, sockets: 1 }; // Old sender/holder skew: no explicit ack.
      };
      await commit([["UPDATE items SET label='after' WHERE id='i1'", []]]);
      for (let i = 0; i < 25; i++) await new Promise((resolve) => setTimeout(resolve, 1));
      const view = { ...socket("late"), subscriptionEpoch: 4, queryId: "live" };
      let executed = false;
      await engine.querySubscribe(view, (db) => { executed = true; return live.subscribeSnapshot(db, view.connectionId, view.tenant, { id: "live", path: "items.live", args: {} }); });
      expect(executed).toBe(false);
      expect(fences).toBe(1);
      expect(delivered).toEqual([]);
    }, MODULE, true);
  });
  it("queues an initial query snapshot before a later commit and fences delayed requests after scope reset", async () => {
    await withEngine(async ({ engine, live, payloads, commit }) => {
      await commit([["INSERT INTO items VALUES ('i1', 1, 'before')", []]]);
      const view = socket("initial");
      const request = { id: "live", path: "items.live", args: {} };
      expect(await engine.querySubscribe(view, (db) => live.subscribeSnapshot(db, view.connectionId, view.tenant, request))).toEqual([]);
      const revision = await commit([["UPDATE items SET label='after' WHERE id='i1'", []]]);
      const replies = frames(payloads, view.connectionId).filter((f) => f.type === "query.result");
      expect(replies.map((f) => f.reason)).toEqual(["initial", "change"]);
      expect(replies.map((f) => f.result[0].label)).toEqual(["before", "after"]);
      const commitPayload = payloads.find(([, p]) => p.watermark === revision)![1];
      expect((commitPayload.sockets.initial as ScopedSocketWork).subscriptionEpoch).toBe(0);
      engine.reset(view.connectionId, 2);
      let ran = false;
      await expect(engine.querySubscribe(view, () => { ran = true; return "stale"; })).rejects.toThrow("stale tenant subscription scope");
      expect(ran).toBe(false);
      expect(live.subscriptionIds(view.connectionId)).toEqual([]);
      view.subscriptionEpoch = 2;
      await engine.querySubscribe(view, (db) => live.subscribeSnapshot(db, view.connectionId, view.tenant, request));
      expect((payloads.at(-1)![1].sockets.initial as ScopedSocketWork).subscriptionEpoch).toBe(2);
    }, MODULE, true);
  });
  it("requires exact terminal enqueue receipts before initial progress; malformed or missing ACKs retain suppression",async()=>{
    for(const mode of ["enqueued","obsolete","missing","duplicate","foreign","extra"]){
      await withEngine(async({engine,live,receiver,commit,payloads,delivered})=>{
        await commit([["INSERT INTO items VALUES ('i1',1,'before')",[]]]);delivered.length=0;payloads.length=0;
        const view={...socket(`ack-${mode}`),initialRequests:[{kind:"query" as const,id:"q",path:"items.live",epoch:0,generation:1}]};
        let recoverySeen!:()=>void;
        const recovering=new Promise<void>(resolve=>{recoverySeen=resolve;});
        receiver.deliver=(_host,payload)=>{
          if(payload.recovery){recoverySeen();return {v:1,sockets:1};} // unsupported recovery stays blocked, never synthetic ACK
          const status={generation:mode==="foreign"?999:1,status:mode==="obsolete"?"obsolete" as const:"enqueued" as const};
          return {v:1,sockets:1,...(mode==="missing"?{}:{initialCompletions:{[view.connectionId]:mode==="duplicate"?[status,status]:[status],...(mode==="extra"?{unoffered:[status]}:{})}})};
        };
        await engine.querySubscribe(view,db=>live.subscribeSnapshot(db,view.connectionId,view.tenant,{id:"q",path:"items.live",args:{}}));
        await engine.settled();
        if(!["enqueued","obsolete"].includes(mode))await Promise.race([recovering,new Promise<never>((_,reject)=>setTimeout(()=>reject(new Error(`missing recovery after ${mode} receipt`)),1000))]);
        if(mode==="enqueued")expect(engine.subscriptionFloor(view)).toBeGreaterThan(0);
        else expect(engine.subscriptionFloor(view)).toBe(0);
        expect(delivered).toEqual([]);
        if(!["enqueued","obsolete"].includes(mode)){
          expect(payloads.some(([,p])=>!!p.recovery)).toBe(true);
          const quiet=socket(`quiet-${mode}`);engine.observe(quiet);
          expect(engine.subscriptionFloor(quiet)).toBe(0);
        }
      },MODULE,true);
    }
  });
  it("puts live-query results before replica frames in the same commit payload, including live-only sockets", async () => {
    await withEngine(async ({ engine, live, payloads, commit }) => {
      await commit([["INSERT INTO items VALUES ('i1', 1, 'before')", []]]);
      for (const id of ["both", "live-only"]) {
        const view = socket(id);
        engine.observe(view);
        const initial = JSON.parse(await live.subscribe(id, view.tenant, { id: "live", path: "items.live", args: {} }));
        expect(initial.type).toBe("query.result");
      }
      engine.dispatch(socket("both"), { type: "replica.open", id: "replica", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      payloads.length = 0;
      const revision = await commit([["UPDATE items SET label='after' WHERE id='i1'", []]]);
      const payload = payloads.find(([, p]) => p.watermark === revision)![1];
      const ops = payload.sockets.both!.ops;
      expect(ops[0]![0]).toBe("f");
      const query = JSON.parse(ops[0]![1] as string);
      expect(query).toMatchObject({ type: "query.result", id: "live", reason: "change" });
      expect(JSON.stringify(query.result)).toContain("after");
      expect(ops.slice(1).some((op) => op[0] === "f" && JSON.parse(op[1]).type.startsWith("replica."))).toBe(true);
      expect(JSON.parse(payload.sockets["live-only"]!.ops[0]![1] as string)).toEqual(query);
      expect(engine.subscriptionFloor(socket("live-only"))).toBeGreaterThanOrEqual(revision);
      engine.reset("live-only", 1);
      expect(live.subscriptionIds("live-only")).toEqual([]);
    }, MODULE, true);
  });
});

/** A manual clock for the keep-alive timer: `advance` runs what falls due. */
function manualClock() {
  let now = 0, seq = 0;
  const timers = new Map<number, { at: number; fn: () => void }>();
  const clock: KeepAliveClock = {
    now: () => now,
    set: (fn, ms) => { const id = ++seq; timers.set(id, { at: now + ms, fn }); return id; },
    clear: (id) => { timers.delete(id as number); },
  };
  const advance = (ms: number) => {
    const end = now + ms;
    for (;;) {
      const due = [...timers].filter(([, t]) => t.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!due) break;
      timers.delete(due[0]); now = due[1].at; due[1].fn();
    }
    now = end;
  };
  return { clock, advance };
}

/** The platform hibernates (and discards memory) after 10 s idle only when no timer is pending. */
const PLATFORM_HIBERNATION_IDLE_MS = 10_000;

describe("BUG-2 a subscribed tenant stays in memory between a session's events", () => {
  it("two sockets, ordinary commits after idle pauses: incremental batches, no reset, never hibernatable while subscribed", async () => {
    const { clock, advance } = manualClock();
    let keep: SubscriberKeepAlive | undefined;
    await withEngine(async ({ engine, payloads, commit }) => {
      keep = new SubscriberKeepAlive(() => engine.holdsSubscriptions(), KEEPALIVE_CHECK_MS, clock);
      await commit([["INSERT INTO items VALUES ('a', 1, 'a')", []]]);
      for (const id of ["A", "B"]) engine.dispatch(socket(id, { batchFrames: true }), { type: "replica.open", id: `sub-${id}`, path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      expect(engine.holdsSubscriptions()).toBe(true);
      expect(keep.holding).toBe(true);
      const instance = engine.instance;
      payloads.length = 0;
      // The walkthrough's pauses between ordinary commits were 6-18 s; each is longer than the platform's idle limit.
      for (let n = 0; n < 5; n++) {
        advance(PLATFORM_HIBERNATION_IDLE_MS + 5_000);
        expect(keep.holding).toBe(true); // a pending timer: the platform cannot hibernate this object
        await commit([[`INSERT INTO items VALUES ('k${n}', ${10 + n}, 'from A')`, []]]);
      }
      const forB = payloads.filter(([, p]) => p.sockets.B);
      expect(forB).toHaveLength(5);
      for (const [, payload] of forB) {
        expect(payload.instance).toBe(instance);
        const ops = payload.sockets.B!.ops;
        expect(ops.map((op) => op[0])).toEqual(["i", "d", "r"]); // transaction item, delta, ready: an incremental batch
        expect(ops.some((op) => op[0] === "f" && /"type":"replica\.reset"/.test(op[1] as string))).toBe(false);
      }
      expect(engine.stats.resets).toBe(0);
      // A tenant nobody subscribes to may hibernate again.
      for (const id of ["A", "B"]) engine.dispatch(socket(id, { batchFrames: true }), { type: "replica.close", id: `sub-${id}` });
      await engine.settled();
      expect(engine.holdsSubscriptions()).toBe(false);
      expect(keep.holding).toBe(false);
    }, MODULE, false, () => keep?.touch());
  });

  it("BUG-9 holds a subscribed tenant with no idle expiry: after 10 idle minutes an ordinary commit is still an incremental batch", async () => {
    const { clock, advance } = manualClock();
    let keep: SubscriberKeepAlive | undefined;
    await withEngine(async ({ engine, payloads, commit }) => {
      keep = new SubscriberKeepAlive(() => engine.holdsSubscriptions(), undefined, clock);
      await commit([["INSERT INTO items VALUES ('a', 1, 'a')", []]]);
      for (const id of ["A", "B"]) engine.dispatch(socket(id, { batchFrames: true }), { type: "replica.open", id: `sub-${id}`, path: "items.all", args: {} , keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      const instance = engine.instance;
      payloads.length = 0;
      // -04, 2026-11-18 03:58:59Z: both clients idle well past the old 120 s grace, so the tenant hibernated
      // and the next event reset every replica. No event reaches the engine for 10 minutes here.
      for (let minute = 0; minute < 10; minute++) {
        advance(60_000);
        expect(keep.holding).toBe(true); // a timer stays pending: the platform cannot hibernate this tenant
      }
      await commit([["INSERT INTO items VALUES ('late', 50, 'from A')", []]]);
      const [, payload] = payloads.find(([, p]) => p.sockets.B)!;
      expect(payload.instance).toBe(instance);
      expect(payload.sockets.B!.ops.map((op) => op[0])).toEqual(["i", "d", "r"]);
      expect(engine.stats.resets).toBe(0);
      for (const id of ["A", "B"]) engine.dispatch(socket(id, { batchFrames: true }), { type: "replica.close", id: `sub-${id}` });
      await engine.settled();
      expect(keep.holding).toBe(false); // released only when the last subscription goes
    }, MODULE, false, () => keep?.touch());
  });

  it("arms the keep-alive for a socket whose only subscription is a live query, once the query is installed", async () => {
    const { clock, advance } = manualClock();
    let keep: SubscriberKeepAlive | undefined;
    await withEngine(async ({ engine, live, commit }) => {
      keep = new SubscriberKeepAlive(() => engine.holdsSubscriptions(), KEEPALIVE_CHECK_MS, clock);
      await commit([["INSERT INTO items VALUES ('i1', 1, 'before')", []]]);
      expect(keep.holding).toBe(false); // nothing subscribed yet: the tenant may hibernate
      const view = { ...socket("live-only"), queryId: "live" } as SocketView;
      const frame = { type: "query.subscribe" as const, id: "live", path: "items.live", args: {} };
      await engine.querySubscribe(view, (db) => live.subscribeSnapshot(db, view.connectionId, view.tenant, frame));
      expect(live.subscriptionIds("live-only")).toEqual(["live"]);
      expect(engine.holdsSubscriptions()).toBe(true);
      expect(keep.holding).toBe(true); // armed by the subscribe itself, not by a later event
      advance(PLATFORM_HIBERNATION_IDLE_MS + 5_000);
      expect(keep.holding).toBe(true);
      engine.queryUnsubscribe(view, "live");
      expect(engine.holdsSubscriptions()).toBe(false);
      expect(keep.holding).toBe(false);
    }, MODULE, true, () => keep?.touch());
  });

  it("holds while subscribed whatever the idle time, and releases only when the last subscription goes (BUG-9 decision)", () => {
    const { clock, advance } = manualClock();
    let subscribed = true;
    const keep = new SubscriberKeepAlive(() => subscribed, 60_000, clock);
    keep.touch();
    advance(24 * 60 * 60_000); // a day with no event
    expect(keep.holding).toBe(true);
    subscribed = false;
    advance(60_000); // the next check finds no subscription and lets the tenant hibernate
    expect(keep.holding).toBe(false);
    subscribed = true;
    keep.touch();
    expect(keep.holding).toBe(true);
    subscribed = false;
    keep.touch(); // an unsubscribe event releases at once
    expect(keep.holding).toBe(false);
    expect(new SubscriberKeepAlive(() => true, 0, clock).holding).toBe(false);
  });

  it("reads GONVEX_TENANT_KEEPALIVE_MS as the re-check period (0 disables)", () => {
    expect(keepAliveCheck(undefined)).toBe(KEEPALIVE_CHECK_MS);
    expect(keepAliveCheck("0")).toBe(0);
    expect(keepAliveCheck("30000")).toBe(30_000);
    expect(keepAliveCheck("-1")).toBe(KEEPALIVE_CHECK_MS);
    expect(keepAliveCheck("forever")).toBe(KEEPALIVE_CHECK_MS);
  });
});

describe("BUG-4 a large tenant's first load converges under ordinary commits", () => {
  // devcopy-stg1117-12 (115,547 rows): 83 replicas opened at once over a gateway RPC hop while the session's own
  // writes committed every few seconds. Each commit found the holder's queue still holding snapshot pages over its
  // 8 MiB credit budget and reset every replica `change-feed-lagged`; the client reopened all of them, and the next
  // commit reset them again (903 resets in 40 s, the workspace never loaded). Rust defers the feed while a socket is
  // backlogged and resets only a socket whose writes stall (lib.rs `defer_feed`, change_feed/backlog.rs).
  const RESET_CAP = 20;
  const load = async (rows: number, replicas: number, options: { hold?: number; stallMs?: number } = {}) => {
    let recoveries = 0, done = false, held = false;
    const ready = new Set<string>();
    const ids = Array.from({ length: replicas }, (_, i) => `sub-${i}`);
    let commits = 0;
    await withEngine(async ({ engine, receiver, commit }) => {
      const insert: Array<[string, unknown[]]> = [];
      for (let i = 0; i < rows; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 200)]]);
      for (let i = 0; i < insert.length; i += 2000) await commit(insert.slice(i, i + 2000));
      const open = () => engine.dispatch(socket("s1"), { type: "replica.openMany", opens: ids.map((id) => ({ id, path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false })) } as never);
      receiver.deliver = async (_host, payload) => {
        // The gateway RPC hop; optionally one delivery hangs (a holder that stops making progress).
        const hang = options.hold !== undefined && !held && Object.keys(payload.sockets).length > 0;
        if (hang) held = true;
        await new Promise((resolve) => setTimeout(resolve, hang ? options.hold : 2));
        if (payload.recovery) {
          // The holder resets every tracked replica; the client reopens them all, as the SDK does on replica.reset.
          recoveries += 1; ready.clear();
          if (recoveries <= RESET_CAP) queueMicrotask(open);
          return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt;
        }
        for (const op of payload.sockets.s1?.ops ?? []) {
          if (op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.ready"')) ready.add(JSON.parse(op[1] as string).id);
        }
        return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
      };
      open();
      // The session keeps writing while the workspace loads (published commits; delivery runs concurrently).
      const writer = (async () => {
        while (!done) {
          await new Promise((resolve) => setTimeout(resolve, 5));
          await commit([[`UPDATE items SET label = 'w${++commits}' WHERE id = 'i000000'`, []]], false);
        }
      })();
      for (let spin = 0; spin < 8000 && ready.size < replicas && recoveries <= RESET_CAP; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      done = true;
      await writer;
      await engine.settled();
    }, MODULE, false, undefined, options.stallMs);
    expect(commits).toBeGreaterThan(replicas / 4);
    return { recoveries, ready: ready.size };
  };

  it("loads every replica of a window over the holder's byte budget with no lagged reset, at two sizes", { timeout: 180_000 }, async () => {
    // ~2.4 MiB of row text per replica (19 MiB of credits): each snapshot alone is over the 8 MiB budget.
    expect(await load(12_000, 24)).toEqual({ recoveries: 0, ready: 24 });
    // Twice the rows (two scan chunks per open) and more replicas: nothing in the commit path scales with size.
    expect(await load(24_000, 32)).toEqual({ recoveries: 0, ready: 32 });
  });

  it("defers only the backlogged holder: another holder gets a commit at once, the backlogged one catches up in order", { timeout: 60_000 }, async () => {
    await withEngine(async ({ engine, receiver, commit }) => {
      // ~1.8 Mi chars in one final snapshot post (one scan chunk): ~14 MiB of credits, over the 8 MiB holder budget.
      const insert: Array<[string, unknown[]]> = [];
      for (let i = 0; i < 7_500; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 200)]]);
      for (let i = 0; i < insert.length; i += 2500) await commit(insert.slice(i, i + 2500));
      const log: Array<{ host: string; at: number; payload: ReplicaDelivery }> = [];
      let release!: () => void, held = false, recoveries = 0;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      receiver.deliver = async (host, payload) => {
        log.push({ host, at: Date.now(), payload });
        if (payload.recovery) { recoveries += 1; return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt; }
        // Holder A's RPC carrying its snapshot's end is slow (a large first load over the gateway hop).
        if (host === "h1" && !held && (payload.sockets.A?.ops ?? []).some((op) => op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.ready"'))) {
          held = true; await gate;
        }
        return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
      };
      engine.dispatch(socket("B", {}, "h2"), { type: "replica.open", id: "sub-B", path: "items.top", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      engine.dispatch(socket("A", {}, "h1"), { type: "replica.open", id: "sub-A", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      for (let spin = 0; spin < 2000 && !held; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(held).toBe(true);

      const started = Date.now();
      const revision = await commit([["UPDATE items SET label = 'live' WHERE id = 'i000000'", []]], false);
      const deltaFor = (id: string, sub: string) => log.find((entry) => (entry.payload.sockets[id]?.ops ?? []).some((op) => op[0] === "f" && (op[1] as string).includes('"type":"replica.delta"') && (op[1] as string).includes(`"id":"${sub}"`)));
      for (let spin = 0; spin < 400 && !deltaFor("B", "sub-B"); spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      const toB = deltaFor("B", "sub-B");
      expect(toB, "holder B gets the commit while A is still draining").toBeDefined();
      expect(toB!.at - started).toBeLessThan(1_000); // the realtime row: a second client within 1 s
      expect(deltaFor("A", "sub-A")).toBeUndefined(); // A is deferred, not reset
      expect(toB!.payload.sockets.B!.watermark).toBe(revision);

      release();
      for (let spin = 0; spin < 2000 && !deltaFor("A", "sub-A"); spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      await engine.settled();
      // A: its snapshot, then the deferred commit from the log, then the watermark; nothing reset either holder.
      const framesA = log.filter((entry) => entry.host === "h1").flatMap((entry) => (entry.payload.sockets.A?.ops ?? []).filter((op) => op[0] === "f").map((op) => JSON.parse(op[1] as string).type as string));
      expect(framesA.indexOf("replica.delta")).toBeGreaterThan(framesA.indexOf("replica.ready"));
      const marks = log.filter((entry) => entry.host === "h1").map((entry) => entry.payload.sockets.A?.watermark ?? entry.payload.watermark).filter((mark): mark is number => mark !== undefined);
      expect(marks).toEqual([...marks].sort((a, b) => a - b));
      expect(marks[marks.length - 1]).toBe(revision);
      expect(recoveries).toBe(0);
      expect(engine.stats.resets).toBe(0);
      expect(log.some((entry) => (entry.payload.sockets.B?.ops ?? []).some((op) => op[0] === "f" && (op[1] as string).includes('"replica.reset"')))).toBe(false);
    }, MODULE, false, undefined, undefined, ["h1", "h2"]);
  });

  /** Holder h1's socket A is held by its large first snapshot (its RPC waits on `release`). */
  const heldLoad = async (h: Harness, extra?: (view: SocketView) => Promise<void>) => {
    const insert: Array<[string, unknown[]]> = [];
    for (let i = 0; i < 7_500; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 200)]]);
    for (let i = 0; i < insert.length; i += 2500) await h.commit(insert.slice(i, i + 2500));
    const log: Array<{ host: string; payload: ReplicaDelivery }> = [];
    const state = { held: false, recoveries: 0, release: () => {} };
    const gate = new Promise<void>((resolve) => { state.release = resolve; });
    h.receiver.deliver = async (host, payload) => {
      log.push({ host, payload });
      if (payload.recovery) { state.recoveries += 1; return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt; }
      if (host === "h1" && !state.held && (payload.sockets.A?.ops ?? []).some((op) => op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.ready"'))) {
        state.held = true; await gate;
      }
      return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
    };
    const view = socket("A", {}, "h1");
    await extra?.(view);
    h.engine.dispatch(view, { type: "replica.open", id: "sub-A", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
    for (let spin = 0; spin < 2000 && !state.held; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
    expect(state.held).toBe(true);
    const marks = () => log.filter((e) => e.host === "h1").map((e) => e.payload.sockets.A?.watermark ?? e.payload.watermark).filter((m): m is number => m !== undefined);
    const until = async (done: () => boolean) => { for (let spin = 0; spin < 4000 && !done() && !state.recoveries; spin++) await new Promise((resolve) => setTimeout(resolve, 5)); };
    return { log, state, marks, until };
  };

  it("never certifies a deferred holder's live query with a watermark before its result was sent", { timeout: 60_000 }, async () => {
    await withEngine(async (h) => {
      const { log, state, marks, until } = await heldLoad(h, async (view) => {
        h.engine.observe(view);
        expect(JSON.parse(await h.live.subscribe("A", view.tenant!, { id: "live", path: "items.live", args: {} })).type).toBe("query.result");
      });
      const revision = await h.commit([["UPDATE items SET label = 'changed-while-deferred' WHERE id = 'i000000'", []]], false);
      for (let spin = 0; spin < 40; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(marks().some((m) => m >= revision), "A is deferred while its snapshot drains").toBe(false);
      state.release();
      await until(() => marks().some((m) => m >= revision));
      await h.engine.settled();
      expect(state.recoveries).toBe(0);
      // Every op to A in order; the first watermark at or past the commit must come after the changed live result.
      const ordered = log.filter((e) => e.host === "h1" && e.payload.sockets.A).flatMap((e) => [
        ...e.payload.sockets.A!.ops.filter((op) => op[0] === "f").map((op) => op[1] as string),
        ...(e.payload.sockets.A!.watermark !== undefined ? [`mark:${e.payload.sockets.A!.watermark}`] : []),
      ]);
      const certified = ordered.findIndex((text) => text.startsWith("mark:") && Number(text.slice(5)) >= revision);
      const result = ordered.findIndex((text) => text.includes('"type":"query.result"') && text.includes("changed-while-deferred"));
      expect(certified).toBeGreaterThanOrEqual(0);
      expect(result, "the changed live result reaches A").toBeGreaterThanOrEqual(0);
      expect(result).toBeLessThan(certified);
    }, MODULE, true, undefined, undefined, ["h1"]);
  });

  it("replays a long deferral in payloads that fit: 300 commits, no reset, in order, watermark to the last", { timeout: 120_000 }, async () => {
    await withEngine(async (h) => {
      const { log, state, marks, until } = await heldLoad(h);
      let last = 0;
      for (let n = 0; n < 300; n++) last = await h.commit([[`UPDATE items SET label = 'c${n}' WHERE id = 'i000001'`, []]], false);
      state.release();
      await until(() => marks().includes(last));
      await h.engine.settled();
      expect(state.recoveries).toBe(0);
      expect(h.engine.stats.resets).toBe(0);
      const seen = marks();
      expect(seen).toEqual([...seen].sort((a, b) => a - b));
      expect(seen[seen.length - 1]).toBe(last);
      const labels = log.filter((e) => e.host === "h1").flatMap((e) => (e.payload.sockets.A?.ops ?? []).filter((op) => op[0] === "f" && (op[1] as string).includes('"type":"replica.delta"')).map((op) => /"label":"(c\d+)"/.exec(op[1] as string)?.[1]));
      expect(labels.length).toBeGreaterThan(0);
      expect(labels[labels.length - 1]).toBe("c299");
      expect(h.conflicts, "catch-up, commits and opens share one invocation queue").toEqual([]);
    }, MODULE, false, undefined, undefined, ["h1"]);
  });

  it("still resets a holder that stops making progress, after the stall bound, and then converges", { timeout: 120_000 }, async () => {
    const { recoveries, ready } = await load(2_000, 4, { hold: 5_000, stallMs: 50 });
    expect(recoveries).toBeGreaterThanOrEqual(1);
    expect(recoveries).toBeLessThanOrEqual(2);
    expect(ready).toBe(4);
  });
});

describe("BUG-10 a live query's initial reply waits for room on a full channel", () => {
  // -12 on 6e76ce42, 09:08:52 Z: a fresh session's live-query reply met a full holder channel; `queryReply` posted it
  // without waiting, the outlet refused it (DeliveryCapacityError) and the whole holder was reset change-feed-lagged:
  // 81 replicas, templates.list and priorities.list among them, so the create dialog showed no templates.
  it("holds the reply until the in-flight payload completes, then sends it; no reset", { timeout: 120_000 }, async () => {
    let recoveries = 0, hold = false;
    const held: Array<() => void> = [];
    await withEngine(async (h) => {
      const insert: Array<[string, unknown[]]> = [];
      for (let i = 0; i < 4_000; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 1500)]]);
      for (let i = 0; i < insert.length; i += 1000) await h.commit(insert.slice(i, i + 1000));
      h.receiver.deliver = async (_host, payload) => {
        if (payload.recovery) { recoveries += 1; return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt; }
        if (hold) await new Promise<void>((resolve) => held.push(resolve));
        return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
      };
      h.engine.dispatch(socket("S1"), { type: "replica.open", id: "all", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      for (let spin = 0; spin < 4000 && !frames(h.payloads, "S1").some((f) => f.type === "replica.ready"); spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      await h.engine.settled();
      // A commit's delta (400 rows) is in flight on h1 and stays there.
      hold = true;
      await h.commit([["UPDATE items SET label = 'busy-' || label WHERE rank < 400", []]], false);
      for (let spin = 0; spin < 2000 && !held.length; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(held.length).toBe(1);
      // A live query on the same holder: its initial result (every row, over the holder's budget) can go only alone.
      const view = { ...socket("Q"), queryId: "live" };
      const subscribed = h.engine.querySubscribe(view, (db) => h.live.subscribeSnapshot(db, view.connectionId, view.tenant, { id: "live", path: "items.live", args: {} }));
      for (let i = 0; i < 40; i++) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(recoveries).toBe(0);
      expect(frames(h.payloads, "Q")).toEqual([]);
      hold = false;
      held.splice(0).forEach((release) => release());
      expect(await subscribed).toEqual([]);
      for (let spin = 0; spin < 4000 && !frames(h.payloads, "Q").some((f) => f.type === "query.result"); spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      await h.engine.settled();
      const replies = frames(h.payloads, "Q").filter((f) => f.type === "query.result");
      expect(replies.map((f) => [f.reason, f.result.length])).toEqual([["initial", 4_000]]);
      expect(recoveries).toBe(0);
      expect(frames(h.payloads, "S1").filter((f) => f.type === "replica.reset")).toEqual([]);
      expect(h.conflicts).toEqual([]);
    }, MODULE, true, undefined, 60_000);
  });

  it("r4617752119: a slow holder's deferred reply holds no turn: another holder's reducer commit and delta go at once, then the reply leads in order", { timeout: 120_000 }, async () => {
    let recoveries = 0, hold = false;
    const held: Array<() => void> = [];
    await withEngine(async (h) => {
      const insert: Array<[string, unknown[]]> = [];
      for (let i = 0; i < 4_000; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 1500)]]);
      for (let i = 0; i < insert.length; i += 1000) await h.commit(insert.slice(i, i + 1000));
      h.receiver.deliver = async (host, payload) => {
        if (payload.recovery) { recoveries += 1; return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt; }
        if (host === "h1" && hold) await new Promise<void>((resolve) => held.push(resolve));
        return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
      };
      h.engine.dispatch(socket("S1", {}, "h1"), { type: "replica.open", id: "all", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      h.engine.dispatch(socket("S2", {}, "h2"), { type: "replica.open", id: "top", path: "items.top", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      const ready = (id: string) => frames(h.payloads, id).some((f) => f.type === "replica.ready");
      for (let spin = 0; spin < 4000 && !(ready("S1") && ready("S2")); spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      await h.engine.settled();
      // h1 is slow: its next RPC does not complete until released.
      hold = true;
      await h.commit([["UPDATE items SET label = 'busy-' || label WHERE rank < 400", []]], false);
      for (let spin = 0; spin < 2000 && !held.length; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(held.length).toBe(1);
      // A live query on h1 whose result does not fit beside the in-flight payload: the subscribe returns at once.
      const view = { ...socket("Q", {}, "h1"), queryId: "live" };
      const started = Date.now();
      expect(await h.engine.querySubscribe(view, (db) => h.live.subscribeSnapshot(db, view.connectionId, view.tenant, { id: "live", path: "items.live", args: {} }))).toEqual([]);
      // Another holder's reducer commit is neither blocked nor delayed: it commits, and h2 gets its delta while h1 is still stuck.
      await h.commit([["UPDATE items SET label = 'moved' WHERE id = 'i000000'", []]], false);
      for (let spin = 0; spin < 2000 && !frames(h.payloads, "S2").some((f) => f.type === "replica.delta" && JSON.stringify(f).includes("moved")); spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(frames(h.payloads, "S2").some((f) => f.type === "replica.delta" && JSON.stringify(f).includes("moved"))).toBe(true);
      expect(Date.now() - started).toBeLessThan(10_000);
      expect(held.length).toBe(1);
      expect(frames(h.payloads, "Q")).toEqual([]);
      expect(recoveries).toBe(0);
      // h1 drains: the reply goes first, then the commit it owes, in order; nothing is reset.
      hold = false;
      held.splice(0).forEach((release) => release());
      for (let spin = 0; spin < 4000 && frames(h.payloads, "Q").filter((f) => f.type === "query.result").length < 2; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      await h.engine.settled();
      const replies = frames(h.payloads, "Q").filter((f) => f.type === "query.result");
      // The deferred holder's catch-up re-snapshots its live queries (BUG-4 `applyFeedSnapshot` reset): "reconnect".
      expect(replies.map((f) => f.reason)).toEqual(["initial", "reconnect"]);
      expect(replies[0].result.length).toBe(4_000);
      expect(replies[1].result.find((row: { id: string }) => row.id === "i000000").label).toBe("moved");
      expect(recoveries).toBe(0);
      expect(h.conflicts).toEqual([]);
    }, MODULE, true, undefined, 60_000, ["h1", "h2"]);
  });
});

describe("BUG-8 apiKeys.list visibility is the Member's direct permission grant, as in Rust", () => {
  // whagons5-client 678bdc80 gonvex/apiKeys.ts: `apiKeys.create` (Action) -> `apiKeys.createRecord` (Reducer) authorizes
  // through the Member's roles (hasSettingsPermission), but `apiKeysVisibility` admits rows with
  // `{operator: "permission", value: "config.integrations"}`, which both runtimes evaluate against the Member's direct
  // `members.permissions` JSON only (Rust visibility.rs `resolve_principal` / visibility_class.rs BitAtom::Permission;
  // the SQL form reads `json_each(_gonvex_member.permissions)`). The walkthrough admin came from
  // `/dev/internal/e2e/members` (Rust lib.rs:1301-1326, kernel reducer/object.ts), which seeds role admin with
  // permissions {"e2e":true}: its keys are stored and, on either backend, never visible to it.
  const APP: ReplicaModule = {
    ...MODULE,
    functions: new Map([["apiKeys.list", { path: "apiKeys.list", kind: "query", delivery: "replica", replica: parseDefinition({
      table: "apiKeys", key: "_id",
      columns: ["_id", "_creationTime", "name", "keyHint", "scopes", "isActive", "lastUsedAt", "expiresAt", "revokedAt", "createdByUserId"],
      excludeWhenSet: ["revokedAt"], orderBy: "_creationTime", orderDirection: "desc", mode: "eager", maxRows: 10000, maxBytes: 33554432,
    }) }]]),
    visibility: new Map([["apiKeys", parsePlan({
      table: "apiKeys", key: "_id", sets: {}, where: { operator: "permission", value: "config.integrations" }, version: 2,
      permissionTables: ["teams", "memberTeams", "workspaceTeams", "memberSpots", "spots", "members", "globalSettings", "workspaces", "personalWorkspaceMembers", "plugins", "permissions", "roles", "rolePermissions", "conversationParticipants", "boardMembers", "workspaceViewShares", "workbenchShares", "marketMembers", "templates", "categories", "formPublicLinks"],
    })]]),
  };
  // The row `apiKeys.createRecord` inserts (no revokedAt, isActive true, createdByUserId = the creator's Member id).
  const insertKey = (id: string, at: number): [string, unknown[]] =>
    ["INSERT INTO apiKeys (_id, _creationTime, name, keyHash, keyHint, scopes, isActive, createdByUserId) VALUES ($1, $2, $3, 'h', 'abcd', '[\"ext:read\",\"ext:tasks:write\"]', 1, 'm-alice')", [id, at, `key ${id}`]];
  const open = (engine: ReplicaDeliveryEngine, id: string) =>
    engine.dispatch(socket("s1"), { type: "replica.open", id, path: "apiKeys.list", args: {}, keys: [], hashes: {}, fullIntegrity: false });
  const rowsOf = (payloads: Array<[string, ReplicaDelivery]>) => frames(payloads, "s1").filter((f) => f.type === "replica.snapshotPage").flatMap((p) => p.result);

  it("an admin seeded with {\"e2e\":true} sees none of its own keys; a direct config.integrations grant shows them, newest first", async () => {
    await withEngine(async ({ engine, payloads, commit }) => {
      await commit([["UPDATE members SET role = 'admin', permissions = '{\"e2e\":true}' WHERE id = 'm-alice'", []]]);
      await commit([insertKey("k1", 1000)]);
      open(engine, "hidden");
      await engine.settled();
      const hidden = frames(payloads, "s1");
      expect(hidden.find((f) => f.type === "replica.ready")).toBeDefined();
      expect(rowsOf(payloads)).toEqual([]);
      engine.dispatch(socket("s1"), { type: "replica.close", id: "hidden" });
      await engine.settled();

      await commit([["UPDATE members SET permissions = '{\"e2e\":true,\"config.integrations\":true}' WHERE id = 'm-alice'", []]]);
      payloads.length = 0;
      open(engine, "granted");
      await engine.settled();
      expect(rowsOf(payloads).map((r: { _id: string }) => r._id)).toEqual(["k1"]);
      expect(rowsOf(payloads)[0]).toMatchObject({ isActive: 1, revokedAt: null, createdByUserId: "m-alice" });

      // A key created while the list is open arrives as a delta; a revoked key leaves it (excludeWhenSet).
      payloads.length = 0;
      await commit([insertKey("k2", 2000)]);
      expect(frames(payloads, "s1").map((f) => f.type)).toEqual(["replica.transaction", "replica.delta", "replica.ready"]);
      payloads.length = 0;
      await commit([["UPDATE apiKeys SET revokedAt = 3000, isActive = 0 WHERE _id = 'k1'", []]]);
      const revoked = frames(payloads, "s1").find((f) => f.type === "replica.delta");
      expect(JSON.stringify(revoked)).toContain("k1");
    }, APP);
  });
});

describe("BUG-4b one large commit is delivered, never reset lagged", () => {
  // -12 on dd5a9bee, ~03:07:50 Z: right after a UI bulk reassign (one commit over many rows), one burst of 108
  // `change-feed-lagged` resets on a holder that was not stalled. Rust writes a large transaction to the socket.
  const bulk = async (rows: number, hosts: string[], rpcMs: number) => {
    let recoveries = 0;
    const deltas = new Map<string, number>();
    await withEngine(async (h) => {
      const insert: Array<[string, unknown[]]> = [];
      for (let i = 0; i < rows; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 200)]]);
      for (let i = 0; i < insert.length; i += 2500) await h.commit(insert.slice(i, i + 2500));
      h.receiver.deliver = async (host, payload) => {
        await new Promise((resolve) => setTimeout(resolve, rpcMs));
        if (payload.recovery) { recoveries += 1; return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt; }
        for (const [id, work] of Object.entries(payload.sockets)) for (const op of work.ops) {
          if (op[0] === "f" && (op[1] as string).includes('"type":"replica.delta"') && (op[1] as string).includes("reassigned")) deltas.set(id, (deltas.get(id) ?? 0) + 1);
        }
        return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
      };
      for (const [n, host] of hosts.entries()) h.engine.dispatch(socket(`S${n}`, {}, host), { type: "replica.open", id: `sub-${n}`, path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      const ready = () => new Set(h.payloads.flatMap(([, p]) => Object.entries(p.sockets).filter(([, w]) => w.ops.some((op) => op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.ready"'))).map(([id]) => id)));
      for (let spin = 0; spin < 4000 && ready().size < hosts.length; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      await h.engine.settled();
      expect(ready().size).toBe(hosts.length);
      // The bulk reassign: one transaction over every row, a delta far over the holder's 8 MiB credit budget.
      await h.commit([["UPDATE items SET label = 'reassigned ' || id", []]], false);
      for (let spin = 0; spin < 4000 && deltas.size < hosts.length && !recoveries; spin++) await new Promise((resolve) => setTimeout(resolve, 5));
      await h.engine.settled();
      expect(h.conflicts).toEqual([]);
    }, MODULE, false, undefined, undefined, hosts);
    return { recoveries, delivered: deltas.size };
  };

  /** Deferral timers on a clock the test advances; between timers the real event loop runs the engine's I/O. */
  const manualClock = () => {
    let now = 0, next = 0;
    const timers = new Map<number, { at: number; fn: () => void }>();
    const turn = async () => { for (let i = 0; i < 20; i++) await new Promise((resolve) => setTimeout(resolve, 0)); };
    return {
      clock: {
        now: () => now,
        setTimeout: (fn: () => void, ms: number) => { timers.set(++next, { at: now + ms, fn }); return next; },
        clearTimeout: (handle: unknown) => { timers.delete(handle as number); },
      } satisfies DeliveryClock,
      /** Runs every timer due within `ms`, in order, letting the engine's work settle after each. */
      async advance(ms: number) {
        const end = now + ms;
        await turn();
        for (;;) {
          const due = [...timers].filter(([, t]) => t.at <= end).sort((x, y) => x[1].at - y[1].at || x[0] - y[0])[0];
          if (!due) break;
          timers.delete(due[0]);
          now = Math.max(now, due[1].at);
          due[1].fn();
          await turn();
        }
        now = end;
        await turn();
      },
    };
  };

  it("a commit over the budget is never reset while another holder keeps the isolate busy; only a stalled holder is (explicit ticks)", { timeout: 120_000 }, async () => {
    // Holder h1 holds every row; h2 holds two rows over an RPC the test resolves by hand. Time moves only by
    // `advance`: the outcome depends on stall ticks, not on how fast this machine builds the bulk payload.
    const STALL = 30_000, TICK = STALL / STALL_TICKS;
    const { clock, advance } = manualClock();
    let recoveries = 0;
    const recovered: string[] = [];
    let holdH2 = false;
    const heldH2: Array<() => void> = [];
    const bulkTo = new Set<string>(), againTo = new Set<string>();
    await withEngine(async (h) => {
      const insert: Array<[string, unknown[]]> = [];
      for (let i = 0; i < 6_000; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 200)]]);
      for (let i = 0; i < insert.length; i += 2500) await h.commit(insert.slice(i, i + 2500));
      h.receiver.deliver = async (host, payload) => {
        if (host === "h2" && holdH2 && !payload.recovery) await new Promise<void>((resolve) => heldH2.push(resolve));
        if (payload.recovery) { recoveries += 1; recovered.push(host); return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt; }
        for (const [id, work] of Object.entries(payload.sockets)) {
          if (work.ops.some((op) => op[0] === "f" && (op[1] as string).includes('"type":"replica.delta"') && (op[1] as string).includes("bulk-"))) bulkTo.add(id);
          if (work.ops.some((op) => op[0] === "f" && (op[1] as string).includes('"type":"replica.delta"') && (op[1] as string).includes("again-"))) againTo.add(id);
        }
        return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
      };
      h.engine.dispatch(socket("S1", {}, "h1"), { type: "replica.open", id: "all", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      h.engine.dispatch(socket("S2", {}, "h2"), { type: "replica.open", id: "top", path: "items.top", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      const ready = () => new Set(h.payloads.flatMap(([, p]) => Object.entries(p.sockets).filter(([, w]) => w.ops.some((op) => op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.ready"'))).map(([id]) => id)));
      for (let step = 0; step < 200 && ready().size < 2; step++) { await advance(TICK); await h.engine.settled(); }
      expect(ready().size).toBe(2);

      // 1. h2's RPC is in flight and stays there: the isolate's normal credits are not zero.
      holdH2 = true;
      await h.commit([["UPDATE items SET label = 'w1' WHERE id = 'i000000'", []]], false);
      for (let step = 0; step < 50 && !heldH2.length; step++) await advance(1);
      expect(heldH2.length).toBe(1);
      // 2. The bulk reassign: h1's payload is over the budget and can go only alone; h1's own channel is empty.
      await h.commit([["UPDATE items SET label = 'bulk-' || id WHERE id <> 'i000000'", []]], false);
      // 3. Just under the stall bound with h2 still in flight: nothing is reset, nothing over the budget went.
      for (let tick = 0; tick < STALL_TICKS - 2; tick++) await advance(TICK);
      expect({ recoveries, bulk: [...bulkTo] }).toEqual({ recoveries: 0, bulk: [] });
      // 4. h2 makes progress: its RPC completes. The isolate drains, h1's large payload goes, then h2's catch-up.
      holdH2 = false;
      heldH2.splice(0).forEach((release) => release());
      for (let tick = 0; tick < 4 * STALL_TICKS && bulkTo.size < 2; tick++) await advance(TICK);
      for (let tick = 0; tick < 2 * STALL_TICKS; tick++) await advance(TICK); // well past the bound: still nothing
      expect({ recoveries, bulk: [...bulkTo].sort() }).toEqual({ recoveries: 0, bulk: ["S1", "S2"] });

      // 5. A holder that really stops: h2's next RPC never completes. Exactly the stall bound in ticks resets it, and
      //    only it. (h2 is deferred behind h1's next large payload's claim, so its stall is watched.)
      const deferred = () => (h.engine as unknown as { behind: Map<string, unknown> }).behind.size;
      for (let tick = 0; tick < 4 * STALL_TICKS && deferred(); tick++) await advance(TICK);
      expect(deferred()).toBe(0);
      holdH2 = true;
      await h.commit([["UPDATE items SET label = 'w2' WHERE id = 'i000000'", []]], false);
      for (let step = 0; step < 50 && !heldH2.length; step++) await advance(1);
      await h.commit([["UPDATE items SET label = 'again-' || id WHERE id <> 'i000000'", []]], false);
      const resetsBefore = h.engine.stats.resets;
      for (let tick = 0; tick < STALL_TICKS - 2; tick++) await advance(TICK);
      expect(h.engine.stats.resets).toBe(resetsBefore);
      for (let tick = 0; tick < 4; tick++) await advance(TICK);
      // The engine reset h2 at the bound; its barrier waits on h2's channel behind the stuck RPC (as a stalled
      // socket's close would), so the holder sees it once that RPC returns.
      expect(h.engine.stats.resets).toBeGreaterThan(resetsBefore);
      expect(recovered).toEqual([]);
      // Its stuck RPC no longer holds the isolate (r4609198131): h1's large payload goes without waiting for it.
      for (let tick = 0; tick < 4 * STALL_TICKS && !againTo.has("S1"); tick++) await advance(TICK);
      expect(againTo.has("S1")).toBe(true);
      holdH2 = false;
      heldH2.splice(0).forEach((release) => release());
      for (let tick = 0; tick < 2 * STALL_TICKS && !recovered.length; tick++) await advance(TICK);
      expect(recovered).toEqual(["h2"]); // only the stalled holder
      expect(h.conflicts).toEqual([]);
    }, MODULE, false, undefined, STALL, ["h1", "h2"], clock);
  });

  it("r4609198131: a holder whose RPC never settles is reset at the stall bound and the large commit then goes (explicit ticks)", { timeout: 120_000 }, async () => {
    const STALL = 30_000, TICK = STALL / STALL_TICKS;
    const { clock, advance } = manualClock();
    const recovered: string[] = [];
    let stuck = false;
    const bulkTo = new Set<string>();
    await withEngine(async (h) => {
      const insert: Array<[string, unknown[]]> = [];
      for (let i = 0; i < 6_000; i++) insert.push(["INSERT INTO items VALUES ($1, $2, $3)", [`i${String(i).padStart(6, "0")}`, i, label(i, 200)]]);
      for (let i = 0; i < insert.length; i += 2500) await h.commit(insert.slice(i, i + 2500));
      h.receiver.deliver = async (host, payload) => {
        if (payload.recovery) { recovered.push(host); return { v: 1, sockets: 1, recoveryAck: 1 } as ReplicaReceipt; }
        if (host === "h2" && stuck) await new Promise<never>(() => {}); // this RPC never settles
        for (const [id, work] of Object.entries(payload.sockets)) {
          if (work.ops.some((op) => op[0] === "f" && (op[1] as string).includes('"type":"replica.delta"') && (op[1] as string).includes("bulk-"))) bulkTo.add(id);
        }
        return { v: 1, sockets: Object.keys(payload.sockets).length } as ReplicaReceipt;
      };
      h.engine.dispatch(socket("S1", {}, "h1"), { type: "replica.open", id: "all", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      h.engine.dispatch(socket("S2", {}, "h2"), { type: "replica.open", id: "top", path: "items.top", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      const ready = () => new Set(h.payloads.flatMap(([, p]) => Object.entries(p.sockets).filter(([, w]) => w.ops.some((op) => op[0] === "f" && (op[1] as string).startsWith('{"type":"replica.ready"'))).map(([id]) => id)));
      for (let step = 0; step < 200 && ready().size < 2; step++) { await advance(TICK); await h.engine.settled(); }
      expect(ready().size).toBe(2);
      // h2's next RPC never settles; h2 then has nothing more to send, so it is never deferred by its own traffic.
      stuck = true;
      await h.commit([["UPDATE items SET label = 'w1' WHERE id = 'i000000'", []]], false);
      for (let step = 0; step < 50 && !h.payloads.some(([host, p]) => host === "h2" && p.watermark !== undefined && JSON.stringify(p).includes("w1")); step++) await advance(1);
      // The bulk reassign leaves h2's two rows alone: only h1's payload, over the budget, claims the isolate.
      await h.commit([["UPDATE items SET label = 'bulk-' || id WHERE id NOT IN ('i000000', 'i000001')", []]], false);
      const resetsBefore = h.engine.stats.resets;
      for (let tick = 0; tick < STALL_TICKS - 2; tick++) await advance(TICK);
      expect({ resets: h.engine.stats.resets - resetsBefore, bulk: [...bulkTo] }).toEqual({ resets: 0, bulk: [] });
      for (let tick = 0; tick < 8; tick++) await advance(TICK);
      // h2 hit the stall rule: it is reset, its stuck RPC no longer holds the isolate, and h1's large commit goes.
      expect(h.engine.stats.resets).toBeGreaterThan(resetsBefore);
      expect([...bulkTo]).toEqual(["S1"]);
      expect(recovered).not.toContain("h1");
      expect(h.conflicts).toEqual([]);
    }, MODULE, false, undefined, STALL, ["h1", "h2"], clock);
  });

  it("one holder", { timeout: 120_000 }, async () => {
    expect(await bulk(6_000, ["h1"], 2)).toEqual({ recoveries: 0, delivered: 1 });
  });
  it("three holders sharing the isolate's credits", { timeout: 120_000 }, async () => {
    expect(await bulk(6_000, ["h1", "h2", "h3"], 20)).toEqual({ recoveries: 0, delivered: 3 });
  });
});
