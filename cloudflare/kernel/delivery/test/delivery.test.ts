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
import { ReplicaDeliveryEngine } from "../engine.ts";
import { LocalOutlet } from "../outlet.ts";
import type { ReplicaDelivery, ReplicaReceipt } from "../protocol.ts";
import type { ScopedSocketWork } from "../recovery.ts";
import { SocketDelivery, type TrackedReplicas } from "../socket.ts";
import { LiveQueries, snapshotRunner, kernelVisibility, moduleFromManifest } from "../../query/index";
import { VisibilityRegistry } from "../../visibility";
import { PAGE_BYTES, PAGE_ROWS } from "../subscription.ts";
import { keepAliveGrace, KEEPALIVE_GRACE_MS, SubscriberKeepAlive, type KeepAliveClock } from "../keepalive.ts";

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
  /** `settle: false` returns once the commit is published, while delivery may still be running (a concurrent writer). */
  commit(statements: Array<[string, unknown[]]>, settle?: boolean): Promise<number>;
}

async function withEngine<T>(body: (harness: Harness) => Promise<T>, module: ReplicaModule = MODULE, composeLive = false, activity?: () => void, stallMs?: number): Promise<T> {
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
    const live = new LiveQueries({ run: snapshotRunner(storage), module: () => liveModule, visibility: kernelVisibility(new VisibilityRegistry()) });
    const receive = (host:string,payload:ReplicaDelivery) => { payloads.push([host,payload]); return (receiver.deliver?.(host,payload) ?? {v:1,sockets:Object.keys(payload.sockets).length}) as ReplicaReceipt; };
    const engine = new ReplicaDeliveryEngine({
      ...(composeLive ? { live } : {}),
      tenantId: "t",
      delivered: (revision) => delivered.push(revision),
      serial: <R>(fn: () => Promise<R>) => { const next = queue.then(fn, fn); queue = next.catch(() => undefined); return next; },
      read: async (fn) => { const r = await storage.invoke("query", async (s) => fn(s)); if (!r.ok) throw r.error; return r.value; },
      module: () => module,
      outlet: new LocalOutlet(receive, () => ["h1"], undefined, receive),
      ...(activity ? { activity } : {}),
      ...(stallMs !== undefined ? { stallMs } : {}),
    });
    let commands = 0;
    return body({
      engine, live, payloads, receiver, delivered,
      async commit(statements, settle = true) {
        const result = await invokeCaptured(storage, "reducer", { commandId: `c${++commands}` }, async (s) => {
          for (const [text, parameters] of statements) s.run(text, parameters);
        }, context);
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
      keep = new SubscriberKeepAlive(() => engine.holdsSubscriptions(), KEEPALIVE_GRACE_MS, clock);
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

  it("arms the keep-alive for a socket whose only subscription is a live query, once the query is installed", async () => {
    const { clock, advance } = manualClock();
    let keep: SubscriberKeepAlive | undefined;
    await withEngine(async ({ engine, live, commit }) => {
      keep = new SubscriberKeepAlive(() => engine.holdsSubscriptions(), KEEPALIVE_GRACE_MS, clock);
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

  it("lets a subscribed tenant hibernate after the grace with no event, and extends the grace on every event", () => {
    const { clock, advance } = manualClock();
    let subscribed = true;
    const keep = new SubscriberKeepAlive(() => subscribed, 60_000, clock);
    keep.touch();
    advance(50_000); keep.touch();
    advance(50_000);
    expect(keep.holding).toBe(true); // 100 s after the first event, 50 s after the last
    advance(10_001);
    expect(keep.holding).toBe(false);
    keep.touch();
    expect(keep.holding).toBe(true);
    subscribed = false;
    keep.touch();
    expect(keep.holding).toBe(false);
    expect(new SubscriberKeepAlive(() => true, 0, clock).holding).toBe(false);
  });

  it("reads GONVEX_TENANT_KEEPALIVE_MS", () => {
    expect(keepAliveGrace(undefined)).toBe(KEEPALIVE_GRACE_MS);
    expect(keepAliveGrace("0")).toBe(0);
    expect(keepAliveGrace("30000")).toBe(30_000);
    expect(keepAliveGrace("-1")).toBe(KEEPALIVE_GRACE_MS);
    expect(keepAliveGrace("forever")).toBe(KEEPALIVE_GRACE_MS);
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

  it("still resets a holder that stops making progress, after the stall bound, and then converges", { timeout: 120_000 }, async () => {
    const { recoveries, ready } = await load(2_000, 4, { hold: 400, stallMs: 50 });
    expect(recoveries).toBeGreaterThanOrEqual(1);
    expect(recoveries).toBeLessThanOrEqual(2);
    expect(ready).toBe(4);
  });
});
