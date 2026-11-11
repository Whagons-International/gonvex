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
import type { ReplicaDelivery } from "../protocol.ts";
import { SocketDelivery, type TrackedReplicas } from "../socket.ts";
import { PAGE_BYTES, PAGE_ROWS } from "../subscription.ts";

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
  payloads: Array<[string, ReplicaDelivery]>;
  commit(statements: Array<[string, unknown[]]>): Promise<number>;
}

async function withEngine<T>(body: (harness: Harness) => Promise<T>): Promise<T> {
  const stub = env.HELLO.getByName(`k10-${++objects}-${Math.random()}`);
  return runInDurableObject(stub, async (_instance, state) => {
    const sql = directSql(state.storage.sql);
    state.storage.transactionSync(() => {
      installChangeFeed(sql);
      sql.run("CREATE TABLE members (id TEXT PRIMARY KEY, account_id TEXT, status TEXT, role TEXT, permissions JSON)");
      sql.run("CREATE TABLE items (id TEXT PRIMARY KEY, rank INTEGER, label TEXT)");
      sql.run("CREATE INDEX items_rank ON items (rank)");
      installChangeFeed(sql);
      sql.run("INSERT INTO members VALUES ('m-alice', 'acct-alice', 'active', 'admin', '{}')");
    });
    const storage = new TenantStorage(state.storage);
    const context = { state: new CaptureState(), wake: new ChangeWake() };
    let queue: Promise<unknown> = Promise.resolve();
    const payloads: Array<[string, ReplicaDelivery]> = [];
    const engine = new ReplicaDeliveryEngine({
      tenantId: "t",
      serial: <R>(fn: () => Promise<R>) => { const next = queue.then(fn, fn); queue = next.catch(() => undefined); return next; },
      read: async (fn) => { const r = await storage.invoke("query", async (s) => fn(s)); if (!r.ok) throw r.error; return r.value; },
      module: () => MODULE,
      outlet: new LocalOutlet((host, payload) => { payloads.push([host, payload]); return { v: 1, sockets: Object.keys(payload.sockets).length }; }, () => ["h1"]),
    });
    let commands = 0;
    return body({
      engine, payloads,
      async commit(statements) {
        const result = await invokeCaptured(storage, "reducer", { commandId: `c${++commands}` }, async (s) => {
          for (const [text, parameters] of statements) s.run(text, parameters);
        }, context);
        if (!result.ok) throw result.error;
        const revision = result.notice?.revision ?? 0;
        engine.wake(revision);
        await engine.settled();
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
