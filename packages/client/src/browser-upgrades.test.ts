import "fake-indexeddb/auto";
import { Dexie } from "dexie";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { browserUpgradeStorage } from "./browser-upgrades.js";
import { IndexedDBLocalReplicaStorage } from "./indexeddb-replica.js";

const opened: ReturnType<typeof browserUpgradeStorage>[] = [];
let prefix = "";
beforeEach(() => {
  prefix = `upgrade-${Math.random()}`;
  vi.stubGlobal("navigator", { locks: { request: (_name: string, _options: unknown, run: () => unknown) => Promise.resolve().then(run) } });
});
afterEach(() => { opened.splice(0).forEach(s => s.close()); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
function open(version: number, migrations: any[] = []) {
  const result = browserUpgradeStorage({ replicaName: prefix, outboxName: `${prefix}-queue`,
    contract: { version, offlineMaxAgeMs: 7 * 86400000 }, initialVersion: 1, migrations });
  opened.push(result); return result;
}
async function seed() {
  const old = open(1); await old.ready;
  await old.storage.replaceSnapshot!({ entities: { tasks: { a: { title: "Hello" } } }, liveQueries: {} }, "tenant-a");
  await old.store.put({ id: 1, scope: "alice/tenant-a", path: "rename", args: { title: "World" },
    idempotencyKey: "stable", entityKeys: ["a"], createdAt: 1, attempts: 1, nextAttemptAt: 0, state: "inflight" });
  return old;
}
const migrations = [{ from: 1, to: 2, replica: (s: any) => {
  const row = s.entities.tasks.a; row.name = row.title; delete row.title; return s;
}, intent: (i: any) => ({ path: "setName", args: { name: i.args.title } }) }];

it("boots a fresh installation after obsolete migration paths are retired", async () => {
  const fresh = open(5); await fresh.ready;
  expect(await fresh.store.load()).toEqual([]);
});

it("allocates distinct durable sequence numbers for concurrent tabs", async () => {
  const first = open(1); await first.ready;
  const second = open(1); await second.ready;
  const ids = await Promise.all([first.store.allocateId!(), second.store.allocateId!()]);
  expect(new Set(ids).size).toBe(2);
});

it("migrates confirmed rows and durable intents, fencing already-open old tabs", async () => {
  const old = await seed();
  const current = open(2, migrations); await current.ready;
  expect((await current.storage.load("tenant-a"))?.entities.tasks?.a).toEqual({ name: "Hello" });
  expect(await current.store.load()).toEqual([expect.objectContaining({ args: { name: "World" }, receiptPath: "rename", idempotencyKey: "stable" })]);
  await expect(old.store.put((await current.store.load())[0]!)).rejects.toThrow("Reload");
});

it("finishes the staged upgrade after interruption without rerunning transforms", async () => {
  (await seed()).close();
  const original = IndexedDBLocalReplicaStorage.prototype.replaceSnapshot;
  const failing = vi.spyOn(IndexedDBLocalReplicaStorage.prototype, "replaceSnapshot").mockImplementationOnce(async function(this: IndexedDBLocalReplicaStorage, ...args) {
    await original.apply(this, args); throw new Error("Browser terminated after replica write");
  });
  const interrupted = open(2, migrations);
  await expect(interrupted.ready).rejects.toThrow("terminated"); interrupted.close(); failing.mockRestore();
  const recovered = open(2, [{ ...migrations[0], intent() { throw new Error("Must not run again"); } }]);
  await recovered.ready;
  expect((await recovered.storage.load("tenant-a"))?.entities.tasks?.a).toEqual({ name: "Hello" });
  expect((await recovered.store.load())[0]).toMatchObject({ path: "setName", args: { name: "World" }, idempotencyKey: "stable" });
});

it("keeps both original stores when a migration cannot preserve meaning", async () => {
  (await seed()).close();
  const broken = open(2, [{ from: 1, to: 2, intent() { throw new Error("Resolve status"); } }]);
  await expect(broken.ready).rejects.toThrow("Resolve status"); broken.close();
  const old = open(1); await old.ready;
  expect((await old.store.load())[0]?.args).toEqual({ title: "World" });
  expect((await old.storage.load("tenant-a"))?.entities.tasks?.a).toEqual({ title: "Hello" });
});


it('appends distinct durable intents across tabs and fences old versions', async () => {
  const first = open(1); await first.ready;
  const second = open(1); await second.ready;
  const draft = { scope: 'tenant', path: 'increment', args: {}, idempotencyKey: 'a', entityKeys: [], createdAt: 1, attempts: 0, nextAttemptAt: 1, state: 'pending' as const };
  const entries = await Promise.all([first.store.append!(draft), second.store.append!({ ...draft, idempotencyKey: 'b' })]);
  expect(entries.map(entry => entry.id).sort()).toEqual([1, 2]);
  expect(await first.store.load()).toHaveLength(2);
  const next = open(2, [{ from: 1, to: 2, intent: (intent: any) => intent }]); await next.ready;
  await expect(first.store.append!(draft)).rejects.toThrow('Reload');
  expect(await next.store.load()).toHaveLength(2);
});


it('never reuses append IDs after deletion when a later writer reserves an ID', async () => {
  const first = open(1); await first.ready;
  const draft = { scope: 'tenant', path: 'increment', args: {}, idempotencyKey: 'a', entityKeys: [], createdAt: 1, attempts: 0, nextAttemptAt: 1, state: 'pending' as const };
  const appended = await first.store.append!(draft);
  await first.store.delete(appended.id);
  const second = open(1); await second.ready;
  const reserved = await second.store.allocateId!();
  const next = await first.store.append!({ ...draft, idempotencyKey: 'b' });
  expect(reserved).toBeGreaterThan(appended.id);
  expect(next.id).toBeGreaterThan(reserved);
  expect((await first.store.load()).map(entry => entry.id)).toEqual([next.id]);
});


it('keeps the existing journal and shared sequence compatible with older readers', async () => {
  const legacy = new Dexie(`${prefix}-queue`);
  legacy.version(2).stores({ entries: '++id, scope, state, nextAttemptAt, [scope+state], [scope+nextAttemptAt]' });
  await legacy.table('entries').put({ id: 50, scope: 'tenant', state: 'pending', nextAttemptAt: 0 });
  await legacy.table('entries').delete(50);
  const metadata = new Dexie(`${prefix}-upgrades`);
  metadata.version(1).stores({ state: '&key' });
  await metadata.table('state').put({ key: 'sequence', version: 50 });
  try {
    const current = open(1); await current.ready;
    const draft = { scope: 'tenant', path: 'increment', args: {}, idempotencyKey: 'a', entityKeys: [], createdAt: 1, attempts: 0, nextAttemptAt: 1, state: 'pending' as const };
    const entry = await current.store.append!(draft);
    expect(entry.id).toBe(51);
    expect((await legacy.table('entries').get(entry.id)).idempotencyKey).toBe('a');
    expect((await metadata.table('state').get('sequence')).version).toBe(51);
  } finally { legacy.close(); metadata.close(); }
});
