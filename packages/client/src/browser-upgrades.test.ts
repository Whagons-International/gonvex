import "fake-indexeddb/auto";
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
