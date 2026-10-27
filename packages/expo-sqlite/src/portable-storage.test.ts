import { afterEach, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import type { LocalReplicaSession, ReplicaRow, ReplicaWindow } from "@gonvex/client";
import { LocalReplica } from "../../client/src/local-replica.js";
import {
  ExpoSQLiteLocalReplicaStorage,
  type ExpoSQLiteDatabase,
} from "./index.js";

const databases: DatabaseSync[] = [];
const replicas: LocalReplica[] = [];
function createReplica(...args: ConstructorParameters<typeof LocalReplica>) {
  const replica = new LocalReplica(...args);
  replicas.push(replica);
  return replica;
}
afterEach(() => {
  for (const replica of replicas.splice(0)) replica.dispose();
  for (const db of databases.splice(0)) db.close();
});
function fixture() {
  const db = new DatabaseSync(":memory:");
  databases.push(db);
  const queries: Array<{ sql: string; params: unknown[] }> = [];
  const adapter: ExpoSQLiteDatabase = {
    async execAsync(sql) {
      db.exec(sql);
    },
    async runAsync(sql, ...params) {
      queries.push({ sql, params });
      return db.prepare(sql).run(...(params as any[]));
    },
    async getAllAsync<T>(sql: string, ...params: unknown[]) {
      queries.push({ sql, params });
      return db.prepare(sql).all(...(params as any[])) as T[];
    },
    async getFirstAsync<T>(sql: string, ...params: unknown[]) {
      queries.push({ sql, params });
      return (db.prepare(sql).get(...(params as any[])) as T) ?? null;
    },
    async withTransactionAsync(task) {
      db.exec("BEGIN");
      try {
        await task();
        db.exec("COMMIT");
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
  };
  return { storage: new ExpoSQLiteLocalReplicaStorage(adapter), queries, adapter };
}

it("loads a bounded working set and reads one cold row through the SQLite primary key", async () => {
  const { storage, queries } = fixture();
  const tasks = Object.fromEntries(
    Array.from({ length: 2000 }, (_, i) => [
      String(i),
      { _id: String(i), name: `Task ${i}`, count: i, note: "x".repeat(1000) },
    ]),
  );
  await storage.replaceSnapshot(
    { entities: { tasks }, liveQueries: {} },
    "tenant",
  );
  queries.length = 0;
  const snapshot = await storage.loadWorkingSet("tenant", {
    maxRows: 100,
    maxBytes: 3000,
  });
  expect(Object.keys(snapshot!.entities.tasks!)).toHaveLength(1);
  const selected = await storage.withReadView(
    "tenant",
    { tasks: { key: "_id", complete: true } },
    (view) =>
      view.select({
        table: "tasks",
        where: { column: "_id", op: "eq", value: "1999" },
      }),
  );
  expect(selected.rows).toEqual([tasks["1999"]]);
  expect(selected.complete).toBe(true);
  expect(
    queries.filter((query) => query.sql.includes("entity = ? AND id = ?")),
  ).toHaveLength(1);
  expect(
    (await storage.loadEntityRows("tenant", "tasks", ["1999"]))[0],
  ).toEqual({ id: "1999", row: tasks["1999"] });
});

it("combines bounded pages before projecting sorted results, without crossing tenant scope", async () => {
  const { storage } = fixture();
  const tasks = Object.fromEntries(
    Array.from({ length: 700 }, (_, i) => [
      String(i),
      { _id: String(i), name: `PR-${i}`, count: i },
    ]),
  );
  await storage.replaceSnapshot(
    { entities: { tasks }, liveQueries: {} },
    "tenant",
  );
  await storage.replaceSnapshot(
    {
      entities: {
        tasks: { secret: { _id: "secret", name: "PR-9999", count: 9999 } },
      },
      liveQueries: {},
    },
    "other",
  );
  const result = await storage.withReadView(
    "tenant",
    { tasks: { key: "_id", complete: true } },
    (view) =>
      view.select({
        table: "tasks",
        columns: ["name"],
        where: { column: "count", op: "gte", value: 500 },
        orderBy: [
          { column: "name", transform: "numericSuffix", direction: "desc" },
        ],
        limit: 3,
      }),
  );
  expect(result).toEqual({
    rows: [{ name: "PR-699" }, { name: "PR-698" }, { name: "PR-697" }],
    complete: true,
  });
});

it("persists only the incoming window projection and preserves other fields and cold rows", async () => {
  const { storage, queries } = fixture();
  const a = { _id: "a", name: "Old", status: "new" },
    b = { _id: "b", name: "Cold" };
  await storage.replaceSnapshot(
    { entities: { tasks: { a, b } }, liveQueries: {} },
    "tenant",
  );
  queries.length = 0;
  const window = {
    signature: "one",
    kind: "replica" as const,
    entity: "tasks",
    key: "_id",
    ids: ["a"],
    completeness: "complete" as const,
    source: "server" as const,
  };
  await storage.replaceWindow(
    window,
    { entities: { tasks: { a: { ...a, name: "stale" } } }, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "Changed" }],
  );
  expect((await storage.load("tenant"))!.entities.tasks).toEqual({
    a: { ...a, name: "Changed" },
    b,
  });
  const writes = queries.filter((query) =>
    query.sql.startsWith("INSERT INTO _gonvex_replica_entities"),
  );
  expect(writes).toHaveLength(1);
  expect(writes[0].params).toHaveLength(4);
});

it("uses secondary indexes for parent lookups and atomically maintains them after edits", async () => {
  const { storage, queries } = fixture();
  const links = Object.fromEntries(
    Array.from({ length: 1000 }, (_, i) => [
      String(i),
      { _id: String(i), parentId: `parent-${i % 100}` },
    ]),
  );
  await storage.replaceSnapshot(
    { entities: { links }, liveQueries: {} },
    "tenant",
  );
  queries.length = 0;
  const read = () =>
    storage.withReadView(
      "tenant",
      { links: { key: "_id", complete: true } },
      (view) =>
        view.select({
          table: "links",
          where: { column: "parentId", op: "eq", value: "parent-42" },
        }),
    );
  expect((await read()).rows).toHaveLength(10);
  expect(
    queries.some((query) => query.sql.includes("_gonvex_replica_lookup")),
  ).toBe(true);
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 2 },
      changes: [
        {
          entity: "links",
          id: "42",
          operation: "update",
          newValue: { _id: "42", parentId: "moved" },
        },
      ],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  expect((await read()).rows).toHaveLength(9);
});

it("merges projected authority and prevents stale snapshots from resurrecting deleted entities", async () => {
  const { storage } = fixture();
  const window = {
    signature: "one",
    kind: "replica" as const,
    entity: "tasks",
    key: "_id",
    ids: ["a"],
    completeness: "complete" as const,
    source: "server" as const,
    cursor: { epoch: "e", revision: 1 },
  };
  await storage.replaceWindow(
    window,
    { entities: {}, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "Old", status: "new" }],
  );
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 3 },
      changes: [
        {
          entity: "tasks",
          id: "a",
          operation: "update",
          newValue: { _id: "a", status: "working" },
        },
      ],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  await storage.replaceWindow(
    { ...window, signature: "other", cursor: { epoch: "e", revision: 2 } },
    { entities: {}, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "Renamed", status: "new" }],
  );
  expect(
    (await storage.loadEntityRows("tenant", "tasks", ["a"]))[0].row,
  ).toEqual({ _id: "a", name: "Renamed", status: "working" });
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 4 },
      changes: [{ entity: "tasks", id: "a", operation: "delete" }],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  await storage.replaceWindow(
    { ...window, signature: "late", cursor: { epoch: "e", revision: 3 } },
    { entities: {}, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "stale" }],
  );
  expect(await storage.loadEntityRows("tenant", "tasks", ["a"])).toEqual([]);
});

it("handles null predicates and only requires fields requested by the reducer", async () => {
  const { storage } = fixture();
  await storage.replaceSnapshot(
    {
      entities: {
        tasks: {
          a: { _id: "a", name: "A", deletedAt: null },
          b: { _id: "b", name: "B", deletedAt: 123 },
        },
      },
      liveQueries: {},
    },
    "tenant",
  );
  const coverage = {
    tasks: {
      key: "_id",
      complete: true,
      columns: ["_id", "name", "deletedAt", "unloadedDescription"],
    },
  };
  const selected = await storage.withReadView("tenant", coverage, (view) =>
    view.select({
      table: "tasks",
      columns: ["name"],
      where: { column: "deletedAt", op: "eq", value: null },
    }),
  );
  expect(selected).toEqual({ rows: [], complete: true });
  const nullable = await storage.withReadView("tenant", coverage, (view) =>
    view.select({
      table: "tasks",
      columns: ["name"],
      where: { column: "deletedAt", op: "isNull" },
    }),
  );
  expect(nullable).toEqual({ rows: [{ name: "A" }], complete: true });
  const mixed = await storage.withReadView("tenant", coverage, (view) =>
    view.select({
      table: "tasks",
      columns: ["name"],
      where: { column: "deletedAt", op: "in", values: [null, 123] },
    }),
  );
  expect(mixed.rows).toEqual([{ name: "B" }]);
});

it("never rewinds a cursor and clears obsolete row authority during a full replacement", async () => {
  const { storage } = fixture();
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 10 },
      changes: [{ entity: "tasks", id: "a", operation: "delete" }],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  await storage.advanceWatermark([], { epoch: "e", revision: 5 }, "tenant");
  expect((await storage.load("tenant"))!.cursor).toEqual({
    epoch: "e",
    revision: 10,
  });
  await storage.replaceSnapshot(
    {
      entities: { tasks: { a: { _id: "a", name: "Restored" } } },
      liveQueries: {},
      cursor: { epoch: "fresh", revision: 1 },
    },
    "tenant",
  );
  await storage.applyTransaction(
    {
      cursor: { epoch: "fresh", revision: 2 },
      changes: [
        {
          entity: "tasks",
          id: "a",
          operation: "update",
          newValue: { _id: "a", name: "Updated" },
        },
      ],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  expect(
    (await storage.loadEntityRows("tenant", "tasks", ["a"]))[0].row.name,
  ).toBe("Updated");
});

it("clears one replica scope for a cache reset without touching the offline session or other scopes", async () => {
  const { storage } = fixture();
  const identity = "identity\u0000ws://runtime\u0000account";
  await storage.saveSession(identity, { directive: { protocolVersion: 1, scope: "s", visibilityScope: "visible", epoch: "e" } } as any);
  await storage.replaceSnapshot({ entities: { tasks: { t1: { _id: "t1", name: "Cached" } } }, liveQueries: {} }, "visible");
  await storage.replaceSnapshot({ entities: { tasks: { t2: { _id: "t2", name: "Other" } } }, liveQueries: {} }, "other");
  await storage.clear("visible");
  expect((await storage.load("visible"))?.entities.tasks?.t1).toBeUndefined();
  expect((await storage.load("other"))?.entities.tasks?.t2).toMatchObject({ name: "Other" });
  expect(await storage.loadSession(identity)).toMatchObject({ directive: { visibilityScope: "visible" } });
});

function completeWindow(
  signature: string,
  entity: string,
  rows: ReplicaRow[],
  cursor = { epoch: "e", revision: 1 },
): ReplicaWindow {
  return {
    signature,
    kind: "replica",
    entity,
    key: "_id",
    ids: rows.map((row) => String(row._id)),
    completeness: "complete",
    source: "server",
    cursor,
  };
}

it.each(["rows", "bytes"] as const)(
  "marks omitted live tables under a %s budget and retries LocalReplica scans on SQLite",
  async (limit) => {
    const { storage, queries } = fixture();
    const row = (_id: string) => ({ _id, name: _id, note: "x".repeat(4096) });
    const first = [row("first")];
    const tasks = [row("a"), row("b"), row("c")];
    const last = [row("last")];
    const windows = [
      completeWindow("first", "aardvark", first),
      completeWindow("tasks", "tasks", tasks),
      completeWindow("last", "zulu", last),
    ];
    await storage.replaceSnapshot(
      {
        cursor: { epoch: "e", revision: 1 },
        entities: {
          aardvark: Object.fromEntries(first.map((row) => [row._id, row])),
          tasks: Object.fromEntries(tasks.map((row) => [row._id, row])),
          zulu: Object.fromEntries(last.map((row) => [row._id, row])),
        },
        liveQueries: Object.fromEntries(windows.map((window) => [window.signature, window])),
      },
      "tenant",
    );
    await storage.applyTransaction(
      {
        cursor: { epoch: "e", revision: 2 },
        changes: [{ entity: "tombstones", id: "deleted", operation: "delete" }],
      },
      { entities: {}, liveQueries: {} },
      "tenant",
    );
    await storage.replaceSnapshot(
      { entities: { otherOnly: { secret: row("secret") } }, liveQueries: {} },
      "other",
    );
    const bytes = (row: ReplicaRow) => JSON.stringify(row).length * 2 + 256;
    // The byte fixture admits two rows, then stops within the same SQL page.
    // The row fixture stops before asking SQLite for the later tables.
    const budget = limit === "rows"
      ? { maxRows: 2, maxBytes: 1024 * 1024 }
      : { maxRows: 10, maxBytes: bytes(first[0]) + bytes(tasks[0]) + Math.floor(bytes(tasks[1]) / 2) };
    const snapshot = await storage.loadWorkingSet("tenant", budget);
    expect(snapshot!.entities).toEqual({
      aardvark: { first: first[0] },
      tasks: { a: tasks[0] },
    });
    // Soft assertions let the baseline continue to the public scan checks.
    expect.soft(snapshot!.residentPartialTables).toEqual(["tasks", "zulu"]);
    expect(snapshot!.liveQueries.tasks.completeness).toBe("complete");
    expect(snapshot!.liveQueries.last.completeness).toBe("complete");
    expect.soft((await storage.loadWorkingSet("tenant", {
      maxRows: 10,
      maxBytes: 1024 * 1024,
    }))!.residentPartialTables).toEqual([]);

    const replica = createReplica(storage, {
      maxResidentRows: budget.maxRows,
      maxResidentBytes: budget.maxBytes,
    });
    await replica.activateScope("tenant");
    expect(replica.entityRows("tasks").length).toBeLessThanOrEqual(1);
    expect(replica.entityRows("zulu")).toEqual([]);
    expect.soft(replica.entityCompleteness("tasks")).toBe("partial");
    expect.soft(replica.entityCompleteness("zulu")).toBe("partial");
    if (limit === "rows") expect(replica.entityCompleteness("aardvark")).toBe("complete");
    const before = replica.snapshot().entities;
    queries.length = 0;
    for (const [table, expected] of [["tasks", tasks], ["zulu", last]] as const) {
      let attempts = 0;
      const selected = await replica.withReadView(
        () => ({ [table]: { key: "_id", complete: true, columns: ["_id", "name", "note"] } }),
        false,
        async (view) => {
          attempts++;
          const result = await view.select({ table, columns: ["_id", "name", "note"] });
          // As in the portable executor, incomplete coverage retries the whole
          // read through LocalReplica's actual SQLite fallback.
          if (!result.complete) {
            const error = new Error("scan needs complete disk coverage");
            error.name = "IncompleteReplicaError";
            throw error;
          }
          return result;
        },
      );
      expect.soft(attempts).toBe(2);
      expect.soft(selected).toEqual({ rows: expected, complete: true });
      expect.soft(queries.some(({ sql, params }) =>
        sql.includes("entity = ? AND id > ?") && params[0] === "tenant" && params[1] === table,
      )).toBe(true);
    }
    expect(replica.snapshot().entities).toEqual(before);
    expect.soft(replica.entityCompleteness("tasks")).toBe("partial");
    expect.soft(replica.entityCompleteness("zulu")).toBe("partial");
  },
);

it.each([
  { maxRows: 0, maxBytes: 1024 },
  { maxRows: 10, maxBytes: 0 },
  { maxRows: 10, maxBytes: 1 },
])("keeps data-only scopes visible when no live row fits %j", async (budget) => {
  const { storage } = fixture();
  await storage.replaceSnapshot(
    { entities: { tasks: { a: { _id: "a" } }, zulu: { z: { _id: "z" } } }, liveQueries: {} },
    "tenant",
  );
  const snapshot = await storage.loadWorkingSet("tenant", budget);
  expect(snapshot).toMatchObject({
    entities: {},
    liveQueries: {},
    residentPartialTables: ["tasks", "zulu"],
  });
  expect(await storage.loadWorkingSet("empty", budget)).toBeUndefined();
});

it("resets all SQLite epoch state before a LocalReplica snapshot, cold hydrate, and new transaction", async () => {
  const { storage, adapter } = fixture();
  const replica = createReplica(storage);
  await replica.activateScope("tenant");
  const materialize = (signature: string, entity: string, rows: ReplicaRow[], epoch: string, revision: number) => {
    const { ids: _ids, ...window } = completeWindow(signature, entity, rows, { epoch, revision });
    return replica.replaceWindow({ ...window, rows });
  };
  await materialize("tasks", "tasks", [
    { _id: "shared", name: "Old", oldOnly: "must disappear" },
    { _id: "retired", name: "Retired" },
  ], "old", 7);
  await materialize("extra", "tasks", [{ _id: "orphan", name: "Orphan" }], "old", 7);
  await materialize("statuses", "statuses", [{ _id: "s", name: "Old status" }], "old", 7);
  await replica.applyTransaction({
    cursor: { epoch: "old", revision: 8 },
    changes: [{ entity: "archive", id: "gone", operation: "delete" }],
  });
  const session: LocalReplicaSession = {
    directive: { protocolVersion: 1, scope: "tenant", visibilityScope: "tenant", epoch: "old" },
    identity: {
      auth: { account: { id: "account" } },
      tenant: { id: "tenant" },
      member: { id: "member", accountId: "account", permissions: {} },
    },
    artifactHash: "cached-artifact",
  };
  await storage.saveSession("identity", session);
  await storage.saveSession("tenant", session);
  await adapter.runAsync(
    `INSERT INTO _gonvex_replica_meta (scope,key,value) VALUES (?, ?, ?)`,
    "tenant", "invariant:test", JSON.stringify({ keep: true }),
  );
  const other = { _id: "other", name: "Other scope" };
  const otherWindow = completeWindow("other", "tasks", [other], { epoch: "other", revision: 1 });
  await storage.replaceWindow(otherWindow, { entities: {}, liveQueries: {}, cursor: otherWindow.cursor }, "other", [other]);
  const beforeOther = await storage.load("other");

  const shared = { _id: "shared", name: "Fresh" };
  const fresh = { _id: "fresh", name: "New" };
  await materialize("tasks", "tasks", [shared, fresh], "new", 1);
  const expected = { tasks: { shared, fresh } };
  expect(replica.snapshot().entities).toEqual(expected);
  expect(replica.hasLiveQuery("statuses")).toBe(false);
  expect(replica.hasLiveQuery("extra")).toBe(false);
  const stored = (await storage.load("tenant"))!;
  expect(stored.entities).toEqual(expected);
  expect(Object.keys(stored.liveQueries)).toEqual(["tasks"]);
  expect(stored.cursor).toEqual({ epoch: "new", revision: 1 });
  // Reading the old window before reset would create fresh tombstones for its
  // retired IDs. No old entity record or authority should survive that order.
  const records = await adapter.getAllAsync<{ id: string; value: string }>(
    `SELECT id,value FROM _gonvex_replica_entities WHERE scope=? ORDER BY id`, "tenant",
  );
  expect(records).toEqual([
    { id: "fresh", value: JSON.stringify(fresh) },
    { id: "shared", value: JSON.stringify(shared) },
  ]);
  const authorities = await adapter.getAllAsync<{ key: string; value: string }>(
    `SELECT key,value FROM _gonvex_replica_meta WHERE scope=? AND key LIKE 'row:%' ORDER BY key`, "tenant",
  );
  expect(authorities.map(({ key }) => key)).toEqual([
    'row:["tasks","fresh"]', 'row:["tasks","shared"]',
  ]);
  expect(authorities.map(({ value }) => JSON.parse(value))).toEqual([
    { epoch: "new", fields: { _id: 1, name: 1 } },
    { epoch: "new", fields: { _id: 1, name: 1 } },
  ]);
  expect(await adapter.getAllAsync(
    `SELECT id FROM _gonvex_replica_lookup WHERE scope=? AND id IN ('retired','orphan','s','gone')`, "tenant",
  )).toEqual([]);

  replica.dispose();
  const coldStorage = new ExpoSQLiteLocalReplicaStorage(adapter);
  const cold = createReplica(coldStorage);
  await cold.activateScope("tenant");
  expect(cold.snapshot().entities).toEqual(expected);
  expect(cold.windowRows("tasks")).toEqual([shared, fresh]);
  expect(cold.hasLiveQuery("statuses")).toBe(false);
  expect(cold.hasLiveQuery("extra")).toBe(false);
  expect(cold.entityCompleteness("tasks")).toBe("complete");
  await cold.applyTransaction({
    cursor: { epoch: "new", revision: 2 },
    changes: [{ entity: "tasks", id: "shared", operation: "update", newValue: { _id: "shared", name: "Updated" } }],
  });
  const updated = { tasks: { shared: { _id: "shared", name: "Updated" }, fresh } };
  expect(cold.snapshot().entities).toEqual(updated);
  expect((await coldStorage.load("tenant"))!.entities).toEqual(updated);
  const again = createReplica(new ExpoSQLiteLocalReplicaStorage(adapter));
  await again.activateScope("tenant");
  expect(again.snapshot().entities).toEqual(updated);
  expect(Object.keys(again.snapshot().liveQueries)).toEqual(["tasks"]);
  expect(await coldStorage.loadSession("identity")).toEqual(session);
  expect(await coldStorage.loadSession("tenant")).toEqual(session);
  expect(await adapter.getFirstAsync(
    `SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key=?`, "tenant", "invariant:test",
  )).toEqual({ value: JSON.stringify({ keep: true }) });
  expect(await coldStorage.load("other")).toEqual(beforeOther);
});

it("rolls back an epoch snapshot reset with its entities, windows, authority, and cursor", async () => {
  const { storage, adapter } = fixture();
  const replica = createReplica(storage);
  await replica.activateScope("tenant");
  await replica.replaceWindow({
    signature: "tasks", kind: "replica", entity: "tasks", key: "_id",
    rows: [{ _id: "old", name: "Old" }], completeness: "complete", source: "server",
    cursor: { epoch: "old", revision: 1 },
  });
  await replica.replaceWindow({
    signature: "statuses", kind: "replica", entity: "statuses", key: "_id",
    rows: [{ _id: "s", name: "Old status" }], completeness: "complete", source: "server",
    cursor: { epoch: "old", revision: 1 },
  });
  const before = replica.snapshot();
  const beforeDisk = await storage.load("tenant");
  const readAuthorities = () => adapter.getAllAsync(
    `SELECT key,value FROM _gonvex_replica_meta WHERE scope=? AND key LIKE 'row:%' ORDER BY key`, "tenant",
  );
  const beforeAuthority = await readAuthorities();
  const run = adapter.runAsync;
  let fail = true;
  adapter.runAsync = async (sql, ...params) => {
    if (fail && sql.includes("VALUES (?, 'cursor', ?)")) {
      fail = false;
      throw new Error("interrupted epoch snapshot");
    }
    return run(sql, ...params);
  };
  await expect(replica.replaceWindow({
    signature: "tasks", kind: "replica", entity: "tasks", key: "_id",
    rows: [{ _id: "new", name: "Fresh" }], completeness: "complete", source: "server",
    cursor: { epoch: "new", revision: 1 },
  })).rejects.toThrow("interrupted epoch snapshot");
  expect(replica.snapshot()).toEqual(before);
  expect(await storage.load("tenant")).toEqual(beforeDisk);
  expect(await readAuthorities()).toEqual(beforeAuthority);
  const cold = createReplica(new ExpoSQLiteLocalReplicaStorage(adapter));
  await cold.activateScope("tenant");
  expect(cold.snapshot().entities).toEqual(before.entities);
  expect(cold.windowRows("statuses")).toEqual([{ _id: "s", name: "Old status" }]);
});

it("checkpoints a window-only epoch without treating its collection revision as a global floor", async () => {
  const { storage, adapter } = fixture();
  const empty = { entities: {}, liveQueries: {} };
  await storage.applyTransaction({ cursor: { epoch: "old", revision: 20 }, changes: [] }, empty, "tenant");
  const put = async (signature: string, revision: number) => {
    const row = { _id: signature, name: signature };
    await storage.replaceWindow(completeWindow(signature, "tasks", [row], { epoch: "new", revision }), empty, "tenant", [row]);
  };
  await put("a", 10);
  expect.soft((await storage.load("tenant"))!.cursor).toEqual({ epoch: "new", revision: 0 });
  await put("b", 12);
  expect.soft(Object.keys((await storage.load("tenant"))!.entities.tasks)).toEqual(["a", "b"]);
  await storage.applyTransaction({ cursor: { epoch: "new", revision: 1 }, changes: [] }, empty, "tenant");
  await put("c", 15);
  const loaded = (await storage.load("tenant"))!;
  expect(loaded.cursor).toEqual({ epoch: "new", revision: 1 });
  expect(loaded.entities.tasks).toEqual({ a: { _id: "a", name: "a" }, b: { _id: "b", name: "b" }, c: { _id: "c", name: "c" } });
  const cold = createReplica(new ExpoSQLiteLocalReplicaStorage(adapter));
  await cold.activateScope("tenant");
  expect(cold.snapshot().cursor).toEqual({ epoch: "new", revision: 1 });
  expect(Object.keys(cold.snapshot().liveQueries)).toEqual(["a", "b", "c"]);
  expect(cold.snapshot().entities.tasks).toEqual(loaded.entities.tasks);
});
