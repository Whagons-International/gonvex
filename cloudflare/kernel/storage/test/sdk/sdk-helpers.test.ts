// SDK data helpers running on the storage layer: the paths of the SDK that
// reach the platform limits K01 handles (more than 100 parameters, REGEXP,
// module savepoints). Tests in this directory run SDK code from its sources.
import { describe, expect, it } from "vitest";
import { applyDataWrites, compileDataRead, existsDataRows } from "@gonvex/module-sdk";
import { objectFor, outcome, rejection } from "../helpers";
import type { StorageSession } from "../../index";

/** Runs `body` in a committed Reducer invocation on a fresh object with `ddl`. */
async function reducer<T>(ddl: string[], body: (session: StorageSession) => Promise<T>) {
  const db = objectFor();
  await db.run((_tenant, state) => { for (const sql of ddl) state.storage.sql.exec(sql); });
  return { db, result: await db.invoke("reducer", body) };
}

async function table() {
  const db = objectFor();
  await db.run((_tenant, state) => {
    state.storage.sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER NOT NULL DEFAULT 0)");
    state.storage.sql.exec("INSERT INTO t VALUES ('seed', 0)");
  });
  return { ...db, ids: async () => (await db.sql<{ id: string }>("SELECT id FROM t ORDER BY id")).map((row) => row.id) };
}

/** The module `db` the SDK's data helpers use, backed by one session. */
const moduleDb = (session: StorageSession) => ({
  query: (sql: string, parameters?: readonly unknown[]) => session.query(sql, parameters ?? []),
  insert: () => { throw new Error("not used"); },
  update: () => { throw new Error("not used"); },
  delete: () => { throw new Error("not used"); },
  deleteMany: () => { throw new Error("not used"); },
});
const ctx = (session: StorageSession) => ({
  db: moduleDb(session),
  auth: { account: { id: "account-1" } },
  tenant: { id: "tenant-1" },
  invocation: { commandId: "command-1" },
}) as never;

describe("existsDataRows (contract-changes 0011 §3)", () => {
  it("runs existsDataRows batches over 100 parameters in groups, with the same results", async () => {
    const ddl = ["CREATE TABLE items (_id TEXT PRIMARY KEY, name TEXT, note TEXT)", "INSERT INTO items VALUES ('a', 'name-2', NULL), ('b', 'name-267', NULL), ('c', 'name-268', 'x')"];
    const { result } = await reducer(ddl, async (session) => {
      const found = await existsDataRows({ query: (sql: string, parameters?: readonly unknown[]) => session.query(sql, parameters ?? []) } as never,
        Array.from({ length: 300 }, (_, index) => ({ table: "items", where: { and: [{ column: "name", op: "eq", value: `name-${index}` }, { column: "note", op: "isNull" }] } })));
      return { found, stats: { ...session.stats } };
    });
    if (!result.ok) throw result.error;
    expect(result.value.found).toEqual(Array.from({ length: 300 }, (_, index) => index === 2 || index === 267));
    // 256 + 44 reads; 100 parameters per statement: 3 + 1 statements.
    expect(result.value.stats.statements).toBe(4);
  });

});

describe("applyDataWrites savepoints (0006, G0-3)", () => {
  it("S1: a caught applyDataWrites failure keeps the rows before and after the batch", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO t (id) VALUES ('before')");
      const failure = await rejection(applyDataWrites(ctx(session), [
        { kind: "insert", table: "t", key: "id", rows: [{ id: "in-batch" }] },
        { kind: "update", table: "t", key: "id", rows: [{ id: "seed", fields: { n: 1 } }] },
        { kind: "insert", table: "t", key: "id", rows: [{ id: "before" }] },
      ]));
      expect(failure).toBe("error returned from database: (code: 1555) UNIQUE constraint failed: t.id");
      await session.query("INSERT INTO t (id) VALUES ('after')");
      return session.query("SELECT n FROM t WHERE id = 'seed'");
    });
    expect(result.ok && result.value).toEqual([{ n: 0 }]);
    expect(await db.ids()).toEqual(["after", "before", "seed"]);
    expect(result.stats.savepoints).toBe(3);
  });

  it("S2: an uncaught batch failure rolls the whole invocation back", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO t (id) VALUES ('before')");
      await applyDataWrites(ctx(session), [{ kind: "insert", table: "t", key: "id", rows: [{ id: "x" }, { id: "seed" }] }]);
    });
    expect(outcome(result)).toBe("error returned from database: (code: 1555) UNIQUE constraint failed: t.id");
    expect(await db.ids()).toEqual(["seed"]);
  });

});

describe("the matches predicate (REGEXP)", () => {
  it("serves the Whagons document-number read (purchasing/common.ts:164)", async () => {
    const db = objectFor();
    await db.run((_tenant, state) => {
      state.storage.sql.exec('CREATE TABLE "purchaseRequests" (_id TEXT PRIMARY KEY, "requestNumber" TEXT)');
      state.storage.sql.exec(`INSERT INTO "purchaseRequests" VALUES ('1', 'PR-009'), ('2', 'PR-0010'), ('3', 'PR-12x'), ('4', 'imported 7'), ('5', 'PR-')`);
    });
    // The SDK compiles `{column, op: 'matches', pattern}` to `"column" REGEXP $n`.
    const read = compileDataRead({ table: "purchaseRequests", columns: ["requestNumber"], where: { column: "requestNumber", op: "matches", pattern: "^PR-[0-9]+$" } });
    const result = await db.invoke("query", (session) => session.query(read.statement, read.parameters));
    expect(result.ok && result.value.map((row) => row.requestNumber).sort()).toEqual(["PR-0010", "PR-009"]);
  });

});
