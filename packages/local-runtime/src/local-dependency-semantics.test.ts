// Pins the local execution behavior that the CLI's local Reducer dependency
// analysis (packages/gonvex/src/local-dependencies.ts) models. If the runtime
// changes which operations need Local Replica coverage, this test fails and
// the analysis must change with it.
import { afterAll, describe, expect, it } from "vitest";
import { applyDataWrites, insertDataRows, reducer, schema, selectRows } from "@gonvex/module-sdk";
import type { ReducerContext } from "@gonvex/module-sdk";
import { createPortableReducer } from "./portable-client.js";
import { memoryReadView } from "./read-view.js";
import type { LocalSchema } from "./schema.js";

const columns = {
  _id: { type: "text", nullable: false },
  _creationTime: { type: "bigint", nullable: true },
  name: { type: "text", nullable: true },
};
// "covered" has a generated local collection; "uncovered" has none.
const localSchema: LocalSchema = { covered: { key: "_id", columns }, uncovered: { key: "_id", columns } };
const run = (body: (ctx: ReducerContext) => Promise<unknown>) => reducer({
  args: schema.object({}),
  result: schema.any(),
  run: async (ctx: ReducerContext) => { await body(ctx); return null; },
});
const reducers = {
  insertGeneratedKey: run((ctx) => ctx.db.insert("uncovered", { name: "a" })),
  insertExplicitKey: run((ctx) => ctx.db.insert("uncovered", { _id: "fixed", name: "a" })),
  insertBatchGeneratedKeys: run((ctx) => insertDataRows(ctx, "uncovered", [{ name: "a" }, { name: "b" }])),
  applyInsertGeneratedKeys: run((ctx) => applyDataWrites(ctx, [{ kind: "insert", table: "uncovered", rows: [{ name: "a" }] }])),
  select: run((ctx) => selectRows(ctx.db, { table: "uncovered", columns: ["_id"] })),
  selectByKey: run((ctx) => selectRows(ctx.db, { table: "uncovered", where: { column: "_id", op: "eq", value: "x" }, limit: 1 })),
  update: run((ctx) => ctx.db.update("uncovered", "x", { name: "b" })),
  delete: run((ctx) => ctx.db.delete("uncovered", "x")),
  updateOwnInsert: run(async (ctx) => {
    const row = await ctx.db.insert<{ _id: string }>("uncovered", { name: "a" });
    await ctx.db.update("uncovered", row._id, { name: "b" });
  }),
  rawQuery: run((ctx) => ctx.db.query("SELECT 1")),
  coveredReadAndKeyedInsert: run(async (ctx) => {
    await selectRows(ctx.db, { table: "covered", columns: ["_id"] });
    await ctx.db.insert("covered", { _id: "fixed", name: "a" });
  }),
};
const host = createPortableReducer({ schema: localSchema, reducers, artifactHash: "test" });
afterAll(() => host.close());
const identity = { auth: { account: { id: "account" } }, tenant: { id: "tenant" }, member: { id: "member", accountId: "account", status: "active" as const, role: "admin", permissions: {} } };
const execution = { scope: "tenant/member", commandId: "intent-1", intentEntropy: "ab".repeat(32), now: 1000, artifactHash: "test", identity };
// A table with no generated collection never receives rows on the device, and
// the client reports coverage only for tables with one.
const view = () => memoryReadView(new Map([["covered", new Map()]]), { covered: { key: "_id", complete: true } });
const execute = (path: keyof typeof reducers) => host.executeRead!(path, {}, view(), execution as never);

describe("which local Reducer operations need a local collection", () => {
  it.each(["insertGeneratedKey", "insertBatchGeneratedKeys", "applyInsertGeneratedKeys", "updateOwnInsert"] as const)(
    "%s runs without one: an intent-generated key is new by construction",
    async (path) => {
      const result = await execute(path);
      expect(result.patches.length).toBeGreaterThan(0);
      expect(result.patches.every((patch) => patch.entity === "uncovered")).toBe(true);
    },
  );

  it.each(["insertExplicitKey", "select", "selectByKey", "update", "delete"] as const)("%s cannot run: it reads the table", async (path) => {
    await expect(execute(path)).rejects.toMatchObject({ name: "IncompleteReplicaError", read: expect.objectContaining({ table: "uncovered" }) });
  });

  it("raw SQL never runs locally", async () => {
    await expect(execute("rawQuery")).rejects.toMatchObject({ name: "UnsupportedLocalOperationError" });
  });

  it("reads and keyed inserts run when the table has a complete collection", async () => {
    const result = await execute("coveredReadAndKeyedInsert");
    expect(result.patches).toEqual([expect.objectContaining({ entity: "covered", rowId: "fixed", op: "insert" })]);
  });
});
