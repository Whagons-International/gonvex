import { afterEach, describe, expect, it } from "vitest";
import { internalReducer, reducer, schema, type ReducerContext } from "@gonvex/module-sdk";
import { LocalReducerRuntime, type LocalExecution, type LocalSnapshot } from "./index.js";

const tables = {
  tasks: { _id: "text", statusId: "text", count: "bigint" },
  logs: { _id: "text", taskId: "text", message: "text", _creationTime: "bigint" },
} as const;
const snapshot = (): LocalSnapshot => ({
  scope: "project/tenant/member",
  tables: {
    tasks: { complete: true, rows: [{ _id: "t1", statusId: "todo", count: 0 }] },
    logs: { complete: true, rows: [] },
  },
});
const execution = (): LocalExecution => ({
  scope: "project/tenant/member", commandId: "command-1", now: 1234,
  artifactHash: "artifact-1",
  identity: {
    auth: { account: { id: "account-1" } }, tenant: { id: "tenant-1" },
    member: { id: "member-1", accountId: "account-1", permissions: {} },
  },
});
const define = (run: (ctx: ReducerContext, args: any) => Promise<any>) => reducer({
  args: schema.any(), result: schema.any(), run,
});
const runtimes: LocalReducerRuntime[] = [];
const runtime = (handlers: Record<string, ReturnType<typeof define>>) => {
  const result = new LocalReducerRuntime({ tables, reducers: handlers, artifactHash: "artifact-1" });
  runtimes.push(result);
  return result;
};
afterEach(async () => { await Promise.all(runtimes.splice(0).map((runtime) => runtime.close())); });

describe("local reducer execution", () => {
  it("returns the same Action and scheduler IDs as the authoritative host", async () => {
    const host = runtime({ schedule: define(async ctx => ({
      action: await ctx.actions.enqueue("notify", {}),
      job: await ctx.scheduler.runAfter(100, "notify", {}),
    })) });
    const result = await host.execute("schedule", {}, snapshot(), execution());
    expect(result.result).toEqual({
      action: "aa14126b-2f16-814a-8623-07601307140c",
      job: "job_c9983b7c-273c-8f8e-91df-ef1b9ba899f6",
    });
    expect(result.deferred[1]).toMatchObject({ at: 1334 });
  });
  it("executes the same handler and derives the complete transaction from database writes", async () => {
    const transition = define(async (ctx, args) => {
      const [task] = await ctx.db.query<any>('SELECT * FROM "tasks" WHERE "_id" = $1', [args.taskId]);
      if (task.statusId !== "todo") throw new Error("Invalid transition");
      await ctx.db.update("tasks", args.taskId, { statusId: "done", count: task.count + 1 });
      const log = await ctx.db.insert<any>("logs", { taskId: args.taskId, message: "done" });
      await ctx.actions.enqueue("notify", { taskId: args.taskId });
      return { logId: log._id };
    });
    const input = snapshot();
    const result = await runtime({ transition }).execute("transition", { taskId: "t1" }, input, execution());
    expect(result.patches).toEqual([
      { entity: "tasks", rowId: "t1", op: "patch", fields: { statusId: "done", count: 1 } },
      { entity: "logs", rowId: result.result.logId, op: "insert", fields: {
        _id: result.result.logId, taskId: "t1", message: "done", _creationTime: 1234,
      } },
    ]);
    expect(result.deferred).toMatchObject([{ kind: "action", path: "notify", args: { taskId: "t1" } }]);
    expect(input.tables.tasks!.rows[0]!.statusId).toBe("todo");
  });

  it("discards every write when the handler fails, including a failure after an awaited write", async () => {
    const host = runtime({ fail: define(async (ctx) => {
      await ctx.db.update("tasks", "t1", { statusId: "done" });
      await ctx.db.insert("logs", { taskId: "t1", message: "done" });
      throw new Error("Approval required");
    }), read: define(async (ctx) => (await ctx.db.query('SELECT * FROM "tasks"'))[0]) });
    await expect(host.execute("fail", {}, snapshot(), execution())).rejects.toThrow("Approval required");
    const next = await host.execute("read", {}, snapshot(), execution());
    expect(next.result).toMatchObject({ statusId: "todo", count: 0 });
    expect(next.patches).toEqual([]);
  });

  it("replays IDs deterministically and isolates commands and tenants", async () => {
    const host = runtime({ create: define((ctx) => ctx.db.insert("logs", { taskId: "t1", message: "created" })) });
    const first = await host.execute("create", {}, snapshot(), execution());
    expect(await host.execute("create", {}, snapshot(), execution())).toEqual(first);
    const second = await host.execute("create", {}, snapshot(), { ...execution(), commandId: "command-2" });
    expect(second.result._id).not.toBe(first.result._id);
    await expect(host.execute("create", {}, snapshot(), { ...execution(), scope: "another-tenant" })).rejects.toThrow(/scope/i);
  });

  it("does not mistake an incomplete table for an empty one", async () => {
    const host = runtime({ read: define((ctx) => ctx.db.query('SELECT COUNT(*) FROM "logs"')) });
    const partial = snapshot();
    partial.tables.logs!.complete = false;
    await expect(host.execute("read", {}, partial, execution())).rejects.toThrow(/logs.*incomplete/i);
  });

  it("allows known primary-key rows from a partial large collection, but not absent rows", async () => {
    const host = runtime({ read: define((ctx, args) => ctx.db.query('SELECT * FROM "tasks" WHERE "_id" = $1 LIMIT 1', [args.id])) });
    const partial = snapshot(); partial.tables.tasks!.complete = false;
    expect((await host.execute("read", { id: "t1" }, partial, execution())).result).toMatchObject([{ _id: "t1" }]);
    await expect(host.execute("read", { id: "missing" }, partial, execution())).rejects.toThrow(/incomplete/i);
  });

  it("does not substitute NULL for columns omitted from a replica projection", async () => {
    const host = runtime({ read: define((ctx) => ctx.db.query('SELECT * FROM "tasks"')) });
    const partial = snapshot(); partial.tables.tasks!.columns = ["_id", "count"];
    await expect(host.execute("read", {}, partial, execution())).rejects.toThrow(/incomplete/i);
  });

  it("captures SQL writes and data-modifying CTEs in the reducer transaction", async () => {
    const host = runtime({ write: define((ctx) => ctx.db.query(`WITH deleted AS (DELETE FROM "tasks" WHERE "_id" = 't1' RETURNING *) INSERT INTO "logs" ("_id", "taskId", "message") SELECT 'log1', "_id", 'removed' FROM deleted RETURNING *`)) });
    const result = await host.execute("write", {}, snapshot(), execution());
    expect(result.patches).toContainEqual({ entity: "tasks", rowId: "t1", op: "delete" });
    expect(result.patches).toContainEqual(expect.objectContaining({ entity: "logs", rowId: "log1", op: "insert" }));
  });

  it("re-executes dependent intent against a new base rather than reusing stale patches", async () => {
    const host = runtime({ increment: define(async (ctx) => {
      const [task] = await ctx.db.query<any>('SELECT * FROM "tasks" WHERE "_id" = $1', ["t1"]);
      await ctx.db.update("tasks", "t1", { count: task.count + 1 });
      return task.count + 1;
    }) });
    const original = await host.execute("increment", {}, snapshot(), execution());
    const rebased = snapshot();
    rebased.tables.tasks!.rows[0]!.count = 10;
    const replay = await host.execute("increment", {}, rebased, execution());
    expect(original.result).toBe(1);
    expect(replay.result).toBe(11);
    expect(replay.patches[0]).toMatchObject({ fields: { count: 11 } });
  });

  it("fails closed on artifact mismatch and internal reducer execution", async () => {
    const host = runtime({ read: define(async () => true) });
    await expect(host.execute("read", {}, snapshot(), { ...execution(), artifactHash: "old" })).rejects.toThrow(/artifact/i);
    const internal = runtime({ secret: internalReducer({ run: async () => true }) as ReturnType<typeof define> });
    await expect(internal.execute("secret", {}, snapshot(), execution())).rejects.toThrow(/public local reducer/i);
  });

  it("rebuilds dependent intents from serialized envelopes after a rejected earlier intent", async () => {
    const handlers = {
      first: define(async (ctx) => {
        await ctx.db.update("tasks", "t1", { count: 100 });
        throw new Error("Server state no longer permits first intent");
      }),
      increment: define(async (ctx) => {
        const [task] = await ctx.db.query<any>('SELECT * FROM "tasks" WHERE "_id" = $1', ["t1"]);
        await ctx.db.update("tasks", "t1", { count: task.count + 1 });
        return task.count + 1;
      }),
    };
    const persisted = JSON.stringify([
      { path: "first", args: {}, execution: execution() },
      { path: "increment", args: {}, execution: { ...execution(), commandId: "second" } },
      { path: "increment", args: {}, execution: { ...execution(), commandId: "third" } },
    ]);
    const host = runtime(handlers);
    const result = await host.replay(snapshot(), JSON.parse(persisted));
    expect(result.rejected.map((item) => item.commandId)).toEqual(["command-1"]);
    expect(result.transactions.map((item) => item.transaction.result)).toEqual([1, 2]);
    const restarted = runtime(handlers);
    expect((await restarted.replay(snapshot(), JSON.parse(persisted))).transactions).toEqual(result.transactions);
  });

  it("checks incomplete dependencies inside joins and CTEs", async () => {
    const host = runtime({ read: define((ctx) => ctx.db.query(
      'WITH matching AS (SELECT "taskId" FROM "logs") SELECT t.* FROM "tasks" t JOIN matching m ON t."_id" = m."taskId"',
    )) });
    const input = snapshot();
    input.tables.logs!.complete = false;
    await expect(host.execute("read", {}, input, execution())).rejects.toThrow(/logs.*incomplete/i);
  });

  it("captures writes hidden inside a read CTE", async () => {
    const host = runtime({ write: define((ctx) => ctx.db.query(
      'WITH removed AS (DELETE FROM "tasks" RETURNING *) SELECT * FROM removed',
    )) });
    expect((await host.execute("write", {}, snapshot(), execution())).patches).toEqual([{ entity: "tasks", rowId: "t1", op: "delete" }]);
  });

  it("keeps incomplete-data replay pending instead of discarding intents", async () => {
    const host = runtime({ read: define((ctx) => ctx.db.query('SELECT COUNT(*) FROM "logs"')) });
    const input = snapshot();
    input.tables.logs!.complete = false;
    await expect(host.replay(input, [{ path: "read", args: {}, execution: execution() }])).rejects.toThrow(/incomplete/i);
  });

  it("captures arguments and snapshot before awaiting concurrent execution", async () => {
    const host = runtime({ update: define(async (ctx, args) => {
      await ctx.db.update("tasks", "t1", { count: args.count });
      return args.count;
    }) });
    const args = { count: 3 };
    const input = snapshot();
    const pending = host.execute("update", args, input, execution());
    args.count = 900;
    input.tables.tasks!.rows = [];
    expect((await pending).result).toBe(3);
  });
});
