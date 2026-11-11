// Host calls, transactions and errors in a real SQLite-backed tenant object.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { CallReport, TenantCall } from "../src/index.js";
import { TestTenant, type SetupOptions } from "./worker.js";

const T0 = 1792324800123;
const identity = {
  account: { id: "acct-1", email: "owner@example.com", name: "Owner" },
  tenant: { id: "tenant-1", projectId: "project-1", name: "Tenant" },
  member: { id: "mem-1", accountId: "acct-1", status: "active", role: "owner", displayName: "Owner", permissions: {} },
};
let tenants = 0;

/** A fresh tenant object per test. */
function tenant(options?: SetupOptions) {
  const stub = env.TENANT.getByName(`tenant-${++tenants}`);
  const within = <T>(run: (instance: TestTenant, state: DurableObjectState) => T | Promise<T>) =>
    runInDurableObject(stub, (instance: TestTenant, state) => run(instance, state));
  const ready = within((instance) => { if (options) instance.setup(options); });
  const call = async (fn: string, kind: TenantCall["kind"], args: unknown, extra: Partial<TenantCall> = {}): Promise<CallReport> => {
    await ready;
    return within((instance) => instance.host.run({
      function: fn, kind, args, identity, now: T0,
      invocation: { channel: "ui", rootChannel: "ui", commandId: "command-1", rootCommandId: "command-1", actorAccountId: "acct-1", actorMemberId: "mem-1" },
      ...extra,
    }));
  };
  const rows = async (sql: string) => { await ready; return within((_i, state) => state.storage.sql.exec(sql).toArray()); };
  return { call, rows, within, ready };
}

const value = (report: CallReport) => {
  if (!report.ok) throw new Error(`expected ok, got ${JSON.stringify(report.error)}`);
  return JSON.parse(report.value as string) as unknown;
};

describe("Queries", () => {
  it("reads typed rows and never commits", async () => {
    const t = tenant();
    value(await t.call("notes.add", "reducer", { id: "n1", body: "hello", done: true, meta: { tags: ["a"] } }));
    const report = await t.call("notes.list", "query", {});
    expect(value(report)).toEqual([{ _id: "n1", body: "hello", done: true, meta: { tags: ["a"] } }]);
    expect(report.committed).toBe(false);
  });

  it("pins the clock across awaits and hides workerd's network and globals", async () => {
    const t = tenant();
    const result = value(await t.call("notes.clock", "query", {})) as Record<string, unknown>;
    expect(result).toMatchObject({
      now: T0, ctxNow: T0, pinnedAcrossAwait: true, constructed: T0,
      typeofFetch: "undefined", typeofGlobalFetch: "undefined", typeofSelfFetch: "undefined", globalDatePinned: true,
      hasInsert: "undefined", hasPermissions: "object", hasActions: "undefined", hasScheduler: "undefined", hasStorage: "undefined",
    });
    expect(result["keys"]).toEqual(["auth", "db", "function", "invocation", "kind", "member", "now", "permissions", "tenant"]);
  });

  it("refuses a write head and a WITH … DELETE (0011.2)", async () => {
    const t = tenant();
    value(await t.call("notes.add", "reducer", { id: "keep", body: "x" }));
    const head = await t.call("notes.writeInQuery", "query", {});
    expect(head.error?.message).toContain('host call failed: db_read host call failed [host_call_failed]: a Query may only read; "delete" is not a read statement');
    const cte = await t.call("notes.cteDelete", "query", {});
    expect(cte.ok).toBe(false);
    expect(cte.error?.message).toContain("attempt to write a readonly database");
    expect(await t.rows("SELECT _id FROM notes")).toEqual([{ _id: "keep" }]);
  });

  it("answers ctx.permissions in a Query with Rust's host error", async () => {
    const t = tenant();
    const report = await t.call("notes.queryPermission", "query", {});
    expect(report.error?.message).toContain("ctx.permissions is available to Reducers with a tenant session");
    expect(report.error?.class).toBe("rejected");
  });
});

describe("Reducers", () => {
  it("inserts, updates and deletes with RETURNING rows decoded by declared type", async () => {
    const t = tenant();
    expect(value(await t.call("notes.add", "reducer", { id: "n1", body: "a", meta: [1, 2] }))).toEqual({ _id: "n1", body: "a", done: false, meta: [1, 2], n: null });
    expect(value(await t.call("notes.update", "reducer", { id: "n1", patch: { done: true, body: "b" } }))).toEqual({ _id: "n1", body: "b", done: true, meta: [1, 2], n: null });
    expect(value(await t.call("notes.update", "reducer", { id: "missing", patch: { body: "b" } }))).toBeNull();
    value(await t.call("notes.add", "reducer", { id: "n2", body: "c" }));
    value(await t.call("notes.add", "reducer", { id: "n3", body: "d" }));
    expect(value(await t.call("notes.remove", "reducer", { id: "n1" }))).toEqual({ deleted: 1 });
    expect(value(await t.call("notes.removeMany", "reducer", { ids: ["n2", "n3", "nope"] }))).toEqual({ deleted: 2 });
    expect(value(await t.call("notes.removeMany", "reducer", { ids: [] }))).toEqual({ deleted: 0 });
    expect(await t.rows("SELECT count(*) AS n FROM notes")).toEqual([{ n: 0 }]);
  });

  it("lets SQLite assign an INTEGER PRIMARY KEY and refuses composite keys", async () => {
    const t = tenant();
    expect(value(await t.call("notes.counter", "reducer", { value: 5 }))).toEqual({ id: 1, value: 5 });
    const composite = await t.call("notes.composite", "reducer", {});
    expect(composite.error?.message).toContain('table "pairs" has a composite primary key, which db.update/delete do not support');
  });

  it("validates arguments with Rust's messages before running", async () => {
    const t = tenant();
    const report = await t.call("notes.add", "reducer", { id: "n1", body: 7 });
    expect(report.error).toMatchObject({ code: "invalid_args", class: "rejected" });
    expect(report.error?.message).toBe("TypeScript module host returned invalid_args: module fixture function notes.add: $.body: expected string, received number");
  });

  it("writes outbox rows with deterministic intent ids", async () => {
    const t = tenant();
    const ids = value(await t.call("notes.enqueue", "reducer", {})) as Record<string, string>;
    expect(ids["action"]).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(ids["later"]).toMatch(/^job_/);
    const outbox = await t.rows("SELECT action_path, args, actor_user_id, actor_email, provenance FROM _gonvex_action_outbox ORDER BY rowid");
    expect(outbox.map((row) => row["action_path"])).toEqual(["mail.send", "_gonvex.scheduler.enqueue", "_gonvex.scheduler.enqueue"]);
    const wake = JSON.parse(String(outbox[2]?.["args"]));
    expect(wake).toMatchObject({ function: "notes.add", runAtUnixMs: 1_800_000_000_000, coalesce: true });
    const later = JSON.parse(String(outbox[1]?.["args"]));
    // runAfter uses the server clock, not ctx.now (B.4, R23).
    expect(later.runAtUnixMs).toBeGreaterThan(T0 + 1000);
    expect(outbox[0]).toMatchObject({ actor_user_id: "acct-1", actor_email: "owner@example.com" });
    expect(JSON.parse(String(outbox[0]?.["provenance"]))).toMatchObject({ commandId: "command-1", channel: "ui", actorMemberId: "mem-1" });
  });

  it("matches the Rust deferred-id vectors", async () => {
    const { intentDeferredId } = await import("../src/index.js");
    expect(await intentDeferredId(crypto.subtle, "tenant-1", "account-1", "command-1", "action", 0)).toBe("aa14126b-2f16-814a-8623-07601307140c");
    expect(await intentDeferredId(crypto.subtle, "tenant-1", "account-1", "command-1", "schedule", 0)).toBe("c9983b7c-273c-8f8e-91df-ef1b9ba899f6");
  });

  it("refuses to schedule an unregistered function", async () => {
    const t = tenant();
    const report = await t.call("notes.scheduleUnknown", "reducer", {});
    expect(report.error?.message).toContain('scheduler host call failed [host_call_failed]: scheduled function "nope.missing" is not a registered Reducer or Action');
  });

  it("seeds Math.random from intentEntropy and denies secure entropy", async () => {
    const t = tenant();
    const seeded = value(await t.call("notes.random", "reducer", {}, { intentEntropy: "ab".repeat(32) })) as { random: number[]; entropy: string };
    expect(seeded.random).toEqual([0.02352941082790494, 0.7535156221129]);
    expect(seeded.entropy).toBe("InvalidAccessError");
    const surface = value(await t.call("notes.clock", "query", {})) as { keys: string[] };
    expect(surface.keys).not.toContain("storage");
    const none = value(await t.call("notes.random", "reducer", {})) as { random: string };
    expect(none.random).toBe("Math.random requires intentEntropy in Queries and Reducers");
  });

  it("rolls back every write when the handler throws", async () => {
    const t = tenant();
    const report = await t.call("notes.throwAfterWrite", "reducer", { id: "doomed" });
    expect(report.error).toMatchObject({ code: "execution_failed", class: "rejected" });
    expect(report.error?.message).toMatch(/^TypeScript module host returned execution_failed: module fixture function notes.throwAfterWrite: Error: boom after write/);
    expect(await t.rows("SELECT * FROM notes")).toEqual([]);
  });

  it("refuses to commit after a caught SQL error (B.7)", async () => {
    const t = tenant();
    const report = await t.call("notes.caughtSqlError", "reducer", { id: "dup" });
    expect(report.error).toMatchObject({ code: "host_call_failed", class: "rejected", message: "module database capability failed: current transaction is aborted, commands ignored until end of transaction block" });
    expect(await t.rows("SELECT * FROM notes")).toEqual([]);
    value(await t.call("notes.add", "reducer", { id: "dup", body: "x" }));
    const aborted = await t.call("notes.abortedThenQuery", "reducer", { id: "dup" });
    expect(aborted.error?.message).toContain("Error: second statement: host call failed: db_read host call failed [host_call_failed]: current transaction is aborted, commands ignored until end of transaction block");
  });

  it("recovers with ROLLBACK TO a module savepoint", async () => {
    const t = tenant();
    value(await t.call("notes.add", "reducer", { id: "existing", body: "x" }));
    expect(value(await t.call("notes.savepointRecover", "reducer", { id: "sp", existing: "existing" }))).toBe("recovered");
    expect((await t.rows("SELECT _id FROM notes ORDER BY _id")).map((row) => row["_id"])).toEqual(["existing", "sp-b"]);
  });

  it("checks deferred foreign keys before commit and keeps the object alive (G0-5)", async () => {
    const t = tenant();
    const before = await t.within((instance) => instance.instance);
    const report = await t.call("notes.orphanChild", "reducer", { id: "c1" });
    expect(report.error?.message).toBe("module database capability failed: error returned from database: (code: 787) FOREIGN KEY constraint failed");
    expect(await t.rows("SELECT * FROM children")).toEqual([]);
    expect(await t.within((instance) => instance.instance)).toBe(before);
    value(await t.call("notes.add", "reducer", { id: "after", body: "still alive" }));
  });

  it("injects record-view defaults and re-reads the record", async () => {
    const t = tenant();
    const result = value(await t.call("notes.recordAdd", "reducer", { label: "first" })) as { row: Record<string, unknown>; columns: Record<string, unknown> };
    expect(result.row["label"]).toBe("first");
    expect(result.row["created"]).toBe(7);
    expect(String(result.row["_id"])).toMatch(/^item-[0-9a-f]{8}$/);
    expect(Object.keys(result.columns)).toEqual(["_id", "label", "created"]);
  });

  it("answers ctx.permissions from the in-memory index", async () => {
    const t = tenant({ permissions: { "edit@": true, "edit@ws-1": false, "view@": "rebuilding" } });
    expect(value(await t.call("notes.permission", "reducer", { permission: "edit" }))).toBe(true);
    expect(value(await t.call("notes.permission", "reducer", { permission: "edit", scope: "ws-1" }))).toBe(false);
    const rebuilding = await t.call("notes.permission", "reducer", { permission: "view" });
    expect(rebuilding.error?.message).toContain("permission bits are rebuilding; retry");
  });

  it("answers storage.getMetadata as Rust does without a storage context", async () => {
    const t = tenant();
    const report = await t.call("notes.metadata", "reducer", {});
    expect(report.error?.message).toContain("host call failed: storage host call failed [host_call_failed]: storage metadata is unavailable");
  });
});

describe("budgets and caps", () => {
  it("caps results at the configured byte limit", async () => {
    const t = tenant({ engine: { maxResultBytes: 1000 } });
    expect((await t.call("notes.big", "query", { size: 900 })).ok).toBe(true);
    const report = await t.call("notes.big", "query", { size: 1000 });
    expect(report.error).toMatchObject({ code: "budget_exceeded", class: "transient" });
    expect(report.error?.message).toContain("notes.big returned more than the 1000 byte result limit");
  });

  it("validates results against the result schema", async () => {
    const t = tenant();
    const report = await t.call("notes.badResult", "query", {});
    expect(report.error).toMatchObject({ code: "invalid_result", class: "rejected" });
    expect(report.error?.message).toContain("$: expected string, received number");
  });

  it("ends a Reducer that waits past its deadline and rolls it back", async () => {
    const t = tenant({ engine: { executionTimeoutMs: 50 } });
    const report = await t.call("notes.slow", "reducer", { ms: 300 });
    expect(report.error).toMatchObject({ code: "budget_exceeded", class: "transient" });
    expect(report.error?.message).toContain("module exceeded its 50 ms execution deadline");
  });

  it("uses the scheduled slice message on the scheduled lane", async () => {
    const t = tenant({ engine: { scheduledSliceMs: 30 } });
    const report = await t.call("notes.slow", "reducer", { ms: 300 }, { lane: "scheduled" });
    expect(report.error?.message).toContain("scheduled reducer notes.slow exceeded its 30 ms time slice for scheduled work");
  });

  it("stops a host-call runaway with a counter, even if it swallows the errors (R25)", async () => {
    const t = tenant({ engine: { maxHostCalls: 50 } });
    expect((await t.call("notes.manyCalls", "reducer", { calls: 50 })).ok).toBe(true);
    const report = await t.call("notes.manyCalls", "reducer", { calls: 10_000 });
    expect(report.error).toMatchObject({ code: "budget_exceeded", class: "transient", retryable: false });
    expect(report.error?.message).toBe("TypeScript module host returned budget_exceeded: module fixture function notes.manyCalls: module exceeded its 10000 ms execution deadline");
    expect(report.stats.hostCalls).toBe(50);
  });

  it("refuses a quarantined command without running it (R25)", async () => {
    const t = tenant();
    await t.within((instance) => instance.host.quarantine.add("poisoned-1"));
    const report = await t.call("notes.add", "reducer", { id: "never", body: "x" }, { invocation: { commandId: "poisoned-1" } });
    expect(report.error?.message).toBe("TypeScript module host returned budget_exceeded: module fixture function notes.add: module exceeded its 10000 ms execution deadline");
    expect(report.error?.class).toBe("transient");
    expect(await t.rows("SELECT * FROM notes")).toEqual([]);
    expect((await t.call("notes.add", "reducer", { id: "ok", body: "x" }, { invocation: { commandId: "fine-1" } })).ok).toBe(true);
  });

  it("recognizes the platform's overrun texts", async () => {
    const { isPlatformOverrun, classifyPlatformError } = await import("../src/index.js");
    for (const message of ["Durable Object exceeded its CPU time limit and was reset.", "Durable Object reset because its code was updated.", "Durable Object is overloaded. Requests queued for too long."]) {
      expect(isPlatformOverrun(new Error(message))).toBe(true);
      expect(classifyPlatformError(new Error(message)).class).toBe("transient");
    }
    expect(classifyPlatformError(new Error("Durable Object exceeded its CPU time limit and was reset.")).code).toBe("budget_exceeded");
    expect(isPlatformOverrun(new Error("no such table: x"))).toBe(false);
  });

  it("fails a denied capability even when the module catches it", async () => {
    const { ModuleEngine, ModuleError } = await import("../src/index.js");
    const { fixtureFunctions } = await import("./worker.js");
    const fixture = await import("./fixtures/host.module.js");
    const engine = new ModuleEngine(fixtureFunctions(), fixture as Record<string, unknown>);
    const error = await engine.invoke({ call: () => ({ status: 200 }) }, {
      function: "net.swallowDenied", kind: "action", args: {},
      context: { nowUnixMs: 0, identity: {}, invocation: {}, capabilities: { network: true } },
    }).then(() => undefined, (cause: unknown) => cause);
    expect(error).toBeInstanceOf(ModuleError);
    expect((error as Error).message).toBe("module execution failed: fetch origin is not declared for this Action");
  });
});
