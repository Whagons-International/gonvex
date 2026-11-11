// Kernel against Rust, before the harness has a kernel backend: the P02
// conformance fixture's module-host functions
// (cloudflare/conformance/fixtures/app/gonvex/host.ts, scenario `module-host`)
// run through the K04 host. The expected values are what the Rust runtime at
// 0bf3ad13 answered in that scenario (node src/cli.mjs --scenario module-host,
// rust-a against rust-b, 2026-11-11).
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
// Resolved by the vitest alias; tsc does not type-check the fixture app (it needs the built SDK).
// @ts-expect-error vitest alias
import * as host from "@conformance-fixture/host.ts";
// @ts-expect-error vitest alias
import * as board from "@conformance-fixture/board.ts";
import { CatalogCache, DurableObjectInvocationStorage, ModuleRegistry, TenantModuleHost, type FunctionContract, type TenantCall } from "../src/index.js";
import type { TestTenant } from "./worker.js";

const any = { kind: "any" };
const functions: FunctionContract[] = [
  { path: "host.querySurface", kind: "query", export: "querySurface", argsSchema: any, resultSchema: any, metadata: { public: true } },
  { path: "host.reducerSurface", kind: "reducer", export: "reducerSurface", argsSchema: any, resultSchema: any },
  { path: "host.caughtSqlError", kind: "reducer", export: "caughtSqlError", argsSchema: any, resultSchema: any },
  { path: "host.savepoints", kind: "reducer", export: "savepoints", argsSchema: any, resultSchema: any },
  { path: "board.add", kind: "reducer", export: "add", argsSchema: any, resultSchema: any },
];
const identity = {
  account: { id: "acct-alice", email: "alice@example.com", name: "Alice" },
  tenant: { id: "tenant-a", projectId: "conformance-fixture" },
  member: { id: "mem-alice", accountId: "acct-alice", status: "active", role: "member" },
};
const BOARD = "CREATE TABLE IF NOT EXISTS board (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, bumps INTEGER NOT NULL DEFAULT 0)";
const OUTBOX = "CREATE TABLE IF NOT EXISTS _gonvex_action_outbox (id TEXT PRIMARY KEY, action_path TEXT NOT NULL, args JSON NOT NULL, actor_user_id TEXT, actor_email TEXT, provenance JSON NOT NULL DEFAULT '{}')";

async function run(calls: [string, TenantCall["kind"], unknown, Partial<TenantCall>?][]) {
  const stub = env.TENANT.getByName(`parity-${Math.random()}`);
  return runInDurableObject(stub, async (_instance: TestTenant, state) => {
    state.storage.sql.exec(BOARD);
    state.storage.sql.exec(OUTBOX);
    const registry = new ModuleRegistry("conformance-fixture");
    registry.install({ moduleId: "conformance-fixture", generation: 1, artifactHash: "fixture", functions, exports: { ...host, ...board } as Record<string, unknown> });
    const tenantHost = new TenantModuleHost({ storage: new DurableObjectInvocationStorage(state.storage), registry, cache: new CatalogCache(true) });
    const out = [];
    for (const [fn, kind, args, extra] of calls) {
      const report = await tenantHost.run({ function: fn, kind, args, identity, invocation: { channel: "ui", commandId: `cmd-${fn}` }, ...extra });
      out.push(report.ok ? { result: JSON.parse(String(report.value)) } : { error: report.error?.message, class: report.error?.class, retryable: report.error?.retryable });
    }
    const rows = state.storage.sql.exec("SELECT id, title, position, bumps FROM board ORDER BY id").toArray();
    return { out, rows };
  });
}

const entropyDenied = "InvalidAccessError: secure entropy is available only in Actions; use intentEntropy in Reducers";
const reducerExpected = (random: unknown, keys: string[]) => ({
  clockPinned: true,
  composite: "GonvexHostError: host call failed: db_write host call failed [host_call_failed]: an update requires at least one column",
  db: ["delete", "deleteMany", "insert", "query", "recordColumns", "update"],
  entropy: entropyDenied,
  identifier: 'GonvexHostError: host call failed: db_write host call failed [host_call_failed]: identifier "bad name" is not a table or column name',
  jobId: true,
  keys,
  random,
  storage: "undefined",
  typeofFetch: "undefined",
  typeofPerformance: "undefined",
  unknownSchedule: 'GonvexHostError: host call failed: scheduler host call failed [host_call_failed]: scheduled function "host.missing" is not a registered Reducer or Action',
});
const reducerKeys = ["actions", "auth", "db", "function", "invocation", "kind", "member", "now", "permissions", "scheduler", "tenant"];

describe("conformance fixture `module-host` against Rust's answers", () => {
  it("matches every observation and the board checksum rows", async () => {
    const { out, rows } = await run([
      ["host.querySurface", "query", {}],
      ["host.reducerSurface", "reducer", {}, { intentEntropy: "ab".repeat(32) }],
      ["host.reducerSurface", "reducer", {}],
      ["host.caughtSqlError", "reducer", { id: "caught" }],
      ["host.savepoints", "reducer", { id: "sp" }],
    ]);
    expect(out[0]).toEqual({
      result: {
        afterWrite: "no error",
        clockPinned: true,
        db: ["query"],
        entropy: entropyDenied,
        keys: ["auth", "db", "function", "invocation", "kind", "member", "now", "permissions", "tenant"],
        permissions: "GonvexHostError: host call failed: db_read host call failed [host_call_failed]: ctx.permissions is available to Reducers with a tenant session",
        random: "Error: Math.random requires intentEntropy in Queries and Reducers",
        typeofFetch: "undefined",
        typeofPerformance: "undefined",
        write: 'GonvexHostError: host call failed: db_read host call failed [host_call_failed]: a Query may only read; "delete" is not a read statement',
      },
    });
    expect(out[1]).toEqual({ result: reducerExpected([0.02352941082790494, 0.7535156221129, 0.8780962645541877], ["actions", "auth", "db", "function", "intentEntropy", "invocation", "kind", "member", "now", "permissions", "scheduler", "tenant"]) });
    expect(out[2]).toEqual({ result: reducerExpected("Math.random requires intentEntropy in Queries and Reducers", reducerKeys) });
    expect(out[3]).toEqual({ error: "module database capability failed: current transaction is aborted, commands ignored until end of transaction block", class: "rejected", retryable: false });
    expect(out[4]).toEqual({ result: "recovered" });
    expect(rows).toEqual([{ id: "sp-after", title: "kept", position: 12, bumps: 0 }]);
  });
});
