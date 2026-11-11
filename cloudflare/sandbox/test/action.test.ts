// ctx.sandbox end to end: K12's ActionRuntime and K04's module host call this
// package's SandboxService as their `sandbox` port, as the Worker wires it.
import { describe, expect, it } from "vitest";
import { ActionRuntime } from "../../actions/src/runtime.js";
import type { TenantActionPort } from "../../actions/src/ports.js";
import type { ActionSession } from "../../actions/src/session.js";
import { ModuleRegistry, type FunctionContract } from "../../kernel/host/src/index.js";
import * as assistant from "./fixtures/assistant.module.js";
import { MemoryFiles, service } from "./support";

const PROJECT = "proj-k20";
const any = { kind: "any" };
const agent = (capabilities: Record<string, unknown>): Record<string, unknown> => ({ actionProfile: "agent", actionCapabilities: capabilities });
const functions: FunctionContract[] = [
  { path: "assistant.analyze", kind: "action", export: "analyze", argsSchema: any, resultSchema: any, metadata: agent({ sandbox: { duckdb: true }, storage: true }) },
  { path: "assistant.failing", kind: "action", export: "failing", argsSchema: any, resultSchema: any, metadata: agent({ sandbox: { duckdb: true } }) },
];

const registry = new ModuleRegistry(PROJECT);
registry.install({ moduleId: PROJECT, generation: 1, artifactHash: "k20-artifact", clientContract: 1, functions, exports: assistant as unknown as Record<string, unknown> });

/** No tools here: a tenant port that refuses. */
const tenant = new Proxy({}, { get: () => async () => { throw new Error("no tenant in this test"); } }) as TenantActionPort;

const session = (tenantId: string): ActionSession => ({
  projectId: PROJECT,
  tenantId,
  account: { id: "acct-1" },
  member: { id: "mem-1", accountId: "acct-1", status: "active", role: "member", displayName: "One", permissions: {} },
});

describe("ctx.sandbox through K12's Action runtime", () => {
  it("an agent Action analyses tasks and an upload, and destroys its sandbox", async () => {
    const tenantId = `t-${crypto.randomUUID()}`;
    const files = new MemoryFiles();
    files.add("upload", "a,b\n1,2\n3,4\n", { tenantId });
    const runtime = new ActionRuntime({ registry, tenant, sandbox: service({}, files) });
    const tasks = [{ statusId: "open" }, { statusId: "done" }, { statusId: "open" }];
    const report = await runtime.execute({ session: session(tenantId), path: "assistant.analyze", args: { tasks, fileId: "upload", filename: "u.csv" } });
    expect(report.ok).toBe(true);
    expect(JSON.parse((report as { value: string }).value)).toEqual({
      status: "succeeded",
      result: { byStatus: [{ statusId: "open", total: 2 }, { statusId: "done", total: 1 }], upload: [{ n: 2 }] },
      imported: { alias: "import_1", tables: [{ tableName: "data", rowCount: 2, columns: ["a", "b"] }] },
    });
  });

  it("errors reach the module with Rust's host-call text; importFile needs the storage capability", async () => {
    const runtime = new ActionRuntime({ registry, tenant, sandbox: service() });
    const who = session(`t-${crypto.randomUUID()}`);
    const call = async (operation: string, args: unknown[]) =>
      JSON.parse(((await runtime.execute({ session: who, path: "assistant.failing", args: { operation, args } })) as { value: string }).value);
    expect(await call("status", ["sbx_00000000-0000-4000-8000-000000000000", "run_1"])).toBe(
      "GonvexHostError: host call failed: sandbox host call failed [host_call_failed]: sandbox was not found",
    );
    expect(await call("importFile", ["sbx_x", { fileId: "f", filename: "a.csv" }])).toBe(
      "GonvexHostError: host call failed: sandbox host call failed [host_call_failed]: sandbox importFile also requires the storage capability",
    );
    expect(await call("create", [{ ttlMs: 999_999_999 }])).toBe(
      "GonvexHostError: host call failed: sandbox host call failed [host_call_failed]: sandbox ttl exceeds runtime policy",
    );
  });
});
