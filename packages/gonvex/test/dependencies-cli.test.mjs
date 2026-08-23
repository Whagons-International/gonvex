import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

test("codegen serializes only the structured Live Query plan", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-dependencies-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "dependency-test", module: { entrypoint: "gonvex/index.ts" } }));
    writeFileSync(
      join(project, "gonvex", "index.ts"),
      `const schema = {
  object: (fields) => ({ kind: "object", fields }),
  array: (items) => ({ kind: "array", items }),
  string: () => ({ kind: "string" }),
  boolean: () => ({ kind: "boolean" }),
};
const liveQuery = (options) => options;
const internalQuery = (options) => options;
const reducer = (options) => options;
export const locations = liveQuery({
  name: "locations.list",
  args: schema.object({ tenantId: schema.string(), offset: schema.string(), limit: schema.string() }),
  result: schema.array(schema.object({ tenantId: schema.string(), updatedAt: schema.string() })),
  liveQueryPlan: {
    table: "locations", key: "id", columns: ["tenantId", "updatedAt"],
    where: { operator: "eq", column: "tenantId", value: { argument: "tenantId" } },
    sort: { columnArgument: "sort", directionArgument: "direction", defaultColumn: "updatedAt", defaultDirection: "desc", allowedColumns: ["updatedAt"] },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 200 },
  },
  run: async () => [],
});
export const upsert = reducer({
  name: "locations.upsert",
  args: schema.object({}), result: schema.object({ ok: schema.boolean() }),
  offline: { mode: "onlineOnly", reason: "test fixture" },
  nonOptimisticReason: "test fixture", run: async () => ({ ok: true }),
});
export const beat = reducer({
  name: "presence.beat",
  args: schema.object({}), result: schema.object({ ok: schema.boolean() }),
  offline: { mode: "onlineOnly", reason: "test fixture" },
  nonOptimisticReason: "test fixture", run: async () => ({ ok: true }),
});
export const agentLookup = internalQuery({
  name: "agents.lookup",
  args: schema.object({ tenantId: schema.string() }),
  result: schema.array(schema.object({ tenantId: schema.string() })),
  liveQueryPlan: { table: "locations", key: "id", columns: ["id", "tenantId"] },
  run: async () => [],
});
`,
    );

    const environment = Object.fromEntries(
      Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")),
    );
    const generated = spawnSync(
      process.execPath,
      [cli, "codegen", "--project", project],
      { env: environment, encoding: "utf8" },
    );
    assert.equal(generated.status, 0, generated.stderr);
    assert.match(generated.stdout, /TypeScript function binding/);

    const manifest = JSON.parse(readFileSync(join(project, "gonvex", "_generated", "manifest.json"), "utf8"));
    assert.deepEqual(manifest.functions["locations.list"].dependencies, {
      liveQueryPlan: {
        table: "locations", key: "id", columns: ["tenantId", "updatedAt"],
        where: { operator: "eq", column: "tenantId", value: { argument: "tenantId" } },
        sort: { columnArgument: "sort", directionArgument: "direction", allowedColumns: ["updatedAt"], defaultColumn: "updatedAt", defaultDirection: "desc" },
        window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 200 },
      },
    });
    assert.equal(manifest.functions["locations.upsert"].kind, "reducer");
    assert.deepEqual(manifest.functions["locations.upsert"].dependencies, {
      nonOptimisticReason: "test fixture",
    });
    assert.deepEqual(manifest.functions["presence.beat"].dependencies, {
      nonOptimisticReason: "test fixture",
    });
    const apiSource = readFileSync(join(project, "gonvex", "_generated", "api.ts"), "utf8");
    assert.match(apiSource, /plan:\s*\{/);
    assert.match(apiSource, /table:\s*"locations"/);
    assert.match(apiSource, /import \{ control as gonvexControl, type LiveQueryPlan \} from "@gonvex\/client"/);
    assert.match(apiSource, /export const control = gonvexControl/);
    const publicSection = apiSource.slice(apiSource.indexOf("export const api ="), apiSource.indexOf("export const internal ="));
    const internalSection = apiSource.slice(apiSource.indexOf("export const internal ="), apiSource.indexOf("export type Api ="));
    assert.doesNotMatch(publicSection, /agents/);
    assert.match(internalSection, /lookup/);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
