import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

function generateBindings(source) {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-optimistic-writes-"));
  mkdirSync(join(project, "gonvex"));
  mkdirSync(join(project, "node_modules", "@gonvex"), { recursive: true });
  symlinkSync(fileURLToPath(new URL("../../client", import.meta.url)), join(project, "node_modules", "@gonvex", "client"), "dir");
  symlinkSync(fileURLToPath(new URL("../../protocol", import.meta.url)), join(project, "node_modules", "@gonvex", "protocol"), "dir");
  writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "optimistic-writes-test", module: { entrypoint: "gonvex/index.ts" } }));
  writeFileSync(join(project, "gonvex", "index.ts"), source);

  const environment = Object.fromEntries(
    Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")),
  );
  spawnSync(
    process.execPath,
    [cli, "codegen", "--project", project],
    { env: environment, encoding: "utf8" },
  );
  return project;
}

test("generated API derives optimistic patches only from an explicit reducer contract", async () => {
  const project = generateBindings(`const schema = {
  object: (fields) => ({ kind: "object", fields }),
  id: (entity) => ({ kind: "id", entity }),
  string: () => ({ kind: "string" }),
  boolean: () => ({ kind: "boolean" }),
};
const reducer = (options) => options;
const query = (options) => options;
const action = (options) => options;
const replicaCollection = (options) => options;
export const update = reducer({
  name: "tasks.update",
  args: schema.object({ taskId: schema.id("tasks"), updates: schema.object({ title: schema.string() }) }),
  result: schema.object({ ok: schema.boolean() }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: { effects: [{ operation: "patch", entity: "tasks", id: ["taskId"], fields: { title: "pending" } }] },
  run: async () => ({ ok: true }),
});
export const preview = query({
  name: "tasks.preview",
  args: schema.object({}), result: schema.object({ ok: schema.boolean() }),
  liveQueryPlan: { table: "tasks", key: "id", columns: ["id"] },
  run: async () => ({ ok: true }),
});
export const reindex = action({
  name: "tasks.reindex",
  args: schema.object({}), result: schema.object({ ok: schema.boolean() }),
  run: async () => ({ ok: true }),
});
export const sync = replicaCollection({
  name: "tasks.sync",
  args: schema.object({}),
  result: schema.object({ id: schema.id("tasks"), title: schema.string() }),
  replica: { table: "tasks", key: "id", columns: ["id", "title"] },
  run: async () => ({ id: "task-1", title: "" }),
});
`);

  try {
    const apiPath = join(project, "gonvex", "_generated", "api.ts");
    const generated = readFileSync(apiPath, "utf8");
    assert.doesNotMatch(generated, /optimisticReducers|optimisticProjection|projection:/);

    const bindings = await import(`${pathToFileURL(apiPath).href}?test=${Date.now()}`);
    assert.ok(bindings.api.tasks.update.optimistic);
    assert.equal(bindings.api.tasks.preview.optimistic, undefined);
    assert.equal(bindings.api.tasks.sync.optimistic, undefined);
    assert.deepEqual(bindings.optimisticTransactions["tasks.update"].effects[0], {
      operation: "patch", entity: "tasks", id: ["taskId"], fields: { title: "pending" },
    });
    assert.deepEqual(bindings.optimisticPatchesFor("tasks.update", { taskId: "task-3" }), [
      { entity: "tasks", rowId: "task-3", op: "patch", fields: { title: "pending" } },
    ]);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});

test("generated API preserves an explicitly reviewed non-optimistic Reducer", async () => {
  const project = generateBindings(`const schema = {
  object: (fields) => ({ kind: "object", fields }),
  string: () => ({ kind: "string" }),
  boolean: () => ({ kind: "boolean" }),
};
const query = (options) => options;
const reducer = (options) => options;
export const list = query({ name: "tasks.list", args: schema.object({}), result: schema.object({ ok: schema.boolean() }), liveQueryPlan: { table: "tasks", key: "id", columns: ["id"] }, run: async () => ({ ok: true }) });
export const create = reducer({ name: "tasks.create", args: schema.object({}), result: schema.object({ ok: schema.boolean() }), offline: { mode: "onlineOnly", reason: "test fixture" }, nonOptimisticReason: "test fixture", run: async () => ({ ok: true }) });
`);

  try {
    const apiPath = join(project, "gonvex", "_generated", "api.ts");
    const bindings = await import(`${pathToFileURL(apiPath).href}?test=${Date.now()}`);
    assert.deepEqual(bindings.optimisticTransactions, {});
    assert.deepEqual(bindings.optimisticPatchesFor("tasks.create", { id: "task-1", title: "No writes" }), []);
    assert.deepEqual(bindings.optimisticPatchesFor("constructor", { id: "task-1" }), []);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
