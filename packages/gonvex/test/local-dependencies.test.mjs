import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  analyzeLocalReducerDependencies,
  localDependencyTables,
  localDependencyViolations,
} from "../dist/local-dependencies.js";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

async function project(t, files, extra = {}) {
  const root = await mkdtemp(join(tmpdir(), "gonvex-local-dependencies-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await mkdir(join(root, "gonvex"));
  await mkdir(join(root, "migrations"));
  await mkdir(join(root, "node_modules", "@gonvex"), { recursive: true });
  // The fixture resolves the real SDK so helper calls type-check as they do in apps.
  await symlink(fileURLToPath(new URL("../../module-sdk", import.meta.url)), join(root, "node_modules", "@gonvex", "module-sdk"));
  for (const [name, source] of Object.entries(files)) await writeFile(join(root, "gonvex", name), source);
  for (const [name, source] of Object.entries(extra)) await writeFile(join(root, name), source);
  return root;
}

const header = `import { reducer, schema, selectRows, selectFirst, insertDataRows, applyDataWrites, getRow } from "@gonvex/module-sdk";
import type { ReducerContext } from "@gonvex/module-sdk";
const options = { args: schema.object({ id: schema.string(), table: schema.string() }), result: schema.any() } as const;
`;

async function analyze(t, files, reducers) {
  const root = await project(t, files);
  return analyzeLocalReducerDependencies({
    root,
    backendDir: join(root, "gonvex"),
    files: Object.keys(files).map((name) => join(root, "gonvex", name)),
    reducers: reducers.map(([file, exportName]) => ({ path: `${file}.${exportName}`, file: `gonvex/${file}.ts`, exportName })),
  });
}

const tableKeys = new Map([["visible", "_id"], ["hidden", "_id"], ["keyed", "code"]]);
const localTables = new Set(["visible"]);
const byPath = (analyses) => Object.fromEntries(analyses.map((analysis) => [analysis.path, localDependencyTables(analysis, tableKeys)]));

test("direct reads, keyed writes and transitive helpers become dependencies", async (t) => {
  const analyses = await analyze(t, {
    "helpers.ts": `import { selectRows } from "@gonvex/module-sdk";
import type { ReducerContext } from "@gonvex/module-sdk";
const deepest = (ctx: ReducerContext) => selectRows(ctx.db, { table: "hidden" });
export async function middle(ctx: ReducerContext) { return deepest(ctx); }`,
    "tasks.ts": `${header}
import { middle } from "./helpers";
const TABLE = "visible";
export const readsVisible = reducer({ ...options, run: async (ctx: ReducerContext) => selectFirst(ctx.db, { table: TABLE, where: { column: "_id", op: "eq", value: "x" } }) });
export const viaHelper = reducer({ ...options, run: async (ctx: ReducerContext) => { await middle(ctx); return null; } });
export const updates = reducer({ ...options, run: async (ctx: ReducerContext, args: { id: string }) => ctx.db.update("visible", args.id, { name: "b" }) });
export const byKey = reducer({ ...options, run: async (ctx: ReducerContext) => getRow(ctx.db, "hidden", "_id", "x") });`,
  }, [["tasks", "readsVisible"], ["tasks", "viaHelper"], ["tasks", "updates"], ["tasks", "byKey"]]);
  assert.deepEqual(byPath(analyses), {
    "tasks.byKey": ["hidden"],
    "tasks.readsVisible": ["visible"],
    "tasks.updates": ["visible"],
    "tasks.viaHelper": ["hidden"],
  });
  assert.deepEqual(
    localDependencyViolations(analyses, localTables, tableKeys).map(({ reducer, table, reason }) => [reducer, table, reason]),
    [["tasks.byKey", "hidden", "no generated local collection"], ["tasks.viaHelper", "hidden", "no generated local collection"]],
  );
});

test("a table passed to a generic helper resolves at each call site", async (t) => {
  const analyses = await analyze(t, {
    "generic.ts": `${header}
async function load(ctx: ReducerContext, table: string) { return selectRows(ctx.db, { table, columns: ["_id"] }); }
const makeReader = (table: string) => async (ctx: ReducerContext) => selectRows(ctx.db, { table });
const readHidden = makeReader("hidden");
export const visibleOnly = reducer({ ...options, run: async (ctx: ReducerContext) => load(ctx, "visible") });
export const both = reducer({ ...options, run: async (ctx: ReducerContext) => { for (const table of ["visible", "hidden"] as const) await load(ctx, table); return null; } });
export const factory = reducer({ ...options, run: async (ctx: ReducerContext) => readHidden(ctx) });`,
  }, [["generic", "visibleOnly"], ["generic", "both"], ["generic", "factory"]]);
  assert.deepEqual(byPath(analyses), {
    "generic.both": ["hidden", "visible"],
    "generic.factory": ["hidden"],
    "generic.visibleOnly": ["visible"],
  });
  assert.deepEqual(analyses.flatMap((analysis) => analysis.unresolved), []);
});

test("intent-keyed inserts need no local rows; explicit keys and other key columns do", async (t) => {
  const analyses = await analyze(t, {
    "inserts.ts": `${header}
export const generated = reducer({ ...options, run: async (ctx: ReducerContext) => {
  await ctx.db.insert("hidden", { name: "a" });
  await insertDataRows(ctx, "hidden", [{ name: "a" }, { name: "b" }]);
  await applyDataWrites(ctx, [{ kind: "insert", table: "hidden", rows: [{ name: "a" }] }]);
  return null;
} });
export const explicit = reducer({ ...options, run: async (ctx: ReducerContext) => ctx.db.insert("hidden", { _id: "fixed", name: "a" }) });
export const otherKey = reducer({ ...options, run: async (ctx: ReducerContext) => ctx.db.insert("keyed", { name: "a" }) });
export const batchUpdate = reducer({ ...options, run: async (ctx: ReducerContext) => applyDataWrites(ctx, [{ kind: "update", table: "hidden", rows: [{ id: "x", fields: { name: "b" } }] }]) });`,
  }, [["inserts", "generated"], ["inserts", "explicit"], ["inserts", "otherKey"], ["inserts", "batchUpdate"]]);
  assert.deepEqual(byPath(analyses), {
    "inserts.batchUpdate": ["hidden"],
    "inserts.explicit": ["hidden"],
    "inserts.generated": [],
    "inserts.otherKey": ["keyed"],
  });
});

test("raw SQL, dynamic tables and unknown tables are reported instead of dropped", async (t) => {
  const analyses = await analyze(t, {
    "dynamic.ts": `${header}
export const raw = reducer({ ...options, run: async (ctx: ReducerContext) => ctx.db.query("SELECT 1") });
export const fromArgs = reducer({ ...options, run: async (ctx: ReducerContext, args: { table: string }) => selectRows(ctx.db, { table: args.table }) });
export const unknown = reducer({ ...options, run: async (ctx: ReducerContext) => selectRows(ctx.db, { table: "missing" }) });`,
  }, [["dynamic", "raw"], ["dynamic", "fromArgs"], ["dynamic", "unknown"]]);
  const violations = localDependencyViolations(analyses, localTables, tableKeys);
  assert.deepEqual(violations.map(({ reducer, table }) => [reducer, table]), [
    ["dynamic.fromArgs", "?"],
    ["dynamic.raw", "*"],
    ["dynamic.unknown", "missing"],
  ]);
  assert.match(violations[0].reason, /cannot be determined statically/);
  assert.match(violations[1].reason, /raw SQL/);
  assert.match(violations[2].reason, /not a table in the tenant schema/);
  assert.match(violations[0].site, /^gonvex\/dynamic\.ts:\d+$/);
});

const migration = 'CREATE TABLE tasks ("_id" text PRIMARY KEY, title text); CREATE TABLE secrets ("_id" text PRIMARY KEY, value text);';
const moduleSource = (body) => `import { reducer, schema, replicaCollection, visibility, selectRows } from "@gonvex/module-sdk";
export const access = visibility({ table: "tasks", key: "_id", sets: {}, where: { operator: "public" } });
export const list = replicaCollection({ args: schema.object({}), result: schema.any(), replica: { table: "tasks", key: "_id", columns: ["_id", "title"], maxRows: 200, maxBytes: 100000 } });
export const rename = reducer({ args: schema.object({ id: schema.string(), title: schema.string() }), result: schema.any(), run: (ctx, args) => ctx.db.update("tasks", args.id, { title: args.title }) });
${body}`;

function codegen(root) {
  const environment = Object.fromEntries(Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")));
  return spawnSync(process.execPath, [cli, "codegen", "--project", root], { env: environment, encoding: "utf8" });
}

test("codegen emits localDependencies and fails on a dependency with no local collection", async (t) => {
  const root = await project(t, { "index.ts": moduleSource(`export const peek = reducer({ args: schema.object({}), result: schema.any(), run: (ctx) => selectRows(ctx.db, { table: "secrets" }) });`) },
    { "gonvex.json": JSON.stringify({ project: "local-deps" }) });
  await writeFile(join(root, "migrations", "0001_init.sql"), migration);
  const failed = codegen(root);
  assert.notEqual(failed.status, 0);
  assert.match(failed.stderr, /Local Reducer dependency check failed/);
  assert.match(failed.stderr, /secrets \(no generated local collection\): 1 Reducer\(s\)/);
  assert.match(failed.stderr, /at gonvex\/index\.ts:\d+ select/);

  await writeFile(join(root, "gonvex.json"), JSON.stringify({ project: "local-deps", module: { localDependencyCheck: "warn" } }));
  const warned = codegen(root);
  assert.equal(warned.status, 0, warned.stderr);
  assert.match(warned.stderr, /warning: 1 data access/);
  const manifest = JSON.parse(await readFile(join(root, "gonvex", "_generated", "manifest.json"), "utf8"));
  assert.deepEqual(manifest.functions.rename.localDependencies, ["tasks"]);
  assert.deepEqual(manifest.functions.peek.localDependencies, ["secrets"]);
  assert.equal(manifest.functions.list.localDependencies, undefined);
  // The client binding lists only dependencies a generated collection can satisfy.
  const executor = await readFile(join(root, "gonvex", "_generated", "local-executor.ts"), "utf8");
  assert.match(executor, /localDependencies: \{"rename":\["tasks"\]\} as const/);
});

test("codegen passes when every dependency has a local collection", async (t) => {
  const root = await project(t, { "index.ts": moduleSource("") }, { "gonvex.json": JSON.stringify({ project: "local-deps" }) });
  await writeFile(join(root, "migrations", "0001_init.sql"), migration);
  const generated = codegen(root);
  assert.equal(generated.status, 0, generated.stderr);
  const manifest = JSON.parse(await readFile(join(root, "gonvex", "_generated", "manifest.json"), "utf8"));
  assert.deepEqual(manifest.functions.rename.localDependencies, ["tasks"]);
});
