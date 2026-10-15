import { reducer, schema } from "@gonvex/module-sdk";
import { createPortableReducer } from "@gonvex/local-runtime/portable-client";
import type { FunctionReference } from "../../src/index.js";
export const artifactHash = "engine-worker-fixture-v1";
export const columns = ["id", "count", ...Array.from({ length: 48 }, (_, index) => `field${index}`)];
export function row(index: number) {
  return Object.fromEntries([["id", String(index)], ["count", index], ...Array.from({ length: 48 }, (_, column) => [`field${column}`, column % 3 === 0 ? { label: "x".repeat(42), active: index % 2 === 0 } : `s${index}_${column}`.padEnd(27, "s")])]);
}
export const visible: FunctionReference = { kind: "query", path: "tasks.visible", delivery: "replica", replica: { table: "tasks", key: "id", columns, maxRows: 250 } };
export const source: FunctionReference = { ...visible, path: "tasks.source", replica: { ...visible.replica!, maxRows: 1_000 } };
export const full: FunctionReference = { ...visible, path: "tasks.full", replica: { ...visible.replica!, maxRows: 20_000 } };
const insert = reducer({ args: schema.object({ id: schema.string() }), result: schema.number(), run: async (ctx, args: { id: string }) => { await ctx.db.insert("edits", { id: args.id, count: 1 }); return 1; } });
const deny = reducer({ args: schema.object({ api: schema.string() }), result: schema.boolean(), run: async (_ctx, args: { api: string }) => {
  if (args.api === "fetch") await fetch("/must-not-fetch");
  else if (args.api === "WebSocket") new WebSocket("ws://localhost/must-not-open");
  else new XMLHttpRequest();
  return false;
} });
export const editCollection: FunctionReference = { kind: "query", path: "__local.edits", delivery: "replica", replica: { table: "edits", key: "id", columns: ["id", "count"], maxRows: 1000 } };
export const editRef: FunctionReference = { kind: "reducer", path: "insert", localExecution: 1, offline: { mode: "allowed" } };
export const denyRef: FunctionReference = { ...editRef, path: "deny" };
export const binding = { mode: "portable" as const, artifactHash, tables: ["edits"], collections: [editCollection], create: () => createPortableReducer({ artifactHash, reducers: { insert, deny }, schema: { edits: { key: "id", columns: { id: { type: "text", nullable: false }, count: { type: "bigint", nullable: false } } } } }) };
