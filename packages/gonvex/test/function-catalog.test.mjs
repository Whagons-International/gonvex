import assert from "node:assert/strict";
import test from "node:test";

import { functionCatalogEntries, renderFunctionCatalog, renderPortableSchema } from "../dist/function-catalog.js";

function artifact() {
  return {
    language: "typescript",
    generation: 8,
    hash: "artifact_123",
    entrypoint: "gonvex/index.ts",
    files: {},
    visibility: {},
    functions: {
      "z.system": { kind: "action", handler: "system", file: "gonvex/z.ts", args: { kind: "object", fields: {} }, result: { kind: "null" }, classification: "system" },
      "tasks.start": {
        kind: "reducer",
        handler: "start",
        file: "gonvex/tasks.ts",
        args: { kind: "object", fields: { expectedVersion: { kind: "number", integer: true }, taskId: { kind: "id", entity: "tasks" }, note: { kind: "optional", value: { kind: "string" } } } },
        result: { kind: "object", fields: { ok: { kind: "literal", value: true }, payload: { kind: "any" } } },
        classification: "interactive",
        interactive: true,
        description: "Start a task",
        agent: { tags: ["workflow", "tasks"], confirmation: "none" },
        offline: { mode: "allowed", conflict: "expectedVersion" },
        optimistic: { effects: [] },
      },
      "internal.secret": { kind: "query", handler: "secret", file: "gonvex/internal.ts", args: { kind: "null" }, result: { kind: "null" }, internal: true, classification: "internal" },
      "internal.forged": { kind: "query", handler: "forged", file: "gonvex/internal.ts", args: { kind: "null" }, result: { kind: "null" }, internal: true, classification: "interactive", interactive: true },
      "search.tasks": { kind: "query", handler: "search", file: "gonvex/search.ts", args: { kind: "array", items: { kind: "string" } }, result: { kind: "record", values: { kind: "null" } }, classification: "interactive", interactive: true },
      "agent.explain": { kind: "action", handler: "explain", file: "gonvex/agent.ts", args: { kind: "object", fields: {} }, result: { kind: "string" }, classification: "interactive", interactive: true },
    },
  };
}

test("catalog entries are deterministic, sorted, exact, and interactive only", () => {
  const first = renderFunctionCatalog(artifact(), "ndjson");
  const second = renderFunctionCatalog(structuredClone(artifact()), "ndjson");
  assert.equal(first, second);
  const rows = first.trim().split("\n").map(JSON.parse);
  assert.deepEqual(rows.map((row) => row.path), ["agent.explain", "search.tasks", "tasks.start"]);
  assert.deepEqual(rows[2].args, artifact().functions["tasks.start"].args);
  assert.deepEqual(rows[2].result, artifact().functions["tasks.start"].result);
  assert.equal(rows[2].artifactHash, "artifact_123");
  assert.deepEqual(rows[2].tags, ["tasks", "workflow"]);
  assert.deepEqual(functionCatalogEntries(artifact()).map((entry) => entry.kind), ["action", "query", "reducer"]);
});

test("TypeScript catalog renders every portable schema without invented precision", () => {
  const output = renderFunctionCatalog(artifact(), "typescript");
  assert.match(output, /"tasks\.start": \{/);
  assert.match(output, /taskId: Id<"tasks">;/);
  assert.match(output, /note\?: string;/);
  assert.match(output, /payload: JsonValue;/);
  assert.match(output, /args: Array<string>;/);
  assert.match(output, /result: Record<string, null>;/);
  assert.match(output, /classification: "interactive";/);
  assert.match(output, /interactive: true;/);
  assert.match(output, /offline: \{ readonly conflict: "expectedVersion"; readonly mode: "allowed"; \};/);
  assert.match(output, /optimistic: \{ readonly effects: readonly \[\]; \};/);
  assert.match(output, /description: "Start a task";/);
  assert.match(output, /tags: readonly \["tasks", "workflow"\];/);
  assert.match(output, /confirmation: "none";/);
  assert.equal(renderPortableSchema({ kind: "boolean" }), "boolean");
  assert.equal(renderPortableSchema({ kind: "literal", value: "open" }), '"open"');
  assert.equal(renderPortableSchema({ kind: "literal", value: null }), "null");
  assert.equal(renderPortableSchema({ kind: "any" }), "JsonValue");
});
