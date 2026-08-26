import assert from "node:assert/strict";
import test from "node:test";

import { action, createModule, internalQuery, schema } from "../dist/index.js";

const queryPlan = {
  table: "tasks",
  key: "id",
  columns: ["id", "title"],
};

test("agent Actions expose only declared internal Query and Reducer tools", () => {
  const app = createModule({ name: "agents", version: "1" });
  app.query("agents.searchTasks", {
    internal: true,
    args: schema.object({ search: schema.string() }),
    result: schema.array(schema.object({ id: schema.id("tasks"), title: schema.string() })),
    liveQueryPlan: queryPlan,
  });
  app.reducer("agents.renameTask", {
    args: schema.object({ taskId: schema.id("tasks"), title: schema.string() }),
    result: schema.object({ ok: schema.boolean() }),
    offline: { mode: "forbidden" },
    interactive: false,
  });
  app.action("agents.run", {
    profile: "agent",
    capabilities: {
      networkOrigins: ["https://api.openai.com"],
      secrets: ["OPENAI_API_KEY"],
      tools: {
        searchTasks: { kind: "query", function: "agents.searchTasks" },
        renameTask: { kind: "reducer", function: "agents.renameTask" },
      },
    },
  });

  const definition = app.manifest().functions["agents.run"];
  assert.equal(definition.actionProfile, "agent");
  assert.deepEqual(definition.actionCapabilities.tools.searchTasks, {
    kind: "query",
    function: "agents.searchTasks",
  });
});

test("agent Query tools must target internal Queries", () => {
  const app = createModule({ name: "agents", version: "1" });
  app.query("tasks.public", { liveQueryPlan: queryPlan });
  app.action("agents.run", {
    profile: "agent",
    capabilities: { tools: { searchTasks: { kind: "query", function: "tasks.public" } } },
  });
  assert.throws(() => app.manifest(), /must target an internal one-shot Query/);
});

test("standard Actions are capability-empty unless explicitly declared", () => {
  const definition = action({
    args: schema.object({}),
    result: schema.object({ ok: schema.boolean() }),
  });
  assert.equal(definition.options.profile, undefined);
  assert.equal(definition.options.capabilities, undefined);
  assert.equal(Object.hasOwn(definition, "internal"), false);

  const internal = internalQuery({ liveQueryPlan: queryPlan });
  assert.equal(internal.internal, true);
});

test("capability declarations reject ambient network and secret access", () => {
  assert.throws(() => action({ capabilities: { networkOrigins: ["https://api.openai.com/v1"] } }), /exact HTTP\(S\) origin/);
  assert.throws(() => action({ capabilities: { secrets: ["openai-key"] } }), /uppercase environment names/);
  assert.throws(() => action({ capabilities: { tools: { search: { kind: "query", function: "tasks.search" } } } }), /profile "agent"/);
  assert.throws(() => action({ capabilities: { sandbox: {} } }), /sandbox requires profile "agent"/);
});

test("agent Actions may opt into the isolated TypeScript sandbox and DuckDB", () => {
  const definition = action({
    profile: "agent",
    capabilities: { sandbox: { duckdb: true }, storage: true },
    run: async (ctx) => {
      const handle = await ctx.sandbox.create();
      return { sandboxId: handle.sandboxId, hasStorage: typeof ctx.storage.store === "function" };
    },
  });
  assert.deepEqual(definition.options.capabilities.sandbox, { duckdb: true });
});

test("agent Actions may invoke the signed interactive catalog only when explicitly declared", () => {
  const definition = action({
    profile: "agent",
    capabilities: { functions: true },
    interactive: false,
    description: "Run one catalog function",
    agent: { tags: ["orchestration"], confirmation: "required" },
  });
  assert.equal(definition.options.capabilities.functions, true);
  assert.equal(definition.options.interactive, false);
  assert.equal(definition.options.description, "Run one catalog function");
  assert.deepEqual(definition.options.agent, {
    tags: ["orchestration"],
    confirmation: "required",
  });

  assert.throws(
    () => action({ capabilities: { functions: true } }),
    /functions require profile "agent"/,
  );
});

test("ModuleBuilder preserves signed catalog metadata and classification defaults", () => {
  const app = createModule({ name: "catalog", version: "1" });
  app.query("tasks.get", {
    liveQueryPlan: queryPlan,
    description: "Get a task",
    agent: { tags: ["tasks", "lookup", "tasks"], confirmation: "none" },
  });
  app.action("tasks.export", {
    description: "Export tasks",
  });
  app.action("tasks.startExport", {
    interactive: true,
    description: "Start a task export",
    agent: { tags: ["tasks", "export"], confirmation: "required" },
  });

  const functions = app.manifest().functions;
  assert.deepEqual(functions["tasks.get"].agent, {
    tags: ["lookup", "tasks"],
    confirmation: "none",
  });
  assert.equal(functions["tasks.get"].classification, "interactive");
  assert.equal(functions["tasks.get"].description, "Get a task");
  assert.equal(functions["tasks.export"].classification, "system");
  assert.equal(functions["tasks.export"].interactive, undefined);
  assert.equal(functions["tasks.startExport"].classification, "interactive");
  assert.equal(functions["tasks.startExport"].interactive, true);
});
