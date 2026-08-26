# Agent Actions

Agent Actions are an opt-in Action profile for AI orchestration. They do not
change the Query/Reducer/Action model: reads still belong to Queries and writes
still belong to Reducers.

## Runtime policy

The deployment must explicitly enable the profile:

```env
GONVEX_AGENT_ACTIONS_ENABLED=true
GONVEX_AGENT_ACTION_CONCURRENCY=4
GONVEX_AGENT_ACTION_TIMEOUT=2m
```

The manifest declaration cannot turn the profile on or raise these limits.
Agent Actions share the bounded module host, but enter through their own
admission lane. Standard Actions remain on the normal short execution budget.

## Declaration

```ts
import { action, internalQuery, schema } from "@gonvex/module-sdk";

export const searchTasks = internalQuery({
  args: schema.object({
    search: schema.string(),
    offset: schema.optional(schema.integer()),
    limit: schema.optional(schema.integer()),
  }),
  result: schema.array(schema.object({
    id: schema.id("tasks"),
    title: schema.string(),
  })),
  liveQueryPlan: {
    table: "tasks",
    key: "id",
    columns: ["id", "title", "workspaceId"],
    search: { argument: "search", columns: ["title"] },
    window: {
      offsetArgument: "offset",
      limitArgument: "limit",
      defaultLimit: 25,
      maxLimit: 100,
    },
  },
});

export const runTaskAgent = action({
  profile: "agent",
  capabilities: {
    networkOrigins: ["https://api.openai.com"],
    secrets: ["OPENAI_API_KEY"],
    tools: {
      searchTasks: { kind: "query", function: "agents.searchTasks" },
      renameTask: { kind: "reducer", function: "tasks.rename" },
    },
    storage: true,
    sandbox: { duckdb: true },
  },
  args: schema.object({ prompt: schema.string() }),
  result: schema.object({ answer: schema.string() }),
  run: async (ctx, args) => {
    const rows = await ctx.tools.searchTasks({ search: args.prompt });
    // Construct the pinned Vercel ToolLoopAgent here. Pass ctx.fetch and
    // ctx.secrets.OPENAI_API_KEY to the provider.
    return { answer: JSON.stringify(rows) };
  },
});
```

The artifact validator requires every Query tool to target an internal Query.
At runtime, V8 exposes only the declared tool names. Rust rejects an undeclared
tool call before it crosses the process boundary, resolves the signed binding,
and enters the normal Query or Reducer executor with the original
account, member, tenant, permissions, visibility plan, admission limits, and
telemetry.

## Rules

- Do not add `ctx.db`, `ctx.runQuery(path)`, or `ctx.runReducer(path)`.
- Keep read tools small, structured, visibility-filtered, and result-bounded.
- Bind write tools to business-intent Reducers, not generic row updates.
- Pass `expectedVersion` for workflow decisions and other conflict-sensitive writes.
- Declare the narrowest network origin and only the secret names the Action uses.
- Do not return secrets or provider credentials from an Action.
- Use the Local Replica for frontend state. An agent Reducer confirmation arrives
  through the same committed transaction stream as any other Reducer.
- Use a browser Realtime session and an ephemeral token for Advanced Voice.
  Do not keep a voice WebSocket inside an ordinary Action.
- Declare `sandbox: {}` only for Actions that execute generated TypeScript.
  Add `duckdb: true` only when the workflow needs tabular analysis.
- Move tenant data through declared internal Query tools. Never give sandbox
  code a database credential or an arbitrary query path.
- Import CSV/XLS/XLSX from Gonvex storage with `ctx.sandbox.importFile`. This
  checks the tenant and private-file owner before copying a read-only DuckDB
  artifact into the caller-owned workspace.
- Do not expose `fs`, `process`, shell access, Node built-ins, project secrets,
  object-storage credentials, or raw host paths to generated code.

## Web compatibility

The TypeScript isolate provides the Web primitives required by the pinned AI
compatibility fixture: browser-conditional package exports, `URL`,
`URLSearchParams`, `TextEncoder`, `TextDecoder`, `AbortController`, timers,
`ReadableStream`, `WritableStream`, `TransformStream`, `Headers`, `Response`,
`crypto.getRandomValues`, `crypto.randomUUID`, and SHA-256 digest support.

`ctx.fetch` remains host-owned, origin-allowlisted, deadline-bound, and capped at
8 MiB per response. The current response stream is backed by that bounded host
buffer; use non-streaming agent completion when the result must be returned by
an ordinary Action.

## Sandbox and DuckDB

The deployment must opt in separately from Agent Actions:

```env
GONVEX_SANDBOX_ENABLED=true
GONVEX_SANDBOX_WORKER_BINARY=/usr/local/bin/gonvex-sandbox-worker
```

The production worker runs out of process, chroots into one account-and-tenant
workspace, drops to the nobody user, installs a network-denying seccomp filter,
and has independent heap, file, workspace, output, row, concurrency, and time
budgets. Global workspace and per-workspace execution-history limits prevent
idle callers from retaining unbounded host memory or disk state.
`GONVEX_SANDBOX_ALLOW_UNCONFINED=true` exists only for local
development and is refused by default.

```ts
const workspace = await ctx.sandbox.create();

// Analyze rows returned by a declared, visibility-checked internal Query.
const tasks = await ctx.tools.tasksForAnalysis({ workspaceId: args.workspaceId });
await ctx.sandbox.writeText(
  workspace.sandboxId,
  "tasks.json",
  JSON.stringify(tasks),
);

// Or attach an authorized uploaded spreadsheet without loading its bytes into
// the agent prompt. This method also requires capabilities.storage = true.
const workbook = await ctx.sandbox.importFile(workspace.sandboxId, {
  fileId: args.fileId,
  filename: args.filename,
});

const execution = await ctx.sandbox.run(workspace.sandboxId, {
  code: `
    const taskRows = JSON.parse(await files.readText("tasks.json"));
    await duckdb.register("tasks", taskRows);
    console.log("available workbook", ${JSON.stringify(workbook)});
    return await duckdb.query(
      "select status, count(*) as total from tasks group by status order by total desc"
    );
  `,
  timeoutMs: 30_000,
});
```

`run` is asynchronous. Poll `status`, or call `cancel`; only a completed
execution returns its result. Files persist for the sandbox TTL, while each V8
worker process is disposable. DuckDB is private to that workspace and starts
with extension auto-install, external access, persistent secrets, and community
extensions disabled and configuration locked after approved workbook databases
are attached read-only.
