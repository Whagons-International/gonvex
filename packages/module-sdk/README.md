# `@gonvex/module-sdk`

The language-neutral Gonvex module contract for Query, Reducer, and Action
definitions.

This package is deliberately a declaration layer. It does not open sockets,
load Postgres drivers, read environment variables, or execute handlers. A
Gonvex host supplies those capabilities through its module engine. The
portable `ModuleManifest` produced here is suitable for `module.json`, client
code generation, deployment validation, and non-TypeScript module adapters.

```ts
import { createModule, schema } from "@gonvex/module-sdk";

const app = createModule({ name: "whagons", version: "1" });

app.reducer("tasks.start", {
  args: schema.object({ taskId: schema.id("tasks") }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [
      { operation: "patch", entity: "tasks", id: ["taskId"], fields: { status: "in_progress" } },
    ],
  },
});

export default app;
```

For TypeScript modules, the top-level helpers are executable declarations. The
V8 runtime executes exported bindings (and calls their `handler`), so a module
can export a reducer directly:

```ts
import { reducer, schema } from "@gonvex/module-sdk";

export const startTask = reducer({
  args: schema.object({ taskId: schema.id("tasks") }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [
      { operation: "patch", entity: "tasks", id: ["taskId"], fields: { status: "in_progress" } },
    ],
  },
  run: async ({ db, actions }, { taskId }) => {
    const task = await db.update("tasks", taskId, { status: "in_progress" });
    await actions.enqueue("notifications.taskStarted", { taskId });
    return task;
  },
});
```

`actions.enqueue` records the Action in the Reducer's Postgres transaction. The
host delivers it only after the business transaction commits and retries failed
deliveries from the durable outbox.

An application that accepts Control Plane invitations declares one internal
Reducer as the tenant-side handoff:

```ts
import {
  internalReducer,
  invitationAcceptance,
  schema,
} from "@gonvex/module-sdk";

export const applyInvitation = internalReducer({
  args: schema.object({
    accountId: schema.string(),
    memberId: schema.string(),
    invitationId: schema.string(),
    teamIds: schema.array(schema.string()),
    payload: schema.any(),
  }),
  result: schema.object({ applied: schema.boolean() }),
  run: async ({ db }, args) => {
    for (const teamId of args.teamIds) {
      await db.insert("memberTeams", { memberId: args.memberId, teamId });
    }
    return { applied: true };
  },
});

export const invitationLifecycle = invitationAcceptance(
  "invitations.applyInvitation",
);
```

The trusted host validates and claims the invitation, creates or activates the
canonical member, and invokes this Reducer in the same tenant transaction. The
Reducer never receives Control Plane credentials.

The host-specific implementation is intentionally separate from this SDK.
The manifest describes a TypeScript module executed by the bounded V8 host.

Replica Collections and Live Queries must reference a table with one exported
visibility plan. Rules are structured so the host can compile them into SQL,
cache equivalent contexts, and route committed old/new rows with the same
semantics:

```ts
import { visibility } from "@gonvex/module-sdk";

export const taskVisibility = visibility({
  table: "tasks",
  key: "id",
  sets: {
    assignedTasks: {
      table: "taskUsers",
      select: "taskId",
      joins: [],
      where: [{ table: "taskUsers", column: "memberId", context: "member.id" }],
    },
  },
  where: {
    operator: "or",
    children: [
      { operator: "permission", value: "tasks.viewAll" },
      { operator: "inSet", column: "id", set: "assignedTasks" },
    ],
  },
});
```

Executable handlers stay in a host-side registry. The registry dispatches only
when both the path and function kind match; it never creates database or
network capabilities itself:

```ts
const runtime = app.createRuntimeRegistry();
await runtime.dispatch({
  path: "tasks.start",
  kind: "reducer",
  context: reducerContext,
  args: { taskId: "task_123" },
});
```

`runtime.registrationPayload()` and `app.runtimePayload()` return deterministic,
handler-free payloads for a host loader. `QueryContext`, `ReducerContext`, and
`ActionContext` remain separate types, so capability boundaries are visible to
module authors and adapters.

Actions start without network, secrets, storage, scheduling, or function-call
authority. Declare the exact surface on the Action:

```ts
export const run = action({
  profile: "agent",
  capabilities: {
    networkOrigins: ["https://api.openai.com"],
    secrets: ["OPENAI_API_KEY"],
    tools: {
      searchTasks: { kind: "query", function: "agents.searchTasks" },
      renameTask: { kind: "reducer", function: "tasks.rename" },
    },
  },
  run: async (ctx) => ctx.tools.searchTasks({ search: "freezer" }),
});
```

Query tools must target `internalQuery(...)` declarations. Actions never
receive `ctx.db`, a generic function dispatcher, or the full project
environment.

Module identity follows the v2 account/member split:

```ts
run: async (ctx) => {
  const account = ctx.auth.account;
  const member = ctx.member;
  const tenant = ctx.tenant;

  if (!account || !member || !tenant) throw new Error("tenant identity is required");
  return { accountId: account.id, memberId: member.id, tenantId: tenant.id };
}
```

`auth.account`, `member`, and `tenant` are nullable because they mirror the
runtime ABI for anonymous or system invocations.

Application modules expose exactly three executable kinds: Query, Reducer, and
Action. An infrastructure adapter that accepts an inbound webhook verifies and
normalizes the request, then invokes an Action; webhooks are not a fourth module
function kind.
