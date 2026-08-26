import { action, liveQuery, query, reducer, schema, visibility } from "@gonvex/module-sdk";

export const messagesVisibility = visibility({
  table: "messages",
  key: "id",
  sets: {},
  where: { operator: "public" },
});

export const list = liveQuery({
  args: schema.object({}),
  result: schema.array(schema.object({
    id: schema.id("messages"),
    body: schema.string(),
    author: schema.string(),
    created_at: schema.datetime(),
  })),
  liveQueryPlan: {
    table: "messages",
    key: "id",
    columns: ["id", "body", "author", "created_at"],
    sort: {
      defaultColumn: "created_at",
      defaultDirection: "asc",
      allowedColumns: ["created_at"],
    },
  },
  run: async () => [],
});

export const get = query({
  interactive: true,
  description: "Read one visible message by ID.",
  agent: { tags: ["messages"], confirmation: "none" },
  args: schema.object({ id: schema.id("messages") }),
  result: schema.array(schema.object({
    id: schema.id("messages"),
    body: schema.string(),
    author: schema.string(),
    created_at: schema.datetime(),
  })),
  liveQueryPlan: {
    table: "messages",
    key: "id",
    columns: ["id", "body", "author", "created_at"],
    where: { operator: "eq", column: "id", value: { argument: "id" } },
  },
  run: async () => [],
});

export const send = reducer({
  interactive: true,
  description: "Send a message as the acting tenant member.",
  agent: { tags: ["messages"], confirmation: "none" },
  args: schema.object({ body: schema.string() }),
  result: schema.object({
    id: schema.id("messages"),
    body: schema.string(),
    author: schema.string(),
    created_at: schema.datetime(),
  }),
  offline: { mode: "onlineOnly", reason: "server assigns the message id" },
  nonOptimisticReason: "server assigns the message id",
  run: async ({ db, member }, args) => {
    if (!member || member.status !== "active") throw new Error("active tenant membership required");
    return db.insert("messages", {
      id: crypto.randomUUID(),
      body: args.body,
      author: member.displayName ?? member.id,
    });
  },
});

export const echo = action({
  interactive: true,
  description: "Return the supplied message without changing durable state.",
  agent: { tags: ["messages"], confirmation: "none" },
  args: schema.object({ message: schema.string() }),
  result: schema.object({ message: schema.string() }),
  run: async (_ctx, args) => args,
});

/** Minimal reference for testing permission-equivalent delegated invocation. */
export const agentInvoke = action({
  profile: "agent",
  interactive: false,
  description: "Invoke one function from the active interactive catalog.",
  capabilities: { functions: true },
  args: schema.object({
    path: schema.string(),
    args: schema.any(),
    artifactHash: schema.string(),
  }),
  result: schema.any(),
  run: (ctx, args) => ctx.functions.invoke({
    path: args.path,
    args: args.args,
    artifactHash: args.artifactHash,
  }),
});
