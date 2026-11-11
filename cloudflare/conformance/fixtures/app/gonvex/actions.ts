import { action, internalQuery, internalReducer, query, schema } from "@gonvex/module-sdk";

// Action runtime (CONTRACT.md B.1, B.2a, B.6; package K12): every Action
// capability through `action.call`. The network origin is the scenario's own
// upstream (scenarios/actions-upstream.mjs), started on a fixed port. Every
// value an Action returns is chosen so it is the same on every run.
//
// Codegen accepts only static literals in the definitions, so the origin is
// written out in each one.

const errorText = async (run: () => unknown): Promise<string> => {
  try { await run(); return "no error"; } catch (error) { return error instanceof Error ? `${error.name}: ${error.message}` : String(error); }
};

/** A call's value or its error text, so one refusal does not hide the other answers. */
const outcome = async (run: () => unknown): Promise<{ value: unknown } | { error: string }> => {
  try { return { value: await run() }; } catch (error) { return { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) }; }
};

const UP = "http://127.0.0.1:47812";

/** Response fields that are the same on every run (no date, no connection headers). */
const shape = async (response: Response, read: "text" | "none" = "text") => ({
  status: response.status,
  statusText: response.statusText,
  url: response.url,
  contentType: response.headers.get("content-type"),
  body: read === "text" ? await response.text() : null,
});

/** An Action with no capabilities: its ctx surface and the globals an Action sees. */
export const surface = action({
  args: schema.object({}),
  result: schema.any(),
  run: async (ctx) => {
    const c = ctx as unknown as Record<string, unknown>;
    const invocation = c.invocation as Record<string, unknown>;
    const before = Date.now();
    return {
      keys: Object.keys(c).sort(),
      kind: c.kind,
      function: c.function,
      clockNotPinned: typeof c.now === "number" && before >= (c.now as number),
      entropy: crypto.getRandomValues(new Uint8Array(8)).length,
      uuid: /^[0-9a-f-]{36}$/.test(crypto.randomUUID()),
      random: typeof Math.random(),
      typeofFetch: typeof (globalThis as { fetch?: unknown }).fetch,
      invocationKeys: Object.keys(invocation).sort(),
      channel: invocation.channel,
      rootChannel: invocation.rootChannel,
      commandIdShape: /^action-[0-9a-f-]{36}$/.test(String(invocation.commandId)),
      rootIsCommand: invocation.rootCommandId === invocation.commandId,
      parentCommandId: invocation.parentCommandId ?? null,
      agentExecutionId: invocation.agentExecutionId ?? null,
    };
  },
});

/** `ctx.fetch` within its declared origin: body, status, redirects, caps, streams. */
export const fetchProbe = action({
  args: schema.object({}),
  result: schema.any(),
  capabilities: { networkOrigins: ["http://127.0.0.1:47812"] },
  run: async (ctx) => {
    const out: Record<string, unknown> = {};
    out.keys = Object.keys(ctx).sort();
    out.json = await shape(await ctx.fetch(`${UP}/json`));
    out.parsed = await (await ctx.fetch(`${UP}/json`)).json();
    out.teapot = await shape(await ctx.fetch(`${UP}/status/418`));
    out.created = await shape(await ctx.fetch(`${UP}/status/201`));
    out.noContent = await shape(await ctx.fetch(`${UP}/status/204`));
    out.echo = await (await ctx.fetch(`${UP}/echo`, { method: "post", headers: { "X-K12": "yes" }, body: "plain body" })).json();
    out.echoJson = await (await ctx.fetch(`${UP}/echo`, { method: "PUT", body: { a: 1 } as unknown as BodyInit })).json();
    out.redirect = await shape(await ctx.fetch(`${UP}/redirect?to=/text`));
    out.redirectChain = await shape(await ctx.fetch(`${UP}/redirect?to=/redirect%3Fto%3D/json`));
    out.redirectOut = await errorText(() => ctx.fetch(`${UP}/redirect?to=http://127.0.0.1:47813/text`));
    out.redirectsAfter = await errorText(() => ctx.fetch(`${UP}/text`).then((r) => r.text()));
    out.binary = await (await ctx.fetch(`${UP}/binary`)).text();
    out.bigLength = await errorText(() => ctx.fetch(`${UP}/bytes?n=8388609`));
    out.bigChunked = await errorText(() => ctx.fetch(`${UP}/bytes?n=9000000&chunked=1`));
    out.missing = await shape(await ctx.fetch(`${UP}/nowhere`));
    out.badMethod = await errorText(() => ctx.fetch(`${UP}/text`, { method: "BAD METHOD" }));
    return out;
  },
});

/** An event stream is read as it arrives, with UTF-8 characters never split. */
export const streamProbe = action({
  args: schema.object({}),
  result: schema.any(),
  capabilities: { networkOrigins: ["http://127.0.0.1:47812"] },
  run: async (ctx) => {
    const response = await ctx.fetch(`${UP}/sse?events=5`);
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    const chunks: string[] = [];
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      chunks.push(decoder.decode(part.value, { stream: true }));
    }
    const text = chunks.join("");
    const cancelled = await ctx.fetch(`${UP}/sse?events=50&delayMs=50`);
    const cancelReader = cancelled.body!.getReader();
    const first = decoder.decode((await cancelReader.read()).value);
    await cancelReader.cancel();
    const big = await errorText(async () => {
      const r = (await ctx.fetch(`${UP}/sse-big`)).body!.getReader();
      for (;;) if ((await r.read()).done) break;
    });
    return {
      status: response.status,
      contentType: response.headers.get("content-type"),
      text,
      events: text.split("\n\n").filter(Boolean).length,
      everyChunkWhole: chunks.every((chunk) => !chunk.includes("�")),
      firstOfCancelled: first.startsWith("data: "),
      big,
      afterStreams: await (await ctx.fetch(`${UP}/text`)).text(),
    };
  },
});

/** A URL outside the declared origins is a denial: it fails the call even when caught. */
export const fetchUndeclared = action({
  args: schema.object({}),
  result: schema.any(),
  capabilities: { networkOrigins: ["http://127.0.0.1:47812"] },
  run: async (ctx) => ({ caught: await errorText(() => ctx.fetch("http://127.0.0.1:47813/text")) }),
});

/** A relative URL has no origin: also a denial. */
export const fetchRelative = action({
  args: schema.object({}),
  result: schema.any(),
  capabilities: { networkOrigins: ["http://127.0.0.1:47812"] },
  run: async (ctx) => ({ caught: await errorText(() => ctx.fetch("/text")) }),
});

/** A declared secret that the project has not configured fails the call before it runs. */
export const secretMissing = action({
  args: schema.object({}),
  result: schema.any(),
  capabilities: { secrets: ["K12_NOT_CONFIGURED"] },
  run: async (ctx) => ({ keys: Object.keys(ctx).sort() }),
});

/** Statically bound tools: a Query, a Reducer and an internal Reducer. */
export const toolsProbe = action({
  profile: "agent",
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  capabilities: {
    tools: {
      rows: { kind: "query", function: "actions.boardRows" },
      add: { kind: "reducer", function: "board.add" },
      hidden: { kind: "internalReducer", function: "actions.internalAdd" },
    },
  },
  run: async (ctx, args) => {
    const tools = ctx.tools as unknown as Record<string, (input?: unknown) => Promise<unknown>>;
    return {
      keys: Object.keys(ctx).sort(),
      tools: Object.keys(tools).sort(),
      before: await outcome(() => tools.rows!({})),
      added: await tools.add!({ id: args.id, title: "from a tool", position: 70 }),
      hidden: await tools.hidden!({ id: `${args.id}-hidden` }),
      after: await outcome(() => tools.rows!({})),
      duplicate: await errorText(() => tools.add!({ id: args.id, title: "again", position: 71 })),
      badArgs: await errorText(() => tools.add!({ id: 7 })),
    };
  },
});

/** `ctx.functions.invoke` on the active artifact: Query, Reducer, Action, and its refusals. */
export const functionsProbe = action({
  profile: "agent",
  interactive: true,
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  capabilities: { functions: true },
  run: async (ctx, args) => {
    const artifactHash = String((ctx.invocation as unknown as Record<string, unknown>).artifactHash);
    const invoke = (path: string, input: unknown, hash = artifactHash) => ctx.functions.invoke({ path, args: input as never, artifactHash: hash });
    return {
      keys: Object.keys(ctx).sort(),
      count: await invoke("actions.boardCount", {}),
      added: await invoke("board.add", { id: args.id, title: "from functions", position: 80 }),
      action: await invoke("actions.childAction", { n: 2 }),
      stale: await errorText(() => invoke("actions.boardCount", {}, "not-the-active-artifact")),
      internal: await errorText(() => invoke("actions.internalAdd", { id: "x" })),
      system: await errorText(() => invoke("actions.surface", {})),
      missing: await errorText(() => invoke("actions.nope", {})),
      recursive: await errorText(() => invoke("actions.functionsProbe", { id: "again" })),
      badArgs: await errorText(() => invoke("board.add", { id: 1 })),
      replica: await outcome(() => invoke("board.all", {})),
    };
  },
});

/** An interactive Action invoked through `ctx.functions`: it sees the delegated invocation. */
export const childAction = action({
  profile: "agent",
  interactive: true,
  args: schema.object({ n: schema.integer() }),
  result: schema.any(),
  run: async (ctx, args) => {
    const invocation = ctx.invocation as unknown as Record<string, unknown>;
    return {
      doubled: args.n * 2,
      channel: invocation.channel,
      rootChannel: invocation.rootChannel,
      hasParent: typeof invocation.parentCommandId === "string",
      commandIdShape: /^agent-[0-9a-f-]{36}$/.test(String(invocation.commandId)),
      agentExecutionShape: /^agent_[0-9a-f]{32}$/.test(String(invocation.agentExecutionId)),
    };
  },
});

/** `ctx.scheduler` from an Action: enqueued directly, outside any transaction. */
export const schedulerProbe = action({
  args: schema.object({}),
  result: schema.any(),
  capabilities: { scheduler: true },
  run: async (ctx) => {
    const after = await ctx.scheduler.runAfter(3_600_000, "board.add", { id: "never", title: "never", position: 1 });
    const at = await ctx.scheduler.runAt(4_102_444_800_000, "actions.surface", {});
    const wake = await ctx.scheduler.wakeAt(4_102_444_800_000, "actions.surface", {});
    const wakeAgain = await ctx.scheduler.wakeAt(4_102_444_700_000, "actions.surface", {});
    return {
      keys: Object.keys(ctx).sort(),
      scheduler: Object.keys(ctx.scheduler).sort(),
      after: /^job_[0-9a-f-]{36}$/.test(after),
      at: /^job_[0-9a-f-]{36}$/.test(at),
      wake: /^job_[0-9a-f-]{36}$/.test(wake),
      coalesced: wake === wakeAgain,
      unregistered: await errorText(() => ctx.scheduler.runAfter(1, "actions.nope", {})),
      query: await errorText(() => ctx.scheduler.runAfter(1, "actions.boardCount", {})),
      negative: await errorText(() => ctx.scheduler.runAfter(-1, "board.add", {})),
    };
  },
});

/** `ctx.storage` from an Action; this backend has no object store configured. */
export const storageProbe = action({
  args: schema.object({}),
  result: schema.any(),
  capabilities: { storage: true },
  run: async (ctx) => {
    const storage = ctx.storage as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>;
    return {
      keys: Object.keys(ctx).sort(),
      storage: Object.keys(storage).sort(),
      metadata: await errorText(() => storage.getMetadata!("missing-file")),
      upload: await errorText(() => storage.generateUploadUrl!({ contentType: "text/plain", size: 4 })),
      unknown: await errorText(() => storage.call!("rename", {})),
    };
  },
});

/** Throws: the error a caller sees. */
export const fails = action({
  args: schema.object({}),
  result: schema.any(),
  run: async () => { throw new Error("k12 action failed on purpose"); },
});

/** A public Action: anonymous callers may run it. */
export const publicEcho = action({
  public: true,
  args: schema.object({ text: schema.string() }),
  result: schema.object({ text: schema.string(), account: schema.any() }),
  run: async (ctx, args) => ({ text: args.text, account: (ctx.auth as { account: unknown }).account ?? null }),
});

/** Tool and functions target. Public, because a one-shot Query with a handler must be (codegen). */
export const boardCount = query({
  public: true,
  args: schema.object({}),
  result: schema.object({ n: schema.integer() }),
  run: async ({ db }) => {
    const [row] = await db.query<{ n: number }>("SELECT count(*) AS n FROM board");
    return { n: row?.n ?? 0 };
  },
});

/** Internal Reducer: reachable only through a tool binding of kind `internalReducer`. */
export const internalAdd = internalReducer({
  args: schema.object({ id: schema.string() }),
  result: schema.object({ id: schema.id("board"), title: schema.string(), position: schema.integer(), bumps: schema.integer() }),
  run: async ({ db }, args) => db.insert("board", { id: args.id, title: "internal", position: 90, bumps: 0 }),
});

/** Internal one-shot Query (a structured plan, as codegen requires): the `query` tool target. */
export const boardRows = internalQuery({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.id("board"), title: schema.string() })),
  liveQueryPlan: { table: "board", key: "id", columns: ["id", "title"] },
});
