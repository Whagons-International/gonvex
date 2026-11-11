// Test Actions for the S4 scenarios (K12): long agent loops over an event
// stream, waits, failures and a queuing Reducer. Plain module code: the
// vitest prelude gives it the module globals, as the Worker build does.
const UP = "http://127.0.0.1:47812";

/** An agent loop: event-stream "turns" with pauses, for `seconds` in total (S4 scenario 1). */
export async function long_agent(ctx, args) {
  const started = Date.now();
  const turns = [];
  while (Date.now() - started < args.seconds * 1000) {
    const response = await ctx.fetch(`${UP}/sse?events=${args.events}&delayMs=${args.delayMs}`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let text = "";
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      text += decoder.decode(part.value, { stream: true });
    }
    const numbers = text.split("\n\n").filter(Boolean).map((frame) => JSON.parse(frame.slice("data: ".length)).n);
    turns.push({ events: numbers.length, inOrder: numbers.every((n, i) => n === i + 1) });
    if (Date.now() - started < args.seconds * 1000) await new Promise((resolve) => setTimeout(resolve, args.pauseMs));
  }
  return { turns: turns.length, events: turns.reduce((sum, turn) => sum + turn.events, 0), inOrder: turns.every((turn) => turn.inOrder), wallMs: Date.now() - started };
}

/** Waits, then answers (a slow upstream, or a stalled runner). */
export async function long_wait(ctx, args) {
  await new Promise((resolve) => setTimeout(resolve, args.ms));
  return { waited: args.ms, tag: args.tag ?? null, channel: ctx.invocation.channel, commandId: ctx.invocation.commandId, parent: ctx.invocation.parentCommandId };
}

/** Always fails (S4 scenario 11: dead after MAX_ATTEMPTS). */
export async function long_fail() {
  throw new Error("long.fail always fails");
}

/** A Reducer that queues an Action in its transaction (K04's actionEnqueue; K07 calls afterCommit). */
export async function queue_enqueue(ctx, args) {
  if (args.row) await ctx.db.query("INSERT INTO board (id, title, position, bumps) VALUES ($1, $2, 0, 0)", [args.row, "queued"]);
  const id = await ctx.actions.enqueue(args.path, args.args ?? null);
  return { id };
}

/** A standard Action with a declared secret. */
export async function long_secret(ctx) {
  return { keys: Object.keys(ctx.secrets), value: ctx.secrets.K12_SECRET };
}

/** Fetches with the deadline almost spent (budgets). */
export async function long_slowFetch(ctx, args) {
  const response = await ctx.fetch(`${UP}/sse?events=${args.events}&delayMs=${args.delayMs}`);
  return { status: response.status, text: await response.text() };
}

/** A public Action that waits (the 60 s public deadline, scaled). */
export async function long_publicWait(_ctx, args) {
  await new Promise((resolve) => setTimeout(resolve, args.ms));
  return { waited: args.ms };
}

/** Seventeen concurrent requests: the 17th is refused. */
export async function long_concurrent(ctx) {
  const calls = Array.from({ length: 17 }, () => ctx.fetch(`${UP}/sse?events=2&delayMs=200`).then(() => "ok", (error) => `${error.name}: ${error.message}`));
  return (await Promise.all(calls)).filter((outcome) => outcome !== "ok");
}

/** Storage, scheduler and sandbox ops through the ports. */
export async function long_ports(ctx) {
  const out = {};
  out.metadata = await ctx.storage.getMetadata("file-1");
  out.store = await ctx.storage.store("aGVsbG8=", { contentType: "text/plain" });
  out.job = await ctx.scheduler.runAfter(1000, "long.wait", { ms: 1 });
  out.wake1 = await ctx.scheduler.wakeAt(4102444800000, "long.wait", { ms: 2 });
  out.wake2 = await ctx.scheduler.wakeAt(4102444700000, "long.wait", { ms: 2 });
  try { await ctx.storage.getMetadata("private-file"); } catch (error) { out.denied = `${error.name}: ${error.message}`; }
  return out;
}

/** The sandbox surface with no sandbox runtime (K20). */
export async function long_sandbox(ctx) {
  try { await ctx.sandbox.create({}); return "no error"; } catch (error) { return `${error.name}: ${error.message}`; }
}

/** One request whose response starts only after `ms` (S4 scenario 4). */
export async function long_slowRequest(ctx, args) {
  const started = Date.now();
  const response = await ctx.fetch(`${UP}/slow?ms=${args.ms}`);
  return { status: response.status, body: await response.text(), waitedAtLeast: Date.now() - started >= args.ms };
}

/** Writes through a tool Reducer, then waits: an Action with an effect (S4 exactly-once). */
export async function long_effect(ctx, args) {
  await new Promise((resolve) => setTimeout(resolve, args.ms));
  return { attemptCommand: ctx.invocation.commandId, tag: args.tag };
}
