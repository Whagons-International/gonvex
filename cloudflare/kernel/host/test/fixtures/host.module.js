// A module bundle for the host tests. The vitest config gives it the same
// prelude the kernel build gives the app bundle, so `Date`, `Math`, `crypto`,
// `fetch` and `globalThis` here are the module's, not workerd's.

const notesList = (ctx) => ctx.db.query("SELECT _id, body, done, meta FROM notes ORDER BY _id");

export const notes_list = { handler: notesList };

export const notes_clock = {
  options: {
    run: async (ctx) => {
      const before = Date.now();
      await ctx.db.query("SELECT 1 AS one");
      return {
        now: Date.now(), ctxNow: ctx.now, pinnedAcrossAwait: Date.now() === before, constructed: +new Date(),
        typeofFetch: typeof fetch, typeofGlobalFetch: typeof globalThis.fetch, typeofSelfFetch: typeof self.fetch,
        globalDatePinned: globalThis.Date.now() === ctx.now,
        hasInsert: typeof ctx.db.insert, hasPermissions: typeof ctx.permissions, hasActions: typeof ctx.actions,
        hasScheduler: typeof ctx.scheduler, hasStorage: typeof ctx.storage, keys: Object.keys(ctx).sort(),
      };
    },
  },
};

export const notes_random = (ctx) => {
  let entropy;
  try { crypto.randomUUID(); entropy = "allowed"; } catch (error) { entropy = error.name; }
  let random;
  try { random = [Math.random(), Math.random()]; } catch (error) { random = error.message; }
  return { random, entropy, intentEntropy: ctx.intentEntropy ?? null };
};

export const notes_writeInQuery = (ctx) => ctx.db.query("DELETE FROM notes");
export const notes_cteDelete = (ctx) => ctx.db.query("WITH doomed AS (SELECT _id FROM notes) DELETE FROM notes WHERE _id IN (SELECT _id FROM doomed)");

export const notes_add = (ctx, args) => ctx.db.insert("notes", { _id: args.id, body: args.body, done: args.done ?? false, meta: args.meta ?? null });
export const notes_update = (ctx, args) => ctx.db.update("notes", args.id, args.patch);
export const notes_remove = (ctx, args) => ctx.db.delete("notes", args.id);
export const notes_removeMany = (ctx, args) => ctx.db.deleteMany("notes", args.ids);
export const notes_counter = (ctx, args) => ctx.db.insert("counters", { value: args.value });
export const notes_composite = (ctx) => ctx.db.update("pairs", "x", { b: "y" });

export const notes_enqueue = async (ctx) => ({
  action: await ctx.actions.enqueue("mail.send", { to: "a@example.com" }),
  later: await ctx.scheduler.runAfter(1000, "notes.add", { id: "later", body: "later" }),
  wake: await ctx.scheduler.wakeAt(1_800_000_000_000, "notes.add", { id: "wake", body: "wake" }),
});
export const notes_scheduleUnknown = (ctx) => ctx.scheduler.runAfter(1, "nope.missing", {});

export const notes_caughtSqlError = async (ctx, args) => {
  await ctx.db.insert("notes", { _id: args.id, body: "first" });
  try { await ctx.db.insert("notes", { _id: args.id, body: "duplicate" }); } catch { /* swallowed */ }
  return "returned normally";
};

export const notes_abortedThenQuery = async (ctx, args) => {
  try { await ctx.db.insert("notes", { _id: args.id, body: "duplicate" }); } catch { /* swallowed */ }
  try { await ctx.db.query("SELECT 1"); } catch (error) { throw new Error(`second statement: ${error.message}`); }
  return "query allowed";
};

export const notes_savepointRecover = async (ctx, args) => {
  await ctx.db.query("SAVEPOINT attempt");
  await ctx.db.insert("notes", { _id: `${args.id}-a`, body: "inside" });
  try { await ctx.db.insert("notes", { _id: args.existing, body: "duplicate" }); } catch { /* recovered below */ }
  await ctx.db.query("ROLLBACK TO attempt");
  await ctx.db.query("RELEASE attempt");
  await ctx.db.insert("notes", { _id: `${args.id}-b`, body: "after" });
  return "recovered";
};

export const notes_throwAfterWrite = async (ctx, args) => {
  await ctx.db.insert("notes", { _id: args.id, body: "doomed" });
  throw new Error("boom after write");
};

export const notes_orphanChild = (ctx, args) => ctx.db.insert("children", { _id: args.id, parent: "no-such-note" });

export const notes_ddl = async (ctx, args) => {
  await ctx.db.query(`CREATE TABLE ${args.table} (_id TEXT PRIMARY KEY, label TEXT)`);
  const row = await ctx.db.insert(args.table, { _id: "x", label: "made" });
  if (args.fail) throw new Error("roll the DDL back");
  return row;
};
export const notes_insertInto = (ctx, args) => ctx.db.insert(args.table, args.row);

export const notes_recordAdd = async (ctx, args) => ({
  row: await ctx.db.insert("items", { label: args.label }),
  columns: await ctx.db.recordColumns("items"),
});

export const notes_big = (_ctx, args) => "x".repeat(args.size);
export const notes_badResult = () => 42;
export const notes_slow = async (ctx, args) => { await new Promise((resolve) => setTimeout(resolve, args.ms)); return ctx.db.query("SELECT 1 AS one"); };
export const notes_spin = async (ctx, args) => {
  const started = performance === undefined ? null : 0; // `performance` is absent in module code
  const end = globalThis.Date === Date ? args.loops : 0;
  let x = 0;
  for (let i = 0; i < end; i++) x = (x + i) % 7;
  await ctx.db.query("SELECT 1 AS one");
  return { x, started };
};
export const notes_permission = (ctx, args) => ctx.permissions.has(args.permission, args.scope);
export const notes_metadata = (ctx) => ctx.storage.getMetadata("file-1");
export const notes_queryPermission = (ctx) => ctx.permissions.has("edit");

export const net_swallowDenied = async (ctx) => {
  try { await ctx.fetch("https://blocked.invalid/x"); } catch { /* swallowed */ }
  return "swallowed";
};

export const notes_manyCalls = async (ctx, args) => {
  for (let i = 0; i < args.calls; i++) {
    try { await ctx.db.query("SELECT 1 AS one"); } catch { /* a runaway that swallows errors */ }
  }
  return "done";
};
