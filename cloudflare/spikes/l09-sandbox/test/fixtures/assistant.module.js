// Agent Actions that use ctx.sandbox the way Whagons' assistant does, run by
// K12's ActionRuntime through K04's module host (test/action.test.ts).

export async function analyze(ctx, args) {
  const sandbox = await ctx.sandbox.create({ ttlMs: 10 * 60_000 });
  await ctx.sandbox.writeText(sandbox.sandboxId, "tasks.json", JSON.stringify(args.tasks));
  const imported = args.fileId ? await ctx.sandbox.importFile(sandbox.sandboxId, { fileId: args.fileId, filename: args.filename }) : null;
  const execution = await ctx.sandbox.run(sandbox.sandboxId, {
    code: `
      const tasks: Array<{ statusId: string }> = JSON.parse(await files.readText("tasks.json"));
      await duckdb.register("tasks", tasks);
      const byStatus = await duckdb.query("SELECT statusId, count(*) AS total FROM tasks GROUP BY statusId ORDER BY total DESC");
      const upload = ${JSON.stringify(Boolean(args.fileId))} ? await duckdb.query('SELECT count(*) AS n FROM "import_1"."data"') : null;
      return { byStatus: byStatus.rows, upload: upload?.rows ?? null };
    `,
    timeoutMs: 30_000,
  });
  const state = await ctx.sandbox.status(sandbox.sandboxId, execution.executionId);
  await ctx.sandbox.destroy(sandbox.sandboxId);
  return { status: state.status, result: state.result, imported };
}

export async function failing(ctx, args) {
  try {
    await ctx.sandbox[args.operation](...args.args);
    return "no error";
  } catch (error) {
    return `${error.name}: ${error.message}`;
  }
}
