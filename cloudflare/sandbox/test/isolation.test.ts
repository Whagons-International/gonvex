// Isolation: no network or other capability inside a run, and no way to reach
// another tenant's, account's or sandbox's files and tables.
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { ctxSandbox, failure, MemoryFiles, service, session } from "./support";
import { runSandboxCode } from "./whagons";

const NOT_FOUND = failure("sandbox was not found");

async function prepared() {
  const owner = session();
  const files = new MemoryFiles();
  files.add("shared", "a,b\n1,2\n", { tenantId: owner.tenantId });
  const sandboxService = service({}, files);
  const sandbox = ctxSandbox(sandboxService, owner);
  const { sandboxId } = await sandbox.create();
  await sandbox.writeText(sandboxId, "secret.txt", "owner only");
  const run = await sandbox.run(sandboxId, { code: 'await duckdb.register("mine", [{ v: 1 }]); return 1;' });
  expect(run.status).toBe("succeeded");
  return { owner, files, sandboxService, sandbox, sandboxId, executionId: run.executionId as string };
}

describe("tenant and account isolation", () => {
  it("another account, tenant or project reaches no part of a sandbox, whatever it knows", async () => {
    const { owner, files, sandboxService, sandbox, sandboxId, executionId } = await prepared();
    const strangers = [
      session({ tenantId: owner.tenantId, accountId: "account-2" }),
      session({ accountId: owner.account.id }),
      session({ projectId: "other-project", tenantId: owner.tenantId, accountId: owner.account.id }),
    ];
    for (const stranger of strangers) {
      files.add("shared", "a,b\n1,2\n", { tenantId: stranger.tenantId });
      const theirs = ctxSandbox(sandboxService, stranger);
      await expect(theirs.readText(sandboxId, "secret.txt")).rejects.toThrow(NOT_FOUND);
      await expect(theirs.readFile(sandboxId, "secret.txt")).rejects.toThrow(NOT_FOUND);
      await expect(theirs.writeText(sandboxId, "secret.txt", "overwritten")).rejects.toThrow(NOT_FOUND);
      await expect(theirs.writeFile(sandboxId, "x", "eA==")).rejects.toThrow(NOT_FOUND);
      await expect(theirs.run(sandboxId, { code: "return await duckdb.query('select * from mine')" })).rejects.toThrow(NOT_FOUND);
      await expect(theirs.status(sandboxId, executionId)).rejects.toThrow(NOT_FOUND);
      await expect(theirs.cancel(sandboxId, executionId)).rejects.toThrow(NOT_FOUND);
      await expect(theirs.importFile(sandboxId, { fileId: "shared", filename: "a.csv" })).rejects.toThrow(NOT_FOUND);
      await expect(theirs.destroy(sandboxId)).rejects.toThrow(NOT_FOUND);
    }
    expect(await sandbox.readText(sandboxId, "secret.txt")).toBe("owner only");
    expect((await sandbox.status(sandboxId, executionId)).status).toBe("succeeded");
  });

  it("an id that is not a sandbox id names nothing", async () => {
    const sandbox = ctxSandbox(service(), session());
    for (const id of ["sbx_x", "../sbx", "sbx_00000000-0000-0000-0000-000000000000/../x"]) {
      await expect(sandbox.status(id, "run_1")).rejects.toThrow(NOT_FOUND);
    }
  });

  it("each sandbox has its own files and tables, even for one account", async () => {
    const who = session();
    const sandbox = ctxSandbox(service(), who);
    const a = (await sandbox.create()).sandboxId;
    const b = (await sandbox.create()).sandboxId;
    await sandbox.writeText(a, "only-a.txt", "a");
    expect((await sandbox.run(a, { code: 'await duckdb.register("t", [{ v: 1 }]); return (await files.list());' })).result).toEqual(["only-a.txt"]);
    const other = await sandbox.run(b, { code: 'const listed = await files.list(); try { await duckdb.query("select * from t"); return "visible"; } catch (e) { return { listed, error: e.message }; }' });
    expect(other.result).toEqual({ listed: [], error: "host call failed: no such table: t: SQLITE_ERROR" });
  });

  it("a private file of another account cannot be imported (IB1)", async () => {
    const who = session();
    const files = new MemoryFiles();
    files.add("private", "a\n1\n", { tenantId: who.tenantId, ownerId: "account-2", visibility: "private" });
    const sandbox = ctxSandbox(service({}, files), who);
    const { sandboxId } = await sandbox.create();
    await expect(sandbox.importFile(sandboxId, { fileId: "private", filename: "a.csv" })).rejects.toThrow(failure("file not found"));
  });

  it("paths stay inside the workspace", async () => {
    const sandbox = ctxSandbox(service(), session());
    const { sandboxId } = await sandbox.create();
    for (const path of ["../x", "/etc/passwd", "./a", "a/../../b", "a/.."]) {
      await expect(sandbox.writeText(sandboxId, path, "x")).rejects.toThrow(failure("sandbox path must be relative and cannot escape the workspace"));
    }
    await sandbox.writeText(sandboxId, "dir//./nested.txt", "ok");
    expect(await sandbox.readText(sandboxId, "dir/nested.txt")).toBe("ok");
    await expect(sandbox.readText(sandboxId, "dir")).rejects.toThrow(failure("Is a directory (os error 21)"));
    await expect(sandbox.writeText(sandboxId, "dir/nested.txt/deeper", "x")).rejects.toThrow(failure("Not a directory (os error 20)"));
    await expect(sandbox.readText(sandboxId, "missing.txt")).rejects.toThrow(failure("No such file or directory (os error 2)"));
    const inside = await sandbox.run(sandboxId, { code: 'try { await files.readText("../escape"); } catch (e) { return e.message; }' });
    expect(inside.result).toBe("host call failed: sandbox path must be relative and cannot escape the workspace");
  });
});

describe("what a run can reach", () => {
  it("no network, no code generation, no bindings: only files and the analysis tables", async () => {
    const ctx = { sandbox: ctxSandbox(service(), session()) };
    const { sandboxId } = await ctx.sandbox.create();
    const probe = await runSandboxCode(ctx, sandboxId, `
      const attempt = async (f: () => unknown) => { try { await f(); return "allowed"; } catch (e) { return String((e as Error).message); } };
      return {
        fetch: await attempt(() => fetch("https://example.com/")),
        connect: await attempt(async () => (await import("cloudflare:sockets")).connect("example.com:80").opened),
        eval: await attempt(() => eval("1 + 1")),
        newFunction: await attempt(() => new Function("return 1")()),
        caches: await attempt(() => caches.default.match("https://example.com/")),
        ctxKeys: Object.keys(ctx).sort(),
        sandboxCreate: await attempt(() => ctx.sandbox.create()),
      };
    `);
    expect(probe.fetch).toMatch(/not permitted to access the internet/);
    expect(probe.connect).toMatch(/not permitted to access the internet/);
    expect(probe.eval).toMatch(/Code generation from strings disallowed/);
    expect(probe.newFunction).toMatch(/Code generation from strings disallowed/);
    expect(probe.caches).not.toBe("allowed");
    expect(probe.ctxKeys).toEqual(["auth", "function", "kind", "member", "now", "sandbox", "tenant"]);
    expect(probe.sandboxCreate).toBe('host call failed: unsupported sandbox worker operation "create"');
  });

  it("module SQL cannot reach workspace files, execution records or other databases", async () => {
    const who = session();
    const files = new MemoryFiles();
    files.add("upload", "a\n1\n", { tenantId: who.tenantId });
    const ctx = { sandbox: ctxSandbox(service({}, files), who) };
    const { sandboxId } = await ctx.sandbox.create();
    await ctx.sandbox.writeText(sandboxId, "secret.txt", "s");
    // An import writes the analysis object's own key-value entry, so its _cf_KV table exists.
    await ctx.sandbox.importFile(sandboxId, { fileId: "upload", filename: "a.csv" });
    const result = await runSandboxCode(ctx, sandboxId, `
      const attempt = async (sql: string) => { try { return (await duckdb.query(sql)).rows; } catch (e) { return (e as Error).message; } };
      return {
        kv: await attempt("SELECT * FROM _cf_KV"),
        attach: await attempt("ATTACH DATABASE 'other.db' AS other"),
        pragma: await attempt("PRAGMA database_list"),
        schema: await attempt("SELECT name FROM sqlite_master"),
        readFile: await attempt("SELECT readfile('secret.txt')"),
      };
    `);
    expect(result.kv).toMatch(/access to _cf_KV\.\w+ is prohibited/);
    expect(result.attach).toMatch(/not authorized/);
    expect(result.pragma).toMatch(/not authorized/);
    // The platform's own table is listed but unreadable; nothing of the workspace is here.
    expect(result.schema.map((row: { name: string }) => row.name).sort()).toEqual(["_cf_KV", "import_1.data"]);
    expect(result.readFile).toMatch(/no such function: readfile/);
  });

  it("a run that outlives its deadline can no longer touch the workspace", async () => {
    const sandbox = ctxSandbox(service(), session());
    const { sandboxId } = await sandbox.create();
    const run = await sandbox.run(sandboxId, { code: 'await new Promise((r) => setTimeout(r, 600)); await files.writeText("late.txt", "x"); return 1;', timeoutMs: 200 });
    expect(run.status).toBe("failed");
    expect(run.error).toBe("module execution exceeded its budget: module exceeded its 200 ms execution deadline");
    await new Promise((resolve) => setTimeout(resolve, 900));
    await expect(sandbox.readText(sandboxId, "late.txt")).rejects.toThrow(failure("No such file or directory (os error 2)"));
  });

  it("the analysis object exposes no state but module tables", async () => {
    const object = env.SANDBOX_ANALYSIS.getByName(`probe-${crypto.randomUUID()}`);
    const answer = await object.call({ operation: "query", payload: { statement: "SELECT name FROM sqlite_master" }, maxRows: 10, budgetBytes: 1 << 20 });
    expect(answer.ok && JSON.parse(answer.value.json).rows).toEqual([]);
  });
});
