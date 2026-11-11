// K20 platform probe. POST /probe/<case> with header `x-k20-key` (a Worker
// secret) runs one case through the real service and answers JSON. Development
// account only; deleted after each measurement (dev/platform.mjs).
import { sandboxConfigFromEnv, SandboxService, DynamicWorkerBackend, type SandboxEnv, type SandboxSession } from "../src/index";
import { WORKBOOK_XLS, WORKBOOK_XLSX } from "../test/fixtures/workbooks";

export { SandboxAnalysis, SandboxRegistry, SandboxWorkspace } from "../src/index";

interface Env extends SandboxEnv { K20_KEY: string; GONVEX_SANDBOX_ENABLED: string }

const bytes = (base64: string) => Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));

function csv(rows: number): string {
  let out = "id,region,amount,closed,day\n";
  const regions = ["north", "south", "east", "west"];
  for (let i = 0; i < rows; i++) out += `${i},${regions[i % 4]},${(i * 7) % 1000 / 10},${i % 3 === 0},2024-0${(i % 9) + 1}-1${i % 9}\n`;
  return out;
}

const FILES: Record<string, () => Uint8Array> = {
  "small.csv": () => new TextEncoder().encode(csv(100)),
  "large.csv": () => new TextEncoder().encode(csv(200_000)),
  "book.xlsx": () => bytes(WORKBOOK_XLSX),
  "book.xls": () => bytes(WORKBOOK_XLS),
};

async function timed<T>(work: () => Promise<T>): Promise<{ ms: number; value?: T; error?: string }> {
  const started = Date.now();
  try {
    const value = await work();
    return { ms: Date.now() - started, value };
  } catch (error) {
    return { ms: Date.now() - started, error: (error as Error).message };
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!env.K20_KEY || request.headers.get("x-k20-key") !== env.K20_KEY) return new Response("forbidden", { status: 403 });
    const name = new URL(request.url).pathname.replace(/^\/probe\//, "");
    const files = {
      async readFile(_session: SandboxSession, fileId: string, maxBytes: number) {
        const make = FILES[fileId];
        if (!make) throw new Error("file not found");
        const data = make();
        if (data.byteLength > maxBytes) throw new Error("stored file exceeds the sandbox import limit");
        return { bytes: data };
      },
    };
    const service = new SandboxService({ backend: new DynamicWorkerBackend(env), config: sandboxConfigFromEnv(env as unknown as Record<string, unknown>), files });
    const who: SandboxSession = { projectId: "k20-probe", tenantId: `tenant-${crypto.randomUUID()}`, account: { id: "account-1" } };
    const call = (operation: string, payload: unknown, session = who) => service.call(operation, payload, session, true);
    const create = async () => ((await call("create", { ttlMs: 10 * 60_000 })) as { sandboxId: string }).sandboxId;
    const run = async (code: string, timeoutMs = 5_000) => {
      const sandboxId = await create();
      const result = await timed(() => call("run", { sandboxId, code, timeoutMs }));
      await call("destroy", { sandboxId }).catch(() => undefined);
      return result;
    };
    const cases: Record<string, () => Promise<unknown>> = {
      // Limits the platform enforces and local workerd does not.
      cpuSpin: () => run("for (;;) {}", 2_000),
      microtaskSpin: () => run("for (;;) { await null; }", 2_000),
      hostCallSpin: () => run("for (;;) { await files.list(); }", 2_000),
      memory: () => run("const kept = []; for (let i = 0; i < 400; i++) kept.push(new Uint8Array(1 << 20).fill(1)); return kept.length;", 20_000),
      memory64: () => run("const kept = []; for (let i = 0; i < 60; i++) kept.push(new Uint8Array(1 << 20).fill(1)); return kept.length;", 20_000),
      deadline: () => run("await new Promise((r) => setTimeout(r, 10_000)); return 1;", 1_000),
      network: () => run(`
        const attempt = async (f) => { try { await f(); return "allowed"; } catch (e) { return String(e.message); } };
        return { fetch: await attempt(() => fetch("https://example.com/")), connect: await attempt(async () => (await import("cloudflare:sockets")).connect("1.1.1.1:53").opened), eval: await attempt(() => eval("1")) };
      `),
      // The object that ran the spin still answers.
      afterSpin: async () => {
        const sandboxId = await create();
        const spin = await timed(() => call("run", { sandboxId, code: "for (;;) {}", timeoutMs: 1_000 }));
        const next = await timed(() => call("run", { sandboxId, code: "return 42;" }));
        await call("destroy", { sandboxId });
        return { spin, next };
      },
      isolation: async () => {
        const sandboxId = await create();
        await call("writeText", { sandboxId, path: "secret.txt", content: "owner" });
        const stranger = { ...who, account: { id: "account-2" } };
        const other = { ...who, tenantId: `tenant-${crypto.randomUUID()}` };
        const result = {
          strangerRead: await timed(() => call("readText", { sandboxId, path: "secret.txt" }, stranger)),
          otherTenantRun: await timed(() => call("run", { sandboxId, code: "return await files.readText('secret.txt')" }, other)),
          ownerRead: await timed(() => call("readText", { sandboxId, path: "secret.txt" })),
        };
        await call("destroy", { sandboxId });
        return result;
      },
      // End to end on the platform, with timings.
      analysis: async () => {
        const out: Record<string, unknown> = {};
        const sandboxId = ((await timed(() => call("create", {}))).value as { sandboxId: string }).sandboxId;
        out.writeText = await timed(() => call("writeText", { sandboxId, path: "tasks.json", content: JSON.stringify(Array.from({ length: 1000 }, (_, i) => ({ id: `t${i}`, statusId: ["open", "done", "late"][i % 3], hours: i % 7 }))) }));
        out.runCold = await timed(() => call("run", { sandboxId, code: 'const tasks = JSON.parse(await files.readText("tasks.json")); await duckdb.register("tasks", tasks); return await duckdb.query("SELECT statusId, count(*) AS total, sum(hours) AS hours FROM tasks GROUP BY statusId ORDER BY statusId");' }));
        out.runSmall = await timed(() => call("run", { sandboxId, code: "return 1 + 1;" }));
        out.importCsv = await timed(() => call("importFile", { sandboxId, fileId: "small.csv", filename: "small.csv" }));
        out.importXlsx = await timed(() => call("importFile", { sandboxId, fileId: "book.xlsx", filename: "book.xlsx" }));
        out.importXls = await timed(() => call("importFile", { sandboxId, fileId: "book.xls", filename: "book.xls" }));
        out.query = await timed(() => call("run", { sandboxId, code: 'return await duckdb.query("SELECT region, count(*) AS n, avg(amount) AS mean FROM import_1.data GROUP BY region ORDER BY region");' }));
        out.summarize = await timed(() => call("run", { sandboxId, code: 'return (await duckdb.query(\'SUMMARIZE SELECT * FROM "import_1"."data"\')).rowCount;' }));
        out.destroy = await timed(() => call("destroy", { sandboxId }));
        return out;
      },
      largeImport: async () => {
        const sandboxId = await create();
        const size = FILES["large.csv"]!().byteLength;
        const imported = await timed(() => call("importFile", { sandboxId, fileId: "large.csv", filename: "large.csv" }));
        const query = await timed(() => call("run", { sandboxId, code: 'return await duckdb.query("SELECT region, count(*) AS n, sum(amount) AS total FROM import_1.data GROUP BY region ORDER BY region");', timeoutMs: 30_000 }));
        await call("destroy", { sandboxId });
        return { bytes: size, imported, query };
      },
    };
    const work = cases[name];
    if (!work) return Response.json({ cases: Object.keys(cases) }, { status: 404 });
    return Response.json({ case: name, at: new Date().toISOString(), result: await work() });
  },
};
