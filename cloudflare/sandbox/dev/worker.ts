// K20 platform probe. POST /probe/<case> with header `x-k20-key` (a Worker
// secret) runs one case through the real service, Container backend and
// Container, and answers JSON. Development account only; deleted after each
// measurement (dev/platform.mjs).
import { ContainerBackend, containerName, sandboxConfigFromEnv, SandboxService, type SandboxEnv, type SandboxFileSource, type SandboxSession } from "../src/index";
import { WORKBOOK_XLS, WORKBOOK_XLSX } from "../test/fixtures/workbooks";

export { SandboxContainer, SandboxRegistry } from "../src/index";

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

/** Stands in for K13 here: the probe has no tenant object. The owner checks are tested in test/contract.test.ts. */
const files: SandboxFileSource = {
  async readFile(_session, fileId, maxBytes) {
    const make = FILES[fileId];
    if (!make) throw new Error("file not found");
    const data = make();
    if (data.byteLength > maxBytes) throw new Error("stored file exceeds the sandbox import limit");
    return { bytes: data };
  },
};

async function measure<T>(work: () => Promise<T>): Promise<{ ms: number; value?: T; error?: string }> {
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
    const url = new URL(request.url);
    const name = url.pathname.replace(/^\/probe\//, "");
    const tenantId = url.searchParams.get("tenant") ?? `tenant-${crypto.randomUUID()}`;
    const service = new SandboxService({ backend: new ContainerBackend(env), config: sandboxConfigFromEnv(env as unknown as Record<string, unknown>), files });
    const who: SandboxSession = { projectId: "k20-probe", tenantId, account: { id: "account-1" }, member: { id: "member-1" } };
    const call = (operation: string, payload: unknown, session = who) => service.call(operation, payload, session, true);
    const create = async () => ((await call("create", { ttlMs: 10 * 60_000 })) as { sandboxId: string }).sandboxId;
    const stats = async () => {
      const answer = (await env.SANDBOX_CONTAINER.getByName(containerName({ project: who.projectId, tenant: tenantId })).stats()) as { ok: boolean; value?: unknown; error?: string };
      return answer.ok ? answer.value : answer.error;
    };
    const run = async (code: string, timeoutMs = 5_000) => {
      const sandboxId = await create();
      const result = await measure(() => call("run", { sandboxId, code, timeoutMs }));
      await call("destroy", { sandboxId }).catch(() => undefined);
      return result;
    };
    const cases: Record<string, () => Promise<unknown>> = {
      // A new tenant: the Container starts with the first create.
      coldStart: async () => {
        const created = await measure(create);
        const firstRun = await measure(async () => call("run", { sandboxId: created.value, code: "return 1 + 1;" }));
        const secondRun = await measure(async () => call("run", { sandboxId: created.value, code: "return 2 + 2;" }));
        const warmCreate = await measure(create);
        return { created, firstRun, secondRun, warmCreate, stats: await stats() };
      },
      network: () => run(`
        const attempt = async (f) => { try { await f(); return "allowed"; } catch (e) { return String(e.message); } };
        return {
          fetch: typeof globalThis.fetch,
          webSocket: typeof globalThis.WebSocket,
          httpfs: await attempt(() => duckdb.query("SELECT * FROM read_csv_auto('https://example.com/a.csv')")),
          readCsv: await attempt(() => duckdb.query("SELECT * FROM read_csv_auto('/etc/passwd')")),
          escape: await attempt(() => files.readText("../../etc/passwd")),
        };
      `),
      isolation: async () => {
        const sandboxId = await create();
        await call("writeText", { sandboxId, path: "secret.txt", content: "owner" });
        const stranger = { ...who, account: { id: "account-2" }, member: { id: "member-2" } };
        const otherTenant = { ...who, tenantId: `tenant-${crypto.randomUUID()}` };
        const result = {
          strangerRead: await measure(() => call("readText", { sandboxId, path: "secret.txt" }, stranger)),
          otherTenantRun: await measure(() => call("run", { sandboxId, code: "return await files.readText('secret.txt')" }, otherTenant)),
          ownerRead: await measure(() => call("readText", { sandboxId, path: "secret.txt" })),
        };
        await call("destroy", { sandboxId });
        return result;
      },
      deadline: () => run("await new Promise((r) => setTimeout(r, 10_000)); return 1;", 1_000),
      cpuSpin: () => run("for (;;) {}", 2_000),
      heap: () => run("const kept = []; for (;;) kept.push(new Array(1e6).fill(1));", 20_000),
      analysis: async () => {
        const out: Record<string, unknown> = {};
        const sandboxId = ((await measure(() => call("create", {}))).value as { sandboxId: string }).sandboxId;
        out.writeText = await measure(() => call("writeText", { sandboxId, path: "tasks.json", content: JSON.stringify(Array.from({ length: 1000 }, (_, i) => ({ id: `t${i}`, statusId: ["open", "done", "late"][i % 3], hours: i % 7 }))) }));
        out.runDuckdb = await measure(() => call("run", { sandboxId, code: 'const tasks = JSON.parse(await files.readText("tasks.json")); await duckdb.register("tasks", tasks); return await duckdb.query("SELECT statusId, count(*) AS total, sum(hours) AS hours FROM tasks GROUP BY statusId ORDER BY statusId");' }));
        out.runSmall = await measure(() => call("run", { sandboxId, code: "return 1 + 1;" }));
        out.importCsv = await measure(() => call("importFile", { sandboxId, fileId: "small.csv", filename: "small.csv" }));
        out.importXlsx = await measure(() => call("importFile", { sandboxId, fileId: "book.xlsx", filename: "book.xlsx" }));
        out.importXls = await measure(() => call("importFile", { sandboxId, fileId: "book.xls", filename: "book.xls" }));
        out.query = await measure(() => call("run", { sandboxId, code: 'return await duckdb.query("SELECT region, count(*) AS n, avg(amount) AS mean FROM import_1.data GROUP BY region ORDER BY region");' }));
        out.summarize = await measure(() => call("run", { sandboxId, code: 'return (await duckdb.query(\'SUMMARIZE SELECT * FROM "import_1"."data"\')).rowCount;' }));
        out.destroy = await measure(() => call("destroy", { sandboxId }));
        out.stats = await stats();
        return out;
      },
      largeImport: async () => {
        const sandboxId = await create();
        const size = FILES["large.csv"]!().byteLength;
        const imported = await measure(() => call("importFile", { sandboxId, fileId: "large.csv", filename: "large.csv" }));
        const query = await measure(() => call("run", { sandboxId, code: 'return await duckdb.query("SELECT region, count(*) AS n, sum(amount) AS total FROM import_1.data GROUP BY region ORDER BY region");', timeoutMs: 30_000 }));
        await call("destroy", { sandboxId });
        return { bytes: size, imported, query, stats: await stats() };
      },
      stats: async () => stats(),
    };
    const work = cases[name];
    if (!work) return Response.json({ cases: Object.keys(cases) }, { status: 404 });
    return Response.json({ case: name, tenantId, at: new Date().toISOString(), result: await work() });
  },
};
