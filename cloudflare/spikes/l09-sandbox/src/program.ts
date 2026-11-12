// The code a run executes: Rust's TypeScript wrapper (sandbox-worker
// `transpile`, main.rs:212-264), type-stripped by Sucrase instead of deno_ast,
// plus the entrypoint module of the Dynamic Worker that runs it.
import { transform } from "sucrase";

/** `transpile(code, duckdb_enabled)`: the same wrapper, bindings and limits (500 lines, 16,384 characters). */
export function wrapProgram(code: string, duckdb: boolean): string {
  const duckdbBinding = duckdb
    ? `const duckdb = Object.freeze({
    query: (statement: string, parameters: unknown[] = []) => ctx.sandbox.__worker.query(statement, parameters),
    register: (name: string, rows: Record<string, unknown>[]) => ctx.sandbox.__worker.register(name, rows),
  });`
    : "";
  return `
export async function run(ctx: any): Promise<any> {
  const __lines: Array<{level: string, message: string}> = [];
  const __format = (value: unknown): string => { try { return typeof value === "string" ? value : JSON.stringify(value); } catch { return String(value); } };
  const __log = (level: string, values: unknown[]) => { if (__lines.length < 500) __lines.push({level, message: values.map(__format).join(" ").slice(0, 16384)}); };
  const console = Object.freeze({
    log: (...values: unknown[]) => __log("log", values),
    info: (...values: unknown[]) => __log("log", values),
    warn: (...values: unknown[]) => __log("warn", values),
    error: (...values: unknown[]) => __log("error", values),
  });
  const files = Object.freeze({
    readText: (path: string) => ctx.sandbox.__worker.readText(path),
    writeText: (path: string, content: string) => ctx.sandbox.__worker.writeText(path, content),
    list: () => ctx.sandbox.__worker.listFiles(),
  });
  ${duckdbBinding}
  const result = await (async (): Promise<unknown> => {
${code}
  })();
  return {result, logs: __lines.slice(0, 500)};
}
`;
}

/** Type-strips the wrapped program. Rust's text for a syntax error is "TypeScript parse failed: …". */
export function compileProgram(code: string, duckdb: boolean): string {
  try {
    return transform(wrapProgram(code, duckdb), { transforms: ["typescript"], disableESTransforms: true }).code;
  } catch (error) {
    throw new Error(`TypeScript parse failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * The Dynamic Worker's main module. It builds the `ctx` the bootstrap gives a
 * sandbox run (gonvex_bootstrap.js:709-730, with the module SDK's argument
 * checks), calls `run`, and answers one JSON envelope. Every host call goes
 * through the single RPC target the workspace passes in; the isolate has no
 * other binding and no network (`globalOutbound: null`).
 */
export const RUNNER_MODULE = `import { WorkerEntrypoint } from "cloudflare:workers";
import { run as program } from "./program.js";

class GonvexHostError extends Error {
  constructor(message, status) {
    super(message);
    this.name = status === "denied" ? "GonvexCapabilityError" : "GonvexHostError";
    this.status = status;
    this.code = status === "denied" ? "CAPABILITY_DENIED" : "HOST_CALL_FAILED";
  }
}
const text = (name, value) => {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(name + " must be a non-empty string");
  return value;
};
const parameterList = (parameters) => {
  if (parameters === undefined || parameters === null) return [];
  if (!Array.isArray(parameters)) throw new TypeError("query parameters must be an array");
  return [...parameters];
};
const byteLength = (value) => new TextEncoder().encode(value).byteLength;

export default class SandboxRunner extends WorkerEntrypoint {
  async run(host, info) {
    const call = async (operation, payload) => {
      const outcome = await host.call(operation, JSON.stringify(payload === undefined ? null : payload));
      if (!outcome.ok) throw new GonvexHostError(outcome.message, "failed");
      return outcome.value === undefined ? undefined : JSON.parse(outcome.value);
    };
    const unsupported = (operation) => (...ignored) => call(operation, {});
    const ctx = Object.freeze({
      kind: "action",
      function: "run",
      now: info.now,
      auth: Object.freeze({ account: null, developerSession: null }),
      tenant: Object.freeze({ id: "sandbox", projectId: "sandbox", name: null }),
      member: null,
      sandbox: Object.freeze({
        create: unsupported("create"),
        destroy: unsupported("destroy"),
        run: unsupported("run"),
        cancel: unsupported("cancel"),
        status: unsupported("status"),
        readFile: unsupported("readFile"),
        writeFile: unsupported("writeFile"),
        readText: unsupported("readText"),
        writeText: unsupported("writeText"),
        importFile: unsupported("importFile"),
        __worker: Object.freeze({
          readText: (path) => call("worker.readText", { path: text("sandbox path", path) }),
          writeText: (path, content) => call("worker.writeText", { path: text("sandbox path", path), content: String(content) }),
          listFiles: () => call("worker.listFiles", {}),
          query: (statement, parameters) => call("worker.query", { statement: text("DuckDB statement", statement), parameters: parameterList(parameters) }),
          register: (name, rows) => call("worker.register", { name: text("DuckDB table", name), rows }),
        }),
      }),
    });
    let value;
    try {
      value = await program(ctx);
    } catch (error) {
      const detail = error instanceof Error ? (error.stack || String(error)) : String(error);
      return { ok: false, error: "module execution failed: " + detail };
    }
    let json;
    try {
      json = JSON.stringify(value);
    } catch (error) {
      return { ok: false, error: "module result is invalid: " + (error instanceof Error ? error.message : String(error)) };
    }
    if (json === undefined) json = "null";
    const size = byteLength(json);
    if (size > info.maxResultBytes) {
      return { ok: false, error: "module execution exceeded its budget: run returned more than the " + info.maxResultBytes + " byte result limit" };
    }
    return { ok: true, json };
  }
}
`;
