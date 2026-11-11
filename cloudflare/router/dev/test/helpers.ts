import { env, runInDurableObject, SELF } from "cloudflare:test";
import { MemoryVersionTarget, PRELUDE_SPECIFIER, ReleaseBuilderCore, type BlobStore } from "../../../control/operator/builder/core.ts";
import { canonicalArtifactHash } from "../../../control/operator/builder/manifest.ts";
import type { OperatorControlObject } from "../../../control/operator/object.ts";
import type { ReleaseBuilder } from "../../../control/operator/release.ts";
import type { LocalKernelTenant } from "../local/tenant.ts";

export const ADMIN = "k19-admin-key";
export const FALLBACK_SYNC_KEY = "k19-fallback-sync-key";

export interface TestEnv {
  CONTROL: DurableObjectNamespace<OperatorControlObject>;
  TENANT: DurableObjectNamespace<LocalKernelTenant>;
  RELEASES: R2Bucket;
}
export const testEnv = env as unknown as TestEnv;

export interface Reply {
  status: number;
  body: any;
  text: string;
  headers: Headers;
}

/** One HTTP call through the router Worker. `token: null` sends no Authorization. */
export async function call(method: string, path: string, options: { token?: string | null; body?: unknown; headers?: Record<string, string>; raw?: string } = {}): Promise<Reply> {
  const headers: Record<string, string> = { ...(options.headers ?? {}) };
  const token = options.token === undefined ? ADMIN : options.token;
  if (token !== null) headers.authorization = `Bearer ${token}`;
  let body: string | undefined;
  if (options.raw !== undefined) body = options.raw;
  else if (options.body !== undefined) {
    body = JSON.stringify(options.body);
    headers["content-type"] ??= "application/json";
  }
  const response = await SELF.fetch(`http://kernel.test${path}`, { method, headers, ...(body !== undefined ? { body } : {}) });
  const text = await response.text();
  let parsed: unknown = undefined;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  return { status: response.status, body: parsed, text, headers: response.headers };
}

export const control = () => testEnv.CONTROL.getByName("control");

/** Installs an in-process release builder in the control object; returns its version target. */
export async function useBuilder(wrap?: (builder: ReleaseBuilder) => ReleaseBuilder): Promise<MemoryVersionTarget> {
  const target = new MemoryVersionTarget();
  const core = new ReleaseBuilderCore({ blobs: testEnv.RELEASES as unknown as BlobStore, target, preludeSpecifier: PRELUDE_SPECIFIER, planOptions: { chunk: { maxStatements: 40 } } });
  await runInDurableObject(control(), async (instance: OperatorControlObject) => instance.setReleaseBuilderForTest(wrap ? wrap(core) : core));
  return target;
}

const b64 = (text: string) => btoa(String.fromCharCode(...new TextEncoder().encode(text)));

export const NOTES_MIGRATION = `-- gonvex:scope tenant
CREATE TABLE notes (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  done BOOLEAN NOT NULL DEFAULT 0,
  meta JSON,
  score REAL,
  created_at INTEGER NOT NULL DEFAULT 0
);
INSERT INTO notes(id,title,done,meta,score,created_at) VALUES
  ('n1','first note',0,'{"tag":"a","ref":"old-id"}',1.5,1),
  ('n2','second note',1,'{"tag":"b"}',NULL,2),
  ('n3','third',0,NULL,3,3);
`;

export const CONTROL_MIGRATION = `-- gonvex:scope control-plane
CREATE TABLE IF NOT EXISTS app_control_settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
`;

export interface ManifestOptions {
  migrations?: Record<string, string>;
  functions?: Record<string, Record<string, unknown>>;
  files?: Record<string, string>;
  crons?: unknown[];
  mutate?: (module: Record<string, unknown>) => void;
  /** Keep the given hash instead of the canonical one. */
  hash?: string;
}

/** A manifest as `gonvex dev` builds it (packages/gonvex buildManifest), with a canonical module hash. */
export async function manifest(project: string, options: ManifestOptions = {}): Promise<Record<string, unknown>> {
  const migrations = options.migrations ?? { "0001_notes.sql": NOTES_MIGRATION };
  const files: Record<string, string> = {};
  for (const [name, sql] of Object.entries(migrations)) files[`migrations/${name}`] = b64(sql);
  for (const [path, text] of Object.entries(options.files ?? {})) files[path] = b64(text);
  const functions = options.functions ?? {
    "notes.list": { kind: "query", args: { kind: "object", fields: {} }, result: { kind: "any" } },
    "notes.add": { kind: "reducer", args: { kind: "object", fields: {} }, result: { kind: "any" } },
    "notes.sync": { kind: "action", args: { kind: "object", fields: {} }, result: { kind: "any" } },
  };
  const module: Record<string, unknown> = {
    language: "typescript",
    generation: 8,
    entrypoint: "gonvex/index.ts",
    functions,
    visibility: {},
    files,
    javascript: { path: "gonvex/_generated/module.js", hash: "0".repeat(64), code: b64('export const list = () => [];\nexport { list as "notes.list" };\n') },
    crons: options.crons ?? [],
    invitationAcceptanceReducer: "",
  };
  options.mutate?.(module);
  module.hash = options.hash ?? (await canonicalArtifactHash(module));
  return { project, generatedAt: "2026-11-11T00:00:00.000Z", functions, schema: { tables: {} }, module, visibility: {} };
}

/** POST /dev/sync as the CLI sends it (syncRuntime in packages/gonvex). */
export function sync(project: string, body: unknown, key: string, headers: Record<string, string> = {}): Promise<Reply> {
  return call("POST", "/dev/sync", { token: null, body, headers: { authorization: `Bearer ${key}`, "x-gonvex-key": key, "x-gonvex-project-id": project, ...headers } });
}

/** Creates a project with the admin key; returns its id and key. */
export async function createProject(name: string, databaseMode: "single" | "multiTenant" = "multiTenant"): Promise<{ id: string; key: string }> {
  const reply = await call("POST", "/dev/projects", { body: { name, databaseMode } });
  if (reply.status !== 201) throw new Error(`create project: ${reply.status} ${reply.text}`);
  return { id: reply.body.project.id, key: reply.body.projectKey };
}

export const tenantObject = (tenantId: string) => testEnv.TENANT.getByName(tenantId);
