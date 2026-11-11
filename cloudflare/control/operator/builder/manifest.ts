/**
 * The checks `/dev/sync` makes on a manifest before it touches a database
 * (rust/crates/runtime/src/lib.rs `apply_dev_sync` items 1-4,
 * modules.rs `validate_manifest_record`, `migrations_from_module`,
 * `artifact_from_manifest`, `canonical_artifact_hash`,
 * `client_contract_from_module`, `crons_from_module`).
 *
 * Plain TypeScript: runs in the Node release builder and in workerd tests.
 */
import { MigrationFileError, parseMigration, type SqlMigration } from "../../../kernel/migrate/migrations.ts";

export class ArtifactError extends Error {}

const invalid = (project: string, message: string) => new ArtifactError(`project ${JSON.stringify(project)} has an invalid module artifact: ${message}`);

type Json = Record<string, unknown>;
const isObject = (value: unknown): value is Json => !!value && typeof value === "object" && !Array.isArray(value);

/** modules.rs `canonical_json`: sorted keys, no whitespace. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  return `{${Object.entries(value as Json)
    .filter(([, child]) => child !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([key, child]) => `${JSON.stringify(key)}:${canonicalJson(child)}`)
    .join(",")}}`;
}

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** modules.rs `canonical_artifact_hash`. */
export async function canonicalArtifactHash(module: Json): Promise<string> {
  const javascript = module.javascript;
  if (!isObject(javascript)) throw new Error("artifact has no JavaScript bundle");
  const contract: Json = {
    generation: module.generation ?? null,
    language: module.language ?? "typescript",
    entrypoint: module.entrypoint ?? null,
    files: module.files ?? {},
    functions: module.functions ?? {},
    visibility: module.visibility ?? {},
    crons: module.crons ?? [],
    invitationAcceptanceReducer: module.invitationAcceptanceReducer ?? "",
    javascript: { path: javascript.path ?? null, hash: typeof javascript.hash === "string" ? javascript.hash.trim().toLowerCase() : "" },
  };
  if (module.reducerBundle !== undefined) {
    const bundle = module.reducerBundle;
    if (!isObject(bundle)) throw new Error("reducerBundle must be an object");
    contract.reducerBundle = { path: bundle.path ?? null, hash: typeof bundle.hash === "string" ? bundle.hash.trim().toLowerCase() : "", functions: bundle.functions ?? [] };
  }
  if (module.permissionGrants !== undefined && module.permissionGrants !== null) contract.permissionGrants = module.permissionGrants;
  return sha256Hex(canonicalJson(contract));
}

/** Rust `STANDARD.decode` then `String::from_utf8`. */
export function decodeBase64Utf8(encoded: string): string {
  if (!/^[A-Za-z0-9+/]*={0,2}$/.test(encoded) || encoded.length % 4 === 1) throw new Error("Invalid input length");
  const binary = atob(encoded);
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
  return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
}

/** modules.rs `migrations_from_module`: only `migrations/<name>.sql` directly under `migrations/`. */
export async function migrationsFromModule(module: Json): Promise<SqlMigration[]> {
  const files = module.files;
  if (!isObject(files)) return [];
  const out: SqlMigration[] = [];
  for (const [path, encoded] of Object.entries(files)) {
    const slash = path.lastIndexOf("/");
    if (path.slice(0, slash) !== "migrations" || !path.endsWith(".sql")) continue;
    const name = path.slice(slash + 1);
    if (typeof encoded !== "string") throw new Error(`migration ${name} must be base64 text`);
    let source: string;
    try {
      source = decodeBase64Utf8(encoded);
    } catch (error) {
      if (error instanceof TypeError || (error as Error).message.includes("utf-8")) throw new Error(`migration ${name} must be UTF-8`);
      throw new Error(`decode migration ${name}: ${(error as Error).message}`);
    }
    try {
      out.push(await parseMigration(name, source));
    } catch (error) {
      if (error instanceof MigrationFileError) throw new Error(error.message);
      throw error;
    }
  }
  return out.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

/** modules.rs `client_contract_from_module`. */
export function clientContractFromModule(module: Json): { version: number; offlineMaxAgeMs: number | null } | null {
  const files = isObject(module.files) ? module.files : undefined;
  const encoded = files?.["client-contract.json"];
  if (encoded === undefined) return null;
  if (typeof encoded !== "string") throw new Error("invalid client contract encoding");
  const contract = JSON.parse(decodeBase64Utf8(encoded)) as Json;
  const version = contract.version;
  if (typeof version !== "number" || !Number.isSafeInteger(version) || version <= 0) throw new Error("invalid client contract version");
  const window = contract.offlineMaxAgeMs;
  if (window !== null && (typeof window !== "number" || !Number.isSafeInteger(window) || window <= 0)) throw new Error("invalid offline window");
  return { version, offlineMaxAgeMs: window as number | null };
}

export interface FunctionFacts {
  kind: "query" | "reducer" | "action";
  internal: boolean;
}

/** The parts of `artifact_from_manifest` that decide whether a module installs. */
export async function checkArtifact(project: string, recordHash: string, module: Json): Promise<Map<string, FunctionFacts>> {
  const hash = typeof module.hash === "string" ? module.hash.trim() : "";
  if (hash === "") throw invalid("unknown", "artifact hash is required");
  if (hash !== recordHash) throw invalid(project, `stored module hash ${JSON.stringify(recordHash)} does not match artifact hash ${JSON.stringify(hash)}`);
  let canonical: string;
  try {
    canonical = await canonicalArtifactHash(module);
  } catch (error) {
    throw invalid(project, `canonical artifact hash: ${(error as Error).message}`);
  }
  if (hash !== canonical) throw invalid(project, `module artifact hash ${JSON.stringify(hash)} does not match canonical contract hash ${JSON.stringify(canonical)}`);
  const javascript = module.javascript;
  if (javascript === undefined) throw invalid(project, "artifact has no JavaScript bundle");
  if (!isObject(javascript) || typeof javascript.path !== "string" || typeof javascript.hash !== "string" || typeof javascript.code !== "string") {
    throw invalid(project, "invalid JavaScript bundle");
  }
  const functions = module.functions;
  if (!isObject(functions)) throw invalid(project, "artifact functions must be an object");
  const facts = new Map<string, FunctionFacts>();
  for (const [path, value] of Object.entries(functions)) {
    if (!isObject(value)) throw invalid(project, `function ${JSON.stringify(path)} must be an object`);
    const kind = typeof value.kind === "string" ? value.kind : "";
    if (kind !== "query" && kind !== "reducer" && kind !== "action") throw invalid(project, `function ${JSON.stringify(path)} has unknown kind ${JSON.stringify(kind)}`);
    for (const flag of ["internal", "public", "interactive"]) {
      if (value[flag] !== undefined && typeof value[flag] !== "boolean") throw invalid(project, `function ${JSON.stringify(path)} ${flag} must be a boolean`);
    }
    const internal = value.internal === true;
    const isPublic = value.public === true;
    if (
      isPublic &&
      (internal ||
        (kind === "query" && ("replica" in value || (typeof value.delivery === "string" ? value.delivery : "oneShot") !== "oneShot")) ||
        (kind === "reducer" &&
          (value.interactive !== false || (isObject(value.offline) && value.offline.mode === "allowed") || "localExecution" in value || "optimistic" in value)))
    ) {
      throw invalid(project, `public function ${JSON.stringify(path)} must be non-internal, one-shot and server-only`);
    }
    const declaredClassification = value.classification;
    if (declaredClassification !== undefined && typeof declaredClassification !== "string") throw invalid(project, `function ${JSON.stringify(path)} classification must be a string`);
    let interactive: boolean;
    if (typeof value.interactive === "boolean") interactive = value.interactive;
    else if (declaredClassification === "interactive") interactive = true;
    else if (declaredClassification === "system" || declaredClassification === "internal") interactive = false;
    else if (declaredClassification !== undefined) throw invalid(project, `function ${JSON.stringify(path)} has unknown classification ${JSON.stringify(declaredClassification)}`);
    else interactive = !internal && kind !== "action";
    const expected = internal ? "internal" : interactive ? "interactive" : "system";
    if ((declaredClassification ?? expected) !== expected) throw invalid(project, `function ${JSON.stringify(path)} classification does not match its internal and interactive flags`);
    const agent = isObject(value.agent) ? value.agent : undefined;
    const confirmation = typeof agent?.confirmation === "string" ? agent.confirmation : "none";
    if (!["none", "required", "destructive"].includes(confirmation)) throw invalid(project, `function ${JSON.stringify(path)} has an invalid agent confirmation`);
    facts.set(path, { kind, internal });
  }
  const bundle = module.reducerBundle;
  if (bundle !== undefined) {
    if (!isObject(bundle) || !Array.isArray(bundle.functions) || typeof bundle.code !== "string") throw invalid(project, "reducerBundle: invalid reducer bundle");
    for (const path of bundle.functions) {
      const definition = facts.get(String(path));
      if (!definition) throw invalid(project, `reducerBundle lists unknown function ${JSON.stringify(path)}`);
      if (definition.kind === "action") throw invalid(project, `reducerBundle lists ${JSON.stringify(path)}, which is not a Query or Reducer`);
    }
  }
  if (module.visibility !== undefined && !isObject(module.visibility)) throw invalid(project, "visibility plans: invalid type: expected a map");
  for (const [table, plan] of Object.entries((module.visibility ?? {}) as Json)) {
    if (isObject(plan) && typeof plan.table === "string" && plan.table !== table) {
      throw invalid(project, `visibility plan key ${JSON.stringify(table)} does not match table ${JSON.stringify(plan.table)}`);
    }
  }
  if (typeof module.entrypoint !== "string" || module.entrypoint.trim() === "") throw invalid("unknown", "artifact entrypoint is required");
  return facts;
}

export interface CronFacts {
  name: string;
  function: string;
  scope: string;
  intervalMs?: number;
  expression?: string;
  dueWhen?: string;
}

/** modules.rs `crons_from_module` (the `dueWhen` read-only admission is K11's, at schedule time). */
export function cronsFromModule(project: string, module: Json, functions: Map<string, FunctionFacts>): CronFacts[] {
  const raw = module.crons ?? [];
  if (!Array.isArray(raw)) throw invalid(project, "invalid crons: invalid type: expected a sequence");
  const names = new Set<string>();
  const crons: CronFacts[] = [];
  for (const item of raw) {
    if (!isObject(item)) throw invalid(project, "invalid crons: invalid type: expected a map");
    const name = typeof item.name === "string" ? item.name : "";
    const fn = typeof item.function === "string" ? item.function : "";
    const scope = typeof item.scope === "string" ? item.scope : "";
    if (name.trim() === "" || fn.trim() === "") throw invalid(project, "cron name and function are required");
    if (names.has(name.trim())) throw invalid(project, `duplicate cron name ${JSON.stringify(name)}`);
    names.add(name.trim());
    if (scope !== "project" && scope !== "tenant") throw invalid(project, `cron ${JSON.stringify(name)} has invalid scope`);
    const interval = item.intervalMs;
    const expression = item.expression;
    const hasInterval = interval !== undefined && interval !== null;
    const hasExpression = expression !== undefined && expression !== null;
    if (hasInterval === hasExpression || interval === 0 || (typeof expression === "string" && expression.trim() === "")) {
      throw invalid(project, `cron ${JSON.stringify(name)} requires exactly one positive intervalMs or expression`);
    }
    const cron: CronFacts = { name, function: fn, scope };
    if (hasInterval) cron.intervalMs = Number(interval);
    if (hasExpression) cron.expression = String(expression);
    if (typeof item.dueWhen === "string" && item.dueWhen.trim() !== "") cron.dueWhen = item.dueWhen;
    crons.push(cron);
  }
  for (const cron of crons) {
    const definition = functions.get(cron.function);
    if (!definition) throw invalid(project, `cron ${JSON.stringify(cron.name)} targets unknown function ${JSON.stringify(cron.function)}`);
    if (definition.kind !== "reducer" && definition.kind !== "action") throw invalid(project, `cron ${JSON.stringify(cron.name)} must target a Reducer or Action`);
  }
  return crons;
}

export type ProjectResolution = { ok: true; project: string } | { ok: false; status: number; error: string; plain?: boolean };

/** lib.rs `apply_dev_sync` items 1-2 on a parsed body. */
export function resolveProject(manifest: unknown, headerProject: string): ProjectResolution {
  if (!isObject(manifest)) {
    return { ok: false, status: 422, error: "Failed to deserialize the JSON body into the target type: invalid type: expected a map", plain: true };
  }
  const header = headerProject.trim();
  const body = typeof manifest.project === "string" ? manifest.project.trim() : "";
  const project = body === "" ? header : body;
  if (project === "" || (header !== "" && header !== project)) return { ok: false, status: 400, error: "manifest project does not match x-gonvex-project-id" };
  return { ok: true, project };
}
