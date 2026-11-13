/**
 * The release builder (0028 §3.1): runs once per release, outside every
 * object. On the platform it runs in a Container (`server.mjs`, `Dockerfile`);
 * locally as a Node process; in tests in-process.
 *
 * - `inspect`: parse the incoming body, resolve the project, run the module checks.
 * - `prepare`: split the body into release pieces in R2 and build the K02
 *   tenant plan (the A01 transform over the whole history: ~10 s of CPU and
 *   more than 128 MB of heap on Whagons, which no object can spend, 0027 §1).
 * - `activate`: build the Worker version that carries every project's active
 *   bundle with K04's prelude (0016 G0-2, 0022 §3); `deploy` puts it live.
 */
import { buildReleasePlan } from "../../../kernel/migrate/build.ts";
import { manifestOf, R2PlanStore, type R2Like, type ReleasePlan } from "../../../kernel/migrate/plan.ts";
import { wrapBundle } from "../../../kernel/host/src/bundle.ts";
import { incomingKey, releaseKey, type ActiveRelease, type Inspection, type ReleaseBuilder, type ReleaseSummary } from "../release.ts";
import { checkArtifact, clientContractFromModule, cronsFromModule, decodeBase64Utf8, migrationsFromModule, resolveProject } from "./manifest.ts";

/** Byte storage the builder reads and writes (R2 directly, or through the kernel's internal blob route). */
export interface BlobStore extends R2Like {
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  put(key: string, value: string): Promise<unknown>;
  delete(key: string): Promise<unknown>;
}

/** Where a built Worker version goes. */
export interface VersionTarget {
  /** Stages the version's files; returns its id. Nothing is live yet. */
  stage(files: Map<string, string>, version: string): Promise<void>;
  /** Puts a staged version live. */
  deploy(version: string): Promise<void>;
}

/** The module the kernel Worker imports for the active releases (the `gonvex:releases` alias). */
export const RELEASES_ENTRY = "releases.js";
export const ACTION_RELEASES_ENTRY = "action-releases.js";

/**
 * The specifier stored bundles import K04's prelude from. The kernel's build
 * resolves it (`alias` in wrangler config) to `kernel/host/src/module-globals.ts`,
 * so a release in R2 does not depend on where a builder put its files.
 */
export const PRELUDE_SPECIFIER = "gonvex:module-globals";

type Json = Record<string, unknown>;

const isObject = (value: unknown): value is Json => !!value && typeof value === "object" && !Array.isArray(value);

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export interface BuilderOptions {
  blobs: BlobStore;
  target: VersionTarget;
  /** Import specifier of K04's `module-globals` in stored bundles (normally `PRELUDE_SPECIFIER`). */
  preludeSpecifier: string;
  /** Plan options (tests use small chunks). */
  planOptions?: Parameters<typeof buildReleasePlan>[1];
  log?: (message: string) => void;
}

export class ReleaseBuilderCore implements ReleaseBuilder {
  /** One parsed body at a time (the Whagons body is ~45 MB; parsing it twice per sync is avoidable). */
  private cached: { incoming: string; manifest: unknown } | undefined;

  private readonly options: BuilderOptions;
  constructor(options: BuilderOptions) {
    this.options = options;
  }

  private log(message: string): void {
    this.options.log?.(message);
  }

  private async body(incoming: string): Promise<{ ok: true; manifest: unknown } | { ok: false; status: number; error: string; plain: true }> {
    if (this.cached?.incoming === incoming) return { ok: true, manifest: this.cached.manifest };
    const object = await this.options.blobs.get(incomingKey(incoming));
    if (!object) return { ok: false, status: 400, error: "the sync body is no longer available", plain: true };
    const text = await object.text();
    try {
      const manifest = JSON.parse(text) as unknown;
      this.cached = { incoming, manifest };
      return { ok: true, manifest };
    } catch (cause) {
      return { ok: false, status: 400, error: `Failed to parse the request body as JSON: ${(cause as Error).message}`, plain: true };
    }
  }

  async inspect(incoming: string, headerProject: string): Promise<Inspection> {
    const parsed = await this.body(incoming);
    if (!parsed.ok) return parsed;
    const resolved = resolveProject(parsed.manifest, headerProject);
    if (!resolved.ok) return resolved;
    const manifest = parsed.manifest as Json;
    const module = manifest.module;
    if (!isObject(module)) return { ok: false, status: 422, error: "manifest has no TypeScript module" };
    const hash = typeof module.hash === "string" ? module.hash.trim() : "";
    try {
      await migrationsFromModule(module);
      await checkArtifact(resolved.project, hash, module);
    } catch (cause) {
      const message = (cause as Error).message;
      return { ok: false, status: 422, error: message.startsWith("project ") ? message : `project ${JSON.stringify(resolved.project)} has an invalid module artifact: ${message}` };
    }
    return { ok: true, project: resolved.project, hash };
  }

  async prepare(incoming: string, project: string): Promise<{ ok: true; summary: ReleaseSummary } | { ok: false; status: number; error: string }> {
    const parsed = await this.body(incoming);
    if (!parsed.ok) return { ok: false, status: parsed.status, error: parsed.error };
    const manifest: Json = { ...(parsed.manifest as Json), project };
    const module = manifest.module as Json;
    const hash = String(module.hash).trim();
    const fail = (error: unknown) => ({ ok: false as const, status: 422, error: (error as Error).message });
    let migrations;
    let facts;
    let clientContract;
    let crons;
    try {
      migrations = await migrationsFromModule(module);
      facts = await checkArtifact(project, hash, module);
      try {
        clientContract = clientContractFromModule(module);
      } catch (cause) {
        throw new Error(`project ${JSON.stringify(project)} has an invalid module artifact: ${(cause as Error).message}`);
      }
      crons = cronsFromModule(project, module, facts);
    } catch (cause) {
      return fail(cause);
    }

    const existing = await this.options.blobs.get(releaseKey(project, hash, "release.json"));
    if (existing) {
      const summary = JSON.parse(await existing.text()) as ReleaseSummary;
      this.log(`release ${project}/${hash} already prepared`);
      return { ok: true, summary: { ...summary, schemaDefinition: manifest.schema ?? {} } };
    }

    // The tenant plan: the A01 transform over every tenant migration (K02).
    let plan: ReleasePlan;
    const started = Date.now();
    try {
      plan = await buildReleasePlan(migrations, this.options.planOptions);
    } catch (cause) {
      return fail(cause);
    }
    this.log(`plan ${plan.id}: ${plan.migrations.length} migrations, ${Object.values(plan.chunks).reduce((n, c) => n + c.length, 0)} chunks in ${Date.now() - started} ms`);
    const existingPlan = await this.options.blobs.get(`plans/${plan.id}/manifest.json`);
    if (!existingPlan) await new R2PlanStore(this.options.blobs).write(plan);

    // The Query/Reducer bundle the Worker version imports: the split bundle when present (S2), else the whole one.
    const bundle = isObject(module.reducerBundle) ? module.reducerBundle : (module.javascript as Json);
    const source = decodeBase64Utf8(String(bundle.code));
    let app: string;
    try {
      app = wrapBundle(source, this.options.preludeSpecifier);
    } catch (cause) {
      return fail(new Error(`project ${JSON.stringify(project)} has an invalid module artifact: ${(cause as Error).message}`));
    }
    const contracts = Object.fromEntries(
      Object.entries(module.functions as Json).map(([path, value]) => {
        const f = value as Json;
        return [path, { kind: f.kind, internal: f.internal === true, args: f.args ?? null, result: f.result ?? null }];
      }),
    );
    // lib.rs: `installed.functions.len()`, the module's function definitions.
    const functionCount = facts.size;
    const storedManifest: Json = {
      project,
      generatedAt: manifest.generatedAt ?? null,
      functions: module.functions,
      schema: manifest.schema ?? {},
      visibility: module.visibility ?? manifest.visibility ?? {},
      module: {
        language: module.language ?? "typescript",
        generation: module.generation ?? null,
        hash,
        entrypoint: module.entrypoint,
        invitationAcceptanceReducer: module.invitationAcceptanceReducer ?? "",
        // K22: the serving host needs schemas, public/interactive flags, plans and client contract.
        files: Object.fromEntries(Object.entries((module.files ?? {}) as Json).filter(([name]) => name === "client-contract.json")),
        crons: module.crons ?? [],
        ...(module.permissionGrants !== undefined ? { permissionGrants: module.permissionGrants } : {}),
        release: { pieces: `releases/${encodeURIComponent(project)}/${hash}/`, planId: plan.id },
      },
    };
    const controlMigrations = migrations.filter((m) => m.scope === "control-plane").map((m) => ({ name: m.name, checksum: m.checksum, scope: m.scope, noTransaction: m.noTransaction, sql: m.sql }));
    const summary: ReleaseSummary = {
      project,
      hash,
      artifactGeneration: Number(module.generation ?? 0),
      functionCount,
      schemaDefinition: manifest.schema ?? {},
      migrations: migrations.map((m) => ({ name: m.name, checksum: m.checksum, scope: m.scope, noTransaction: m.noTransaction })),
      controlMigrations,
      planId: plan.id,
      clientContract,
      crons,
      invitationAcceptanceReducer: typeof module.invitationAcceptanceReducer === "string" ? module.invitationAcceptanceReducer.trim() : "",
      storedManifest,
      bundleBytes: new TextEncoder().encode(app).length,
    };
    await this.options.blobs.put(releaseKey(project, hash, "manifest.json"), JSON.stringify(manifest));
    await this.options.blobs.put(releaseKey(project, hash, "view.json"), JSON.stringify({ functions: manifest.functions ?? {}, schema: manifest.schema ?? {}, visibility: module.visibility ?? manifest.visibility ?? {} }));
    await this.options.blobs.put(releaseKey(project, hash, "app.js"), app);
    const actions = wrapBundle(decodeBase64Utf8(String((module.javascript as Json).code)), this.options.preludeSpecifier);
    await this.options.blobs.put(releaseKey(project, hash, "actions.js"), actions);
    await this.options.blobs.put(`releases/${encodeURIComponent(project)}/${hash}/functions.json`, JSON.stringify(contracts));
    // Written last: a release is prepared only when every piece is.
    await this.options.blobs.put(releaseKey(project, hash, "release.json"), JSON.stringify(summary));
    this.cached = undefined;
    return { ok: true, summary };
  }

  async activate(releases: ActiveRelease[]): Promise<{ ok: true; version: string } | { ok: false; error: string }> {
    try {
      const files = new Map<string, string>();
      const imports: string[] = [];
      const entries: string[] = [];
      const actionImports: string[] = [];
      const actionEntries: string[] = [];
      for (const [index, release] of releases.entries()) {
        const base = `releases/${encodeURIComponent(release.project)}/${release.hash}`;
        const app = await this.options.blobs.get(`${base}/app.js`);
        const functions = await this.options.blobs.get(`${base}/functions.json`);
        const summary = await this.options.blobs.get(`${base}/release.json`);
        if (!app || !functions || !summary) throw new Error(`release ${release.project}/${release.hash} is not prepared`);
        const facts = JSON.parse(await summary.text()) as ReleaseSummary;
        const actions = await this.options.blobs.get(`${base}/actions.js`);
        if (!actions) throw new Error(`release ${release.project}/${release.hash} has no Action bundle; sync it again`);
        files.set(`${base}/actions.js`, await actions.text());
        files.set(`${base}/manifest.json`, JSON.stringify(facts.storedManifest));
        actionImports.push(`import * as app${index} from "./${base}/actions.js";`, `import manifest${index} from "./${base}/manifest.json";`);
        actionEntries.push(`  ${JSON.stringify(release.project)}: { manifest: manifest${index}, exports: app${index} },`);
        files.set(`${base}/app.js`, await app.text());
        files.set(`${base}/functions.json`, await functions.text());
        imports.push(`import * as app${index} from "./${base}/app.js";`, `import functions${index} from "./${base}/functions.json";`, `import manifest${index} from "./${base}/manifest.json";`);
        entries.push(
          `  ${JSON.stringify(release.project)}: { project: ${JSON.stringify(release.project)}, hash: ${JSON.stringify(release.hash)}, generation: ${release.generation}, ` +
            `clientContract: ${JSON.stringify(facts.clientContract?.version ?? null)}, functions: functions${index}, manifest: manifest${index}, exports: app${index} },`,
        );
      }
      const body = `// Generated by the K19 release builder. Do not edit.\n${imports.join("\n")}\nexport const releases = {\n${entries.join("\n")}\n};\n`;
      files.set(ACTION_RELEASES_ENTRY, `// Generated by K19.\n${actionImports.join("\n")}\nexport const releases = {\n${actionEntries.join("\n")}\n};\n`);
      const version = (await sha256Hex(body + [...files].map(([name, content]) => `${name.length}:${name}${content.length}:${content}`).join(""))).slice(0, 16);
      files.set(RELEASES_ENTRY, `${body.replace("// Generated", `// Version ${version}. Generated`)}export const version = ${JSON.stringify(version)};\n`);
      await this.options.target.stage(files, version);
      return { ok: true, version };
    } catch (cause) {
      return { ok: false, error: `worker version build failed: ${(cause as Error).message}` };
    }
  }

  async deploy(version: string): Promise<{ ok: true } | { ok: false; error: string }> {
    try {
      await this.options.target.deploy(version);
      return { ok: true };
    } catch (cause) {
      return { ok: false, error: `worker version deploy failed: ${(cause as Error).message}` };
    }
  }
}

/** A version target that only remembers (tests). */
export class MemoryVersionTarget implements VersionTarget {
  readonly staged = new Map<string, Map<string, string>>();
  readonly deployed: string[] = [];
  async stage(files: Map<string, string>, version: string): Promise<void> {
    this.staged.set(version, files);
  }
  async deploy(version: string): Promise<void> {
    if (!this.staged.has(version)) throw new Error(`version ${version} was not staged`);
    this.deployed.push(version);
  }
}

export { manifestOf };
