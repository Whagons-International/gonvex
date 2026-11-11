/**
 * Releases: what one `/dev/sync` produces, where its pieces live, and the
 * interface to the release builder that runs outside every object.
 *
 * The Whagons manifest is about 45 MB of JSON (files 27 MB, JavaScript
 * bundles 14 MB, function manifest 1.6 MB). That is over the 32 MiB Workers
 * RPC argument cap and far over the 2 MB row cap, and parsing it would take a
 * large share of a 128 MB isolate. So the router streams the request body into
 * R2 untouched, and the release builder (a Container on the platform, a Node
 * process locally; 0028 §3.1) parses, validates, splits and plans it. The
 * control object only ever sees the small `ReleaseSummary`.
 *
 * R2 layout (bucket binding `RELEASES`):
 *
 *   incoming/<id>.json                      the raw request body (deleted after the sync)
 *   releases/<project>/<hash>/manifest.json the manifest as synced, project rewritten (Rust's stored manifest)
 *   releases/<project>/<hash>/view.json     {functions, schema, visibility} for GET /dev/manifest
 *   releases/<project>/<hash>/app.js        the Query/Reducer bundle the Worker version imports
 *   releases/<project>/<hash>/release.json  ReleaseSummary
 *   plans/<planId>/...                      the K02 release plan (R2PlanStore)
 */

/** Migration facts, as Rust's `SqlMigration` (no SQL text). */
export interface MigrationInfo {
  name: string;
  checksum: string;
  scope: "tenant" | "control-plane";
  noTransaction: boolean;
}

export interface CronInfo {
  name: string;
  function: string;
  scope: string;
  intervalMs?: number;
  expression?: string;
  dueWhen?: string;
}

export interface ReleaseSummary {
  project: string;
  /** `module.hash`, the canonical artifact hash. */
  hash: string;
  /** Format generation of the artifact (`module.generation`). */
  artifactGeneration: number;
  functionCount: number;
  /** `manifest.schema`, echoed back as `schemaDefinition`. */
  schemaDefinition: unknown;
  migrations: MigrationInfo[];
  /** Control-plane migrations with their SQL (small; applied in the control object). */
  controlMigrations: Array<MigrationInfo & { sql: string }>;
  /** Tenant plan id (K02 `planId`); null when the module has no tenant migrations. */
  planId: string | null;
  clientContract: { version: number; offlineMaxAgeMs: number | null } | null;
  crons: CronInfo[];
  invitationAcceptanceReducer: string;
  /** What `gonvex_runtime_manifests.manifest` holds in the kernel: without code and files (2 MB row cap). */
  storedManifest: Record<string, unknown>;
  /** Bytes of the stored app bundle (the Worker version module). */
  bundleBytes: number;
}

/** Inspection of an incoming body: everything Rust checks before it touches a database. */
export type Inspection =
  | { ok: true; project: string; hash: string }
  | { ok: false; status: number; error: string; plain?: boolean };

export interface ActiveRelease {
  project: string;
  hash: string;
  generation: number;
}

/**
 * The release builder. All three calls are idempotent per (project, hash).
 */
export interface ReleaseBuilder {
  /**
   * Parses `incoming/<id>.json`: project resolution against the header (D.2.2
   * items 1-2) and the module checks of `validate_manifest_record` (item 4).
   * Writes nothing but the result.
   */
  inspect(incoming: string, headerProject: string): Promise<Inspection>;
  /**
   * Splits the accepted body into its release pieces and builds the K02 tenant
   * plan (A01 transform, chunks) into R2. Returns the summary, or a 422 text
   * (an unhandled construct fails loudly, 0014 rule 2).
   */
  prepare(incoming: string, project: string): Promise<{ ok: true; summary: ReleaseSummary } | { ok: false; status: number; error: string }>;
  /**
   * Builds and deploys the Worker version that carries `releases` (every
   * project's active bundle, 0016 G0-2). Locally this rewrites the generated
   * module the local kernel imports, and workerd reloads.
   */
  activate(releases: ActiveRelease[]): Promise<{ ok: true; version: string } | { ok: false; error: string }>;
  /**
   * Puts an activated version live. The router calls it after the control
   * object answered, so a deploy that restarts the control object cannot cut
   * the response. Locally it returns at once and swaps the version shortly
   * after (workerd reloads, which would cut every request in flight).
   */
  deploy(version: string): Promise<{ ok: true } | { ok: false; error: string }>;
}

export const releaseKey = (project: string, hash: string, piece: "manifest.json" | "view.json" | "app.js" | "release.json") =>
  `releases/${encodeURIComponent(project)}/${hash}/${piece}`;
export const incomingKey = (id: string) => `incoming/${id}.json`;

/** One rollout step on the queue (D.2.4): apply `planId` to one tenant object. */
export interface RolloutMessage {
  syncId: string;
  project: string;
  tenant: string;
  planId: string;
  index: number;
}

export interface RolloutReport {
  syncId: string;
  tenant: string;
  index: number;
  state: "done" | "failed";
  error: string | null;
}

/** HTTP client for a builder behind a fetcher (a Container binding, a service binding, or a URL). */
export class HttpReleaseBuilder implements ReleaseBuilder {
  private readonly send: (path: string, body: unknown) => Promise<Response>;
  constructor(send: (path: string, body: unknown) => Promise<Response>) {
    this.send = send;
  }

  static forUrl(base: string, token: string | undefined, fetcher: typeof fetch = fetch): HttpReleaseBuilder {
    const root = base.replace(/\/+$/, "");
    return new HttpReleaseBuilder((path, body) =>
      fetcher(`${root}${path}`, { method: "POST", headers: { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(body) }),
    );
  }

  private async call<T>(path: string, body: unknown): Promise<T> {
    const response = await this.send(path, body);
    const text = await response.text();
    if (!response.ok) throw new Error(`release builder ${path} failed (${response.status}): ${text.slice(0, 500)}`);
    return JSON.parse(text) as T;
  }

  inspect(incoming: string, headerProject: string): Promise<Inspection> {
    return this.call("/inspect", { incoming, headerProject });
  }
  prepare(incoming: string, project: string) {
    return this.call<{ ok: true; summary: ReleaseSummary } | { ok: false; status: number; error: string }>("/prepare", { incoming, project });
  }
  activate(releases: ActiveRelease[]) {
    return this.call<{ ok: true; version: string } | { ok: false; error: string }>("/activate", { releases });
  }
  deploy(version: string) {
    return this.call<{ ok: true } | { ok: false; error: string }>("/deploy", { version });
  }
}
