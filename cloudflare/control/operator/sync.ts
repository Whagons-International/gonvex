/**
 * `/dev/sync` in the control object (CONTRACT D.2, D.2.4; 0012 R1-R5; 0016 G0-2).
 *
 * Order, as rust/crates/runtime/src/lib.rs `apply_dev_sync`:
 *
 *  1. inspect the body (builder): project resolution and the module checks;
 *  2. the sync key (`project_accepts_sync_key`, with `GONVEX_DEV_SYNC_KEY` as
 *     the fallback for a project without a key, R3);
 *  3. a per-project lock: a second concurrent sync gets 409 (R5, kernel only);
 *  4. prepare the release (builder): pieces and the K02 tenant plan in R2;
 *  5. control-plane migrations, in this object's SQLite with Rust's ledger;
 *  6. tenant rollout: one queue message per tenant, sequential in `tenant_id`
 *     order, first failure aborts with 422 (fail-fast, R1); the request is held
 *     open until the last tenant reports or the deadline passes (R4);
 *  7. activation: the Worker version carrying every project's active bundle is
 *     built (builder), the release becomes active and the manifest is saved;
 *     the router then deploys that version (outside this object, so a deploy
 *     that restarts this object cannot cut the response) and tenant objects
 *     send `system.reload` / `client.updateRequired` (`ModuleReloaded`).
 */
import type { Sql } from "../src/sql";
import { NOW_MS, num, str } from "../src/sql";
import { HttpError } from "./http";
import type { ActiveRelease, ReleaseBuilder, ReleaseSummary, RolloutMessage, RolloutReport } from "./release";

export const OPERATOR_SCHEMA = `
CREATE TABLE IF NOT EXISTS gonvex_migrations (
  name TEXT PRIMARY KEY, checksum TEXT NOT NULL,
  applied_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  duration_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS gonvex_cf_releases (
  project_id TEXT NOT NULL, hash TEXT NOT NULL, summary JSON NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending', generation INTEGER, error TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  activated_at INTEGER,
  PRIMARY KEY(project_id, hash)
);
CREATE TABLE IF NOT EXISTS gonvex_cf_active_releases (
  project_id TEXT PRIMARY KEY, hash TEXT NOT NULL, generation INTEGER NOT NULL, plan_id TEXT NOT NULL,
  version TEXT NOT NULL DEFAULT '',
  activated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE IF NOT EXISTS gonvex_cf_syncs (
  sync_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, hash TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', tenants JSON NOT NULL DEFAULT '[]',
  plan_id TEXT NOT NULL DEFAULT '', next_index INTEGER NOT NULL DEFAULT 0,
  started_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  expires_at INTEGER NOT NULL, finished_at INTEGER
);
CREATE TABLE IF NOT EXISTS gonvex_cf_publications (
  sync_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, hash TEXT NOT NULL,
  generation INTEGER NOT NULL, version TEXT NOT NULL, previous_hash TEXT,
  tenants JSON NOT NULL, completed INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS gonvex_cf_syncs_running ON gonvex_cf_syncs(project_id, state);
`;

export interface SyncPorts {
  sql: Sql;
  builder: ReleaseBuilder;
  /** Sends one rollout step to the queue. */
  enqueue(message: RolloutMessage): Promise<void>;
  /** Only the serving composition can acknowledge its baked Worker version. */
  versionReady(version: string): boolean;
  activateTenants(project: string, tenants: string[], hash: string, generation: number): Promise<boolean>;
  moduleReloaded(project: string, tenants: string[], hash: string, generation: number, clientContract: number | null, publicationId: string): Promise<void>;
  /** K11: re-plan the project's crons for the new module. A no-op until K11 provides it. */
  syncCrons(project: string, summary: ReleaseSummary, tenants: string[]): Promise<void>;
  deadlineMs: number;
  /** `GONVEX_DEV_SYNC_KEY`. */
  devSyncKey: string | undefined;
  acceptsKey(project: string, key: string, fallback: string | undefined): boolean;
}

export interface SyncRequest {
  incoming: string;
  /** A client retry resumes this publication; each incoming body still has a unique R2 key. */
  requestId?: string;
  headerProject: string;
  key: string;
}

export type SyncOutcome =
  | { ok: true; body: Record<string, unknown>; deploy: string | null; publication?: string }
  | { ok: false; status: number; error: string; plain?: boolean };

interface Waiter {
  resolve(result: { ok: true } | { ok: false; error: string }): void;
}

/** Rust's `SqlMigration` ledger for the control plane (provision.rs `apply_migrations_locked`, control scope). */
export function applyControlMigrations(sql: Sql, migrations: ReleaseSummary["controlMigrations"]): void {
  for (const migration of migrations) {
    const stored = sql.value<string>("SELECT checksum FROM gonvex_migrations WHERE name=$1", migration.name);
    if (stored !== undefined) {
      if (str(stored) !== migration.checksum) {
        throw new Error(`migration ${migration.name} checksum mismatch: database has ${str(stored)}, artifact has ${migration.checksum}`);
      }
      continue;
    }
    const started = Date.now();
    const record = () => sql.run("INSERT INTO gonvex_migrations(name,checksum,applied_at,duration_ms) VALUES($1,$2," + NOW_MS + ",$3)", migration.name, migration.checksum, Date.now() - started);
    if (migration.noTransaction) {
      // Statement by statement; earlier statements stay after a failure. The ledger row is last.
      sql.script(migration.sql);
      record();
    } else {
      sql.tx(() => {
        sql.script(migration.sql);
        record();
      });
    }
  }
}

export class SyncCoordinator {
  private readonly waiters = new Map<string, Waiter>();
  private readonly publications = new Map<string, Promise<{ ready: boolean }>>();

  constructor(private readonly ports: SyncPorts) {}

  private get sql(): Sql {
    return this.ports.sql;
  }

  activeRelease(project: string): (ActiveRelease & { planId: string; summary: ReleaseSummary }) | null {
    const row = this.sql.get(
      `SELECT a.project_id,a.hash,a.generation,a.plan_id,r.summary FROM gonvex_cf_active_releases a
       JOIN gonvex_cf_releases r ON r.project_id=a.project_id AND r.hash=a.hash WHERE a.project_id=$1`,
      project,
    );
    if (!row) return null;
    return { project: str(row.project_id), hash: str(row.hash), generation: num(row.generation), planId: str(row.plan_id), summary: JSON.parse(str(row.summary)) as ReleaseSummary };
  }

  activeReleases(): ActiveRelease[] {
    return this.sql.all("SELECT project_id,hash,generation FROM gonvex_cf_active_releases ORDER BY project_id").map((row) => ({ project: str(row.project_id), hash: str(row.hash), generation: num(row.generation) }));
  }

  /** `tenant_routes`: non-deleted, non-disabled tenants with a database, by `tenant_id`. */
  tenantRoutes(project: string): string[] {
    return this.sql
      .all(
        `SELECT tenant_id FROM gonvex_runtime_tenants
         WHERE project_id=$1 AND deleted_at IS NULL AND status NOT IN ('deleted','disabled') AND database_url<>''
         ORDER BY tenant_id`,
        project,
      )
      .map((row) => str(row.tenant_id));
  }

  /** The held-open sync (0012 R4). */
  async sync(request: SyncRequest): Promise<SyncOutcome> {
    const { builder } = this.ports;
    const inspection = await builder.inspect(request.incoming, request.headerProject);
    if (!inspection.ok) return { ok: false, status: inspection.status, error: inspection.error, ...(inspection.plain ? { plain: true } : {}) };
    const project = inspection.project;
    if (!this.ports.acceptsKey(project, request.key, this.ports.devSyncKey)) return { ok: false, status: 401, error: "invalid Gonvex sync key" };

    const syncId = request.requestId ?? crypto.randomUUID();
    const publication = this.sql.get(`SELECT p.*,r.summary,s.state FROM gonvex_cf_publications p JOIN gonvex_cf_releases r
      ON r.project_id=p.project_id AND r.hash=p.hash JOIN gonvex_cf_syncs s ON s.sync_id=p.sync_id WHERE p.sync_id=$1`, syncId);
    if (publication) {
      if (publication.project_id !== project || publication.hash !== inspection.hash) return {ok:false,status:409,error:"sync request id already belongs to a different module"};
      if (!num(publication.completed) && publication.state !== "publishing") return {ok:false,status:409,error:"sync publication is no longer current"};
      const summary = JSON.parse(str(publication.summary)) as ReleaseSummary;
      return {ok:true,deploy:num(publication.completed) || this.ports.versionReady(str(publication.version)) ? null : str(publication.version),publication:syncId,body:this.response(project,summary)};
    }
    const prior = this.sql.get("SELECT project_id,hash,state FROM gonvex_cf_syncs WHERE sync_id=$1",syncId);
    if (prior && (prior.project_id !== project || (prior.hash && prior.hash !== inspection.hash) || ["finished","failed"].includes(str(prior.state)))) return {ok:false,status:409,error:"sync request id already used"};
    if (!this.lock(project, syncId)) return { ok: false, status: 409, error: `a /dev/sync of project ${project} is already running` };
    try {
      const prepared = await builder.prepare(request.incoming, project);
      if (!prepared.ok) return { ok: false, status: prepared.status, error: prepared.error };
      const summary = prepared.summary;
      this.sql.tx(() => {
        this.sql.run(
          `INSERT INTO gonvex_cf_releases(project_id,hash,summary,state) VALUES($1,$2,$3,'pending')
           ON CONFLICT(project_id,hash) DO UPDATE SET summary=excluded.summary,
             state=CASE WHEN gonvex_cf_releases.state='active' THEN 'active' ELSE 'pending' END,error=''`,
          project,
          summary.hash,
          JSON.stringify(summary),
        );
        this.sql.run("UPDATE gonvex_cf_syncs SET hash=$2,plan_id=$3 WHERE sync_id=$1", syncId, summary.hash, summary.planId ?? "");
      });

      try {
        applyControlMigrations(this.sql, summary.controlMigrations);
      } catch (error) {
        return this.failed(project, summary.hash, 422, (error as Error).message);
      }

      const tenants = this.tenantRoutes(project);
      if (tenants.length > 0) {
        const rollout = await this.rollout(syncId, project, summary, tenants);
        if (!rollout.ok) return this.failed(project, summary.hash, rollout.status, rollout.error);
      }

      // Activation (modules.rs install): the Worker version with every active bundle.
      const previous = this.activeRelease(project);
      const generation = num(this.sql.value("SELECT COALESCE(MAX(generation),0)+1 AS g FROM gonvex_cf_releases WHERE project_id=$1", project));
      const releases = this.activeReleases().filter((release) => release.project !== project);
      releases.push({ project, hash: summary.hash, generation });
      releases.sort((a, b) => (a.project < b.project ? -1 : a.project > b.project ? 1 : 0));
      const activated = await builder.activate(releases);
      if (!activated.ok) return this.failed(project, summary.hash, 422, activated.error);

      // The router deploys outside this object: a real code update may reset it.
      // Persist the publication before handing off, and keep its project lock.
      this.sql.tx(() => {
        this.sql.run(`INSERT INTO gonvex_cf_publications(sync_id,project_id,hash,generation,version,previous_hash,tenants)
          VALUES($1,$2,$3,$4,$5,$6,$7)`, syncId, project, summary.hash, generation, activated.version, previous?.hash ?? null, JSON.stringify(tenants));
        this.sql.run("UPDATE gonvex_cf_syncs SET state='publishing' WHERE sync_id=$1", syncId);
      });
      return {
        ok: true,
        deploy: activated.version,
        publication: syncId,
        body: this.response(project,summary),
      };
    } finally {
      this.unlock(syncId);
    }
  }

  private response(project: string, summary: ReleaseSummary): Record<string, unknown> {
    return {ok:true,project,functionCount:summary.functionCount,schemaDefinition:summary.schemaDefinition ?? {},runtimeReloaded:true};
  }

  /** Idempotent post-deploy publication, safe to resume after a Control reset. */
  publish(syncId: string): Promise<{ ready: boolean }> {
    const existing = this.publications.get(syncId);
    if (existing) return existing;
    const work = this.publishOnce(syncId).finally(() => this.publications.delete(syncId));
    this.publications.set(syncId, work);
    return work;
  }

  private async publishOnce(syncId: string): Promise<{ ready: boolean }> {
    const row = this.sql.get(`SELECT p.*,r.summary,s.state FROM gonvex_cf_publications p JOIN gonvex_cf_releases r
      ON r.project_id=p.project_id AND r.hash=p.hash JOIN gonvex_cf_syncs s ON s.sync_id=p.sync_id WHERE p.sync_id=$1`, syncId);
    if (!row) throw new Error("release publication not found");
    if (num(row.completed)) return { ready: true };
    if (row.state !== "publishing") throw new HttpError(409, "release publication is no longer current");
    if (!this.ports.versionReady(str(row.version))) return { ready: false };
    const project = str(row.project_id), hash = str(row.hash), generation = num(row.generation);
    const tenants = JSON.parse(str(row.tenants)) as string[], summary = JSON.parse(str(row.summary)) as ReleaseSummary;
    if (!await this.ports.activateTenants(project, tenants, hash, generation)) return { ready: false };
    if (this.sql.value("SELECT state FROM gonvex_cf_syncs WHERE sync_id=$1", syncId) !== "publishing") throw new HttpError(409, "release publication was superseded");
    this.activate(project, summary, generation, str(row.version), row.previous_hash == null ? undefined : str(row.previous_hash));
    try { await this.ports.syncCrons(project, summary, tenants); }
    catch (cause) { throw new HttpError(422,(cause as Error).message); }
    await this.ports.moduleReloaded(project, tenants, hash, generation, summary.clientContract?.version ?? null, syncId);
    this.sql.tx(() => {
      this.sql.run("UPDATE gonvex_cf_publications SET completed=1 WHERE sync_id=$1", syncId);
      this.sql.run(`UPDATE gonvex_cf_syncs SET state='finished',finished_at=${NOW_MS} WHERE sync_id=$1`, syncId);
    });
    return { ready: true };
  }

  private failed(project: string, hash: string, status: number, error: string): SyncOutcome {
    this.sql.run("UPDATE gonvex_cf_releases SET state=CASE WHEN state='active' THEN 'active' ELSE 'failed' END,error=$3 WHERE project_id=$1 AND hash=$2", project, hash, error);
    return { ok: false, status, error };
  }

  /** Flips the active release and saves the manifest (`save_runtime_manifest`), one transaction. */
  private activate(project: string, summary: ReleaseSummary, generation: number, version: string, previous: string | undefined): void {
    this.sql.tx(() => {
      if (previous && previous !== summary.hash) this.sql.run("UPDATE gonvex_cf_releases SET state='retired' WHERE project_id=$1 AND hash=$2", project, previous);
      this.sql.run(`UPDATE gonvex_cf_releases SET state='active',generation=$3,activated_at=${NOW_MS},error='' WHERE project_id=$1 AND hash=$2`, project, summary.hash, generation);
      this.sql.run(
        `INSERT INTO gonvex_cf_active_releases(project_id,hash,generation,plan_id,version,activated_at) VALUES($1,$2,$3,$4,$5,${NOW_MS})
         ON CONFLICT(project_id) DO UPDATE SET hash=excluded.hash,generation=excluded.generation,plan_id=excluded.plan_id,version=excluded.version,activated_at=excluded.activated_at`,
        project,
        summary.hash,
        generation,
        summary.planId ?? "",
        version,
      );
      this.sql.run(
        `INSERT INTO gonvex_runtime_manifests(project_id,manifest,module_hash,updated_at) VALUES($1,$2,$3,${NOW_MS})
         ON CONFLICT(project_id) DO UPDATE SET manifest=excluded.manifest,module_hash=excluded.module_hash,updated_at=excluded.updated_at`,
        project,
        JSON.stringify(summary.storedManifest),
        summary.hash,
      );
    });
  }

  // ------------------------------------------------------------- lock (R5)

  private lock(project: string, syncId: string): boolean {
    return this.sql.tx(() => {
      const publishing=this.sql.value<string>("SELECT sync_id FROM gonvex_cf_syncs WHERE project_id=$1 AND state='publishing'",project);
      if (publishing && this.publications.has(publishing)) return false;
      // A lock outlives a reset of this object only until its deadline.
      this.sql.run(`UPDATE gonvex_cf_syncs SET state='abandoned',finished_at=${NOW_MS} WHERE project_id=$1 AND state IN('running','publishing') AND expires_at<=${NOW_MS}`, project);
      if (this.sql.value("SELECT 1 FROM gonvex_cf_syncs WHERE project_id=$1 AND state='publishing'", project) !== undefined) return false;
      const running = this.sql.value("SELECT sync_id FROM gonvex_cf_syncs WHERE project_id=$1 AND state='running'", project);
      if (running !== undefined && this.waiters.has(`lock:${str(running)}`)) return false;
      if (running !== undefined) {
        // Held by a request this object instance no longer serves (it restarted): the request is gone.
        this.sql.run(`UPDATE gonvex_cf_syncs SET state='abandoned',finished_at=${NOW_MS} WHERE sync_id=$1`, running);
      }
      this.sql.run(`INSERT INTO gonvex_cf_syncs(sync_id,project_id,state,expires_at) VALUES($1,$2,'running',${NOW_MS}+$3)
        ON CONFLICT(sync_id) DO UPDATE SET state='running',started_at=${NOW_MS},expires_at=excluded.expires_at,finished_at=NULL`, syncId, project, this.ports.deadlineMs + 60_000);
      this.waiters.set(`lock:${syncId}`, { resolve: () => undefined });
      return true;
    });
  }

  private unlock(syncId: string): void {
    this.waiters.delete(`lock:${syncId}`);
    this.sql.run(`UPDATE gonvex_cf_syncs SET state='finished',finished_at=${NOW_MS} WHERE sync_id=$1 AND state='running'`, syncId);
  }

  /** Projects with a sync in flight in this instance (tests, health). */
  running(): string[] {
    return this.sql.all("SELECT project_id FROM gonvex_cf_syncs WHERE state='running'").map((row) => str(row.project_id));
  }

  // ------------------------------------------------------------- rollout

  private async rollout(syncId: string, project: string, summary: ReleaseSummary, tenants: string[]): Promise<{ ok: true } | { ok: false; status: number; error: string }> {
    const planId = summary.planId;
    if (!planId) return { ok: false, status: 422, error: "release has no tenant plan" };
    this.sql.run("UPDATE gonvex_cf_syncs SET tenants=$2,next_index=0 WHERE sync_id=$1", syncId, JSON.stringify(tenants));
    const done = new Promise<{ ok: true } | { ok: false; error: string }>((resolve) => this.waiters.set(syncId, { resolve }));
    await this.ports.enqueue({ syncId, project, tenant: tenants[0]!, planId, index: 0 });
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<{ ok: false; error: string; timeout: true }>((resolve) => {
      timer = setTimeout(() => resolve({ ok: false, error: `tenant migration rollout did not finish within ${Math.round(this.ports.deadlineMs / 1000)} s`, timeout: true }), this.ports.deadlineMs);
    });
    const result = await Promise.race([done, deadline]);
    clearTimeout(timer);
    this.waiters.delete(syncId);
    if (result.ok) return { ok: true };
    this.sql.run("UPDATE gonvex_cf_syncs SET state='failed',error=$2 WHERE sync_id=$1", syncId, result.error);
    return { ok: false, status: "timeout" in result ? 503 : 422, error: result.error };
  }

  /** The queue consumer's report for one tenant: enqueue the next one, or finish the sync. */
  async report(report: RolloutReport): Promise<{ accepted: boolean }> {
    const row = this.sql.get("SELECT project_id,state,tenants,plan_id,next_index FROM gonvex_cf_syncs WHERE sync_id=$1", report.syncId);
    if (!row || str(row.state) !== "running" || num(row.next_index) !== report.index) return { accepted: false };
    const waiter = this.waiters.get(report.syncId);
    if (report.state === "failed") {
      this.sql.run("UPDATE gonvex_cf_syncs SET state='failed',error=$2 WHERE sync_id=$1", report.syncId, report.error ?? "");
      waiter?.resolve({ ok: false, error: report.error ?? "tenant migration failed" });
      return { accepted: true };
    }
    const tenants = JSON.parse(str(row.tenants)) as string[];
    const next = report.index + 1;
    this.sql.run("UPDATE gonvex_cf_syncs SET next_index=$2 WHERE sync_id=$1", report.syncId, next);
    if (next >= tenants.length) {
      waiter?.resolve({ ok: true });
      return { accepted: true };
    }
    await this.ports.enqueue({ syncId: report.syncId, project: str(row.project_id), tenant: tenants[next]!, planId: str(row.plan_id), index: next });
    return { accepted: true };
  }
}

/** Rust's 422 texts carry the tenant-provision error; the kernel keeps them. */
export function requireActive(release: { planId: string } | null): { planId: string } {
  if (!release) throw new HttpError(422, "install the TypeScript module before creating the first tenant shard");
  return release;
}
