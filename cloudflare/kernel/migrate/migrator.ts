// The per-object step of provisioning and tenant migration rollout (K02).
//
// A tenant object applies a release plan (plan.ts) in the order Rust's
// `provision_tenant_database` does: the runtime prelude (tenant identity,
// reference ledger, schema options, migration ledger), every tenant migration
// not yet in `gonvex_migrations`, then the change-feed install (K03). The work
// is cut into storage transactions of at most ~200 statements (A01 T7) and
// spread over events, so no event approaches the object's CPU limit
// (0022 §3b: cpu_ms 5000, no chunk over 2 s):
//
// - Progress lives in the object's KV storage and commits in the same
//   transaction as each chunk, so an object reset (CPU limit, eviction, deploy)
//   loses at most the chunk in flight. An alarm resumes the work.
// - Before each transactional migration (and before the change-feed install)
//   the object takes a point-in-time bookmark. A migration that fails after
//   some of its chunks committed is rolled back to it, which gives Rust's
//   "the failing migration rolls back, earlier ones stay".
// - A chunk that keeps resetting the object fails the migration after
//   `maxAttempts` tries instead of looping.
//
// The step never runs application code. While a unit is partly applied,
// `gateClosed()` is true and the object must not run Reducers or other writers
// (Rust holds its schema lock for the whole provision).

import type { CaptureHooks, CaptureSql, CommitNotice, InstallUnit } from "./capture-port.ts";
import { unitKey, type PlanManifest, type PlanStore, type PlannedMigration, type PlannedPrelude } from "./plan.ts";

export const PROGRESS_KEY = "gonvex/migrate/progress";
export const APPLIED_PREFIX = "gonvex/migrate/applied/";
/** Holds `_gonvex_record_pending` rows between chunks of one migration (see `stash`). Exists only mid-migration. */
export const RECORD_STASH = "_gonvex_st_migrate_record_stash";

export interface Budget {
  /** Chunks (storage transactions) per event at most. */
  maxChunksPerEvent: number;
  /** Stop starting new chunks once this much wall time has passed in the event. */
  maxMsPerEvent: number;
  /** Change-feed install statements per storage transaction. */
  captureStatementsPerTransaction: number;
  /** Tries of one chunk that end in an object reset before the migration fails. */
  maxAttempts: number;
  /** The watchdog alarm that resumes work after a reset fires this long after an event starts. */
  watchdogMs: number;
  /** Tables per foreign-key-check step of a rebuild migration. */
  foreignKeyTablesPerStep: number;
}

export const DEFAULT_BUDGET: Budget = {
  maxChunksPerEvent: 4,
  maxMsPerEvent: 1500,
  captureStatementsPerTransaction: 200,
  maxAttempts: 3,
  watchdogMs: 10_000,
  foreignKeyTablesPerStep: 100,
};

type Phase = "prelude" | "migrations" | "capture" | "done" | "failed";

/** `restore-point`: written just before a bookmark is taken. Seeing it again means the object was restored to it. */
type UnitState = "pending" | "restore-point" | "running";

export interface Progress {
  planId: string;
  phase: Phase;
  /** Index into the plan's prelude or migrations. */
  unit: number;
  /** Next chunk of the unit. */
  chunk: number;
  unitState: UnitState;
  bookmark: string | null;
  /** Active milliseconds spent on the current migration (its ledger `duration_ms`). */
  unitMs: number;
  /** Uncommitted tries of the current chunk. */
  attempts: number;
  foreignKeysOff: boolean;
  applied: string[];
  revisions: number[];
  events: number;
  activeMs: number;
  startedAt: number;
  updatedAt: number;
  error: string | null;
  /** A restore to `bookmark` is scheduled for the next session; the next event aborts the object. */
  restorePending: boolean;
  /** First receipt of the last change-feed batch: the install must make progress. */
  lastReceipt: string | null;
  /** Foreign-key rebuild: next table to check, or -1 when not checking. */
  fkTable: number;
}

export interface MigrationFailure {
  planId: string;
  unit: string;
  chunk: number;
  error: string;
  restored: boolean;
}

export interface MigrationStatus {
  planId: string | null;
  state: "idle" | "running" | "done" | "failed";
  phase: Phase | null;
  unit: string | null;
  chunk: number;
  gateClosed: boolean;
  applied: string[];
  revisions: number[];
  events: number;
  activeMs: number;
  error: string | null;
}

export interface MigratorOptions {
  storage: DurableObjectStorage;
  plans: PlanStore;
  capture: CaptureHooks;
  /** `ctx.abort`: restarts the object so a scheduled bookmark restore takes effect. */
  abort: (reason: string) => void;
  /**
   * Rust's `reference_integrity::after_migration`, inside each tenant migration's last
   * transaction. Default: a no-op when no reference catalog is declared, otherwise an error
   * (declared catalogs belong to K07; Whagons declares none).
   */
  afterMigration?: (sql: SqlStorage) => void;
  /** K01 `invalidateSchema()`: called after each committed schema change. */
  schemaChanged?: () => void;
  /** After commit, as Rust publishes change-feed notices after provisioning. */
  publish?: (notice: CommitNotice) => void;
  /** Called before a failed migration is rolled back; the failure text does not survive the restore. */
  reportFailure?: (failure: MigrationFailure) => void | Promise<void>;
  budget?: Partial<Budget>;
  /** Called after each committed storage transaction (measurement and logs). */
  observe?: (event: TransactionEvent) => void;
}

export interface TransactionEvent {
  planId: string;
  unit: string;
  chunk: number;
  statements: number;
  /** Wall time of the transaction, including the SQL and the commit bookkeeping. */
  ms: number;
}

class StepFailure extends Error {
  readonly restore: boolean;
  constructor(message: string, restore: boolean) {
    super(message);
    this.restore = restore;
  }
}

const message = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** Rust's `reference_integrity::after_migration` for a database without a declared reference catalog. */
export function refuseDeclaredReferences(sql: SqlStorage): void {
  const one = (q: string) => sql.exec<{ n: number }>(q).one().n;
  const declared =
    one("SELECT count(*) AS n FROM _gonvex_reference_requests") +
    one("SELECT count(*) AS n FROM _gonvex_reference_tables") +
    one("SELECT count(*) AS n FROM _gonvex_reference_pending") +
    one("SELECT count(*) AS n FROM _gonvex_reference_catalog WHERE json_type(declaration,'$.exclusions') IS NOT NULL");
  if (declared > 0) {
    throw new Error("declared reference catalogs (_gonvex_reference_requests) are not supported by the kernel yet (K07); this database declares one");
  }
}

export class TenantMigrator {
  private readonly storage: DurableObjectStorage;
  private readonly sql: SqlStorage;
  private readonly budget: Budget;
  private readonly manifests = new Map<string, PlanManifest>();
  private running: Promise<unknown> = Promise.resolve();
  private sessionForeignKeysOff = false;

  private readonly options: MigratorOptions;

  constructor(options: MigratorOptions) {
    this.options = options;
    this.storage = options.storage;
    this.sql = options.storage.sql;
    this.budget = { ...DEFAULT_BUDGET, ...options.budget };
  }

  private load(): Progress | undefined {
    return this.storage.kv.get<Progress>(PROGRESS_KEY);
  }

  private save(progress: Progress): void {
    progress.updatedAt = Date.now();
    this.storage.kv.put(PROGRESS_KEY, progress);
  }

  /** True while a unit is partly applied: application writes must wait. */
  gateClosed(): boolean {
    const p = this.load();
    if (!p || p.phase === "done") return false;
    if (p.phase === "failed") return p.restorePending;
    return p.unitState === "running" || p.chunk > 0 || p.foreignKeysOff;
  }

  status(): MigrationStatus {
    const p = this.load();
    if (!p) return { planId: null, state: "idle", phase: null, unit: null, chunk: 0, gateClosed: false, applied: [], revisions: [], events: 0, activeMs: 0, error: null };
    const manifest = this.manifests.get(p.planId);
    const unit = !manifest ? null : p.phase === "prelude" ? manifest.prelude[p.unit]?.name ?? null : p.phase === "migrations" ? manifest.migrations[p.unit]?.name ?? null : p.phase === "capture" ? "change-feed" : null;
    return {
      planId: p.planId,
      state: p.phase === "done" ? "done" : p.phase === "failed" ? "failed" : "running",
      phase: p.phase,
      unit,
      chunk: p.chunk,
      gateClosed: this.gateClosed(),
      applied: p.applied,
      revisions: p.revisions,
      events: p.events,
      activeMs: p.activeMs,
      error: p.error,
    };
  }

  /** `gonvex_migrations` rows, in name order (empty before the ledger exists). */
  ledger(): Array<{ name: string; checksum: string; applied_at: number; duration_ms: number }> {
    const exists = this.sql.exec("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='gonvex_migrations'").toArray().length > 0;
    if (!exists) return [];
    return this.sql.exec<{ name: string; checksum: string; applied_at: number; duration_ms: number }>("SELECT name, checksum, applied_at, duration_ms FROM gonvex_migrations ORDER BY name").toArray();
  }

  /**
   * Starts applying `planId`, or continues it, and runs one event's slice of work.
   * The object's alarm continues from there, so callers may also just wait and poll
   * `status()`. Starting a plan while a different one is partly applied is refused.
   */
  async applyRelease(planId: string): Promise<MigrationStatus> {
    return this.serialize(async () => {
      const current = this.load();
      if (!current || current.planId !== planId || current.phase === "done" || (current.phase === "failed" && !current.restorePending)) {
        if (current && current.planId !== planId && current.phase !== "done" && current.phase !== "failed" && this.gateClosed()) {
          throw new Error(`tenant migration ${current.planId} is in progress; a sync of ${planId} must wait for it`);
        }
        await this.manifest(planId);
        const now = Date.now();
        this.save({
          planId, phase: "prelude", unit: 0, chunk: 0, unitState: "pending", bookmark: null, unitMs: 0, attempts: 0,
          foreignKeysOff: false, applied: [], revisions: [], events: 0, activeMs: 0, startedAt: now, updatedAt: now, error: null, restorePending: false, lastReceipt: null, fkTable: -1,
        });
      }
      return this.slice();
    });
  }

  /** The object's alarm handler calls this (it multiplexes the one alarm per object). */
  async alarm(): Promise<MigrationStatus> {
    return this.serialize(() => this.slice());
  }

  /** When the object should be woken for migration work, or null. */
  wantsAlarm(): boolean {
    const p = this.load();
    return !!p && (p.phase === "prelude" || p.phase === "migrations" || p.phase === "capture" || p.restorePending);
  }

  private serialize<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.running.then(fn, fn);
    this.running = next.catch(() => undefined);
    return next;
  }

  private async manifest(planId: string): Promise<PlanManifest> {
    let manifest = this.manifests.get(planId);
    if (!manifest) {
      const loaded = await this.options.plans.manifest(planId);
      if (!loaded) throw new Error(`release plan ${planId} not found`);
      manifest = loaded;
      this.manifests.set(planId, manifest);
    }
    return manifest;
  }

  private async slice(): Promise<MigrationStatus> {
    const p = this.load();
    if (!p || p.phase === "done") return this.status();
    if (p.restorePending) {
      // The bookmark restore takes effect when the object restarts.
      await this.storage.deleteAlarm();
      this.options.abort(`restoring the restore point of failed migration (${p.error ?? "unknown error"})`);
      return this.status();
    }
    if (p.phase === "failed") return this.status();
    if (p.unitState === "restore-point") {
      // Only a restore (or a reset in the instant before the bookmark) brings this state back.
      p.phase = "failed";
      p.unitState = "pending";
      p.chunk = 0;
      p.error = p.error ?? "the migration failed and was rolled back to its restore point; sync again to retry";
      this.save(p);
      await this.storage.deleteAlarm();
      return this.status();
    }
    const manifest = await this.manifest(p.planId);
    const started = Date.now();
    // Watchdog: resumes the work if this event dies (CPU limit, deferred-FK reset, eviction).
    await this.storage.setAlarm(started + this.budget.watchdogMs);
    if (p.foreignKeysOff && !this.sessionForeignKeysOff) {
      // A new session starts with foreign keys on; a rebuild in progress needs them off again.
      await this.foreignKeys(false);
    }
    p.events += 1;
    this.save(p);
    let transactions = 0;
    let retryAt = 0;
    try {
      while (transactions < this.budget.maxChunksPerEvent && Date.now() - started < this.budget.maxMsPerEvent) {
        const progressed = await this.step(manifest);
        if (progressed === "done") break;
        if (progressed === "transaction") transactions += 1;
      }
    } catch (error) {
      if (error instanceof StepFailure) await this.failed(error, manifest);
      else {
        // Not a SQL failure (a plan fetch, a storage call): nothing was applied, so retry later.
        // `attempts` still bounds a step that never gets through.
        const p = this.load()!;
        p.error = `retrying: ${message(error)}`;
        this.save(p);
        retryAt = Date.now() + 1000;
      }
    }
    const after = this.load()!;
    after.activeMs += Date.now() - started;
    if (after.phase !== "failed" && retryAt === 0 && after.error?.startsWith("retrying: ")) after.error = null;
    this.save(after);
    if (after.phase === "done" || (after.phase === "failed" && !after.restorePending)) await this.storage.deleteAlarm();
    else await this.storage.setAlarm(retryAt || Date.now());
    return this.status();
  }

  /** One unit of progress: a skip, a transaction, or the end of the plan. */
  private async step(manifest: PlanManifest): Promise<"skip" | "transaction" | "done"> {
    const p = this.load()!;
    switch (p.phase) {
      case "prelude": {
        const unit = manifest.prelude[p.unit];
        if (!unit) return this.advancePhase(p, "migrations");
        return this.prelude(p, unit, manifest);
      }
      case "migrations": {
        const unit = manifest.migrations[p.unit];
        if (!unit) return this.advancePhase(p, "capture");
        return this.migration(p, unit, manifest);
      }
      case "capture":
        return this.changeFeed(p);
      default:
        return "done";
    }
  }

  private advancePhase(p: Progress, phase: Phase): "skip" {
    p.phase = phase;
    p.unit = 0;
    p.chunk = 0;
    p.unitState = "pending";
    p.bookmark = null;
    this.save(p);
    return "skip";
  }

  private nextUnit(p: Progress): void {
    p.fkTable = -1;
    p.unit += 1;
    p.chunk = 0;
    p.unitMs = 0;
    p.attempts = 0;
    p.unitState = "pending";
    p.bookmark = null;
  }

  /** Records a try of the next transaction durably, so a reset during it is counted. */
  private async attempt(p: Progress, what: string): Promise<void> {
    if (p.attempts >= this.budget.maxAttempts) {
      throw new StepFailure(`${what} reset the object ${p.attempts} times without committing (CPU limit or a deferred foreign-key violation at commit)`, true);
    }
    p.attempts += 1;
    this.save(p);
    await this.storage.sync();
  }

  /** Takes the restore point for the current unit (see `UnitState`). */
  private async restorePoint(p: Progress): Promise<void> {
    p.unitState = "restore-point";
    this.save(p);
    await this.storage.sync();
    const bookmark = await this.storage.getCurrentBookmark();
    p.unitState = "running";
    p.bookmark = bookmark;
    this.save(p);
  }

  /**
   * `PRAGMA foreign_keys` is a no-op inside a transaction, and an event's storage writes
   * open one. So: commit what is pending first, then switch, then check it took effect.
   */
  private async foreignKeys(on: boolean): Promise<void> {
    await this.storage.sync();
    this.sql.exec(`PRAGMA foreign_keys=${on ? "ON" : "OFF"}`);
    const now = this.sql.exec<{ foreign_keys: number }>("PRAGMA foreign_keys").one().foreign_keys;
    if (now !== (on ? 1 : 0)) throw new StepFailure(`could not turn foreign keys ${on ? "on" : "off"} for a foreign-key rebuild`, false);
    this.sessionForeignKeysOff = !on;
  }

  private run(statements: readonly string[]): void {
    for (const statement of statements) this.sql.exec(statement);
  }

  private async prelude(p: Progress, unit: PlannedPrelude, manifest: PlanManifest): Promise<"transaction"> {
    if (unit.name === "tenant-identity" && p.chunk === 0) {
      // provision.rs install_tenant_identity: refuse a pre-identity-v2 members table.
      const legacy = this.sql.exec("SELECT 1 FROM pragma_table_info('members') WHERE name='user_id'").toArray().length > 0;
      if (legacy) throw new StepFailure("identity-v2 migration required: tenant members still use user_id", false);
    }
    await this.attempt(p, `prelude ${unit.name} chunk ${p.chunk}`);
    const statements = await this.options.plans.chunk(manifest.id, unitKey(unit), p.chunk);
    const chunk = p.chunk;
    const t0 = Date.now();
    try {
      this.storage.transactionSync(() => {
        this.run(statements);
        p.chunk += 1;
        p.attempts = 0;
        if (p.chunk >= unit.chunks) this.nextUnit(p);
        this.save(p);
      });
    } catch (error) {
      throw new StepFailure(message(error), false);
    }
    this.options.observe?.({ planId: manifest.id, unit: unitKey(unit), chunk, statements: statements.length, ms: Date.now() - t0 });
    this.options.schemaChanged?.();
    return "transaction";
  }

  private async migration(p: Progress, m: PlannedMigration, manifest: PlanManifest): Promise<"skip" | "transaction"> {
    if (p.chunk === 0 && p.unitState === "pending") {
      const recorded = this.sql.exec<{ checksum: string }>("SELECT checksum FROM gonvex_migrations WHERE name=?", m.name).toArray()[0];
      if (recorded) {
        if (recorded.checksum !== m.checksum) {
          throw new StepFailure(`migration ${m.name} checksum mismatch: database has ${recorded.checksum}, artifact has ${m.checksum}`, false);
        }
        this.nextUnit(p);
        this.save(p);
        return "skip";
      }
      if (m.refusal) throw new StepFailure(m.refusal, false);
      // No-transaction migrations keep Rust's semantics: earlier statements stay after a failure.
      if (!m.noTransaction) await this.restorePoint(p);
      else p.unitState = "running";
      if (m.foreignKeyRebuild) {
        p.foreignKeysOff = true;
        this.save(p);
        await this.foreignKeys(false);
      }
    }
    // A foreign-key rebuild ends with the check and then the ledger, in steps of their own
    // (chunk == chunks: check tables; then finish). Foreign keys stay off and the gate closed.
    if (m.foreignKeyRebuild && p.chunk >= m.chunks) {
      if (p.fkTable >= 0) {
        await this.attempt(p, `migration ${m.name} foreign-key check from table ${p.fkTable}`);
        return this.foreignKeyCheck(p, m);
      }
      return this.finish(p, m, manifest, []);
    }
    const last = p.chunk === m.chunks - 1 && !m.foreignKeyRebuild;
    await this.attempt(p, `migration ${m.name} chunk ${p.chunk + 1}/${m.chunks}`);
    const statements = await this.options.plans.chunk(manifest.id, unitKey(m), p.chunk);
    if (last) return this.finish(p, m, manifest, statements);
    const started = Date.now();
    const chunk = p.chunk;
    try {
      if (m.noTransaction) this.run(statements);
      this.storage.transactionSync(() => {
        if (!m.noTransaction) {
          this.unstash();
          this.run(statements);
          this.stash();
        }
        p.unitMs += Date.now() - started;
        p.chunk += 1;
        p.attempts = 0;
        if (m.foreignKeyRebuild && p.chunk === m.chunks) p.fkTable = 0;
        this.save(p);
      });
    } catch (error) {
      // A failure in the first chunk changed nothing; later ones roll back to the restore point.
      throw new StepFailure(message(error), !m.noTransaction && chunk > 0);
    }
    this.options.observe?.({ planId: manifest.id, unit: unitKey(m), chunk, statements: statements.length, ms: Date.now() - started });
    this.options.schemaChanged?.();
    return "transaction";
  }

  /**
   * Rust's `pragma_foreign_key_check LIMIT 1` over the whole database, one table at a time
   * and spread over events: the whole-database form runs SQLite out of memory on the Whagons
   * schema in workerd, which breaks the object's output gate and resets it. A violation fails
   * the migration with Rust's text and rolls it back to its restore point.
   */
  private foreignKeyCheck(p: Progress, m: PlannedMigration): "transaction" {
    const tables = this.sql
      .exec<{ name: string }>("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND sql NOT LIKE 'CREATE VIRTUAL%' ORDER BY rowid")
      .toArray()
      .map((r) => r.name);
    const started = Date.now();
    let checked = 0;
    while (p.fkTable < tables.length && checked < this.budget.foreignKeyTablesPerStep && Date.now() - started < this.budget.maxMsPerEvent) {
      const table = tables[p.fkTable]!;
      const invalid = this.sql.exec<{ table: string; parent: string; fkid: number }>('SELECT "table",rowid,parent,fkid FROM pragma_foreign_key_check(?) LIMIT 1', table).toArray()[0];
      if (invalid) throw new StepFailure(`foreign-key rebuild rejected: ${invalid.table} -> ${invalid.parent} (constraint ${invalid.fkid})`, true);
      p.fkTable += 1;
      checked += 1;
    }
    if (p.fkTable >= tables.length) p.fkTable = -1;
    p.attempts = 0;
    p.unitMs += Date.now() - started;
    this.save(p);
    this.options.observe?.({ planId: p.planId, unit: `fk-check/${m.name}`, chunk: p.fkTable, statements: checked, ms: Date.now() - started });
    return "transaction";
  }

  /** The migration's last transaction: its last statements, then Rust's after_migration, finalize and ledger row. */
  private async finish(p: Progress, m: PlannedMigration, manifest: PlanManifest, statements: string[]): Promise<"transaction"> {
    if (m.foreignKeyRebuild) await this.attempt(p, `migration ${m.name} ledger`);
    const started = Date.now();
    let notice: CommitNotice | undefined;
    try {
      // `-- gonvex:no-transaction`: each statement on its own, the ledger row only after all of them.
      if (m.noTransaction) this.run(statements);
      this.storage.transactionSync(() => {
        if (!m.noTransaction) {
          this.unstash();
          this.run(statements);
        }
        p.unitMs += Date.now() - started;
        notice = this.finishMigration(m, p.unitMs, manifest);
        p.applied.push(m.name);
        if (notice) p.revisions.push(notice.revision);
        this.nextUnit(p);
        this.save(p);
      });
    } catch (error) {
      if (error instanceof StepFailure) throw error;
      throw new StepFailure(message(error), !m.noTransaction && (p.chunk > 0 || m.foreignKeyRebuild));
    }
    this.options.observe?.({ planId: manifest.id, unit: unitKey(m), chunk: m.chunks - 1, statements: statements.length, ms: Date.now() - started });
    if (m.foreignKeyRebuild) {
      await this.foreignKeys(true);
      const after = this.load()!;
      after.foreignKeysOff = false;
      this.save(after);
    }
    this.options.schemaChanged?.();
    if (notice) this.options.publish?.(notice);
    return "transaction";
  }

  /** The end of one migration's transaction, in Rust's order: after_migration, finalize, ledger row (the FK check ran before). */
  private finishMigration(m: PlannedMigration, durationMs: number, manifest: PlanManifest): CommitNotice | undefined {
    (this.options.afterMigration ?? refuseDeclaredReferences)(this.sql);
    const notice = this.options.capture.finalize(this.options.capture.directSql(this.sql), { commandId: `migration:${m.name}`, channel: "system" });
    this.sql.exec("INSERT INTO gonvex_migrations(name,checksum,duration_ms) VALUES(?,?,?)", m.name, m.checksum, Math.round(durationMs));
    // 0014 rule 1: the transform version is recorded in the ledger metadata, outside Rust's table.
    this.storage.kv.put(APPLIED_PREFIX + m.name, { planId: manifest.id, transformVersion: manifest.transformVersion, chunks: m.chunks, statements: m.statements });
    return notice;
  }

  /**
   * Record capture stages first-before images in `_gonvex_record_pending`, whose deferred
   * guard FK fails any commit that did not finalize (and a failed deferred FK resets the
   * object). Between chunks of one migration the rows wait in a stash table, and the next
   * chunk puts them back, so the migration still flushes once, with the first images.
   */
  private stash(): void {
    const pending = this.sql.exec("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_record_pending'").toArray().length > 0;
    if (!pending || this.sql.exec("SELECT 1 FROM _gonvex_record_pending LIMIT 1").toArray().length === 0) return;
    this.sql.exec(`CREATE TABLE ${RECORD_STASH} AS SELECT table_name,row_id,old_value FROM _gonvex_record_pending`);
    this.sql.exec("DELETE FROM _gonvex_record_pending");
  }

  private unstash(): void {
    if (this.sql.exec("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", RECORD_STASH).toArray().length === 0) return;
    this.sql.exec(`INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT table_name,row_id,old_value FROM ${RECORD_STASH}`);
    this.sql.exec(`DROP TABLE ${RECORD_STASH}`);
  }

  private async changeFeed(p: Progress): Promise<"transaction" | "done"> {
    if (p.unitState === "pending") await this.restorePoint(p);
    const capture = this.options.capture;
    const direct = capture.directSql(this.sql);
    let units: InstallUnit[];
    try {
      units = capture.planChangeFeedInstall(direct);
    } catch (error) {
      throw new StepFailure(message(error), true);
    }
    if (units.length === 0) {
      p.phase = "done";
      p.unitState = "pending";
      p.bookmark = null;
      this.save(p);
      return "done";
    }
    if (units[0]!.receipt === p.lastReceipt) {
      throw new StepFailure(`change-feed install does not converge: receipt ${units[0]!.receipt} still differs after it was installed`, true);
    }
    const batch: InstallUnit[] = [];
    let statements = 0;
    for (const unit of units) {
      if (batch.length && statements + unit.statements.length > this.budget.captureStatementsPerTransaction) break;
      batch.push(unit);
      statements += unit.statements.length;
    }
    await this.attempt(p, `change-feed install (${batch.length} receipts, ${statements} statements)`);
    const t0 = Date.now();
    try {
      this.storage.transactionSync(() => {
        for (const unit of batch) capture.applyInstallUnit(direct, unit);
        p.lastReceipt = batch[0]!.receipt;
        p.chunk += 1;
        p.attempts = 0;
        this.save(p);
      });
    } catch (error) {
      throw new StepFailure(message(error), true);
    }
    this.options.observe?.({ planId: p.planId, unit: "change-feed", chunk: p.chunk - 1, statements, ms: Date.now() - t0 });
    this.options.schemaChanged?.();
    return "transaction";
  }

  private async failed(error: StepFailure, manifest: PlanManifest): Promise<void> {
    const p = this.load()!;
    const restore = error.restore;
    const text = message(error);
    const unit = p.phase === "prelude" ? `prelude ${manifest.prelude[p.unit]?.name}` : p.phase === "migrations" ? `migration ${manifest.migrations[p.unit]?.name}` : "change-feed install";
    const canRestore = restore && p.bookmark !== null;
    await this.options.reportFailure?.({ planId: p.planId, unit, chunk: p.chunk, error: text, restored: canRestore });
    if (p.foreignKeysOff) {
      await this.foreignKeys(true);
      p.foreignKeysOff = false;
    }
    p.phase = "failed";
    p.error = `${unit}: ${text}`;
    let restoring = false;
    if (canRestore) {
      try {
        await this.storage.onNextSessionRestoreBookmark(p.bookmark!);
        restoring = true;
      } catch (error) {
        // Local workerd has no point-in-time recovery. The object stays partly migrated and says so.
        p.error += `; the restore point could not be scheduled (${message(error)}), so the chunks already committed remain`;
      }
    }
    if (restoring) {
      p.restorePending = true;
    } else {
      p.chunk = 0;
      p.unitState = "pending";
      p.bookmark = null;
    }
    this.save(p);
  }
}

/** A `CaptureSql` over a Durable Object's SqlStorage, for code that has no K01 session. */
export function directSql(sql: SqlStorage): CaptureSql {
  return {
    run(query, params = []) {
      const cursor = sql.exec(query, ...params);
      const rows = [...cursor.raw()] as unknown[][];
      return { columnNames: cursor.columnNames, rows, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
    },
  };
}
