/// <reference path="../host/src/shims.d.ts" />
// One tenant object's kernel, composed: K01 storage (mixed FK tracking), K03
// capture, K02 migrations, K04 module host, and the K07 Reducer path. The
// Durable Object (`object.ts`) is a thin shell around it; other packages plug
// in where the options say (K08 Queries, K10 delivery, K11 outbox drain).
//
// Every write to the tenant database goes through `serial` (K01 runs one
// invocation at a time, and a migration chunk must not interleave with a
// Reducer's open storage transaction).

import { applyInstallUnit, CaptureState, ChangeWake, directSql, finalize, planChangeFeedInstall, type CommitNotice } from "../capture/index.ts";
import type { CaptureHooks } from "../migrate/capture-port.ts";
import { TenantMigrator, type MigrationStatus, type PlanStore } from "../migrate/index.ts";
import { ModuleRegistry, TenantModuleHost, type ModuleExports } from "../host/src/index.js";
import type { FileMetadataSource } from "../host/src/database-host";
import type { TenantCall } from "../host/src/invoke";
import type { ClientFrame } from "../session/decode";
import { frames as sessionFrames } from "../session/protocol";
import type { SocketView } from "../session/ports";
import type { CaptureSql } from "../capture/sql.ts";
import { moduleArtifact, projectModuleFromManifest, type ProjectModule } from "./module.ts";
import { ReducerPath, type CommittedCall } from "./path.ts";
import { PermissionBits } from "./permissions.ts";
import { pruneReceipts, RECEIPT_PRUNE_INTERVAL_MS, RECEIPT_QUERY, type ReceiptPruneResult } from "./receipts.ts";
import { ReducerStorage } from "./storage.ts";
import { afterMigration, flushReferences } from "./reference/integrity.ts";

/** Frames other packages own (K08 Queries, K10 replicas, K12 Actions, K14 presence) get this until they land. */
export const NOT_IMPLEMENTED = "not implemented by the kernel tenant object yet";

export interface KernelTenantOptions {
  readonly ctx: DurableObjectState;
  /** The Worker version's bundle for an artifact hash (0016 G0-2), or undefined. */
  readonly bundle: (artifactHash: string) => ModuleExports | undefined;
  /** Release plans (K02): built once per release outside the object. */
  readonly plans: PlanStore;
  readonly files?: (() => FileMetadataSource | undefined) | undefined;
  /** A Worker swap may carry a newer manifest than the previous object's KV. */
  readonly restoreManifest?: ((stored: StoredModule) => StoredModule) | undefined;
  readonly tenantOnly?: boolean | undefined;
  readonly beforeInvoke?: ((call: TenantCall) => void | Promise<void>) | undefined;
  readonly afterInvoke?: ((call: TenantCall) => void) | undefined;
  /**
   * Declared reference integrity: flush before finalize, and K02's
   * `after_migration`. Default: the port of reference_integrity.rs
   * (`reference/`); a no-op on databases that declare nothing.
   */
  readonly references?: {
    flush(sql: CaptureSql): void;
    afterMigration(sql: SqlStorage): void;
  };
  /** After every committed Reducer call (outbox wake for K11, delivery for K10). */
  readonly onCommit?: (call: CommittedCall) => void;
  /** The session's own Member changed in its call's commit (deliver the discard to its gateway). */
  readonly ownMembershipChanged?: (socket: SocketView, revision: number) => void;
  readonly subscriptionFloor?: (socket: SocketView) => number | undefined;
  readonly clock?: () => number;
}

/** What the object persists about its active module (the manifest without code). */
export interface StoredModule {
  readonly projectId: string;
  readonly manifest: Record<string, unknown>;
}

export const MODULE_KEY = "gonvex/reducer/module";
export const RECEIPT_PRUNE_KEY = "gonvex/reducer/receipt-prune";

/** The manifest without the bundle's code (kept in the object; the code is in the Worker version). */
export function storedManifest(manifest: Record<string, unknown>): Record<string, unknown> {
  const module = { ...((manifest.module ?? {}) as Record<string, unknown>) };
  delete module.javascript;
  const files = (module.files ?? {}) as Record<string, string>;
  module.files = Object.fromEntries(Object.entries(files).filter(([name]) => name === "client-contract.json" || name.startsWith("migrations/")));
  const { project: _project, ...rest } = manifest;
  return { ...rest, module };
}

export class KernelTenant {
  readonly capture = { state: new CaptureState(), wake: new ChangeWake() };
  readonly storage: ReducerStorage;
  readonly permissions = new PermissionBits(undefined);
  readonly migrator: TenantMigrator;
  readonly reducers: ReducerPath;
  private registry: ModuleRegistry | undefined;
  private host: TenantModuleHost | undefined;
  private active: ProjectModule | undefined;
  private generations = 0;
  private reported: { generation: number; tables: Set<string> } | undefined;

  constructor(private readonly options: KernelTenantOptions) {
    const ctx = options.ctx;
    this.storage = new ReducerStorage(ctx.storage, this.capture, {
      writeTracking: "mixed",
      rowReported: (table) => this.rowReported(table),
    });
    const state = this.capture.state;
    const hooks: CaptureHooks = {
      directSql: (sql) => directSql(sql) as unknown as ReturnType<CaptureHooks["directSql"]>,
      finalize: (sql, provenance) => finalize(sql as CaptureSql, provenance, { state }) as ReturnType<CaptureHooks["finalize"]>,
      planChangeFeedInstall: (sql) => planChangeFeedInstall(sql as CaptureSql) as ReturnType<CaptureHooks["planChangeFeedInstall"]>,
      applyInstallUnit: (sql, unit) => applyInstallUnit(sql as CaptureSql, unit as Parameters<typeof applyInstallUnit>[1]),
    };
    const references = options.references ?? {
      flush: (sql: CaptureSql) => { flushReferences(sql); },
      // Inside a migration's last transaction; one declaration is atomic as a nested storage transaction (Rust's savepoint).
      afterMigration: (sql: SqlStorage) => afterMigration(directSql(sql), { atomic: (fn) => ctx.storage.transactionSync(fn) }),
    };
    this.migrator = new TenantMigrator({
      storage: ctx.storage,
      plans: options.plans,
      capture: hooks,
      abort: (reason) => ctx.abort(reason),
      schemaChanged: () => this.storage.invalidateSchema(),
      publish: (notice) => this.capture.wake.publish(notice as unknown as CommitNotice),
      afterMigration: (sql: SqlStorage) => references.afterMigration(sql),
    });
    this.reducers = new ReducerPath({
      storage: this.storage,
      direct: ctx.storage,
      host: { run: (call) => this.runHost(call) } as TenantModuleHost,
      module: () => this.active,
      permissions: this.permissions,
      gateClosed: () => this.migrator.gateClosed(),
      flushReferences: references.flush,
      ...(options.clock ? { clock: options.clock } : {}),
      ...(options.onCommit ? { onCommit: options.onCommit } : {}),
      ...(options.ownMembershipChanged ? { ownMembershipChanged: options.ownMembershipChanged } : {}),
      ...(options.subscriptionFloor ? { subscriptionFloor: options.subscriptionFloor } : {}),
    });
  }

  get module(): ProjectModule | undefined { return this.active; }

  private requireHost(): TenantModuleHost {
    if (!this.host) throw new Error("no module generation is installed");
    return this.host;
  }

  /**
   * Mixed FK tracking (0028 §2d): capture reports the rows of captured entity
   * tables that have rowids (K03 `reportRows`). Read once per schema generation.
   */
  private rowReported(table: string): boolean {
    const generation = this.storage.catalog.generation;
    if (this.reported?.generation !== generation) {
      const rows = this.options.ctx.storage.sql.exec<{ name: string; ddl: string | null }>(
        `SELECT t.name AS name, t.sql AS ddl FROM sqlite_schema t
         WHERE t.type = 'table' AND EXISTS (SELECT 1 FROM sqlite_schema g WHERE g.type = 'trigger' AND g.tbl_name = t.name
           AND g.name = '_gonvex_sync_' || t.name || '_insert' AND g.sql LIKE '%AFTER INSERT%')`,
      ).toArray();
      const tables = new Set(rows.filter((row) => !/\bWITHOUT\s+ROWID\b/i.test(row.ddl ?? "")).map((row) => row.name.toLowerCase()));
      this.reported = { generation, tables };
    }
    return this.reported.tables.has(table);
  }

  // ---- module generations ------------------------------------------------------------

  /** Restores the active module after a wake. */
  restore(): void {
    const previous = this.options.ctx.storage.kv.get<StoredModule>(MODULE_KEY);
    const stored = previous && (this.options.restoreManifest?.(previous) ?? previous);
    if (stored) {
      this.installCode(stored.projectId, stored.manifest);
      if (stored !== previous) this.options.ctx.storage.kv.put(MODULE_KEY, stored);
    }
  }

  /**
   * `/dev/sync` for this tenant: migrate to the release's plan, then activate
   * the generation (Rust's order: migrate, install, activate; 0012 R5).
   * `planId` names the release plan in the plan store (null: no migrations).
   */
  async activate(projectId: string, manifest: Record<string, unknown>, planId: string | null): Promise<MigrationStatus | null> {
    return this.reducers.serial(async () => {
      let status: MigrationStatus | null = null;
      if (planId) {
        status = await this.migrator.applyRelease(planId);
        while (status.state === "running") status = await this.migrator.alarm();
        if (status.state === "failed") throw new Error(status.error ?? "tenant migration failed");
      }
      const stored = storedManifest(manifest);
      this.installCode(projectId, stored);
      this.options.ctx.storage.kv.put(MODULE_KEY, { projectId, manifest: stored } satisfies StoredModule);
      return status;
    });
  }

  private installCode(projectId: string, manifest: Record<string, unknown>): void {
    const module = projectModuleFromManifest(projectId, manifest);
    if (this.active?.artifactHash === module.artifactHash && this.active.projectId === projectId) return;
    const exports = this.options.bundle(module.artifactHash);
    if (!exports) throw new Error(`module bundle ${module.artifactHash} is not part of this Worker version`);
    if (!this.registry || this.registry.moduleId !== projectId) {
      this.registry = new ModuleRegistry(projectId);
      const files = this.options.files?.();
      this.host = new TenantModuleHost({ storage: this.storage, registry: this.registry, permissions: this.permissions, ...(files ? { files } : {}) });
    }
    const artifact = moduleArtifact(module, ++this.generations, exports);
    this.registry.install(this.options.tenantOnly ? { ...artifact, functions: artifact.functions.filter((fn) => fn.kind !== "action") } : artifact);
    this.permissions.setSource(module.permissionGrants);
    this.active = module;
  }

  recordOverrun(commandId: string, path?: string): void { this.requireHost().quarantine.add(commandId, path, this.active?.artifactHash); }

  private async runHost(call: TenantCall) {
    await this.options.beforeInvoke?.(call);
    try { return await this.requireHost().run(call); } finally { this.options.afterInvoke?.(call); }
  }
  invoke(call: TenantCall) { return this.runHost(call); }

  // ---- frames ------------------------------------------------------------------------

  /** Tenant-scoped frames of a socket with a tenant session (K05 `TenantPort.dispatch`). */
  async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    switch (frame.type) {
      case "reducer.call": return this.reducers.reducerCall(socket, frame);
      case "reducer.callMany": return this.reducers.reducerCallMany(socket, frame.calls);
      case "query.call":
        if (frame.path === RECEIPT_QUERY) return this.reducers.receiptQuery(socket, frame.id, frame.args);
        return [sessionFrames.queryError(frame.id, frame.path, NOT_IMPLEMENTED)];
      case "query.subscribe": return [sessionFrames.queryError(frame.id, frame.path, NOT_IMPLEMENTED)];
      case "action.call": return [sessionFrames.actionError(frame.id, frame.path, NOT_IMPLEMENTED, frame.trace)];
      case "replica.open": return [sessionFrames.replicaError(frame.id, frame.path, NOT_IMPLEMENTED)];
      case "replica.openMany": return frame.opens.map((o) => sessionFrames.replicaError(o.id, o.path, NOT_IMPLEMENTED));
      case "presence.subscribe": return [sessionFrames.presenceError(frame.id, NOT_IMPLEMENTED)];
      default: return [];
    }
  }

  // ---- alarm work ------------------------------------------------------------------------

  /** 0009 rule 5: receipt expiry on the tenant alarm, in pages, never inside a Reducer. */
  async pruneReceipts(now: number, options: { maxPages?: number; force?: boolean; offlineMaxAgeMs?: number | null } = {}): Promise<ReceiptPruneResult | null> {
    const kv = this.options.ctx.storage.kv;
    const last = kv.get<number>(RECEIPT_PRUNE_KEY);
    if (!options.force && last !== undefined && now - last < RECEIPT_PRUNE_INTERVAL_MS) return null;
    // `offlineMaxAgeMs` overrides the module's window (test seam only).
    const window = options.offlineMaxAgeMs !== undefined ? options.offlineMaxAgeMs : this.active?.offlineMaxAgeMs ?? null;
    const storage = this.options.ctx.storage;
    // Each page is its own storage transaction, queued behind Reducers like any write.
    const result = await pruneReceipts(
      { sql: storage.sql, transactionSync: (fn) => storage.transactionSync(fn) },
      {
        offlineMaxAgeMs: window, now,
        ...(options.maxPages !== undefined ? { maxPages: options.maxPages } : {}),
        runPage: (page) => this.reducers.serial(async () => page()),
      },
    );
    if (result.done) kv.put(RECEIPT_PRUNE_KEY, now);
    return result;
  }
}
