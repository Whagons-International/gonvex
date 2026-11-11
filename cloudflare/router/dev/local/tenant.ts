/**
 * The local kernel's tenant object: K01 storage, K02 migrations, K03 capture
 * and K04 generations behind the operator port (control/operator/tenant/port.ts).
 *
 * It is the assembly `gonvex dev --once` and the dashboard run against
 * locally, and the fixture of K19's tests. The tenant object that serves
 * sockets, Reducers and replicas is composed by the wave-2 packages (K07,
 * K08, K10); it implements the same `OperatorTenantPort` by delegating to
 * `TenantOperator`, as this one does.
 *
 * Its module comes from the Worker version (0016 G0-2): `gonvex:releases` is
 * the module the release builder generates with every project's active
 * bundle; this object installs its project's release into a K04
 * `ModuleRegistry` on first use, which evaluates the app bundle with K04's
 * prelude and validates every function contract.
 */
import { DurableObject } from "cloudflare:workers";
// @ts-expect-error resolved by the build: wrangler `alias` (local kernel) or the vitest alias (tests).
import * as generated from "gonvex:releases";
import { applyInstallUnit, CaptureState, ChangeWake, directSql, finalize, planChangeFeedInstall } from "../../../kernel/capture/index.ts";
import { ModuleRegistry, type FunctionContract } from "../../../kernel/host/src/index.ts";
import type { CaptureHooks } from "../../../kernel/migrate/capture-port.ts";
import { TenantMigrator, type MigrationStatus } from "../../../kernel/migrate/migrator.ts";
import { R2PlanStore } from "../../../kernel/migrate/plan.ts";
import { TenantStorage } from "../../../kernel/storage/session.ts";
import type { ActiveMemberRow, OperatorDataSink, OperatorTenantPort, PortResult, ReplaceResult, RowsPage, RowsQuery, ServedModule, TableSummary } from "../../../control/operator/tenant/port.ts";
import { TenantOperator } from "../../../control/operator/tenant/operator.ts";

interface BakedRelease {
  project: string;
  hash: string;
  generation: number;
  clientContract: number | null;
  functions: Record<string, { kind: "query" | "reducer" | "action"; internal: boolean; args: unknown; result: unknown }>;
  exports: Record<string, unknown>;
}

const releases = ((generated as { releases?: unknown }).releases ?? {}) as Record<string, BakedRelease>;

export interface LocalTenantEnv {
  RELEASES: R2Bucket;
  CONTROL: DurableObjectNamespace;
}

const PROJECT_KEY = "k19/project";
const CONTROL_NAME = "control";

export class LocalKernelTenant extends DurableObject<LocalTenantEnv> implements OperatorTenantPort {
  private readonly storageLayer: TenantStorage;
  private readonly capture = new CaptureState();
  private readonly wake = new ChangeWake();
  private readonly migrator: TenantMigrator;
  private readonly operator: TenantOperator;
  private registry: ModuleRegistry | undefined;
  private served: ServedModule | null = null;
  private notifyScheduled = false;

  constructor(state: DurableObjectState, env: LocalTenantEnv) {
    super(state, env);
    this.storageLayer = new TenantStorage(state.storage);
    this.migrator = new TenantMigrator({
      storage: state.storage,
      plans: new R2PlanStore(env.RELEASES),
      // K03 exports, as K02 agreed them (the shapes match structurally; K02 types them loosely).
      capture: { directSql, finalize, planChangeFeedInstall, applyInstallUnit } as unknown as CaptureHooks,
      abort: (reason) => state.abort(reason),
      schemaChanged: () => {
        this.storageLayer.invalidateSchema();
        this.capture.invalidate();
      },
      publish: (notice) => this.wake.publish(notice),
    });
    this.operator = new TenantOperator({ storage: state.storage, tenantStorage: this.storageLayer, capture: this.capture, wake: this.wake, migrator: this.migrator });
    // dashboard.rs re-reads data subscriptions on the tenant change feed.
    this.wake.subscribe(() => this.scheduleDataChanged());
  }

  private project(): string | undefined {
    return this.ctx.storage.kv.get<string>(PROJECT_KEY);
  }

  private scheduleDataChanged(): void {
    const project = this.project();
    if (!project || this.notifyScheduled) return;
    this.notifyScheduled = true;
    const tenant = this.ctx.id.name ?? "";
    this.ctx.waitUntil(
      (async () => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        this.notifyScheduled = false;
        const control = this.env.CONTROL.getByName(CONTROL_NAME) as unknown as OperatorDataSink;
        await control.operatorDataChanged(project, tenant).catch(() => undefined);
      })(),
    );
  }

  override async alarm(): Promise<void> {
    if (this.migrator.wantsAlarm()) await this.migrator.alarm();
  }

  // ------------------------------------------------------------ rollout

  async operatorApplyRelease(planId: string, projectId?: string): Promise<MigrationStatus> {
    if (projectId && this.project() !== projectId) this.ctx.storage.kv.put(PROJECT_KEY, projectId);
    return this.operator.applyRelease(planId);
  }

  async operatorMigrationStatus(): Promise<MigrationStatus> {
    return this.migrator.status();
  }

  async operatorLedger() {
    return this.migrator.ledger();
  }

  /** Installs the project's baked release into the K04 registry (once per object lifetime). */
  async operatorServedModule(projectId: string): Promise<ServedModule | null> {
    if (this.served?.projectId === projectId) return this.served;
    const release = releases[projectId];
    if (!release) return null;
    const started = Date.now();
    let error: string | null = null;
    let count = 0;
    try {
      const registry = new ModuleRegistry(projectId);
      const functions: FunctionContract[] = Object.entries(release.functions).map(([path, f]) => ({
        path,
        kind: f.kind,
        internal: f.internal,
        argsSchema: f.args ?? { kind: "any" },
        resultSchema: f.result ?? { kind: "any" },
        // Actions are not in the Query/Reducer bundle; they run in the Action Worker (K12).
        ...(f.kind === "action" ? { export: "__k19_action" } : {}),
      }));
      count = functions.length;
      registry.install({
        moduleId: projectId,
        generation: release.generation,
        artifactHash: release.hash,
        clientContract: release.clientContract ?? undefined,
        functions,
        exports: { ...release.exports, __k19_action: () => null },
      });
      this.registry = registry;
    } catch (cause) {
      error = (cause as { message?: string }).message ?? String(cause);
    }
    this.served = { projectId, hash: release.hash, generation: release.generation, functions: count, loadMs: Date.now() - started, error };
    return this.served;
  }

  async operatorModuleReloaded(event: { projectId: string; hash: string; generation: number; clientContract: number | null }): Promise<void> {
    if (this.project() !== event.projectId) this.ctx.storage.kv.put(PROJECT_KEY, event.projectId);
    // In stage 1 the new Worker version restarts or wakes this object with the
    // new bundle; sockets (K05 gateways) see the new artifactHash on re-auth.
    // A registry from an older version is dropped so the next use reloads it.
    if (this.served && this.served.hash !== event.hash) {
      this.served = null;
      this.registry = undefined;
    }
  }

  async operatorDeleteAll(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
  }

  // ------------------------------------------------------------ data

  async operatorListTables(): Promise<PortResult<TableSummary[]>> {
    return this.operator.listTables();
  }
  async operatorReadRows(table: string, query: RowsQuery): Promise<PortResult<RowsPage>> {
    return this.operator.readRows(table, query);
  }
  async operatorInsertRow(table: string, payload: Record<string, unknown>): Promise<PortResult<unknown>> {
    return this.operator.insertRow(table, payload);
  }
  async operatorUpdateRow(table: string, rowId: string, payload: Record<string, unknown>): Promise<PortResult<unknown>> {
    return this.operator.updateRow(table, rowId, payload);
  }
  async operatorDeleteRow(table: string, rowId: string): Promise<PortResult<unknown>> {
    return this.operator.deleteRow(table, rowId);
  }
  async operatorReplaceReferences(replacements: Record<string, string>, dryRun: boolean): Promise<PortResult<ReplaceResult>> {
    return this.operator.replaceReferences(replacements, dryRun);
  }
  async operatorActiveMembers(): Promise<PortResult<ActiveMemberRow[]>> {
    return this.operator.activeMembers();
  }
  async operatorRevokeMember(memberId: string): Promise<PortResult<boolean>> {
    return this.operator.revokeMember(memberId);
  }

  /** Tests: raw SQL on the object (never routed to the public). */
  async debugQuery(sql: string, ...params: SqlStorageValue[]): Promise<Record<string, SqlStorageValue>[]> {
    return this.ctx.storage.sql.exec(sql, ...params).toArray();
  }
}
