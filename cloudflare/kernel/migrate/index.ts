// Public interface of migrations and provisioning (package K02).
// K19's rollout calls `buildReleasePlan` once per release (outside any tenant
// object), stores the plan, and sends each tenant object `applyRelease(planId)`.
// A tenant object imports `./migrator.ts` and `./plan.ts` only, so its bundle
// does not carry the transform (`build.ts`).

export { MigrationFileError, migrationsFromFiles, parseMigration, sha256Hex, validMigrationName, type MigrationScope, type SqlMigration } from "./migrations.ts";
export { buildReleasePlan, PLAN_FORMAT, planId, PRELUDE } from "./build.ts";
export {
  manifestOf,
  MemoryPlanStore,
  R2PlanStore,
  unitKey,
  type PlanManifest,
  type PlannedMigration,
  type PlannedPrelude,
  type PlannedUnit,
  type PlanStore,
  type R2Like,
  type ReleasePlan,
} from "./plan.ts";
export {
  APPLIED_PREFIX,
  DEFAULT_BUDGET,
  directSql,
  PROGRESS_KEY,
  RECORD_STASH,
  refuseDeclaredReferences,
  TenantMigrator,
  type Budget,
  type MigrationFailure,
  type MigrationStatus,
  type MigratorOptions,
  type Progress,
  type TransactionEvent,
} from "./migrator.ts";
export type { CaptureHooks, CaptureSql, CommitNotice, InstallUnit, Provenance, RawResult } from "./capture-port.ts";
export { LEDGER_SQL, NOW_MS, REFERENCE_SCHEMA_SQL, SCHEMA_OPTIONS_SQL, TENANT_IDENTITY_SQL } from "./runtime-sql.ts";
