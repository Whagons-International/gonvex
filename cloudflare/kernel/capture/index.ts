// Kernel change capture (package K03). See README.md.

export { CaptureState, finalize, type CommitNotice, type FinalizeOptions, type FinalizeStats } from "./finalize.ts";
export { changeCommandId, transactionRow, type Provenance, type TransactionRow } from "./provenance.ts";
export {
  applyInstallUnit,
  bootstrapDigest,
  CaptureInstallError,
  INFRASTRUCTURE_SQL,
  installChangeFeed,
  planChangeFeedInstall,
  type InstallResult,
  type InstallUnit,
  type PlanOptions,
} from "./install.ts";
export { SCHEMA_OPTIONS_SQL, SYNC_INFRASTRUCTURE_SQL } from "./infrastructure.ts";
export { DELETE_NO_OP_SQL, keepRowidReference, plainCaptureStatements, ROW_IMAGE_SPLIT_ASSIGNMENTS, ROW_IMAGE_SPLIT_SQL, type ImageCatalog } from "./images.ts";
export { PRUNE_INTERVAL_MS, PRUNE_PAGE_ROWS, pruneChangeLog, pruneDue, RETENTION_MS, type PruneOptions, type PruneResult, type PruneStorage } from "./prune.ts";
export { ChangeWake, type ChangeListener, type ChangeWakeup } from "./wake.ts";
export { invokeCaptured, type CaptureContext, type CapturedResult, type CapturedSession, type Invoker } from "./invoke.ts";
export {
  captureTriggerNames,
  dependencyTriggerNames,
  entityCaptureSql,
  ownedCaptureSql,
  recordCaptureSql,
  triggerStatements,
  type RecordOwner,
} from "./triggers.ts";
export { counters, directSql, type CaptureSql, type RawResult, type RowCounters, type RowReporter, type WriteOperation } from "./sql.ts";
export { sha256Hex } from "./digest.ts";
