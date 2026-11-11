// What provisioning needs from change capture (K03, `cloudflare/kernel/capture/`).
// The shapes are the ones agreed with K03; the tenant object passes K03's
// exports in. Rust calls the same two things from provision.rs: `feed::finalize`
// inside each migration's transaction, and `install_change_feed_locked` after
// the migrations.

export interface RawResult {
  columnNames: string[];
  rows: unknown[][];
  rowsRead: number;
  rowsWritten: number;
}

/** K03's SQL handle (the shape of K01's `StorageSession.run`). */
export interface CaptureSql {
  run(sql: string, params?: readonly unknown[]): RawResult;
}

export interface Provenance {
  commandId?: string;
  rootCommandId?: string;
  rootChannel?: string;
  channel?: string;
  actorAccountId?: string;
  actorMemberId?: string;
  onBehalfOfMemberId?: string;
  agentExecutionId?: string;
}

export interface CommitNotice {
  epoch: string;
  revision: number;
  tables: string[];
  changes: number;
  rowsWritten: number;
}

/**
 * One receipt's worth of change-feed DDL, already through A01 T5. `digest` is the
 * SHA-256 of Rust's pre-transform script (so `_gonvex_sync_bootstrap` equals Rust's);
 * `null` deletes the receipt (its statements drop stale triggers).
 */
export interface InstallUnit {
  receipt: string;
  digest: string | null;
  statements: string[];
  kind: "infrastructure" | "table" | "dependency" | "drop";
}

export interface CaptureHooks {
  directSql(sql: SqlStorage): CaptureSql;
  /** feed::finalize: flush record images, assign one revision with provenance. */
  finalize(sql: CaptureSql, provenance: Provenance): CommitNotice | undefined;
  /** install_change_feed_locked, as units: only those whose receipt or installed triggers do not match. */
  planChangeFeedInstall(sql: CaptureSql): InstallUnit[];
  /** Runs a unit's statements and writes (or deletes) its receipt, in the caller's transaction. */
  applyInstallUnit(sql: CaptureSql, unit: InstallUnit): void;
}
