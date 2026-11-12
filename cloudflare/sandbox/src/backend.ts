// The one interface between `ctx.sandbox` and whatever runs the code.
//
// `SandboxService` (service.ts) owns the contract: the payload checks, Rust's
// texts and their order, the policy and the storage read for `importFile`.
// A backend owns the workspaces: their files, their TTL, the executions and
// the analysis tables. K20 ships `ContainerBackend` (Rust's sandbox worker
// with DuckDB in a Container, 0048 §2.1). The L09 candidate
// (cloudflare/spikes/l09-sandbox, note 0047 §4) implements the same interface.
import type { SandboxConfig } from "./config";

/** Rust's `Scope`: a sandbox belongs to one account in one tenant of one project. */
export interface SandboxScope {
  project: string;
  tenant: string;
  account: string;
}

export interface SandboxHandle {
  sandboxId: string;
  expiresAt: number;
  duckdb: boolean;
}

export type ExecutionStatus = "running" | "succeeded" | "failed" | "cancelled";

/** `execution_json`. */
export interface SandboxExecution {
  sandboxId: string;
  executionId: string;
  status: ExecutionStatus;
  startedAt: number;
  finishedAt: number | null;
  result: unknown;
  error: string;
  logs: unknown;
}

export interface ImportedTable {
  tableName: string;
  rowCount: number;
  columns: string[];
}

export interface ImportResult {
  alias: string;
  tables: ImportedTable[];
}

/** Each call has passed `exact_fields` and the required-string checks already. */
export interface SandboxBackend {
  create(scope: SandboxScope, request: { ttlMs: number; duckdb: boolean }, config: SandboxConfig): Promise<SandboxHandle>;
  destroy(scope: SandboxScope, sandboxId: string, config: SandboxConfig): Promise<{ sandboxId: string; destroyed: true }>;
  run(scope: SandboxScope, request: { sandboxId: string; code: string; timeoutMs: number }, config: SandboxConfig): Promise<SandboxExecution>;
  cancel(scope: SandboxScope, sandboxId: string, executionId: string, config: SandboxConfig): Promise<SandboxExecution>;
  status(scope: SandboxScope, sandboxId: string, executionId: string, config: SandboxConfig): Promise<SandboxExecution>;
  readFile(scope: SandboxScope, sandboxId: string, path: string, config: SandboxConfig): Promise<Uint8Array>;
  writeFile(scope: SandboxScope, sandboxId: string, path: string, content: Uint8Array, config: SandboxConfig): Promise<number>;
  importFile(scope: SandboxScope, request: { sandboxId: string; filename: string; body: ReadableStream<Uint8Array>; size: number }, config: SandboxConfig): Promise<ImportResult>;
}
