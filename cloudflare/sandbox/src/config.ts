// Sandbox policy (RT/config.rs:108-158, 366-460). The defaults and the
// GONVEX_SANDBOX_* variable names are Rust's. The settings that describe the
// host (worker binary, root directory, chroot uid/gid, DuckDB memory) are the
// Container's and keep Rust's values there (container/manager.mjs).

export interface SandboxConfig {
  /** GONVEX_SANDBOX_ENABLED (default false): "sandbox is disabled by runtime policy". */
  enabled: boolean;
  /** Runs executing at once per tenant (Rust: per process; 0048 §2.3). */
  concurrency: number;
  /** Live sandboxes per (project, tenant, account). */
  maxPerAccount: number;
  /** Live sandboxes in the deployment. */
  maxTotal: number;
  /** Executions per sandbox over its lifetime. */
  maxExecutions: number;
  defaultTtlMs: number;
  maxTtlMs: number;
  defaultTimeoutMs: number;
  maxTimeoutMs: number;
  maxCodeBytes: number;
  maxFileBytes: number;
  maxWorkspaceBytes: number;
  maxOutputBytes: number;
  /** Rows one `duckdb.query` returns before `truncated: true`. */
  maxRows: number;
  /** The V8 heap cap the worker applies to a run. */
  maxHeapBytes: number;
}

export const DEFAULT_SANDBOX_CONFIG: Readonly<SandboxConfig> = Object.freeze({
  enabled: false,
  concurrency: 2,
  maxPerAccount: 4,
  maxTotal: 128,
  maxExecutions: 16,
  defaultTtlMs: 30 * 60_000,
  maxTtlMs: 2 * 60 * 60_000,
  defaultTimeoutMs: 30_000,
  maxTimeoutMs: 120_000,
  maxCodeBytes: 512 << 10,
  maxFileBytes: 64 << 20,
  maxWorkspaceBytes: 256 << 20,
  maxOutputBytes: 8 << 20,
  maxRows: 500,
  maxHeapBytes: 64 << 20,
});

const INTEGER_SETTINGS: ReadonlyArray<[keyof SandboxConfig, string]> = [
  ["concurrency", "GONVEX_SANDBOX_CONCURRENCY"],
  ["maxPerAccount", "GONVEX_SANDBOX_MAX_PER_ACCOUNT"],
  ["maxTotal", "GONVEX_SANDBOX_MAX_TOTAL"],
  ["maxExecutions", "GONVEX_SANDBOX_MAX_EXECUTIONS"],
  ["defaultTtlMs", "GONVEX_SANDBOX_DEFAULT_TTL_MS"],
  ["maxTtlMs", "GONVEX_SANDBOX_MAX_TTL_MS"],
  ["defaultTimeoutMs", "GONVEX_SANDBOX_DEFAULT_TIMEOUT_MS"],
  ["maxTimeoutMs", "GONVEX_SANDBOX_MAX_TIMEOUT_MS"],
  ["maxCodeBytes", "GONVEX_SANDBOX_MAX_CODE_BYTES"],
  ["maxFileBytes", "GONVEX_SANDBOX_MAX_FILE_BYTES"],
  ["maxWorkspaceBytes", "GONVEX_SANDBOX_MAX_WORKSPACE_BYTES"],
  ["maxOutputBytes", "GONVEX_SANDBOX_MAX_OUTPUT_BYTES"],
  ["maxRows", "GONVEX_SANDBOX_MAX_ROWS"],
  ["maxHeapBytes", "GONVEX_SANDBOX_MAX_HEAP_BYTES"],
];

const text = (value: unknown): string | null => {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
};

/** `config.rs` `boolean()`: true/1/yes/on, false/0/no/off, anything else is a configuration error. */
function boolean(name: string, value: unknown, fallback: boolean): boolean {
  const raw = text(value);
  if (raw === null) return fallback;
  switch (raw.toLowerCase()) {
    case "true": case "1": case "yes": case "on": return true;
    case "false": case "0": case "no": case "off": return false;
    default: throw new Error(`${name} must be a boolean, got ${JSON.stringify(raw)}`);
  }
}

function integer(name: string, value: unknown, fallback: number): number {
  const raw = text(value);
  if (raw === null) return fallback;
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(Number(raw))) throw new Error(`${name} must be a non-negative integer, got ${JSON.stringify(raw)}`);
  return Number(raw);
}

/** Reads the policy from Worker vars, with Rust's names and defaults. */
export function sandboxConfigFromEnv(env: Record<string, unknown>): SandboxConfig {
  const config: SandboxConfig = { ...DEFAULT_SANDBOX_CONFIG };
  config.enabled = boolean("GONVEX_SANDBOX_ENABLED", env["GONVEX_SANDBOX_ENABLED"], DEFAULT_SANDBOX_CONFIG.enabled);
  for (const [key, name] of INTEGER_SETTINGS) {
    (config as unknown as Record<string, number>)[key] = integer(name, env[name], DEFAULT_SANDBOX_CONFIG[key] as number);
  }
  return config;
}
