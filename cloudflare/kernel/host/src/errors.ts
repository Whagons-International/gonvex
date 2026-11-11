// Error vocabulary across the module boundary (CONTRACT.md B.5).
//
// Texts and codes are contract: modules and tests match on them, and the
// first line of a failed call's `error` is compared exactly (0015 S0-1).

/** `ModuleError` (MR/lib.rs:521-539) with Rust's Display prefixes. */
export type ModuleErrorKind =
  | "FunctionNotFound"
  | "WrongFunctionKind"
  | "InvalidArtifact"
  | "BudgetExceeded"
  | "Unsupported"
  | "InvalidArguments"
  | "InvalidResult"
  | "Execution";

const display: Record<ModuleErrorKind, (detail: string) => string> = {
  FunctionNotFound: (d) => `module function ${d} is not registered`,
  WrongFunctionKind: (d) => `module function ${d} has the wrong kind`,
  InvalidArtifact: (d) => `module artifact is invalid: ${d}`,
  BudgetExceeded: (d) => `module execution exceeded its budget: ${d}`,
  Unsupported: (d) => `module engine is not available: ${d}`,
  InvalidArguments: (d) => `module arguments are invalid: ${d}`,
  InvalidResult: (d) => `module result is invalid: ${d}`,
  Execution: (d) => `module execution failed: ${d}`,
};

/** Wire codes (MH/protocol.rs:29-76). */
export const codes = {
  BAD_REQUEST: "bad_request",
  UNSUPPORTED: "unsupported",
  FRAME_TOO_LARGE: "frame_too_large",
  INVALID_ARTIFACT: "invalid_artifact",
  ARTIFACT_HASH_MISMATCH: "artifact_hash_mismatch",
  MODULE_LOAD_FAILED: "module_load_failed",
  GENERATION_CONFLICT: "generation_conflict",
  UNKNOWN_GENERATION: "unknown_generation",
  MODULE_NOT_LOADED: "module_not_loaded",
  FUNCTION_NOT_FOUND: "function_not_found",
  WRONG_FUNCTION_KIND: "wrong_function_kind",
  INVALID_ARGS: "invalid_args",
  INVALID_RESULT: "invalid_result",
  BUDGET_EXCEEDED: "budget_exceeded",
  EXECUTION_FAILED: "execution_failed",
  CANCELLED: "cancelled",
  SHUTTING_DOWN: "shutting_down",
  HOST_CALL_FAILED: "host_call_failed",
} as const;
export type WireCode = (typeof codes)[keyof typeof codes];

const wireCodeOf: Record<ModuleErrorKind, WireCode> = {
  FunctionNotFound: codes.FUNCTION_NOT_FOUND,
  WrongFunctionKind: codes.WRONG_FUNCTION_KIND,
  InvalidArtifact: codes.INVALID_ARTIFACT,
  BudgetExceeded: codes.BUDGET_EXCEEDED,
  Unsupported: codes.UNSUPPORTED,
  InvalidArguments: codes.INVALID_ARGS,
  InvalidResult: codes.INVALID_RESULT,
  Execution: codes.EXECUTION_FAILED,
};

export class ModuleError extends Error {
  readonly kind: ModuleErrorKind;
  readonly detail: string;
  constructor(kind: ModuleErrorKind, detail: string) {
    super(display[kind](detail));
    this.name = "ModuleError";
    this.kind = kind;
    this.detail = detail;
  }

  /** `WireError` for this failure (MH/session.rs module_error). */
  wire(moduleId: string, functionPath: string): WireError {
    const where = functionPath ? `module ${moduleId} function ${functionPath}` : `module ${moduleId}`;
    const message =
      this.kind === "FunctionNotFound" ? `${where}: ${this.detail} is not registered`
      : this.kind === "WrongFunctionKind" ? `${where}: ${this.detail} has a different kind`
      : `${where}: ${this.detail}`;
    return { code: wireCodeOf[this.kind], message, retryable: false };
  }
}

export interface WireError {
  code: WireCode;
  message: string;
  retryable: boolean;
}

/** `retryable:true` codes (MH/session.rs:415-425,671-693). */
export const retryableCodes: ReadonlySet<WireCode> = new Set([codes.SHUTTING_DOWN, codes.UNKNOWN_GENERATION, codes.MODULE_NOT_LOADED]);

export function wireError(code: WireCode, message: string): WireError {
  return { code, message, retryable: retryableCodes.has(code) };
}

/** `reducer.error.class` (rust/crates/protocol/src/lib.rs:107-147). */
export type ErrorClass = "rejected" | "transient" | "update_required" | "unauthenticated";

const transientCodes: ReadonlySet<WireCode> = new Set([
  codes.CANCELLED, codes.SHUTTING_DOWN, codes.BUDGET_EXCEEDED, codes.MODULE_NOT_LOADED, codes.UNKNOWN_GENERATION,
  codes.GENERATION_CONFLICT, codes.MODULE_LOAD_FAILED, codes.INVALID_ARTIFACT, codes.ARTIFACT_HASH_MISMATCH, codes.HOST_CALL_FAILED,
]);

/** RT/error_class.rs: remote wire code classification. */
export function classifyWire(error: WireError): ErrorClass {
  return error.retryable || transientCodes.has(error.code) ? "transient" : "rejected";
}

/** SQLite primary result codes that are transient (`code & 0xff`, RT/error_class.rs). */
const transientSqlite = new Set([5, 6, 7, 9, 10, 13, 14, 15]);
export function classifySqliteCode(code: number): ErrorClass {
  return transientSqlite.has(code & 0xff) ? "transient" : "rejected";
}

/**
 * Platform failures (B.5 closing paragraph): DO overload, storage busy and
 * an object reset are transient; a CPU-limit kill is `budget_exceeded`
 * (transient); DO SQLite limit errors are rejected with a clear message.
 */
export function classifyPlatformError(error: unknown): { code: WireCode; message: string; class: ErrorClass } {
  const message = String((error as { message?: unknown } | null)?.message ?? error);
  if (/exceeded its CPU time limit|CPU time limit exceeded|exceeded CPU/i.test(message)) {
    return { code: codes.BUDGET_EXCEEDED, message, class: "transient" };
  }
  if (/overloaded|reset because its code was updated|Durable Object reset|storage operation exceeded timeout|SQLITE_BUSY|Network connection lost|broken\./i.test(message)) {
    return { code: codes.HOST_CALL_FAILED, message, class: "transient" };
  }
  return { code: codes.EXECUTION_FAILED, message, class: "rejected" };
}

/** `HostError` (MR/lib.rs:437-443). */
export const hostErrors = {
  capabilityDenied: (capability: string) => `capability ${capability} is not available to this invocation`,
  failed: (message: string) => `host call failed: ${message}`,
  /** In-process and socket transports wrap every host failure the same way (MH/session.rs:158-161, RT/in_process.rs:321-323). */
  wrapped: (capability: string, message: string) => `${capability} host call failed [${codes.HOST_CALL_FAILED}]: ${message}`,
};

/** Rust's sqlx text: "error returned from database: (code: N) <sqlite message>". */
const sqliteCodes: Record<string, number> = {
  SQLITE_ERROR: 1, SQLITE_INTERNAL: 2, SQLITE_PERM: 3, SQLITE_ABORT: 4, SQLITE_BUSY: 5, SQLITE_LOCKED: 6, SQLITE_NOMEM: 7,
  SQLITE_READONLY: 8, SQLITE_INTERRUPT: 9, SQLITE_IOERR: 10, SQLITE_CORRUPT: 11, SQLITE_FULL: 13, SQLITE_CANTOPEN: 14,
  SQLITE_PROTOCOL: 15, SQLITE_SCHEMA: 17, SQLITE_TOOBIG: 18, SQLITE_CONSTRAINT: 19, SQLITE_MISMATCH: 20, SQLITE_AUTH: 23,
  SQLITE_RANGE: 25,
  SQLITE_CONSTRAINT_CHECK: 275, SQLITE_CONSTRAINT_COMMITHOOK: 531, SQLITE_CONSTRAINT_FOREIGNKEY: 787,
  SQLITE_CONSTRAINT_FUNCTION: 1043, SQLITE_CONSTRAINT_NOTNULL: 1299, SQLITE_CONSTRAINT_PRIMARYKEY: 1555,
  SQLITE_CONSTRAINT_TRIGGER: 1811, SQLITE_CONSTRAINT_UNIQUE: 2067, SQLITE_CONSTRAINT_VTAB: 2323, SQLITE_CONSTRAINT_ROWID: 2579,
};

export interface SqlFailure {
  /** The text a module sees. */
  message: string;
  /** Extended SQLite result code when known. */
  code: number | undefined;
}

/** workerd: "<message>: SQLITE_X (extended: SQLITE_X_Y)" -> Rust's sqlx text. */
export function rustSqlMessage(error: unknown): SqlFailure {
  const text = String((error as { message?: unknown } | null)?.message ?? error);
  const match = /^([\s\S]*): (SQLITE_[A-Z_]+)(?: \(extended: (SQLITE_[A-Z_]+)\))?$/.exec(text);
  if (!match) return { message: text, code: undefined };
  const code = (match[3] !== undefined ? sqliteCodes[match[3]] : undefined) ?? sqliteCodes[match[2] ?? ""];
  return code === undefined ? { message: match[1] ?? text, code: undefined } : { message: `error returned from database: (code: ${code}) ${match[1]}`, code };
}
