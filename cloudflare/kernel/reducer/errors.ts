// Reducer failures with Rust's client text and class.
//
// `ExecutionError` (rust/crates/runtime/src/execution.rs:32-76) Display texts,
// `DatabaseError` (rust/crates/store/src/lib.rs:60-110) for the variants this
// path raises, and the classification of rust/crates/runtime/src/error_class.rs.
// `retryable` is true only for `transient` and `unauthenticated` (proto:100-147).

export type ReducerErrorClass = "rejected" | "transient" | "update_required" | "unauthenticated";

/** What a failed call puts in `reducer.error` (or `query.error` for the receipt query). */
export interface ReducerFailure {
  readonly message: string;
  readonly class: ReducerErrorClass;
}

export const failure = (message: string, errorClass: ReducerErrorClass): ReducerFailure => ({ message, class: errorClass });

export const retryable = (errorClass: ReducerErrorClass): boolean => errorClass === "transient" || errorClass === "unauthenticated";

/**
 * Rust's `{:?}` for a `str` (`<str as Debug>::fmt`): quotes, escapes `\\`, `"`,
 * `\t`, `\r`, `\n`, `\0`, other control characters as `\u{..}`; printable
 * Unicode stays as is.
 */
export function rustDebug(value: string): string {
  let out = '"';
  for (const char of value) {
    const code = char.codePointAt(0)!;
    if (char === '"') out += '\\"';
    else if (char === "\\") out += "\\\\";
    else if (char === "\n") out += "\\n";
    else if (char === "\r") out += "\\r";
    else if (char === "\t") out += "\\t";
    else if (code === 0) out += "\\0";
    else if (code < 0x20 || code === 0x7f || (code >= 0x80 && code < 0xa0)) out += `\\u{${code.toString(16)}}`;
    else out += char;
  }
  return out + '"';
}

/** The texts of the failures the Reducer path raises itself (before or after the module host). */
export const errors = {
  /** ExecutionError::ModuleMissing: transient (error_class.rs:120). */
  moduleMissing: (project: string) => failure(`project ${rustDebug(project)} has no active TypeScript module`, "transient"),
  functionMissing: (path: string) => failure(`function ${rustDebug(path)} is not registered`, "rejected"),
  wrongKind: (path: string, expected: string, actual: string) => failure(`function ${rustDebug(path)} is a ${actual}, not a ${expected}`, "rejected"),
  internalFunction: (path: string) => failure(`internal function ${rustDebug(path)} cannot be called by a client`, "rejected"),
  /** ExecutionError::HostCall. */
  hostCall: (message: string, transient = false) => failure(`module database capability failed: ${message}`, transient ? "transient" : "rejected"),
  invalidArguments: (path: string, message: string) => failure(`function ${rustDebug(path)} arguments do not match its schema: ${message}`, "rejected"),
  staleArtifact: (expected: string, active: string) => failure(`STALE_REDUCER_ARTIFACT: expected ${rustDebug(expected)}, active artifact is ${rustDebug(active)}`, "update_required"),
  clientUpdateRequired: (expected: bigint | number, active: number) => failure(`CLIENT_UPDATE_REQUIRED: expected contract ${expected}, active contract is ${active}`, "update_required"),
  /** DatabaseError::IdempotencyPathMismatch: rejected. */
  pathMismatch: (key: string, storedPath: string, requestedPath: string) =>
    failure(`idempotency key ${rustDebug(key)} was already used by reducer ${rustDebug(storedPath)}; refusing to replay it for ${rustDebug(requestedPath)}`, "rejected"),
  /** DatabaseError::AmbiguousReducerReceipts: transient. */
  ambiguousReceipts: () => failure("ambiguous current and legacy receipt history requires recovery", "transient"),
  /** DatabaseError::MemberNotFound: rejected (the account lost access). */
  memberNotFound: (accountId: string) => failure(`active tenant member for account ${rustDebug(accountId)} not found`, "rejected"),
  /** 0009 rule 3 (CONTRACT A.5): a key that hits an expired receipt. */
  receiptExpired: () => failure("Reducer receipt expired", "rejected"),
  /**
   * 0028 §3 (K02 note 0027 #3): an application write while a migration unit is
   * partly applied. Rust's writers wait on the schema lock; the kernel cannot
   * hold a call across migration events, so it refuses with Rust's transient class.
   */
  migrationInProgress: () => failure("module database capability failed: tenant schema migration in progress; retry", "transient"),
  /** lib.rs:5045 */
  unauthenticated: () => failure("authenticate with an active tenant before calling a Reducer", "unauthenticated"),
} as const;

/** Rust's receipt-query texts (execution.rs:143-245). */
export const receiptQueryErrors = {
  anonymous: () => errors.hostCall("receipt lookup requires an authenticated tenant"),
  missingArgs: () => errors.invalidArguments("__gonvex.reducerReceipt", "idempotencyKey and receiptPath are required"),
  clock: () => errors.hostCall("receipt clock cannot be represented by the SDK"),
} as const;
