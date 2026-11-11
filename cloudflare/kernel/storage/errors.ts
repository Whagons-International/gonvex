// Error texts the module sees, in the Rust runtime's words.
//
// The Rust host turns a SQLite failure into sqlx's Display text,
// `error returned from database: (code: N) <sqlite3_errmsg>`, where N is the
// extended result code. workerd reports the same failure as
// `<message>[ at offset K]: SQLITE_X[ (extended: SQLITE_X_Y)]`.

/** SQLite result codes by name (sqlite3.h), primary and extended. */
const CODES: Readonly<Record<string, number>> = {
  SQLITE_ERROR: 1, SQLITE_INTERNAL: 2, SQLITE_PERM: 3, SQLITE_ABORT: 4, SQLITE_BUSY: 5, SQLITE_LOCKED: 6,
  SQLITE_NOMEM: 7, SQLITE_READONLY: 8, SQLITE_INTERRUPT: 9, SQLITE_IOERR: 10, SQLITE_CORRUPT: 11,
  SQLITE_NOTFOUND: 12, SQLITE_FULL: 13, SQLITE_CANTOPEN: 14, SQLITE_PROTOCOL: 15, SQLITE_EMPTY: 16,
  SQLITE_SCHEMA: 17, SQLITE_TOOBIG: 18, SQLITE_CONSTRAINT: 19, SQLITE_MISMATCH: 20, SQLITE_MISUSE: 21,
  SQLITE_NOLFS: 22, SQLITE_AUTH: 23, SQLITE_FORMAT: 24, SQLITE_RANGE: 25, SQLITE_NOTADB: 26,
  SQLITE_ERROR_MISSING_COLLSEQ: 257, SQLITE_ERROR_RETRY: 513, SQLITE_ERROR_SNAPSHOT: 769,
  SQLITE_ABORT_ROLLBACK: 516, SQLITE_BUSY_RECOVERY: 261, SQLITE_BUSY_SNAPSHOT: 517, SQLITE_BUSY_TIMEOUT: 773,
  SQLITE_LOCKED_SHAREDCACHE: 262, SQLITE_LOCKED_VTAB: 518,
  SQLITE_READONLY_RECOVERY: 264, SQLITE_READONLY_CANTLOCK: 520, SQLITE_READONLY_ROLLBACK: 776,
  SQLITE_READONLY_DBMOVED: 1032, SQLITE_READONLY_CANTINIT: 1288, SQLITE_READONLY_DIRECTORY: 1544,
  SQLITE_IOERR_READ: 266, SQLITE_IOERR_SHORT_READ: 522, SQLITE_IOERR_WRITE: 778, SQLITE_IOERR_FSYNC: 1034,
  SQLITE_CORRUPT_VTAB: 267, SQLITE_CORRUPT_SEQUENCE: 523, SQLITE_CORRUPT_INDEX: 779,
  SQLITE_CONSTRAINT_CHECK: 275, SQLITE_CONSTRAINT_COMMITHOOK: 531, SQLITE_CONSTRAINT_FOREIGNKEY: 787,
  SQLITE_CONSTRAINT_FUNCTION: 1043, SQLITE_CONSTRAINT_NOTNULL: 1299, SQLITE_CONSTRAINT_PRIMARYKEY: 1555,
  SQLITE_CONSTRAINT_TRIGGER: 1811, SQLITE_CONSTRAINT_UNIQUE: 2067, SQLITE_CONSTRAINT_VTAB: 2323,
  SQLITE_CONSTRAINT_ROWID: 2579, SQLITE_CONSTRAINT_PINNED: 2835, SQLITE_CONSTRAINT_DATATYPE: 3091,
  SQLITE_AUTH_USER: 279,
};

const WORKERD_SQL_ERROR = /^([\s\S]*?)(?: at offset \d+)?: (SQLITE_[A-Z_]+)(?: \(extended: (SQLITE_[A-Z_]+)\))?$/;

export interface SqliteFailure {
  /** The extended result code, when workerd reported one. */
  readonly code: number | undefined;
  /** sqlite3_errmsg, without workerd's offset and code suffix. */
  readonly sqliteMessage: string;
  /** The text the Rust runtime gives the module. */
  readonly message: string;
}

/** Parses a workerd SQL error into the Rust runtime's text. */
export function sqliteFailure(error: unknown): SqliteFailure {
  const text = String((error as { message?: unknown } | null)?.message ?? error);
  const match = WORKERD_SQL_ERROR.exec(text);
  if (!match) return { code: undefined, sqliteMessage: text, message: text };
  const sqliteMessage = match[1]!;
  const code = (match[3] === undefined ? undefined : CODES[match[3]]) ?? CODES[match[2]!];
  if (code === undefined) return { code, sqliteMessage, message: sqliteMessage };
  return { code, sqliteMessage, message: databaseMessage(code, sqliteMessage) };
}

/** sqlx's Display for a database error. */
export function databaseMessage(code: number, sqliteMessage: string): string {
  return `error returned from database: (code: ${code}) ${sqliteMessage}`;
}

/** sqlx `Error::Protocol` Display (Rust's parameter binding errors). */
export function protocolMessage(message: string): string {
  return `encountered unexpected or invalid data: ${message}`;
}

/** host_calls.rs ABORTED: every call after a failed statement, until ROLLBACK TO. */
export const ABORTED = "current transaction is aborted, commands ignored until end of transaction block";

/** What Rust reports when a deferred foreign key fails at COMMIT. */
export const FOREIGN_KEY_FAILED = databaseMessage(787, "FOREIGN KEY constraint failed");

/** What Rust reports when a Query statement writes (read-only connection). */
export const READ_ONLY_WRITE = databaseMessage(8, "attempt to write a readonly database");

export type StorageErrorKind =
  /** Refused before it ran: admission, a Query write, parameters, a platform limit. */
  | "refused"
  /** SQLite failed the statement. */
  | "sql"
  /** A call after an earlier failure, before ROLLBACK TO. */
  | "aborted"
  /** The deferred-FK check found a violation before commit. */
  | "foreign_key"
  /** The invocation transaction is closed. */
  | "closed";

/** A failure the module sees as a host-call error with Rust's text. */
export class StorageError extends Error {
  constructor(
    message: string,
    readonly kind: StorageErrorKind,
    readonly code?: number,
    /** Kernel-side detail for logs; never shown to the module. */
    readonly detail?: string,
  ) {
    super(message);
    this.name = "StorageError";
  }
}
