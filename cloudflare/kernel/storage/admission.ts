// Admission for module-provided SQL, ported from
// rust/crates/runtime/src/sql_admission.rs and the read-only rules of
// host_calls.rs. Admitting exactly one statement is a security boundary:
// Durable Object `sql.exec` runs every statement in a string (it returns the
// last one's rows), so `SELECT 1; DELETE FROM tasks` must never reach it.

import { headWord, isSymbol, isWord, SqlLexError, splitTopLevel, tokenize, closeParen, type Token } from "./lexer";

export class AdmissionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AdmissionError";
  }
}

export interface AdmittedStatement {
  /** The trimmed statement text; token ranges index into it. */
  readonly sql: string;
  /** Tokens of the one statement, without leading or trailing `;`. */
  readonly tokens: readonly Token[];
  /** First token lowercased, "(" for a parenthesis, "" for anything else (a quoted identifier). */
  readonly firstWord: string;
}

/** Splits tokens into statements the way sql_admission.rs counts them. */
function statements(tokens: readonly Token[]): Token[][] {
  const out: Token[][] = [];
  let current: Token[] | undefined;
  for (const found of tokens) {
    if (isSymbol(found, ";")) {
      if (current) out.push(current);
      current = undefined;
      continue;
    }
    (current ??= []).push(found);
  }
  if (current) out.push(current);
  return out;
}

function requireHead(tokens: readonly Token[]): void {
  const [first, second, third] = [headWord(tokens, 0), headWord(tokens, 1), headWord(tokens, 2)];
  let control = false;
  if (first === "commit" || first === "end" || first === "abort" || first === "begin" || first === "start") control = true;
  else if (first === "rollback") control = !(second === "to" || ((second === "work" || second === "transaction") && third === "to"));
  else if (first === "prepare") control = second === "transaction" && third !== "as" && third !== "(";
  if (control) throw new AdmissionError("transaction boundaries belong to the invocation; raw transaction control is not allowed");
  if (first === "attach" || first === "detach" || first === "pragma" || first === "vacuum") {
    throw new AdmissionError(`${first.toUpperCase()} statements are not allowed in module SQL`);
  }
}

/**
 * Admits exactly one statement that does not control the invocation
 * transaction or reach outside the tenant database (ATTACH, DETACH, PRAGMA,
 * VACUUM). Error texts are Rust's.
 */
export function admitStatement(statement: string): AdmittedStatement {
  const sql = statement.trim();
  if (!sql) throw new AdmissionError("a database statement is required");
  let tokens: Token[];
  try {
    tokens = tokenize(sql);
  } catch (error) {
    if (error instanceof SqlLexError) throw new AdmissionError(error.message);
    throw error;
  }
  const found = statements(tokens);
  if (found.length > 1) throw new AdmissionError("a database statement may not contain more than one statement");
  const body = found[0] ?? [];
  requireHead(body);
  return { sql, tokens: body, firstWord: headWord(body, 0) };
}

const READ_HEADS = new Set(["select", "with", "values", "explain"]);

/** host_calls.rs `require_read_head`: a Query statement must start with a read keyword. */
export function requireReadHead(firstWord: string): void {
  if (!READ_HEADS.has(firstWord)) {
    throw new AdmissionError(`a Query may only read; ${JSON.stringify(firstWord)} is not a read statement`);
  }
}

/** Index of the statement's main verb: past a `WITH [RECURSIVE] name [(…)] AS [NOT] [MATERIALIZED] (…), …` prefix. */
export function mainVerbIndex(tokens: readonly Token[]): number {
  if (!isWord(tokens[0], "with")) return 0;
  let index = 1;
  if (isWord(tokens[index], "recursive")) index++;
  for (;;) {
    const name = tokens[index];
    if (!name || (name.type !== "word" && name.type !== "id" && name.type !== "string")) return -1;
    index++;
    if (isSymbol(tokens[index], "(")) {
      const end = closeParen(tokens, index);
      if (end < 0) return -1;
      index = end + 1;
    }
    if (!isWord(tokens[index], "as")) return -1;
    index++;
    if (isWord(tokens[index], "not")) index++;
    if (isWord(tokens[index], "materialized")) index++;
    const end = closeParen(tokens, index);
    if (end < 0) return -1;
    index = end + 1;
    if (isSymbol(tokens[index], ",")) { index++; continue; }
    return index;
  }
}

const WRITE_VERBS = new Set(["insert", "replace", "update", "delete"]);

/**
 * True when a statement admitted under a read head still writes: SQLite
 * admits `WITH … DELETE/INSERT/UPDATE`. In Rust a Query runs on a read-only
 * connection, so such a statement fails with SQLITE_READONLY before it
 * changes anything; the kernel runs Queries on the writer and refuses them
 * here (contract-changes 0011 §2).
 */
export function readStatementWrites(tokens: readonly Token[]): boolean {
  const head = headWord(tokens, 0);
  if (head === "with") {
    const verb = mainVerbIndex(tokens);
    return verb >= 0 && WRITE_VERBS.has(headWord(tokens, verb));
  }
  return false;
}

export type SavepointCommand =
  | { readonly command: "savepoint"; readonly name: string }
  | { readonly command: "release"; readonly name: string }
  | { readonly command: "rollback"; readonly name: string };

const nameOf = (found: Token | undefined): string | undefined =>
  found && (found.type === "word" || found.type === "id" || found.type === "string") ? found.value : undefined;

/**
 * `SAVEPOINT name`, `RELEASE [SAVEPOINT] name` and `ROLLBACK [TRANSACTION|WORK]
 * TO [SAVEPOINT] name`. Durable Object SQL refuses these; the storage layer
 * maps them onto nested storage transactions (0006, 0016 G0-3). A statement
 * with that head but another shape is a syntax error, as in SQLite.
 */
export function savepointCommand(tokens: readonly Token[]): SavepointCommand | undefined {
  const head = headWord(tokens, 0);
  let index: number;
  let command: SavepointCommand["command"];
  if (head === "savepoint") { command = "savepoint"; index = 1; }
  else if (head === "release") { command = "release"; index = isWord(tokens[1], "savepoint") ? 2 : 1; }
  else if (head === "rollback") {
    index = 1;
    if (isWord(tokens[index], "transaction") || isWord(tokens[index], "work")) index++;
    if (!isWord(tokens[index], "to")) return undefined;
    index++;
    if (isWord(tokens[index], "savepoint")) index++;
    command = "rollback";
  } else return undefined;
  const name = nameOf(tokens[index]);
  const extra = tokens[index + 1];
  if (name === undefined || extra !== undefined) {
    const near = extra ?? tokens[index];
    throw new SavepointSyntaxError(near ? `near "${near.text}": syntax error` : "incomplete input");
  }
  return { command, name };
}

export class SavepointSyntaxError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SavepointSyntaxError";
  }
}

/**
 * The table a single-table write statement targets, for decoding RETURNING
 * rows by its declared types (host_calls.rs `write_target`: only a simple
 * `INSERT INTO t`, `DELETE FROM t` or `UPDATE t` head; a `WITH` prefix or
 * `REPLACE INTO` gives none). Leading comments are already gone.
 */
export function writeTarget(tokens: readonly Token[]): string | undefined {
  const first = headWord(tokens, 0);
  let index: number;
  if (first === "insert") {
    if (!isWord(tokens[1], "into")) return undefined;
    index = 2;
  } else if (first === "delete") {
    if (!isWord(tokens[1], "from")) return undefined;
    index = 2;
  } else if (first === "update") index = 1;
  else return undefined;
  return qualifiedName(tokens, index);
}

/** `name`, `"name"` or `schema.name` at `index`, with quotes removed. */
function qualifiedName(tokens: readonly Token[], index: number): string | undefined {
  const first = tokens[index];
  if (!first || (first.type !== "word" && first.type !== "id" && first.type !== "string")) return undefined;
  if (isSymbol(tokens[index + 1], ".")) {
    const second = tokens[index + 2];
    if (!second || (second.type !== "word" && second.type !== "id" && second.type !== "string")) return undefined;
    return `${first.value}.${second.value}`;
  }
  return first.value;
}

export type WriteOperation = "insert" | "update" | "delete";

export interface WrittenTable {
  readonly table: string;
  readonly operations: ReadonlySet<WriteOperation>;
}

/**
 * The table and operations of any write statement, `WITH` prefix, `INSERT OR
 * …`, `REPLACE`, upserts and `UPDATE OR …` included. Used to track what an
 * invocation wrote for the deferred-FK check. `REPLACE` and `INSERT OR
 * REPLACE` may delete conflicting rows; an upsert may update.
 */
export function writtenTable(tokens: readonly Token[]): WrittenTable | undefined {
  const verb = mainVerbIndex(tokens);
  if (verb < 0) return undefined;
  const head = headWord(tokens, verb);
  let index = verb + 1;
  const operations = new Set<WriteOperation>();
  if (head === "insert" || head === "replace") {
    operations.add("insert");
    if (head === "replace") operations.add("delete");
    if (isWord(tokens[index], "or")) {
      if (isWord(tokens[index + 1], "replace")) operations.add("delete");
      index += 2;
    }
    if (!isWord(tokens[index], "into")) return undefined;
    index++;
    const rest = tokens.slice(index);
    if (rest.some((found) => isWord(found, "conflict"))) operations.add("update");
  } else if (head === "update") {
    operations.add("update");
    if (isWord(tokens[index], "or")) {
      if (isWord(tokens[index + 1], "replace")) operations.add("delete");
      index += 2;
    }
  } else if (head === "delete") {
    operations.add("delete");
    if (!isWord(tokens[index], "from")) return undefined;
    index++;
  } else return undefined;
  const table = qualifiedName(tokens, index);
  return table === undefined ? undefined : { table, operations };
}

export { splitTopLevel };
