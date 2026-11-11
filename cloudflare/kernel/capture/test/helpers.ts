// Test helpers: a Durable Object per test, and a stand-in for K01's
// TenantStorage/StorageSession (PR #339, not merged when K03 was written).
//
// The stand-in keeps K01's transaction rules that capture depends on: one
// `ctx.storage.transaction` per invocation; module SAVEPOINT / RELEASE /
// ROLLBACK TO as nested storage transactions on a LIFO stack (K01's own
// mechanism); a failed statement aborts the invocation until ROLLBACK TO; a
// Reducer commits only when its body returns with nothing outstanding; a
// Query never commits. It does none of K01's admission, decoding or binding
// work beyond what the fixture statements need.

import { env, runInDurableObject } from "cloudflare:test";
import type { CapturedSession, WriteOperation } from "../index.ts";

let objects = 0;

export async function withObject<T>(fn: (state: DurableObjectState) => T | Promise<T>): Promise<T> {
  objects += 1;
  const stub = env.HELLO.getByName(`k03-test-${objects}-${Math.random()}`);
  return runInDurableObject(stub, (_instance, state) => fn(state));
}

export const rows = <T = Record<string, SqlStorageValue>>(sql: SqlStorage, query: string, ...binds: SqlStorageValue[]): T[] =>
  [...sql.exec(query, ...binds)] as T[];

export const exec = (sql: SqlStorage, statements: readonly string[]) => {
  for (const s of statements) sql.exec(s);
};

const ROLLBACK = Symbol("rollback");

interface OpenSavepoint {
  name: string;
  settle: { resolve: () => void; reject: (reason: unknown) => void };
  done: Promise<unknown>;
}

/** Module parameters bound as the Rust host binds them (sql_parameters.rs), for the fixture's shapes. */
function bindValue(value: unknown): SqlStorageValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number" || typeof value === "string") return value;
  return JSON.stringify(value);
}

export class StandInSession implements CapturedSession {
  aborted: string | undefined;
  discarded = false;
  readonly noted: Array<{ table: string; operation: WriteOperation; rowids: number[] }> = [];
  readonly stats = { statements: 0, rowsRead: 0, rowsWritten: 0 };
  private readonly savepoints: OpenSavepoint[] = [];

  constructor(private readonly storage: DurableObjectStorage, readonly kind: "reducer" | "query") {}

  run(text: string, parameters: readonly unknown[] = []) {
    if (this.aborted !== undefined) throw new Error("current transaction is aborted, commands ignored until end of transaction block");
    try {
      const cursor = this.storage.sql.exec(text.replace(/\$(\d+)/g, "?$1"), ...parameters.map(bindValue));
      const result = { columnNames: cursor.columnNames, rows: [...cursor.raw()], rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
      this.stats.statements += 1;
      this.stats.rowsRead += result.rowsRead;
      this.stats.rowsWritten += result.rowsWritten;
      return result;
    } catch (error) {
      this.aborted = String((error as Error).message);
      throw error;
    }
  }

  noteRows(table: string, operation: WriteOperation, rowids: readonly number[]): void {
    this.noted.push({ table, operation, rowids: [...rowids] });
  }

  /** Module SQL: savepoint commands, or one statement. */
  async query(text: string, parameters: readonly unknown[] = []): Promise<void> {
    const command = /^\s*(SAVEPOINT|RELEASE|ROLLBACK\s+TO)\s+(\w+)\s*$/i.exec(text);
    if (!command) {
      this.run(text, parameters);
      return;
    }
    const verb = command[1]!.toUpperCase().replace(/\s+/g, " ");
    const name = command[2]!.toLowerCase();
    if (verb === "SAVEPOINT") return this.open(name);
    const index = this.savepoints.map((s) => s.name).lastIndexOf(name);
    if (index < 0) throw new Error(`no such savepoint: ${name}`);
    if (verb === "RELEASE") {
      while (this.savepoints.length > index) await this.close(true);
      return;
    }
    while (this.savepoints.length > index) await this.close(false);
    this.aborted = undefined;
    await this.open(name);
  }

  private async open(name: string): Promise<void> {
    let settle!: OpenSavepoint["settle"];
    const gate = new Promise<void>((resolve, reject) => (settle = { resolve, reject }));
    let started!: () => void;
    const opened = new Promise<void>((resolve) => (started = resolve));
    const done = this.storage.transaction(async () => {
      started();
      await gate;
    });
    done.catch(() => {});
    this.savepoints.push({ name, settle, done });
    await opened;
  }

  private async close(commit: boolean): Promise<void> {
    const top = this.savepoints.pop()!;
    if (commit) top.settle.resolve();
    else top.settle.reject(new Error("ROLLBACK TO"));
    await top.done.catch(() => {});
  }

  async settle(commit: boolean): Promise<void> {
    while (this.savepoints.length) await this.close(commit);
  }
}

type StandInResult<T> = { ok: true; value: T; committed: boolean; session: StandInSession } | { ok: false; error: unknown; session: StandInSession };

/** K01's `TenantStorage.invoke`, reduced to its transaction rules. */
export class StandInStorage {
  constructor(private readonly storage: DurableObjectStorage) {}

  async invoke<T>(kind: "reducer" | "query", body: (session: StandInSession) => Promise<T>): Promise<StandInResult<T>> {
    const session = new StandInSession(this.storage, kind);
    let result: StandInResult<T> | undefined;
    try {
      await this.storage.transaction(async () => {
        let value: T | undefined;
        let error: unknown;
        let failed = false;
        try {
          value = await body(session);
        } catch (thrown) {
          failed = true;
          error = thrown;
        }
        const commit = !failed && kind === "reducer" && !session.discarded;
        await session.settle(commit);
        if (commit && session.aborted !== undefined) {
          failed = true;
          error = new Error(`aborted: ${session.aborted}`);
        }
        result = failed ? { ok: false, error, session } : { ok: true, value: value as T, committed: commit, session };
        if (!commit || failed) throw ROLLBACK;
      });
    } catch (thrown) {
      if (thrown !== ROLLBACK) throw thrown;
    }
    return result!;
  }
}
