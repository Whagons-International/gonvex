// Admission rules, ported from the Rust tests in
// rust/crates/runtime/src/sql_admission.rs and host_calls.rs.
import { describe, expect, it } from "vitest";
import { admitStatement, AdmissionError, readStatementWrites, requireReadHead, savepointCommand, writeTarget, writtenTable } from "../admission";

const admitted = (sql: string) => { try { admitStatement(sql); return true; } catch (error) { if (error instanceof AdmissionError) return false; throw error; } };
const reason = (sql: string) => { try { admitStatement(sql); return "ok"; } catch (error) { return (error as Error).message; } };

describe("sql_admission.rs", () => {
  it("rejects invocation transaction boundaries", () => {
    for (const statement of [
      ";COMMIT", ";;/* prefix */ END", ";--prefix\nCOMMIT", "COMMIT", "COMMIT TRANSACTION", "END", "END TRANSACTION",
      "ABORT", "ROLLBACK", "ROLLBACK TRANSACTION", "BEGIN", "BEGIN IMMEDIATE", "BEGIN EXCLUSIVE TRANSACTION",
      "START TRANSACTION", "PREPARE TRANSACTION 'command'", "/* prefix */ COMMIT", "-- prefix\nEND", "cOmMiT;",
      "ROLLBACK /* reason */ TRANSACTION",
    ]) expect(admitted(statement), statement).toBe(false);
    expect(reason("COMMIT")).toBe("transaction boundaries belong to the invocation; raw transaction control is not allowed");
  });

  it("preserves savepoints and keyword boundaries", () => {
    for (const statement of [
      "SELECT 'COMMIT'", "WITH x AS (SELECT 1) SELECT * FROM x", "SAVEPOINT app", "RELEASE SAVEPOINT app", "RELEASE app",
      "ROLLBACK TO app", "ROLLBACK TO SAVEPOINT app", "ROLLBACK TRANSACTION TO app", "ROLLBACK /* reason */ TO app",
      "COMMITMENTS", "commit$app", "SELECT 1; -- trailing\n", "-- note; with a semicolon\nSELECT 1",
    ]) expect(admitted(statement), statement).toBe(true);
  });

  it("refuses statements that reach outside the tenant database", () => {
    for (const statement of [
      "ATTACH DATABASE '/tmp/other.db' AS other", "attach '/tmp/other.db' as other", "DETACH other",
      "PRAGMA foreign_keys = OFF", "PRAGMA journal_mode", "VACUUM INTO '/tmp/copy.db'",
    ]) expect(admitted(statement), statement).toBe(false);
    expect(reason("PRAGMA foreign_keys = OFF")).toBe("PRAGMA statements are not allowed in module SQL");
    expect(reason("attach 'x' as y")).toBe("ATTACH statements are not allowed in module SQL");
    expect(reason("VACUUM")).toBe("VACUUM statements are not allowed in module SQL");
    expect(admitted("SELECT * FROM pragma_table_info('tasks')")).toBe(true);
  });

  it("counts statements with SQLite lexical rules", () => {
    for (const statement of [
      "SELECT 'a'';b' AS n", 'SELECT "a;b" FROM t', "SELECT `a;b` FROM t", "SELECT [a;b] FROM t", "SELECT x'00' AS n",
      "SELECT 1 /* ; */ AS n", "/* outer /* inner */ SELECT 1", "SELECT 1 -- comment\r; DELETE FROM tasks", "SELECT $a(';')",
    ]) expect(admitted(statement), statement).toBe(true);
    for (const statement of [
      "SELECT 1; SELECT 2", "SELECT 1 -- comment\n; DELETE FROM tasks", "SELECT E'a\\';ATTACH 'x' AS y;--'",
      "SELECT $$;ATTACH 'x' AS y;$$", "SELECT `'`;ATTACH 'x' AS y;--'", "SELECT 1 /* a /* b */ ; ATTACH 'x' AS y; -- */",
      "SELECT [a'];ATTACH 'x' AS y;--']", "SELECT $a(')\n;ATTACH 'x' AS y;--'",
    ]) expect(admitted(statement), `${statement} must not be admitted as one statement`).toBe(false);
    expect(reason("SELECT 1; SELECT 2")).toBe("a database statement may not contain more than one statement");
    expect(reason("SELECT 'open")).toBe("a database statement contains an unterminated literal or comment");
    expect(admitted("SELECT 1 /* open")).toBe(false);
    expect(admitted("SELECT [open")).toBe(false);
    expect(reason("SELECT '\0'")).toBe("a database statement may not contain NUL");
    expect(reason("  ")).toBe("a database statement is required");
  });

  it("reports the first word", () => {
    expect(admitStatement("  sElEcT 1").firstWord).toBe("select");
    expect(admitStatement(";;/* x */ WITH a AS (SELECT 1) SELECT * FROM a").firstWord).toBe("with");
    expect(admitStatement('"select" 1').firstWord).toBe("");
  });
});

describe("Query statements (host_calls.rs, contract-changes 0011)", () => {
  it("requires a read head", () => {
    for (const head of ["select", "with", "values", "explain"]) expect(() => requireReadHead(head)).not.toThrow();
    expect(() => requireReadHead("delete")).toThrow('a Query may only read; "delete" is not a read statement');
  });

  it("finds writes behind a WITH prefix", () => {
    const writes = (sql: string) => readStatementWrites(admitStatement(sql).tokens);
    expect(writes("WITH x AS (SELECT 1) DELETE FROM t")).toBe(true);
    expect(writes("WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM x WHERE n < 3), y AS MATERIALIZED (SELECT 2) INSERT INTO t SELECT n FROM x")).toBe(true);
    expect(writes("WITH x AS NOT MATERIALIZED (SELECT 1) UPDATE t SET a = 1")).toBe(true);
    expect(writes("WITH x AS (DELETE FROM t RETURNING 1) SELECT 1")).toBe(false); // SQLite rejects it anyway
    expect(writes("WITH x AS (SELECT 1) SELECT * FROM x")).toBe(false);
    expect(writes("SELECT 'DELETE FROM t'")).toBe(false);
  });
});

describe("statement shapes", () => {
  it("parses module savepoint commands", () => {
    const command = (sql: string) => savepointCommand(admitStatement(sql).tokens);
    expect(command("SAVEPOINT gonvex_data_writes")).toEqual({ command: "savepoint", name: "gonvex_data_writes" });
    expect(command('RELEASE SAVEPOINT "a b"')).toEqual({ command: "release", name: "a b" });
    expect(command("release x")).toEqual({ command: "release", name: "x" });
    expect(command("ROLLBACK TRANSACTION TO SAVEPOINT x")).toEqual({ command: "rollback", name: "x" });
    expect(command("SELECT 1")).toBeUndefined();
    expect(() => command("SAVEPOINT a b")).toThrow('near "b": syntax error');
  });

  it("finds write targets like host_calls.rs write_target", () => {
    const target = (sql: string) => writeTarget(admitStatement(sql).tokens);
    expect(target('INSERT INTO "tasks"(a) VALUES (1) RETURNING *')).toBe("tasks");
    expect(target("UPDATE main.tasks SET a = 1")).toBe("main.tasks");
    expect(target("DELETE FROM t WHERE 1")).toBe("t");
    expect(target("INSERT OR REPLACE INTO t VALUES (1)")).toBeUndefined();
    expect(target("REPLACE INTO t VALUES (1)")).toBeUndefined();
    expect(target("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x")).toBeUndefined();
  });

  it("finds written tables and operations for the deferred-FK check", () => {
    const written = (sql: string) => { const found = writtenTable(admitStatement(sql).tokens); return found && [found.table, [...found.operations].sort()]; };
    expect(written("INSERT INTO t VALUES (1)")).toEqual(["t", ["insert"]]);
    expect(written("INSERT OR REPLACE INTO t VALUES (1)")).toEqual(["t", ["delete", "insert"]]);
    expect(written("REPLACE INTO t VALUES (1)")).toEqual(["t", ["delete", "insert"]]);
    expect(written("INSERT INTO t VALUES (1) ON CONFLICT (a) DO UPDATE SET b = 2")).toEqual(["t", ["insert", "update"]]);
    expect(written("UPDATE OR REPLACE t SET a = 1")).toEqual(["t", ["delete", "update"]]);
    expect(written("WITH x AS (SELECT 1) DELETE FROM t")).toEqual(["t", ["delete"]]);
    expect(written("SELECT 1")).toBeUndefined();
  });
});
