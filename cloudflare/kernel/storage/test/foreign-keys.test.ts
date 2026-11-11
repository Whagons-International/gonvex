// The deferred-foreign-key check before commit (contract-changes 0016 G0-5).
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { objectFor, outcome } from "./helpers";
import { FOREIGN_KEY_FAILED } from "../index";
import { deferredClauses } from "../foreign-keys";

const SCHEMA = [
  "CREATE TABLE statuses (id TEXT PRIMARY KEY)",
  "CREATE TABLE tasks (id TEXT PRIMARY KEY, status_id TEXT REFERENCES statuses(id) DEFERRABLE INITIALLY DEFERRED, note TEXT)",
  "CREATE TABLE audit (id INTEGER PRIMARY KEY, task_id TEXT)",
  "CREATE TABLE task_links (id TEXT PRIMARY KEY, task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE)",
  "CREATE TABLE link_notes (id TEXT PRIMARY KEY, link_id TEXT REFERENCES task_links DEFERRABLE INITIALLY DEFERRED)",
  "CREATE TABLE strict_children (id TEXT PRIMARY KEY, status_id TEXT REFERENCES statuses(id))",
  // A trigger that writes a child row with a reference taken from the audit row.
  "CREATE TRIGGER audit_mirror AFTER INSERT ON audit BEGIN INSERT INTO tasks (id, status_id) VALUES ('mirror-' || NEW.id, NEW.task_id); END",
  "INSERT INTO statuses VALUES ('open'), ('done')",
  "INSERT INTO tasks VALUES ('t1', 'open', NULL)",
  "INSERT INTO task_links VALUES ('l1', 't1')",
  "INSERT INTO link_notes VALUES ('n1', 'l1')",
];

async function tenant() {
  const db = objectFor();
  await db.run((_tenant, state) => { for (const sql of SCHEMA) state.storage.sql.exec(sql); });
  return db;
}

describe("deferred foreign keys", () => {
  it("fails the Reducer with Rust's commit error, rolls back, and keeps the object alive", async () => {
    const db = await tenant();
    await runInDurableObject(db.stub, (instance) => { (instance as unknown as { marker: number }).marker = 42; });
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO tasks (id, status_id) VALUES ($1, $2)", ["t2", "missing"]);
      await session.query("UPDATE tasks SET note = 'written' WHERE id = 't1'");
    });
    expect(outcome(result)).toBe(FOREIGN_KEY_FAILED);
    expect(FOREIGN_KEY_FAILED).toBe("error returned from database: (code: 787) FOREIGN KEY constraint failed");
    expect(await db.sql("SELECT id, note FROM tasks ORDER BY id")).toEqual([{ id: "t1", note: null }]);
    // Same instance: the object was not reset.
    expect(await runInDurableObject(db.stub, (instance) => (instance as unknown as { marker?: number }).marker)).toBe(42);
    expect(result.stats.foreignKeyCheck).toMatchObject({ tablesChecked: 1 });
  });

  it("is needed: without the check, the platform's commit resets the object (S2 hazard)", async () => {
    const db = await tenant();
    await runInDurableObject(db.stub, (instance) => { (instance as unknown as { marker: number }).marker = 7; });
    const failure = await runInDurableObject(db.stub, (_instance, state) =>
      state.storage.transaction(async () => { state.storage.sql.exec("INSERT INTO tasks (id, status_id) VALUES ('bad', 'missing')"); }))
      .then(() => "committed", (error: Error) => error.message);
    // workerd logs "FOREIGN KEY constraint failed" at the commit, breaks the input gate and resets the object.
    expect(failure).toMatch(/^internal error; reference = /);
    // The broken stub is dead; a new stub for the same name reaches the restarted object.
    const restarted = objectFor(db.name);
    expect(await runInDurableObject(restarted.stub, (instance) => (instance as unknown as { marker?: number }).marker)).toBeUndefined();
    expect(await restarted.sql("SELECT id FROM tasks WHERE id = 'bad'")).toEqual([]);
  });

  it("commits when the transaction repairs the reference before it ends", async () => {
    const db = await tenant();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO tasks (id, status_id) VALUES ('t2', 'later')");
      await session.query("INSERT INTO statuses (id) VALUES ('later')");
    });
    expect(outcome(result)).toBe("ok");
    expect(await db.sql("SELECT status_id FROM tasks WHERE id = 't2'")).toEqual([{ status_id: "later" }]);
  });

  it("checks the parent side: deleting a referenced row", async () => {
    const db = await tenant();
    const result = await db.invoke("reducer", (session) => session.query("DELETE FROM statuses WHERE id = 'open'"));
    expect(outcome(result)).toBe(FOREIGN_KEY_FAILED);
    expect(await db.sql("SELECT count(*) AS n FROM statuses")).toEqual([{ n: 2 }]);
  });

  it("follows writes through triggers", async () => {
    const db = await tenant();
    const result = await db.invoke("reducer", (session) => session.query("INSERT INTO audit (task_id) VALUES ('missing')"));
    expect(outcome(result)).toBe(FOREIGN_KEY_FAILED);
    expect(result.stats.foreignKeyCheck?.tablesChecked).toBeGreaterThanOrEqual(1);
    const fine = await db.invoke("reducer", (session) => session.query("INSERT INTO audit (task_id) VALUES ('done')"));
    expect(outcome(fine)).toBe("ok");
  });

  it("follows ON DELETE CASCADE to a grandchild's deferred key", async () => {
    const db = await tenant();
    // tasks -> task_links (cascade) -> link_notes (deferred, implicit parent key).
    const result = await db.invoke("reducer", (session) => session.query("DELETE FROM tasks WHERE id = 't1'"));
    expect(outcome(result)).toBe(FOREIGN_KEY_FAILED);
    expect(await db.sql("SELECT count(*) AS n FROM task_links")).toEqual([{ n: 1 }]);
  });

  it("leaves immediate foreign keys to SQLite, which fails the statement", async () => {
    const db = await tenant();
    const result = await db.invoke("reducer", (session) => session.query("INSERT INTO strict_children VALUES ('c', 'missing')"));
    expect(outcome(result)).toBe("error returned from database: (code: 787) FOREIGN KEY constraint failed");
  });

  it("checks only what the invocation wrote", async () => {
    const db = await tenant();
    // A violation that predates the invocation, in a table it does not touch.
    await db.run((_tenant, state) => {
      state.storage.sql.exec("PRAGMA foreign_keys = OFF");
      state.storage.sql.exec("INSERT INTO link_notes VALUES ('orphan', 'gone')");
      state.storage.sql.exec("PRAGMA foreign_keys = ON");
    });
    const result = await db.invoke("reducer", (session) => session.query("INSERT INTO tasks (id, status_id) VALUES ('t3', 'done')"));
    expect(outcome(result)).toBe("ok");
    const read = await db.invoke("reducer", (session) => session.query("SELECT * FROM tasks"));
    expect(read.stats.foreignKeyCheck).toBeUndefined();
  });

  it("checks reported rows by rowid only (change capture, K03)", async () => {
    const db = await tenant();
    await db.run((tenant) => { tenant.writeTracking = "rows"; });
    const write = (id: string, status: string) => db.invoke("reducer", async (session) => {
      const inserted = session.run("INSERT INTO tasks (id, status_id) VALUES (?1, ?2) RETURNING rowid", [id, status]);
      session.noteRows("tasks", "insert", inserted.rows.map((row) => Number(row[0])));
    });
    const good = await write("t9", "done");
    expect(outcome(good)).toBe("ok");
    expect(good.stats.foreignKeyCheck).toMatchObject({ targeted: 1, tablesChecked: 0 });
    expect(good.stats.foreignKeyCheck!.rowsRead).toBeLessThan(10);
    const bad = await write("t10", "missing");
    expect(outcome(bad)).toBe(FOREIGN_KEY_FAILED);
    expect(await db.sql("SELECT id FROM tasks WHERE id = 't10'")).toEqual([]);
  });

  it("reads deferrability from the table SQL in SQLite's foreign key order", () => {
    expect(deferredClauses(`CREATE TABLE x (
      a TEXT REFERENCES p(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED,
      b TEXT REFERENCES p NOT DEFERRABLE,
      c TEXT REFERENCES "p" (id) MATCH SIMPLE DEFERRABLE INITIALLY IMMEDIATE NOT NULL,
      d TEXT, e TEXT,
      FOREIGN KEY (d, e) REFERENCES q (x, y) ON UPDATE CASCADE ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
    )`)).toEqual([true, false, false, true]);
  });
});

describe("deferred foreign key check cost (G0-5: measure it)", () => {
  it("measures table-level and row-level checks on 100,000 children", async () => {
    const db = objectFor();
    const numbers = await db.run(async (tenant, state) => {
      const sql = state.storage.sql;
      sql.exec("CREATE TABLE parents (id TEXT PRIMARY KEY)");
      sql.exec("CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)");
      sql.exec("CREATE INDEX children_parent ON children (parent_id)");
      sql.exec("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1000) INSERT INTO parents SELECT 'p' || i FROM n");
      sql.exec("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100000) INSERT INTO children SELECT 'c' || i, 'p' || (i % 1000 + 1) FROM n");
      const measure = async (label: string, write: (session: import("../index").StorageSession) => Promise<void>) => {
        const result = await tenant.invoke("reducer", write);
        return { label, ok: result.ok, ...result.stats.foreignKeyCheck };
      };
      const table = await measure("table-level, 1 child inserted", async (session) => {
        await session.query("INSERT INTO children VALUES ($1, $2)", ["new-1", "p5"]);
      });
      const parent = await measure("parent side, 1 parent deleted (100k children scanned)", async (session) => {
        await session.query("INSERT INTO parents VALUES ('spare')");
        await session.query("DELETE FROM parents WHERE id = 'spare'");
      });
      tenant.writeTracking = "rows";
      const targeted = await tenant.invoke("reducer", async (session) => {
        const inserted = session.run("INSERT INTO children VALUES (?1, ?2) RETURNING rowid", ["new-2", "p6"]);
        session.noteRows("children", "insert", inserted.rows.map((row) => Number(row[0])));
      });
      tenant.writeTracking = "statements";
      const started = performance.now();
      const full = sql.exec("SELECT count(*) AS n FROM pragma_foreign_key_check").one();
      const fullMs = performance.now() - started;
      return { table, parent, targeted: { ok: targeted.ok, ...targeted.stats.foreignKeyCheck }, full: { violations: full.n, ms: fullMs } };
    });
    console.log("K01 FK check cost", JSON.stringify(numbers));
    expect(numbers.table.ok && numbers.parent.ok && numbers.targeted.ok).toBe(true);
    expect(numbers.targeted).toMatchObject({ targeted: 1, tablesChecked: 0 });
    expect(numbers.targeted.rowsRead).toBeLessThan(20);
  });
});
