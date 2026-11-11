// One storage transaction per invocation, module savepoints as nested storage
// transactions, and Rust's failure rules (contract-changes 0006, 0011, 0016 G0-3).
import { describe, expect, it } from "vitest";
import { objectFor, outcome, rejection } from "./helpers";
import { ABORTED, CLOSED, READ_ONLY_WRITE, type StorageSession } from "../index";

async function table() {
  const db = objectFor();
  await db.run((_tenant, state) => {
    state.storage.sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER NOT NULL DEFAULT 0)");
    state.storage.sql.exec("INSERT INTO t VALUES ('seed', 0)");
  });
  return { ...db, ids: async () => (await db.sql<{ id: string }>("SELECT id FROM t ORDER BY id")).map((row) => row.id) };
}

describe("one transaction per invocation", () => {
  it("commits a Reducer's writes across awaits", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO t (id) VALUES ($1)", ["a"]);
      await crypto.subtle.digest("SHA-256", new Uint8Array(8));
      await session.query("INSERT INTO t (id) VALUES ($1)", ["b"]);
      return "done";
    });
    expect(result.ok && result.committed).toBe(true);
    expect(await db.ids()).toEqual(["a", "b", "seed"]);
  });

  it("rolls everything back when the Reducer throws after writing", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO t (id) VALUES ('a')");
      await session.query("UPDATE t SET n = 9");
      throw new Error("fixture rejected the intent");
    });
    expect(outcome(result)).toBe("fixture rejected the intent");
    expect(await db.sql("SELECT * FROM t")).toEqual([{ id: "seed", n: 0 }]);
  });

  it("does not commit a Reducer that caught a SQL error without ROLLBACK TO", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO t (id) VALUES ('a')");
      const failure = await rejection(session.query("INSERT INTO t (id) VALUES ('a')"));
      expect(failure).toBe("error returned from database: (code: 1555) UNIQUE constraint failed: t.id");
      // Every later call fails until ROLLBACK TO (Postgres-like "aborted").
      expect(await rejection(session.query("SELECT 1"))).toBe(ABORTED);
      return "caught";
    });
    expect(outcome(result)).toBe(ABORTED);
    expect(await db.ids()).toEqual(["seed"]);
  });

  it("discards a successful invocation on request (Rust finish(false))", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("INSERT INTO t (id) VALUES ('a')");
      session.discard();
      return 1;
    });
    expect(result.ok && !result.committed).toBe(true);
    expect(await db.ids()).toEqual(["seed"]);
  });

  it("closes the session when the invocation ends", async () => {
    const db = await table();
    let kept: StorageSession | undefined;
    await db.invoke("reducer", async (session) => { kept = session; });
    expect(await rejection(kept!.query("SELECT 1"))).toBe(CLOSED);
  });

  it("counts rows read and written per statement and per invocation", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      const insert = session.run("INSERT INTO t (id) VALUES (?1), (?2)", ["a", "b"]);
      const scan = session.run("SELECT * FROM t");
      return { insert: [insert.rowsRead, insert.rowsWritten], scan: [scan.rowsRead, scan.rowsWritten] };
    });
    if (!result.ok) throw result.error;
    expect(result.value.scan).toEqual([3, 0]);
    expect(result.value.insert[1]).toBeGreaterThanOrEqual(2);
    expect(result.stats.rowsWritten).toBe(result.value.insert[1]);
    expect(result.stats.statements).toBeGreaterThanOrEqual(2);
  });
});

describe("module savepoints as nested storage transactions (0006, G0-3)", () => {
  it("S3: nested savepoints, ROLLBACK TO and continuing", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("SAVEPOINT outer_sp");
      await session.query("INSERT INTO t (id) VALUES ('rolled-1')");
      await session.query("SAVEPOINT inner_sp");
      await session.query("INSERT INTO t (id) VALUES ('rolled-2')");
      await session.query("ROLLBACK TO outer_sp");
      // ROLLBACK TO keeps outer_sp open, as SQLite does.
      await session.query("INSERT INTO t (id) VALUES ('kept')");
      await session.query("RELEASE outer_sp");
      return session.query("SELECT id FROM t ORDER BY id");
    });
    expect(result.ok && result.value).toEqual([{ id: "kept" }, { id: "seed" }]);
    expect(await db.ids()).toEqual(["kept", "seed"]);
  });

  it("S4: a caught SQL error recovered by ROLLBACK TO commits", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("SAVEPOINT sp");
      await session.query("INSERT INTO t (id) VALUES ('a')");
      await rejection(session.query("INSERT INTO t (id) VALUES ('seed')"));
      expect(await rejection(session.query("RELEASE sp"))).toBe(ABORTED);
      await session.query("ROLLBACK TO SAVEPOINT sp");
      await session.query("INSERT INTO t (id) VALUES ('b')");
      await session.query("RELEASE SAVEPOINT sp");
    });
    expect(outcome(result)).toBe("ok");
    expect(await db.ids()).toEqual(["b", "seed"]);
  });

  it("releases savepoints left open on success and rolls them back on failure", async () => {
    const db = await table();
    const kept = await db.invoke("reducer", async (session) => {
      await session.query("SAVEPOINT left_open");
      await session.query("INSERT INTO t (id) VALUES ('open-commit')");
    });
    expect(outcome(kept)).toBe("ok");
    const dropped = await db.invoke("reducer", async (session) => {
      await session.query("SAVEPOINT left_open");
      await session.query("INSERT INTO t (id) VALUES ('open-fail')");
      throw new Error("no");
    });
    expect(outcome(dropped)).toBe("no");
    expect(await db.ids()).toEqual(["open-commit", "seed"]);
  });

  it("fails RELEASE or ROLLBACK TO of an unknown savepoint like SQLite", async () => {
    const db = await table();
    const result = await db.invoke("reducer", (session) => session.query("RELEASE nowhere"));
    expect(outcome(result)).toBe("error returned from database: (code: 1) no such savepoint: nowhere");
  });

  it("keeps the pinned savepoint name case-insensitive", async () => {
    const db = await table();
    const result = await db.invoke("reducer", async (session) => {
      await session.query("SAVEPOINT Mixed");
      await session.query("INSERT INTO t (id) VALUES ('x')");
      await session.query("ROLLBACK TO mixed");
      await session.query("RELEASE MIXED");
    });
    expect(outcome(result)).toBe("ok");
    expect(await db.ids()).toEqual(["seed"]);
  });
});

describe("read-only Queries (contract-changes 0011 §2)", () => {
  it("refuses a write head like Rust's Query", async () => {
    const db = await table();
    const result = await db.invoke("query", (session) => session.query("DELETE FROM t"));
    expect(outcome(result)).toBe('a Query may only read; "delete" is not a read statement');
  });

  it("refuses WITH … DELETE with Rust's read-only error, before anything changes", async () => {
    const db = await table();
    const result = await db.invoke("query", async (session) => {
      const failure = await rejection(session.query("WITH x AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT 'seed' FROM x)"));
      expect(await rejection(session.query("SELECT 1"))).toBe(ABORTED);
      return failure;
    });
    expect(result.ok && result.value).toBe(READ_ONLY_WRITE);
    expect(READ_ONLY_WRITE).toBe("error returned from database: (code: 8) attempt to write a readonly database");
    expect(await db.ids()).toEqual(["seed"]);
  });

  it("never commits, even when it returns normally", async () => {
    const db = await table();
    const result = await db.invoke("query", async (session) => session.query("SELECT count(*) AS n FROM t"));
    expect(result.ok && result.value).toEqual([{ n: 1 }]);
    expect(result.ok && result.committed).toBe(false);
  });
});
