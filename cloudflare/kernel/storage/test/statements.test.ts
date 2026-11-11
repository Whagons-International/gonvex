// Module statements on Durable Object SQLite: binding, limits, errors and
// decoding, each against the Rust runtime's behavior.
import { describe, expect, it } from "vitest";
import { objectFor, outcome } from "./helpers";
import type { StorageSession } from "../index";

/** Runs `body` in a committed Reducer invocation on a fresh object with `ddl`. */
async function reducer<T>(ddl: string[], body: (session: StorageSession) => Promise<T>) {
  const db = objectFor();
  await db.run((_tenant, state) => { for (const sql of ddl) state.storage.sql.exec(sql); });
  const result = await db.invoke("reducer", body);
  return { db, result };
}

async function value<T>(ddl: string[], body: (session: StorageSession) => Promise<T>): Promise<T> {
  const { result } = await reducer(ddl, body);
  if (!result.ok) throw result.error;
  return result.value;
}

const ERROR = async (ddl: string[], body: (session: StorageSession) => Promise<unknown>) => {
  const { result } = await reducer(ddl, body);
  return outcome(result);
};

describe("$N placeholders (contract-changes 0011 §1)", () => {
  it("binds out-of-order and repeated placeholders by number", async () => {
    const rows = await value([], (db) => db.query("SELECT $2 AS b, $1 AS a, $2 || $1 AS ba, $1 AS again", ["one", "two"]));
    expect(rows).toEqual([{ b: "two", a: "one", ba: "twoone", again: "one" }]);
  });

  it("writes out-of-order placeholders into the right columns", async () => {
    const rows = await value(["CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, b INTEGER)"], async (db) => {
      await db.query("INSERT INTO t (b, id, a) VALUES ($3, $1, $2)", ["row", "alpha", 7]);
      await db.query("UPDATE t SET a = $2 WHERE id = $1", ["row", "beta"]);
      return db.query("SELECT * FROM t WHERE id = $1 AND b = $2", ["row", 7]);
    });
    expect(rows).toEqual([{ id: "row", a: "beta", b: 7 }]);
  });

  it("follows Rust's bind rules: ?N and ? by number, missing values NULL from there on", async () => {
    expect(await value([], (db) => db.query("SELECT ?2 AS b, ? AS first, ?1 AS a", ["x", "y"]))).toEqual([{ b: "y", first: "x", a: "x" }]);
    // `$3` has no value: it and every later parameter bind NULL, as direct.rs `bind` breaks.
    expect(await value([], (db) => db.query("SELECT $1 AS a, $3 AS c, $2 AS b", ["x", "y"]))).toEqual([{ a: "x", c: null, b: null }]);
    expect(await value([], (db) => db.query("SELECT $1 AS a", ["x", "unused"]))).toEqual([{ a: "x" }]);
    expect(await ERROR([], (db) => db.query("SELECT :name", ["x"]))).toBe("encountered unexpected or invalid data: unsupported SQL parameter format: :name");
    expect(await ERROR([], (db) => db.query("SELECT $name", ["x"]))).toBe("encountered unexpected or invalid data: parameters with non-integer names are not currently supported: name");
    expect(await ERROR([], (db) => db.query("SELECT ?0", []))).toBe("error returned from database: (code: 1) variable number must be between ?1 and ?32766");
  });

  it("allows a high parameter number when few values are bound", async () => {
    const parameters = Array.from({ length: 150 }, (_, index) => `v${index + 1}`);
    expect(await value([], (db) => db.query("SELECT $150 AS last, $1 AS first", parameters))).toEqual([{ last: "v150", first: "v1" }]);
  });

  it("keeps the module's text as the name of an unaliased parameter column", async () => {
    expect(await value([], (db) => db.query("SELECT $1, $2 + 1", ["a", 2]))).toEqual([{ $1: "a", "$2 + 1": 3 }]);
    expect(await value([], (db) => db.query("SELECT ?1, '?1' AS literal", ["a"]))).toEqual([{ "?1": "a", literal: "?1" }]);
    // Known gap: `$1` and `?1` in one statement share SQLite's slot 1, and both columns come back named "$1".
  });
});

describe("parameter types (sql_parameters.rs)", () => {
  it("binds integers as INTEGER, not REAL", async () => {
    const rows = await value(["CREATE TABLE t (id TEXT PRIMARY KEY, s TEXT, u)"], async (db) => {
      await db.query("INSERT INTO t VALUES ($1, $2, $3)", ["a", 3, 4]);
      return db.query("SELECT s, typeof(s) AS ts, u, typeof(u) AS tu, typeof($1) AS t1, $1 / 2 AS half, json_object('n', $1) AS j FROM t", [3]);
    });
    // Durable Object SQL alone would store "3.0" and give 1.5 and {"n":3.0}.
    expect(rows).toEqual([{ s: "3", ts: "text", u: 4, tu: "integer", t1: "integer", half: 1, j: { n: 3 } }]);
  });

  it("compares integers with TEXT columns like a bound integer, and uses the index", async () => {
    const ddl = ["CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER)", "CREATE INDEX t_n ON t (n)", "INSERT INTO t VALUES ('03', 1), ('3', 2)"];
    const result = await value(ddl, async (db) => ({
      // TEXT affinity applies to a bound integer: '3' matches, '03' does not.
      text: await db.query("SELECT id FROM t WHERE id = $1", [3]),
      plan: db.explain("SELECT id FROM t WHERE n = $1", [2]).rows.map((row) => String(row[1])),
    }));
    expect(result.text).toEqual([{ id: "3" }]);
    // The integer parameter still drives an index seek on t_n.
    expect(result.plan).toContain("SeekGE");
  });

  it("binds booleans as 0/1, reals as REAL, objects and arrays as JSON text, and refuses u64", async () => {
    const rows = await value([], (db) => db.query("SELECT $1 AS t, $2 AS f, typeof($3) AS r, $4 AS o, $5 AS a, typeof($6) AS big", [true, false, 1.5, { b: 1, a: [2] }, [1, "x"], 2 ** 60]));
    expect(rows).toEqual([{ t: 1, f: 0, r: "real", o: { b: 1, a: [2] }, a: [1, "x"], big: "integer" }]);
    expect(await ERROR([], (db) => db.query("SELECT $1", [2 ** 63]))).toBe("parameter $1: integer is outside the SQLite integer range");
    expect(await ERROR([], (db) => db.query("SELECT 1", "nope" as unknown as unknown[]))).toBe("query parameters must be an array");
  });
});

describe("100 bound parameters (contract-changes 0011 §3)", () => {
  it("fails module SQL that binds more than 100 parameters, naming the limit", async () => {
    const list = Array.from({ length: 101 }, (_, index) => `$${index + 1}`).join(", ");
    const parameters = Array.from({ length: 101 }, (_, index) => index);
    expect(await ERROR(["CREATE TABLE t (n INTEGER)"], (db) => db.query(`SELECT n FROM t WHERE n IN (${list})`, parameters)))
      .toBe("a database statement binds 101 parameters; Durable Object SQLite allows at most 100 per statement");
  });

  it("chunks kernel IN lists under 100 parameters", async () => {
    const ids = Array.from({ length: 250 }, (_, index) => `id-${index}`);
    const ddl = ["CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER)"];
    const rows = await value(ddl, async (db) => {
      db.runInChunks((list) => `INSERT INTO t (id, n) SELECT value, ?1 FROM json_each(json_array(${list}))`, ids, [5]);
      const before = db.stats.statements;
      const result = db.runInChunks((list) => `SELECT count(*) AS n FROM t WHERE n = ?1 AND id IN (${list})`, ids, [5]);
      return { counts: result.rows.map((row) => row[0]), statements: db.stats.statements - before };
    });
    expect(rows.counts).toEqual([99, 99, 52]);
    expect(rows.statements).toBe(3);
  });
});

describe("refusals and errors", () => {
  it("never runs a second statement", async () => {
    const { db, result } = await reducer(["CREATE TABLE t (id TEXT)", "INSERT INTO t VALUES ('keep')"], (session) => session.query("SELECT 1; DELETE FROM t"));
    expect(outcome(result)).toBe("a database statement may not contain more than one statement");
    expect(await db.sql("SELECT * FROM t")).toEqual([{ id: "keep" }]);
  });

  it("refuses ATTACH, DETACH, PRAGMA and VACUUM before Durable Object SQL sees them", async () => {
    for (const [sql, text] of [["ATTACH ':memory:' AS x", "ATTACH"], ["DETACH x", "DETACH"], ["PRAGMA defer_foreign_keys = ON", "PRAGMA"], ["VACUUM", "VACUUM"]] as const) {
      expect(await ERROR([], (db) => db.query(sql))).toBe(`${text} statements are not allowed in module SQL`);
    }
  });

  it("reports SQLite failures in sqlx's words with the extended code", async () => {
    const ddl = ["CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER NOT NULL CHECK (n > 0))", "INSERT INTO t VALUES ('a', 1)"];
    expect(await ERROR(ddl, (db) => db.query("INSERT INTO t VALUES ('a', 2)"))).toBe("error returned from database: (code: 1555) UNIQUE constraint failed: t.id");
    expect(await ERROR(ddl, (db) => db.query("INSERT INTO t VALUES ('b', NULL)"))).toBe("error returned from database: (code: 1299) NOT NULL constraint failed: t.n");
    expect(await ERROR(ddl, (db) => db.query("INSERT INTO t VALUES ('b', 0)"))).toBe("error returned from database: (code: 275) CHECK constraint failed: n > 0");
    expect(await ERROR(ddl, (db) => db.query("SELEC 1"))).toBe('error returned from database: (code: 1) near "SELEC": syntax error');
    expect(await ERROR(ddl, (db) => db.query("SELECT * FROM missing"))).toBe("error returned from database: (code: 1) no such table: missing");
  });
});

describe("decoding by declared type (rows.rs, direct.rs write_json)", () => {
  const ddl = [
    "CREATE TABLE d (id TEXT PRIMARY KEY, meta JSON, done BOOLEAN, n BIGINT, r DOUBLE PRECISION, note TEXT, data BLOB)",
    `INSERT INTO d VALUES ('a', '{"x":[1,2]}', 1, 9007199254740991, 1.5, '{"not":"json"}', x'00ff10'), ('b', 'not json', 0, NULL, 1e999, 'true', NULL)`,
  ];

  it("decodes JSON and BOOLEAN columns, keeps TEXT verbatim, BLOB as lowercase hex, non-finite REAL as null", async () => {
    const rows = await value(ddl, (db) => db.query("SELECT * FROM d ORDER BY id"));
    expect(rows).toEqual([
      { id: "a", meta: { x: [1, 2] }, done: true, n: 9007199254740991, r: 1.5, note: '{"not":"json"}', data: "00ff10" },
      { id: "b", meta: "not json", done: false, n: null, r: null, note: "true", data: null },
    ]);
  });

  it("propagates declared types through aliases, joins, subqueries and CTEs (sql_projection.rs)", async () => {
    const rows = await value(ddl, (db) => db.query(
      "WITH c AS (SELECT meta, done FROM d), renamed(payload, complete) AS MATERIALIZED (SELECT * FROM c) " +
      "SELECT q.payload, q.complete, d2.note AS label, count(*) OVER () AS total FROM (SELECT * FROM renamed) q JOIN d d2 ON d2.id = 'a' ORDER BY 2 DESC",
    ));
    expect(rows[0]).toEqual({ payload: { x: [1, 2] }, complete: true, label: '{"not":"json"}', total: 2 });
  });

  it("decodes expression columns by storage class, with JSON-shaped text parsed", async () => {
    const rows = await value(ddl, (db) => db.query("SELECT json_group_array(id) AS ids, 'false' AS f, '{\"a\":1}' AS o, 1 AS one, x'ab' AS b, 'plain' AS p, json('0') AS zero FROM d"));
    expect(rows).toEqual([{ ids: ["a", "b"], f: false, o: { a: 1 }, one: 1, b: "ab", p: "plain", zero: 0 }]);
  });

  it("decodes RETURNING rows by the write target's catalog", async () => {
    const rows = await value(ddl, (db) => db.query("INSERT INTO d (id, meta, done, note) VALUES ($1, $2, $3, $4) RETURNING id, meta, done, note", ["c", JSON.stringify(["x"]), true, "[1]"]));
    expect(rows).toEqual([{ id: "c", meta: ["x"], done: true, note: "[1]" }]);
  });

  it("reads columns of json_each and views", async () => {
    const rows = await value([...ddl, "CREATE VIEW v AS SELECT id, meta AS m, done FROM d"], async (db) => ({
      view: await db.query("SELECT * FROM v WHERE id = 'a'"),
      each: await db.query("SELECT d.id, j.value FROM d, json_each(d.meta, '$.x') AS j WHERE d.id = 'a' ORDER BY j.key"),
    }));
    expect(rows.view).toEqual([{ id: "a", m: { x: [1, 2] }, done: true }]);
    expect(rows.each).toEqual([{ id: "a", value: 1 }, { id: "a", value: 2 }]);
  });

  it("uses logical record schemas from _gonvex_record_views", async () => {
    const records = [
      "CREATE TABLE _gonvex_record_views (name TEXT PRIMARY KEY, schema TEXT NOT NULL)",
      "CREATE TABLE storage_r (id TEXT PRIMARY KEY, payload TEXT)",
      `INSERT INTO _gonvex_record_views VALUES ('R', '{"key":"id","columns":{"id":{"type":"text"},"payload":{"type":"json"}}}')`,
      "CREATE VIEW r AS SELECT id, payload FROM storage_r",
      `INSERT INTO storage_r VALUES ('k', '{"deep":true}')`,
    ];
    const rows = await value(records, (db) => db.query("SELECT * FROM r"));
    expect(rows).toEqual([{ id: "k", payload: { deep: true } }]);
  });

  it("fails a SELECT over a composite-key table like the Rust catalog does", async () => {
    expect(await ERROR(["CREATE TABLE pair (a TEXT, b TEXT, PRIMARY KEY (a, b))"], (db) => db.query("SELECT * FROM pair")))
      .toBe('table "pair" has a composite primary key, which db.update/delete do not support');
  });

  it("caches catalogs across invocations (rows read drop to the statement's own)", async () => {
    const db = objectFor();
    await db.run((_tenant, state) => { for (const sql of ddl) state.storage.sql.exec(sql); });
    const first = await db.invoke("query", (session) => session.query("SELECT meta FROM d"));
    const second = await db.invoke("query", (session) => session.query("SELECT meta FROM d"));
    expect(first.stats.catalog.statements).toBeGreaterThan(0);
    expect(second.stats.catalog.statements).toBe(0);
    expect(second.stats.rowsRead).toBe(2);
    // Module DDL invalidates it.
    await db.invoke("reducer", (session) => session.query("ALTER TABLE d ADD COLUMN extra JSON"));
    const third = await db.invoke("query", (session) => session.query("SELECT meta FROM d"));
    expect(third.stats.catalog.statements).toBeGreaterThan(0);
  });

  it("drops cached catalogs again when module DDL rolls back", async () => {
    const db = objectFor();
    await db.run((_tenant, state) => { for (const sql of ddl) state.storage.sql.exec(sql); });
    // A stale catalog (with a rolled-back column) no longer matches `*`, so BOOLEAN `done` would decode as 1.
    const read = async () => {
      const result = await db.invoke("query", (session) => session.query("SELECT * FROM d WHERE id = 'a'"));
      return result.ok && result.value[0];
    };
    const failed = await db.invoke("reducer", async (session) => {
      await session.query("ALTER TABLE d ADD COLUMN later JSON");
      await session.query("SELECT * FROM d"); // caches d with `later`
      throw new Error("roll back the DDL");
    });
    expect(outcome(failed)).toBe("roll back the DDL");
    expect(await read()).toMatchObject({ done: true });
    const savepoint = await db.invoke("reducer", async (session) => {
      await session.query("SAVEPOINT s");
      await session.query("ALTER TABLE d ADD COLUMN later TEXT");
      await session.query("SELECT * FROM d");
      await session.query("ROLLBACK TO s");
      return session.query("SELECT * FROM d WHERE id = 'a'");
    });
    expect(savepoint.ok && savepoint.value[0]).toMatchObject({ done: true });
    expect(savepoint.ok && Object.keys(savepoint.value[0]!)).not.toContain("later");
    expect(await read()).toMatchObject({ done: true });
  });
});
