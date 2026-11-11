// The transform against real Durable Object SQLite in workerd: each original
// construct is refused, the transformed DDL loads, and both return the same
// results where the original can run at all.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  checkStatement,
  chunkStatements,
  recordFlushSql,
  recordView,
  RECORD_PENDING_CLEANUP_SQL,
  RECORD_PENDING_NAMES_SQL,
  SchemaTransform,
  splitStatements,
  TransformError,
  transformMigrations,
} from "../index.ts";
import { applyPairs, CAPTURED_TABLE, deepFold, ITEMS_RECORD, NOTES_SHAPE, RECORD_SCHEMA, referenceSchema } from "./fixtures.ts";

type Sql = SqlStorage;
let objects = 0;

/** Runs `fn` against a fresh Durable Object's SQLite. */
async function withObject<T>(fn: (sql: Sql, storage: DurableObjectStorage) => T): Promise<T> {
  objects += 1;
  const stub = env.HELLO.getByName(`a01-test-${objects}-${Math.random()}`);
  return runInDurableObject(stub, (_instance, state) => fn(state.storage.sql, state.storage));
}

const exec = (sql: Sql, statements: string[]) => {
  for (const s of statements) sql.exec(s);
};
const errorOf = (fn: () => unknown): string | null => {
  try {
    fn();
    return null;
  } catch (error) {
    return (error as Error).message;
  }
};
const rows = <T extends Record<string, SqlStorageValue>>(sql: Sql, query: string) => sql.exec<T>(query).toArray();

describe("T1 compound chains", () => {
  const view = `CREATE VIEW v AS ${Array.from({ length: 12 }, (_, k) => `SELECT ${k} AS n, 'x${k}' AS s`).join(" UNION ALL ")} ORDER BY n DESC`;

  it("the original is refused; the left-nested chain loads and returns the same rows", async () => {
    const out = new SchemaTransform().migration("t1", `${view};`);
    expect(out.rewrites).toContainEqual({ rule: "T1", object: "v" });
    await withObject((sql) => {
      expect(errorOf(() => sql.exec(view))).toMatch(/too many terms in compound SELECT/);
      exec(sql, out.statements);
      expect(rows(sql, "SELECT n, s FROM v")).toEqual(Array.from({ length: 12 }, (_, k) => ({ n: 11 - k, s: `x${11 - k}` })));
    });
  });
});

describe("T3 expression depth", () => {
  const { sql: chain, pairs } = deepFold("lower(trim(name))", 150);
  const samples = ["Анна", "ЖЁЛТЫЙ-цвет", "  Mixed Case-Name ", "plain", "Яя-ЯЯ"];

  it("a deep chain in a view becomes a recursive fold with the same values", async () => {
    const view = `CREATE VIEW folded AS SELECT name, ${chain} AS k FROM people`;
    const out = new SchemaTransform().migration("t3v", `CREATE TABLE people(name TEXT); ${view};`);
    expect(out.rewrites).toContainEqual({ rule: "T3", object: "folded" });
    await withObject((sql) => {
      sql.exec("CREATE TABLE probe(name TEXT)");
      expect(errorOf(() => sql.exec(view.replace("people", "probe").replace("folded", "probe_v")))).toMatch(/Expression tree is too large/);
      exec(sql, out.statements);
      for (const s of samples) sql.exec("INSERT INTO people VALUES (?)", s);
      const got = new Map(rows<{ name: string; k: string }>(sql, "SELECT name, k FROM folded").map((r) => [r.name, r.k]));
      expect(samples.map((s) => got.get(s))).toEqual(samples.map((s) => applyPairs(s.trim().replace(/[A-Z]/g, (c) => c.toLowerCase()), pairs)));
    });
  });

  it("a deep unique index becomes generated columns and rejects duplicates with the same error", async () => {
    const index = `CREATE UNIQUE INDEX "people_key_unique" ON people (coalesce(nullif(${chain}, ''), 'none')) WHERE deleted IS NULL`;
    const out = new SchemaTransform().migration("t3i", `CREATE TABLE people(name TEXT, deleted INTEGER); ${index};`);
    expect(out.statements.filter((s) => s.startsWith("ALTER TABLE"))).toHaveLength(1);
    await withObject((sql) => {
      exec(sql, out.statements);
      sql.exec("INSERT INTO people(name) VALUES ('Анна')");
      expect(errorOf(() => sql.exec("INSERT INTO people(name) VALUES ('АННА')"))).toMatch(/UNIQUE constraint failed: index 'people_key_unique'/);
      sql.exec("INSERT INTO people(name, deleted) VALUES ('АННА', 1)"); // partial index: deleted rows do not collide
      // table_info and INSERT ... VALUES do not see the generated column; SELECT * does, so the transform refuses it.
      expect(rows(sql, "SELECT name FROM pragma_table_info('people')").map((r) => r.name)).toEqual(["name", "deleted"]);
      expect(Object.keys(rows(sql, "SELECT * FROM people")[0]!)).toEqual(["name", "deleted", "_gonvex_st_fold_people_key_unique_0_0"]);
    });
    const t = new SchemaTransform();
    t.migration("a", `CREATE TABLE people(name TEXT, deleted INTEGER); ${index};`);
    expect(() => t.migration("x", "CREATE TABLE copy AS SELECT * FROM people")).toThrow(/x: copy: unhandled-construct: selects \* from people/);
    expect(() => t.migration("x", "CREATE VIEW pv AS SELECT p.* FROM people p")).toThrow(/unhandled-construct/);
    expect(t.migration("x", "CREATE VIEW pc AS SELECT count(*) AS n FROM people").statements).toHaveLength(1);
    const dropped = t.migration("b", `DROP INDEX "people_key_unique";`);
    expect(dropped.statements[1]).toMatch(/^ALTER TABLE "people" DROP COLUMN "_gonvex_st_fold_people_key_unique_0_0"$/);
    expect(t.addedObjects()).toEqual([]);
  });
});

describe("T4 TEMP tables", () => {
  it("become ordinary tables dropped by the end of the migration", async () => {
    const source = "CREATE TEMP TABLE guard(id INTEGER); INSERT INTO guard VALUES (1); CREATE TABLE kept(x);";
    const out = new SchemaTransform().migration("t4", source);
    expect(out.statements[0]).toBe("CREATE TABLE guard(id INTEGER)");
    expect(out.statements.at(-1)).toBe('DROP TABLE IF EXISTS "guard"');
    expect(out.chunks).toHaveLength(1);
    await withObject((sql) => {
      expect(errorOf(() => sql.exec("CREATE TEMP TABLE guard(id INTEGER)"))).toMatch(/not authorized/);
      exec(sql, out.statements);
      expect(rows(sql, "SELECT name FROM sqlite_schema WHERE name IN ('guard','kept')")).toEqual([{ name: "kept" }]);
    });
  });
});

describe("T6 per-branch reference statements", () => {
  const writes = [
    `INSERT INTO src VALUES (1, '{"f0":"a","f1":["b","c"],"f2":"d"}')`,
    `INSERT INTO src VALUES (2, '{"f0":"x","f3":["y"],"f4":"z","f11":["q","r"]}')`,
    `UPDATE src SET data = '{"f0":"a2","f1":["c"],"f5":["e"],"f6":"g"}' WHERE _id = 1`,
    `UPDATE src SET data = data WHERE _id = 2`,
    `INSERT INTO src VALUES (3, '{"f2":1,"f6":"n"}')`,
    `DELETE FROM src WHERE _id = 2`,
    `UPDATE src SET data = '{"f7":["m","m"],"f8":"o"}' WHERE _id = 3`,
  ];
  const dump = (sql: Sql) => rows(sql, `SELECT sourceId, path, "target" FROM bindings ORDER BY sourceId, path`);

  it("gives the same bindings and guard errors as the original layer", async () => {
    const schema = referenceSchema(12);
    const out = new SchemaTransform().migration("t6", schema);
    expect(out.rewrites.filter((r) => r.rule === "T6").length).toBeGreaterThan(4);
    // Every output statement passes the static checks, including branches read per statement.
    for (const s of out.statements) expect(checkStatement(s)).toEqual([]);
    const original = await withObject((sql) => {
      // 12 branches: the original view is refused outright.
      expect(errorOf(() => exec(sql, splitStatements(schema)))).toMatch(/too many terms in compound SELECT/);
      return null;
    });
    void original;
    // Reference: the original layer with the compound limit respected (T1 only), on the same writes.
    const reference = new SchemaTransform({ referenceMinBranches: 1_000, limits: { unionBranchesRead: Infinity } }).migration("t1-only", schema);
    const expected = await withObject((sql) => {
      exec(sql, reference.statements);
      const outcomes = writes.map((w) => errorOf(() => sql.exec(w)));
      const direct = errorOf(() => sql.exec(`INSERT INTO bindings VALUES (1, '$.f9', 'zz')`));
      return { outcomes, bindings: dump(sql), direct };
    });
    const actual = await withObject((sql) => {
      exec(sql, out.statements);
      const outcomes = writes.map((w) => errorOf(() => sql.exec(w)));
      const direct = errorOf(() => sql.exec(`INSERT INTO bindings VALUES (1, '$.f9', 'zz')`));
      const scratch = rows(sql, `SELECT count(*) AS n FROM "_gonvex_st_referenceExpected_src__rows"`);
      return { outcomes, bindings: dump(sql), direct, scratch };
    });
    expect(expected.bindings.length).toBeGreaterThan(5);
    expect(expected.outcomes).toEqual([null, null, null, null, null, null, null]);
    expect(actual.outcomes).toEqual(expected.outcomes);
    expect(actual.bindings).toEqual(expected.bindings);
    expect(actual.direct).toEqual(expected.direct);
    expect(actual.direct).toMatch(/relationship projection must match its source/);
    expect(actual.scratch).toEqual([{ n: 0 }]);
  });

  it("refuses a writer of a guarded table that does not fill", () => {
    const t = new SchemaTransform();
    t.migration("a", referenceSchema(3));
    expect(() => t.migration("b", "CREATE TRIGGER sneaky AFTER INSERT ON owners BEGIN INSERT INTO bindings VALUES (NEW.ownerId, '$', NULL); END;")).toThrow(TransformError);
  });

  it("drops every piece with the view", () => {
    const t = new SchemaTransform();
    t.migration("a", referenceSchema(3));
    expect(t.addedObjects().length).toBe(3 + 2 + 1 + 1);
    const out = t.migration("b", `DROP VIEW "referenceExpected_src";`);
    expect(out.statements.length).toBe(1 + 6);
    expect(t.addedObjects()).toEqual([]);
  });
});

describe("T5 capture without custom functions", () => {
  it("record images and the per-record flush match the runtime's JSON", async () => {
    const t = new SchemaTransform();
    const out = t.runtime("records", RECORD_SCHEMA, { records: [ITEMS_RECORD] });
    expect(out.statements.join("\n")).not.toMatch(/gonvex_record_value/i);
    const record = recordView(ITEMS_RECORD);
    await withObject((sql, storage) => {
      exec(sql, out.statements);
      sql.exec("INSERT INTO _gonvex_identity_keys VALUES (1, 7, 'k1'), (2, 7, 'k2')");
      const commit = (statements: string[]) =>
        storage.transactionSync(() => {
          exec(sql, statements);
          for (const { table_name } of rows<{ table_name: string }>(sql, RECORD_PENDING_NAMES_SQL)) {
            expect(table_name).toBe("items");
            sql.exec(recordFlushSql(record));
          }
          sql.exec(RECORD_PENDING_CLEANUP_SQL);
        });
      commit([`INSERT INTO "recordStorage_items" VALUES (1, 'a', 1, '{"x":[1,2],"y":"s"}', 1.5, x'00ff')`]);
      commit([`UPDATE "recordStorage_items" SET done = 0, score = 2 WHERE _id = 1`, `INSERT INTO "recordStorage_items" VALUES (2, 'b', NULL, NULL, NULL, NULL)`]);
      commit([`UPDATE "recordStorage_items" SET name = name WHERE _id = 2`]);
      commit([`DELETE FROM "recordStorage_items" WHERE _id = 2`]);
      const changes = rows<{ row_id: string; operation: string; new_value: string }>(sql, "SELECT row_id, operation, new_value FROM _gonvex_sync_changes ORDER BY event_id");
      expect(changes.map((c) => [c.row_id, c.operation, JSON.parse(c.new_value)])).toEqual([
        ["k1", "insert", [null, { _id: "k1", name: "a", done: true, meta: { x: [1, 2], y: "s" }, score: 1.5, raw: "00ff" }, []]],
        ["k1", "update", [{ _id: "k1", name: "a", done: true, meta: { x: [1, 2], y: "s" }, score: 1.5, raw: "00ff" }, { _id: "k1", name: "a", done: false, meta: { x: [1, 2], y: "s" }, score: 2, raw: "00ff" }, ["done", "score"]]],
        ["k2", "insert", [null, { _id: "k2", name: "b", done: null, meta: null, score: null, raw: null }, []]],
        ["k2", "delete", [{ _id: "k2", name: "b", done: null, meta: null, score: null, raw: null }, null, []]],
      ]);
      // Key order is column order, as serde_json preserve_order writes it.
      expect(changes[0]!.new_value).toBe('[null,{"_id":"k1","name":"a","done":true,"meta":{"x":[1,2],"y":"s"},"score":1.5,"raw":"00ff"},[]]');
    });
  });

  it("gonvex_capture triggers become json_object row images", async () => {
    const out = new SchemaTransform().runtime("capture", `CREATE TABLE _gonvex_sync_changes(event_id INTEGER PRIMARY KEY, table_name TEXT, row_id TEXT, operation TEXT, new_value JSON); ${CAPTURED_TABLE}`, {
      tables: [NOTES_SHAPE],
    });
    await withObject((sql) => {
      exec(sql, out.statements);
      sql.exec(`INSERT INTO notes VALUES ('n1', 'hi', 1, 5, '{"a":1}')`);
      sql.exec(`UPDATE notes SET body = 'hi' WHERE id = 'n1'`); // no change: NULL, as gonvex_capture returns
      sql.exec(`UPDATE notes SET n = 6, extra = 'not json' WHERE id = 'n1'`); // malformed JSON falls back to the text
      sql.exec(`DELETE FROM notes WHERE id = 'n1'`);
      const changes = rows<{ operation: string; new_value: string | null }>(sql, "SELECT operation, new_value FROM _gonvex_sync_changes ORDER BY event_id");
      expect(changes.map((c) => [c.operation, c.new_value === null ? null : JSON.parse(c.new_value)])).toEqual([
        ["insert", [null, { id: "n1", body: "hi", pinned: true, n: 5, extra: { a: 1 } }, []]],
        ["update", null],
        ["update", [{ id: "n1", body: "hi", pinned: true, n: 5, extra: { a: 1 } }, { id: "n1", body: "hi", pinned: true, n: 6, extra: "not json" }, ["n", "extra"]]],
        ["delete", [{ id: "n1", body: "hi", pinned: true, n: 6, extra: "not json" }, null, []]],
      ]);
    });
  });

  it("fails loudly without the catalog entry", () => {
    expect(() => new SchemaTransform().runtime("records", RECORD_SCHEMA)).toThrow(/catalog: gonvex_record_value\(\): record view items is not in the catalog/);
  });
});

describe("loud failures, chunks, determinism", () => {
  it("names migration, object and limit for an unknown construct", () => {
    try {
      new SchemaTransform().migration("0042_bad.sql", "CREATE TABLE t(a TEXT); CREATE VIEW w AS SELECT a FROM t WHERE a REGEXP 'x';");
      expect.unreachable();
    } catch (error) {
      expect(error).toBeInstanceOf(TransformError);
      expect(error).toMatchObject({ migration: "0042_bad.sql", object: "w", limit: "custom-function" });
    }
    expect(() => new SchemaTransform().migration("m", "CREATE VIEW w AS SELECT my_udf(1)")).toThrow(/migration m: w: custom-function/);
    expect(() => new SchemaTransform().migration("m", `CREATE VIEW w AS SELECT '${"x".repeat(100_000)}'`)).toThrow(/migration m: w: (statement|ddl)-bytes/);
  });

  it("chunks at 200 statements and keeps a TEMP table's lifetime in one chunk", () => {
    const statements = Array.from({ length: 450 }, (_, k) => `INSERT INTO t VALUES (${k})`);
    expect(chunkStatements(statements).map((c) => c.length)).toEqual([200, 200, 50]);
    expect(chunkStatements(statements, {}, [[190, 205]]).map((c) => c.length)).toEqual([190, 200, 60]);
    expect(chunkStatements(statements, { maxBytes: 1_000 }).every((c) => c.join("").length <= 1_000)).toBe(true);
  });

  it("never ends a chunk inside a deferred-FK span or a T6 fill", async () => {
    // Scope rows reference an always-empty guard table (deferred): they must be
    // deleted before COMMIT, so the INSERT..DELETE span shares one chunk.
    const setup = `${referenceSchema(3)}
CREATE TABLE guard(id INTEGER PRIMARY KEY);
CREATE TABLE scope(id INTEGER, g INTEGER NOT NULL DEFAULT 0 REFERENCES guard(id) DEFERRABLE INITIALLY DEFERRED);`;
    const body = [
      ...Array.from({ length: 195 }, (_, k) => `INSERT INTO src VALUES (${k + 1}, '{}')`),
      "INSERT INTO scope(id) VALUES (1)",
      ...Array.from({ length: 10 }, (_, k) => `UPDATE src SET data = '{"f0":"v${k}"}' WHERE _id = ${k + 1}`),
      `INSERT INTO owners(sourceName, ownerId) SELECT 'src', _id FROM src`,
      `INSERT INTO bindings SELECT e.ownerId, e.path, e."target" FROM "referenceExpected_src" e WHERE 1 ON CONFLICT DO NOTHING`,
      "DELETE FROM owners",
      "DELETE FROM scope",
    ].join(";\n");
    const t = new SchemaTransform();
    const first = t.migration("setup", setup);
    const out = t.migration("backfill", body);
    const at = (needle: string) => out.chunks.findIndex((c) => c.some((s) => s.startsWith(needle)));
    expect(out.chunks.length).toBeGreaterThan(1);
    expect(at("INSERT INTO scope")).toBe(at("DELETE FROM scope"));
    const fill = out.chunks.findIndex((c) => c.some((s) => s.includes("__queue")));
    expect(out.chunks[fill]!.some((s) => s.startsWith("INSERT INTO bindings"))).toBe(true);
    await withObject((sql, storage) => {
      exec(sql, first.statements);
      for (const chunk of out.chunks) storage.transactionSync(() => exec(sql, chunk));
      expect(rows(sql, "SELECT count(*) AS n FROM scope")).toEqual([{ n: 0 }]);
    });
  });

  it("is deterministic byte for byte", () => {
    const input = [
      { name: "a", sql: referenceSchema(9) },
      { name: "b", sql: `CREATE TEMP TABLE g(x); CREATE INDEX i ON src(${deepFold("data", 120).sql});` },
    ];
    const run = () => {
      const { migrations, transform } = transformMigrations(input);
      return JSON.stringify([migrations, transform.runtime("records", RECORD_SCHEMA, { records: [ITEMS_RECORD] }), transform.addedObjects()]);
    };
    expect(run()).toBe(run());
  });
});
