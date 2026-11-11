// Port of packages/module-sdk/test/data-write-sqlite.test.mjs: the SDK's batch
// UPDATE runs against Durable Object SQLite through the storage layer (the
// runtime's module SQL path, `$1` placeholders included) and is compared
// with the previous `UPDATE ... FROM json_each(array)` statement on the same
// rows. The legacy statement and the dumps are fixture SQL, run directly.
import { expect, test } from "vitest";
import { updateDataRows } from "@gonvex/module-sdk";
import { objectFor } from "../helpers";

type Row = Record<string, SqlStorageValue>;

async function database() {
  const db = objectFor();
  await db.run((_tenant, state) => {
    state.storage.sql.exec(`CREATE TABLE members (id TEXT PRIMARY KEY, permissions JSON NOT NULL DEFAULT '{}', role TEXT, rank INTEGER)`);
    state.storage.sql.exec(`CREATE TABLE counters (n INTEGER PRIMARY KEY, label TEXT)`);
  });
  return db;
}
type Database = Awaited<ReturnType<typeof database>>;

/** The module's `db`, served by one committed Reducer invocation on the storage layer. */
async function update(db: Database, table: string, updates: Parameters<typeof updateDataRows>[2], key: string, statements: string[]) {
  const result = await db.invoke("reducer", async (session) => {
    await updateDataRows({
      query: async (sql: string, parameters?: readonly unknown[]) => {
        statements.push(sql);
        return session.query(sql, parameters ?? []);
      },
    } as Parameters<typeof updateDataRows>[0], table, updates, key);
  });
  if (!result.ok) throw result.error;
}

const legacy = (db: Database, table: string, key: string, updates: { id: string; fields: Record<string, unknown> }[]) => {
  const columns = Object.keys(updates[0]!.fields);
  const value = (name: string) => `CASE WHEN (SELECT upper(type) FROM pragma_table_info('${table}') WHERE name = '${name}') LIKE '%JSON%' THEN nullif(source.value -> '$.${name}', 'null') ELSE source.value ->> '$.${name}' END`;
  return db.sql(`UPDATE "${table}" SET ${columns.map((name) => `"${name}" = ${value(name)}`).join(", ")} FROM json_each(?) AS source WHERE "${table}"."${key}" = source.value ->> '$.${key}'`,
    JSON.stringify(updates.map((entry) => ({ ...entry.fields, [key]: entry.id }))));
};
const dump = (db: Database, table: string) => db.sql<Row>(`SELECT * FROM ${table} ORDER BY 1`);

for (const [name, ids] of [
  ["label-safe ids", ["a", "b.c", "123e4567-e89b-12d3-a456-426614174000", "x:y", "9", "__proto__"]],
  ["ids that need the json_each lookup", ["$x", 'a"b', "a[0]", "sp ace", "é", "o'k", "plain"]],
] as const) {
  test(`batch update by primary key matches the json_each join (${name})`, async () => {
    const before = async (db: Database) => {
      for (const [index, id] of [...ids, "untouched"].entries()) {
        await db.sql("INSERT INTO members (id, permissions, role, rank) VALUES (?, ?, ?, ?)", id, JSON.stringify({ old: index }), "member", index);
      }
    };
    const updates = ids.map((id, index) => ({ id, fields: { permissions: { new: index, list: [id] }, role: index % 2 ? null : `r${index}`, rank: index * 10 } }));
    const expected = await database(); await before(expected); await legacy(expected, "members", "id", updates);
    const actual = await database(); await before(actual);
    const statements: string[] = [];
    await update(actual, "members", updates, "id", statements);
    expect(statements.length).toBe(1);
    expect(statements[0]).not.toMatch(/FROM json_each\(\$1\) AS source WHERE "members"/);
    expect(await dump(actual, "members")).toEqual(await dump(expected, "members"));
  });
}

test("batch update finds integer primary keys and skips missing rows", async () => {
  const seed = async (db: Database) => { for (const n of [1, 2, 3]) await db.sql("INSERT INTO counters VALUES (?, ?)", n, `n${n}`); };
  const updates = [{ id: "2", fields: { label: "two" } }, { id: "7", fields: { label: "seven" } }];
  const expected = await database(); await seed(expected); await legacy(expected, "counters", "n", updates);
  const actual = await database(); await seed(actual);
  await update(actual, "counters", updates, "n", []);
  expect(await dump(actual, "counters")).toEqual(await dump(expected, "counters"));
  expect((await dump(actual, "counters"))[1]!.label).toBe("two");
});

test("batch update looks rows up by key instead of scanning the table per batch row", async () => {
  const db = await database();
  await db.run((_tenant, state) => { for (let index = 0; index < 2000; index++) state.storage.sql.exec("INSERT INTO members (id) VALUES (?)", `m${index}`); });
  const statements: string[] = [];
  const updates = Array.from({ length: 500 }, (_, index) => ({ id: `m${index * 4}`, fields: { role: "admin" } }));
  await update(db, "members", updates, "id", statements);
  const plan = (await db.sql<{ detail: string }>(`EXPLAIN QUERY PLAN ${statements[0]!.replace(/\$(\d+)/g, "?$1")}`, "{}")).map((row) => row.detail).join(" | ");
  expect(plan).not.toMatch(/SCAN source/);
  expect((await db.sql<{ n: number }>("SELECT count(*) AS n FROM members WHERE role = 'admin'"))[0]!.n).toBe(500);
});
