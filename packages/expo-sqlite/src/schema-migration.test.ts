import { afterEach, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import { ExpoSQLiteLocalReplicaStorage, type ExpoSQLiteDatabase } from "./index.js";

const databases: DatabaseSync[] = [];
afterEach(() => { for (const db of databases.splice(0)) db.close(); });

function fixture() {
  const db = new DatabaseSync(":memory:");
  databases.push(db);
  const adapter: ExpoSQLiteDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...params) { return db.prepare(sql).run(...params as any[]); },
    async getAllAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).all(...params as any[]) as T[]; },
    async getFirstAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).get(...params as any[]) as T ?? null; },
    async withTransactionAsync(run) {
      db.exec("BEGIN");
      try { await run(); db.exec("COMMIT"); }
      catch (error) { db.exec("ROLLBACK"); throw error; }
    },
  };
  return { db, adapter, storage: () => new ExpoSQLiteLocalReplicaStorage(adapter) };
}

const oldScoped = `
  CREATE TABLE _gonvex_replica_entities(scope TEXT, entity TEXT, id TEXT, value TEXT, PRIMARY KEY(scope,entity,id));
  CREATE TABLE _gonvex_replica_queries(scope TEXT, signature TEXT, value TEXT, PRIMARY KEY(scope,signature));
  CREATE TABLE _gonvex_replica_meta(scope TEXT, key TEXT, value TEXT, PRIMARY KEY(scope,key));
  INSERT INTO _gonvex_replica_entities VALUES('alice','tasks','a','{"_id":"a","parentId":"p"}');
  INSERT INTO _gonvex_replica_meta VALUES('alice','cursor','{"epoch":"e","revision":7}');
`;

it("retains historical scoped rows and cursor on a plain load", async () => {
  const { db, storage } = fixture();
  db.exec(oldScoped + "PRAGMA user_version=2;");
  expect(await storage().load("alice")).toMatchObject({
    entities: { tasks: { a: { _id: "a", parentId: "p" } } }, cursor: { epoch: "e", revision: 7 },
  });
});

it("copies unscoped historical rows only into the default owner", async () => {
  const { db, storage } = fixture();
  db.exec(`CREATE TABLE _gonvex_replica_entities(entity TEXT,id TEXT,value TEXT,PRIMARY KEY(entity,id));
    INSERT INTO _gonvex_replica_entities VALUES('tasks','a','{"_id":"a","parentId":"p"}');`);
  const disk = storage();
  expect((await disk.load())?.entities.tasks.a).toEqual({ _id: "a", parentId: "p" });
  expect(await disk.load("alice")).toBeUndefined();
});

it.each([0, 2, 3, 700])("rebuilds historical scoped lookups without consuming application user_version=%s", async appVersion => {
  const { db, storage } = fixture();
  db.exec(oldScoped + `PRAGMA user_version=${appVersion};`);
  const disk = storage();
  const result = await disk.withReadView("alice", { tasks: { key: "_id", complete: true } },
    view => view.select({ table: "tasks", where: { column: "parentId", op: "eq", value: "p" } }));
  expect(result).toEqual({ rows: [{ _id: "a", parentId: "p" }], complete: true });
  expect(db.prepare("PRAGMA user_version").get()).toEqual({ user_version: appVersion });
  expect((await storage().load("alice"))?.cursor).toEqual({ epoch: "e", revision: 7 });
});

it.each([4, 0, -1, 1.5, "unknown"])("refuses an unsupported Gonvex schema version before changing data: %s", async version => {
  const { db, storage } = fixture();
  db.exec(oldScoped + "PRAGMA user_version=700; CREATE TABLE _gonvex_replica_schema(id INTEGER PRIMARY KEY, version);");
  db.prepare("INSERT INTO _gonvex_replica_schema VALUES(1,?)").run(version);
  const snapshot = () => ({
    schema: db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").all(),
    entities: db.prepare("SELECT * FROM _gonvex_replica_entities").all(),
    meta: db.prepare("SELECT * FROM _gonvex_replica_meta").all(),
    version: db.prepare("SELECT * FROM _gonvex_replica_schema").all(),
    appVersion: db.prepare("PRAGMA user_version").get(),
  });
  const before = snapshot();
  await expect(storage().load("alice")).rejects.toThrow(/schema|version/i);
  expect(snapshot()).toEqual(before);
});

it("rolls back lookup initialization and its version checkpoint, then permits retry", async () => {
  const { db, adapter, storage } = fixture();
  db.exec(oldScoped + "PRAGMA user_version=700;");
  const original = adapter.execAsync;
  let fail = true;
  adapter.execAsync = async sql => {
    if (fail && sql.includes("INSERT OR REPLACE INTO _gonvex_replica_lookup")) {
      fail = false;
      throw new Error("migration interrupted");
    }
    return original(sql);
  };
  const disk = storage();
  await expect(disk.load("alice")).rejects.toThrow("migration interrupted");
  expect(db.prepare("SELECT name FROM sqlite_master WHERE name IN ('_gonvex_replica_lookup','_gonvex_replica_schema')").all()).toEqual([]);
  expect((await disk.load("alice"))?.entities.tasks.a).toEqual({ _id: "a", parentId: "p" });
  expect(db.prepare("PRAGMA user_version").get()).toEqual({ user_version: 700 });
});

it("does not overwrite a newer checkpoint installed before transaction admission", async () => {
  const { db, adapter, storage } = fixture();
  db.exec(oldScoped + "CREATE TABLE _gonvex_replica_schema(id INTEGER PRIMARY KEY,version INTEGER); INSERT INTO _gonvex_replica_schema VALUES(1,3);");
  const original = adapter.withTransactionAsync;
  adapter.withTransactionAsync = async run => {
    // A different connection/version can commit before our transaction begins.
    db.exec("UPDATE _gonvex_replica_schema SET version=4");
    return original(run);
  };
  await expect(storage().load("alice")).rejects.toThrow(/schema|version/i);
  expect(db.prepare("SELECT version FROM _gonvex_replica_schema").get()).toEqual({ version: 4 });
  expect(db.prepare("SELECT value FROM _gonvex_replica_entities WHERE id='a'").get()).toEqual({ value: '{"_id":"a","parentId":"p"}' });
});
