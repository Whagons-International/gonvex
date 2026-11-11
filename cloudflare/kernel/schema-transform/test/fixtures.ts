// Small schemas with the shapes the Whagons schema has, for tests that run the
// original and the transformed DDL side by side in Durable Object SQLite.

/** A reference-integrity layer of the generator's shape (scripts/schema/reference-sql.mjs in whagons5-client). */
export function referenceSchema(branches: number): string {
  const ctes = ["r0 AS MATERIALIZED (SELECT s._id ownerId, s.data value, '$' path FROM src s WHERE s._id IN (SELECT ownerId FROM owners WHERE sourceName='src'))"];
  const terms: string[] = [];
  for (let k = 0; k < branches; k += 1) {
    // Branch k reads field fk, as a scalar (even k) or an array of ids (odd k).
    ctes.push(
      k % 2 === 0
        ? `r${k + 1} AS MATERIALIZED (SELECT n.ownerId, n.value -> '$.f${k}' value, n.path||'.f${k}' path FROM r0 n WHERE json_type(n.value,'$.f${k}') IS NOT NULL)`
        : `r${k + 1} AS MATERIALIZED (SELECT n.ownerId, CASE WHEN j.type IN ('object','array') THEN j.value ELSE json_quote(j.value) END value, n.path||'.f${k}['||j.key||']' path FROM r0 n, json_each(n.value,'$.f${k}') j)`,
    );
    terms.push(`SELECT n.ownerId,n.path,CASE WHEN json_type(n.value)='text' THEN json_extract(n.value,'$') END "target",CASE WHEN json_type(n.value)='text' THEN 'f${k % 3}' ELSE 'invalid' END fingerprint FROM r${k + 1} n`);
  }
  ctes.push(`u AS MATERIALIZED (${terms.join("\nUNION ALL\n")})`);
  const view = `CREATE VIEW "referenceExpected_src" AS WITH RECURSIVE ${ctes.join(",\n")}
SELECT ownerId,path,CASE WHEN count(DISTINCT fingerprint)=1 AND min(fingerprint)<>'invalid' THEN max("target") END "target" FROM u GROUP BY ownerId,path`;
  const scopeIn = (row: string) => `INSERT INTO owners(sourceName,ownerId) VALUES('src',${row}._id) ON CONFLICT(sourceName,ownerId) DO UPDATE SET depth=depth+1`;
  const scopeOut = (row: string) =>
    `UPDATE owners SET depth=depth-1 WHERE sourceName='src' AND ownerId=${row}._id; DELETE FROM owners WHERE sourceName='src' AND ownerId=${row}._id AND depth=0`;
  const insertBindings = `INSERT INTO bindings(sourceId,path,"target") SELECT e.ownerId,e.path,e."target" FROM (SELECT * FROM "referenceExpected_src" WHERE ownerId=NEW._id) e WHERE 1 ON CONFLICT(sourceId,path) DO NOTHING`;
  return `
CREATE TABLE src(_id INTEGER PRIMARY KEY, data TEXT);
CREATE TABLE owners(sourceName TEXT NOT NULL, ownerId INTEGER NOT NULL, depth INTEGER NOT NULL DEFAULT 1 CHECK(depth>=0), PRIMARY KEY(sourceName, ownerId)) WITHOUT ROWID;
${view};
CREATE TABLE bindings(sourceId INTEGER NOT NULL REFERENCES src(_id) ON DELETE CASCADE, path TEXT NOT NULL, "target", PRIMARY KEY(sourceId, path));
CREATE TRIGGER bindings_insert_guard BEFORE INSERT ON bindings WHEN NOT EXISTS(SELECT 1 FROM owners WHERE sourceName='src' AND ownerId=NEW.sourceId AND depth>0) OR NOT EXISTS(SELECT 1 FROM "referenceExpected_src" e WHERE e.ownerId=NEW.sourceId AND e.path=NEW.path AND e."target" IS NEW."target") BEGIN SELECT RAISE(ABORT,'relationship projection must match its source'); END;
CREATE TRIGGER bindings_delete_guard BEFORE DELETE ON bindings WHEN NOT EXISTS(SELECT 1 FROM owners WHERE sourceName='src' AND ownerId=OLD.sourceId AND depth>0) OR EXISTS(SELECT 1 FROM "referenceExpected_src" e WHERE e.ownerId=OLD.sourceId AND e.path=OLD.path AND e."target" IS OLD."target") BEGIN SELECT RAISE(ABORT,'relationship projection is derived; change its source'); END;
CREATE TRIGGER src_insert AFTER INSERT ON src BEGIN ${scopeIn("NEW")}; ${insertBindings}; ${scopeOut("NEW")}; END;
CREATE TRIGGER src_update AFTER UPDATE OF data ON src WHEN NEW.data IS NOT OLD.data BEGIN ${scopeIn("NEW")}; DELETE FROM bindings WHERE sourceId=NEW._id AND NOT EXISTS(SELECT 1 FROM (SELECT * FROM "referenceExpected_src" WHERE ownerId=NEW._id) e WHERE e.ownerId=bindings.sourceId AND e.path=bindings.path AND e."target" IS bindings."target"); ${insertBindings}; ${scopeOut("NEW")}; END;
CREATE TRIGGER src_before_delete BEFORE DELETE ON src BEGIN ${scopeIn("OLD")}; END;
CREATE TRIGGER src_after_delete AFTER DELETE ON src BEGIN ${scopeOut("OLD")}; END;
`;
}

/** A logical record over normalized storage, with the runtime's record capture triggers (record_views::capture_triggers_sql). */
export const RECORD_SCHEMA = `
CREATE TABLE _gonvex_identity_keys(_id INTEGER PRIMARY KEY, domain INTEGER NOT NULL, value TEXT NOT NULL, UNIQUE(domain, value)) STRICT;
CREATE TABLE "recordStorage_items"("_id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT, "done" INTEGER, "meta" TEXT, "score" REAL, "raw" BLOB);
CREATE VIEW "items" AS SELECT i.value "_id", s."name", s."done", s."meta", s."score", s."raw" FROM "recordStorage_items" s JOIN _gonvex_identity_keys i ON i._id = s."_id" AND i.domain = 7;
CREATE TABLE _gonvex_record_pending(table_name TEXT NOT NULL, row_id TEXT NOT NULL, old_value TEXT, PRIMARY KEY(table_name, row_id)) STRICT, WITHOUT ROWID;
CREATE TABLE _gonvex_sync_changes(event_id INTEGER PRIMARY KEY AUTOINCREMENT, revision INTEGER, table_name TEXT NOT NULL, row_id TEXT NOT NULL, operation TEXT NOT NULL, old_value JSON, new_value JSON, changed_columns JSON NOT NULL DEFAULT '[]');
CREATE TRIGGER "_gonvex_sync_recordStorage_items_insert" BEFORE INSERT ON "recordStorage_items" BEGIN
INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT 'items',(SELECT value FROM _gonvex_identity_keys WHERE _id=NEW."_id"),gonvex_record_value('items',(SELECT value FROM _gonvex_identity_keys WHERE _id=NEW."_id")) WHERE NOT EXISTS(SELECT 1 FROM _gonvex_record_pending WHERE table_name='items' AND row_id=(SELECT value FROM _gonvex_identity_keys WHERE _id=NEW."_id"));
END;
CREATE TRIGGER "_gonvex_sync_recordStorage_items_update" BEFORE UPDATE ON "recordStorage_items" BEGIN
INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT 'items',(SELECT value FROM _gonvex_identity_keys WHERE _id=OLD."_id"),gonvex_record_value('items',(SELECT value FROM _gonvex_identity_keys WHERE _id=OLD."_id")) WHERE NOT EXISTS(SELECT 1 FROM _gonvex_record_pending WHERE table_name='items' AND row_id=(SELECT value FROM _gonvex_identity_keys WHERE _id=OLD."_id"));
END;
CREATE TRIGGER "_gonvex_sync_recordStorage_items_delete" BEFORE DELETE ON "recordStorage_items" BEGIN
INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT 'items',(SELECT value FROM _gonvex_identity_keys WHERE _id=OLD."_id"),gonvex_record_value('items',(SELECT value FROM _gonvex_identity_keys WHERE _id=OLD."_id")) WHERE NOT EXISTS(SELECT 1 FROM _gonvex_record_pending WHERE table_name='items' AND row_id=(SELECT value FROM _gonvex_identity_keys WHERE _id=OLD."_id"));
END;
`;

export const ITEMS_RECORD = {
  name: "items",
  storage_table: "recordStorage_items",
  key_column: "_id",
  schema: JSON.stringify({
    key: "_id",
    columns: { _id: { type: "text" }, name: { type: "text" }, done: { type: "boolean" }, meta: { type: "jsonb" }, score: { type: "double precision" }, raw: { type: "bytea" } },
  }),
};

/** The feed's plain table capture (feed::capture_triggers_sql) for a table `notes`. */
export const CAPTURED_TABLE = `
CREATE TABLE notes(id TEXT PRIMARY KEY, body TEXT, pinned BOOLEAN, n INTEGER, extra JSON);
CREATE TRIGGER "_gonvex_sync_notes_insert" AFTER INSERT ON "notes" BEGIN
  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)
  VALUES ('notes', CAST(NEW."id" AS TEXT), 'insert', gonvex_capture('notes', NEW.rowid, 0));
END;
CREATE TRIGGER "_gonvex_sync_notes_update" AFTER UPDATE ON "notes" BEGIN
  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)
  VALUES ('notes', CAST(NEW."id" AS TEXT), 'update', gonvex_capture('notes', NEW.rowid, 1));
END;
CREATE TRIGGER "_gonvex_sync_notes_delete" AFTER DELETE ON "notes" BEGIN
  INSERT INTO _gonvex_sync_changes (table_name, row_id, operation, new_value)
  VALUES ('notes', CAST(OLD."id" AS TEXT), 'delete', gonvex_capture('notes', OLD.rowid, 2));
END;
`;

export const NOTES_SHAPE = {
  name: "notes",
  columns: [
    { name: "id", declared: "TEXT", pk: 1 },
    { name: "body", declared: "TEXT", pk: 0 },
    { name: "pinned", declared: "BOOLEAN", pk: 0 },
    { name: "n", declared: "INTEGER", pk: 0 },
    { name: "extra", declared: "JSON", pk: 0 },
  ],
};

/** `replace()` pairs like the Whagons Unicode case fold, `n` levels deep around `inner`. */
export function deepFold(inner: string, n: number): { sql: string; pairs: Array<[string, string]> } {
  const pairs: Array<[string, string]> = [];
  for (let k = 0; k < n; k += 1) {
    const upper = String.fromCodePoint(0x410 + (k % 32));
    pairs.push(k % 32 === 31 ? ["-", " "] : [upper, upper.toLowerCase()]);
  }
  let sql = inner;
  for (const [a, b] of pairs) sql = `replace(${sql}, '${a}', '${b}')`;
  return { sql, pairs };
}

export function applyPairs(value: string, pairs: Array<[string, string]>): string {
  let s = value;
  for (const [a, b] of pairs) if (a !== "") s = s.split(a).join(b);
  return s;
}
