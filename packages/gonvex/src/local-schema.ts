import{sqliteSchemaBootstrap}from'@gonvex/local-runtime/schema';
import type { LocalSchema } from "@gonvex/local-runtime/schema";

// The framework's tenant identity table exists before application migrations.
// Keep this in parity with TENANT_IDENTITY_SQL in rust/crates/store/src/provision.rs.
const identitySchema = `CREATE TABLE members (
  id TEXT PRIMARY KEY, account_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'active',
  display_name TEXT NOT NULL DEFAULT '', avatar_url TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT 'member', permissions JSON NOT NULL DEFAULT '{}',
  membership_revision INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
)`;

type SqliteModule = typeof import("node:sqlite");

async function loadSqlite(): Promise<SqliteModule> {
  try {
    return await import("node:sqlite");
  } catch (error) {
    throw new Error(
      `Local schema compilation needs Node's built-in node:sqlite (Node 22.13 or newer); running ${process.version}`,
      { cause: error },
    );
  }
}

/** Compile schema using SQLite itself, the engine tenants run on: apply the
 * migrations to an in-memory database and read the declared columns back. */
export async function compileLocalSchema(migrations: readonly { name: string; sql: string }[]): Promise<LocalSchema> {
  const { DatabaseSync } = await loadSqlite();
  const db = new DatabaseSync(":memory:");
  try {
    db.exec("PRAGMA foreign_keys = ON");
    db.exec(identitySchema);
    db.exec(sqliteSchemaBootstrap);
    for (const migration of migrations) {
      const rebuild=migration.sql.split(/\r?\n/).some(line=>line.trim()==='-- gonvex:foreign-key-rebuild');
      try {
        if(rebuild)db.exec('PRAGMA foreign_keys=OFF');
        db.exec('BEGIN');
        db.exec(migration.sql);
        if(rebuild){
          const invalid=db.prepare('PRAGMA foreign_key_check').all();
          if(invalid.length)throw new Error(`Foreign-key rebuild rejected: ${JSON.stringify(invalid[0])}`);
        }
        db.exec('COMMIT');
      }
      catch (error) { throw new Error(`Local schema compilation failed in ${migration.name}: ${error instanceof Error ? error.message : String(error)}`, { cause: error }); }
      finally { if(db.isTransaction)db.exec('ROLLBACK');if(rebuild)db.exec('PRAGMA foreign_keys=ON'); }
    }
    const rows = db.prepare(`SELECT t.name AS table_name, c.name AS column_name, c.type AS type,
      c."notnull" AS not_null, c.dflt_value AS column_default, c.pk AS primary_key
      FROM pragma_table_list AS t JOIN pragma_table_info(t.name) AS c
      WHERE t.schema = 'main' AND t.type = 'table' AND t.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
      ORDER BY t.name, c.cid`).all() as {
      table_name: string; column_name: string; type: string; not_null: number;
      column_default: string | null; primary_key: number;
    }[];
    const records=db.prepare('SELECT name,storage_table,schema FROM _gonvex_record_views ORDER BY name').all() as {name:string;storage_table:string;schema:string}[];
    const recordNames=new Set(records.map(record=>record.name));
    const storageTables=new Set(records.map(record=>record.storage_table));
    const internal=new Set((db.prepare("SELECT name FROM _gonvex_table_options WHERE capture<>'entity'").all() as {name:string}[]).map(row=>row.name));
    const schema: LocalSchema = {};
    for (const row of rows) {
      if (row.table_name.startsWith("_gonvex") || row.table_name.startsWith("gonvex_") || internal.has(row.table_name) || storageTables.has(row.table_name)) continue;
      const table = schema[row.table_name] ??= { key: "_id", columns: {} };
      // The first primary key column is the row key (composite keys keep
      // their leading column, as before).
      if (row.primary_key === 1) table.key = row.column_name;
      table.columns[row.column_name] = {
        // Declared type as written, lower-cased; local-runtime classifies it
        // with SQLite's declared-type rules (JSON, BOOL, INT, REAL, TEXT).
        type: row.type.toLowerCase(),
        // SQLite lets non-INTEGER primary keys hold NULL unless declared NOT
        // NULL; Gonvex row keys are always required.
        nullable: !row.not_null && !row.primary_key,
        ...(row.column_default ? { default: row.column_default } : {}),
      };
    }
    for(const record of records){
      const logical=JSON.parse(record.schema) as LocalSchema[string];
      const source=db.prepare("SELECT type FROM sqlite_schema WHERE name=?").get(record.storage_table) as {type:string}|undefined;
      const view=db.prepare("SELECT type FROM sqlite_schema WHERE name=?").get(record.name) as {type:string}|undefined;
      if(source?.type!=='table'||!view||(record.name!==record.storage_table&&view.type!=='view'))throw new Error(`Invalid record view ${record.name}`);
      const projected=new Set((db.prepare('SELECT name FROM pragma_table_info(?)').all(record.name) as {name:string}[]).map(row=>row.name));
      if(!logical.columns?.[logical.key]||projected.size!==Object.keys(logical.columns).length||Object.keys(logical.columns).some(column=>!projected.has(column)))throw new Error(`Record view ${record.name} differs from its declared schema`);
      schema[record.name]=logical;
    }
    for(const[name,table]of Object.entries(schema)){
      if(recordNames.has(name))continue;
      const rows=db.prepare('SELECT * FROM pragma_foreign_key_list(?) ORDER BY id,seq').all(name) as {id:number;seq:number;table:string;from:string;to:string;on_delete:string;on_update:string}[];
      const keys=new Map<number,{columns:string[];targetTable:string;targetColumns:string[];onDelete:string;onUpdate:string}>();
      for(const row of rows){
        if(!keys.has(row.id))keys.set(row.id,{columns:[],targetTable:row.table,targetColumns:[],onDelete:row.on_delete,onUpdate:row.on_update});
        const key=keys.get(row.id)!;key.columns.push(row.from);key.targetColumns.push(row.to);
      }
      if(keys.size)table.foreignKeys=[...keys.values()];
    }
    return schema;
  } finally { db.close(); }
}
