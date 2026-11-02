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
    for (const migration of migrations) {
      try { db.exec(migration.sql); }
      catch (error) { throw new Error(`Local schema compilation failed in ${migration.name}: ${error instanceof Error ? error.message : String(error)}`, { cause: error }); }
    }
    const rows = db.prepare(`SELECT t.name AS table_name, c.name AS column_name, c.type AS type,
      c."notnull" AS not_null, c.dflt_value AS column_default, c.pk AS primary_key
      FROM sqlite_schema AS t JOIN pragma_table_info(t.name) AS c
      WHERE t.type = 'table' AND t.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
      ORDER BY t.name, c.cid`).all() as {
      table_name: string; column_name: string; type: string; not_null: number;
      column_default: string | null; primary_key: number;
    }[];
    const schema: LocalSchema = {};
    for (const row of rows) {
      if (row.table_name.startsWith("_gonvex")) continue;
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
    return schema;
  } finally { db.close(); }
}
