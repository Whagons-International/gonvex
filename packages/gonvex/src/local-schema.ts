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
export type SchemaDialect = "sqlite" | "postgres";
/** PG constraint metadata is preserved even for internal tables that cannot
 * participate in the single-key Local Replica contract. */
export type CompiledLocalSchema = LocalSchema & Record<string, LocalSchema[string] & {
  primaryKey?: string[]; localKeyUnsupported?: "composite";
}>;

export async function compileLocalSchema(migrations: readonly { name: string; sql: string }[], dialect: SchemaDialect = "sqlite"): Promise<CompiledLocalSchema> {
  if (dialect === "postgres") return compilePostgresSchema(migrations);
  if (dialect !== "sqlite") throw new Error(`Unsupported schema dialect: ${dialect}`);
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
    const schema: CompiledLocalSchema = {};
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


/** Execute original PG migrations in the embedded PostgreSQL engine. No SQL
 * rewriting, failed-statement skipping, or conversion to SQLite affinities. */
async function compilePostgresSchema(migrations: readonly { name: string; sql: string }[]): Promise<CompiledLocalSchema> {
  const [{ PGlite }, { pg_trgm }, { pgcrypto }] = await Promise.all([
    import("@electric-sql/pglite"), import("@electric-sql/pglite/contrib/pg_trgm"), import("@electric-sql/pglite/contrib/pgcrypto"),
  ]);
  const db = new PGlite({ extensions: { pg_trgm, pgcrypto } });
  try {
    await db.exec(`CREATE TABLE members (
      id text PRIMARY KEY, account_id text NOT NULL UNIQUE, status text NOT NULL DEFAULT 'active',
      display_name text NOT NULL DEFAULT '', avatar_url text NOT NULL DEFAULT '', role text NOT NULL DEFAULT 'member',
      permissions jsonb NOT NULL DEFAULT '{}'::jsonb, membership_revision bigint NOT NULL DEFAULT 1,
      created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
    )`);
    for (const migration of migrations) {
      try { await db.exec(migration.sql); }
      catch (error) { throw new Error(`PostgreSQL schema compilation failed in ${migration.name}: ${error instanceof Error ? error.message : String(error)}`, { cause: error }); }
    }
    const { rows } = await db.query<{ table_name: string; column_name: string; type: string; nullable: boolean; column_default: string | null; primary_key: number | null; key_columns: number | null }>(`
      SELECT c.relname AS table_name, a.attname AS column_name,
        format_type(a.atttypid,a.atttypmod) AS type, NOT a.attnotnull AS nullable,
        pg_get_expr(d.adbin,d.adrelid) AS column_default,
        (SELECT k.position::int FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,position)
          WHERE i.indrelid=c.oid AND i.indisprimary AND k.attnum=a.attnum) AS primary_key,
        (SELECT i.indnkeyatts::int FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary) AS key_columns
      FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
      JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
      WHERE n.nspname='public' AND c.relkind='r' ORDER BY c.relname,a.attnum`);
    const schema: CompiledLocalSchema = {};
    for (const row of rows) {
      if (row.table_name.startsWith("_gonvex")) continue;
      const table = schema[row.table_name] ??= { key: "_id", columns: {} };
      if (row.primary_key !== null) {
        (table.primaryKey ??= [])[row.primary_key - 1] = row.column_name;
        if (row.key_columns === 1) table.key = row.column_name;
        else table.localKeyUnsupported = "composite";
      }
      table.columns[row.column_name] = { type: row.type, nullable: row.nullable,
        ...(row.column_default === null ? {} : { default: row.column_default }) };
    }
    return schema;
  } finally { await db.close(); }
}
