import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";

export type LocalColumn = { type: string; nullable: boolean; default?: string };
export type LocalTableSchema = { key: string; columns: Record<string, LocalColumn> };
export type LocalSchema = Record<string, LocalTableSchema>;

// The framework's tenant identity table exists before application migrations.
// Keep this in parity with TENANT_IDENTITY_SQL in postgres/src/provision.rs.
const identitySchema = `CREATE TABLE members (
  id text PRIMARY KEY, account_id text NOT NULL UNIQUE, status text NOT NULL DEFAULT 'active',
  display_name text NOT NULL DEFAULT '', avatar_url text NOT NULL DEFAULT '',
  role text NOT NULL DEFAULT 'member', permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
  membership_revision bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
)`;

/** Compile schema using PostgreSQL itself, without connecting to a service. */
export async function compileLocalSchema(migrations: readonly { name: string; sql: string }[]): Promise<LocalSchema> {
  const db = new PGlite({ extensions: { pg_trgm } });
  try {
    await db.exec(identitySchema);
    for (const migration of migrations) {
      // gen_random_uuid is built into modern PostgreSQL. PGlite does not ship
      // pgcrypto's OpenSSL extension; remaining references fail explicitly.
      const sql = migration.sql.replace(/CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?["']?pgcrypto["']?\s*;/gi, "");
      try { await db.exec(sql); }
      catch (error) { throw new Error(`Local schema compilation failed in ${migration.name}: ${error instanceof Error ? error.message : String(error)}`, { cause: error }); }
    }
    const { rows } = await db.query<{
      table_name: string; column_name: string; type: string; nullable: boolean;
      column_default: string | null; primary_key: boolean;
    }>(`SELECT c.relname AS table_name, a.attname AS column_name,
      format_type(a.atttypid, a.atttypmod) AS type, NOT a.attnotnull AS nullable,
      pg_get_expr(d.adbin, d.adrelid) AS column_default,
      EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary AND a.attnum=ANY(i.indkey)) AS primary_key
      FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
      JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
      WHERE n.nspname='public' AND c.relkind='r'
      ORDER BY c.relname, a.attnum`);
    const schema: LocalSchema = {};
    for (const row of rows) {
      if (row.table_name.startsWith("_gonvex")) continue;
      const table = schema[row.table_name] ??= { key: "_id", columns: {} };
      if (row.primary_key) table.key = row.column_name;
      table.columns[row.column_name] = {
        type: row.type, nullable: row.nullable,
        ...(row.column_default ? { default: row.column_default } : {}),
      };
    }
    return schema;
  } finally { await db.close(); }
}
