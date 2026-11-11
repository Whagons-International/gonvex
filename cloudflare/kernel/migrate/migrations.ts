// Migration files as /dev/sync receives them: names, directives and checksums,
// with the Rust runtime's rules (rust/crates/runtime/src/modules.rs
// `migrations_from_module`, rust/crates/store/src/provision.rs `SqlMigration`,
// CONTRACT.md E.a.6).

export type MigrationScope = "tenant" | "control-plane";

export interface SqlMigration {
  name: string;
  /** SHA-256 hex of the SQL text, as Rust's `SqlMigration::new`. */
  checksum: string;
  scope: MigrationScope;
  noTransaction: boolean;
  /** A line that is exactly `-- gonvex:foreign-key-rebuild` (after trimming), anywhere in the file. */
  foreignKeyRebuild: boolean;
  sql: string;
}

export class MigrationFileError extends Error {
  override name = "MigrationFileError";
}

const encoder = new TextEncoder();

export async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Rust's `str::lines`: split on `\n`, drop one trailing `\r` per line, no final empty line. */
function lines(text: string): string[] {
  const out = text.split("\n").map((l) => (l.endsWith("\r") ? l.slice(0, -1) : l));
  if (out.length && out[out.length - 1] === "") out.pop();
  return out;
}

/** Rust's `str::trim` (Unicode White_Space). */
const trim = (s: string) => s.replace(/^[\s\u0085]+|[\s\u0085]+$/gu, "");

const NAME = /^[0-9]{4}_[A-Za-z0-9_-]*\.sql$/;

/** Validates a migration file name: `NNNN_description.sql`, at least 10 bytes. */
export function validMigrationName(name: string): boolean {
  return encoder.encode(name).length >= 10 && NAME.test(name);
}

/**
 * Parses one migration file. Directives come from the leading comment block only
 * (`-- gonvex:scope tenant|control-plane`, required; `-- gonvex:no-transaction`);
 * the foreign-key rebuild line may appear anywhere, as Rust's `rebuilds_foreign_keys`.
 */
export async function parseMigration(name: string, sql: string): Promise<SqlMigration> {
  if (!validMigrationName(name)) throw new MigrationFileError(`migration ${JSON.stringify(name)} must be named NNNN_description.sql`);
  let scope: MigrationScope | null = null;
  let noTransaction = false;
  for (const raw of lines(sql)) {
    const line = trim(raw);
    if (!line) continue;
    if (!line.startsWith("--")) break;
    const directive = trim(line.slice(2));
    if (directive.startsWith("gonvex:scope")) {
      const value = trim(directive.slice("gonvex:scope".length));
      if (value === "tenant") scope = "tenant";
      else if (value === "control-plane") scope = "control-plane";
      else throw new MigrationFileError(`migration ${name}: invalid scope ${JSON.stringify(value)}`);
    } else if (directive === "gonvex:no-transaction") {
      noTransaction = true;
    }
  }
  if (!scope) throw new MigrationFileError(`migration ${name}: missing required -- gonvex:scope tenant|control-plane directive`);
  const foreignKeyRebuild = lines(sql).some((l) => trim(l) === "-- gonvex:foreign-key-rebuild");
  return { name, checksum: await sha256Hex(sql), scope, noTransaction, foreignKeyRebuild, sql };
}

/**
 * The migrations of a module's `files` map (path -> SQL text, already decoded):
 * only `migrations/<name>.sql` directly under `migrations/`, sorted by name.
 */
export async function migrationsFromFiles(files: Readonly<Record<string, string>>): Promise<SqlMigration[]> {
  const out: SqlMigration[] = [];
  for (const [path, sql] of Object.entries(files)) {
    const m = /^migrations\/([^/]+)$/.exec(path);
    if (!m || !path.endsWith(".sql")) continue;
    out.push(await parseMigration(m[1]!, sql));
  }
  // Rust sorts by name with byte order.
  return out.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}
