// Provisions a Whagons tenant schema in node:sqlite the way the Rust store
// does (rust/crates/store/src/provision.rs, at the reference commit):
//   reference_integrity/schema.sql, schema_options.sql, TENANT_IDENTITY_SQL,
//   gonvex_migrations ledger, every tenant-scoped migration in order
//   (foreign_keys OFF around `gonvex:foreign-key-rebuild`), then
//   SYNC_INFRASTRUCTURE_SQL.
// Not reproduced (owned by K02/K03): reference_integrity::after_migration's
// generated staging triggers and flush, and per-table capture triggers.
//
// Writes ../.artifact/tenant-schema.sqlite and prints object counts.
// Usage: node scripts/provision-reference.mjs /path/to/whagons5-client
import { DatabaseSync } from "node:sqlite";
import { readFileSync, readdirSync, rmSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "../../../..");
const store = join(repo, "rust/crates/store/src");
const rawConst = (file, name) => {
  const source = readFileSync(join(store, file), "utf8");
  const match = new RegExp(`const ${name}: &str = r#"([\\s\\S]*?)"#;`).exec(source);
  if (!match) throw new Error(`${name} not found in ${file}`);
  return match[1];
};
const app = resolve(process.argv[2] ?? "");
const out = resolve(here, "../.artifact/tenant-schema.sqlite");
rmSync(out, { force: true });
const db = new DatabaseSync(out);
db.function("regexp", { deterministic: true }, (pattern, value) => value == null ? null : new RegExp(String(pattern)).test(String(value)) ? 1 : 0);
const started = performance.now();
db.exec("PRAGMA foreign_keys=ON");
db.exec(readFileSync(join(store, "reference_integrity/schema.sql"), "utf8"));
db.exec(readFileSync(join(store, "schema_options.sql"), "utf8"));
db.exec(rawConst("provision.rs", "TENANT_IDENTITY_SQL"));
db.exec("CREATE TABLE IF NOT EXISTS gonvex_migrations (name TEXT PRIMARY KEY, checksum TEXT NOT NULL, applied_at INTEGER NOT NULL DEFAULT 0, duration_ms INTEGER NOT NULL)");
const timings = [];
for (const name of readdirSync(join(app, "migrations")).filter((f) => f.endsWith(".sql")).sort()) {
  const sql = readFileSync(join(app, "migrations", name), "utf8");
  const scope = /^-- gonvex:scope (\w+)/m.exec(sql)?.[1] ?? "tenant";
  if (scope !== "tenant") continue;
  const rebuild = /^-- gonvex:foreign-key-rebuild/m.test(sql);
  const t0 = performance.now();
  if (rebuild) db.exec("PRAGMA foreign_keys=OFF");
  db.exec("BEGIN");
  try {
    db.exec(sql);
    if (rebuild) {
      const bad = db.prepare('SELECT "table", parent FROM pragma_foreign_key_check LIMIT 1').get();
      if (bad) throw new Error(`foreign-key rebuild rejected: ${bad.table} -> ${bad.parent}`);
    }
    db.prepare("INSERT INTO gonvex_migrations(name, checksum, duration_ms) VALUES (?, 'spike', ?)").run(name, Math.round(performance.now() - t0));
    db.exec("COMMIT");
  } catch (error) {
    db.exec("ROLLBACK");
    throw new Error(`${name}: ${error.message}`);
  } finally {
    if (rebuild) db.exec("PRAGMA foreign_keys=ON");
  }
  timings.push([name, Math.round(performance.now() - t0)]);
}
db.exec(rawConst("feed.rs", "SYNC_INFRASTRUCTURE_SQL"));
const counts = Object.fromEntries(db.prepare("SELECT type, count(*) AS n FROM sqlite_schema GROUP BY type").all().map((r) => [r.type, r.n]));
const version = db.prepare("SELECT sqlite_version() AS v").get().v;
db.close();
console.log(JSON.stringify({ sqlite: version, ms: Math.round(performance.now() - started), counts, timings }, null, 2));
