// Builds release plans (plan.ts) from a module's migrations. Runs once per
// release wherever the transform fits: about 8 s of CPU and a heap peak over
// 128 MB on the Whagons history, so not in a tenant object (see the PR notes).

import { SchemaTransform, TRANSFORM_VERSION, type TransformOptions } from "../schema-transform/index.ts";
import { sha256Hex, type SqlMigration } from "./migrations.ts";
import { unitKey, type PlannedMigration, type PlannedPrelude, type ReleasePlan } from "./plan.ts";
import { LEDGER_SQL, REFERENCE_SCHEMA_SQL, SCHEMA_OPTIONS_SQL, TENANT_IDENTITY_SQL } from "./runtime-sql.ts";

/** Bumped when the plan layout or the apply rules change. Part of every plan id. */
export const PLAN_FORMAT = "k02.1";

/**
 * The runtime DDL Rust runs before the tenant migrations on every provision, in
 * its order (provision.rs `provision_tenant_database`, `apply_migrations_locked`):
 * tenant identity, the reference ledger, schema options, the migration ledger.
 */
export const PRELUDE: ReadonlyArray<{ name: string; sql: string }> = [
  { name: "tenant-identity", sql: TENANT_IDENTITY_SQL },
  { name: "reference-ledger", sql: REFERENCE_SCHEMA_SQL },
  { name: "schema-options", sql: SCHEMA_OPTIONS_SQL },
  { name: "migration-ledger", sql: LEDGER_SQL },
];

export async function planId(migrations: readonly Pick<SqlMigration, "name" | "checksum">[]): Promise<string> {
  const identity = JSON.stringify({ format: PLAN_FORMAT, transform: TRANSFORM_VERSION, prelude: await Promise.all(PRELUDE.map((p) => sha256Hex(p.sql))), migrations: migrations.map((m) => [m.name, m.checksum]) });
  return (await sha256Hex(identity)).slice(0, 32);
}

const VACUUM = /^\s*VACUUM\b/i;

/**
 * Builds the tenant plan of a release. `migrations` are every migration of the
 * module (any scope, as `migrationsFromFiles` returns them); only tenant-scope
 * ones are planned, in name order. Throws the transform's `TransformError` on an
 * unhandled construct (0014 rule 2: provisioning stops, nothing is skipped).
 */
export async function buildReleasePlan(migrations: readonly SqlMigration[], options: TransformOptions = {}): Promise<ReleasePlan> {
  const tenant = migrations.filter((m) => m.scope === "tenant");
  const transform = new SchemaTransform(options);
  const chunks: Record<string, string[][]> = {};
  const prelude: PlannedPrelude[] = PRELUDE.map(({ name, sql }) => {
    const out = transform.runtime(name, sql);
    const unit: PlannedPrelude = { kind: "prelude", name, chunks: out.chunks.length };
    chunks[unitKey(unit)] = out.chunks;
    return unit;
  });
  const planned: PlannedMigration[] = tenant.map((m) => {
    const out = transform.migration(m.name, m.sql);
    let refusal: string | null = null;
    if (m.foreignKeyRebuild && m.noTransaction) refusal = "foreign-key rebuild requires an atomic tenant migration";
    else if (out.statements.some((s) => VACUUM.test(s))) {
      // CONTRACT.md E.a.6: a Durable Object cannot VACUUM; refuse with a clear error instead of SQLite's.
      refusal = `migration ${m.name}: VACUUM is not available in a Durable Object (the platform manages its storage)`;
    }
    const unit: PlannedMigration = {
      kind: "migration",
      name: m.name,
      checksum: m.checksum,
      noTransaction: m.noTransaction,
      foreignKeyRebuild: m.foreignKeyRebuild,
      refusal,
      statements: out.statements.length,
      chunks: out.chunks.length,
    };
    chunks[unitKey(unit)] = out.chunks;
    return unit;
  });
  return {
    id: await planId(tenant),
    format: PLAN_FORMAT,
    transformVersion: TRANSFORM_VERSION,
    prelude,
    migrations: planned,
    addedObjects: transform.addedObjects(),
    chunks,
  };
}

