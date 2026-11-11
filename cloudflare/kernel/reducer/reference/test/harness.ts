// The Rust integration fixture (rust/crates/store/tests/reference_integrity.rs
// `Fixture`) on one Durable Object.
//
// Rust's pieces and their stand-ins here:
//   provision_tenant_database  install the tenant schema (identity, schema
//                              options, reference ledger, migration ledger),
//                              then each unapplied migration in one
//                              transaction: script, afterMigration, the
//                              deferred-FK check, the ledger row;
//   TenantTransaction          transactionSync: script, flush when staged,
//                              then the deferred-FK check K01 runs before a
//                              Durable Object commits (0016 G0-5) — SQLite
//                              itself only checks deferred keys at COMMIT,
//                              which a Durable Object does after the event.
import { env } from "cloudflare:test";
import { expect } from "vitest";
import { directSql } from "../../../capture/sql.ts";
import { LEDGER_SQL, SCHEMA_OPTIONS_SQL, TENANT_IDENTITY_SQL } from "../../../migrate/runtime-sql.ts";
import { afterMigration, declareReferences, flushReferences, hasStagedReferences, installReferences, type Atomic } from "../index.ts";

/**
 * The plain SQLite-backed Durable Object namespace the tests run in (the
 * standalone harness's `REF` binding, worker.ts). The only line to change
 * when the tests move under another test Worker.
 */
export const objectNamespace = (): DurableObjectNamespace => (env as unknown as { REF: DurableObjectNamespace }).REF;

/** K01's commit text for a deferred foreign key violation. */
export const DEFERRED_FK_ERROR = "error returned from database: (code: 787) FOREIGN KEY constraint failed";

export class Tenant {
  readonly sql;
  readonly atomic: Atomic;
  constructor(readonly storage: DurableObjectStorage) {
    this.sql = directSql(storage.sql);
    this.atomic = (fn) => storage.transactionSync(fn);
  }

  private exec(script: string): void {
    this.storage.sql.exec(script);
  }

  /** The commit-time deferred foreign key check. */
  deferredCheck(): void {
    if (this.storage.sql.exec("SELECT 1 FROM pragma_foreign_key_check LIMIT 1").toArray().length > 0) throw new Error(DEFERRED_FK_ERROR);
  }

  provision(name: string, source: string): void {
    this.exec(TENANT_IDENTITY_SQL);
    this.exec(SCHEMA_OPTIONS_SQL);
    installReferences(this.sql);
    this.exec(LEDGER_SQL);
    if (this.scalar(`SELECT count(*) FROM gonvex_migrations WHERE name='${name.replace(/'/g, "''")}'`) > 0) return;
    this.storage.transactionSync(() => {
      this.exec(source);
      afterMigration(this.sql, { atomic: this.atomic });
      this.deferredCheck();
      this.storage.sql.exec("INSERT INTO gonvex_migrations(name,checksum,duration_ms) VALUES(?,?,0)", name, "checksum");
    });
  }

  /** `Fixture::run`: one TenantTransaction. */
  run(script: string): void {
    this.storage.transactionSync(() => {
      this.exec(script);
      this.commit();
    });
  }

  /** `TenantTransaction::commit`: flush when staged, then the deferred check. */
  commit(): void {
    if (hasStagedReferences(this.sql)) flushReferences(this.sql);
    this.deferredCheck();
  }

  ok(script: string): void {
    try {
      this.run(script);
    } catch (error) {
      throw new Error(`${script}: ${String(error)}`);
    }
  }

  /** The statement must be refused and leave no effect. */
  rejected(script: string): void {
    let text: string | null = null;
    try {
      this.run(script);
    } catch (error) {
      text = error instanceof Error ? error.message : String(error);
    }
    expect(text, `accepted: ${script}`).not.toBeNull();
    expect(text!.includes("FOREIGN KEY") || text!.includes("reference"), `${script}: ${text}`).toBe(true);
  }

  /** Rust's error text of a failed TenantTransaction. */
  error(run: () => void): string {
    try {
      run();
    } catch (error) {
      return error instanceof Error ? error.message : String(error);
    }
    throw new Error("expected an error");
  }

  scalar(sql: string): number {
    const row = this.storage.sql.exec(sql).toArray()[0];
    return row === undefined ? Number.NaN : Number(Object.values(row)[0]);
  }

  text(sql: string): string | null {
    const row = this.storage.sql.exec(sql).toArray()[0];
    const value = row === undefined ? null : Object.values(row)[0];
    return value === null || value === undefined ? null : String(value);
  }

  /** The installed catalog as plain JSON (no integer-like keys or big numbers in these catalogs). */
  catalog(): Record<string, any> {
    return JSON.parse(this.text("SELECT declaration FROM _gonvex_reference_catalog")!) as Record<string, any>;
  }

  /** `reference_integrity::declare` in its own TenantTransaction. */
  declare(catalog: unknown): void {
    this.storage.transactionSync(() => {
      declareReferences(this.sql, catalog, { atomic: this.atomic });
      this.commit();
    });
  }

  violations(): number {
    return this.scalar("SELECT count(*) FROM gonvex_reference_violations");
  }

  /** Live owners of a shared token must equal its ledger owner count. */
  assertOwnerCount(ownersSql: string, token: string, rowId: string): void {
    const owners = this.scalar(ownersSql);
    const counted = this.scalar(
      `SELECT COALESCE((SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='${token}' AND row_id='${rowId}' AND state='live'),0)`,
    );
    expect(owners > 0, "every owner disappeared").toBe(true);
    expect(owners).toBe(counted);
    expect(this.violations()).toBe(0);
  }
}
