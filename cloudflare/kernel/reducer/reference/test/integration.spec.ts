// rust/crates/store/tests/reference_integrity.rs on Durable Object SQLite.
//
// Declared reference integrity on SQLite. The single writer serializes
// transactions, so the PostgreSQL concurrency races become both commit
// orders; the invariant is the same: conflicting intents cannot both commit
// and the ledger never dangles.
//
// The SQL fixtures are the Rust files themselves (`?raw`).
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
// @ts-expect-error vite raw import
import aliasesSql from "../../../../../rust/crates/store/tests/reference_integrity/aliases.sql?raw";
// @ts-expect-error vite raw import
import fixtureSql from "../../../../../rust/crates/store/tests/reference_integrity/fixture.sql?raw";
// @ts-expect-error vite raw import
import unionSql from "../../../../../rust/crates/store/tests/reference_integrity/union.sql?raw";
import { objectNamespace, Tenant } from "./harness.ts";

const namespace = objectNamespace();
let objects = 0;

/** `Fixture::with_migration`: two tenants, each provisioned twice (reprovisioning must retain the declarations, legacy edges and guards). */
async function fixture(source: string = fixtureSql) {
  const stubs = [namespace.getByName(`tenant-${++objects}`), namespace.getByName(`tenant-${++objects}`)] as const;
  for (const stub of stubs) {
    await runInDurableObject(stub, (_instance, state) => {
      const tenant = new Tenant(state.storage);
      tenant.provision("0001_references.sql", source);
      tenant.provision("0001_references.sql", source);
    });
  }
  const within = <T>(stub: DurableObjectStub, run: (tenant: Tenant) => T) =>
    runInDurableObject(stub, (_instance, state) => run(new Tenant(state.storage)));
  return { route: (run: (tenant: Tenant) => void) => within(stubs[0], run), other: (run: (tenant: Tenant) => void) => within(stubs[1], run) };
}

describe("reference_integrity.rs integration tests", () => {
  it("declared_references_protect_all_sql_writers_without_repairing_legacy_rows", async () => {
    const { route, other } = await fixture();
    await route((f) => {
      expect(f.violations(), "legacy baseline lost").toBe(1);
      f.ok("UPDATE workspaces SET note='unrelated update' WHERE _id='legacy'");
      expect(f.violations()).toBe(1);

      f.rejected(`UPDATE teams SET "deletedAt"=2 WHERE _id='before'`);
      expect(f.scalar(`SELECT count(*) FROM teams WHERE _id='before' AND "deletedAt" IS NULL`), "delete partially committed").toBe(1);
      expect(f.text("SELECT teams FROM workspaces WHERE _id='before'"), "security relationship changed").toBe('["before"]');
      f.rejected("DELETE FROM teams WHERE _id='before'");
      f.rejected(`INSERT INTO workspaces VALUES('bad','["absent"]',NULL,NULL)`);
      f.rejected("INSERT INTO workspaces VALUES('bad-shape','[{}]',NULL,NULL)");
      f.rejected(`INSERT INTO approvals VALUES('bad-type',NULL,'[{"type":"unknown","id":"before"}]')`);
      f.ok(`INSERT INTO approvals VALUES('good',NULL,'[{"type":"team","id":"before"}]')`);
      f.ok("DELETE FROM approvals WHERE _id='good'");

      // A restored legacy target is protected from then on.
      f.ok("INSERT INTO teams VALUES('missing',NULL)");
      expect(f.violations()).toBe(0);
      f.rejected("DELETE FROM teams WHERE _id='missing'");

      // Historical identity: soft deletion is allowed, hard deletion is not.
      f.ok("INSERT INTO teams VALUES('history-only',NULL)");
      f.ok("INSERT INTO history VALUES('h','history-only')");
      f.ok(`UPDATE teams SET "deletedAt"=5 WHERE _id='history-only'`);
      f.rejected("DELETE FROM teams WHERE _id='history-only'");

      // Explicit reassignment in either order within one transaction.
      f.ok(`UPDATE teams SET "deletedAt"=3 WHERE _id='before'; UPDATE workspaces SET teams='[]' WHERE _id='before'`);
      expect(f.violations()).toBe(0);
    });
    // A matching ID in another tenant cannot satisfy this tenant's reference.
    await other((f) => f.run("INSERT INTO teams VALUES('other-only',NULL)"));
    await route((f) => {
      f.rejected(`INSERT INTO workspaces VALUES('cross-tenant','["other-only"]',NULL,NULL)`);
      // Rust also asserts that no `_gonvex_reference_%` table enters `_gonvex_sync_changes`.
      // K03's capture is not installed in this harness; it never captures `_gonvex*` tables.

      // A writer that bypasses TenantTransaction (and so never flushes) fails closed:
      // here, a transaction that never flushes fails the commit-time deferred check.
      const raw = f.error(() =>
        f.storage.transactionSync(() => {
          f.storage.sql.exec("INSERT INTO teams VALUES('raw',NULL)");
          f.deferredCheck();
        }),
      );
      expect(raw).toContain("FOREIGN KEY");
      expect(f.scalar("SELECT count(*) FROM teams WHERE _id='raw'")).toBe(0);
    });
  });

  it("link_and_delete_cannot_both_commit_in_either_order", async () => {
    const { route } = await fixture();
    await route((f) => {
      for (const deleteFirst of [true, false]) {
        f.ok("INSERT INTO teams VALUES('race',NULL)");
        const link = `INSERT INTO workspaces VALUES('race','["race"]',NULL,NULL)`;
        const remove = "DELETE FROM teams WHERE _id='race'";
        const [first, second] = deleteFirst ? [remove, link] : [link, remove];
        f.ok(first);
        f.rejected(second);
        expect(f.scalar("SELECT count(*) FROM gonvex_reference_violations WHERE source_id='race'"), `delete_first=${deleteFirst}`).toBe(0);
        f.ok("DELETE FROM workspaces WHERE _id='race'; DELETE FROM teams WHERE _id='race'");
      }
    });
  });

  it("alternate_keys_unions_lifecycle_and_catalog_upgrades_are_enforced", async () => {
    const { route } = await fixture(aliasesSql);
    await route((f) => {
      f.rejected(`UPDATE "aliasTeams" SET code='beta' WHERE _id='one'`);
      f.rejected(`UPDATE "aliasTeams" SET "deletedAt"=1 WHERE _id='one'`);
      // Numeric IDs need a declared policy.
      f.rejected(`UPDATE "aliasLinks" SET payload='{"first":17}'`);
      const catalog = f.catalog();
      catalog.references[2].allowNumbers = true;
      catalog.references[2].numberReason = "The legacy consumer explicitly normalizes numeric identifiers through String.";
      f.declare(catalog);
      f.ok(`INSERT INTO "aliasTeams" VALUES('17','seventeen',NULL)`);
      f.ok(`UPDATE "aliasLinks" SET payload='{"first":17}'`);
      f.rejected(`DELETE FROM "aliasTeams" WHERE _id='17'`);
      // JSON serialized inside a scalar enforces the same reference paths.
      f.ok(`UPDATE "aliasLinks" SET payload=json_quote('{"first":"one","other":"builtin"}')`);
      f.rejected(`UPDATE "aliasLinks" SET payload=json_quote('{"first":"missing"}')`);
      f.rejected(`UPDATE "aliasLinks" SET payload=json_quote('not-json')`);
      f.ok(`UPDATE "aliasLinks" SET alias=NULL,payload=NULL`);
      // A shared namespace stays live while either real table still owns it.
      f.ok(`UPDATE "aliasTeams" SET "deletedAt"=2 WHERE _id='one'`);
      f.rejected(`DELETE FROM "aliasBackups" WHERE _id='one'`);
      f.ok(`UPDATE "aliasLinks" SET ref=NULL`);
      f.ok(`DELETE FROM "aliasBackups" WHERE _id='one'`);
      f.ok(`INSERT INTO "aliasLinks" VALUES('revoked','missing',NULL,NULL,3)`);
      f.rejected(`UPDATE "aliasLinks" SET "deletedAt"=NULL WHERE _id='revoked'`);
      // Rebuild declarations without changing app data or losing alternate keys.
      f.declare(f.catalog());
      expect(f.violations()).toBe(0);
      expect(f.text(`SELECT code FROM "aliasTeams" WHERE _id='one'`)).toBe("alpha");
      expect(f.scalar("SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='one' AND state='exists'")).toBe(1);
      expect(
        f.scalar("SELECT count(*) FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='one' AND state='live'"),
        "shared live token survived the last live owner",
      ).toBe(0);
      // Coverage anchors every selection at its declared column.
      const moved = f.catalog();
      moved.references[0].column = "alias";
      expect(f.error(() => f.declare(moved))).toContain("selection must start at its declared column");
      // Invalid catalogs are refused without replacing the installed one.
      const invalid = f.catalog();
      invalid.tables[0].key = "code";
      f.error(() => f.declare(invalid));
      expect(f.catalog().tables[0].key).toBe("_id");
    });
  });

  it("union_owners_cannot_remove_the_last_referenced_target", async () => {
    const { route } = await fixture(unionSql);
    await route((f) => {
      for (const soft of [false, true]) {
        f.ok(
          "INSERT INTO union_owners_a VALUES('shared-id','code',NULL); INSERT INTO union_owners_b VALUES('shared-id',NULL); INSERT INTO union_links VALUES('link','shared-id',NULL)",
        );
        const [first, second] = soft
          ? [`UPDATE union_owners_a SET "deletedAt"=1`, `UPDATE union_owners_b SET "deletedAt"=1`]
          : ["DELETE FROM union_owners_a", "DELETE FROM union_owners_b"];
        f.ok(first);
        f.rejected(second);
        f.assertOwnerCount(
          `SELECT (SELECT count(*) FROM union_owners_a WHERE "deletedAt" IS NULL)+(SELECT count(*) FROM union_owners_b WHERE "deletedAt" IS NULL)`,
          "shared",
          "shared-id",
        );
        f.ok("DELETE FROM union_links; DELETE FROM union_owners_a; DELETE FROM union_owners_b");
      }
    });
  });

  it("union_replacement_preserves_tokens_and_exact_owner_counts", async () => {
    const { route } = await fixture(unionSql);
    await route((f) => {
      f.ok("INSERT INTO union_owners_a VALUES('shared-id','code',NULL); INSERT INTO union_links VALUES('link','shared-id',NULL)");
      // The link prevents deleting the only owner before its replacement exists.
      f.rejected("DELETE FROM union_owners_a");
      // Replacement in one transaction keeps the token.
      f.ok("INSERT INTO union_owners_b VALUES('shared-id',NULL); DELETE FROM union_owners_a");
      f.assertOwnerCount(
        `SELECT (SELECT count(*) FROM union_owners_a WHERE "deletedAt" IS NULL)+(SELECT count(*) FROM union_owners_b WHERE "deletedAt" IS NULL)`,
        "shared",
        "shared-id",
      );
    });
  });

  it("tenant_migration_rejects_unclassified_storage_before_committing", async () => {
    const { route } = await fixture(aliasesSql);
    await route((f) => {
      for (const column of ["arbitrary_owner", "unreviewedId"]) {
        const error = f.error(() => f.provision(`0002_${column}.sql`, `ALTER TABLE "aliasLinks" ADD COLUMN "${column}" TEXT`));
        expect(error).toContain(`unclassified reference candidate aliasLinks.${column}`);
        expect(f.scalar(`SELECT count(*) FROM pragma_table_info('aliasLinks') WHERE name='${column}'`), "failed migration committed an unclassified column").toBe(0);
        expect(f.scalar(`SELECT count(*) FROM gonvex_migrations WHERE name='0002_${column}.sql'`), "failed migration was recorded").toBe(0);
      }
    });
  });

  it("nonunique_alias_owners_cannot_remove_the_last_live_selector", async () => {
    const { route } = await fixture(unionSql);
    await route((f) => {
      for (const soft of [false, true]) {
        f.ok("INSERT INTO union_owners_a VALUES('first','slug',NULL),('second','slug',NULL); INSERT INTO union_links VALUES('link',NULL,'slug')");
        const action = soft ? `UPDATE union_owners_a SET "deletedAt"=1 WHERE _id=` : "DELETE FROM union_owners_a WHERE _id=";
        f.ok(`${action}'first'`);
        f.rejected(`${action}'second'`);
        f.assertOwnerCount(`SELECT count(*) FROM union_owners_a WHERE "deletedAt" IS NULL`, "shared_code", "slug");
        f.ok("DELETE FROM union_links; DELETE FROM union_owners_a");
      }
    });
  });

  function bindingCatalog(base: Record<string, any>): Record<string, any> {
    const catalog = structuredClone(base);
    catalog.tables = [
      { name: "teams", key: "_id", live: "$.deletedAt == null" },
      { name: "workspaces", key: "_id", live: "$.deletedAt == null" },
      { name: "approvals", key: "_id", live: "$.deletedAt == null" },
      { name: "history", key: "_id", live: "true" },
      { name: "fields", key: "_id", live: "true" },
      { name: "field_values", key: "_id", live: "true" },
    ];
    catalog.bindings = [{ name: "fieldDomain", source: "fields", columns: ["options"] }];
    catalog.references = [
      {
        name: "value.domain", source: "field_values", column: "value", path: "$.value[*]",
        lookup: { binding: "fieldDomain", key: "$.fieldId", when: '$.options.source == "entity"' },
        discriminator: "$.options.entity", targets: { teams: "teams", workspaces: "workspaces" }, state: "live",
      },
      {
        name: "nested.domain", source: "field_values", column: "value", at: "$.value[*] ? (exists(@.fieldId))", path: "$.selected[*]",
        lookup: { binding: "fieldDomain", key: "$.fieldId", when: '$.options.source == "entity"' },
        discriminator: "$.options.entity", targets: { teams: "teams", workspaces: "workspaces" }, state: "live",
      },
    ];
    return catalog;
  }

  it("configuration_selected_domains_guard_values", async () => {
    const { route } = await fixture();
    await route((f) => {
      f.ok(`CREATE TABLE fields(_id TEXT PRIMARY KEY,options JSON,label TEXT);
            CREATE TABLE field_values(_id TEXT PRIMARY KEY,"fieldId" TEXT,value JSON);
            INSERT INTO fields VALUES('manual','{"source":"manual"}','Original');
            INSERT INTO field_values VALUES('old','manual','"arbitrary text"'),('legacy-config','absent-legacy','"unknown domain"');`);
      f.declare(bindingCatalog(f.catalog()));
      // A legacy value must not acquire an unchecked domain.
      f.rejected(`INSERT INTO fields VALUES('absent-legacy','{"source":"entity","entity":"teams"}','Previously missing')`);
      f.ok("DELETE FROM field_values WHERE _id='legacy-config'");
      // Unrelated parent text can change without releasing the domain binding.
      f.ok("UPDATE fields SET label='Renamed' WHERE _id='manual'");
      f.rejected(`UPDATE fields SET options='{"source":"entity","entity":"teams"}' WHERE _id='manual'`);
      f.ok(`INSERT INTO fields VALUES('entity','{"source":"entity","entity":"teams"}','Team');
            INSERT INTO teams VALUES('selected',NULL);
            INSERT INTO field_values VALUES('entity-value','entity','["selected"]');
            INSERT INTO field_values VALUES('nested-value',NULL,
              '[{"fieldId":"manual","selected":"free text"},{"fieldId":"entity","selected":"selected"}]');`);
      f.rejected("DELETE FROM teams WHERE _id='selected'");
      f.rejected(`UPDATE fields SET options='{"source":"entity","entity":"workspaces"}' WHERE _id='entity'`);
      f.rejected(`INSERT INTO field_values VALUES('missing','absent','"selected"')`);
      // Clear dependents and change configuration atomically.
      f.ok(`DELETE FROM field_values WHERE "fieldId"='entity' OR _id='nested-value';
            UPDATE fields SET options='{"source":"entity","entity":"workspaces"}' WHERE _id='entity';`);
      f.declare(f.catalog());
      expect(f.violations(), "configuration binding ledger has an orphan").toBe(0);

      // Large parent configurations work and keep their binding across edits.
      f.ok(`INSERT INTO fields VALUES('large',json_object('source','manual','labels',printf('%.*c',30000,'x')),'Large');
            INSERT INTO field_values VALUES('large-value','large','"free text"');
            UPDATE fields SET label='Still large' WHERE _id='large';`);

      // Chained parent lookups select a domain by each object's local key.
      f.ok(`CREATE TABLE templates(_id TEXT PRIMARY KEY,attributes JSON);
            ALTER TABLE workspaces ADD COLUMN "templateId" TEXT;
            ALTER TABLE field_values ADD COLUMN "workspaceId" TEXT;
            INSERT INTO templates VALUES('typed','[{"id":"place","reference":"teams"}]');
            INSERT INTO workspaces(_id,"templateId") VALUES('parent','typed');
            INSERT INTO teams VALUES('nested-target',NULL);`);
      const catalog = f.catalog();
      catalog.tables.push({ name: "templates", key: "_id", live: "true" });
      catalog.bindings.push(
        { name: "workspaceTemplate", source: "workspaces", columns: ["templateId"] },
        { name: "templateAttributes", source: "templates", columns: ["attributes"] },
      );
      catalog.references.push({
        name: "value.attribute", source: "field_values", column: "value", state: "live",
        at: '$.value ? (@.type() == "object").keyvalue() ? (@.value.kind == "reference")', path: "$.value.refId",
        lookup: [
          { binding: "workspaceTemplate", from: "document", key: "$.workspaceId", required: true },
          { binding: "templateAttributes", from: "configuration", key: "$.templateId", required: true },
        ],
        discriminator: "$.attributes[*] ? (@.id == $entry.key).reference", targets: { teams: "teams" },
      });
      f.declare(catalog);
      f.ok(`INSERT INTO field_values(_id,value,"workspaceId") VALUES('attribute-value','{"place":{"kind":"reference","refId":"nested-target"}}','parent')`);
      f.rejected("DELETE FROM teams WHERE _id='nested-target'");
      f.rejected("UPDATE templates SET attributes='[]' WHERE _id='typed'");
      expect(f.violations()).toBe(0);
    });
  });

  it("configuration_change_and_new_value_cannot_both_commit", async () => {
    const { route } = await fixture();
    await route((f) => {
      f.ok(`CREATE TABLE fields(_id TEXT PRIMARY KEY,options JSON,label TEXT);
            CREATE TABLE field_values(_id TEXT PRIMARY KEY,"fieldId" TEXT,value JSON);`);
      f.declare(bindingCatalog(f.catalog()));
      for (const configurationFirst of [true, false]) {
        f.ok(`INSERT INTO fields VALUES('race-config','{"source":"manual"}','Race')`);
        const link = `INSERT INTO field_values(_id,"fieldId",value) VALUES('race-value','race-config','"free text"')`;
        const change = `UPDATE fields SET options='{"source":"entity","entity":"teams"}' WHERE _id='race-config'`;
        const [first, second] = configurationFirst ? [change, link] : [link, change];
        f.ok(first);
        f.rejected(second);
        expect(f.violations(), `configuration_first=${configurationFirst}`).toBe(0);
        f.ok("DELETE FROM field_values WHERE _id='race-value'; DELETE FROM fields WHERE _id='race-config'");
      }
    });
  });
});
