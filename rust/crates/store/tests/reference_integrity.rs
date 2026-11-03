//! Declared reference integrity on SQLite. The single writer serializes
//! transactions, so the PostgreSQL concurrency races become both commit
//! orders; the invariant is the same: conflicting intents cannot both commit
//! and the ledger never dangles.

use std::collections::BTreeMap;

use gonvex_store::{
    execute_script, reference_integrity, ControlPlane, DataDir, DatabaseError, MigrationScope,
    PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
};
use serde_json::{json, Value};

struct TestDir(std::path::PathBuf);

impl TestDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::var_os("GONVEX_TEST_DATA_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("{prefix}-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _dir: TestDir,
    pools: PoolRegistry,
    control: ControlPlane,
    route: TenantRoute,
    other: TenantRoute,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_migration(include_str!("reference_integrity/fixture.sql")).await
    }

    async fn with_migration(source: &str) -> Self {
        let dir = TestDir::new("gonvex-refs");
        let data = DataDir::new(&dir.0);
        let pools = PoolRegistry::new(PoolLimits::default());
        let control = ControlPlane::connect(&data.control_url(), pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let routes = ["tenant-1", "tenant-2"].map(|tenant| TenantRoute {
            project_id: "project".to_owned(),
            tenant_id: tenant.to_owned(),
            database_url: data.tenant_url("project", tenant).unwrap(),
        });
        let migration = SqlMigration::new(
            "0001_references.sql".to_owned(),
            MigrationScope::Tenant,
            false,
            source.to_owned(),
        );
        for route in &routes {
            control
                .clone()
                .provision_tenant_database(route.clone(), vec![migration.clone()])
                .await
                .unwrap();
            // Reprovisioning must retain the declarations, legacy edges and guards.
            control
                .clone()
                .provision_tenant_database(route.clone(), vec![migration.clone()])
                .await
                .unwrap();
        }
        let [route, other] = routes;
        Self {
            _dir: dir,
            pools,
            control,
            route,
            other,
        }
    }

    async fn run_on(&self, route: &TenantRoute, sql: &str) -> Result<(), DatabaseError> {
        let mut transaction = self.control.begin_tenant_transaction(route, false).await?;
        execute_script(transaction.transaction(), sql).await?;
        transaction.commit().await
    }

    async fn run(&self, sql: &str) -> Result<(), DatabaseError> {
        self.run_on(&self.route, sql).await
    }

    async fn ok(&self, sql: &str) {
        self.run(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    /// The statement must be refused and leave no effect.
    async fn rejected(&self, sql: &str) {
        let error = self
            .run(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("accepted: {sql}"));
        let text = error.to_string();
        assert!(
            text.contains("FOREIGN KEY") || text.contains("reference"),
            "{sql}: {text}"
        );
    }

    async fn scalar(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pools.reader(&self.route.database_url).await.unwrap())
            .await
            .unwrap()
    }

    async fn text(&self, sql: &str) -> Option<String> {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pools.reader(&self.route.database_url).await.unwrap())
            .await
            .unwrap()
    }

    async fn catalog(&self) -> Value {
        serde_json::from_str(
            &self
                .text("SELECT declaration FROM _gonvex_reference_catalog")
                .await
                .unwrap(),
        )
        .unwrap()
    }

    async fn declare(&self, catalog: &Value) -> Result<(), DatabaseError> {
        let mut transaction = self
            .control
            .begin_tenant_transaction(&self.route, false)
            .await?;
        reference_integrity::declare(transaction.transaction(), catalog).await?;
        transaction.commit().await
    }

    async fn violations(&self) -> i64 {
        self.scalar("SELECT count(*) FROM gonvex_reference_violations")
            .await
    }

    /// Live owners of a shared token must equal its ledger owner count.
    async fn assert_owner_count(&self, owners_sql: &str, token: &str, row_id: &str) {
        let owners = self.scalar(owners_sql).await;
        let counted = self
            .scalar(&format!(
                "SELECT COALESCE((SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='{token}' AND row_id='{row_id}' AND state='live'),0)"
            ))
            .await;
        assert!(owners > 0, "every owner disappeared");
        assert_eq!(owners, counted);
        assert_eq!(self.violations().await, 0);
    }
}

#[tokio::test]
async fn declared_references_protect_all_sql_writers_without_repairing_legacy_rows() {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.violations().await, 1, "legacy baseline lost");
    fixture
        .ok("UPDATE workspaces SET note='unrelated update' WHERE _id='legacy'")
        .await;
    assert_eq!(fixture.violations().await, 1);

    fixture
        .rejected(r#"UPDATE teams SET "deletedAt"=2 WHERE _id='before'"#)
        .await;
    assert_eq!(
        fixture
            .scalar(r#"SELECT count(*) FROM teams WHERE _id='before' AND "deletedAt" IS NULL"#)
            .await,
        1,
        "delete partially committed"
    );
    assert_eq!(
        fixture
            .text("SELECT teams FROM workspaces WHERE _id='before'")
            .await
            .as_deref(),
        Some(r#"["before"]"#),
        "security relationship changed"
    );
    fixture
        .rejected("DELETE FROM teams WHERE _id='before'")
        .await;
    fixture
        .rejected(r#"INSERT INTO workspaces VALUES('bad','["absent"]',NULL,NULL)"#)
        .await;
    fixture
        .rejected("INSERT INTO workspaces VALUES('bad-shape','[{}]',NULL,NULL)")
        .await;
    fixture
        .rejected(
            r#"INSERT INTO approvals VALUES('bad-type',NULL,'[{"type":"unknown","id":"before"}]')"#,
        )
        .await;
    fixture
        .ok(r#"INSERT INTO approvals VALUES('good',NULL,'[{"type":"team","id":"before"}]')"#)
        .await;
    fixture.ok("DELETE FROM approvals WHERE _id='good'").await;

    // A restored legacy target is protected from then on.
    fixture.ok("INSERT INTO teams VALUES('missing',NULL)").await;
    assert_eq!(fixture.violations().await, 0);
    fixture
        .rejected("DELETE FROM teams WHERE _id='missing'")
        .await;

    // Historical identity: soft deletion is allowed, hard deletion is not.
    fixture
        .ok("INSERT INTO teams VALUES('history-only',NULL)")
        .await;
    fixture
        .ok("INSERT INTO history VALUES('h','history-only')")
        .await;
    fixture
        .ok(r#"UPDATE teams SET "deletedAt"=5 WHERE _id='history-only'"#)
        .await;
    fixture
        .rejected("DELETE FROM teams WHERE _id='history-only'")
        .await;

    // Explicit reassignment in either order within one transaction.
    fixture
        .ok(r#"UPDATE teams SET "deletedAt"=3 WHERE _id='before'; UPDATE workspaces SET teams='[]' WHERE _id='before'"#)
        .await;
    assert_eq!(fixture.violations().await, 0);

    // A matching ID in another tenant cannot satisfy this tenant's reference.
    fixture
        .run_on(
            &fixture.other,
            "INSERT INTO teams VALUES('other-only',NULL)",
        )
        .await
        .unwrap();
    fixture
        .rejected(r#"INSERT INTO workspaces VALUES('cross-tenant','["other-only"]',NULL,NULL)"#)
        .await;
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE table_name LIKE '\\_gonvex\\_reference\\_%' ESCAPE '\\'")
            .await,
        0,
        "reference infrastructure must not enter the application change feed"
    );
    // A writer that bypasses TenantTransaction (and so never flushes) fails closed.
    let raw = sqlx::query("INSERT INTO teams VALUES('raw',NULL)")
        .execute(
            &fixture
                .pools
                .pool(&fixture.route.database_url)
                .await
                .unwrap(),
        )
        .await;
    assert!(raw.is_err(), "unflushed reference changes committed");
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM teams WHERE _id='raw'")
            .await,
        0
    );
}

#[tokio::test]
async fn link_and_delete_cannot_both_commit_in_either_order() {
    let fixture = Fixture::new().await;
    for delete_first in [true, false] {
        fixture.ok("INSERT INTO teams VALUES('race',NULL)").await;
        let link = r#"INSERT INTO workspaces VALUES('race','["race"]',NULL,NULL)"#;
        let delete = "DELETE FROM teams WHERE _id='race'";
        let (first, second) = if delete_first {
            (delete, link)
        } else {
            (link, delete)
        };
        fixture.ok(first).await;
        fixture.rejected(second).await;
        assert_eq!(
            fixture
                .scalar("SELECT count(*) FROM gonvex_reference_violations WHERE source_id='race'")
                .await,
            0,
            "delete_first={delete_first}"
        );
        fixture
            .ok("DELETE FROM workspaces WHERE _id='race'; DELETE FROM teams WHERE _id='race'")
            .await;
    }
}

#[tokio::test]
async fn alternate_keys_unions_lifecycle_and_catalog_upgrades_are_enforced() {
    let fixture = Fixture::with_migration(include_str!("reference_integrity/aliases.sql")).await;
    fixture
        .rejected(r#"UPDATE "aliasTeams" SET code='beta' WHERE _id='one'"#)
        .await;
    fixture
        .rejected(r#"UPDATE "aliasTeams" SET "deletedAt"=1 WHERE _id='one'"#)
        .await;
    // Numeric IDs need a declared policy.
    fixture
        .rejected(r#"UPDATE "aliasLinks" SET payload='{"first":17}'"#)
        .await;
    let mut catalog = fixture.catalog().await;
    catalog["references"][2]["allowNumbers"] = json!(true);
    catalog["references"][2]["numberReason"] =
        json!("The legacy consumer explicitly normalizes numeric identifiers through String.");
    fixture.declare(&catalog).await.unwrap();
    fixture
        .ok(r#"INSERT INTO "aliasTeams" VALUES('17','seventeen',NULL)"#)
        .await;
    fixture
        .ok(r#"UPDATE "aliasLinks" SET payload='{"first":17}'"#)
        .await;
    fixture
        .rejected(r#"DELETE FROM "aliasTeams" WHERE _id='17'"#)
        .await;
    // JSON serialized inside a scalar enforces the same reference paths.
    fixture
        .ok(r#"UPDATE "aliasLinks" SET payload=json_quote('{"first":"one","other":"builtin"}')"#)
        .await;
    fixture
        .rejected(r#"UPDATE "aliasLinks" SET payload=json_quote('{"first":"missing"}')"#)
        .await;
    fixture
        .rejected(r#"UPDATE "aliasLinks" SET payload=json_quote('not-json')"#)
        .await;
    fixture
        .ok(r#"UPDATE "aliasLinks" SET alias=NULL,payload=NULL"#)
        .await;
    // A shared namespace stays live while either real table still owns it.
    fixture
        .ok(r#"UPDATE "aliasTeams" SET "deletedAt"=2 WHERE _id='one'"#)
        .await;
    fixture
        .rejected(r#"DELETE FROM "aliasBackups" WHERE _id='one'"#)
        .await;
    fixture.ok(r#"UPDATE "aliasLinks" SET ref=NULL"#).await;
    fixture
        .ok(r#"DELETE FROM "aliasBackups" WHERE _id='one'"#)
        .await;
    fixture
        .ok(r#"INSERT INTO "aliasLinks" VALUES('revoked','missing',NULL,NULL,3)"#)
        .await;
    fixture
        .rejected(r#"UPDATE "aliasLinks" SET "deletedAt"=NULL WHERE _id='revoked'"#)
        .await;
    // Rebuild declarations without changing app data or losing alternate keys.
    fixture.declare(&fixture.catalog().await).await.unwrap();
    assert_eq!(fixture.violations().await, 0);
    assert_eq!(
        fixture
            .text(r#"SELECT code FROM "aliasTeams" WHERE _id='one'"#)
            .await
            .as_deref(),
        Some("alpha")
    );
    assert_eq!(
        fixture
            .scalar("SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='one' AND state='exists'")
            .await,
        1
    );
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='one' AND state='live'")
            .await,
        0,
        "shared live token survived the last live owner"
    );
    // Coverage anchors every selection at its declared column.
    let mut moved = fixture.catalog().await;
    moved["references"][0]["column"] = json!("alias");
    let error = fixture.declare(&moved).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("selection must start at its declared column"),
        "{error}"
    );
    // Invalid catalogs are refused without replacing the installed one.
    let mut invalid = fixture.catalog().await;
    invalid["tables"][0]["key"] = json!("code");
    assert!(fixture.declare(&invalid).await.is_err());
    assert_eq!(fixture.catalog().await["tables"][0]["key"], "_id");
}

#[tokio::test]
async fn union_owners_cannot_remove_the_last_referenced_target() {
    let fixture = Fixture::with_migration(include_str!("reference_integrity/union.sql")).await;
    for soft in [false, true] {
        fixture
            .ok("INSERT INTO union_owners_a VALUES('shared-id','code',NULL); INSERT INTO union_owners_b VALUES('shared-id',NULL); INSERT INTO union_links VALUES('link','shared-id',NULL)")
            .await;
        let (first, second) = if soft {
            (
                r#"UPDATE union_owners_a SET "deletedAt"=1"#,
                r#"UPDATE union_owners_b SET "deletedAt"=1"#,
            )
        } else {
            ("DELETE FROM union_owners_a", "DELETE FROM union_owners_b")
        };
        fixture.ok(first).await;
        fixture.rejected(second).await;
        fixture
            .assert_owner_count(
                r#"SELECT (SELECT count(*) FROM union_owners_a WHERE "deletedAt" IS NULL)+(SELECT count(*) FROM union_owners_b WHERE "deletedAt" IS NULL)"#,
                "shared",
                "shared-id",
            )
            .await;
        fixture
            .ok("DELETE FROM union_links; DELETE FROM union_owners_a; DELETE FROM union_owners_b")
            .await;
    }
}

#[tokio::test]
async fn union_replacement_preserves_tokens_and_exact_owner_counts() {
    let fixture = Fixture::with_migration(include_str!("reference_integrity/union.sql")).await;
    fixture
        .ok("INSERT INTO union_owners_a VALUES('shared-id','code',NULL); INSERT INTO union_links VALUES('link','shared-id',NULL)")
        .await;
    // The link prevents deleting the only owner before its replacement exists.
    fixture.rejected("DELETE FROM union_owners_a").await;
    // Replacement in one transaction keeps the token.
    fixture
        .ok("INSERT INTO union_owners_b VALUES('shared-id',NULL); DELETE FROM union_owners_a")
        .await;
    fixture
        .assert_owner_count(
            r#"SELECT (SELECT count(*) FROM union_owners_a WHERE "deletedAt" IS NULL)+(SELECT count(*) FROM union_owners_b WHERE "deletedAt" IS NULL)"#,
            "shared",
            "shared-id",
        )
        .await;
}

#[tokio::test]
async fn tenant_migration_rejects_unclassified_storage_before_committing() {
    let fixture = Fixture::with_migration(include_str!("reference_integrity/aliases.sql")).await;
    for column in ["arbitrary_owner", "unreviewedId"] {
        let migration = SqlMigration::new(
            format!("0002_{column}.sql"),
            MigrationScope::Tenant,
            false,
            format!("ALTER TABLE \"aliasLinks\" ADD COLUMN \"{column}\" TEXT"),
        );
        let error = fixture
            .control
            .clone()
            .provision_tenant_database(fixture.route.clone(), vec![migration])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(&format!(
                "unclassified reference candidate aliasLinks.{column}"
            )),
            "{error}"
        );
        assert_eq!(
            fixture
                .scalar(&format!(
                    "SELECT count(*) FROM pragma_table_info('aliasLinks') WHERE name='{column}'"
                ))
                .await,
            0,
            "failed migration committed an unclassified column"
        );
        assert_eq!(
            fixture
                .scalar(&format!(
                    "SELECT count(*) FROM gonvex_migrations WHERE name='0002_{column}.sql'"
                ))
                .await,
            0,
            "failed migration was recorded"
        );
    }
}

#[tokio::test]
async fn nonunique_alias_owners_cannot_remove_the_last_live_selector() {
    let fixture = Fixture::with_migration(include_str!("reference_integrity/union.sql")).await;
    for soft in [false, true] {
        fixture
            .ok("INSERT INTO union_owners_a VALUES('first','slug',NULL),('second','slug',NULL); INSERT INTO union_links VALUES('link',NULL,'slug')")
            .await;
        let action = if soft {
            r#"UPDATE union_owners_a SET "deletedAt"=1 WHERE _id="#
        } else {
            "DELETE FROM union_owners_a WHERE _id="
        };
        fixture.ok(&format!("{action}'first'")).await;
        fixture.rejected(&format!("{action}'second'")).await;
        fixture
            .assert_owner_count(
                r#"SELECT count(*) FROM union_owners_a WHERE "deletedAt" IS NULL"#,
                "shared_code",
                "slug",
            )
            .await;
        fixture
            .ok("DELETE FROM union_links; DELETE FROM union_owners_a")
            .await;
    }
}

fn binding_catalog(base: &Value) -> Value {
    let mut catalog = base.clone();
    catalog["tables"] = json!([
        {"name":"teams","key":"_id","live":"$.deletedAt == null"},
        {"name":"workspaces","key":"_id","live":"$.deletedAt == null"},
        {"name":"approvals","key":"_id","live":"$.deletedAt == null"},
        {"name":"history","key":"_id","live":"true"},
        {"name":"fields","key":"_id","live":"true"},
        {"name":"field_values","key":"_id","live":"true"}
    ]);
    catalog["bindings"] = json!([{"name":"fieldDomain","source":"fields","columns":["options"]}]);
    catalog["references"] = json!([{
        "name":"value.domain","source":"field_values","column":"value","path":"$.value[*]",
        "lookup":{"binding":"fieldDomain","key":"$.fieldId","when":"$.options.source == \"entity\""},
        "discriminator":"$.options.entity","targets":{"teams":"teams","workspaces":"workspaces"},"state":"live"
    },{
        "name":"nested.domain","source":"field_values","column":"value","at":"$.value[*] ? (exists(@.fieldId))","path":"$.selected[*]",
        "lookup":{"binding":"fieldDomain","key":"$.fieldId","when":"$.options.source == \"entity\""},
        "discriminator":"$.options.entity","targets":{"teams":"teams","workspaces":"workspaces"},"state":"live"
    }]);
    catalog
}

#[tokio::test]
async fn configuration_selected_domains_guard_values() {
    let fixture = Fixture::new().await;
    fixture
        .ok(r#"CREATE TABLE fields(_id TEXT PRIMARY KEY,options JSON,label TEXT);
               CREATE TABLE field_values(_id TEXT PRIMARY KEY,"fieldId" TEXT,value JSON);
               INSERT INTO fields VALUES('manual','{"source":"manual"}','Original');
               INSERT INTO field_values VALUES('old','manual','"arbitrary text"'),('legacy-config','absent-legacy','"unknown domain"');"#)
        .await;
    let catalog = binding_catalog(&fixture.catalog().await);
    fixture.declare(&catalog).await.unwrap();
    // A legacy value must not acquire an unchecked domain.
    fixture
        .rejected(r#"INSERT INTO fields VALUES('absent-legacy','{"source":"entity","entity":"teams"}','Previously missing')"#)
        .await;
    fixture
        .ok("DELETE FROM field_values WHERE _id='legacy-config'")
        .await;
    // Unrelated parent text can change without releasing the domain binding.
    fixture
        .ok("UPDATE fields SET label='Renamed' WHERE _id='manual'")
        .await;
    fixture
        .rejected(r#"UPDATE fields SET options='{"source":"entity","entity":"teams"}' WHERE _id='manual'"#)
        .await;
    fixture
        .ok(r#"INSERT INTO fields VALUES('entity','{"source":"entity","entity":"teams"}','Team');
               INSERT INTO teams VALUES('selected',NULL);
               INSERT INTO field_values VALUES('entity-value','entity','["selected"]');
               INSERT INTO field_values VALUES('nested-value',NULL,
                 '[{"fieldId":"manual","selected":"free text"},{"fieldId":"entity","selected":"selected"}]');"#)
        .await;
    fixture
        .rejected("DELETE FROM teams WHERE _id='selected'")
        .await;
    fixture
        .rejected(r#"UPDATE fields SET options='{"source":"entity","entity":"workspaces"}' WHERE _id='entity'"#)
        .await;
    fixture
        .rejected(r#"INSERT INTO field_values VALUES('missing','absent','"selected"')"#)
        .await;
    // Clear dependents and change configuration atomically.
    fixture
        .ok(r#"DELETE FROM field_values WHERE "fieldId"='entity' OR _id='nested-value';
               UPDATE fields SET options='{"source":"entity","entity":"workspaces"}' WHERE _id='entity';"#)
        .await;
    fixture.declare(&fixture.catalog().await).await.unwrap();
    assert_eq!(
        fixture.violations().await,
        0,
        "configuration binding ledger has an orphan"
    );

    // Large parent configurations work and keep their binding across edits.
    fixture
        .ok(r#"INSERT INTO fields VALUES('large',json_object('source','manual','labels',printf('%.*c',30000,'x')),'Large');
               INSERT INTO field_values VALUES('large-value','large','"free text"');
               UPDATE fields SET label='Still large' WHERE _id='large';"#)
        .await;

    // Chained parent lookups select a domain by each object's local key.
    fixture
        .ok(
            r#"CREATE TABLE templates(_id TEXT PRIMARY KEY,attributes JSON);
               ALTER TABLE workspaces ADD COLUMN "templateId" TEXT;
               ALTER TABLE field_values ADD COLUMN "workspaceId" TEXT;
               INSERT INTO templates VALUES('typed','[{"id":"place","reference":"teams"}]');
               INSERT INTO workspaces(_id,"templateId") VALUES('parent','typed');
               INSERT INTO teams VALUES('nested-target',NULL);"#,
        )
        .await;
    let mut catalog = fixture.catalog().await;
    catalog["tables"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"templates","key":"_id","live":"true"}));
    catalog["bindings"].as_array_mut().unwrap().extend([
        json!({"name":"workspaceTemplate","source":"workspaces","columns":["templateId"]}),
        json!({"name":"templateAttributes","source":"templates","columns":["attributes"]}),
    ]);
    catalog["references"].as_array_mut().unwrap().push(json!({
        "name":"value.attribute","source":"field_values","column":"value","state":"live",
        "at":"$.value ? (@.type() == \"object\").keyvalue() ? (@.value.kind == \"reference\")","path":"$.value.refId",
        "lookup":[{"binding":"workspaceTemplate","from":"document","key":"$.workspaceId","required":true},
          {"binding":"templateAttributes","from":"configuration","key":"$.templateId","required":true}],
        "discriminator":"$.attributes[*] ? (@.id == $entry.key).reference","targets":{"teams":"teams"}
    }));
    fixture.declare(&catalog).await.unwrap();
    fixture
        .ok(r#"INSERT INTO field_values(_id,value,"workspaceId") VALUES('attribute-value','{"place":{"kind":"reference","refId":"nested-target"}}','parent')"#)
        .await;
    fixture
        .rejected("DELETE FROM teams WHERE _id='nested-target'")
        .await;
    fixture
        .rejected("UPDATE templates SET attributes='[]' WHERE _id='typed'")
        .await;
    assert_eq!(fixture.violations().await, 0);
}

#[tokio::test]
async fn configuration_change_and_new_value_cannot_both_commit() {
    let fixture = Fixture::new().await;
    fixture
        .ok(
            r#"CREATE TABLE fields(_id TEXT PRIMARY KEY,options JSON,label TEXT);
               CREATE TABLE field_values(_id TEXT PRIMARY KEY,"fieldId" TEXT,value JSON);"#,
        )
        .await;
    fixture
        .declare(&binding_catalog(&fixture.catalog().await))
        .await
        .unwrap();
    for configuration_first in [true, false] {
        fixture
            .ok(r#"INSERT INTO fields VALUES('race-config','{"source":"manual"}','Race')"#)
            .await;
        let link = r#"INSERT INTO field_values(_id,"fieldId",value) VALUES('race-value','race-config','"free text"')"#;
        let change = r#"UPDATE fields SET options='{"source":"entity","entity":"teams"}' WHERE _id='race-config'"#;
        let (first, second) = if configuration_first {
            (change, link)
        } else {
            (link, change)
        };
        fixture.ok(first).await;
        fixture.rejected(second).await;
        assert_eq!(
            fixture.violations().await,
            0,
            "configuration_first={configuration_first}"
        );
        fixture
            .ok("DELETE FROM field_values WHERE _id='race-value'; DELETE FROM fields WHERE _id='race-config'")
            .await;
    }
}
