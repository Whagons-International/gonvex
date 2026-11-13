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

#[tokio::test]
async fn catalog_install_already_has_planner_statistics_before_reopening() {
    let f = Fixture::new().await;
    let pool = f.pools.pool(&f.route.database_url).await.unwrap();
    let rules: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_reference_rules")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(rules > 0);
    let stat: String = sqlx::query_scalar(
        "SELECT stat FROM sqlite_stat1 WHERE idx='_gonvex_reference_rules_source'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        stat.split_whitespace()
            .next()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        rules
    );
    f.pools.close().await;
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

    /// Commit can compile a changed declaration before a deferred FK rejects
    /// it. That failed transaction must not poison subsequent policy reads.
    async fn rejected_declaration(&self, catalog: &Value, sql: &str) {
        let mut transaction = self
            .control
            .begin_tenant_transaction(&self.route, false)
            .await
            .unwrap();
        reference_integrity::declare(transaction.transaction(), catalog)
            .await
            .unwrap();
        execute_script(transaction.transaction(), sql)
            .await
            .unwrap();
        assert!(transaction.commit().await.is_err());
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
async fn tenants_without_declarations_do_not_install_catalog_cache_storage() {
    let fixture =
        Fixture::with_migration("CREATE TABLE plain_rows(_id TEXT PRIMARY KEY,value TEXT)").await;
    assert_eq!(fixture.scalar("SELECT count(*) FROM sqlite_schema WHERE name='_gonvex_reference_catalog_revision'").await,0);
    fixture
        .ok("INSERT INTO plain_rows VALUES('row','value')")
        .await;
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_pending")
            .await,
        0
    );
    assert_eq!(fixture.scalar("SELECT count(*) FROM sqlite_schema WHERE name='_gonvex_reference_catalog_revision'").await,0);
    fixture.pools.close().await;
}

#[tokio::test]
async fn prefixed_and_expression_selectors_are_explicit_and_fail_closed() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture.catalog().await;
    let rule = &mut catalog["references"][2];
    rule["valuePrefix"] = json!("team:");
    rule["prefixReason"] =
        json!("Only tagged values identify a team; other labels are local vocabulary.");
    rule["ignoreContainingAll"] = json!(["{{", "}}"]);
    rule["ignoreReason"] = json!(
        "Template expressions resolve when executed, rather than identifying a stored entity."
    );
    rule["allowEmpty"] = json!(true);
    fixture.declare(&catalog).await.unwrap();
    fixture.ok("INSERT INTO history VALUES('tagged','team:before'),('empty',''),('local','other:missing'),('expression','team:{{chosen}}')").await;
    fixture
        .rejected("INSERT INTO history VALUES('dangling','team:missing')")
        .await;
    fixture
        .rejected("DELETE FROM teams WHERE _id='before'")
        .await;
    let mut unsupported = catalog.clone();
    unsupported["references"][2]["guessMissingTargets"] = json!(true);
    let error = fixture.declare(&unsupported).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("unsupported policy guessMissingTargets"));
    fixture
        .rejected("INSERT INTO history VALUES('still-dangling','team:missing')")
        .await;
    fixture.pools.close().await;
}

#[tokio::test]
async fn unrelated_content_skips_staging_but_predicate_columns_do_not() {
    let fixture = Fixture::new().await;
    let writer = fixture
        .pools
        .pool(&fixture.route.database_url)
        .await
        .unwrap();
    // This write needs no reference flush: neither edges, target identity nor
    // lifecycle depend on note, even though the row has a legacy violation.
    sqlx::query("UPDATE workspaces SET note='content only' WHERE _id='legacy'")
        .execute(&writer)
        .await
        .unwrap();
    assert_eq!(fixture.violations().await, 1);
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_pending")
            .await,
        0
    );

    let mut transaction = fixture
        .control
        .begin_tenant_transaction(&fixture.route, false)
        .await
        .unwrap();
    execute_script(
        transaction.transaction(),
        "UPDATE workspaces SET teams='[]' WHERE _id='before'",
    )
    .await
    .unwrap();
    let (old_copy, new_copy, event_id): (Option<String>, Option<String>, Option<i64>) =
        sqlx::query_as("SELECT old_doc,new_doc,change_event_id FROM _gonvex_reference_pending")
            .fetch_one(&mut **transaction.transaction())
            .await
            .unwrap();
    assert_eq!(
        (old_copy, new_copy),
        (None, None),
        "reference staging must not duplicate the feed's row images or cold content"
    );
    assert!(
        event_id.is_some(),
        "reference work must point at the captured change"
    );
    transaction.commit().await.unwrap();

    // A sibling field used by sourceWhen becomes policy input and cannot be
    // changed by a writer that omits shared transactional validation.
    let mut catalog = fixture.catalog().await;
    catalog["references"][0]["sourceWhen"] = json!("$.note == \"guarded\"");
    fixture.declare(&catalog).await.unwrap();
    assert!(
        sqlx::query("UPDATE workspaces SET note='guarded' WHERE _id='legacy'")
            .execute(&writer)
            .await
            .is_err()
    );
    fixture
        .rejected("UPDATE workspaces SET note='guarded' WHERE _id='legacy'")
        .await;
    fixture.pools.close().await;
}

#[tokio::test]
async fn captured_references_share_one_guard_and_follow_savepoint_rollback() {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.scalar("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='_gonvex_reference_from_feed'").await,1);
    assert_eq!(fixture.scalar("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name LIKE '_gonvex_reference_%' AND tbl_name<>'_gonvex_sync_changes'").await,0);
    assert_eq!(fixture.scalar("SELECT count(*) FROM pragma_foreign_key_list('_gonvex_reference_pending') WHERE \"from\"='change_event_id' AND \"table\"='_gonvex_sync_changes' AND \"to\"='event_id'").await,1);
    let mut transaction = fixture
        .control
        .begin_tenant_transaction(&fixture.route, false)
        .await
        .unwrap();
    execute_script(
        transaction.transaction(),
        "SAVEPOINT edit; UPDATE workspaces SET teams='[]' WHERE _id='before'",
    )
    .await
    .unwrap();
    let pending:i64=sqlx::query_scalar("SELECT count(*) FROM _gonvex_reference_pending WHERE change_event_id IS NOT NULL AND old_doc IS NULL AND new_doc IS NULL")
        .fetch_one(&mut **transaction.transaction()).await.unwrap();
    assert_eq!(pending, 1);
    execute_script(transaction.transaction(), "ROLLBACK TO edit; RELEASE edit")
        .await
        .unwrap();
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_reference_pending")
        .fetch_one(&mut **transaction.transaction())
        .await
        .unwrap();
    assert_eq!(
        pending, 0,
        "rolling back the captured change must also remove its validation work"
    );
    transaction.commit().await.unwrap();
    fixture.pools.close().await;
}

#[tokio::test]
async fn inventory_and_commit_cross_batch_boundaries_without_partial_effects() {
    let fixture = Fixture::new().await;
    fixture.ok("WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<599) INSERT INTO workspaces SELECT 'bulk-'||i,'[\"before\"]',NULL,'cold content' FROM n").await;
    let catalog = fixture.catalog().await;
    fixture.declare(&catalog).await.unwrap();
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_edges WHERE source_id LIKE 'bulk-%'")
            .await,
        600
    );
    fixture.rejected("UPDATE workspaces SET teams=CASE WHEN _id='bulk-599' THEN '[\"missing-last-batch\"]' ELSE '[]' END WHERE _id LIKE 'bulk-%'").await;
    assert_eq!(
        fixture
            .scalar(
                "SELECT count(*) FROM workspaces WHERE _id LIKE 'bulk-%' AND teams='[\"before\"]'"
            )
            .await,
        600
    );
    fixture
        .rejected("DELETE FROM teams WHERE _id='before'")
        .await;
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_pending")
            .await,
        0
    );
    fixture.pools.close().await;
}

#[tokio::test]
async fn compiled_catalog_follows_commit_rollback_and_tenant_isolation() {
    let fixture = Fixture::new().await;
    // Populate the authoritative transaction's cache, then change the policy.
    fixture
        .ok("INSERT INTO history VALUES('first','before')")
        .await;
    let original = fixture.catalog().await;
    let mut next = original.clone();
    next["references"][0]["target"] = json!("approvals");
    // The changed policy is compiled during commit, but its bad source rolls
    // the whole declaration back. Original teams must still be accepted.
    fixture
        .rejected_declaration(
            &next,
            "INSERT INTO workspaces VALUES('bad','[\"missing\"]',NULL,'')",
        )
        .await;
    fixture
        .ok("INSERT INTO workspaces VALUES('after-rollback','[\"before\"]',NULL,'')")
        .await;
    fixture
        .rejected("INSERT INTO workspaces VALUES('still-bad','[\"missing\"]',NULL,'')")
        .await;

    fixture
        .ok("INSERT INTO approvals VALUES('new-parent',NULL,'[]')")
        .await;
    fixture.declare(&next).await.unwrap();
    fixture
        .ok("INSERT INTO workspaces VALUES('new-policy','[\"new-parent\"]',NULL,'')")
        .await;
    fixture
        .rejected("INSERT INTO workspaces VALUES('old-policy','[\"before\"]',NULL,'')")
        .await;
    fixture
        .run_on(
            &fixture.other,
            "INSERT INTO workspaces VALUES('other-tenant','[\"before\"]',NULL,'')",
        )
        .await
        .unwrap();
    // Reinstalling the original declaration restores the original contract;
    // prior invalid rows stay inventoried, never hidden or silently repaired.
    fixture.declare(&original).await.unwrap();
    fixture
        .ok("INSERT INTO workspaces VALUES('restored-policy','[\"before\"]',NULL,'')")
        .await;
    fixture
        .rejected("INSERT INTO workspaces VALUES('new-no-longer-valid','[\"new-parent\"]',NULL,'')")
        .await;
    fixture
        .ok("INSERT INTO teams VALUES('first-tenant-only',NULL)")
        .await;
    assert!(fixture
        .run_on(
            &fixture.other,
            "INSERT INTO history VALUES('must-not-borrow','first-tenant-only')",
        )
        .await
        .is_err());
    fixture
        .run_on(
            &fixture.other,
            "INSERT INTO teams VALUES('second-tenant-only',NULL); INSERT INTO history VALUES('own-parent','second-tenant-only')",
        )
        .await
        .unwrap();
    fixture
        .rejected("INSERT INTO history VALUES('must-not-borrow','second-tenant-only')")
        .await;
    fixture
        .pools
        .close_database(&fixture.route.database_url)
        .await;
    fixture
        .run_on(
            &fixture.other,
            "INSERT INTO history VALUES('after-other-closed','second-tenant-only')",
        )
        .await
        .unwrap();
    fixture.pools.close().await;
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
    fixture.pools.close().await;
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
    fixture.pools.close().await;
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
    fixture.pools.close().await;
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
    fixture.pools.close().await;
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
    fixture.pools.close().await;
}

#[tokio::test]
async fn migration_cannot_leave_a_declaration_pointing_at_a_removed_table_or_key() {
    let fixture = Fixture::new().await;
    for (suffix, sql) in [
        ("target", "DROP TABLE teams"),
        ("key", "ALTER TABLE teams RENAME COLUMN _id TO renamed"),
    ] {
        let migration = SqlMigration::new(
            format!("0002_remove_{suffix}.sql"),
            MigrationScope::Tenant,
            false,
            sql.to_owned(),
        );
        let error = fixture
            .control
            .clone()
            .provision_tenant_database(fixture.route.clone(), vec![migration])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("declared reference table teams lost its public key"),
            "{error}"
        );
        fixture
            .rejected("DELETE FROM teams WHERE _id='before'")
            .await;
        assert_eq!(
            fixture
                .scalar("SELECT count(*) FROM teams WHERE _id='before'")
                .await,
            1
        );
    }
    fixture.pools.close().await;
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
    fixture.pools.close().await;
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
    fixture.pools.close().await;
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
    fixture.pools.close().await;
}

#[tokio::test]
async fn valid_configuration_edits_and_reassignments_use_final_transaction_state() {
    let fixture = Fixture::new().await;
    fixture.ok("CREATE TABLE fields(_id TEXT PRIMARY KEY,options JSON,label TEXT); CREATE TABLE field_values(_id TEXT PRIMARY KEY,\"fieldId\" TEXT,value JSON)").await;
    fixture
        .declare(&binding_catalog(&fixture.catalog().await))
        .await
        .unwrap();
    fixture
        .ok(r#"INSERT INTO teams VALUES('selected',NULL);
      INSERT INTO workspaces VALUES('space-valid','[]',NULL,'');
      INSERT INTO fields VALUES('configuration','{"source":"manual"}','Field');
      INSERT INTO field_values VALUES('value','configuration','"selected"')"#)
        .await;
    // This changes the domain, but the existing value already names a real
    // target. A configuration version token must not reject a valid edit.
    fixture.ok(r#"UPDATE fields SET options='{"source":"entity","entity":"teams"}' WHERE _id='configuration'"#).await;
    fixture.ok(r#"UPDATE fields SET options='{"source":"entity","entity":"teams","label":"renamed"}' WHERE _id='configuration'"#).await;
    for parent_first in [false, true] {
        let parent = r#"UPDATE fields SET options='{"source":"entity","entity":"workspaces"}' WHERE _id='configuration';"#;
        let value = r#"UPDATE field_values SET value='"space-valid"' WHERE _id='value';"#;
        fixture
            .ok(&if parent_first {
                format!("{parent}{value}")
            } else {
                format!("{value}{parent}")
            })
            .await;
        fixture
            .rejected("DELETE FROM workspaces WHERE _id='space-valid'")
            .await;
        fixture.ok(r#"UPDATE fields SET options='{"source":"entity","entity":"teams"}' WHERE _id='configuration'; UPDATE field_values SET value='"selected"' WHERE _id='value'"#).await;
    }
    assert_eq!(fixture.violations().await, 0);
    // Ignoring an explicit flush error must not bypass the deferred guard.
    let writer = fixture
        .pools
        .pool(&fixture.route.database_url)
        .await
        .unwrap();
    let mut transaction = writer.begin().await.unwrap();
    execute_script(
        &mut transaction,
        "UPDATE field_values SET value='[{}]' WHERE _id='value'",
    )
    .await
    .unwrap();
    assert!(reference_integrity::flush(&mut transaction).await.is_err());
    assert!(transaction.commit().await.is_err());
    fixture
        .rejected("DELETE FROM teams WHERE _id='selected'")
        .await;
    fixture.pools.close().await;
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
    fixture.pools.close().await;
}

#[tokio::test]
async fn scoped_assertions_validate_final_parent_and_child_values_without_tally_fanout() {
    let fixture = Fixture::new().await;
    fixture.ok("CREATE TABLE polls(_id TEXT PRIMARY KEY,data JSON); CREATE TABLE ballots(_id TEXT PRIMARY KEY,postId TEXT,optionIds JSON)").await;
    let mut catalog = fixture.catalog().await;
    catalog["version"] = json!(2);
    for name in ["polls", "ballots"] {
        catalog["tables"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":name,"key":"_id","live":"true"}));
    }
    catalog["bindings"] = json!([{"name":"pollChoices","source":"polls","paths":["$.data.poll.options","$.data.poll.multiple"]}]);
    catalog["references"].as_array_mut().unwrap().extend([
        json!({"name":"ballot.shape","source":"ballots","column":"optionIds","state":"exists","sourceWhen":"true",
            "lookup":{"binding":"pollChoices","from":"document","key":"$.postId","required":true},
            "assert":"strict $document.optionIds.type() == \"array\" && $document.optionIds.size() > 0 && ($.data.poll.multiple == true || $document.optionIds.size() == 1)"}),
        json!({"name":"ballot.options","source":"ballots","column":"optionIds","state":"exists","sourceWhen":"true","at":"strict $.optionIds[*]",
            "lookup":{"binding":"pollChoices","from":"document","key":"$.postId","required":true},
            "assert":"strict $entry.type() == \"string\" && exists($.data.poll.options[*] ? (@.id == $entry))"}),
    ]);
    fixture.declare(&catalog).await.unwrap();
    fixture.ok(r#"INSERT INTO polls VALUES('p','{"poll":{"options":[{"id":"a"},{"id":"b"}],"multiple":true,"tallies":{}}}'); INSERT INTO ballots VALUES('v','p','["a","b"]')"#).await;
    let version = fixture.scalar("SELECT version FROM _gonvex_reference_configurations WHERE binding_name='pollChoices' AND row_id='p'").await;
    fixture
        .ok(r#"UPDATE polls SET data=json_set(data,'$.poll.tallies.a',1) WHERE _id='p'"#)
        .await;
    assert_eq!(fixture.scalar("SELECT version FROM _gonvex_reference_configurations WHERE binding_name='pollChoices' AND row_id='p'").await,version,"tally writes must not version the choice domain or fan out to other voters");
    for sql in [
        "UPDATE ballots SET optionIds='[]'",
        "UPDATE ballots SET optionIds='\"a\"'",
        "UPDATE ballots SET optionIds='[1]'",
        "UPDATE ballots SET optionIds='[\"absent\"]'",
        "UPDATE ballots SET postId='missing'",
        "DELETE FROM polls",
        "UPDATE polls SET data=json_set(data,'$.poll.multiple',json('false'))",
        "UPDATE polls SET data=json_set(data,'$.poll.options',json('[{\"id\":\"b\"}]'))",
    ] {
        fixture.rejected(sql).await;
    }
    for parent_first in [true, false] {
        let parent = r#"UPDATE polls SET data=json_set(data,'$.poll.options',json('[{"id":"c"}]'),'$.poll.multiple',json('false'));"#;
        let child = "UPDATE ballots SET optionIds='[\"c\"]';";
        fixture
            .ok(&if parent_first {
                format!("{parent}{child}")
            } else {
                format!("{child}{parent}")
            })
            .await;
        fixture.ok(r#"UPDATE polls SET data=json_set(data,'$.poll.options',json('[{"id":"a"},{"id":"b"}]'),'$.poll.multiple',json('true')); UPDATE ballots SET optionIds='["a","b"]'"#).await;
    }
    // Ignoring a flush error cannot release the original deferred SQL guard.
    let writer = fixture
        .pools
        .pool(&fixture.route.database_url)
        .await
        .unwrap();
    let mut tx = writer.begin().await.unwrap();
    execute_script(&mut tx, "UPDATE ballots SET optionIds='[\"absent\"]'")
        .await
        .unwrap();
    assert!(reference_integrity::flush(&mut tx).await.is_err());
    assert!(tx.commit().await.is_err());
    assert_eq!(
        fixture
            .text("SELECT optionIds FROM ballots")
            .await
            .as_deref(),
        Some("[\"a\",\"b\"]")
    );
    let mut older = catalog.clone();
    older["version"] = json!(1);
    assert!(fixture.declare(&older).await.is_err());
    let mut invalid = catalog.clone();
    invalid["bindings"][0]["paths"] = json!(["$.data.poll[*]"]);
    assert!(fixture.declare(&invalid).await.is_err());
    invalid["bindings"][0]["paths"] = json!(["$.data", "$.data.poll"]);
    assert!(fixture.declare(&invalid).await.is_err());
    // A failing assertion in old rows refuses installation, rather than
    // discarding values or claiming this refinement is a grandfathered FK.
    let mut invalid = catalog.clone();
    invalid["references"]
        .as_array_mut()
        .unwrap()
        .last_mut()
        .unwrap()["assert"] = json!("false");
    assert!(fixture.declare(&invalid).await.is_err());
    assert_eq!(fixture.catalog().await, catalog);
    fixture.ok("UPDATE ballots SET optionIds='[\"a\"]'").await;
    fixture.pools.close().await;
}

#[tokio::test]
async fn scoped_numeric_keys_and_owner_changes_use_explicit_configuration_assertions() {
    let fixture = Fixture::new().await;
    fixture.ok("CREATE TABLE versions(_id TEXT PRIMARY KEY,formId TEXT,fields JSON); CREATE TABLE links(_id TEXT PRIMARY KEY,versionId TEXT,formId TEXT,fieldId TEXT)").await;
    let mut catalog = fixture.catalog().await;
    catalog["version"] = json!(2);
    for name in ["versions", "links"] {
        catalog["tables"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":name,"key":"_id","live":"true"}));
    }
    catalog["bindings"] =
        json!([{"name":"versionFields","source":"versions","columns":["formId","fields"]}]);
    catalog["references"].as_array_mut().unwrap().push(json!({"name":"link.field","source":"links","column":"fieldId","state":"exists","sourceWhen":"true",
        "lookup":{"binding":"versionFields","from":"document","key":"$.versionId","required":true},
        "assert":"strict $.formId == $document.formId && exists($.fields[*] ? (@.id.string() == $document.fieldId))"}));
    fixture.declare(&catalog).await.unwrap();
    fixture.ok(r#"INSERT INTO versions VALUES('v','f','[{"id":17}]'); INSERT INTO links VALUES('l','v','f','17')"#).await;
    fixture.rejected("UPDATE links SET fieldId='017'").await;
    fixture.rejected("UPDATE links SET formId='other'").await;
    fixture.rejected("UPDATE versions SET formId='other'").await;
    fixture.rejected("UPDATE versions SET fields='[]'").await;
    fixture
        .ok("UPDATE links SET formId='other'; UPDATE versions SET formId='other'")
        .await;
    fixture.rejected("DELETE FROM versions").await;
    fixture.ok("DELETE FROM versions; DELETE FROM links").await;
    fixture.pools.close().await;
}

#[tokio::test]
async fn exact_one_owner_unions_revalidate_incoming_edges_on_both_target_deltas() {
    let fixture = Fixture::with_migration(include_str!("reference_integrity/aliases.sql")).await;
    fixture.ok("DELETE FROM aliasBackups").await;
    let mut catalog = fixture.catalog().await;
    catalog["version"] = json!(2);
    catalog["references"][0]["singleOwner"] = json!(true);
    catalog["references"][0]["ownerReason"] =
        json!("An untyped union selector must resolve to exactly one physical owner.");
    fixture.declare(&catalog).await.unwrap();
    fixture
        .rejected("INSERT INTO aliasBackups VALUES('one',NULL)")
        .await;
    fixture.ok("INSERT INTO aliasBackups VALUES('unreferenced',NULL); INSERT INTO aliasTeams VALUES('unreferenced','unused',NULL)").await;
    fixture
        .rejected("UPDATE aliasLinks SET ref='unreferenced'")
        .await;
    fixture
        .ok("UPDATE aliasLinks SET ref=NULL,alias=NULL,payload='{}'")
        .await;
    fixture.ok("DELETE FROM aliasTeams WHERE _id='one'; INSERT INTO aliasBackups VALUES('one',NULL); UPDATE aliasLinks SET ref='one'").await;
    // Public IDs are immutable; the supported delete/insert key move must
    // also preserve the incoming reference rather than leave it dangling.
    fixture.rejected("DELETE FROM aliasBackups WHERE _id='one'; INSERT INTO aliasBackups VALUES('other',NULL)").await;
    // Both serial writer orders refuse the conflicting second intent.
    for target_first in [true, false] {
        fixture.ok("UPDATE aliasLinks SET ref=NULL; DELETE FROM aliasTeams WHERE _id='unreferenced'; DELETE FROM aliasBackups WHERE _id='unreferenced'; INSERT INTO aliasTeams VALUES('unreferenced','unused',NULL)").await;
        let duplicate = "INSERT INTO aliasBackups VALUES('unreferenced',NULL)";
        let link = "UPDATE aliasLinks SET ref='unreferenced'";
        fixture
            .ok(if target_first { duplicate } else { link })
            .await;
        fixture
            .rejected(if target_first { link } else { duplicate })
            .await;
    }
    fixture.pools.close().await;
}

#[tokio::test]
async fn strict_existing_policy_refuses_an_invalid_upgrade_atomically() {
    let fixture = Fixture::new().await;
    let before = fixture.catalog().await;
    let mut strict = before.clone();
    strict["version"] = json!(2);
    strict["existing"] = json!("reject");
    assert!(fixture.declare(&strict).await.is_err());
    assert_eq!(fixture.catalog().await, before);
    assert_eq!(fixture.violations().await, 1);
    fixture.ok("INSERT INTO teams VALUES('missing',NULL)").await;
    fixture.declare(&strict).await.unwrap();
    assert_eq!(fixture.violations().await, 0);
    fixture
        .rejected("DELETE FROM teams WHERE _id='missing'")
        .await;
    fixture.pools.close().await;
}

#[tokio::test]
async fn only_referenced_configurations_and_requested_target_states_are_stored() {
    let fixture=Fixture::with_migration(r#"CREATE TABLE targets(_id TEXT PRIMARY KEY,deletedAt INTEGER);
      CREATE TABLE parents(_id TEXT PRIMARY KEY,options JSON);
      CREATE TABLE sources(_id TEXT PRIMARY KEY,parentId TEXT,value JSON);
      INSERT INTO targets VALUES('selected',NULL);
      WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<600)
      INSERT INTO parents SELECT 'parent-'||i,'{"source":"entity","coldLabel":"not part of the domain"}' FROM n;"#).await;
    let catalog = json!({"version":2,"existing":"reject","tables":[
      {"name":"targets","key":"_id","live":"$.deletedAt == null"},
      {"name":"parents","key":"_id","live":"true"},
      {"name":"sources","key":"_id","live":"true"}],
      "bindings":[{"name":"domain","source":"parents","paths":["$.options.source"]}],
      "references":[{"name":"source.value","source":"sources","column":"value","path":"$.value[*]","state":"exists",
        "lookup":{"binding":"domain","key":"$.parentId","required":true},"target":"targets"}]});
    fixture.declare(&catalog).await.unwrap();
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_configurations")
            .await,
        0,
        "unused parents are not copied"
    );
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_targets WHERE state='live'")
            .await,
        0,
        "no rule requests a live-state token"
    );
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_targets")
            .await,
        1,
        "source-only identities are not target dictionaries"
    );
    fixture
        .ok(r#"INSERT INTO sources VALUES('s','parent-1','["selected"]')"#)
        .await;
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_configurations")
            .await,
        1
    );
    let stored = fixture
        .text("SELECT document FROM _gonvex_reference_configurations")
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&stored).unwrap(),
        json!({"options":{"source":"entity"}})
    );
    fixture
        .rejected(r#"INSERT INTO sources VALUES('invalid','parent-2','["absent"]')"#)
        .await;
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_configurations")
            .await,
        1,
        "failed source creation must roll back lazy derived state too"
    );
    fixture
        .rejected("DELETE FROM parents WHERE _id='parent-1'")
        .await;
    fixture
        .ok("UPDATE targets SET deletedAt=1 WHERE _id='selected'")
        .await;
    fixture.ok("DELETE FROM sources").await;
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_configurations")
            .await,
        0,
        "last reference owns projection lifetime"
    );
    fixture
        .ok(r#"UPDATE parents SET options='{"source":"other"}' WHERE _id='parent-500'"#)
        .await;
    assert_eq!(
        fixture
            .scalar("SELECT count(*) FROM _gonvex_reference_configurations")
            .await,
        0
    );
    fixture.pools.close().await;
}

// Review regressions: a batch cursor must retain SQLite storage class, even
// though the public row image deliberately normalizes identities to JSON.
async fn review_inventory_keyset(expression: &str) {
    let f = Fixture::new().await;
    f.ok(&format!("CREATE TABLE reviewTargets(_id PRIMARY KEY); CREATE TABLE reviewSources(_id TEXT PRIMARY KEY, refs JSON); WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<599) INSERT INTO reviewTargets SELECT {expression} FROM n; INSERT INTO reviewSources VALUES('source','[]');")).await;
    let catalog = json!({"version":2,"existing":"reject","tables":[
        {"name":"reviewTargets","key":"_id","live":"true"},
        {"name":"reviewSources","key":"_id","live":"true"}],
        "references":[{"name":"review.targets","source":"reviewSources","path":"$.refs[*]","target":"reviewTargets","state":"exists"}]});
    let installed =
        tokio::time::timeout(std::time::Duration::from_secs(3), f.declare(&catalog)).await;
    let count = if matches!(installed, Ok(Ok(()))) {
        f.scalar("SELECT count(*) FROM _gonvex_reference_targets WHERE table_name='reviewTargets'")
            .await
    } else {
        0
    };
    f.pools.close().await;
    assert!(
        matches!(installed, Ok(Ok(()))),
        "typed inventory did not finish: {installed:?}"
    );
    assert_eq!(count, 600, "typed inventory skipped keys for {expression}");
}

#[tokio::test]
async fn review_inventory_integer_keys_without_affinity() {
    review_inventory_keyset("i").await;
}
#[tokio::test]
async fn review_inventory_real_keys_without_affinity() {
    review_inventory_keyset("i+0.5").await;
}
#[tokio::test]
async fn review_inventory_blob_keys() {
    review_inventory_keyset("CAST(printf('%04d',i) AS BLOB)").await;
}
#[tokio::test]
async fn review_inventory_mixed_storage_classes() {
    review_inventory_keyset("CASE WHEN i<200 THEN i WHEN i<400 THEN printf('text-%04d',i) ELSE CAST(printf('blob-%04d',i) AS BLOB) END").await;
}

#[tokio::test]
async fn review_root_entry_dependencies_enforce_lookups_predicates_and_assertions() {
    let f = Fixture::new().await;
    f.ok("CREATE TABLE reviewParents(_id TEXT PRIMARY KEY,kind TEXT); CREATE TABLE reviewChildren(_id TEXT PRIMARY KEY,targetId TEXT,parentId TEXT,secondaryParentId TEXT,expected TEXT,targetKind TEXT,assertParentId TEXT,assertion TEXT); INSERT INTO reviewParents VALUES('p','teams'),('p2','teams'); INSERT INTO reviewChildren VALUES('child','before','p','p2','teams','teams','p','teams');").await;
    let catalog = json!({"version":2,"existing":"reject","tables":[
        {"name":"teams","key":"_id","live":"$.deletedAt == null"},
        {"name":"reviewParents","key":"_id","live":"true"},
        {"name":"reviewChildren","key":"_id","live":"true"}],
        "bindings":[{"name":"reviewDomain","source":"reviewParents","columns":["kind"]}],
        "references":[
          {"name":"review.ref","source":"reviewChildren","path":"$.targetId","state":"exists",
           "lookup":[{"binding":"reviewDomain","key":"$entry.parentId","when":"$.kind == $entry.expected"},
                     {"binding":"reviewDomain","from":"configuration","key":"$entry.secondaryParentId","when":"$.kind == $entry.expected"}],
           "discriminator":"$entry.targetKind","targets":{"teams":"teams"}},
          {"name":"review.assert","source":"reviewChildren","state":"exists",
           "lookup":{"binding":"reviewDomain","key":"$entry.assertParentId"},"assert":"$entry.assertion == $.kind"}]});
    let installed = f.declare(&catalog).await;
    let edges = f
        .scalar("SELECT count(*) FROM _gonvex_reference_edges WHERE source_table='reviewChildren'")
        .await;
    let invalid_target = f
        .run("UPDATE reviewChildren SET targetId='missing'")
        .await
        .is_err();
    let invalid_assertion = f
        .run("UPDATE reviewChildren SET assertion='wrong'")
        .await
        .is_err();
    let invalid_discriminator = f
        .run("UPDATE reviewChildren SET targetKind='unknown'")
        .await
        .is_err();
    let invalid_parent = f
        .run("UPDATE reviewParents SET kind='wrong' WHERE _id='p'")
        .await
        .is_err();
    f.pools.close().await;
    assert!(
        installed.is_ok(),
        "valid root entry rule rejected: {installed:?}"
    );
    assert_eq!(
        edges, 4,
        "optional root entry lookups silently omitted edges"
    );
    assert!(invalid_target && invalid_assertion && invalid_discriminator && invalid_parent);
}

#[tokio::test]
async fn review_storage_decoding_preserves_literal_ids_without_explicit_normalization() {
    let f = Fixture::with_migration(include_str!("reference_integrity/aliases.sql")).await;
    f.ok("INSERT INTO aliasTeams VALUES(' a,b ','literal-code',NULL)")
        .await;
    let object = f
        .run("UPDATE aliasLinks SET payload=json_object('first',' a,b ','other','builtin')")
        .await;
    let serialized = f.run("UPDATE aliasLinks SET payload=json_quote(json_object('first',' a,b ','other','builtin'))").await;
    let protected = f
        .run("DELETE FROM aliasTeams WHERE _id=' a,b '")
        .await
        .is_err();
    f.pools.close().await;
    assert!(object.is_ok(), "literal JSON ID changed: {object:?}");
    assert!(
        serialized.is_ok(),
        "serialized literal ID changed: {serialized:?}"
    );
    assert!(protected, "literal ID was not protected from deletion");
}

#[tokio::test]
async fn review_non_text_source_keys_and_configuration_lookups_use_indexed_typed_probes() {
    for key in ["17", "CAST('child' AS BLOB)"] {
        let f = Fixture::new().await;
        f.ok("CREATE TABLE reviewParents(_id PRIMARY KEY,allowed BOOLEAN); CREATE TABLE reviewChildren(_id PRIMARY KEY,ref TEXT,parentId TEXT); INSERT INTO reviewParents VALUES(CAST('p' AS BLOB),1);").await;
        let catalog = json!({"version":2,"existing":"reject","tables":[
            {"name":"teams","key":"_id","live":"$.deletedAt == null"},
            {"name":"reviewParents","key":"_id","live":"true"},
            {"name":"reviewChildren","key":"_id","live":"true"}],
            "bindings":[{"name":"reviewAllowed","source":"reviewParents","columns":["allowed"]}],
            "references":[
              {"name":"review.ref","source":"reviewChildren","path":"$.ref","target":"teams","state":"exists"},
              {"name":"review.assert","source":"reviewChildren","state":"exists","lookup":{"binding":"reviewAllowed","key":"$.parentId","required":true},"assert":"$.allowed == true"}]});
        f.declare(&catalog).await.unwrap();
        let valid = f
            .run(&format!(
                "INSERT INTO reviewChildren VALUES({key},'before','70')"
            ))
            .await;
        let invalid = f
            .run("UPDATE reviewChildren SET ref='missing'")
            .await
            .is_err();
        let parent = f.run("UPDATE reviewParents SET allowed=0").await.is_err();
        let target = f.run("DELETE FROM teams WHERE _id='before'").await.is_err();
        let identity = if key == "17" { "17" } else { "6368696c64" };
        let ambiguous = f
            .run(&format!(
                "INSERT INTO reviewChildren VALUES('{identity}','before','70')"
            ))
            .await
            .is_err();
        let remaining = f.scalar("SELECT count(*) FROM reviewChildren").await;
        f.pools.close().await;
        assert!(valid.is_ok(), "valid typed-key write rejected: {valid:?}");
        assert!(
            invalid && parent && target,
            "typed-key point lookup lost enforcement"
        );
        assert!(
            ambiguous,
            "ambiguous normalized source identity was accepted"
        );
        assert_eq!(remaining, 1, "ambiguous insertion was not rolled back");
    }
}

#[tokio::test]
async fn review_runtime_upgrade_rebuilds_older_capture_projections() {
    use sha2::{Digest, Sha256};

    let f = Fixture::new().await;
    let catalog = json!({"version":2,"existing":"reject","tables":[
        {"name":"teams","key":"_id","live":"true"},
        {"name":"workspaces","key":"_id","live":"true"}],
        "bindings":[{"name":"reviewTeam","source":"teams","columns":["deletedAt"]}],
        "references":[{"name":"review.root","source":"workspaces","state":"exists","sourceWhen":"$._id == \"before\"",
            "lookup":{"binding":"reviewTeam","key":"$entry.note","required":true},
            "assert":"$.deletedAt == null"}]});
    f.declare(&catalog).await.unwrap();
    let migration = SqlMigration::new(
        "0001_references.sql".to_owned(),
        MigrationScope::Tenant,
        false,
        include_str!("reference_integrity/fixture.sql").to_owned(),
    );
    f.control
        .clone()
        .provision_tenant_database(f.route.clone(), vec![migration.clone()])
        .await
        .unwrap();

    // Reproduce the previous compiler's persisted projection and otherwise
    // valid receipt. Neither the schema cookie nor catalog changed.
    let mut transaction = f
        .control
        .begin_tenant_transaction(&f.route, false)
        .await
        .unwrap();
    let connection: &mut sqlx::SqliteConnection = transaction.transaction();
    sqlx::query("UPDATE _gonvex_reference_tables SET capture_columns='[\"_id\"]' WHERE table_name='workspaces'")
        .execute(&mut *connection).await.unwrap();
    let cookie: i64 = sqlx::query_scalar("PRAGMA main.schema_version")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let digest: String = sqlx::query_scalar(
        "SELECT digest FROM _gonvex_reference_catalog_revision WHERE singleton=1",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    let trigger: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_schema WHERE name='_gonvex_reference_from_feed'",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    let old_receipt = format!(
        "reference-feed-v1:{cookie}:{digest}:{:x}",
        Sha256::digest(trigger.as_bytes())
    );
    sqlx::query("UPDATE _gonvex_sync_bootstrap SET digest=$1 WHERE name='reference-capture'")
        .bind(old_receipt)
        .execute(connection)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    f.control
        .clone()
        .provision_tenant_database(f.route.clone(), vec![migration])
        .await
        .unwrap();
    let repaired = f.scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_reference_tables,json_each(capture_columns) WHERE table_name='workspaces' AND value='note')").await;
    let rejected = f
        .run("UPDATE workspaces SET note='missing' WHERE _id='before'")
        .await
        .is_err();
    let note = f
        .text("SELECT note FROM workspaces WHERE _id='before'")
        .await;
    f.pools.close().await;
    assert_eq!(
        repaired, 1,
        "old compiler projection survived runtime upgrade"
    );
    assert!(
        rejected,
        "upgraded capture projection omitted root entry validation"
    );
    assert_eq!(note.as_deref(), Some("before"));
}

#[tokio::test]
async fn review_noninvertible_public_key_encodings_are_refused_atomically() {
    for kind in ["BOOLEAN", "JSON"] {
        for populated in [false, true] {
            let f = Fixture::new().await;
            f.ok(&format!(
                "CREATE TABLE reviewEncodedKeys(_id {kind} PRIMARY KEY,ref TEXT)"
            ))
            .await;
            if populated {
                f.ok("INSERT INTO reviewEncodedKeys VALUES(1,'before')")
                    .await;
            }
            let previous = f.catalog().await;
            let previous_edges = f
                .scalar("SELECT count(*) FROM _gonvex_reference_edges")
                .await;
            let catalog = json!({"version":2,"existing":"reject","tables":[
                {"name":"teams","key":"_id","live":"true"},
                {"name":"reviewEncodedKeys","key":"_id","live":"true"}],
                "references":[{"name":"review.encoded","source":"reviewEncodedKeys","path":"$.ref","target":"teams","state":"exists"}]});
            let outcome = f.declare(&catalog).await;
            let unchanged = f.catalog().await == previous;
            let edges = f
                .scalar("SELECT count(*) FROM _gonvex_reference_edges")
                .await;
            let rows = f.scalar("SELECT count(*) FROM reviewEncodedKeys").await;
            f.pools.close().await;
            let error = outcome.expect_err("noninvertible public key encoding was accepted");
            assert!(error.to_string().contains("public key encoding"), "{error}");
            assert!(
                unchanged,
                "failed declaration replaced the previous catalog"
            );
            assert_eq!(
                edges, previous_edges,
                "failed declaration changed existing edges"
            );
            assert_eq!(
                rows,
                i64::from(populated),
                "declaration changed application rows"
            );
        }
    }
}

#[tokio::test]
async fn review_schema_cannot_change_an_enforced_key_to_boolean() {
    let f = Fixture::new().await;
    f.ok("CREATE TABLE reviewTypedKeys(_id INTEGER PRIMARY KEY,ref TEXT); INSERT INTO reviewTypedKeys VALUES(1,'before')").await;
    let catalog = json!({"version":2,"existing":"reject","tables":[
        {"name":"teams","key":"_id","live":"true"},
        {"name":"reviewTypedKeys","key":"_id","live":"true"}],
        "references":[{"name":"review.typed","source":"reviewTypedKeys","path":"$.ref","target":"teams","state":"exists"}]});
    f.declare(&catalog).await.unwrap();
    let migrations = vec![
        SqlMigration::new("0001_references.sql".into(), MigrationScope::Tenant, false,
            include_str!("reference_integrity/fixture.sql").into()),
        SqlMigration::new("0002_boolean_key.sql".into(), MigrationScope::Tenant, false,
            "ALTER TABLE reviewTypedKeys RENAME TO reviewOldKeys; CREATE TABLE reviewTypedKeys(_id BOOLEAN PRIMARY KEY,ref TEXT); INSERT INTO reviewTypedKeys SELECT * FROM reviewOldKeys; DROP TABLE reviewOldKeys;".into())];
    let outcome = f
        .control
        .clone()
        .provision_tenant_database(f.route.clone(), migrations)
        .await;
    let kind = f
        .text("SELECT type FROM pragma_table_info('reviewTypedKeys') WHERE name='_id'")
        .await;
    let journal = f
        .scalar("SELECT count(*) FROM gonvex_migrations WHERE name='0002_boolean_key.sql'")
        .await;
    let rejected = f
        .run("UPDATE reviewTypedKeys SET ref='missing' WHERE _id=1")
        .await
        .is_err();
    let reference = f.text("SELECT ref FROM reviewTypedKeys WHERE _id=1").await;
    f.pools.close().await;
    let error = outcome.expect_err("schema changed an enforced key to a noninvertible encoding");
    assert!(error.to_string().contains("public key encoding"), "{error}");
    assert_eq!(kind.as_deref(), Some("INTEGER"));
    assert_eq!(journal, 0, "failed schema change was recorded");
    assert!(rejected, "failed migration disabled previous enforcement");
    assert_eq!(reference.as_deref(), Some("before"));
}
