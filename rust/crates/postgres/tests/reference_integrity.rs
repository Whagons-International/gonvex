use std::{collections::BTreeMap, time::SystemTime};

use gonvex_postgres::{
    ControlPlane, MigrationScope, PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
};
use sqlx::{postgres::PgPoolOptions, PgPool};

struct Fixture {
    admin: PgPool,
    tenant: PgPool,
    other: PgPool,
    schemas: Vec<String>,
    pools: PoolRegistry,
    control: ControlPlane,
    route: TenantRoute,
}

impl Fixture {
    async fn new() -> Option<Self> {
        Self::with_migration(include_str!("reference_integrity/fixture.sql")).await
    }

    async fn with_migration(source: &str) -> Option<Self> {
        let base = std::env::var("GONVEX_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())?;
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&base)
            .await
            .unwrap();
        let schemas = ["control", "tenant", "other"]
            .map(|kind| format!("gonvex_refs_{kind}_{nonce}"))
            .to_vec();
        for schema in &schemas {
            sqlx::raw_sql(&format!("CREATE SCHEMA {schema}"))
                .execute(&admin)
                .await
                .unwrap();
        }
        let separator = if base.contains('?') { '&' } else { '?' };
        let urls = schemas
            .iter()
            .map(|schema| format!("{base}{separator}options=-csearch_path%3D{schema}"))
            .collect::<Vec<_>>();
        let pools = PoolRegistry::new(PoolLimits::default());
        let control = ControlPlane::connect(&urls[0], pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        for (index, url) in urls.iter().enumerate().skip(1) {
            let route = TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: format!("tenant-{index}"),
                database_url: url.clone(),
            };
            let migration = SqlMigration::new(
                "0001_references.sql".to_owned(),
                MigrationScope::Tenant,
                false,
                source.to_owned(),
            );
            control
                .clone()
                .provision_tenant_database(route.clone(), vec![migration.clone()])
                .await
                .unwrap();
            // Reprovisioning must retain the declarations, legacy edges and guards.
            control
                .clone()
                .provision_tenant_database(route, vec![migration])
                .await
                .unwrap();
        }
        let tenant = PgPoolOptions::new()
            .max_connections(4)
            .connect(&urls[1])
            .await
            .unwrap();
        let other = PgPoolOptions::new()
            .max_connections(1)
            .connect(&urls[2])
            .await
            .unwrap();
        Some(Self {
            admin,
            tenant,
            other,
            schemas,
            pools,
            control,
            route: TenantRoute {
                project_id: "project".to_owned(),
                tenant_id: "tenant-1".to_owned(),
                database_url: urls[1].clone(),
            },
        })
    }

    async fn close(self) {
        self.tenant.close().await;
        self.other.close().await;
        self.pools.close().await;
        for schema in self.schemas.into_iter().rev() {
            sqlx::raw_sql(&format!("DROP SCHEMA {schema} CASCADE"))
                .execute(&self.admin)
                .await
                .unwrap();
        }
        self.admin.close().await;
    }
}

fn assert_integrity_error(error: sqlx::Error) {
    let code = error
        .as_database_error()
        .and_then(|error| error.code())
        .unwrap();
    assert!(
        matches!(code.as_ref(), "23503" | "40001" | "40P01"),
        "{error}"
    );
}

// The deferred change-feed clock also participates in commit locking, so a
// valid database deadlock victim may be either writer. The safety property is
// that both conflicting intents cannot commit; validate every rejected result.
fn assert_race_integrity(first: Result<(), sqlx::Error>, second: Result<(), sqlx::Error>) {
    assert!(
        first.is_err() || second.is_err(),
        "both conflicting transactions committed"
    );
    for result in [first, second] {
        if let Err(error) = result {
            assert_integrity_error(error);
        }
    }
}

#[tokio::test]
async fn declared_references_protect_all_sql_writers_without_repairing_legacy_rows() {
    let Some(fixture) = Fixture::new().await else {
        eprintln!("GONVEX_TEST_POSTGRES_URL not set; skipping PostgreSQL reference test");
        return;
    };
    sqlx::raw_sql(include_str!("reference_integrity/regression.sql"))
        .execute(&fixture.tenant)
        .await
        .unwrap();
    // A matching ID in another tenant cannot satisfy this tenant's FK.
    sqlx::raw_sql("INSERT INTO teams VALUES('other-only',NULL)")
        .execute(&fixture.other)
        .await
        .unwrap();
    let error =
        sqlx::raw_sql("INSERT INTO workspaces VALUES('cross-tenant','[\"other-only\"]',NULL,NULL)")
            .execute(&fixture.tenant)
            .await
            .unwrap_err();
    assert_integrity_error(error);
    let exposed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM _gonvex_sync_changes WHERE table_name LIKE '_gonvex_reference_%'",
    )
    .fetch_one(&fixture.tenant)
    .await
    .unwrap();
    assert_eq!(
        exposed, 0,
        "reference infrastructure must not enter the application change feed"
    );
    fixture.close().await;
}

#[tokio::test]
async fn concurrent_link_and_delete_cannot_commit_a_dangling_reference() {
    let Some(fixture) = Fixture::new().await else {
        eprintln!("GONVEX_TEST_POSTGRES_URL not set; skipping PostgreSQL reference races");
        return;
    };
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        for delete_first in [true, false] {
            sqlx::raw_sql("INSERT INTO teams VALUES('race',NULL)")
                .execute(&fixture.tenant)
                .await
                .unwrap();
            let mut link = fixture.tenant.begin().await.unwrap();
            let mut deletion = fixture.tenant.begin().await.unwrap();
            for transaction in [&mut link, &mut deletion] {
                sqlx::raw_sql(&format!(
                    "SET TRANSACTION ISOLATION LEVEL {isolation}; SELECT count(*) FROM teams"
                ))
                .execute(&mut **transaction)
                .await
                .unwrap();
            }
            if delete_first {
                sqlx::raw_sql("DELETE FROM teams WHERE _id='race'")
                    .execute(&mut *deletion)
                    .await
                    .unwrap();
                sqlx::raw_sql("INSERT INTO workspaces VALUES('race','[\"race\"]',NULL,NULL)")
                    .execute(&mut *link)
                    .await
                    .unwrap();
                let (link_result, delete_result) = tokio::join!(link.commit(), deletion.commit());
                assert_race_integrity(link_result, delete_result);
            } else {
                sqlx::raw_sql("INSERT INTO workspaces VALUES('race','[\"race\"]',NULL,NULL); SET CONSTRAINTS gonvex_reference_target IMMEDIATE")
                    .execute(&mut *link).await.unwrap();
                let deleting = async {
                    sqlx::raw_sql("DELETE FROM teams WHERE _id='race'")
                        .execute(&mut *deletion)
                        .await?;
                    deletion.commit().await
                };
                let (delete_result, link_result) = tokio::join!(deleting, link.commit());
                assert_race_integrity(link_result, delete_result);
            }
            let orphans: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM gonvex_reference_violations() WHERE source_id='race'",
            )
            .fetch_one(&fixture.tenant)
            .await
            .unwrap();
            assert_eq!(orphans, 0, "{isolation}, delete_first={delete_first}");
            sqlx::raw_sql(
                "DELETE FROM workspaces WHERE _id='race'; DELETE FROM teams WHERE _id='race'",
            )
            .execute(&fixture.tenant)
            .await
            .unwrap();
        }
    }
    fixture.close().await;
}

#[tokio::test]
async fn alternate_keys_unions_lifecycle_and_catalog_upgrades_are_enforced() {
    let Some(fixture) =
        Fixture::with_migration(include_str!("reference_integrity/aliases.sql")).await
    else {
        return;
    };
    fixture.close().await;
}

#[tokio::test]
async fn concurrent_union_owners_cannot_remove_the_last_referenced_target() {
    let Some(fixture) =
        Fixture::with_migration(include_str!("reference_integrity/union.sql")).await
    else {
        return;
    };
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        for soft in [false, true] {
            sqlx::raw_sql("INSERT INTO union_owners_a VALUES('shared-id','code',NULL); INSERT INTO union_owners_b VALUES('shared-id',NULL); INSERT INTO union_links VALUES('link','shared-id',NULL)")
                .execute(&fixture.tenant).await.unwrap();
            let mut first = fixture.tenant.begin().await.unwrap();
            let mut second = fixture.tenant.begin().await.unwrap();
            for transaction in [&mut first, &mut second] {
                sqlx::raw_sql(&format!("SET TRANSACTION ISOLATION LEVEL {isolation}; SELECT count(*) FROM union_owners_a"))
                    .execute(&mut **transaction).await.unwrap();
            }
            let action = if soft {
                "UPDATE union_owners_a SET \"deletedAt\"=1"
            } else {
                "DELETE FROM union_owners_a"
            };
            sqlx::raw_sql(action).execute(&mut *first).await.unwrap();
            let other_action = if soft {
                "UPDATE union_owners_b SET \"deletedAt\"=1"
            } else {
                "DELETE FROM union_owners_b"
            };
            let removing_second = async {
                sqlx::raw_sql(other_action).execute(&mut *second).await?;
                second.commit().await
            };
            let (first_result, second_result) = tokio::join!(first.commit(), removing_second);
            assert_race_integrity(first_result, second_result);
            let (owners,token_owners,orphans):(i64,i64,i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM union_owners_a WHERE \"deletedAt\" IS NULL)+(SELECT count(*) FROM union_owners_b WHERE \"deletedAt\" IS NULL), (SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='shared-id' AND state='live'), (SELECT count(*) FROM gonvex_reference_violations())")
                .fetch_one(&fixture.tenant).await.unwrap();
            assert!(owners > 0, "both shared owners disappeared");
            assert_eq!(owners, token_owners, "{isolation}, soft={soft}");
            assert_eq!(orphans, 0);
            sqlx::raw_sql(
                "DELETE FROM union_links; DELETE FROM union_owners_a; DELETE FROM union_owners_b",
            )
            .execute(&fixture.tenant)
            .await
            .unwrap();
        }
    }
    fixture.close().await;
}

#[tokio::test]
async fn concurrent_union_replacement_preserves_tokens_and_exact_owner_counts() {
    let Some(fixture) =
        Fixture::with_migration(include_str!("reference_integrity/union.sql")).await
    else {
        return;
    };
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        sqlx::raw_sql("INSERT INTO union_owners_a VALUES('shared-id','code',NULL); INSERT INTO union_links VALUES('link','shared-id',NULL)")
            .execute(&fixture.tenant).await.unwrap();
        let mut deletion = fixture.tenant.begin().await.unwrap();
        let mut insertion = fixture.tenant.begin().await.unwrap();
        for transaction in [&mut deletion, &mut insertion] {
            sqlx::raw_sql(&format!(
                "SET TRANSACTION ISOLATION LEVEL {isolation}; SELECT count(*) FROM union_owners_a"
            ))
            .execute(&mut **transaction)
            .await
            .unwrap();
        }
        sqlx::raw_sql("DELETE FROM union_owners_a")
            .execute(&mut *deletion)
            .await
            .unwrap();
        let inserting = async {
            sqlx::raw_sql("INSERT INTO union_owners_b VALUES('shared-id',NULL)")
                .execute(&mut *insertion)
                .await?;
            insertion.commit().await
        };
        let (deleted, inserted) = tokio::join!(deletion.commit(), inserting);
        // An existing link prevents the deletion from committing before its
        // replacement. PostgreSQL may reject the competing inserter as well.
        assert_integrity_error(deleted.unwrap_err());
        if let Err(error) = inserted {
            assert_integrity_error(error);
        }
        let (owners,token_owners,orphans):(i64,i64,i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM union_owners_a WHERE \"deletedAt\" IS NULL)+(SELECT count(*) FROM union_owners_b WHERE \"deletedAt\" IS NULL), (SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='shared-id' AND state='live'), (SELECT count(*) FROM gonvex_reference_violations())")
            .fetch_one(&fixture.tenant).await.unwrap();
        assert_eq!(owners, token_owners, "{isolation}");
        assert_eq!(orphans, 0);
        sqlx::raw_sql(
            "DELETE FROM union_links; DELETE FROM union_owners_a; DELETE FROM union_owners_b",
        )
        .execute(&fixture.tenant)
        .await
        .unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
async fn tenant_migration_rejects_unclassified_storage_before_committing() {
    let Some(fixture) =
        Fixture::with_migration(include_str!("reference_integrity/aliases.sql")).await
    else {
        return;
    };
    let migration = SqlMigration::new(
        "0002_unclassified.sql".to_owned(),
        MigrationScope::Tenant,
        false,
        "ALTER TABLE \"aliasLinks\" ADD COLUMN arbitrary_owner text".to_owned(),
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
            .contains("unclassified reference candidate aliasLinks.arbitrary_owner"),
        "{error}"
    );
    let columns:i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='aliasLinks' AND column_name='arbitrary_owner'")
        .fetch_one(&fixture.tenant).await.unwrap();
    assert_eq!(
        columns, 0,
        "failed migration committed an unclassified column"
    );
    let recorded: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM gonvex_migrations WHERE name='0002_unclassified.sql'",
    )
    .fetch_one(&fixture.tenant)
    .await
    .unwrap();
    assert_eq!(recorded, 0, "failed migration was recorded");
    fixture.close().await;
}

#[tokio::test]
async fn concurrent_nonunique_alias_owners_cannot_remove_the_last_live_selector() {
    let Some(fixture) =
        Fixture::with_migration(include_str!("reference_integrity/union.sql")).await
    else {
        return;
    };
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        for soft in [false, true] {
            sqlx::raw_sql("INSERT INTO union_owners_a VALUES('first','slug',NULL),('second','slug',NULL); INSERT INTO union_links VALUES('link',NULL,'slug')")
                .execute(&fixture.tenant).await.unwrap();
            let mut first = fixture.tenant.begin().await.unwrap();
            let mut second = fixture.tenant.begin().await.unwrap();
            for transaction in [&mut first, &mut second] {
                sqlx::raw_sql(&format!("SET TRANSACTION ISOLATION LEVEL {isolation}; SELECT count(*) FROM union_owners_a"))
                    .execute(&mut **transaction).await.unwrap();
            }
            let action = if soft {
                "UPDATE union_owners_a SET \"deletedAt\"=1 WHERE _id="
            } else {
                "DELETE FROM union_owners_a WHERE _id="
            };
            sqlx::raw_sql(&format!("{action}'first'"))
                .execute(&mut *first)
                .await
                .unwrap();
            let removing_second = async {
                sqlx::raw_sql(&format!("{action}'second'"))
                    .execute(&mut *second)
                    .await?;
                second.commit().await
            };
            let (first_result, second_result) = tokio::join!(first.commit(), removing_second);
            assert_race_integrity(first_result, second_result);
            let (owners,token_owners,orphans):(i64,i64,i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM union_owners_a WHERE \"deletedAt\" IS NULL), (SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='shared_code' AND row_id='slug' AND state='live'), (SELECT count(*) FROM gonvex_reference_violations())")
                .fetch_one(&fixture.tenant).await.unwrap();
            assert!(owners > 0);
            assert_eq!(owners, token_owners, "{isolation}, soft={soft}");
            assert_eq!(orphans, 0);
            sqlx::raw_sql("DELETE FROM union_links; DELETE FROM union_owners_a")
                .execute(&fixture.tenant)
                .await
                .unwrap();
        }
    }
    fixture.close().await;
}

#[tokio::test]
async fn configuration_selected_domains_guard_values_and_native_races() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    sqlx::raw_sql(include_str!("reference_integrity/bindings.sql"))
        .execute(&fixture.tenant)
        .await
        .unwrap();
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        for configuration_first in [true, false] {
            sqlx::raw_sql(
                "INSERT INTO fields VALUES('race-config','{\"source\":\"manual\"}','Race')",
            )
            .execute(&fixture.tenant)
            .await
            .unwrap();
            let mut link = fixture.tenant.begin().await.unwrap();
            let mut configure = fixture.tenant.begin().await.unwrap();
            for transaction in [&mut link, &mut configure] {
                sqlx::raw_sql(&format!(
                    "SET TRANSACTION ISOLATION LEVEL {isolation}; SELECT count(*) FROM fields"
                ))
                .execute(&mut **transaction)
                .await
                .unwrap();
            }
            let add_link = "INSERT INTO field_values(_id,\"fieldId\",value) VALUES('race-value','race-config','\"free text\"')";
            let change = "UPDATE fields SET options='{\"source\":\"entity\",\"entity\":\"teams\"}' WHERE _id='race-config'";
            if configuration_first {
                sqlx::raw_sql(change)
                    .execute(&mut *configure)
                    .await
                    .unwrap();
                sqlx::raw_sql(add_link).execute(&mut *link).await.unwrap();
                let (a, b) = tokio::join!(link.commit(), configure.commit());
                assert_race_integrity(a, b);
            } else {
                sqlx::raw_sql(add_link).execute(&mut *link).await.unwrap();
                sqlx::raw_sql("SET CONSTRAINTS gonvex_reference_target IMMEDIATE")
                    .execute(&mut *link)
                    .await
                    .unwrap();
                let changing = async {
                    sqlx::raw_sql(change).execute(&mut *configure).await?;
                    configure.commit().await
                };
                let (a, b) = tokio::join!(link.commit(), changing);
                assert_race_integrity(a, b);
            }
            let orphans: i64 =
                sqlx::query_scalar("SELECT count(*) FROM gonvex_reference_violations()")
                    .fetch_one(&fixture.tenant)
                    .await
                    .unwrap();
            assert_eq!(
                orphans, 0,
                "{isolation}, configuration_first={configuration_first}"
            );
            sqlx::raw_sql("DELETE FROM field_values WHERE _id='race-value'; DELETE FROM fields WHERE _id='race-config'")
                .execute(&fixture.tenant).await.unwrap();
        }
    }
    fixture.close().await;
}
