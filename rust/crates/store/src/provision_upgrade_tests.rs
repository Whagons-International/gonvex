//! Upgrade from the actual pre-guard SQL and its matching bootstrap receipts.

use super::*;
use sqlx::postgres::PgPoolOptions;

const OLD_INFRASTRUCTURE: &str = include_str!("testdata/sync_upgrade_8bae/infrastructure.sql");
const OLD_TASKS: &str = include_str!("testdata/sync_upgrade_8bae/tasks.sql");

#[derive(Debug, PartialEq, Eq)]
struct InstalledState {
    finalizer: String,
    stage: String,
    receipts: Vec<(String, String)>,
}

type DdlAudit = Vec<(String, String)>;

struct UpgradeObservations {
    old: InstalledState,
    failed: InstalledState,
    upgraded: InstalledState,
    warm: InstalledState,
    invalid_error: Option<String>,
    failed_ddl: DdlAudit,
    upgrade_ddl: DdlAudit,
    warm_ddl: DdlAudit,
    session_revision: String,
    complete_feed: bool,
}

#[tokio::test]
async fn sync_bootstrap_upgrades_real_8bae_receipts_atomically() {
    let Ok(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL") else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping sync upgrade test");
        return;
    };
    let schema = format!("sync_upgrade_{}", uuid::Uuid::new_v4().simple());
    let mut url = Url::parse(&base_url).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    // One private session keeps the deliberately preset session GUC on the
    // same connection as the captured write. No shared pool state is changed.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(url.as_str())
        .await
        .unwrap();

    let observed = observe_upgrade(&pool, &schema).await;
    // Keep assertion failures and SQL errors from leaving a database-wide
    // event trigger installed. Run all cleanup before inspecting observations.
    let drop_trigger = sqlx::query(&format!("DROP EVENT TRIGGER IF EXISTS {schema}"))
        .execute(&admin)
        .await;
    pool.close().await;
    let drop_schema = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    drop_trigger.unwrap();
    drop_schema.unwrap();
    let observed = observed.unwrap();

    // Independent pins from testdata/sync_upgrade_8bae/manifest.json. Receipt
    // insertion still hashes the installed SQL through record_bootstrap.
    assert_eq!(
        bootstrap_digest(OLD_INFRASTRUCTURE),
        "fb03d86b9151a6196ca744cfeb6b167c2a86d292720d7f8add55eb93c93baaf5"
    );
    assert_eq!(
        bootstrap_digest(OLD_TASKS),
        "6b90237008ace136f4f755b22b6460a98afb7b4a1d77e5fec0191af3a716bb35"
    );
    assert_eq!(
        observed.old.receipts,
        vec![
            (
                "infrastructure".to_owned(),
                bootstrap_digest(OLD_INFRASTRUCTURE)
            ),
            ("table:tasks".to_owned(), bootstrap_digest(OLD_TASKS)),
        ],
        "the starting receipts must hash the exact installed baseline SQL"
    );
    assert!(observed
        .old
        .finalizer
        .contains("revision_text:=current_setting('gonvex.sync_revision',true);"));
    assert!(observed
        .old
        .finalizer
        .contains("PERFORM set_config('gonvex.sync_revision',next_revision::text,true);"));
    assert!(!observed.old.stage.contains("_gonvex_sync_transactions"));

    let invalid_error = observed
        .invalid_error
        .expect("z_invalid must reject the upgrade");
    assert!(invalid_error.contains("z_invalid"), "{invalid_error}");
    assert!(invalid_error.contains("no primary key"), "{invalid_error}");
    assert_eq!(
        observed.failed, observed.old,
        "both function definitions and both receipts must survive the failed upgrade"
    );
    assert!(
        observed.failed_ddl.is_empty(),
        "transactional DDL audit rows must roll back: {:?}",
        observed.failed_ddl
    );

    assert_ne!(observed.upgraded.finalizer, observed.old.finalizer);
    assert_ne!(observed.upgraded.stage, observed.old.stage);
    let provenance_guard =
        "FROM _gonvex_sync_transactions WHERE transaction_id=txid_current()::bigint";
    assert!(observed.upgraded.finalizer.contains(provenance_guard));
    assert!(!observed.upgraded.finalizer.contains("gonvex.sync_revision"));
    assert!(observed.upgraded.stage.contains(provenance_guard));
    assert!(observed
        .upgraded
        .stage
        .contains("tracked writes are not allowed after sync finalization"));
    assert_eq!(observed.upgraded.receipts.len(), 2);
    assert_eq!(
        observed.upgraded.receipts[0],
        (
            "infrastructure".to_owned(),
            bootstrap_digest(SYNC_INFRASTRUCTURE_SQL)
        )
    );
    assert_ne!(observed.upgraded.receipts[0].1, observed.old.receipts[0].1);
    assert_eq!(observed.upgraded.receipts[1].0, "table:tasks");
    assert_ne!(observed.upgraded.receipts[1].1, observed.old.receipts[1].1);
    for function in [
        "gonvex_sync_finalize_transaction()",
        "gonvex_sync_tasks_stage()",
    ] {
        assert!(observed
            .upgrade_ddl
            .iter()
            .any(|(tag, identity)| tag == "CREATE FUNCTION" && identity.contains(function)));
    }
    assert_eq!(
        observed
            .upgrade_ddl
            .iter()
            .filter(|(tag, _)| tag == "CREATE TRIGGER")
            .count(),
        2
    );
    // The real installer rebuilds its current table statement on the warm
    // pass. Zero DDL proves the new table receipt matches that exact source
    // hash, without copying the current Rust SQL template into this test.
    assert_eq!(observed.warm, observed.upgraded);
    assert!(
        observed.warm_ddl.is_empty(),
        "a warm upgrade must execute zero DDL: {:?}",
        observed.warm_ddl
    );
    assert_eq!(observed.session_revision, "1");
    assert!(
        observed.complete_feed,
        "the upgraded functions must capture the complete transaction despite a preset session revision"
    );
}

async fn observe_upgrade(
    pool: &sqlx::PgPool,
    schema: &str,
) -> Result<UpgradeObservations, DatabaseError> {
    // Same event-trigger privilege and schema filter as the existing native
    // sync_bootstrap_caches_repairs_and_commits_atomically regression.
    execute_script_pool(
        pool,
        &format!(
            r#"
CREATE TABLE gonvex_ddl_audit (tag text, identity text);
CREATE FUNCTION {schema}.audit_ddl() RETURNS event_trigger AS $$
BEGIN
  INSERT INTO {schema}.gonvex_ddl_audit
  SELECT command_tag, object_identity FROM pg_event_trigger_ddl_commands() AS command
  WHERE schema_name='{schema}' OR (
    command.classid='pg_trigger'::regclass AND EXISTS (
      SELECT FROM pg_trigger AS trigger
      JOIN pg_class AS relation ON relation.oid=trigger.tgrelid
      JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
      WHERE trigger.oid=command.objid AND namespace.nspname='{schema}'
    )
  );
END; $$ LANGUAGE plpgsql;
CREATE EVENT TRIGGER {schema} ON ddl_command_end EXECUTE FUNCTION {schema}.audit_ddl();
CREATE TABLE tasks (id text PRIMARY KEY, title text);
CREATE TABLE _gonvex_sync_bootstrap (name text PRIMARY KEY, digest text NOT NULL);
"#
        ),
    )
    .await?;
    // Install the actual old function bodies and trigger DDL, then let the
    // production receipt writer hash those same bytes. No historical writes
    // are made, since this fix does not backfill old unfinalized feed rows.
    let mut tx = pool.begin().await?;
    execute_script_transaction(&mut tx, OLD_INFRASTRUCTURE).await?;
    execute_script_transaction(&mut tx, OLD_TASKS).await?;
    record_bootstrap(&mut tx, "infrastructure", OLD_INFRASTRUCTURE).await?;
    record_bootstrap(&mut tx, "table:tasks", OLD_TASKS).await?;
    tx.commit().await?;
    let old = installed_state(pool).await?;

    // The installer upgrades infrastructure first and visits tasks before
    // z_invalid in its ordered catalog traversal, then fails for the missing PK.
    sqlx::query("CREATE TABLE z_invalid (value text)")
        .execute(pool)
        .await?;
    clear_upgrade_audit(pool).await?;
    let invalid_error = install_change_feed(pool.clone())
        .await
        .err()
        .map(|error| error.to_string());
    let failed = installed_state(pool).await?;
    let failed_ddl = upgrade_audit(pool).await?;

    sqlx::query("DROP TABLE z_invalid").execute(pool).await?;
    clear_upgrade_audit(pool).await?;
    install_change_feed(pool.clone()).await?;
    let upgraded = installed_state(pool).await?;
    let upgrade_ddl = upgrade_audit(pool).await?;

    clear_upgrade_audit(pool).await?;
    install_change_feed(pool.clone()).await?;
    let warm = installed_state(pool).await?;
    let warm_ddl = upgrade_audit(pool).await?;

    let mut tx = pool.begin().await?;
    let session_revision: String =
        sqlx::query_scalar("SELECT set_config('gonvex.sync_revision','1',false)")
            .fetch_one(&mut *tx)
            .await?;
    sqlx::query("SELECT set_config('gonvex.command_id','upgrade-command',true)")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT set_config('gonvex.invocation_channel','mutation',true)")
        .execute(&mut *tx)
        .await?;
    let transaction_id: i64 = sqlx::query_scalar("SELECT txid_current()::bigint")
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO tasks(id,title) VALUES ('captured-a','captured'),('captured-b','captured')",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    let complete_feed: bool = sqlx::query_scalar(
        r#"SELECT
  (SELECT count(*) FROM tasks)=2
  AND (SELECT revision=1 FROM _gonvex_sync_clock WHERE singleton)
  AND (SELECT count(*) FROM _gonvex_sync_transactions)=1
  AND EXISTS (
    SELECT FROM _gonvex_sync_transactions WHERE transaction_id=$1 AND revision=1
      AND root_command_id='upgrade-command' AND origin_command_id='upgrade-command'
      AND root_invocation_channel='mutation' AND invocation_channel='mutation'
  )
  AND (SELECT count(*) FROM _gonvex_sync_changes)=2
  AND (SELECT count(*) FROM _gonvex_sync_changes
    WHERE transaction_id=$1 AND revision=1 AND ordinal IN (1,2)
      AND command_id='upgrade-command' AND table_name='tasks' AND operation='insert'
      AND row_id IN ('captured-a','captured-b') AND old_value IS NULL
      AND new_value=jsonb_build_object('id',row_id,'title','captured')
      AND changed_columns @> ARRAY['id','title']::text[] AND cardinality(changed_columns)=2)=2
  AND (SELECT count(DISTINCT ordinal) FROM _gonvex_sync_changes)=2
  AND (SELECT count(DISTINCT row_id) FROM _gonvex_sync_changes)=2"#,
    )
    .bind(transaction_id)
    .fetch_one(pool)
    .await?;
    Ok(UpgradeObservations {
        old,
        failed,
        upgraded,
        warm,
        invalid_error,
        failed_ddl,
        upgrade_ddl,
        warm_ddl,
        session_revision,
        complete_feed,
    })
}

async fn installed_state(pool: &sqlx::PgPool) -> Result<InstalledState, DatabaseError> {
    let definition =
        "SELECT pg_get_functiondef(to_regprocedure(format('%I.%I()',current_schema(),$1)))";
    let finalizer = sqlx::query_scalar(definition)
        .bind("gonvex_sync_finalize_transaction")
        .fetch_one(pool)
        .await?;
    let stage = sqlx::query_scalar(definition)
        .bind("gonvex_sync_tasks_stage")
        .fetch_one(pool)
        .await?;
    let receipts = sqlx::query_as("SELECT name,digest FROM _gonvex_sync_bootstrap ORDER BY name")
        .fetch_all(pool)
        .await?;
    Ok(InstalledState {
        finalizer,
        stage,
        receipts,
    })
}

async fn clear_upgrade_audit(pool: &sqlx::PgPool) -> Result<(), DatabaseError> {
    sqlx::query("DELETE FROM gonvex_ddl_audit")
        .execute(pool)
        .await?;
    Ok(())
}

async fn upgrade_audit(pool: &sqlx::PgPool) -> Result<DdlAudit, DatabaseError> {
    Ok(
        sqlx::query_as("SELECT tag,identity FROM gonvex_ddl_audit ORDER BY tag,identity")
            .fetch_all(pool)
            .await?,
    )
}
