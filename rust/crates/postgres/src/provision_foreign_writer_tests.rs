//! A matching bootstrap receipt must not hide sync functions that another writer replaced afterwards.
//!
//! Staging 2026-10-30: an older runtime build ran during a rollback and rewrote every table's stage function with
//! its own generator (one `jsonb_build_object` for all columns, no finalization guard). The current runtime came
//! back, found its receipts and the objects present, and skipped the repair. Every write to a table wider than 50
//! columns then failed with 54023 "cannot pass more than 100 arguments to a function".

use super::*;
use sqlx::postgres::PgPoolOptions;

const OLD_INFRASTRUCTURE: &str = include_str!("testdata/sync_upgrade_8bae/infrastructure.sql");
const WIDE_COLUMNS: usize = 53;

#[test]
fn function_bodies_are_read_verbatim_from_install_scripts() {
    let infrastructure = function_bodies(SYNC_INFRASTRUCTURE_SQL);
    assert_eq!(infrastructure.len(), 1);
    assert_eq!(infrastructure[0].0, "gonvex_sync_finalize_transaction");
    assert!(infrastructure[0]
        .1
        .starts_with("\nDECLARE next_revision bigint;"));
    assert!(infrastructure[0].1.ends_with("RETURN NULL;\nEND; "));
    let old = function_bodies(OLD_INFRASTRUCTURE);
    assert_eq!(old.len(), 1);
    assert_ne!(
        old[0].1, infrastructure[0].1,
        "the fixture is a genuinely different finalizer"
    );
    let quoted = "CREATE OR REPLACE FUNCTION \"gonvex_sync_t_stage\"() RETURNS trigger AS $$\nBEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;\nDROP TRIGGER IF EXISTS x ON t;";
    assert_eq!(
        function_bodies(quoted),
        vec![("gonvex_sync_t_stage", "\nBEGIN RETURN NULL; END; ")]
    );
    assert!(stage_body_digest("SELECT 1").is_err());
}

/// The single-object projection of the older generator, exactly as it was installed on staging.
fn unchunked_stage(columns: &[String]) -> String {
    let projection = |alias: &str| {
        let pairs = columns
            .iter()
            .map(|c| format!("'{c}',{alias}.\"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!("jsonb_build_object({pairs})")
    };
    format!(
        r#"CREATE OR REPLACE FUNCTION gonvex_sync_wide_stage() RETURNS trigger AS $$
DECLARE old_data jsonb; new_data jsonb; changed_columns text[]; row_key text;
BEGIN
  IF TG_OP='INSERT' THEN
    old_data:=NULL; new_data:={new};
    row_key:=NEW."id"::text; changed_columns:=ARRAY(SELECT jsonb_object_keys(new_data));
  ELSIF TG_OP='DELETE' THEN
    old_data:={old}; new_data:=NULL;
    row_key:=OLD."id"::text; changed_columns:=ARRAY(SELECT jsonb_object_keys(old_data));
  ELSE
    old_data:={old}; new_data:={new};
    row_key:=NEW."id"::text;
    changed_columns:=ARRAY(SELECT key FROM jsonb_object_keys(old_data||new_data) changed(key)
                           WHERE old_data->key IS DISTINCT FROM new_data->key ORDER BY key);
  END IF;
  INSERT INTO _gonvex_sync_changes
    (transaction_id,table_name,row_id,operation,old_value,new_value,changed_columns)
  VALUES(txid_current()::bigint,'wide',row_key,lower(TG_OP),old_data,new_data,changed_columns);
  RETURN NULL;
END; $$ LANGUAGE plpgsql"#,
        new = projection("NEW"),
        old = projection("OLD"),
    )
}

#[tokio::test]
async fn sync_bootstrap_repairs_functions_replaced_behind_matching_receipts() {
    let Ok(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL") else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping foreign-writer repair test");
        return;
    };
    let schema = format!("sync_foreign_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let mut url = Url::parse(&base_url).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(url.as_str())
        .await
        .unwrap();
    let result = async {
        let columns = std::iter::once("id".to_owned())
            .chain((1..WIDE_COLUMNS).map(|i| format!("c{i:02}")))
            .collect::<Vec<_>>();
        let definition = columns.iter().skip(1).map(|c| format!("\"{c}\" text")).collect::<Vec<_>>().join(",");
        sqlx::query(&format!("CREATE TABLE wide (id text PRIMARY KEY, {definition})")).execute(&pool).await?;
        install_change_feed(pool.clone()).await?;
        let receipts = || sqlx::query_as::<_, (String, String)>("SELECT name,digest FROM _gonvex_sync_bootstrap ORDER BY name").fetch_all(&pool);
        let installed = receipts().await?;
        sqlx::query("INSERT INTO wide(id,c01) VALUES ('before','x')").execute(&pool).await?;

        // The older writer replaces the functions and leaves this runtime's receipts untouched.
        sqlx::query(&unchunked_stage(&columns)).execute(&pool).await?;
        let (_, old_finalizer) = function_bodies(OLD_INFRASTRUCTURE)[0];
        sqlx::query(&format!("CREATE OR REPLACE FUNCTION gonvex_sync_finalize_transaction() RETURNS trigger AS $${old_finalizer}$$ LANGUAGE plpgsql"))
            .execute(&pool).await?;
        assert_eq!(receipts().await?, installed);
        let broken = sqlx::query("INSERT INTO wide(id,c01) VALUES ('broken','x')").execute(&pool).await.unwrap_err();
        assert_eq!(broken.as_database_error().and_then(|e| e.code()).as_deref(), Some("54023"), "{broken}");

        install_change_feed(pool.clone()).await?;
        assert_eq!(receipts().await?, installed, "the repair reinstalls the same definitions");
        sqlx::query("INSERT INTO wide(id,c01) VALUES ('repaired','x')").execute(&pool).await?;
        let recorded: Vec<(String, Option<i64>)> = sqlx::query_as(
            "SELECT row_id,revision FROM _gonvex_sync_changes WHERE table_name='wide' ORDER BY event_id",
        ).fetch_all(&pool).await?;
        assert_eq!(recorded.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["before", "repaired"]);
        assert!(recorded.iter().all(|(_, revision)| revision.is_some()), "the current finalizer assigns revisions: {recorded:?}");
        for (name, body) in function_bodies(SYNC_INFRASTRUCTURE_SQL) {
            let installed: String = sqlx::query_scalar("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(format('%I.%I()', current_schema(), $1))")
                .bind(name).fetch_one(&pool).await?;
            assert_eq!(installed, body);
        }
        // A warm boot with matching bodies still keeps everything as is.
        install_change_feed(pool.clone()).await?;
        sqlx::query("INSERT INTO wide(id,c01) VALUES ('warm','x')").execute(&pool).await?;
        Ok::<(), DatabaseError>(())
    }
    .await;
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    result.unwrap();
}
