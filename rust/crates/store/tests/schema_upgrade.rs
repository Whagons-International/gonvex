use std::collections::BTreeMap;

use gonvex_store::{ControlPlane, DataDir, PoolLimits, PoolRegistry};
use sqlx::Row;

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

async fn columns(pool: &sqlx::SqlitePool, table: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT name FROM pragma_table_info($1) ORDER BY name")
        .bind(table)
        .fetch_all(pool)
        .await
        .unwrap()
}

/// An older Control Plane file (renamed objects, missing columns, legacy
/// empty identity tables) upgrades in place, and a second start is a no-op.
#[tokio::test]
async fn control_schema_upgrades_an_older_layout_in_place() {
    let dir = TestDir::new("gonvex-upgrade");
    let url = DataDir::new(&dir.0).control_url();
    let pools = PoolRegistry::new(PoolLimits::default());
    let fixture = pools.pool(&url).await.unwrap();
    sqlx::raw_sql(
        r#"
        CREATE TABLE gonvex_runtime_projects (
          id TEXT PRIMARY KEY,name TEXT NOT NULL,environment TEXT NOT NULL DEFAULT 'development',
          database_name TEXT NOT NULL DEFAULT '',database_url TEXT NOT NULL DEFAULT '',
          storage_bucket TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'active',
          description TEXT NOT NULL DEFAULT '',project_key TEXT NOT NULL DEFAULT '',
          provisioned BOOLEAN NOT NULL DEFAULT 1,runtime_created BOOLEAN NOT NULL DEFAULT 1,
          created_at INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO gonvex_runtime_projects(id,name) VALUES('project','Project');
        CREATE TABLE gonvex_runtime_tenants (
          relationship_id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
          tenant_id TEXT NOT NULL,name TEXT NOT NULL,database_alias TEXT NOT NULL DEFAULT '',
          database_name TEXT NOT NULL DEFAULT '',database_url TEXT NOT NULL DEFAULT '',
          domain TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'active',
          description TEXT NOT NULL DEFAULT '',provisioned BOOLEAN NOT NULL DEFAULT 0,
          runtime_created BOOLEAN NOT NULL DEFAULT 0,created_at INTEGER NOT NULL DEFAULT 0,
          updated_at INTEGER NOT NULL DEFAULT 0,UNIQUE(project_id,tenant_id)
        );
        INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name)
        VALUES('relationship','project','tenant','Tenant');
        CREATE TABLE gonvex_runtime_manifests (
          project_id TEXT PRIMARY KEY,manifest JSON NOT NULL,bundle_hash TEXT NOT NULL DEFAULT '',
          created_at INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO gonvex_runtime_manifests(project_id,manifest,bundle_hash)
        VALUES('project','{}','artifact-old');
        CREATE TABLE gonvex_runtime_mutation_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,project_id TEXT NOT NULL DEFAULT '',kind TEXT NOT NULL,
          entry JSON NOT NULL,created_at INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO gonvex_runtime_mutation_logs(project_id,kind,entry)
        VALUES('project','mutation','{"kind":"mutation","path":"tasks.start"}');
        CREATE TABLE gonvex_scheduled_jobs (
          id TEXT PRIMARY KEY, project_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '',
          function_path TEXT NOT NULL, args JSON, run_at INTEGER NOT NULL,
          scheduled_for INTEGER NOT NULL, cron_name TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'pending', claim_sequence INTEGER NOT NULL DEFAULT 0,
          claim_token TEXT NOT NULL DEFAULT '', lease_until INTEGER,
          created_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0, completed_at INTEGER
        );
        INSERT INTO gonvex_scheduled_jobs(id,project_id,tenant_id,function_path,run_at,scheduled_for,cron_name)
        VALUES('old','project','tenant','f',1,1,'nightly'),('new','project','tenant','f',2,2,'nightly');
        CREATE TABLE gonvex_auth_users (
          id TEXT PRIMARY KEY,project_id TEXT NOT NULL,provider TEXT NOT NULL,provider_subject TEXT NOT NULL
        );
        CREATE TABLE gonvex_auth_sessions (
          token_hash TEXT PRIMARY KEY,project_id TEXT NOT NULL,user_id TEXT NOT NULL,
          family_id TEXT NOT NULL DEFAULT '',expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL DEFAULT 0
        );
        "#,
    )
    .execute(&fixture)
    .await
    .unwrap();

    let control = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    control.ping().await.unwrap();

    let manifest =
        sqlx::query("SELECT module_hash FROM gonvex_runtime_manifests WHERE project_id='project'")
            .fetch_one(&fixture)
            .await
            .unwrap();
    assert_eq!(manifest.get::<String, _>("module_hash"), "artifact-old");
    let log = sqlx::query("SELECT kind,entry FROM gonvex_runtime_reducer_logs")
        .fetch_one(&fixture)
        .await
        .unwrap();
    assert_eq!(log.get::<String, _>("kind"), "reducer");
    assert_eq!(log.get::<serde_json::Value, _>("entry")["kind"], "reducer");
    let tenant_columns = columns(&fixture, "gonvex_runtime_tenants").await;
    for column in ["timezone", "profile", "seat_limit", "deleted_at"] {
        assert!(tenant_columns.iter().any(|name| name == column), "{column}");
    }
    let project_columns = columns(&fixture, "gonvex_runtime_projects").await;
    for column in ["test_tab", "auth_mode", "owner_email", "database_mode"] {
        assert!(
            project_columns.iter().any(|name| name == column),
            "{column}"
        );
    }
    assert!(columns(&fixture, "gonvex_scheduled_jobs")
        .await
        .iter()
        .any(|name| name == "wake_key"));
    // Only the newest pending run of a cron survives the uniqueness upgrade.
    let jobs: Vec<String> = sqlx::query_scalar("SELECT id FROM gonvex_scheduled_jobs ORDER BY id")
        .fetch_all(&fixture)
        .await
        .unwrap();
    assert_eq!(jobs, ["new"]);
    assert_eq!(
        columns(&fixture, "gonvex_auth_sessions")
            .await
            .iter()
            .filter(|name| *name == "account_id" || *name == "user_id")
            .collect::<Vec<_>>(),
        ["account_id"]
    );
    let legacy_tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN('gonvex_auth_users','gonvex_auth_memberships')",
    )
    .fetch_one(&fixture)
    .await
    .unwrap();
    assert_eq!(legacy_tables, 0);
    let project: (bool, String) = sqlx::query_as(
        "SELECT test_tab, auth_mode FROM gonvex_runtime_projects WHERE id='project'",
    )
    .fetch_one(&fixture)
    .await
    .unwrap();
    assert_eq!(project, (false, "gonvex-native".to_owned()));

    // A restart on the current layout changes no schema.
    let version: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&fixture)
        .await
        .unwrap();
    ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let after: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&fixture)
        .await
        .unwrap();
    assert_eq!(version, after);
    pools.close().await;
}

#[tokio::test]
async fn control_schema_refuses_to_discard_populated_legacy_identities() {
    let dir = TestDir::new("gonvex-identity-guard");
    let url = DataDir::new(&dir.0).control_url();
    let pools = PoolRegistry::new(PoolLimits::default());
    let fixture = pools.pool(&url).await.unwrap();
    sqlx::raw_sql(
        r#"
        CREATE TABLE gonvex_runtime_projects(id TEXT PRIMARY KEY);
        INSERT INTO gonvex_runtime_projects(id) VALUES('project');
        CREATE TABLE gonvex_auth_users(
          id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id),
          provider TEXT NOT NULL,provider_subject TEXT NOT NULL
        );
        INSERT INTO gonvex_auth_users(id,project_id,provider,provider_subject)
        VALUES('legacy-user','project','google','subject');
        "#,
    )
    .execute(&fixture)
    .await
    .unwrap();

    let error = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
        .await
        .err()
        .expect("populated legacy identity tables must block automatic startup");
    assert!(
        error
            .to_string()
            .contains("gonvex-admin migrate identity-v2"),
        "{error}"
    );
    let preserved: i64 = sqlx::query_scalar("SELECT count(*) FROM gonvex_auth_users")
        .fetch_one(&fixture)
        .await
        .unwrap();
    assert_eq!(preserved, 1);
    pools.close().await;
}
