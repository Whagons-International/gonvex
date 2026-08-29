use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

fn scoped_url(base: &str, schema: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-csearch_path%3D{schema}")
}

#[tokio::test]
async fn rust_control_schema_upgrades_the_published_go_layout_in_place() {
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping schema upgrade test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("gonvex_rust_upgrade_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
        .execute(&admin)
        .await
        .unwrap();
    let database_url = scoped_url(&base_url, &schema);
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"
        CREATE TABLE gonvex_runtime_projects (
          id text PRIMARY KEY,name text NOT NULL,environment text NOT NULL DEFAULT 'development',
          database_name text NOT NULL DEFAULT '',database_url text NOT NULL DEFAULT '',
          storage_bucket text NOT NULL DEFAULT '',status text NOT NULL DEFAULT 'active',
          description text NOT NULL DEFAULT '',project_key text NOT NULL DEFAULT '',
          provisioned boolean NOT NULL DEFAULT true,runtime_created boolean NOT NULL DEFAULT true,
          created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now()
        );
        INSERT INTO gonvex_runtime_projects(id,name) VALUES('project','Project');
        CREATE TABLE gonvex_runtime_tenants (
          relationship_id text PRIMARY KEY,project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
          tenant_id text NOT NULL,name text NOT NULL,database_alias text NOT NULL DEFAULT '',
          database_name text NOT NULL DEFAULT '',database_url text NOT NULL DEFAULT '',
          domain text NOT NULL DEFAULT '',status text NOT NULL DEFAULT 'active',
          description text NOT NULL DEFAULT '',provisioned boolean NOT NULL DEFAULT false,
          runtime_created boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),UNIQUE(project_id,tenant_id)
        );
        INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name)
        VALUES('relationship','project','tenant','Tenant');
        CREATE TABLE gonvex_runtime_manifests (
          project_id text PRIMARY KEY,manifest jsonb NOT NULL,bundle_hash text NOT NULL DEFAULT '',
          created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now()
        );
        INSERT INTO gonvex_runtime_manifests(project_id,manifest,bundle_hash)
        VALUES('project','{}','artifact-old');
        CREATE TABLE gonvex_runtime_mutation_logs (
          id bigserial PRIMARY KEY,project_id text NOT NULL DEFAULT '',kind text NOT NULL,
          entry jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT gonvex_runtime_mutation_logs_kind_check CHECK(kind IN('mutation','internalMutation'))
        );
        INSERT INTO gonvex_runtime_mutation_logs(project_id,kind,entry)
        VALUES('project','mutation','{"kind":"mutation","path":"tasks.start"}');
        CREATE TABLE gonvex_auth_providers (
          project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
          provider text NOT NULL,enabled boolean NOT NULL DEFAULT true,
          signup_mode text NOT NULL DEFAULT 'personal',created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,provider)
        );
        CREATE TABLE gonvex_auth_users (
          id text PRIMARY KEY,project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
          provider text NOT NULL,provider_subject text NOT NULL,email text NOT NULL DEFAULT '',
          email_verified boolean NOT NULL DEFAULT false,name text NOT NULL DEFAULT '',picture text NOT NULL DEFAULT '',
          created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
          UNIQUE(project_id,provider,provider_subject),UNIQUE(project_id,id)
        );
        CREATE TABLE gonvex_auth_sessions (
          token_hash text PRIMARY KEY,project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
          user_id text NOT NULL REFERENCES gonvex_auth_users(id) ON DELETE CASCADE,
          family_id text NOT NULL DEFAULT '',expires_at timestamptz NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        CREATE TABLE gonvex_auth_memberships (
          project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
          user_id text NOT NULL,tenant_id text NOT NULL,role text NOT NULL DEFAULT 'member',
          permissions jsonb NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,user_id,tenant_id),
          FOREIGN KEY(project_id,user_id) REFERENCES gonvex_auth_users(project_id,id) ON DELETE CASCADE,
          FOREIGN KEY(project_id,tenant_id) REFERENCES gonvex_runtime_tenants(project_id,tenant_id) ON DELETE CASCADE
        );
        CREATE TABLE gonvex_auth_membership_invitations (
          project_id text NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
          tenant_id text NOT NULL,email text NOT NULL,role text NOT NULL DEFAULT 'member',
          permissions jsonb NOT NULL DEFAULT '{}',invited_by text NOT NULL DEFAULT '',
          expires_at timestamptz NOT NULL DEFAULT(now()+interval '7 days'),
          created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY(project_id,tenant_id,email),
          FOREIGN KEY(project_id,tenant_id) REFERENCES gonvex_runtime_tenants(project_id,tenant_id) ON DELETE CASCADE
        );
        "#,
    )
    .execute(&fixture)
    .await
    .unwrap();

    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&database_url, pools.clone(), BTreeMap::new())
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
    assert_eq!(
        log.get::<sqlx::types::Json<serde_json::Value>, _>("entry")
            .0["kind"],
        "reducer"
    );
    let columns = sqlx::query_scalar::<_, String>(
        r#"SELECT column_name FROM information_schema.columns
           WHERE table_schema=current_schema()
             AND ((table_name='gonvex_runtime_tenants' AND column_name IN('timezone','profile','seat_limit','deleted_at'))
               OR (table_name='gonvex_auth_membership_invitations' AND column_name IN('id','token_hash','team_ids','handoff_state')))
           ORDER BY table_name,column_name"#,
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    assert_eq!(columns.len(), 8, "{columns:?}");
    let session_columns = sqlx::query_scalar::<_, String>(
        r#"SELECT column_name FROM information_schema.columns
           WHERE table_schema=current_schema() AND table_name='gonvex_auth_sessions'
             AND column_name IN('account_id','user_id') ORDER BY column_name"#,
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    assert_eq!(session_columns, vec!["account_id"]);
    let legacy_tables: i64 = sqlx::query_scalar(
        r#"SELECT count(*) FROM pg_class
           WHERE relnamespace=current_schema()::regnamespace
             AND relname IN('gonvex_auth_users','gonvex_auth_memberships')"#,
    )
    .fetch_one(&fixture)
    .await
    .unwrap();
    assert_eq!(legacy_tables, 0);

    fixture.close().await;
    pools.close().await;
    sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn rust_control_schema_refuses_to_discard_populated_legacy_identities() {
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping schema upgrade test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("gonvex_rust_identity_guard_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
        .execute(&admin)
        .await
        .unwrap();
    let database_url = scoped_url(&base_url, &schema);
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"
        CREATE TABLE gonvex_runtime_projects(id text PRIMARY KEY);
        INSERT INTO gonvex_runtime_projects(id) VALUES('project');
        CREATE TABLE gonvex_auth_users(
          id text PRIMARY KEY,project_id text NOT NULL REFERENCES gonvex_runtime_projects(id),
          provider text NOT NULL,provider_subject text NOT NULL
        );
        INSERT INTO gonvex_auth_users(id,project_id,provider,provider_subject)
        VALUES('legacy-user','project','google','subject');
        "#,
    )
    .execute(&fixture)
    .await
    .unwrap();

    let pools = PoolRegistry::new(PoolLimits::default());
    let error = ControlPlane::connect(&database_url, pools.clone(), BTreeMap::new())
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

    fixture.close().await;
    pools.close().await;
    sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
