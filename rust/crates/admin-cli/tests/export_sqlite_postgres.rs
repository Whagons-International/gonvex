//! `gonvex-admin export-sqlite` against a real Postgres deployment. Runs only
//! when GONVEX_TEST_POSTGRES_URL names a disposable database: the tool reads
//! Postgres by definition, so there is no SQLite-only substitute.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use gonvex_admin::export_sqlite::{export, ExportOptions, MigrationDirs};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

fn scoped_url(base: &str, schema: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-csearch_path%3D{schema}")
}

fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(path)
}

async fn open(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
            .unwrap()
            .foreign_keys(true),
    )
    .await
    .unwrap()
}

fn options(control_url: &str, data_dir: &Path) -> ExportOptions {
    ExportOptions {
        control_plane_url: control_url.to_owned(),
        data_dir: data_dir.to_path_buf(),
        migrations: MigrationDirs {
            default: Some(fixture("export_sqlite/migrations")),
            ..MigrationDirs::default()
        },
        projects: Vec::new(),
        tenants: Vec::new(),
        force: false,
        allow_drop_columns: false,
        allow_drop_tables: false,
        keep_epoch: false,
        no_control_plane: false,
        sample_rows: 50,
        source_url_maps: Vec::new(),
        report_file: None,
    }
}

const TENANT_APP_SQL: &str = r#"
CREATE TABLE tasks (
  id uuid PRIMARY KEY, title text NOT NULL, done boolean NOT NULL DEFAULT false,
  due_at timestamptz, due_on date, meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  tags text[] NOT NULL DEFAULT '{}', price numeric(10,2), price_text numeric,
  score double precision, parent_id uuid
);
CREATE TABLE labels (
  id bigserial PRIMARY KEY, task_id uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  name text NOT NULL
);
CREATE TABLE IF NOT EXISTS _gonvex_reducer_idempotency (
  subject text NOT NULL DEFAULT '', idempotency_key text NOT NULL, path text NOT NULL,
  result jsonb, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (subject, idempotency_key)
);
"#;

#[tokio::test]
async fn export_sqlite_moves_a_postgres_deployment() {
    let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping export-sqlite test");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let control_schema = format!("gonvex_export_control_{nonce}");
    let project_schema = format!("gonvex_export_project_{nonce}");
    let tenant_schema = format!("gonvex_export_tenant_{nonce}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    for schema in [&control_schema, &project_schema, &tenant_schema] {
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .unwrap();
    }
    let control_url = scoped_url(&base_url, &control_schema);
    let project_url = scoped_url(&base_url, &project_schema);
    let tenant_url = scoped_url(&base_url, &tenant_schema);

    // Control Plane, with the pre-port Postgres schema.
    let control = PgPoolOptions::new()
        .max_connections(1)
        .connect(&control_url)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("fixtures/postgres/control_schema.sql"))
        .execute(&control)
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"CREATE TABLE app_settings (key text PRIMARY KEY, value jsonb NOT NULL, updated_at timestamptz NOT NULL);
           INSERT INTO app_settings VALUES ('theme', '{"dark": true, "n": 12345678901234567}', '2024-05-06T07:08:09.123456Z');
           INSERT INTO accounts (id, email, name, disabled_at, created_at)
             VALUES ('acct-1', 'ada@example.com', 'Ada', NULL, '2024-01-02T03:04:05.678901Z');
           INSERT INTO account_identities (account_id, provider, subject, email, verified_email)
             VALUES ('acct-1', 'google', 'sub-1', 'ada@example.com', true);"#,
    )
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_runtime_projects (id, name, database_name, database_mode, database_url, project_key)
           VALUES ('proj', 'Project', 'gonvex_proj', 'multiTenant', $1, 'gvx_key')"#,
    )
    .bind(&project_url)
    .execute(&control)
    .await
    .unwrap();
    for (tenant, url, deleted) in [
        ("acme", tenant_url.as_str(), false),
        ("gone", "postgres://gonvex:secret@db/gone", true),
    ] {
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_tenants
               (relationship_id, project_id, tenant_id, name, database_name, database_url, profile, deleted_at)
               VALUES ($1, 'proj', $1, $1, 'tenant_' || $1, $2, '{"plan": "pro"}', CASE WHEN $3 THEN now() END)"#,
        )
        .bind(tenant)
        .bind(url)
        .bind(deleted)
        .execute(&control)
        .await
        .unwrap();
    }
    sqlx::query(
        r#"INSERT INTO gonvex_auth_providers (project_id, provider, client_secret_encrypted)
           VALUES ('proj', 'google', '\x00ff10'::bytea)"#,
    )
    .execute(&control)
    .await
    .unwrap();

    // Project and tenant databases: the pre-port runtime schema plus the app.
    for url in [&project_url, &tenant_url] {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("fixtures/postgres/tenant_identity.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("fixtures/postgres/sync_infrastructure.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(TENANT_APP_SQL).execute(&pool).await.unwrap();
        pool.close().await;
    }
    let tenant = PgPoolOptions::new()
        .max_connections(1)
        .connect(&tenant_url)
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"INSERT INTO tasks (id, title, done, due_at, due_on, meta, tags, price, price_text, score, parent_id)
           VALUES
             ('11111111-1111-4111-8111-111111111111', 'root', true, '2024-01-02T03:04:05.678901Z', '2024-01-02',
              '{"b": 1, "a": [true, null]}', ARRAY['x', 'y'], 12.50, 12.50, 1.5, NULL),
             ('22222222-2222-4222-8222-222222222222', 'child', false, NULL, NULL, '{}', '{}', NULL, NULL, NULL,
              '11111111-1111-4111-8111-111111111111'),
             ('33333333-3333-4333-8333-333333333333', 'orphan', false, '1969-12-31T23:59:59.9995Z', NULL, '"text"', '{}',
              NULL, 0.1, -2, '99999999-9999-4999-8999-999999999999');
           INSERT INTO tasks (id, title) SELECT gen_random_uuid(), 'bulk ' || n FROM generate_series(1, 500) n;
           INSERT INTO labels (task_id, name) VALUES ('11111111-1111-4111-8111-111111111111', 'red');
           INSERT INTO members (id, account_id, status, display_name, permissions)
             VALUES ('mem-1', 'acct-1', 'active', 'Ada', '{"admin": true}');
           INSERT INTO _gonvex_action_outbox (id, action_path, args, status)
             VALUES ('act-1', 'mail:send', '{"to": "ada"}', 'pending');
           INSERT INTO _gonvex_reducer_idempotency (subject, idempotency_key, path, result)
             VALUES ('acct-1', 'k-1', 'tasks:create', '{"ok": true}');
           INSERT INTO _gonvex_sync_changes (transaction_id, revision, ordinal, table_name, row_id, operation, new_value)
             VALUES (1, 41, 1, 'tasks', 'x', 'insert', '{}'), (2, 42, 1, 'tasks', 'y', 'insert', '{}');
           UPDATE _gonvex_sync_clock SET revision = 42, epoch = 'pg-epoch';"#,
    )
    .execute(&tenant)
    .await
    .unwrap();

    let directory = std::env::temp_dir().join(format!("gonvex-export-{nonce}"));
    let report = export(&options(&control_url, &directory)).await.unwrap();
    let text = serde_json::to_string_pretty(&report).unwrap();
    assert!(report.ok, "{text}");
    assert!(!text.contains("secret"), "report leaks a password: {text}");
    let tenant_report = report
        .databases
        .iter()
        .find(|database| database.tenant_ids == ["acme"])
        .expect("acme exported");
    let tasks = tenant_report
        .tables
        .iter()
        .find(|table| table.table == "tasks")
        .unwrap();
    assert_eq!((tasks.source_rows, tasks.target_rows), (503, 503));
    assert_eq!(tasks.sampled_rows, 50);
    assert_eq!(tasks.defaulted_columns, vec!["note"]);
    assert!(tenant_report
        .skipped_source_tables
        .contains_key("_gonvex_sync_changes"));
    assert_eq!(
        report.databases.len(),
        2,
        "project and acme databases, not the deleted tenant"
    );

    // Control Plane routes point at the data directory; no Postgres URL survives.
    let data = gonvex_store::DataDir::new(&directory);
    let mut control_db = open(&data.control_path()).await;
    let routes: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT tenant_id, database_url, database_name FROM gonvex_runtime_tenants ORDER BY tenant_id",
    )
    .fetch_all(&mut control_db)
    .await
    .unwrap();
    assert_eq!(
        routes,
        vec![
            (
                "acme".to_owned(),
                data.tenant_url("proj", "acme").unwrap(),
                "projects/proj/tenants/acme.db".to_owned()
            ),
            (
                "gone".to_owned(),
                data.tenant_url("proj", "gone").unwrap(),
                "projects/proj/tenants/gone.db".to_owned()
            ),
        ]
    );
    assert!(!data.tenant_path("proj", "gone").unwrap().exists());
    let project_url: String =
        sqlx::query_scalar("SELECT database_url FROM gonvex_runtime_projects WHERE id='proj'")
            .fetch_one(&mut control_db)
            .await
            .unwrap();
    assert_eq!(project_url, data.project_url("proj").unwrap());
    let account: (i64, i64) = sqlx::query_as(
        "SELECT created_at, (SELECT verified_email FROM account_identities) FROM accounts",
    )
    .fetch_one(&mut control_db)
    .await
    .unwrap();
    assert_eq!(account, (1_704_164_645_678, 1));
    let secret: Vec<u8> =
        sqlx::query_scalar("SELECT client_secret_encrypted FROM gonvex_auth_providers")
            .fetch_one(&mut control_db)
            .await
            .unwrap();
    assert_eq!(secret, vec![0x00, 0xff, 0x10]);
    let setting: (String, i64) = sqlx::query_as("SELECT value, updated_at FROM app_settings")
        .fetch_one(&mut control_db)
        .await
        .unwrap();
    assert_eq!(
        setting,
        (
            // jsonb key order; the 17-digit integer keeps every digit.
            r#"{"n":12345678901234567,"dark":true}"#.to_owned(),
            1_714_979_289_123
        )
    );
    control_db.close().await.unwrap();

    // Tenant file: converted rows, fresh feed at the source revision.
    let mut acme = open(&data.tenant_path("proj", "acme").unwrap()).await;
    let root = sqlx::query(
        "SELECT done, due_at, due_on, meta, tags, price, price_text, score, typeof(price) FROM tasks WHERE id='11111111-1111-4111-8111-111111111111'",
    )
    .fetch_one(&mut acme)
    .await
    .unwrap();
    assert_eq!(root.get::<i64, _>(0), 1);
    assert_eq!(root.get::<i64, _>(1), 1_704_164_645_678);
    assert_eq!(root.get::<i64, _>(2), 1_704_153_600_000);
    assert_eq!(root.get::<String, _>(3), r#"{"a":[true,null],"b":1}"#);
    assert_eq!(root.get::<String, _>(4), r#"["x","y"]"#);
    assert_eq!(root.get::<f64, _>(5), 12.5);
    assert_eq!(root.get::<String, _>(6), "12.50");
    assert_eq!(root.get::<f64, _>(7), 1.5);
    assert_eq!(root.get::<String, _>(8), "real");
    let orphan: (i64, String) = sqlx::query_as(
        "SELECT due_at, meta FROM tasks WHERE id='33333333-3333-4333-8333-333333333333'",
    )
    .fetch_one(&mut acme)
    .await
    .unwrap();
    assert_eq!(orphan, (-1, "\"text\"".to_owned()));
    let seeded: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks WHERE id='seed'")
        .fetch_one(&mut acme)
        .await
        .unwrap();
    assert_eq!(seeded, 0, "migration seed rows are replaced by the copy");
    let clock: (String, i64, i64) =
        sqlx::query_as("SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock")
            .fetch_one(&mut acme)
            .await
            .unwrap();
    assert_ne!(clock.0, "pg-epoch", "epoch rotates so clients resync");
    assert_eq!((clock.1, clock.2), (42, 42));
    let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"SELECT (SELECT count(*) FROM _gonvex_sync_changes),
                  (SELECT count(*) FROM _gonvex_action_outbox WHERE status='pending'),
                  (SELECT count(*) FROM _gonvex_reducer_idempotency),
                  (SELECT count(*) FROM _gonvex_control_plane_membership_outbox WHERE member_id='mem-1'),
                  (SELECT count(*) FROM labels)"#,
    )
    .fetch_one(&mut acme)
    .await
    .unwrap();
    assert_eq!(counts, (0, 1, 1, 1, 1));
    // The reference ledger was rebuilt: the dangling parent is an
    // inventoried (unenforced) violation, the valid one an edge.
    let violations: Vec<String> =
        sqlx::query_scalar("SELECT source_id FROM gonvex_reference_violations")
            .fetch_all(&mut acme)
            .await
            .unwrap();
    assert_eq!(violations, vec!["33333333-3333-4333-8333-333333333333"]);
    // Change capture is live again: a write stages a change row.
    let mut transaction = acme.begin().await.unwrap();
    sqlx::query("UPDATE tasks SET title='renamed' WHERE id='11111111-1111-4111-8111-111111111111'")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let staged: i64 =
        sqlx::query_scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE revision IS NULL")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_eq!(staged, 1);
    transaction.rollback().await.unwrap();
    acme.close().await.unwrap();

    // Never overwrite without --force.
    let error = export(&options(&control_url, &directory))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("--force"), "{error}");

    // A Postgres column with no SQLite column fails loudly unless allowed.
    sqlx::query("ALTER TABLE tasks ADD COLUMN legacy text")
        .execute(&tenant)
        .await
        .unwrap();
    let mut forced = options(&control_url, &directory);
    forced.force = true;
    let error = export(&forced).await.unwrap_err();
    assert!(error.to_string().contains("legacy"), "{error}");
    forced.allow_drop_columns = true;
    let report = export(&forced).await.unwrap();
    assert!(
        report.ok,
        "{}",
        serde_json::to_string_pretty(&report).unwrap()
    );

    tenant.close().await;
    control.close().await;
    for schema in [&control_schema, &project_schema, &tenant_schema] {
        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
    }
    let _ = std::fs::remove_dir_all(&directory);
}
