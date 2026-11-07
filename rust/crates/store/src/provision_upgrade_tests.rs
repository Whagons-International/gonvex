//! Migration runner and sync bootstrap on SQLite: whole-script execution in
//! both transaction modes, cached warm boots that execute zero DDL, repair and
//! atomic rollback of change-capture installation.

use super::*;
use crate::transaction_commit_tests::TestDir;
use crate::{begin_transaction, PoolLimits};

struct Database {
    _dir: TestDir,
    pools: PoolRegistry,
    url: String,
}

#[tokio::test]
async fn foreign_key_rebuild_preserves_cascading_children_and_restores_enforcement() {
    let database = Database::new("gonvex-fk-rebuild");
    database.execute("CREATE TABLE parents(id TEXT PRIMARY KEY); CREATE TABLE children(id TEXT PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE CASCADE); INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p');").await;
    let migration=SqlMigration::new("rebuild".into(),MigrationScope::Tenant,false,
        "-- gonvex:foreign-key-rebuild\nCREATE TABLE parents_new(id TEXT PRIMARY KEY); INSERT INTO parents_new SELECT * FROM parents; DROP TABLE parents; ALTER TABLE parents_new RENAME TO parents;".into());
    apply_migrations(
        &database.pools,
        &database.url,
        vec![migration],
        MigrationScope::Tenant,
    )
    .await
    .unwrap();
    assert_eq!(database.scalar("SELECT count(*) FROM children").await, 1);
    let pool = database.pools.pool(&database.url).await.unwrap();
    let mut connection = pool.acquire().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
        1
    );
    assert!(sqlx::query("INSERT INTO children VALUES('bad','missing')")
        .execute(&mut *connection)
        .await
        .is_err());
    drop(connection);
    database.pools.close().await;
}

#[tokio::test]
async fn foreign_key_rebuild_rejects_existing_orphans_and_rolls_back_schema_and_receipt() {
    let database = Database::new("gonvex-fk-rebuild-rollback");
    database.execute("CREATE TABLE parents(id TEXT PRIMARY KEY); CREATE TABLE children(id TEXT PRIMARY KEY,parent TEXT); INSERT INTO children VALUES('legacy','missing');").await;
    let migration=SqlMigration::new("rebuild".into(),MigrationScope::Tenant,false,
        "-- gonvex:foreign-key-rebuild\nCREATE TABLE children_new(id TEXT PRIMARY KEY,parent TEXT REFERENCES parents(id)); INSERT INTO children_new SELECT * FROM children; DROP TABLE children; ALTER TABLE children_new RENAME TO children;".into());
    let error = apply_migrations(
        &database.pools,
        &database.url,
        vec![migration],
        MigrationScope::Tenant,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("foreign-key rebuild rejected"),
        "{error}"
    );
    assert_eq!(database.scalar("SELECT count(*) FROM children").await, 1);
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM pragma_foreign_key_list('children')")
            .await,
        0
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM gonvex_migrations WHERE name='rebuild'")
            .await,
        0
    );
    let pool = database.pools.pool(&database.url).await.unwrap();
    let mut connection = pool.acquire().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
        1
    );
    drop(connection);
    database.pools.close().await;
}

impl Database {
    fn new(prefix: &str) -> Self {
        let dir = TestDir::new(prefix);
        let url = crate::database_url(&dir.0.join("tenant.db"));
        Self {
            _dir: dir,
            pools: PoolRegistry::new(PoolLimits::default()),
            url,
        }
    }

    async fn execute(&self, sql: &str) {
        sqlx::raw_sql(sql)
            .execute(&self.pools.pool(&self.url).await.unwrap())
            .await
            .unwrap();
    }

    async fn scalar(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pools.reader(&self.url).await.unwrap())
            .await
            .unwrap()
    }

    async fn install(&self) -> Result<(), DatabaseError> {
        let _lock = self.pools.lock(&self.url, SCHEMA_LOCK).await?;
        let pool = self.pools.pool(&self.url).await?;
        let mut connection = pool.acquire().await?;
        install_change_feed_locked(&mut connection).await
    }

    async fn schema_version(&self) -> i64 {
        self.scalar("PRAGMA schema_version").await
    }

    /// Writes through a TenantTransaction and returns the staged image.
    async fn assert_change(&self, id: &str, extra: bool) {
        let mut transaction = begin_transaction(&self.pools, &self.url, false)
            .await
            .unwrap();
        let sql = if extra {
            "INSERT INTO tasks(id,title,extra) VALUES($1,'test','new column')"
        } else {
            "INSERT INTO tasks(id,title) VALUES($1,'test')"
        };
        sqlx::query(sql)
            .bind(id)
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let row = sqlx::query(
            "SELECT revision, new_value FROM _gonvex_sync_changes WHERE table_name='tasks' AND row_id=$1",
        )
        .bind(id)
        .fetch_one(&self.pools.reader(&self.url).await.unwrap())
        .await
        .unwrap();
        assert!(row.get::<Option<i64>, _>("revision").is_some());
        let value: serde_json::Value = row.get("new_value");
        assert_eq!(value["title"], "test");
        if extra {
            assert_eq!(value["extra"], "new column");
        }
    }
}

#[tokio::test]
async fn migrations_run_whole_scripts_once_in_both_transaction_modes() {
    let database = Database::new("gonvex-migrations");
    database
        .execute("CREATE TABLE migration_values (name TEXT, value TEXT)")
        .await;
    let mut migrations = Vec::new();
    let mut expected = Vec::new();
    for no_transaction in [false, true] {
        for (case, literal, value, comment) in [
            ("comment", "'first'", "first", " -- comment;\r\n"),
            ("quote", "'a'';b'", "a';b", ""),
            ("block", "'semi;colon'", "semi;colon", " /* ; */"),
        ] {
            let name = format!("{case}_{no_transaction}");
            migrations.push(SqlMigration::new(
                name.clone(),
                MigrationScope::ControlPlane,
                no_transaction,
                format!(
                    "INSERT INTO migration_values VALUES ('{name}', {literal}){comment}; \
                     INSERT INTO migration_values VALUES ('{name}', 'second');"
                ),
            ));
            expected.push((name.clone(), value.to_owned()));
            expected.push((name, "second".to_owned()));
        }
    }
    // Trigger bodies contain semicolons; raw scripts need no splitter.
    migrations.push(SqlMigration::new(
        "trigger".to_owned(),
        MigrationScope::ControlPlane,
        false,
        "CREATE TRIGGER copy_values AFTER INSERT ON migration_values WHEN NEW.name='trigger-source' BEGIN \
           INSERT INTO migration_values VALUES ('trigger', 'one'); \
           INSERT INTO migration_values VALUES ('trigger', 'two'); \
         END; INSERT INTO migration_values VALUES ('trigger-source', 'x');"
            .to_owned(),
    ));
    for (name, value) in [
        ("trigger", "one"),
        ("trigger", "two"),
        ("trigger-source", "x"),
    ] {
        expected.push((name.to_owned(), value.to_owned()));
    }
    let applied = apply_migrations(
        &database.pools,
        &database.url,
        migrations.clone(),
        MigrationScope::ControlPlane,
    )
    .await
    .unwrap();
    assert_eq!(applied.len(), migrations.len());
    assert!(apply_migrations(
        &database.pools,
        &database.url,
        migrations.clone(),
        MigrationScope::ControlPlane
    )
    .await
    .unwrap()
    .is_empty());
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT name,value FROM migration_values ORDER BY name,value")
            .fetch_all(&database.pools.reader(&database.url).await.unwrap())
            .await
            .unwrap();
    expected.sort();
    assert_eq!(rows, expected, "replay must not execute migrations twice");

    // A changed artifact is refused; a failing script leaves no partial effect
    // and no ledger row.
    let mut changed = migrations[0].clone();
    changed.sql.push_str(" -- edited");
    changed.checksum = SqlMigration::new(
        changed.name.clone(),
        changed.scope,
        false,
        changed.sql.clone(),
    )
    .checksum;
    let error = apply_migrations(
        &database.pools,
        &database.url,
        vec![changed],
        MigrationScope::ControlPlane,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"), "{error}");
    let failing = SqlMigration::new(
        "failing".to_owned(),
        MigrationScope::ControlPlane,
        false,
        "INSERT INTO migration_values VALUES ('failing','x'); SELECT * FROM missing_table;"
            .to_owned(),
    );
    assert!(apply_migrations(
        &database.pools,
        &database.url,
        vec![failing],
        MigrationScope::ControlPlane
    )
    .await
    .is_err());
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM migration_values WHERE name='failing'")
            .await,
        0
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM gonvex_migrations WHERE name='failing'")
            .await,
        0
    );
    database.pools.close().await;
}

#[tokio::test]
async fn sync_bootstrap_caches_repairs_and_commits_atomically() {
    let database = Database::new("gonvex-bootstrap");
    database
        .execute(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT);
             CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<5000)
             INSERT INTO tasks SELECT CAST(i AS TEXT), 'task ' || i FROM n;",
        )
        .await;
    let started = Instant::now();
    database.install().await.unwrap();
    eprintln!(
        "5k-row fixture cold sync bootstrap: {:?}",
        started.elapsed()
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_bootstrap")
            .await,
        3
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name LIKE '\\_gonvex\\_sync\\_%' ESCAPE '\\'")
            .await,
        6
    );
    database.assert_change("first", false).await;

    let version = database.schema_version().await;
    database.install().await.unwrap();
    assert_eq!(
        database.schema_version().await,
        version,
        "warm boot must execute zero DDL"
    );
    database.assert_change("warm", false).await;

    database
        .execute("ALTER TABLE tasks ADD COLUMN extra TEXT")
        .await;
    database.install().await.unwrap();
    database.assert_change("changed", true).await;

    database
        .execute("DROP TRIGGER _gonvex_sync_tasks_insert")
        .await;
    database.install().await.unwrap();
    database.assert_change("repaired", true).await;

    // A later invalid table rolls back earlier table repairs and their receipts.
    let receipt = sqlx::query_scalar::<_, String>(
        "SELECT digest FROM _gonvex_sync_bootstrap WHERE name='table:tasks'",
    )
    .fetch_one(&database.pools.reader(&database.url).await.unwrap())
    .await
    .unwrap();
    database
        .execute("ALTER TABLE tasks ADD COLUMN another TEXT")
        .await;
    database
        .execute("CREATE TABLE z_invalid (value TEXT)")
        .await;
    let version = database.schema_version().await;
    let error = database.install().await.unwrap_err();
    assert!(error.to_string().contains("z_invalid"), "{error}");
    assert!(error.to_string().contains("no primary key"), "{error}");
    assert_eq!(database.schema_version().await, version);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT digest FROM _gonvex_sync_bootstrap WHERE name='table:tasks'"
        )
        .fetch_one(&database.pools.reader(&database.url).await.unwrap())
        .await
        .unwrap(),
        receipt
    );
    database.execute("DROP TABLE z_invalid").await;
    database.install().await.unwrap();

    // Concurrent cold installers serialize on the schema lock.
    database.execute("DROP TABLE _gonvex_sync_bootstrap").await;
    let (a, b) = tokio::join!(database.install(), database.install());
    a.unwrap();
    b.unwrap();
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_bootstrap")
            .await,
        3
    );
    let version = database.schema_version().await;
    database.install().await.unwrap();
    assert_eq!(database.schema_version().await, version);
    database.pools.close().await;
}

#[tokio::test]
async fn provisioning_twice_is_idempotent_and_projects_membership() {
    let dir = TestDir::new("gonvex-provision");
    let data = crate::DataDir::new(&dir.0);
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&data.control_url(), pools.clone(), Default::default())
        .await
        .unwrap();
    let route = TenantRoute {
        project_id: "project".to_owned(),
        tenant_id: "tenant".to_owned(),
        database_url: data.tenant_url("project", "tenant").unwrap(),
    };
    let mut migrations = vec![SqlMigration::new(
        "0001_tasks.sql".to_owned(),
        MigrationScope::Tenant,
        false,
        "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN NOT NULL DEFAULT 0);"
            .to_owned(),
    )];
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations.clone())
        .await
        .unwrap();
    let writer = pools.pool(&route.database_url).await.unwrap();
    let schema_version = || async {
        sqlx::query_scalar::<_, i64>("PRAGMA schema_version")
            .fetch_one(&writer)
            .await
            .unwrap()
    };
    let version = schema_version().await;
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations.clone())
        .await
        .unwrap();
    // Membership triggers are rebuilt on each boot; nothing else changes.
    let rebuilt = schema_version().await;
    assert!(rebuilt - version <= 6, "{version} -> {rebuilt}");

    // A later migration's own writes are revisioned under its command id and published.
    let mut listener = pools.listener(&route.database_url).await.unwrap();
    listener.listen(feed::CHANGE_FEED_CHANNEL).await.unwrap();
    migrations.push(SqlMigration::new(
        "0002_seed.sql".to_owned(),
        MigrationScope::Tenant,
        false,
        "INSERT INTO tasks(id,title) VALUES ('seed','Seeded by migration');".to_owned(),
    ));
    control
        .clone()
        .provision_tenant_database(route.clone(), migrations)
        .await
        .unwrap();
    let notification = tokio::time::timeout(std::time::Duration::from_secs(2), listener.recv())
        .await
        .unwrap()
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(notification.payload()).unwrap();
    assert_eq!(payload["revision"], 1);
    let command: String = sqlx::query_scalar(
        "SELECT command_id FROM _gonvex_sync_changes WHERE table_name='tasks' AND row_id='seed'",
    )
    .fetch_one(&writer)
    .await
    .unwrap();
    assert_eq!(command, "migration:0002_seed.sql");

    let mut transaction = control
        .begin_tenant_transaction(&route, false)
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO members(id,account_id,status,membership_revision) VALUES('m1','a1','active',1);
         UPDATE members SET status='suspended',membership_revision=2 WHERE id='m1';",
    )
    .execute(&mut **transaction.transaction())
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let outbox: (String, String, i64) = sqlx::query_as(
        "SELECT member_id,status,membership_revision FROM _gonvex_control_plane_membership_outbox WHERE account_id='a1'",
    )
    .fetch_one(&writer)
    .await
    .unwrap();
    assert_eq!(outbox, ("m1".to_owned(), "suspended".to_owned(), 2));
    let mut transaction = control
        .begin_tenant_transaction(&route, false)
        .await
        .unwrap();
    sqlx::raw_sql("DELETE FROM members WHERE id='m1'")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let outbox: (String, i64) = sqlx::query_as(
        "SELECT status,membership_revision FROM _gonvex_control_plane_membership_outbox WHERE account_id='a1'",
    )
    .fetch_one(&writer)
    .await
    .unwrap();
    assert_eq!(outbox, ("revoked".to_owned(), 3));
    // A stale projection never overwrites a newer one.
    sqlx::query("INSERT INTO members(id,account_id,status,membership_revision) VALUES('m1','a1','active',1)")
        .execute(&writer)
        .await
        .unwrap();
    let status: String = sqlx::query_scalar(
        "SELECT status FROM _gonvex_control_plane_membership_outbox WHERE account_id='a1'",
    )
    .fetch_one(&writer)
    .await
    .unwrap();
    assert_eq!(status, "revoked");
    pools.close().await;
}

#[tokio::test]
async fn identity_v1_tenants_are_refused() {
    let database = Database::new("gonvex-identity");
    database
        .execute("CREATE TABLE members (id TEXT PRIMARY KEY, user_id TEXT)")
        .await;
    let pool = database.pools.pool(&database.url).await.unwrap();
    let mut connection = pool.acquire().await.unwrap();
    let error = install_tenant_identity(&mut connection).await.unwrap_err();
    assert!(error.to_string().contains("identity-v2"), "{error}");
}

#[tokio::test]
async fn change_capture_skips_fts_virtual_and_shadow_tables() {
    let database = Database::new("gonvex-fts");
    database
        .execute(
            "CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT);
             CREATE VIRTUAL TABLE docs_fts USING fts5(body, content='docs', content_rowid='rowid');
             CREATE TRIGGER docs_fts_insert AFTER INSERT ON docs BEGIN
               INSERT INTO docs_fts(rowid, body) VALUES (NEW.rowid, NEW.body);
             END;",
        )
        .await;
    database.install().await.unwrap();
    let captured: Vec<String> = sqlx::query_scalar(
        r"SELECT DISTINCT tbl_name FROM sqlite_schema WHERE type='trigger' AND name LIKE '\_gonvex\_sync\_%' ESCAPE '\' ORDER BY 1",
    )
    .fetch_all(&database.pools.reader(&database.url).await.unwrap())
    .await
    .unwrap();
    assert_eq!(captured, ["docs"]);
    let mut transaction = begin_transaction(&database.pools, &database.url, false)
        .await
        .unwrap();
    sqlx::query("INSERT INTO docs VALUES ('d1', 'searchable text')")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM docs_fts WHERE docs_fts MATCH 'searchable'")
            .await,
        1
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE table_name<>'docs'")
            .await,
        0
    );
}

#[tokio::test]
async fn derived_table_capture_is_owned_and_removes_old_capture_receipts() {
    let database = Database::new("gonvex-derived");
    database.execute("CREATE TABLE parents(id TEXT PRIMARY KEY); CREATE TABLE edges(id TEXT PRIMARY KEY,owner TEXT REFERENCES parents(id));").await;
    database.install().await.unwrap();
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_bootstrap WHERE name='table:edges'")
            .await,
        1
    );
    database.execute("INSERT INTO _gonvex_table_options(name,capture,owner_table,owner_column) VALUES('edges','derived','parents','owner')").await;
    database.install().await.unwrap();
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_bootstrap WHERE name='table:edges'")
            .await,
        0
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name='edges'")
            .await,
        0
    );
    let mut transaction = begin_transaction(&database.pools, &database.url, false)
        .await
        .unwrap();
    sqlx::raw_sql("INSERT INTO parents VALUES('p'); INSERT INTO edges VALUES('e','p');")
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE table_name='edges'")
            .await,
        0
    );
    database
        .execute("UPDATE _gonvex_table_options SET owner_column='absent' WHERE name='edges'")
        .await;
    let error = database.install().await.unwrap_err();
    assert!(error.to_string().contains("owner foreign key"), "{error}");
    database.pools.close().await;
}

#[tokio::test]
async fn normalized_records_capture_public_images_after_all_sql_changes_and_savepoint_rollback() {
    let database = Database::new("gonvex-records");
    database.install().await.unwrap();
    database.execute(r#"
      INSERT INTO _gonvex_id_domains(_id,name) VALUES(1,'tasks');
      INSERT INTO _gonvex_identity_keys(_id,domain,value) VALUES(10,1,'public-task');
      INSERT INTO _gonvex_interned_text(_id,value) VALUES(1,'Shared title'),(2,'Other title');
      CREATE TABLE taskStorage(_id INTEGER PRIMARY KEY REFERENCES _gonvex_identity_keys(_id),titleId INTEGER REFERENCES _gonvex_interned_text(_id),metadata BLOB,done BOOLEAN NOT NULL DEFAULT 0);
      CREATE VIEW tasks AS SELECT i.value _id,t.value name,json(s.metadata) metadata,s.done FROM taskStorage s JOIN _gonvex_identity_keys i ON i._id=s._id LEFT JOIN _gonvex_interned_text t ON t._id=s.titleId;
    "#).await;
    let schema = serde_json::json!({"key":"_id","columns":{"_id":{"type":"text","nullable":false},"name":{"type":"text","nullable":false},"metadata":{"type":"json","nullable":true},"done":{"type":"boolean","nullable":false}}});
    sqlx::query("INSERT INTO _gonvex_record_views(name,storage_table,key_column,schema) VALUES('tasks','taskStorage','_id',$1)")
      .bind(schema.to_string()).execute(&database.pools.pool(&database.url).await.unwrap()).await.unwrap();
    database.install().await.unwrap();
    let mut transaction = begin_transaction(&database.pools, &database.url, false)
        .await
        .unwrap();
    sqlx::query("INSERT INTO taskStorage(_id,titleId,metadata) VALUES(10,1,jsonb('{\"nested\":[true,12]}'))").execute(&mut **transaction.transaction()).await.unwrap();
    transaction.commit().await.unwrap();
    let reader = database.pools.reader(&database.url).await.unwrap();
    let inserted: serde_json::Value = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT new_value FROM _gonvex_sync_changes WHERE table_name='tasks'",
        )
        .fetch_one(&reader)
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        inserted,
        serde_json::json!({"_id":"public-task","name":"Shared title","metadata":{"nested":[true,12]},"done":false})
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE table_name='taskStorage'")
            .await,
        0
    );
    let mut transaction = begin_transaction(&database.pools, &database.url, false)
        .await
        .unwrap();
    sqlx::raw_sql("UPDATE taskStorage SET titleId=2,done=1 WHERE _id=10; UPDATE taskStorage SET done=0 WHERE _id=10;").execute(&mut **transaction.transaction()).await.unwrap();
    transaction.commit().await.unwrap();
    let (old,new,changed):(String,String,String)=sqlx::query_as("SELECT old_value,new_value,changed_columns FROM _gonvex_sync_changes WHERE table_name='tasks' ORDER BY revision DESC LIMIT 1").fetch_one(&reader).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&old).unwrap()["name"],
        "Shared title"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&new).unwrap()["name"],
        "Other title"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&changed).unwrap(),
        serde_json::json!(["name"])
    );
    let mut transaction = begin_transaction(&database.pools, &database.url, false)
        .await
        .unwrap();
    sqlx::raw_sql("SAVEPOINT draft; UPDATE taskStorage SET titleId=1 WHERE _id=10; ROLLBACK TO draft; RELEASE draft; UPDATE taskStorage SET done=1 WHERE _id=10;").execute(&mut **transaction.transaction()).await.unwrap();
    transaction.commit().await.unwrap();
    let changed:String=sqlx::query_scalar("SELECT changed_columns FROM _gonvex_sync_changes WHERE table_name='tasks' ORDER BY revision DESC LIMIT 1").fetch_one(&reader).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&changed).unwrap(),
        serde_json::json!(["done"])
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_record_pending")
            .await,
        0
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM pragma_foreign_key_check")
            .await,
        0
    );
    // The guard also protects a raw writer which forgot transaction finalization.
    assert!(sqlx::query("UPDATE taskStorage SET done=0 WHERE _id=10")
        .execute(&database.pools.pool(&database.url).await.unwrap())
        .await
        .is_err());
    assert_eq!(database.scalar("SELECT done FROM taskStorage").await, 1);
    database.pools.close().await;
}

#[tokio::test]
async fn normalized_owned_rows_capture_all_affected_owners_once() {
    let database = Database::new("gonvex-owned-records");
    database.install().await.unwrap();
    database.execute(r#"
 INSERT INTO _gonvex_id_domains VALUES(1,'tasks');
 INSERT INTO _gonvex_identity_keys VALUES(10,1,'a'),(11,1,'b');
 CREATE TABLE schedules(_id INTEGER PRIMARY KEY,value TEXT NOT NULL);
 CREATE TABLE taskStorage(_id INTEGER PRIMARY KEY REFERENCES _gonvex_identity_keys(_id),scheduleId INTEGER REFERENCES schedules(_id));
 CREATE TABLE details(_id INTEGER PRIMARY KEY,taskId INTEGER NOT NULL UNIQUE REFERENCES taskStorage(_id) ON DELETE CASCADE,value TEXT);
 CREATE VIEW tasks AS SELECT i.value _id,s.value schedule,d.value detail FROM taskStorage t JOIN _gonvex_identity_keys i ON i._id=t._id LEFT JOIN schedules s ON s._id=t.scheduleId LEFT JOIN details d ON d.taskId=t._id;
 INSERT INTO _gonvex_record_views VALUES('tasks','taskStorage','_id','{"key":"_id","columns":{"_id":{"type":"text"},"schedule":{"type":"text"},"detail":{"type":"text"}}}');
 INSERT INTO _gonvex_table_options(name,capture,owner_table,owner_column,owner_direction) VALUES('schedules','owned','taskStorage','scheduleId','inbound'),('details','owned','taskStorage','taskId','outbound');
 INSERT INTO schedules VALUES(1,'daily'); INSERT INTO taskStorage VALUES(10,1),(11,1); INSERT INTO details VALUES(1,10,'first');
 "#).await;
    database.install().await.unwrap();
    let mut tx = begin_transaction(&database.pools, &database.url, false)
        .await
        .unwrap();
    sqlx::raw_sql("UPDATE schedules SET value='weekly'; UPDATE details SET value='second'; SAVEPOINT draft; UPDATE details SET value='discarded'; ROLLBACK TO draft; RELEASE draft;").execute(&mut **tx.transaction()).await.unwrap();
    tx.commit().await.unwrap();
    let reader = database.pools.reader(&database.url).await.unwrap();
    let rows:Vec<(String,String,String)>=sqlx::query_as("SELECT row_id,old_value,new_value FROM _gonvex_sync_changes WHERE table_name='tasks' ORDER BY row_id").fetch_all(&reader).await.unwrap();
    assert_eq!(rows.len(), 2);
    for (id, old, new) in rows {
        let old: serde_json::Value = serde_json::from_str(&old).unwrap();
        let new: serde_json::Value = serde_json::from_str(&new).unwrap();
        assert_eq!(old["schedule"], "daily");
        assert_eq!(new["schedule"], "weekly");
        if id == "a" {
            assert_eq!(old["detail"], "first");
            assert_eq!(new["detail"], "second");
        }
    }
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_record_pending")
            .await,
        0
    );
    database.pools.close().await;
}

#[tokio::test]
async fn entity_definitions_capture_their_dependent_logical_records() {
    let database = Database::new("gonvex-dependent-records");
    database.install().await.unwrap();
    database.execute(r#"
 INSERT INTO _gonvex_id_domains VALUES(1,'values'); INSERT INTO _gonvex_identity_keys VALUES(10,1,'value');
 CREATE TABLE fields(_id INTEGER PRIMARY KEY,name TEXT NOT NULL); INSERT INTO fields VALUES(1,'Original');
 CREATE TABLE valueStorage(_id INTEGER PRIMARY KEY REFERENCES _gonvex_identity_keys(_id),fieldId INTEGER NOT NULL REFERENCES fields(_id),value INTEGER);
 INSERT INTO valueStorage VALUES(10,1,7);
 CREATE VIEW valuesPublic AS SELECT i.value _id,f.name,v.value FROM valueStorage v JOIN _gonvex_identity_keys i ON i._id=v._id JOIN fields f ON f._id=v.fieldId;
 INSERT INTO _gonvex_record_views VALUES('valuesPublic','valueStorage','_id','{"key":"_id","columns":{"_id":{"type":"text"},"name":{"type":"text"},"value":{"type":"integer"}}}');
 INSERT INTO _gonvex_record_dependencies(source_table,source_column,owner_storage,owner_column) VALUES('fields','_id','valueStorage','fieldId');
 "#).await;
    database.install().await.unwrap();
    let mut tx = begin_transaction(&database.pools, &database.url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE fields SET name='Renamed' WHERE _id=1")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let reader = database.pools.reader(&database.url).await.unwrap();
    let (old, new): (String, String) = sqlx::query_as(
        "SELECT old_value,new_value FROM _gonvex_sync_changes WHERE table_name='valuesPublic'",
    )
    .fetch_one(&reader)
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&old).unwrap()["name"],
        "Original"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&new).unwrap()["name"],
        "Renamed"
    );
    assert_eq!(
        database
            .scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE table_name='fields'")
            .await,
        1
    );
    database.pools.close().await;
}
