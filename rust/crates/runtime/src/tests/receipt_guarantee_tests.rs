//! Native receipt/commit composition on owned SQLite files and a real module host.
//!
//! SQLite gives every database file one writer connection (writes serialize in
//! process) and a read-only pool whose transactions see one WAL snapshot. The
//! races that PostgreSQL witnessed with advisory locks and `pg_blocking_pids`
//! are orchestrated here with the mechanisms that now provide the guarantee:
//! the runtime's keyed in-process locks, the single writer connection (a
//! fixture holds it in a real tenant transaction so reducers queue behind it)
//! and query admission.
use super::*;
use base64::Engine as _;
use gonvex_store::{MigrationScope, RuntimeManifestRecord, SqlMigration, TenantRoute};
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqlitePool;
use sqlx::Row as _;
use std::path::PathBuf;

const SCHEMA_LOCK: &str = "gonvex:reducer-receipt-schema";

struct Fixture {
    runtime: Runtime,
    control: ControlPlane,
    session: TenantSession,
    /// The runtime's single tenant writer connection.
    pool: SqlitePool,
    /// The runtime's read-only tenant pool (committed WAL snapshots only).
    reader: SqlitePool,
    /// The Control Plane writer.
    control_pool: SqlitePool,
    root: PathBuf,
    _data: Option<tempfile::TempDir>,
}

/// The runtime writer is checked out by some transaction (its only connection).
fn writer_owned(pool: &SqlitePool) -> bool {
    pool.size() == 1 && pool.num_idle() == 0
}

/// Competing SDK claims of one logical intent, serialized by the runtime's
/// own single tenant writer (the only writer in production). The gate holds
/// that writer in a real tenant write transaction while both SDK reducers are
/// admitted and queue behind it; reads keep working and see none of the
/// intent meanwhile. `commit`: the gate releases without effect, one claimant
/// executes and the other replays its receipt. `rollback`: the gate is a first
/// owner that claimed the same receipt and wrote the business row, then rolls
/// back; a claimant must take over (execute, not replay a phantom receipt).
#[derive(Default)]
struct SDKConcurrencyGate {
    index: usize,
    gate: Option<gonvex_store::TenantTransaction>,
    stable: usize,
    witnesses: Vec<serde_json::Value>,
}

/// Transactions queued for this tenant database's single writer.
fn queued_writers(fixture: &Fixture) -> u32 {
    let path = gonvex_store::database_path(&fixture.session.route.database_url);
    fixture
        .runtime
        .inner
        .pools
        .writer_queues(usize::MAX)
        .into_iter()
        .find(|queue| std::path::Path::new(&queue.database) == path)
        .map_or(0, |queue| queue.waiting)
}

impl SDKConcurrencyGate {
    async fn release(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.rollback().await.unwrap();
        }
    }

    async fn poll(&mut self, fixture: &Fixture, root: &std::path::Path) {
        if self.index >= 2 {
            return;
        }
        let mode = ["commit", "rollback"][self.index];
        let directory = root.join("concurrency").join(mode);
        if self.gate.is_none() {
            let request = directory.join("request.json");
            if !tokio::fs::try_exists(&request).await.unwrap() {
                return;
            }
            let request: serde_json::Value =
                serde_json::from_slice(&tokio::fs::read(request).await.unwrap()).unwrap();
            assert_eq!(request["mode"], mode);
            assert_eq!(request["rowId"], format!("sdk-concurrent-{mode}"));
            let key = request["key"].as_str().unwrap();
            assert!(!key.is_empty());
            // The runtime's own writer: the same single connection reducers use.
            let mut gate = gonvex_store::begin_transaction(
                &fixture.runtime.inner.pools,
                &fixture.session.route.database_url,
                false,
            )
            .await
            .unwrap();
            if mode == "rollback" {
                assert!(gate.claim_reducer("account", key, "write").await.unwrap());
                sqlx::query("INSERT INTO receipt_records(id, value) VALUES ($1, 7)")
                    .bind(request["rowId"].as_str().unwrap())
                    .execute(&mut **gate.transaction())
                    .await
                    .unwrap();
            }
            self.gate = Some(gate);
            self.stable = 0;
            tokio::fs::write(directory.join("prepared.tmp"), b"{\"ready\":true}")
                .await
                .unwrap();
            tokio::fs::rename(
                directory.join("prepared.tmp"),
                directory.join("prepared.json"),
            )
            .await
            .unwrap();
            return;
        }
        let row_id = format!("sdk-concurrent-{mode}");
        let queued = queued_writers(fixture);
        let owned = writer_owned(&fixture.pool);
        // Reads proceed while the writer is held, and see nothing uncommitted:
        // a second writer that let a claimant commit would show here.
        let visible = tokio::time::timeout(
            Duration::from_secs(2),
            sqlx::query("SELECT count(*) FROM receipt_records WHERE id=$1")
                .bind(&row_id)
                .fetch_one(&fixture.reader),
        )
        .await;
        let read = visible.is_ok();
        let rows = visible
            .ok()
            .map(|row| row.unwrap().get::<i64, _>(0))
            .unwrap_or(-1);
        assert_eq!(
            rows.max(0),
            0,
            "a claimant committed while the single writer was held"
        );
        if owned && queued >= 2 && read {
            self.stable += 1;
        } else {
            self.stable = 0;
            tokio::fs::write(
                directory.join("wait-graph.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "writerOwned": owned, "queuedWriters": queued, "readDuringWrite": read,
                    "admissionActive": fixture.runtime.inner.pools.snapshot().await.admission_active,
                    "writerQueues": fixture.runtime.inner.pools.writer_queues(8)}))
                .unwrap(),
            )
            .await
            .unwrap();
        }
        // Sustained (three polls 50 ms apart), not one sample.
        if self.stable < 3 {
            return;
        }
        let mut witness = serde_json::json!({"overlap": true, "mode": mode,
            "gate": "runtime-writer", "writerOwned": owned, "queuedWriters": queued,
            "readDuringWrite": read, "rowsWhileHeld": rows});
        if mode == "rollback" {
            witness["ownerRolledBack"] = serde_json::json!(true);
        }
        if std::env::var_os("GONVEX_SDK_CONCURRENCY_DAMAGE").is_some() {
            witness["overlap"] = serde_json::json!(false);
        }
        self.release().await;
        tokio::fs::write(
            directory.join("witness.tmp"),
            serde_json::to_vec(&witness).unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::rename(
            directory.join("witness.tmp"),
            directory.join("witness.json"),
        )
        .await
        .unwrap();
        self.witnesses.push(witness);
        self.stable = 0;
        self.index += 1;
    }
}

impl Fixture {
    async fn new() -> Option<Self> {
        Self::build(false, None).await
    }

    async fn create(sdk: bool) -> Option<Self> {
        Self::build(sdk, None).await
    }

    /// `root` reuses one exact data directory (process-crash recovery);
    /// otherwise each fixture owns a fresh directory.
    async fn build(sdk: bool, root: Option<PathBuf>) -> Option<Self> {
        let required = std::env::var_os("GONVEX_RECEIPT_TEST_REQUIRE_NATIVE").is_some();
        let Some(binary) = std::env::var("GONVEX_TEST_MODULE_HOST_BINARY").ok() else {
            assert!(!required, "native receipt test requires the module host");
            return None;
        };
        let (data, root) = match root {
            Some(root) => (None, root),
            None => {
                let data = test_data_dir();
                let root = data.path().to_path_buf();
                (Some(data), root)
            }
        };
        let control_url = gonvex_store::database_url(&root.join("control.db"));
        let url = gonvex_store::database_url(&root.join("tenant.db"));
        let mut cfg = config(true);
        cfg.module_host.binary = Some(binary.into());
        cfg.module_host.endpoint = Some(format!(
            "unix:{}/receipt-{}.sock",
            std::env::temp_dir().display(),
            uuid::Uuid::new_v4().simple()
        ));
        // The SDK composition runs two competing reducers at once (both must
        // reach the writer queue); other fixtures keep one isolate.
        cfg.module_host.isolate_pool_size = if sdk { 2 } else { 1 };
        cfg.module_host.max_concurrent_calls = 2;
        cfg.module_host.start_timeout = Duration::from_secs(5);
        let runtime = Runtime::new(cfg);
        let control =
            ControlPlane::connect(&control_url, runtime.inner.pools.clone(), BTreeMap::new())
                .await
                .unwrap();
        let pool = runtime.inner.pools.pool(&url).await.unwrap();
        let reader = runtime.inner.pools.reader(&url).await.unwrap();
        let control_pool = runtime.inner.pools.pool(&control_url).await.unwrap();
        // Created before provisioning so the change-feed capture triggers exist.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS receipt_records(id TEXT PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let route = TenantRoute {
            project_id: "project".into(),
            tenant_id: "tenant".into(),
            database_url: url,
        };
        control
            .clone()
            .provision_tenant_database(route.clone(), Vec::new())
            .await
            .unwrap();
        *runtime.inner.control_plane.write().await = Some(control.clone());
        runtime.inner.module_host.start().await.unwrap();
        let code = r#"
export async function write(ctx, args) { await ctx.db.insert("receipt_records", { id: args.id, value: args.value }); return args; }
export async function noop(ctx, args) { return args; }
export async function abort(ctx, args) { await ctx.db.insert("receipt_records", { id: args.id, value: args.value }); throw new Error("intentional rollback"); }
export function all() { throw new Error("replica collections must use the server SQL plan"); }
"#;
        let mut functions = serde_json::json!({});
        for path in ["write", "noop", "abort"] {
            functions[path] = serde_json::json!({"kind":"reducer", "public":false, "internal":false,
                "interactive":false, "classification":"system", "handler":path, "file":"receipt.js",
                "delivery":"oneShot", "args":{"kind":"any"}, "result":{"kind":"any"}});
        }
        let mut module = serde_json::json!({"generation":1,"language":"typescript","entrypoint":"receipt.js",
            "files":{},"visibility":{},"crons":[],"functions":functions,
            "javascript":{"path":"receipt.js","hash":format!("{:x}",Sha256::digest(code.as_bytes())),
            "code":base64::engine::general_purpose::STANDARD.encode(code)}});
        if sdk {
            module["functions"]["all"] = serde_json::json!({"kind":"query","public":false,"internal":false,
                "interactive":false,"classification":"system","handler":"all","file":"receipt.js",
                "delivery":"replica","args":{"kind":"any"},"result":{"kind":"any"},
                "replica":{"table":"receipt_records","key":"id","columns":["id","value"],"maxRows":100}});
            module["functions"]["quietLive"] = serde_json::json!({"kind":"query","handler":"all","file":"receipt.js",
                "delivery":"live","args":{"kind":"any"},"result":{"kind":"any"},
                "dependencies":{"liveQueryPlan":{"table":"receipt_records","key":"id","columns":["id","value"]}}});
            module["visibility"] = serde_json::json!({"receipt_records":{"table":"receipt_records","key":"id",
                "where":{"operator":"public"}}});
        }
        let hash = crate::modules::canonical_artifact_hash(module.as_object().unwrap()).unwrap();
        module["hash"] = serde_json::json!(hash);
        runtime
            .inner
            .modules
            .install(
                &runtime.inner.module_host,
                RuntimeManifestRecord {
                    project_id: "project".into(),
                    module_hash: hash,
                    manifest: serde_json::json!({"module":module}),
                },
            )
            .await
            .unwrap();
        let mut session = TenantSession::anonymous(route);
        session.identity.account.id = "account".into();
        session.member.id = "member".into();
        session.member.account_id = "account".into();
        session.member.status = "active".into();
        Some(Self {
            runtime,
            control,
            session,
            pool,
            reader,
            control_pool,
            root,
            _data: data,
        })
    }

    async fn call(
        &self,
        id: &str,
        key: &str,
        path: &str,
        row: &str,
    ) -> execution::ReducerExecution {
        self.runtime
            .execute_tenant_reducer(
                &self.session,
                id,
                Some(key),
                path,
                serde_json::json!({"id":row,"value":7}),
            )
            .await
            .unwrap()
    }

    /// Tenant writes go through a `TenantTransaction`, like every runtime
    /// write, so the change feed revisions them in the same commit.
    async fn tenant_exec_on(&self, route: &TenantRoute, sql: &str) {
        let mut transaction = self
            .control
            .begin_tenant_transaction(route, false)
            .await
            .unwrap();
        gonvex_store::execute_script(transaction.transaction(), sql)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }

    async fn tenant_exec(&self, sql: &str) {
        self.tenant_exec_on(&self.session.route, sql).await;
    }

    /// Schema changes made by an operator, outside any runtime transaction.
    async fn ddl(&self, sql: &str) {
        let mut connection = self.pool.acquire().await.unwrap();
        gonvex_store::execute_script(&mut connection, sql)
            .await
            .unwrap();
    }

    /// Committed state as any other connection sees it.
    async fn count(&self, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&self.reader)
            .await
            .unwrap()
    }

    async fn table_exists(&self, name: &str) -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=$1)",
        )
        .bind(name)
        .fetch_one(&self.reader)
        .await
        .unwrap()
    }

    async fn integrity(&self) -> String {
        sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_one(&self.reader)
            .await
            .unwrap()
    }

    async fn enable_sdk_session(&self) {
        let expires = gonvex_store::unix_millis() + 3_600_000;
        sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,environment,database_mode,status,auth_mode) VALUES ('project','Receipt fixture','test','multiTenant','active','firebase')")
            .execute(&self.control_pool).await.unwrap();
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,status,database_url) VALUES ('tenant','project','tenant','Receipt fixture','active',$1)")
            .bind(&self.session.route.database_url).execute(&self.control_pool).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,auth_realm_id,email,name) VALUES ('account','project','receipt@example.test','Receipt fixture')")
            .execute(&self.control_pool).await.unwrap();
        sqlx::query("INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,expires_at) VALUES ($1,'project','account',$2)")
            .bind(gonvex_store::token_hash("gvx_session_receipt_sdk_fixture")).bind(expires).execute(&self.control_pool).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,auth_realm_id,email,name) VALUES ('bob','project','bob@example.test','Bob fixture')")
            .execute(&self.control_pool).await.unwrap();
        sqlx::query("INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,expires_at) VALUES ($1,'project','bob',$2)")
            .bind(gonvex_store::token_hash("gvx_session_receipt_sdk_bob")).bind(expires).execute(&self.control_pool).await.unwrap();
        self.tenant_exec("INSERT INTO members(id,account_id,status,role,permissions) VALUES ('member','account','active','manager','{}'); INSERT INTO members(id,account_id,status,role,permissions) VALUES ('bob-member','bob','active','manager','{}')").await;
        let admitted = self
            .control
            .authenticate_session(
                "gvx_session_receipt_sdk_fixture",
                Some("project"),
                Some("tenant"),
            )
            .await
            .unwrap();
        assert_eq!(admitted.identity.account.id, "account");
        assert_eq!(admitted.member.id, "member");
    }

    /// A second tenant in its own SQLite file (replaces CREATE DATABASE).
    async fn enable_sdk_tenant_b(&self) -> SqlitePool {
        let route = TenantRoute {
            project_id: "project".into(),
            tenant_id: "tenant-b".into(),
            database_url: gonvex_store::database_url(&self.root.join("tenant-b.db")),
        };
        self.control
            .clone()
            .provision_tenant_database(
                route.clone(),
                vec![SqlMigration::new(
                    "0001_receipt_records.sql".into(),
                    MigrationScope::Tenant,
                    false,
                    "CREATE TABLE receipt_records(id TEXT PRIMARY KEY,value INTEGER NOT NULL)"
                        .into(),
                )],
            )
            .await
            .unwrap();
        self.control
            .ensure_reducer_idempotency(&route)
            .await
            .unwrap();
        self.tenant_exec_on(&route, "INSERT INTO members(id,account_id,status,role,permissions) VALUES ('member-b','account','active','manager','{}'); INSERT INTO receipt_records(id,value) VALUES ('tenant-b-private',73)").await;
        // Independent databases can have either numeric clock ordering. Give
        // B a higher counter through real commits so an incorrect old-owner
        // acknowledgement cannot hide behind B's initially smaller revision.
        for index in 0..100 {
            self.tenant_exec_on(
                &route,
                &format!(
                    "UPDATE receipt_records SET value={} WHERE id='tenant-b-private'",
                    72 + index % 2
                ),
            )
            .await;
        }
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,status,database_url) VALUES ('tenant-b','project','tenant-b','Isolated B','active',$1)")
            .bind(&route.database_url).execute(&self.control_pool).await.unwrap();
        let session = self
            .control
            .authenticate_session(
                "gvx_session_receipt_sdk_fixture",
                Some("project"),
                Some("tenant-b"),
            )
            .await
            .unwrap();
        assert_eq!(session.route.database_url, route.database_url);
        assert_eq!(session.member.id, "member-b");
        self.runtime
            .inner
            .pools
            .reader(&route.database_url)
            .await
            .unwrap()
    }

    async fn close(self) {
        self.runtime.shutdown().await;
        drop(self._data);
    }

    async fn sync_client_contract(&self, version: u64) -> serde_json::Value {
        let (write, edit, column) = if version == 1 {
            ("write", "edit", "value")
        } else {
            assert_eq!(version, 2);
            ("writeV2", "editV2", "count")
        };
        let code = format!(
            "export async function {write}(ctx,args) {{ await ctx.db.insert('receipt_records',{{id:args.id,{column}:args.{column}}}); return args; }}\nexport async function {edit}(ctx,args) {{ await ctx.db.update('receipt_records',args.id,{{{column}:args.{column}}}); return args; }}\nexport function all() {{ throw Error('replica query must use SQL'); }}"
        );
        let mut functions = serde_json::json!({});
        for path in [write, edit] {
            functions[path] = serde_json::json!({"kind":"reducer","public":false,"internal":false,
                "interactive":false,"classification":"system","handler":path,"file":"upgrade.js",
                "delivery":"oneShot","args":{"kind":"any"},"result":{"kind":"any"}});
        }
        functions["all"] = serde_json::json!({"kind":"query","public":false,"internal":false,
            "interactive":false,"classification":"system","handler":"all","file":"upgrade.js",
            "delivery":"replica","args":{"kind":"any"},"result":{"kind":"any"},
            "replica":{"table":"receipt_records","key":"id","columns":["id",column],"maxRows":100}});
        let mut files = serde_json::json!({"client-contract.json":base64::engine::general_purpose::STANDARD.encode(
            serde_json::to_vec(&serde_json::json!({"version":version,"offlineMaxAgeMs":null})).unwrap())});
        if version == 2 {
            files["migrations/0001_rename_value.sql"] = serde_json::json!(base64::engine::general_purpose::STANDARD.encode(
                "-- gonvex:scope tenant\nALTER TABLE receipt_records RENAME COLUMN value TO count;\n"));
        }
        let mut module = serde_json::json!({"generation":version,"language":"typescript","entrypoint":"upgrade.js",
            "files":files,"visibility":{"receipt_records":{"table":"receipt_records","key":"id","where":{"operator":"public"}}},
            "crons":[],"functions":functions,"javascript":{"path":"upgrade.js",
            "hash":format!("{:x}",Sha256::digest(code.as_bytes())),"code":base64::engine::general_purpose::STANDARD.encode(&code)}});
        module["hash"] = serde_json::json!(crate::modules::canonical_artifact_hash(
            module.as_object().unwrap()
        )
        .unwrap());
        let mut manifest = serde_json::json!({"project":"project","module":module});
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer receipt-upgrade-sync".parse().unwrap(),
        );
        let result = apply_dev_sync(&self.runtime, &headers, &mut manifest)
            .await
            .unwrap();
        assert_eq!(result["runtimeReloaded"], true);
        let installed = self.runtime.inner.modules.project("project").await.unwrap();
        assert_eq!(installed.client_contract, Some(version));
        serde_json::json!({"clientContract":version,"generation":installed.generation,
            "artifactHash":installed.artifact_hash,"functions":installed.functions.keys().collect::<Vec<_>>()})
    }
}

#[tokio::test]
#[ignore = "requires the module-upgrade browser coordinator"]
async fn authenticated_sdk_module_upgrade_recovery() {
    let script = std::env::var("GONVEX_SDK_MODULE_UPGRADE_SCRIPT")
        .expect("module upgrade requires its coordinator");
    let output_dir = std::env::var("GONVEX_SDK_RECEIPT_OUTPUT").unwrap();
    let f = Fixture::create(true)
        .await
        .expect("module upgrade requires native fixtures");
    f.enable_sdk_session().await;
    sqlx::query(
        "UPDATE gonvex_runtime_projects SET project_key='receipt-upgrade-sync' WHERE id='project'",
    )
    .execute(&f.control_pool)
    .await
    .unwrap();
    f.tenant_exec("INSERT INTO receipt_records(id,value) VALUES ('confirmed-before-upgrade',11)")
        .await;
    // Business-effect journal written by triggers in the same transaction as
    // each effect (replaces the plpgsql trigger function).
    f.ddl("CREATE TABLE upgrade_effects(position INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT NOT NULL); CREATE TRIGGER upgrade_effect_insert AFTER INSERT ON receipt_records BEGIN INSERT INTO upgrade_effects(operation) VALUES ('INSERT'); END; CREATE TRIGGER upgrade_effect_update AFTER UPDATE ON receipt_records BEGIN INSERT INTO upgrade_effects(operation) VALUES ('UPDATE'); END;")
        .await;
    let initial_module = f.sync_client_contract(1).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = f.runtime.router();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut command = tokio::process::Command::new("node");
    command
        .arg(script)
        .arg("--url")
        .arg(format!("ws://{address}/ws"))
        .arg("--output")
        .arg(&output_dir)
        .kill_on_drop(true);
    let directory = std::path::PathBuf::from(&output_dir);
    let mut replacement = None;
    let output = tokio::time::timeout(Duration::from_secs(90), async {
        let output = command.output();
        tokio::pin!(output);
        loop {
            tokio::select! {
                result = &mut output => break result,
                _ = tokio::time::sleep(Duration::from_millis(25)) => {
                    if replacement.is_some() || !tokio::fs::try_exists(directory.join("replace-request.json")).await.unwrap() { continue; }
                    let request: serde_json::Value = serde_json::from_slice(&tokio::fs::read(directory.join("replace-request.json")).await.unwrap()).unwrap();
                    let first = request["keys"][0].as_str().unwrap();
                    let second = request["keys"][1].as_str().unwrap();
                    assert_ne!(first, second);
                    let effects: Vec<String> = sqlx::query_scalar("SELECT operation FROM upgrade_effects ORDER BY position")
                        .fetch_all(&f.reader).await.unwrap();
                    assert_eq!(effects, ["INSERT"], "second intent executed before module replacement");
                    let receipts: Vec<String> = sqlx::query_scalar("SELECT path FROM _gonvex_reducer_idempotency WHERE subject='account' ORDER BY path")
                        .fetch_all(&f.reader).await.unwrap();
                    assert_eq!(receipts, ["write"]);
                    let module = f.sync_client_contract(2).await;
                    assert_ne!(module["artifactHash"], initial_module["artifactHash"]);
                    assert!(module["generation"].as_u64().unwrap() > initial_module["generation"].as_u64().unwrap());
                    let value: i64 = sqlx::query_scalar("SELECT count FROM receipt_records WHERE id='upgrade-intent'")
                        .fetch_one(&f.reader).await.unwrap();
                    assert_eq!(value, 7);
                    let key: String = sqlx::query_scalar("SELECT idempotency_key FROM _gonvex_reducer_idempotency WHERE subject='account'")
                        .fetch_one(&f.reader).await.unwrap();
                    assert_eq!(key, first);
                    let witness = serde_json::json!({"before":initial_module,"after":module,"effectsBeforeReplacement":effects,
                        "receiptPathsBeforeReplacement":receipts,"committedKey":key,"renamedColumnValue":value});
                    tokio::fs::write(directory.join("replaced.tmp"),serde_json::to_vec(&witness).unwrap()).await.unwrap();
                    tokio::fs::rename(directory.join("replaced.tmp"),directory.join("replaced.json")).await.unwrap();
                    replacement = Some(witness);
                }
            }
        }
    }).await;
    let successful = matches!(&output, Ok(Ok(child)) if child.status.success());
    let effects: Vec<String> = if successful {
        sqlx::query_scalar("SELECT operation FROM upgrade_effects ORDER BY position")
            .fetch_all(&f.reader)
            .await
            .unwrap()
    } else {
        Vec::new()
    };
    let receipts: Vec<String> = if successful {
        sqlx::query_scalar(
            "SELECT path FROM _gonvex_reducer_idempotency WHERE subject='account' ORDER BY path",
        )
        .fetch_all(&f.reader)
        .await
        .unwrap()
    } else {
        Vec::new()
    };
    server.abort();
    f.close().await;
    let output = output.expect("module upgrade browser timed out").unwrap();
    assert!(
        output.status.success(),
        "module upgrade failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(replacement.is_some(), "runtime module was never replaced");
    assert_eq!(
        effects,
        ["INSERT", "UPDATE"],
        "module upgrade repeated or reordered business effects"
    );
    assert_eq!(
        receipts,
        ["edit", "write"],
        "module upgrade changed receipt identity"
    );
}

#[tokio::test]
#[ignore = "requires the authenticated SDK/storage coordinator and built packages"]
async fn authenticated_sdk_receipt_replica_composition() {
    let script = std::env::var("GONVEX_SDK_RECEIPT_SCRIPT")
        .expect("authenticated SDK receipt test requires its coordinator");
    let output_dir = std::env::var("GONVEX_SDK_RECEIPT_OUTPUT")
        .expect("authenticated SDK receipt test requires an evidence directory");
    let f = Fixture::create(true)
        .await
        .expect("authenticated SDK receipt test requires native fixtures");
    f.enable_sdk_session().await;
    let tenant_b = f.enable_sdk_tenant_b().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = f.runtime.router();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut command = tokio::process::Command::new("node");
    command
        .arg(script)
        .arg("--url")
        .arg(format!("ws://{address}/ws"))
        .arg("--output")
        .arg(&output_dir)
        .kill_on_drop(true);
    let coordination = std::path::PathBuf::from(&output_dir).join("browser-upgrade");
    let mut rollback_key = None;
    let mut concurrency = SDKConcurrencyGate::default();
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        let output = command.output();
        tokio::pin!(output);
        loop {
            tokio::select! {
                result = &mut output => break result,
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    concurrency.poll(&f, std::path::Path::new(&output_dir)).await;
                    if rollback_key.is_some() { continue; }
                    let request = coordination.join("rollback-request.json");
                    if !tokio::fs::try_exists(&request).await.unwrap() { continue; }
                    let request: serde_json::Value = serde_json::from_slice(&tokio::fs::read(request).await.unwrap()).unwrap();
                    assert_eq!(request["rowId"], "sdk-upgrade-rollback");
                    let key = request["key"].as_str().unwrap().to_owned();
                    assert!(!key.is_empty());
                    if std::env::var_os("GONVEX_SDK_ROLLBACK_DAMAGE").is_some() {
                        f.tenant_exec("INSERT INTO receipt_records(id,value) VALUES ('sdk-upgrade-rollback',7)").await;
                    }
                    // One WAL snapshot, after the SDK recorded the actual reducer
                    // failure and before the browser starts its upgrade/retry.
                    let row = sqlx::query("SELECT (SELECT count(*) FROM receipt_records WHERE id=$1) AS business, (SELECT count(*) FROM _gonvex_reducer_idempotency WHERE subject='account' AND idempotency_key=$2) AS receipts")
                        .bind("sdk-upgrade-rollback").bind(&key).fetch_one(&f.reader).await.unwrap();
                    let witness = serde_json::json!({"business":row.get::<i64,_>("business"),"receipts":row.get::<i64,_>("receipts")});
                    tokio::fs::write(coordination.join("rollback-observed.tmp"), serde_json::to_vec(&witness).unwrap()).await.unwrap();
                    tokio::fs::rename(coordination.join("rollback-observed.tmp"),coordination.join("rollback-observed.json")).await.unwrap();
                    rollback_key = Some(key);
                }
            }
        }
    }).await;
    concurrency.release().await;
    let renamed_receipt = if let Some(key) = rollback_key.as_ref() {
        sqlx::query_scalar::<_, String>("SELECT path FROM _gonvex_reducer_idempotency WHERE subject='account' AND idempotency_key=$1")
            .bind(key).fetch_optional(&f.reader).await.unwrap()
    } else {
        None
    };
    let business = f.count("receipt_records WHERE value=7").await;
    let receipts = f
        .count("_gonvex_reducer_idempotency WHERE subject='account'")
        .await;
    let bob_receipts = f
        .count("_gonvex_reducer_idempotency WHERE subject='bob'")
        .await;
    let tenant_b_business: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM receipt_records WHERE id='sdk-tenant-bob' AND value=7",
    )
    .fetch_one(&tenant_b)
    .await
    .unwrap();
    let tenant_b_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM _gonvex_reducer_idempotency WHERE subject='account'",
    )
    .fetch_one(&tenant_b)
    .await
    .unwrap();
    let leaked_into_b: i64 =
        sqlx::query_scalar("SELECT count(*) FROM receipt_records WHERE id='sdk-tenant-alice'")
            .fetch_one(&tenant_b)
            .await
            .unwrap();
    let leaked_into_a = f
        .count("receipt_records WHERE id IN ('sdk-tenant-bob','tenant-b-private')")
        .await;
    // Each competing intent took effect exactly once, from an executed claim
    // (a stored result), after the gate released the single writer.
    let mut concurrent = Vec::new();
    for mode in ["commit", "rollback"] {
        let row = format!("sdk-concurrent-{mode}");
        let business = f
            .count(&format!("receipt_records WHERE id='{row}' AND value=7"))
            .await;
        let executed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM _gonvex_reducer_idempotency WHERE subject='account' AND path='write' AND result LIKE '%' || $1 || '%'",
        )
        .bind(&row)
        .fetch_one(&f.reader)
        .await
        .unwrap();
        concurrent.push((mode, business, executed));
    }
    server.abort();
    f.close().await;
    let output = result
        .expect("authenticated SDK composition exceeded its fixture deadline")
        .unwrap();
    assert!(
        output.status.success(),
        "SDK composition failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        business, 12,
        "SDK composition repeated or lost committed business effects"
    );
    assert_eq!(
        receipts, 11,
        "SDK composition changed the authenticated receipt namespace"
    );
    assert!(
        rollback_key.is_some(),
        "SDK upgrade omitted the real rollback witness"
    );
    assert_eq!(
        bob_receipts, 1,
        "account switch repeated or misattributed Bob receipts"
    );
    assert_eq!(
        tenant_b_business, 1,
        "replacement tenant business effect missing or repeated"
    );
    assert_eq!(
        tenant_b_receipts, 1,
        "replacement tenant receipt namespace changed"
    );
    assert_eq!(
        leaked_into_b, 0,
        "original tenant business effect leaked into B"
    );
    assert_eq!(leaked_into_a, 0, "replacement tenant data leaked into A");
    assert_eq!(
        concurrency.witnesses.len(),
        2,
        "both concurrent SDK outcomes require witnessed overlap"
    );
    for (mode, business, executed) in concurrent {
        assert_eq!(
            business, 1,
            "{mode}: competing claims must take effect exactly once"
        );
        assert_eq!(
            executed, 1,
            "{mode}: exactly one executed receipt must remain"
        );
    }
    assert_eq!(
        renamed_receipt.as_deref(),
        Some("abort"),
        "upgrade changed the original receipt namespace"
    );
}

#[tokio::test]
async fn receipt_initialization_preserves_legacy_replay() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let first = f
        .call("original", "legacy-key", "write", "legacy-row")
        .await;
    f.ddl("ALTER TABLE _gonvex_reducer_idempotency RENAME TO _gonvex_mutation_idempotency")
        .await;
    assert!(!f.table_exists("_gonvex_reducer_idempotency").await);
    let replay = f
        .runtime
        .execute_tenant_reducer(
            &f.session,
            "new-transport",
            Some("legacy-key"),
            "write",
            serde_json::json!({"id":"legacy-row","value":7}),
        )
        .await;
    assert!(
        replay.is_ok(),
        "legacy receipt was lost during initialization: {:?}",
        replay.as_ref().err()
    );
    let replay = replay.unwrap();
    assert_eq!(replay.value, first.value);
    assert!(replay.committed_revision.unwrap() >= first.committed_revision.unwrap());
    assert!(!f.table_exists("_gonvex_mutation_idempotency").await);
    assert!(f.table_exists("_gonvex_reducer_idempotency").await);
    assert_eq!(f.count("receipt_records").await, 1);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    assert_eq!(f.integrity().await, "ok");
    f.close().await;
}

#[tokio::test]
async fn receipt_initialization_refuses_ambiguous_histories() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.call("original", "known-key", "write", "known-row").await;
    f.ddl("CREATE TABLE _gonvex_mutation_idempotency (subject TEXT NOT NULL DEFAULT '', idempotency_key TEXT NOT NULL, path TEXT NOT NULL, result JSON, created_at INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (subject, idempotency_key))")
        .await;
    let result = f
        .runtime
        .execute_tenant_reducer(
            &f.session,
            "ambiguous-call",
            Some("new-key"),
            "write",
            serde_json::json!({"id":"must-not-execute","value":7}),
        )
        .await;
    let Err(error) = result else {
        panic!("ambiguous receipt histories allowed business execution");
    };
    assert!(error.to_string().contains("ambiguous"));
    assert_eq!(
        error.reducer_error_class(),
        crate::error_class::ReducerErrorClass::Transient,
        "receipt recovery must retain the intent as infrastructure failure"
    );
    assert_eq!(f.count("receipt_records").await, 1);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    assert_eq!(f.count("_gonvex_mutation_idempotency").await, 0);
    f.close().await;
}

#[tokio::test]
async fn receipt_initialization_repairs_missing_index_and_observes_schema_reset() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let indexed = "SELECT EXISTS(SELECT 1 FROM pragma_index_list('_gonvex_reducer_idempotency') WHERE name='gonvex_reducer_idempotency_created_at')";
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    f.ddl("DROP INDEX gonvex_reducer_idempotency_created_at")
        .await;
    assert!(!sqlx::query_scalar::<_, bool>(indexed)
        .fetch_one(&f.reader)
        .await
        .unwrap());
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>(indexed)
            .fetch_one(&f.reader)
            .await
            .unwrap(),
        "missing receipt index was not repaired"
    );
    // The repaired index is consistent with its table.
    assert_eq!(f.integrity().await, "ok");
    // No process cache may outlive a schema reset: the catalog is inspected
    // on every call.
    f.ddl("DROP TABLE _gonvex_reducer_idempotency").await;
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    assert!(f.table_exists("_gonvex_reducer_idempotency").await);
    assert!(sqlx::query_scalar::<_, bool>(indexed)
        .fetch_one(&f.reader)
        .await
        .unwrap());
    f.call("after-reset", "fresh-key", "write", "fresh-row")
        .await;
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    f.close().await;
}

/// Polls until `condition` holds, then requires it to keep holding for a
/// further settle period (a blocked waiter stays blocked; a sample alone would
/// not distinguish waiting from passing through).
async fn witness_sustained<F, Fut>(label: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{label}: never observed"));
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(condition().await, "{label}: not sustained");
    }
}

#[tokio::test]
async fn receipt_initialization_serializes_cold_callers() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.ddl("DROP TABLE IF EXISTS _gonvex_reducer_idempotency")
        .await;
    let pools = f.runtime.inner.pools.clone();
    let url = f.session.route.database_url.clone();
    assert_eq!(pools.snapshot().await.admission_active, 0);
    // Own the keyed schema lock that serializes cold initialization (the
    // in-process replacement for the advisory lock).
    let gate = pools.lock(&url, SCHEMA_LOCK).await.unwrap();
    let mut initializers = Vec::new();
    for _ in 0..2 {
        let control = f.control.clone();
        let route = f.session.route.clone();
        initializers.push(tokio::spawn(async move {
            control.ensure_reducer_idempotency(&route).await
        }));
    }
    // Both cold callers are admitted, saw the missing table, and wait on the
    // schema lock: neither finishes and neither creates the table.
    witness_sustained("both cold initializers waiting on the schema lock", || {
        let pools = pools.clone();
        let finished = initializers.iter().any(|task| task.is_finished());
        async move { !finished && pools.snapshot().await.admission_active == 2 }
    })
    .await;
    assert!(!f.table_exists("_gonvex_reducer_idempotency").await);
    drop(gate);
    for initializer in initializers {
        tokio::time::timeout(Duration::from_secs(5), initializer)
            .await
            .expect("serialized initializer did not finish after the lock was released")
            .unwrap()
            .unwrap();
    }
    assert_eq!(pools.snapshot().await.admission_active, 0);
    assert!(f.table_exists("_gonvex_reducer_idempotency").await);
    assert_eq!(f.integrity().await, "ok");
    // Two callers with one key: exactly one executes, the other replays.
    let (first, second) = tokio::join!(
        f.call("cold-first", "cold-key", "write", "cold-row"),
        f.call("cold-second", "cold-key", "write", "cold-row"),
    );
    assert_eq!(first.value, second.value);
    assert_eq!(f.count("receipt_records").await, 1);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    f.close().await;
}

#[tokio::test]
async fn receipt_initialization_cancellation_releases_lock_and_admission() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.ddl("DROP TABLE IF EXISTS _gonvex_reducer_idempotency")
        .await;
    let pools = f.runtime.inner.pools.clone();
    let url = f.session.route.database_url.clone();
    assert_eq!(pools.snapshot().await.admission_active, 0);
    let gate = pools.lock(&url, SCHEMA_LOCK).await.unwrap();
    let control = f.control.clone();
    let route = f.session.route.clone();
    let initializer = tokio::spawn(async move { control.ensure_reducer_idempotency(&route).await });
    witness_sustained(
        "initializer admitted and waiting on the schema lock",
        || {
            let pools = pools.clone();
            let finished = initializer.is_finished();
            async move { !finished && pools.snapshot().await.admission_active == 1 }
        },
    )
    .await;
    assert!(!f.table_exists("_gonvex_reducer_idempotency").await);
    initializer.abort();
    assert!(initializer.await.unwrap_err().is_cancelled());
    assert_eq!(
        pools.snapshot().await.admission_active,
        0,
        "cancelled initialization leaked query admission"
    );
    drop(gate);
    tokio::time::timeout(
        Duration::from_secs(5),
        f.control.ensure_reducer_idempotency(&f.session.route),
    )
    .await
    .expect("cancelled initializer retained the schema lock or the writer")
    .unwrap();
    assert_eq!(pools.snapshot().await.admission_active, 0);
    let probe = tokio::time::timeout(Duration::from_millis(20), pools.lock(&url, SCHEMA_LOCK))
        .await
        .expect("schema lock survived cancellation and recovery")
        .unwrap();
    drop(probe);
    // The single writer connection was returned, not leaked by the cancelled
    // or the recovering initializer (sqlx returns connections asynchronously).
    let writer = tokio::time::timeout(Duration::from_secs(1), f.pool.acquire())
        .await
        .expect("cancelled initializer retained the single writer connection")
        .unwrap();
    drop(writer);
    // Admission is runtime-wide. Fill it exactly: a cancelled initializer
    // must have returned exactly its own permit, never two.
    let capacity = pools.snapshot().await.admission_limit;
    let mut permits = Vec::new();
    for _ in 0..capacity {
        permits.push(
            tokio::time::timeout(Duration::from_secs(1), pools.admit(&url))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(pools.snapshot().await.admission_active, capacity);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), pools.admit(&url))
            .await
            .is_err(),
        "cancellation released query admission twice"
    );
    drop(permits);
    assert_eq!(pools.snapshot().await.admission_active, 0);
    f.call("after-cancellation", "cancel-key", "write", "cancel-row")
        .await;
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    f.close().await;
}

#[tokio::test]
async fn receipt_lookup_progresses_while_snapshot_preparations_own_pool_connections() {
    use futures_util::FutureExt as _;
    let Some(f) = Fixture::create(true).await else {
        return;
    };
    f.enable_sdk_session().await;
    let committed: i64 =
        sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton=1")
            .fetch_one(&f.reader)
            .await
            .unwrap();
    // The PostgreSQL gate was an exclusive lock on the clock. Hold its SQLite
    // analog for the whole test: the single writer inside a transaction that
    // advanced the clock without committing. Snapshot readers never wait on it.
    let mut gate = f.pool.begin().await.unwrap();
    sqlx::query("UPDATE _gonvex_sync_clock SET revision=revision+1000 WHERE singleton=1")
        .execute(&mut *gate)
        .await
        .unwrap();
    let readers = f.runtime.inner.config.database_max_connections as usize;
    assert_eq!(readers, 2, "fixture expects two reader connections");
    let owned = Arc::new(tokio::sync::Barrier::new(readers + 1));
    let (release, released) = tokio::sync::watch::channel(false);
    let mut queue = crate::replica_open_queue::ReplicaOpenQueue::default();
    for id in ["opening-first", "opening-second"] {
        let runtime = f.runtime.clone();
        let session = f.session.clone();
        let reader = f.reader.clone();
        let owned = owned.clone();
        let mut released = released.clone();
        let request: ReplicaOpenRequest = serde_json::from_value(serde_json::json!({
            "id":id,"path":"all","args":{}
        }))
        .unwrap();
        queue
            .push(
                id.into(),
                128,
                false,
                async move {
                    // Each preparation owns one reader connection in an open
                    // snapshot transaction until the socket releases it.
                    let mut snapshot = reader.begin().await.unwrap();
                    let seen: i64 = sqlx::query_scalar(
                        "SELECT revision FROM _gonvex_sync_clock WHERE singleton=1",
                    )
                    .fetch_one(&mut *snapshot)
                    .await
                    .unwrap();
                    owned.wait().await;
                    released.wait_for(|released| *released).await.unwrap();
                    drop(snapshot);
                    let result = runtime
                        .open_replica_negotiated(&session, request, false)
                        .await;
                    (seen, result)
                }
                .boxed(),
            )
            .unwrap();
    }
    // Poll exactly as the socket select loop does until both preparations
    // own every reader connection.
    let witnessed = {
        let pending = queue.next();
        tokio::pin!(pending);
        tokio::select! {
            _ = &mut pending => panic!("snapshot gate failed to hold preparation"),
            witnessed = tokio::time::timeout(Duration::from_secs(5), owned.wait()) => witnessed,
        }
    };
    assert!(
        witnessed.is_ok(),
        "both snapshot pool owners were not witnessed"
    );
    assert_eq!(f.reader.size() as usize, readers);
    assert_eq!(f.reader.num_idle(), 0);
    assert!(
        f.reader.try_acquire().is_none(),
        "a reader connection was available while both preparations owned the pool"
    );
    // Release the preparations but do not poll the queue: the inline lookup
    // needs a reader connection that only the spawned preparations can return.
    release.send(true).unwrap();
    let lookup = tokio::time::timeout(
        Duration::from_secs(5),
        f.runtime.execute_tenant_query(
            &f.session,
            execution::REDUCER_RECEIPT_QUERY,
            serde_json::json!({"idempotencyKey":"missing","receiptPath":"write"}),
        ),
    )
    .await;
    assert!(
        lookup.is_ok(),
        "inline receipt lookup starved behind snapshot preparations that own the reader pool"
    );
    assert_eq!(lookup.unwrap().unwrap(), serde_json::json!({"found":false}));
    // The real snapshot preparations complete while the writer is still held,
    // and observe only the committed clock.
    for _ in 0..2 {
        let (seen, result) = tokio::time::timeout(Duration::from_secs(5), queue.next())
            .await
            .expect("snapshot preparation waited on the held writer")
            .unwrap();
        assert_eq!(seen, committed);
        let subscription = result.unwrap().subscription.unwrap();
        assert_eq!(subscription.cursor.revision as i64, committed);
    }
    assert!(queue.is_empty());
    gate.rollback().await.unwrap();
    queue.clear();
    f.close().await;
}

#[tokio::test]
async fn accepted_receipt_lookup_never_claims_or_executes_a_reducer() {
    let Some(f) = Fixture::create(true).await else {
        return;
    };
    f.enable_sdk_session().await;
    let session = f
        .control
        .authenticate_session(
            "gvx_session_receipt_sdk_fixture",
            Some("project"),
            Some("tenant"),
        )
        .await
        .unwrap();
    let lookup =
        |key: &str, path: &str| serde_json::json!({"idempotencyKey":key,"receiptPath":path});
    assert_eq!(
        f.runtime
            .execute_tenant_query(
                &session,
                execution::REDUCER_RECEIPT_QUERY,
                lookup("missing", "write")
            )
            .await
            .unwrap(),
        serde_json::json!({"found":false})
    );
    assert!(!f.table_exists("_gonvex_reducer_idempotency").await);
    let committed = f.call("request", "known", "write", "original").await;
    let status = f
        .runtime
        .execute_tenant_query(
            &session,
            execution::REDUCER_RECEIPT_QUERY,
            lookup("known", "write"),
        )
        .await
        .unwrap();
    assert_eq!(status["found"], true);
    assert!(
        status.get("result").is_none(),
        "receipt lookup exposed a cached business result"
    );
    assert!(
        status["cursor"]["revision"].as_u64().unwrap() >= committed.committed_revision.unwrap()
    );
    assert!(!status["cursor"]["epoch"].as_str().unwrap().is_empty());
    assert!(f
        .runtime
        .execute_tenant_query(
            &session,
            execution::REDUCER_RECEIPT_QUERY,
            lookup("known", "noop")
        )
        .await
        .is_err());
    let mut anonymous = session.clone();
    anonymous.identity.account.id.clear();
    assert!(f
        .runtime
        .execute_tenant_query(
            &anonymous,
            execution::REDUCER_RECEIPT_QUERY,
            lookup("known", "write")
        )
        .await
        .is_err());
    let mut other = session.clone();
    other.identity.account.id = "other-account".into();
    other.member.id = "other-member".into();
    other.member.account_id = "other-account".into();
    f.tenant_exec("INSERT INTO members(id,account_id,status,role,permissions) VALUES ('other-member','other-account','active','manager','{}')").await;
    assert_eq!(
        f.runtime
            .execute_tenant_query(
                &other,
                execution::REDUCER_RECEIPT_QUERY,
                lookup("known", "write")
            )
            .await
            .unwrap(),
        serde_json::json!({"found":false})
    );
    // Looking up either missing or existing keys cannot append receipts or effects.
    assert_eq!(
        f.runtime
            .execute_tenant_query(
                &session,
                execution::REDUCER_RECEIPT_QUERY,
                lookup("still-missing", "write")
            )
            .await
            .unwrap(),
        serde_json::json!({"found":false})
    );
    assert_eq!(f.count("receipt_records").await, 1);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    // Reading historical receipts must neither migrate nor silently choose
    // between conflicting current and legacy journals.
    f.ddl("ALTER TABLE _gonvex_reducer_idempotency RENAME TO _gonvex_mutation_idempotency")
        .await;
    assert_eq!(
        f.runtime
            .execute_tenant_query(
                &session,
                execution::REDUCER_RECEIPT_QUERY,
                lookup("known", "write")
            )
            .await
            .unwrap()["found"],
        true
    );
    assert!(!f.table_exists("_gonvex_reducer_idempotency").await);
    f.ddl("CREATE TABLE _gonvex_reducer_idempotency (subject TEXT NOT NULL DEFAULT '', idempotency_key TEXT NOT NULL, path TEXT NOT NULL, result JSON, created_at INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (subject, idempotency_key))").await;
    assert!(f
        .runtime
        .execute_tenant_query(
            &session,
            execution::REDUCER_RECEIPT_QUERY,
            lookup("known", "write")
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("ambiguous"));
    f.ddl("DROP TABLE _gonvex_reducer_idempotency; ALTER TABLE _gonvex_mutation_idempotency RENAME TO _gonvex_reducer_idempotency").await;
    f.tenant_exec("UPDATE members SET status='inactive' WHERE id='member'")
        .await;
    assert!(f
        .runtime
        .execute_tenant_query(
            &session,
            execution::REDUCER_RECEIPT_QUERY,
            lookup("known", "write")
        )
        .await
        .is_err());
    f.close().await;
}

async fn assert_replay_barrier(prune: bool, replace_request_id: bool) {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let first = f
        .call("first-request", "same-intent", "write", "original")
        .await;
    assert!(first.committed_revision.is_some());
    let later = f
        .call("later-request", "other-intent", "write", "later")
        .await;
    assert!(later.committed_revision > first.committed_revision);
    if prune {
        // 31 days in milliseconds.
        f.ddl("UPDATE _gonvex_sync_changes SET created_at=created_at-2678400000")
            .await;
        crate::change_feed::prune_retained(&f.runtime.inner.pools, &f.session.route)
            .await
            .unwrap();
        assert_eq!(
            f.control
                .command_revision(&f.session.route, "first-request")
                .await
                .unwrap(),
            None
        );
        assert_eq!(f.count("_gonvex_sync_changes").await, 0);
    }
    let replay = f
        .call(
            if replace_request_id {
                "new-transport-id"
            } else {
                "first-request"
            },
            "same-intent",
            "write",
            "original",
        )
        .await;
    assert_eq!(replay.value, first.value);
    assert!(
        replay.committed_revision >= later.committed_revision,
        "replay lost its sufficient current synchronization barrier: first={:?}, later={:?}, replay={:?}",
        first.committed_revision,
        later.committed_revision,
        replay.committed_revision
    );
    assert_eq!(f.count("receipt_records").await, 2);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 2);
    f.close().await;
}

#[tokio::test]
async fn replay_retains_barrier_with_a_new_transport_id() {
    assert_replay_barrier(false, true).await;
}
#[tokio::test]
async fn replay_retains_barrier_after_change_feed_pruning() {
    assert_replay_barrier(true, false).await;
}

#[tokio::test]
async fn replay_does_not_succeed_when_its_clock_cannot_be_read() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.call("first-request", "same-intent", "write", "original")
        .await;
    f.ddl("DROP TABLE _gonvex_sync_clock").await;
    assert!(f
        .runtime
        .execute_tenant_reducer(
            &f.session,
            "first-request",
            Some("same-intent"),
            "write",
            serde_json::json!({"id":"original","value":7})
        )
        .await
        .is_err());
    assert_eq!(f.count("receipt_records").await, 1);
    f.close().await;
}

#[tokio::test]
async fn failed_reducer_rolls_back_business_write_and_receipt_together() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    assert!(f
        .runtime
        .execute_tenant_reducer(
            &f.session,
            "failed",
            Some("retry"),
            "abort",
            serde_json::json!({"id":"rolled-back","value":7})
        )
        .await
        .is_err());
    for table in ["receipt_records", "_gonvex_reducer_idempotency"] {
        assert_eq!(f.count(table).await, 0);
    }
    // No captured change row survives the rollback for a later commit to adopt.
    assert_eq!(f.count("_gonvex_sync_changes").await, 0);
    f.call("retry-after-rollback", "retry", "write", "committed")
        .await;
    assert_eq!(f.count("receipt_records").await, 1);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    f.close().await;
}

#[tokio::test]
async fn noop_receipt_replays_with_a_zero_revision_barrier() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let first = f.call("first", "noop-intent", "noop", "unused").await;
    let replay = f.call("new-request", "noop-intent", "noop", "unused").await;
    assert_eq!(first.value, replay.value);
    assert_eq!(replay.committed_revision, Some(0));
    f.close().await;
}

#[tokio::test]
async fn receipt_rejects_path_reuse_and_separates_account_subjects() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.call("first", "same-key", "write", "original").await;
    let mismatch = f
        .runtime
        .execute_tenant_reducer(
            &f.session,
            "wrong-path",
            Some("same-key"),
            "noop",
            serde_json::json!({}),
        )
        .await;
    let Err(error) = mismatch else {
        panic!("a receipt key was replayed for a different reducer path");
    };
    assert!(
        error.to_string().contains("refusing to replay"),
        "unexpected path-mismatch error: {error}"
    );
    let mut other = f.session.clone();
    other.identity.account.id = "other-account".into();
    other.member.account_id = "other-account".into();
    f.runtime
        .execute_tenant_reducer(
            &other,
            "other-request",
            Some("same-key"),
            "write",
            serde_json::json!({"id":"other-row","value":9}),
        )
        .await
        .unwrap();
    assert_eq!(f.count("receipt_records").await, 2);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 2);
    f.close().await;
}

// The owner holds the tenant's single writer inside an open transaction that
// has claimed the receipt and written its effect. The contender is admitted
// and waits for that writer; it cannot observe or race the uncommitted claim.
async fn competing_claim(owner_commits: bool) {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    let pools = f.runtime.inner.pools.clone();
    let mut owner = f
        .control
        .begin_tenant_transaction(&f.session.route, false)
        .await
        .unwrap();
    assert!(owner
        .claim_reducer("account", "competing", "write")
        .await
        .unwrap());
    sqlx::query("INSERT INTO receipt_records VALUES ('owner', 1)")
        .execute(&mut **owner.transaction())
        .await
        .unwrap();
    owner
        .store_reducer_result("account", "competing", &serde_json::json!({"owner":true}))
        .await
        .unwrap();
    // Uncommitted effects and the receipt are invisible to other connections.
    for table in ["receipt_records", "_gonvex_reducer_idempotency"] {
        assert_eq!(f.count(table).await, 0);
    }
    assert!(writer_owned(&f.pool));
    assert_eq!(pools.snapshot().await.admission_active, 1);
    let control = f.control.clone();
    let route = f.session.route.clone();
    let task = tokio::spawn(async move {
        let mut contender = control
            .begin_tenant_transaction(&route, false)
            .await
            .unwrap();
        let claimed = contender
            .claim_reducer("account", "competing", "write")
            .await
            .unwrap();
        if claimed {
            sqlx::query("INSERT INTO receipt_records VALUES ('contender', 2)")
                .execute(&mut **contender.transaction())
                .await
                .unwrap();
            contender
                .store_reducer_result("account", "competing", &serde_json::json!({"owner":false}))
                .await
                .unwrap();
        }
        contender.commit().await.unwrap();
        claimed
    });
    // The contender is admitted (its permit is held for its transaction) and
    // blocked on the single writer the owner holds.
    witness_sustained("contender waiting on the receipt owner's writer", || {
        let pools = pools.clone();
        let finished = task.is_finished();
        let owned = writer_owned(&f.pool);
        async move { !finished && owned && pools.snapshot().await.admission_active == 2 }
    })
    .await;
    if owner_commits {
        owner.commit().await.unwrap();
    } else {
        owner.rollback().await.unwrap();
    }
    let claimed = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed, !owner_commits);
    assert_eq!(f.count("receipt_records").await, 1);
    assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    let replay = f
        .control
        .replay_reducer_result(&f.session.route, "account", "competing", "write")
        .await
        .unwrap();
    assert_eq!(replay, serde_json::json!({"owner":owner_commits}));
    f.close().await;
}

#[tokio::test]
async fn competing_claim_replays_after_owner_commits() {
    competing_claim(true).await;
}

#[tokio::test]
async fn competing_claim_executes_after_owner_rolls_back() {
    competing_claim(false).await;
}

// Only the process-crash coordinator (verification/backend-receipts/
// check_restart.py) may run this test. It runs twice per case on one data
// directory: the `crash` phase stages a boundary and is SIGKILLed, the
// `recover` phase is a fresh process that reopens the same SQLite files.
#[tokio::test]
#[ignore = "requires the runtime process-crash coordinator"]
async fn receipt_recovery_after_process_crash() {
    let coordination = std::env::var("GONVEX_RECEIPT_CRASH_COORDINATION")
        .expect("receipt crash test requires its process-crash coordinator");
    let mode = std::env::var("GONVEX_RECEIPT_CRASH_MODE")
        .expect("receipt crash test requires an explicit commit boundary");
    assert!(matches!(mode.as_str(), "committed" | "uncommitted"));
    let phase = std::env::var("GONVEX_RECEIPT_CRASH_PHASE")
        .expect("receipt crash test requires an explicit phase");
    assert!(matches!(phase.as_str(), "crash" | "recover"));
    let root = PathBuf::from(
        std::env::var("GONVEX_TEST_DATA_DIR")
            .expect("receipt crash test requires a retained data directory"),
    );
    // A SIGKILLed owner must not retain the exclusive data-directory lock.
    let _owner = gonvex_store::DataDir::new(&root)
        .lock()
        .expect("data directory is still owned after the runtime process died");
    let f = Fixture::build(false, Some(root))
        .await
        .expect("receipt crash test requires native fixtures");
    let database = gonvex_store::database_path(&f.session.route.database_url);
    if phase == "crash" {
        f.control
            .ensure_reducer_idempotency(&f.session.route)
            .await
            .unwrap();
        let mut interrupted = None;
        if mode == "committed" {
            // The process dies after the effect and receipt commit. Discard
            // this result and retry later as if the network reply was lost.
            let _lost_reply = f
                .call("lost-reply", "crash-intent", "write", "original")
                .await;
            assert_eq!(f.count("receipt_records").await, 1);
            assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
        } else {
            let mut transaction = f
                .control
                .begin_tenant_transaction(&f.session.route, false)
                .await
                .unwrap();
            transaction
                .set_command_id("interrupted-request")
                .await
                .unwrap();
            assert!(transaction
                .claim_reducer("account", "crash-intent", "write")
                .await
                .unwrap());
            sqlx::query("INSERT INTO receipt_records VALUES ('uncommitted', 99)")
                .execute(&mut **transaction.transaction())
                .await
                .unwrap();
            transaction
                .store_reducer_result(
                    "account",
                    "crash-intent",
                    &serde_json::json!({"uncommitted":true}),
                )
                .await
                .unwrap();
            // Another connection observes neither side before the crash.
            for table in ["receipt_records", "_gonvex_reducer_idempotency"] {
                assert_eq!(f.count(table).await, 0);
            }
            interrupted = Some(transaction);
        }
        let marker = serde_json::json!({"mode":mode,"database":database});
        let mut file = std::fs::File::create(format!("{coordination}.ready.tmp")).unwrap();
        std::io::Write::write_all(&mut file, marker.to_string().as_bytes()).unwrap();
        file.sync_all().unwrap();
        std::fs::rename(
            format!("{coordination}.ready.tmp"),
            format!("{coordination}.ready"),
        )
        .unwrap();
        // Hold the open transaction (or the committed state) until SIGKILL.
        let _held = interrupted;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    // Fresh process on the same files: WAL recovery happened on open.
    assert_eq!(
        f.integrity().await,
        "ok",
        "crash left a corrupt tenant file"
    );
    if mode == "committed" {
        assert_eq!(
            f.count("receipt_records").await,
            1,
            "crash recovery changed committed business count"
        );
        assert_eq!(f.count("_gonvex_reducer_idempotency").await, 1);
    } else {
        for table in ["receipt_records", "_gonvex_reducer_idempotency"] {
            assert_eq!(
                f.count(table).await,
                0,
                "uncommitted {table} survived the crash"
            );
        }
    }
    let recovered = f
        .call(
            "new-transport-after-crash",
            "crash-intent",
            "write",
            "original",
        )
        .await;
    assert_eq!(
        recovered.value,
        serde_json::json!({"id":"original","value":7})
    );
    assert!(recovered.committed_revision.is_some());
    let replay = f
        .call(
            "second-transport-after-crash",
            "crash-intent",
            "write",
            "original",
        )
        .await;
    assert_eq!(replay.value, recovered.value);
    assert!(replay.committed_revision >= recovered.committed_revision);
    assert_eq!(
        f.count("receipt_records").await,
        1,
        "crash recovery changed committed business count"
    );
    assert_eq!(
        f.count("_gonvex_reducer_idempotency").await,
        1,
        "crash recovery changed committed receipt count"
    );
    let row = sqlx::query_as::<_, (String, i64)>("SELECT id, value FROM receipt_records")
        .fetch_one(&f.reader)
        .await
        .unwrap();
    assert_eq!(row, ("original".to_owned(), 7));
    assert_eq!(f.integrity().await, "ok");
    f.close().await;
}

/// Real socket loop + PostgreSQL: an unchanged A stays ready at 258, while
/// late/reopened B needs a new ordered W259 in a tenant with no further writes.
#[tokio::test]
async fn quiet_open_cohort_legacy_socket() {
    quiet_open_cohort_socket(false, false).await;
}
#[tokio::test]
async fn quiet_open_cohort_paged_socket() {
    quiet_open_cohort_socket(true, false).await;
}
#[tokio::test]
async fn quiet_open_cohort_native_socket() {
    quiet_open_cohort_socket(true, true).await;
}
async fn quiet_open_cohort_socket(paged: bool, native: bool) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as Wire;
    type Socket = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;
    async fn read(socket: &mut Socket) -> serde_json::Value {
        let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("quiet cohort never emitted its terminal watermark")
            .unwrap()
            .unwrap();
        serde_json::from_slice(&frame.into_data()).unwrap()
    }
    async fn send(socket: &mut Socket, value: serde_json::Value) {
        socket
            .send(Wire::Text(value.to_string().into()))
            .await
            .unwrap();
    }
    async fn cohort(socket: &mut Socket, id: &str, revision: u64, paged: bool, native: bool) {
        let mut ready = false;
        let mut last_page = false;
        loop {
            let frame = read(socket).await;
            match frame["type"].as_str().unwrap() {
                "replica.snapshotStart" => {
                    assert_eq!(frame["id"], id);
                }
                "replica.snapshotPage" => {
                    last_page = frame["last"].as_bool().unwrap();
                }
                "replica.snapshotEncodedPage" => {
                    last_page = frame["last"].as_bool().unwrap();
                    if last_page {
                        assert!(
                            tokio::time::timeout(Duration::from_millis(80), socket.next())
                                .await
                                .is_err(),
                            "ready/watermark overtook the native durable ACK"
                        );
                    }
                    send(
                        socket,
                        serde_json::json!({"type":"replica.snapshotAck","id":id,
                        "snapshotId":frame["snapshotId"],"sequence":frame["sequence"]}),
                    )
                    .await;
                }
                "replica.snapshot" => {
                    assert!(!native);
                }
                "replica.ready" => {
                    assert_eq!(frame["id"], id);
                    assert_eq!(frame["cursor"]["revision"], revision);
                    if paged {
                        assert!(last_page);
                    }
                    ready = true;
                }
                "replica.watermark" => {
                    assert!(ready, "cohort watermark overtook final ready");
                    assert_eq!(frame["revision"], revision);
                    break;
                }
                other => panic!("unexpected {other}: {frame}"),
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(80), socket.next())
                .await
                .is_err(),
            "idle cohort emitted a duplicate watermark"
        );
    }
    {
        let Some(f) = Fixture::create(true).await else {
            return;
        };
        f.enable_sdk_session().await;
        sqlx::query("WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<100) INSERT INTO receipt_records SELECT $1 || n,n FROM numbers")
            .bind("x".repeat(5000))
            .execute(&f.pool).await.unwrap();
        sqlx::query("UPDATE _gonvex_sync_clock SET revision=258")
            .execute(&f.pool)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime = f.runtime.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                runtime
                    .router()
                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
            .await
            .unwrap();
        assert_eq!(read(&mut socket).await["type"], "session.ready");
        let auth = serde_json::json!({"type":"auth","id":"auth","project":"project","tenant":"tenant",
            "token":"gvx_session_receipt_sdk_fixture","capabilities":{"replicaWatermark":1,
                "replicaSnapshotPages":if paged {1} else {0},"replicaSetDigest":1,
                "replicaNativeRows":if native {1} else {0}}});
        send(&mut socket, auth.clone()).await;
        assert_eq!(read(&mut socket).await["type"], "auth.result");
        send(
            &mut socket,
            serde_json::json!({"type":"replica.open","id":"A","path":"all","args":{}}),
        )
        .await;
        cohort(&mut socket, "A", 258, paged, native).await;
        send(
            &mut socket,
            serde_json::json!({"type":"query.subscribe","id":"live","path":"quietLive","args":{}}),
        )
        .await;
        assert_eq!(read(&mut socket).await["type"], "query.result");
        assert_eq!(read(&mut socket).await["revision"], 258);
        // Publish a durable unrelated transaction through the real change-feed
        // reader. A's SQL rows are unchanged; no second ready is warranted.
        let mut tx = f.pool.begin().await.unwrap();
        sqlx::query("INSERT INTO _gonvex_sync_changes(transaction_id,revision,ordinal,table_name,row_id,operation,new_value) VALUES (259,259,0,'unrelated','one','insert','{}')").execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE _gonvex_sync_clock SET revision=259")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify('gonvex_change_feed','{\"revision\":259}')")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let watermark = read(&mut socket).await;
        assert_eq!(watermark["type"], "replica.watermark");
        assert_eq!(watermark["revision"], 259);
        for _ in 0..2 {
            send(
                &mut socket,
                serde_json::json!({"type":"replica.open","id":"B","path":"all","args":{}}),
            )
            .await;
            cohort(&mut socket, "B", 259, paged, native).await;
        }
        // A rejected open must release the cohort without certifying any
        // nonexistent subscription or waiting for another transaction.
        send(
            &mut socket,
            serde_json::json!({"type":"replica.open","id":"bad","path":"missing","args":{}}),
        )
        .await;
        assert_eq!(read(&mut socket).await["type"], "replica.error");
        assert_eq!(read(&mut socket).await["revision"], 259);
        if native {
            send(
                &mut socket,
                serde_json::json!({"type":"replica.open","id":"closing","path":"all","args":{}}),
            )
            .await;
            loop {
                let frame = read(&mut socket).await;
                if frame["type"] == "replica.snapshotEncodedPage" {
                    break;
                }
            }
            send(
                &mut socket,
                serde_json::json!({"type":"replica.close","id":"closing"}),
            )
            .await;
            loop {
                let frame = read(&mut socket).await;
                if frame["type"] == "replica.closed" {
                    break;
                }
                assert_eq!(frame["type"], "replica.snapshotEncodedPage");
            }
            let terminal = read(&mut socket).await;
            assert_eq!(terminal["type"], "replica.watermark");
            assert_eq!(terminal["revision"], 259);
        }
        // Auth reset must admit a new cohort even at the same revision.
        send(&mut socket, auth).await;
        assert_eq!(read(&mut socket).await["type"], "auth.result");
        send(
            &mut socket,
            serde_json::json!({"type":"replica.open","id":"A","path":"all","args":{}}),
        )
        .await;
        cohort(&mut socket, "A", 259, paged, native).await;
        socket.close(None).await.unwrap();
        server.abort();
        f.close().await;
    }
}
