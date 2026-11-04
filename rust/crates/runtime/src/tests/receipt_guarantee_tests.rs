//! Native receipt/commit composition, using an owned PostgreSQL fixture and host.
use super::*;
use base64::Engine as _;
use gonvex_postgres::{MigrationScope, RuntimeManifestRecord, SqlMigration, TenantRoute};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row as _;

struct Fixture {
    runtime: Runtime,
    control: ControlPlane,
    session: TenantSession,
    pool: sqlx::PgPool,
    admin: sqlx::PgPool,
    schema: String,
}

#[derive(Default)]
struct SDKConcurrencyGate {
    index: usize,
    connection: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
    controller_pid: i32,
    overlap: Option<serde_json::Value>,
    witnesses: Vec<serde_json::Value>,
}

impl SDKConcurrencyGate {
    async fn release(&mut self) {
        if let Some(mut connection) = self.connection.take() {
            assert!(
                sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock(39071030)")
                    .fetch_one(&mut *connection)
                    .await
                    .unwrap()
            );
        }
    }

    async fn poll(&mut self, fixture: &Fixture, root: &std::path::Path) {
        if self.index >= 2 {
            return;
        }
        let mode = ["commit", "rollback"][self.index];
        let directory = root.join("concurrency").join(mode);
        if self.connection.is_none() {
            let request = directory.join("request.json");
            if !tokio::fs::try_exists(&request).await.unwrap() {
                return;
            }
            let request: serde_json::Value =
                serde_json::from_slice(&tokio::fs::read(request).await.unwrap()).unwrap();
            assert_eq!(request["mode"], mode);
            assert_eq!(request["rowId"], format!("sdk-concurrent-{mode}"));
            assert!(!request["key"].as_str().unwrap().is_empty());
            let mut connection = fixture.pool.acquire().await.unwrap();
            self.controller_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            sqlx::query("SELECT pg_advisory_lock(39071030)")
                .execute(&mut *connection)
                .await
                .unwrap();
            self.connection = Some(connection);
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
        if self.overlap.is_none() {
            // Require the second runtime transaction to be inside the unique
            // receipt INSERT and blocked by the first business-write owner.
            let row = sqlx::query("SELECT a.pid AS first_pid,b.pid AS second_pid,pg_blocking_pids(a.pid) AS first_blockers,pg_blocking_pids(b.pid) AS second_blockers FROM pg_stat_activity a CROSS JOIN pg_stat_activity b WHERE $1=ANY(pg_blocking_pids(a.pid)) AND a.pid=ANY(pg_blocking_pids(b.pid)) AND a.pid<>b.pid AND a.query LIKE '%receipt_records%' AND b.query LIKE '%INSERT INTO _gonvex_reducer_idempotency%' LIMIT 1")
                .bind(self.controller_pid).fetch_optional(&fixture.pool).await.unwrap();
            let Some(row) = row else {
                let waiting = sqlx::query("SELECT pid,query,state,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE datname=current_database() AND cardinality(pg_blocking_pids(pid))>0")
                    .fetch_all(&fixture.pool).await.unwrap();
                let graph: Vec<_> = waiting.into_iter().map(|row| serde_json::json!({"pid":row.get::<i32,_>("pid"),"query":row.get::<String,_>("query"),"state":row.get::<String,_>("state"),"blockers":row.get::<Vec<i32>,_>("blockers")})).collect();
                tokio::fs::write(
                    directory.join("wait-graph.json"),
                    serde_json::to_vec_pretty(&graph).unwrap(),
                )
                .await
                .unwrap();
                return;
            };
            let first: i32 = row.get("first_pid");
            let second: i32 = row.get("second_pid");
            self.overlap = Some(
                serde_json::json!({"overlap":true,"mode":mode,"controllerPid":self.controller_pid,
                "firstPid":first,"secondPid":second,"firstBlockers":row.get::<Vec<i32>,_>("first_blockers"),
                "secondBlockers":row.get::<Vec<i32>,_>("second_blockers")}),
            );
            if mode == "rollback" {
                assert!(
                    sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
                        .bind(first)
                        .fetch_one(&fixture.pool)
                        .await
                        .unwrap()
                );
                return;
            }
        }
        let mut witness = self.overlap.as_ref().unwrap().clone();
        if mode == "rollback" {
            let second = witness["secondPid"].as_i64().unwrap() as i32;
            let takeover: bool = sqlx::query_scalar("SELECT $1=ANY(pg_blocking_pids($2))")
                .bind(self.controller_pid)
                .bind(second)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
            if !takeover {
                return;
            }
            witness["takeover"] = serde_json::json!(true);
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
        self.overlap = None;
        self.index += 1;
    }
}

impl Fixture {
    async fn new() -> Option<Self> {
        Self::create(false).await
    }

    async fn create(sdk: bool) -> Option<Self> {
        let required = std::env::var_os("GONVEX_RECEIPT_TEST_REQUIRE_NATIVE").is_some();
        let Some(base) = std::env::var("GONVEX_TEST_POSTGRES_URL").ok() else {
            assert!(!required, "native receipt test requires PostgreSQL");
            return None;
        };
        let Some(binary) = std::env::var("GONVEX_TEST_MODULE_HOST_BINARY").ok() else {
            assert!(!required, "native receipt test requires the module host");
            return None;
        };
        let schema = format!("receipt_guarantee_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&base)
            .await
            .unwrap();
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{separator}options=-csearch_path%3D{schema}");
        let mut cfg = config(true);
        cfg.module_host.binary = Some(binary.into());
        cfg.module_host.endpoint = Some(format!(
            "unix:{}/receipt-{}.sock",
            std::env::temp_dir().display(),
            uuid::Uuid::new_v4().simple()
        ));
        cfg.module_host.isolate_pool_size = 1;
        cfg.module_host.max_concurrent_calls = 2;
        cfg.module_host.start_timeout = Duration::from_secs(5);
        let runtime = Runtime::new(cfg);
        let control = ControlPlane::connect(&url, runtime.inner.pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE receipt_records(id text PRIMARY KEY, value integer NOT NULL)")
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
            admin,
            schema,
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

    async fn enable_sdk_session(&self) {
        sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,environment,database_mode,status,auth_mode) VALUES ('project','Receipt fixture','test','multiTenant','active','firebase')")
            .execute(&self.pool).await.unwrap();
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,status,database_url) VALUES ('tenant','project','tenant','Receipt fixture','active',$1)")
            .bind(&self.session.route.database_url).execute(&self.pool).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,auth_realm_id,email,name) VALUES ('account','project','receipt@example.test','Receipt fixture')")
            .execute(&self.pool).await.unwrap();
        sqlx::query("INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,expires_at) VALUES ($1,'project','account',now()+interval '1 hour')")
            .bind(gonvex_postgres::token_hash("gvx_session_receipt_sdk_fixture")).execute(&self.pool).await.unwrap();
        sqlx::query("INSERT INTO members(id,account_id,status,role,permissions) VALUES ('member','account','active','manager','{}')")
            .execute(&self.pool).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,auth_realm_id,email,name) VALUES ('bob','project','bob@example.test','Bob fixture')").execute(&self.pool).await.unwrap();
        sqlx::query("INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,expires_at) VALUES ($1,'project','bob',now()+interval '1 hour')")
            .bind(gonvex_postgres::token_hash("gvx_session_receipt_sdk_bob")).execute(&self.pool).await.unwrap();
        sqlx::query("INSERT INTO members(id,account_id,status,role,permissions) VALUES ('bob-member','bob','active','manager','{}')").execute(&self.pool).await.unwrap();
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

    async fn enable_sdk_tenant_b(&self) -> (String, sqlx::PgPool) {
        let database = format!("{}_b", self.schema);
        sqlx::query(&format!("CREATE DATABASE {database}"))
            .execute(&self.admin)
            .await
            .unwrap();
        let base = std::env::var("GONVEX_TEST_POSTGRES_URL").unwrap();
        let mut url = url::Url::parse(&base).unwrap();
        url.set_path(&format!("/{database}"));
        let parameters: Vec<_> = url
            .query_pairs()
            .filter(|(name, _)| name != "options")
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect();
        url.query_pairs_mut().clear().extend_pairs(parameters);
        let route = TenantRoute {
            project_id: "project".into(),
            tenant_id: "tenant-b".into(),
            database_url: url.to_string(),
        };
        self.control
            .clone()
            .provision_tenant_database(
                route.clone(),
                vec![SqlMigration::new(
                    "0001_receipt_records.sql".into(),
                    MigrationScope::Tenant,
                    false,
                    "CREATE TABLE receipt_records(id text PRIMARY KEY,value integer NOT NULL)"
                        .into(),
                )],
            )
            .await
            .unwrap();
        self.control
            .ensure_reducer_idempotency(&route)
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&route.database_url)
            .await
            .unwrap();
        sqlx::query("INSERT INTO members(id,account_id,status,role,permissions) VALUES ('member-b','account','active','manager','{}')")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO receipt_records(id,value) VALUES ('tenant-b-private',73)")
            .execute(&pool)
            .await
            .unwrap();
        // Independent databases can have either numeric clock ordering. Give
        // B a higher counter through real commits so an incorrect old-owner
        // acknowledgement cannot hide behind B's initially smaller revision.
        for index in 0..100 {
            sqlx::query("UPDATE receipt_records SET value=$1 WHERE id='tenant-b-private'")
                .bind(72 + index % 2)
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,status,database_url) VALUES ('tenant-b','project','tenant-b','Isolated B','active',$1)")
            .bind(&route.database_url).execute(&self.pool).await.unwrap();
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
        (database, pool)
    }

    async fn close(self) {
        self.runtime.shutdown().await;
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
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
#[ignore = "requires the module-upgrade browser/PostgreSQL coordinator"]
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
    .execute(&f.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO receipt_records(id,value) VALUES ('confirmed-before-upgrade',11)")
        .execute(&f.pool)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE upgrade_effects(position bigserial PRIMARY KEY,operation text NOT NULL); CREATE FUNCTION upgrade_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO upgrade_effects(operation) VALUES (TG_OP); RETURN NEW; END $$; CREATE TRIGGER upgrade_effect AFTER INSERT OR UPDATE ON receipt_records FOR EACH ROW EXECUTE FUNCTION upgrade_effect()")
        .execute(&f.pool).await.unwrap();
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
                        .fetch_all(&f.pool).await.unwrap();
                    assert_eq!(effects, ["INSERT"], "second intent executed before module replacement");
                    let receipts: Vec<String> = sqlx::query_scalar("SELECT path FROM _gonvex_reducer_idempotency WHERE subject='account' ORDER BY path")
                        .fetch_all(&f.pool).await.unwrap();
                    assert_eq!(receipts, ["write"]);
                    let module = f.sync_client_contract(2).await;
                    assert_ne!(module["artifactHash"], initial_module["artifactHash"]);
                    assert!(module["generation"].as_u64().unwrap() > initial_module["generation"].as_u64().unwrap());
                    let value: i32 = sqlx::query_scalar("SELECT count FROM receipt_records WHERE id='upgrade-intent'")
                        .fetch_one(&f.pool).await.unwrap();
                    assert_eq!(value, 7);
                    let key: String = sqlx::query_scalar("SELECT idempotency_key FROM _gonvex_reducer_idempotency WHERE subject='account'")
                        .fetch_one(&f.pool).await.unwrap();
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
            .fetch_all(&f.pool)
            .await
            .unwrap()
    } else {
        Vec::new()
    };
    let receipts: Vec<String> = if successful {
        sqlx::query_scalar(
            "SELECT path FROM _gonvex_reducer_idempotency WHERE subject='account' ORDER BY path",
        )
        .fetch_all(&f.pool)
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
    let (tenant_b_database, tenant_b_pool) = f.enable_sdk_tenant_b().await;
    sqlx::raw_sql("CREATE FUNCTION sdk_receipt_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id IN ('sdk-concurrent-commit','sdk-concurrent-rollback') THEN PERFORM pg_advisory_xact_lock(39071030); END IF; RETURN NEW; END $$; CREATE TRIGGER sdk_receipt_gate BEFORE INSERT ON receipt_records FOR EACH ROW EXECUTE FUNCTION sdk_receipt_gate()")
        .execute(&f.pool).await.unwrap();
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
                        sqlx::query("INSERT INTO receipt_records(id,value) VALUES ('sdk-upgrade-rollback',7)").execute(&f.pool).await.unwrap();
                    }
                    // One SQL snapshot, after the SDK recorded the actual reducer
                    // failure and before the browser starts its upgrade/retry.
                    let row = sqlx::query("SELECT (SELECT count(*) FROM receipt_records WHERE id=$1) AS business, (SELECT count(*) FROM _gonvex_reducer_idempotency WHERE subject='account' AND idempotency_key=$2) AS receipts")
                        .bind("sdk-upgrade-rollback").bind(&key).fetch_one(&f.pool).await.unwrap();
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
            .bind(key).fetch_optional(&f.pool).await.unwrap()
    } else {
        None
    };
    let business =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records WHERE value=7")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    let receipts = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM _gonvex_reducer_idempotency WHERE subject='account'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    let bob_receipts = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM _gonvex_reducer_idempotency WHERE subject='bob'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    let tenant_b_business: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM receipt_records WHERE id='sdk-tenant-bob' AND value=7",
    )
    .fetch_one(&tenant_b_pool)
    .await
    .unwrap();
    let tenant_b_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM _gonvex_reducer_idempotency WHERE subject='account'",
    )
    .fetch_one(&tenant_b_pool)
    .await
    .unwrap();
    let leaked_into_b: i64 =
        sqlx::query_scalar("SELECT count(*) FROM receipt_records WHERE id='sdk-tenant-alice'")
            .fetch_one(&tenant_b_pool)
            .await
            .unwrap();
    let leaked_into_a: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM receipt_records WHERE id IN ('sdk-tenant-bob','tenant-b-private')",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    server.abort();
    tenant_b_pool.close().await;
    f.close().await;
    let cleanup = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("GONVEX_TEST_POSTGRES_URL").unwrap())
        .await
        .unwrap();
    sqlx::query(&format!("DROP DATABASE {tenant_b_database} WITH (FORCE)"))
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;
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
    sqlx::query("ALTER TABLE _gonvex_reducer_idempotency RENAME TO _gonvex_mutation_idempotency")
        .execute(&f.pool)
        .await
        .unwrap();
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
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('_gonvex_mutation_idempotency') IS NOT NULL"
    )
    .fetch_one(&f.pool)
    .await
    .unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_reducer_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    f.close().await;
}

#[tokio::test]
async fn receipt_initialization_refuses_ambiguous_histories() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.call("original", "known-key", "write", "known-row").await;
    sqlx::query("CREATE TABLE _gonvex_mutation_idempotency (LIKE _gonvex_reducer_idempotency INCLUDING ALL)")
        .execute(&f.pool).await.unwrap();
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_reducer_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_mutation_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
    f.close().await;
}

#[tokio::test]
async fn receipt_initialization_repairs_missing_index_and_observes_schema_reset() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    sqlx::query("DROP INDEX gonvex_reducer_idempotency_created_at")
        .execute(&f.pool)
        .await
        .unwrap();
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pg_index WHERE indexrelid=to_regclass('gonvex_reducer_idempotency_created_at') AND indrelid=to_regclass('_gonvex_reducer_idempotency') AND indisvalid AND indisready)")
        .fetch_one(&f.pool).await.unwrap());
    sqlx::query("DROP TABLE _gonvex_reducer_idempotency")
        .execute(&f.pool)
        .await
        .unwrap();
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('_gonvex_reducer_idempotency') IS NOT NULL"
    )
    .fetch_one(&f.pool)
    .await
    .unwrap());
    f.call("after-reset", "fresh-key", "write", "fresh-row")
        .await;
    f.close().await;
}

#[tokio::test]
async fn receipt_initialization_serializes_cold_callers() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    sqlx::query("DROP TABLE IF EXISTS _gonvex_reducer_idempotency")
        .execute(&f.pool)
        .await
        .unwrap();
    let mut gate = f.pool.acquire().await.unwrap();
    let controller: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtext('gonvex:reducer-receipt-schema'))")
        .execute(&mut *gate)
        .await
        .unwrap();
    let mut initializers = Vec::new();
    for _ in 0..2 {
        let control = f.control.clone();
        let route = f.session.route.clone();
        initializers.push(tokio::spawn(async move {
            control.ensure_reducer_idempotency(&route).await
        }));
    }
    let witnessed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%pg_advisory_xact_lock%' AND datname=current_database()")
                .bind(controller).fetch_one(&f.pool).await.unwrap();
            if blocked == 2 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT pg_advisory_unlock(hashtext('gonvex:reducer-receipt-schema'))"
    )
    .fetch_one(&mut *gate)
    .await
    .unwrap());
    drop(gate);
    for initializer in initializers {
        initializer.await.unwrap().unwrap();
    }
    assert!(
        witnessed.is_ok(),
        "cold receipt initialization did not serialize both callers"
    );
    let (first, second) = tokio::join!(
        f.call("cold-first", "cold-key", "write", "cold-row"),
        f.call("cold-second", "cold-key", "write", "cold-row"),
    );
    assert_eq!(first.value, second.value);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_reducer_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    f.close().await;
}

#[tokio::test]
async fn receipt_initialization_cancellation_releases_lock_and_admission() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    sqlx::query("DROP TABLE IF EXISTS _gonvex_reducer_idempotency")
        .execute(&f.pool)
        .await
        .unwrap();
    let pools = &f.runtime.inner.pools;
    assert_eq!(pools.snapshot().await.admission_active, 0);
    let mut gate = f.pool.acquire().await.unwrap();
    let controller: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtext('gonvex:reducer-receipt-schema'))")
        .execute(&mut *gate)
        .await
        .unwrap();
    let control = f.control.clone();
    let route = f.session.route.clone();
    let initializer = tokio::spawn(async move { control.ensure_reducer_idempotency(&route).await });
    let blocked = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%pg_advisory_xact_lock%' AND datname=current_database())")
                .bind(controller).fetch_one(&f.pool).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    assert!(
        blocked.is_ok(),
        "cancellation omitted the actual schema-lock wait"
    );
    assert_eq!(pools.snapshot().await.admission_active, 1);
    initializer.abort();
    assert!(initializer.await.unwrap_err().is_cancelled());
    assert_eq!(
        pools.snapshot().await.admission_active,
        0,
        "cancelled initialization leaked query admission"
    );
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT pg_advisory_unlock(hashtext('gonvex:reducer-receipt-schema'))"
    )
    .fetch_one(&mut *gate)
    .await
    .unwrap());
    drop(gate);
    tokio::time::timeout(
        Duration::from_secs(5),
        f.control.ensure_reducer_idempotency(&f.session.route),
    )
    .await
    .expect("cancelled initializer retained the schema lock or pool connection")
    .unwrap();
    assert_eq!(pools.snapshot().await.admission_active, 0);
    let mut probe = f.pool.acquire().await.unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT pg_try_advisory_lock(hashtext('gonvex:reducer-receipt-schema'))"
        )
        .fetch_one(&mut *probe)
        .await
        .unwrap(),
        "schema lock survived cancellation and recovery"
    );
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT pg_advisory_unlock(hashtext('gonvex:reducer-receipt-schema'))"
    )
    .fetch_one(&mut *probe)
    .await
    .unwrap());
    drop(probe);
    // Exercise the routed database's local slots independently of shared
    // capacity. A cancelled initializer must return exactly its original slot.
    let database_capacity = f
        .runtime
        .inner
        .config
        .database_max_connections
        .clamp(1, pools.snapshot().await.admission_limit as u32);
    let mut permits = Vec::new();
    for _ in 0..database_capacity {
        permits.push(
            tokio::time::timeout(
                Duration::from_secs(1),
                pools.admit(&f.session.route.database_url),
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            pools.admit(&f.session.route.database_url)
        )
        .await
        .is_err(),
        "cancellation released database admission twice"
    );
    drop(permits);
    assert_eq!(pools.snapshot().await.admission_active, 0);

    // Each intentionally unopened lazy pool has independent local capacity.
    // Fill the shared semaphore without connecting to fictitious databases.
    let capacity = pools.snapshot().await.admission_limit;
    let mut permits = Vec::new();
    for index in 0..capacity {
        let url = format!("postgresql://localhost/gonvex_admission_only_{index}");
        permits.push(
            tokio::time::timeout(Duration::from_secs(1), pools.admit(&url))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(pools.snapshot().await.admission_active, capacity);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            pools.admit("postgresql://localhost/gonvex_admission_only_extra")
        )
        .await
        .is_err(),
        "cancellation released query admission twice"
    );
    drop(permits);
    assert_eq!(pools.snapshot().await.admission_active, 0);
    f.call("after-cancellation", "cancel-key", "write", "cancel-row")
        .await;
    f.close().await;
}

#[tokio::test]
async fn receipt_lookup_progresses_while_snapshot_preparations_own_pool_connections() {
    use futures_util::FutureExt as _;
    let Some(f) = Fixture::create(true).await else {
        return;
    };
    f.enable_sdk_session().await;
    let mut gate = f.pool.begin().await.unwrap();
    let controller: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    sqlx::query("LOCK TABLE _gonvex_sync_clock IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await
        .unwrap();
    let mut queue = crate::replica_open_queue::ReplicaOpenQueue::default();
    for id in ["opening-first", "opening-second"] {
        let runtime = f.runtime.clone();
        let session = f.session.clone();
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
                    runtime
                        .open_replica_negotiated(&session, request, false)
                        .await
                }
                .boxed(),
            )
            .unwrap();
    }
    // Poll exactly as the socket select loop does. Both actual snapshot
    // transactions must be inside PostgreSQL before an inline query begins.
    let witnessed = {
        let pending = queue.next();
        tokio::pin!(pending);
        tokio::select! {
            _ = &mut pending => panic!("snapshot gate failed to hold preparation"),
            witnessed = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%_gonvex_sync_clock%' AND datname=current_database()")
                        .bind(controller).fetch_one(&f.pool).await.unwrap();
                    if blocked == 2 { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }) => witnessed,
        }
    };
    gate.commit().await.unwrap();
    let lookup = tokio::time::timeout(
        Duration::from_secs(5),
        f.runtime.execute_tenant_query(
            &f.session,
            execution::REDUCER_RECEIPT_QUERY,
            serde_json::json!({"idempotencyKey":"missing","receiptPath":"write"}),
        ),
    )
    .await;
    queue.clear();
    f.close().await;
    assert!(
        witnessed.is_ok(),
        "both snapshot pool owners were not witnessed"
    );
    assert!(
        lookup.is_ok(),
        "inline receipt lookup starved snapshot preparation and pool cleanup"
    );
    assert_eq!(lookup.unwrap().unwrap(), serde_json::json!({"found":false}));
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
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('_gonvex_reducer_idempotency') IS NOT NULL"
    )
    .fetch_one(&f.pool)
    .await
    .unwrap());
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
    sqlx::query("INSERT INTO members(id,account_id,status,role,permissions) VALUES ('other-member','other-account','active','manager','{}')").execute(&f.pool).await.unwrap();
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_reducer_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    // Reading historical receipts must neither migrate nor silently choose
    // between conflicting current and legacy journals.
    sqlx::query("ALTER TABLE _gonvex_reducer_idempotency RENAME TO _gonvex_mutation_idempotency")
        .execute(&f.pool)
        .await
        .unwrap();
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
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('_gonvex_reducer_idempotency') IS NOT NULL"
    )
    .fetch_one(&f.pool)
    .await
    .unwrap());
    sqlx::query("CREATE TABLE _gonvex_reducer_idempotency (LIKE _gonvex_mutation_idempotency INCLUDING ALL)").execute(&f.pool).await.unwrap();
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
    sqlx::query("DROP TABLE _gonvex_reducer_idempotency")
        .execute(&f.pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE _gonvex_mutation_idempotency RENAME TO _gonvex_reducer_idempotency")
        .execute(&f.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status='inactive' WHERE id='member'")
        .execute(&f.pool)
        .await
        .unwrap();
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
        sqlx::query(
            "UPDATE _gonvex_sync_changes SET created_at=clock_timestamp()-interval '31 days'",
        )
        .execute(&f.pool)
        .await
        .unwrap();
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
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_sync_changes")
                .fetch_one(&f.pool)
                .await
                .unwrap(),
            0
        );
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_reducer_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        2
    );
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
    sqlx::query("DROP TABLE _gonvex_sync_clock")
        .execute(&f.pool)
        .await
        .unwrap();
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
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
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&f.pool)
                .await
                .unwrap(),
            0
        );
    }
    f.call("retry-after-rollback", "retry", "write", "committed")
        .await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
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
    assert!(f
        .runtime
        .execute_tenant_reducer(
            &f.session,
            "wrong-path",
            Some("same-key"),
            "noop",
            serde_json::json!({})
        )
        .await
        .is_err());
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_reducer_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        2
    );
    f.close().await;
}

// Observe the actual PostgreSQL unique-index wait. A delay alone would not
// establish that both claims overlap before the owner commits or rolls back.
async fn competing_claim(owner_commits: bool) {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    let mut owner = f
        .control
        .begin_tenant_transaction(&f.session.route, false)
        .await
        .unwrap();
    let owner_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut **owner.transaction())
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
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&f.pool)
                .await
                .unwrap(),
            0
        );
    }
    let mut contender = f
        .control
        .begin_tenant_transaction(&f.session.route, false)
        .await
        .unwrap();
    let contender_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut **contender.transaction())
        .await
        .unwrap();
    let task = tokio::spawn(async move {
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
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked = sqlx::query_scalar::<_, bool>("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(owner_pid)
                .bind(contender_pid)
                .fetch_one(&f.pool)
                .await
                .unwrap();
            if blocked {
                break;
            }
            assert!(
                !task.is_finished(),
                "contender completed without waiting on receipt owner"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("PostgreSQL did not report the receipt claim blocker");
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
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

// Only the owned Docker coordinator may run this test. The ordinary suite
// excludes it; mandatory CI explicitly runs it with --ignored and --exact.
#[tokio::test]
#[ignore = "requires the owned PostgreSQL process-crash coordinator"]
async fn receipt_recovery_after_postgres_process_crash() {
    let coordination = std::env::var("GONVEX_RECEIPT_CRASH_COORDINATION")
        .expect("receipt crash test requires its process-crash coordinator");
    let mode = std::env::var("GONVEX_RECEIPT_CRASH_MODE")
        .expect("receipt crash test requires an explicit commit boundary");
    assert!(matches!(mode.as_str(), "committed" | "uncommitted"));
    let f = Fixture::new()
        .await
        .expect("receipt crash test requires native fixtures");
    f.control
        .ensure_reducer_idempotency(&f.session.route)
        .await
        .unwrap();
    let mut interrupted = None;
    if mode == "committed" {
        // The coordinator kills PostgreSQL after the effect and receipt commit.
        // Discard this result and retry as if the network reply was lost.
        let _lost_reply = f
            .call("lost-reply", "crash-intent", "write", "original")
            .await;
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
        // A different connection must observe neither side before the crash.
        for table in ["receipt_records", "_gonvex_reducer_idempotency"] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&f.pool)
                    .await
                    .unwrap(),
                0
            );
        }
        interrupted = Some(transaction);
    }
    let marker = serde_json::json!({"mode":mode,"schema":f.schema});
    let mut file = std::fs::File::create(format!("{coordination}.ready.tmp")).unwrap();
    std::io::Write::write_all(&mut file, marker.to_string().as_bytes()).unwrap();
    file.sync_all().unwrap();
    std::fs::rename(
        format!("{coordination}.ready.tmp"),
        format!("{coordination}.ready"),
    )
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while !std::path::Path::new(&format!("{coordination}.resume")).is_file() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "process-crash coordinator did not resume the test"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(interrupted);
    // Pooled sockets may contain a connection closed by the killed postmaster.
    // Clear them before the first retry so the assertion concerns durable state.
    f.runtime
        .inner
        .pools
        .close_database(&f.session.route.database_url)
        .await;
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
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM receipt_records")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1,
        "crash recovery changed committed business count"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _gonvex_reducer_idempotency")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1,
        "crash recovery changed committed receipt count"
    );
    let row = sqlx::query_as::<_, (String, i32)>("SELECT id, value FROM receipt_records")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(row, ("original".to_owned(), 7));
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
        sqlx::query("INSERT INTO receipt_records SELECT repeat('x',5000)||n,n FROM generate_series(1,100) n")
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
