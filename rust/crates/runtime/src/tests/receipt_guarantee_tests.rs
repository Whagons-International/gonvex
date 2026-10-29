//! Native receipt/commit composition, using an owned PostgreSQL fixture and host.
use super::*;
use base64::Engine as _;
use gonvex_postgres::{RuntimeManifestRecord, TenantRoute};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;

struct Fixture {
    runtime: Runtime,
    control: ControlPlane,
    session: TenantSession,
    pool: sqlx::PgPool,
    admin: sqlx::PgPool,
    schema: String,
}

impl Fixture {
    async fn new() -> Option<Self> {
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

    async fn close(self) {
        self.runtime.shutdown().await;
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
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
    assert!(replay.committed_revision >= later.committed_revision,
        "replay lost its sufficient current synchronization barrier: first={:?}, later={:?}, replay={:?}", first.committed_revision,later.committed_revision,replay.committed_revision);
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
