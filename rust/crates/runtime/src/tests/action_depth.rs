//! End to end, in-process V8: durable hops start at depth 0 and are bounded,
//! root Actions run one at a time, the outbox backlog stays in SQLite, and the
//! burst never needs more than the warm isolate plus one Action isolate.

use std::time::{Duration, Instant};

use base64::Engine as _;
use gonvex_store::{RuntimeManifestRecord, TenantRoute};
use sha2::{Digest, Sha256};
use sqlx::Row;

use super::*;

const CODE: &str = r#"
export async function burst(ctx, args) {
  for (let i = 0; i < args.n; i++) await ctx.actions.enqueue("work", { i });
  return null;
}
export async function work(ctx, args) {
  await ctx.tools.begin({ i: args.i });
  await new Promise((resolve) => setTimeout(resolve, args.ms ?? 150));
  await ctx.tools.end({ i: args.i });
  return null;
}
export async function begin(ctx, args) {
  await ctx.db.insert("active", { id: String(args.i) });
  const rows = await ctx.db.query("SELECT count(*) AS c FROM active", []);
  await ctx.db.insert("log", { id: String(args.i), concurrent: Number(rows[0].c) });
  return null;
}
export async function end(ctx, args) {
  await ctx.db.delete("active", String(args.i));
  return null;
}
export async function loop(ctx) {
  await ctx.db.insert("hops", { id: ctx.invocation.commandId });
  await ctx.scheduler.runAfter(0, "loop", {});
  return null;
}
"#;

struct Lab {
    runtime: Runtime,
    fixture: sqlx::SqlitePool,
    session: TenantSession,
    _data: tempfile::TempDir,
}

async fn lab(action_concurrency: usize) -> Lab {
    let data = test_data_dir();
    let control_url = gonvex_store::database_url(&data.path().join("control.db"));
    let url = gonvex_store::database_url(&data.path().join("tenant.db"));
    let mut cfg = config(true);
    cfg.module_host.in_process = true;
    cfg.module_host.isolate_pool_size = 1;
    cfg.module_host.start_timeout = Duration::from_secs(20);
    let mut runtime = Runtime::new(cfg);
    Arc::get_mut(&mut runtime.inner)
        .expect("no other runtime handle yet")
        .action_gate = crate::action_gate::ActionGate::new(action_concurrency);
    let control = ControlPlane::connect(&control_url, runtime.inner.pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let control_fixture = runtime.inner.pools.pool(&control_url).await.unwrap();
    let fixture = runtime.inner.pools.pool(&url).await.unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES('project','Project','multiTenant','active')").execute(&control_fixture).await.unwrap();
    sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,domain,database_url,status) VALUES('route','project','tenant-id','Tenant','tenant',$1,'active')").bind(&url).execute(&control_fixture).await.unwrap();
    let route = TenantRoute {
        project_id: "project".into(),
        tenant_id: "tenant-id".into(),
        database_url: url.clone(),
    };
    for table in [
        "CREATE TABLE active(id TEXT PRIMARY KEY)",
        "CREATE TABLE log(id TEXT PRIMARY KEY, concurrent INTEGER NOT NULL)",
        "CREATE TABLE hops(id TEXT PRIMARY KEY)",
    ] {
        sqlx::query(table).execute(&fixture).await.unwrap();
    }
    control
        .clone()
        .provision_tenant_database(route.clone(), Vec::new())
        .await
        .unwrap();
    *runtime.inner.control_plane.write().await = Some(control);
    runtime.inner.module_host.start().await.unwrap();

    let mut functions = serde_json::json!({});
    for (path, kind, internal) in [
        ("burst", "reducer", false),
        ("work", "action", false),
        ("begin", "reducer", true),
        ("end", "reducer", true),
        ("loop", "reducer", false),
    ] {
        functions[path] = serde_json::json!({"kind":kind,"public":false,"internal":internal,
            "interactive":false,"classification":if internal {"internal"} else {"system"},
            "handler":path,"file":"test.js","delivery":"oneShot",
            "args":{"kind":"any"},"result":{"kind":"any"}});
    }
    functions["work"]["actionCapabilities"] = serde_json::json!({"tools":{
        "begin":{"kind":"internalReducer","function":"begin"},
        "end":{"kind":"internalReducer","function":"end"}}});
    let mut module = serde_json::json!({"generation":1,"language":"typescript","entrypoint":"test.js",
        "files":{},"visibility":{},"crons":[],"functions":functions,
        "javascript":{"path":"test.js","hash":format!("{:x}", Sha256::digest(CODE.as_bytes())),
            "code":base64::engine::general_purpose::STANDARD.encode(CODE)}});
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
    // The system actor (as for cron runs): outbox deliveries reuse the
    // committing session and scheduled jobs the system session, instead of
    // resolving a Member row this fixture does not have.
    let session = crate::execution::system_tenant_session("project", route);
    Lab {
        runtime,
        fixture,
        session,
        _data: data,
    }
}

async fn count(pool: &sqlx::SqlitePool, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

#[test]
fn outbox_actions_run_one_at_a_time_with_the_backlog_in_sqlite() {
    crate::in_process::tests::run(async {
        let lab = lab(1).await;
        const N: i64 = 6;
        lab.runtime
            .execute_tenant_reducer(
                &lab.session,
                "burst",
                None,
                "burst",
                serde_json::json!({ "n": N }),
            )
            .await
            .unwrap();
        // While the drainer works, at most one outbox row is claimed, the
        // rest wait as `pending` rows, and the burst needs at most the warm
        // isolate plus one Action isolate.
        let deadline = Instant::now() + Duration::from_secs(60);
        let (mut max_processing, mut max_isolates, mut max_complete) = (0, 0, 0);
        let mut saw_backlog = false;
        loop {
            let processing = count(
                &lab.fixture,
                "SELECT count(*) FROM _gonvex_action_outbox WHERE status='processing'",
            )
            .await;
            let pending = count(
                &lab.fixture,
                "SELECT count(*) FROM _gonvex_action_outbox WHERE status='pending'",
            )
            .await;
            saw_backlog |= processing == 1 && pending >= 1;
            max_processing = max_processing.max(processing);
            let (isolates, complete) = gonvex_module_runtime_v8::live_isolates();
            max_isolates = max_isolates.max(isolates);
            max_complete = max_complete.max(complete);
            if count(&lab.fixture, "SELECT count(*) FROM log").await == N && processing == 0 {
                break;
            }
            assert!(Instant::now() < deadline, "outbox did not drain");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(max_processing, 1, "the drainer claims after the permit");
        assert!(saw_backlog, "queued Actions waited in SQLite");
        assert_eq!(
            count(&lab.fixture, "SELECT max(concurrent) FROM log").await,
            1,
            "root Actions never overlapped"
        );
        // Unsplit artifact: one warm isolate (Queries/Reducers, including
        // the Actions' tool Reducers) and one on-demand Action isolate.
        assert!(max_isolates <= 2, "isolates: {max_isolates}");
        assert!(
            max_complete <= 2,
            "complete-bundle isolates: {max_complete}"
        );
        let snapshot = lab.runtime.inner.action_gate.snapshot();
        assert_eq!(
            snapshot[crate::action_gate::ActionClass::Outbox as usize].2,
            N as usize
        );
        lab.runtime.inner.module_host.shutdown().await;
    });
}

#[test]
fn a_waiting_user_action_goes_before_queued_outbox_work() {
    crate::in_process::tests::run(async {
        let lab = lab(1).await;
        lab.runtime
            .execute_tenant_reducer(
                &lab.session,
                "burst",
                None,
                "burst",
                serde_json::json!({ "n": 3 }),
            )
            .await
            .unwrap();
        // Wait until the first outbox Action holds the permit.
        let deadline = Instant::now() + Duration::from_secs(30);
        while count(&lab.fixture, "SELECT count(*) FROM log").await == 0 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        lab.runtime
            .execute_tenant_action(&lab.session, "work", serde_json::json!({"i": 100, "ms": 1}))
            .await
            .unwrap();
        // The user Action ran right after the first outbox Action, before the
        // two queued ones (their rows are still pending or just claimed).
        let done = count(&lab.fixture, "SELECT count(*) FROM log WHERE id <> '100'").await;
        assert!(done <= 2, "user Action waited behind {done} outbox Actions");
        let deadline = Instant::now() + Duration::from_secs(30);
        while count(&lab.fixture, "SELECT count(*) FROM log").await < 4 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            count(&lab.fixture, "SELECT max(concurrent) FROM log").await,
            1
        );
        lab.runtime.inner.module_host.shutdown().await;
    });
}

#[test]
fn a_self_rescheduling_reducer_runs_past_depth_eight_and_stops_at_the_hop_bound() {
    crate::in_process::tests::run(async {
        let lab = lab(1).await;
        lab.runtime.inner.scheduler.start(lab.runtime.clone());
        lab.runtime
            .execute_tenant_reducer(&lab.session, "loop", None, "loop", serde_json::json!({}))
            .await
            .unwrap();
        // The user call plus one scheduled run per hop, hops 1..=MAX.
        let expected = 1 + i64::from(crate::execution::max_durable_hops());
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut last = (0, Instant::now());
        loop {
            let runs = count(&lab.fixture, "SELECT count(*) FROM hops").await;
            assert!(runs <= expected, "the chain ran past the hop bound: {runs}");
            if runs != last.0 {
                last = (runs, Instant::now());
            } else if runs == expected && last.1.elapsed() > Duration::from_secs(2) {
                break;
            }
            assert!(Instant::now() < deadline, "chain stalled at {runs} runs");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(expected > i64::from(gonvex_module_runtime::MAX_INVOCATION_DEPTH) + 1);
        lab.runtime.inner.scheduler.shutdown();
        lab.runtime.inner.module_host.shutdown().await;
    });
}
