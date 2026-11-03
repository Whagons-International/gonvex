//! Local-only physical-pool stress, deliberately independent of application latency.
//! Baseline mode reproduces the pre-fix per-database pools and SQL admission.
//! Set GONVEX_TEST_POSTGRES_URL to an owned numeric-loopback Postgres with
//! max_connections=100. Run --baseline, then no flags for the selected defaults.
//! --idle-one and --single measure tuning alternatives. --pgbouncer routes only
//! query pools to loopback port 57433; keep LISTEN direct, use transaction mode,
//! pool_size=2, max_user_connections=52, and max_prepared_statements=200.
//! Each successful run removes its 26 synthetic databases. Pressure-aborted runs
//! leave budget_synthetic_0..25 for the fixture owner to clean before retrying.
use gonvex_postgres::{PoolLimits, PoolRegistry};
use sqlx::postgres::{PgListener, PgPoolOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() {
    let base = std::env::var("GONVEX_TEST_POSTGRES_URL").expect("explicit disposable local URL");
    let url = url::Url::parse(&base).unwrap();
    assert!(
        matches!(url.host_str(), Some("127.0.0.1" | "::1")),
        "numeric loopback only"
    );
    let warm_two = !std::env::args().any(|arg| arg == "--idle-one");
    let single = std::env::args().any(|arg| arg == "--single");
    let bouncer = std::env::args().any(|arg| arg == "--pgbouncer");
    let baseline = std::env::args().any(|arg| arg == "--baseline");
    let users: usize = std::env::var("LOAD_USERS")
        .unwrap_or("300".into())
        .parse()
        .unwrap();
    assert!(users <= 600);
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base)
        .await
        .unwrap();
    let max: String = sqlx::query_scalar("SHOW max_connections")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(max, "100", "fixture must use incident connection limit");
    let mut urls = Vec::new();
    for index in 0..26 {
        let name = format!("budget_synthetic_{index}");
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await
            .unwrap();
        let mut tenant = url.clone();
        tenant.set_path(&format!("/{name}"));
        tenant
            .query_pairs_mut()
            .append_pair("application_name", "gonvex-local-budget-load");
        urls.push(tenant.to_string());
    }
    // Entirely synthetic size fixtures. These are SQL pool tests, not Whagons
    // reducers or client subscription measurements.
    for (index, url) in urls.iter().enumerate() {
        let fixture = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE tasks (id integer PRIMARY KEY, title text, status text, updated_at timestamptz DEFAULT now())").execute(&fixture).await.unwrap();
        sqlx::query("CREATE TABLE members (id integer PRIMARY KEY, name text)")
            .execute(&fixture)
            .await
            .unwrap();
        let tasks = match index {
            0 => 1900,
            1 => 8800,
            _ => 300,
        };
        let members = match index {
            0 => 85,
            1 => 89,
            _ => 20,
        };
        sqlx::query("INSERT INTO tasks(id,title,status) SELECT n, 'Synthetic task ' || n, 'open' FROM generate_series(1,$1) n").bind(tasks).execute(&fixture).await.unwrap();
        sqlx::query("INSERT INTO members(id,name) SELECT n, 'Synthetic member ' || n FROM generate_series(1,$1) n").bind(members).execute(&fixture).await.unwrap();
        fixture.close().await;
    }
    let registry = PoolRegistry::new(PoolLimits {
        max_total_connections: 20,
        max_connections_per_database: if single { 1 } else { 2 },
        max_idle_connections_per_database: if warm_two { 2 } else { 1 },
        idle_timeout: PoolLimits::default().idle_timeout,
        acquire_timeout: Duration::from_secs(30),
    });
    let mut listeners = Vec::new();
    for url in &urls {
        let mut listener = PgListener::connect(url).await.unwrap();
        listener.listen("budget_load").await.unwrap();
        listeners.push(listener);
    }
    let shared = Arc::new(Semaphore::new(20));
    let locals: Vec<_> = urls.iter().map(|_| Arc::new(Semaphore::new(16))).collect();
    let mut old = Vec::new();
    if baseline || bouncer {
        for url in &urls {
            let mut query_url = url::Url::parse(url).unwrap();
            if bouncer {
                query_url.set_port(Some(57433)).unwrap();
            }
            let pool = PgPoolOptions::new()
                .max_connections(16)
                .min_connections(0)
                .idle_timeout(Duration::from_secs(1))
                .max_lifetime(Duration::from_secs(30 * 60))
                .acquire_timeout(Duration::from_secs(30))
                .connect(query_url.as_str())
                .await
                .unwrap();
            sqlx::query("SELECT 1").execute(&pool).await.unwrap();
            old.push(pool);
        }
    } else {
        for url in &urls {
            let pool = registry.pool(url).await.unwrap();
            sqlx::query("SELECT 1").execute(&pool).await.unwrap();
        }
    }
    let observed = admin.clone();
    let monitor = tokio::spawn(async move {
        let mut peak = 0i64;
        let mut server_peak = 0i64;
        loop {
            let total: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='gonvex-local-budget-load'")
                .fetch_one(&observed).await.unwrap();
            let server_total: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend'",
            )
            .fetch_one(&observed)
            .await
            .unwrap();
            server_peak = server_peak.max(server_total);
            peak = peak.max(total);
            println!(
                "{}",
                serde_json::json!({"sampleConnections":total,"peakConnections":peak,"serverConnections":server_total,"peakServerConnections":server_peak})
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let mut jobs = Vec::new();
    for user in 0..users {
        let url = urls[user % 5].clone();
        let shared = shared.clone();
        let local = locals[user % 5].clone();
        let registry = registry.clone();
        let old = old.get(user % 5).cloned();
        jobs.push(tokio::spawn(async move {
            let mut results = Vec::new();
            for _ in 0..10 {
                let psi = std::fs::read_to_string("/proc/pressure/memory").unwrap();
                let full = psi.lines().find(|line| line.starts_with("full ")).unwrap();
                let pressure: f64 = full
                    .split_whitespace()
                    .find_map(|field| field.strip_prefix("avg10="))
                    .unwrap()
                    .parse()
                    .unwrap();
                assert!(
                    pressure <= 5.0,
                    "stop load: memory PSI full exceeds build-headroom gate"
                );
                let started = Instant::now();
                let result = if let Some(pool) = &old {
                    let permits = tokio::time::timeout(Duration::from_secs(30), async {
                        let local = local.acquire().await.unwrap();
                        let shared = shared.acquire().await.unwrap();
                        (local, shared)
                    }).await;
                    match permits {
                    Ok((_local, _shared)) => sqlx::query("WITH delay AS MATERIALIZED (SELECT pg_sleep(0.01)) UPDATE tasks SET updated_at=now() FROM delay WHERE id=1")
                        .execute(pool)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string()),
                    Err(_) => Err("database query admission timed out".to_owned()),
                    }
                } else {
                    match registry.pool(&url).await {
                        Ok(pool) => async {
                            let _admission = registry.admit(&url).await.map_err(|e| e.to_string())?;
                            sqlx::query("WITH delay AS MATERIALIZED (SELECT pg_sleep(0.01)) UPDATE tasks SET updated_at=now() FROM delay WHERE id=1")
                            .execute(&pool)
                            .await
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                        }.await,
                        Err(e) => Err(e.to_string()),
                    }
                };
                results.push((started.elapsed().as_secs_f64() * 1000.0, result.err()));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            results
        }));
    }
    let mut results = Vec::new();
    for job in jobs {
        results.extend(job.await.unwrap());
    }
    monitor.abort();
    let _ = monitor.await;
    let mut times: Vec<_> = results.iter().map(|r| r.0).collect();
    times.sort_by(f64::total_cmp);
    let percentile = |p: f64| times[((times.len() - 1) as f64 * p) as usize];
    let errors: Vec<_> = results.iter().filter_map(|r| r.1.as_ref()).collect();
    println!(
        "{}",
        serde_json::json!({"baseline":baseline,"pgbouncer":bouncer,"singleConnection":single,"warmTwo":warm_two,"users":users,"operations":times.len(),"errors":errors.len(),"firstError":errors.first(),"p50Ms":percentile(0.5),"p95Ms":percentile(0.95),"p99Ms":percentile(0.99)})
    );
    drop(listeners);
    for pool in old {
        pool.close().await;
    }
    registry.close().await;
    for index in 0..26 {
        sqlx::query(&format!(
            "DROP DATABASE budget_synthetic_{index} WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .unwrap();
    }
    admin.close().await;
}
