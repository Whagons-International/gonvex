//! Idle-connection driver: server CPU per mostly idle, authenticated
//! presence-v2 WebSocket (docs/performance/conn-idle.md).
//!
//!   taskset -c 3 cargo bench -p gonvex-runtime --features perf-fixture --bench conn_idle
//!
//! The runtime runs in this process (pin it to one CPU with taskset: one
//! Tokio worker, as in the 1-vCPU container). The clients run in a child
//! process (this binary re-executed with `CONN_IDLE_ROLE=client`) on the
//! other CPUs, so the measured process CPU is the server's alone.
//!
//! Every client authenticates with a plain session token, advertises
//! presence v2, sends `presence.set` + `presence.subscribe` (as the thin
//! client and the SDK do) and then only answers Pings, unless:
//!
//! Env:
//!   CONN_IDLE_CONNECTIONS   sockets (1000)
//!   CONN_IDLE_SHARES        tenant shares in percent ("39,39,11,6,2,2,1")
//!   CONN_IDLE_WARMUP        seconds after all sockets are up (10)
//!   CONN_IDLE_SECONDS       measured window (60; covers 3 pings + 2 auth
//!                           revalidations per socket)
//!   CONN_IDLE_TRANSITION_S  mean seconds between `presence.set` per socket
//!                           (0 = none; the lab uses 101)
//!   CONN_IDLE_RECONNECT_PER_S  total reconnects per second across sockets
//!                           (0 = none; the lab's 500-user runs ~1.35/s)
//!   CONN_IDLE_BATCH_FRAMES  1 = advertise `replicaBatchFrames` (as the SDK
//!                           does): presence arrives as `replica.batch` items
//!   CONN_IDLE_LABEL         free-form label copied into the JSON
//!
//! Presence delivery latency (`client.latencyMs`): for every Member change a
//! socket receives inside the measured window, receipt time minus the time
//! the Member's socket sent that `presence.set` (same client process, one
//! clock), over every subscriber of the tenant. Changes that are not the
//! Member's latest sent state (superseded inside one window) are skipped.
//!   BENCH_JSON_OUT          append the result as one JSON line

use std::collections::BTreeMap;
use std::io::{BufRead, Write as _};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use gonvex_runtime::accept::{AcceptedSocket, TunedListener};
use gonvex_runtime::config::{Config, ModuleHostConfig, SandboxConfig};
use gonvex_runtime::Runtime;
use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

fn main() {
    if std::env::var("CONN_IDLE_ROLE").as_deref() == Ok("client") {
        client_main();
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(server_main());
}

// ---------------------------------------------------------------- server ---

/// Production defaults (`Config::from_env`, plus any `GONVEX_*` overrides in
/// the environment) for pools, compression, presence and sockets.
fn config(data_dir: &std::path::Path, control_url: String) -> Config {
    let defaults = Config::from_env().expect("GONVEX_* configuration");
    Config {
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        control_plane_database_url: Some(control_url),
        default_database_url: Some(gonvex_store::database_url(data_dir)),
        tenant_database_urls: BTreeMap::new(),
        project_database_urls: BTreeMap::new(),
        require_auth: true,
        websocket_compression: defaults.websocket_compression,
        websocket_compression_threshold: defaults.websocket_compression_threshold,
        control_secret: Some("conn-idle-control-secret-that-is-long-enough".to_owned()),
        auth_public_url: Some("http://localhost:8080".to_owned()),
        admin_key: Some("conn-idle-admin-key".to_owned()),
        dev_sync_key: None,
        dashboard_account: None,
        dashboard_password: None,
        dashboard_auth_project_id: Some("project".to_owned()),
        google_client_id: None,
        google_client_secret: None,
        database_max_total_connections: defaults.database_max_total_connections,
        database_max_connections: defaults.database_max_connections,
        database_max_idle_connections: defaults.database_max_idle_connections,
        database_idle_timeout: defaults.database_idle_timeout,
        module_host: ModuleHostConfig {
            enabled: false,
            in_process: false,
            max_heap_bytes: 64 << 20,
            binary: None,
            endpoint: None,
            start_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            max_frame_bytes: 1 << 20,
            max_concurrent_calls: 4,
            isolate_pool_size: 1,
            execution_timeout: Duration::from_secs(1),
            action_execution_timeout: Duration::from_secs(900),
        },
        runtime_version: "conn-idle".to_owned(),
        sandbox: SandboxConfig::default(),
        storage: defaults.storage,
        service_principals: Vec::new(),
        public_functions: Default::default(),
        presence: defaults.presence,
        connection: defaults.connection,
    }
}

/// Splits `total` sockets over tenants by percent shares (largest remainder).
fn split(total: usize, shares: &[f64]) -> Vec<usize> {
    let sum: f64 = shares.iter().sum();
    let mut counts: Vec<usize> = shares
        .iter()
        .map(|share| (total as f64 * share / sum).floor() as usize)
        .collect();
    let mut left = total - counts.iter().sum::<usize>();
    let mut index = 0;
    while left > 0 {
        let slots = counts.len();
        counts[index % slots] += 1;
        left -= 1;
        index += 1;
    }
    counts
}

/// Accounts, members, tenant index rows and long-lived session tokens,
/// written directly (no password hashing). Returns `(token, tenant)` pairs.
async fn provision(
    data_dir: &std::path::Path,
    control_url: &str,
    counts: &[usize],
) -> Vec<(String, String)> {
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(control_url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    let fixture = pools.pool(control_url).await.unwrap();
    sqlx::query(
        r#"INSERT INTO gonvex_runtime_projects
           (id,name,database_mode,database_url,status,auth_mode)
           VALUES('project','Project','multiTenant',$1,'active','gonvex-native')"#,
    )
    .bind(control_url)
    .execute(&fixture)
    .await
    .unwrap();
    let expires = chrono::Utc::now().timestamp_millis() + 24 * 3600 * 1000;
    let mut tokens = Vec::new();
    let mut next = 0usize;
    for (index, count) in counts.iter().enumerate() {
        let tenant = format!("tenant-{index}");
        let tenant_url = gonvex_store::database_url(&data_dir.join(format!("{tenant}.db")));
        let route = TenantRoute {
            project_id: "project".to_owned(),
            tenant_id: tenant.clone(),
            database_url: tenant_url.clone(),
        };
        control
            .clone()
            .provision_tenant_database(route.clone(), Vec::new())
            .await
            .unwrap();
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_tenants
               (relationship_id,project_id,tenant_id,name,database_url,status)
               VALUES($1,'project',$1,$1,$2,'active')"#,
        )
        .bind(&tenant)
        .bind(&tenant_url)
        .execute(&fixture)
        .await
        .unwrap();
        let ids: Vec<usize> = (next..next + count).collect();
        next += count;
        let mut tenant_tx = control
            .begin_tenant_transaction(&route, false)
            .await
            .unwrap();
        tenant_tx
            .set_command_id(&format!("conn-idle:{tenant}"))
            .await
            .unwrap();
        for id in &ids {
            sqlx::query(
                r#"INSERT INTO members
                   (id,account_id,status,display_name,avatar_url,role,permissions,updated_at)
                   VALUES($1,$2,'active',$3,'','admin','{}',CAST(unixepoch('subsec')*1000 AS INTEGER))"#,
            )
            .bind(format!("member_{id}"))
            .bind(format!("acct_{id}"))
            .bind(format!("User {id}"))
            .execute(&mut **tenant_tx.transaction())
            .await
            .unwrap();
        }
        tenant_tx.commit().await.unwrap();
        let mut control_tx = control.begin_control_transaction(false).await.unwrap();
        for id in &ids {
            let token = format!(
                "gvx_session_conn_idle_{id:06}_{}",
                uuid::Uuid::new_v4().simple()
            );
            for statement in [
                "INSERT INTO accounts(id,auth_realm_id,email,name) VALUES($1,'project',$2,$3)",
                "INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status) VALUES($1,$4,$5,'active')",
                "INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,family_id,expires_at) VALUES($6,'project',$1,'',$7)",
            ] {
                sqlx::query(statement)
                    .bind(format!("acct_{id}"))
                    .bind(format!("user{id}@conn-idle.test"))
                    .bind(format!("User {id}"))
                    .bind(&tenant)
                    .bind(format!("member_{id}"))
                    .bind(gonvex_store::token_hash(&token))
                    .bind(expires)
                    .execute(&mut **control_tx.transaction())
                    .await
                    .unwrap();
            }
            tokens.push((token, tenant.clone()));
        }
        control_tx.commit().await.unwrap();
        pools.close_database(&tenant_url).await;
    }
    pools.close_database(control_url).await;
    tokens
}

/// Per-thread CPU (ticks) by (thread name with digits stripped, tid).
fn thread_cpu() -> BTreeMap<(String, String), u64> {
    let mut threads = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
        return threads;
    };
    for entry in entries.flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let name: String = stat[open + 1..close]
            .chars()
            .map(|c| if c.is_ascii_digit() { '#' } else { c })
            .collect();
        let fields: Vec<&str> = stat[close + 2..].split(' ').collect();
        // Fields after the name start at state (3rd field): utime is the
        // 14th field overall, stime the 15th.
        let ticks = fields
            .get(11)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            + fields
                .get(12)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
        threads.insert(
            (name, entry.file_name().to_string_lossy().into_owned()),
            ticks,
        );
    }
    threads
}

struct Usage {
    cpu_us: u64,
    voluntary: u64,
    involuntary: u64,
}

fn usage() -> Usage {
    let mut raw: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut raw) };
    let us = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64;
    Usage {
        cpu_us: us(raw.ru_utime) + us(raw.ru_stime),
        voluntary: raw.ru_nvcsw as u64,
        involuntary: raw.ru_nivcsw as u64,
    }
}

fn poll_counts() -> BTreeMap<String, (u64, u64)> {
    gonvex_profiler::snapshot()
        .into_iter()
        .filter(|stage| stage.name().starts_with("poll."))
        .map(|stage| (stage.name().to_owned(), (stage.count, stage.sum)))
        .fold(BTreeMap::new(), |mut map, (name, (count, sum))| {
            let entry: &mut (u64, u64) = map.entry(name).or_default();
            entry.0 += count;
            entry.1 += sum;
            map
        })
}

async fn server_main() {
    let connections: usize = env("CONN_IDLE_CONNECTIONS", 1000);
    let shares: Vec<f64> = std::env::var("CONN_IDLE_SHARES")
        .unwrap_or_else(|_| "39,39,11,6,2,2,1".to_owned())
        .split(',')
        .filter_map(|share| share.trim().parse().ok())
        .collect();
    let warmup: u64 = env("CONN_IDLE_WARMUP", 10);
    let seconds: u64 = env("CONN_IDLE_SECONDS", 60);
    let counts = split(connections, &shares);

    let root = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| ".".into())
        .join(".cache/gonvex-conn-idle");
    std::fs::create_dir_all(&root).unwrap();
    let data = tempfile::tempdir_in(&root).unwrap();
    let control_url = gonvex_store::database_url(&data.path().join("control.db"));
    let started = Instant::now();
    let tokens = provision(data.path(), &control_url, &counts).await;
    eprintln!(
        "conn_idle: provisioned {} members in {:?}",
        tokens.len(),
        started.elapsed()
    );

    let config = config(data.path(), control_url);
    let runtime_presence = (
        config.presence.tick.as_millis() as u64,
        config.presence.leading_edge,
    );
    let tuning = config.connection.socket;
    let runtime = Runtime::new(config);
    runtime.start().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = runtime.router();
    tokio::spawn(async move {
        axum::serve(
            TunedListener::new(listener, tuning),
            router.into_make_service_with_connect_info::<AcceptedSocket>(),
        )
        .await
        .unwrap();
    });

    let tokens_path = data.path().join("tokens.tsv");
    std::fs::write(
        &tokens_path,
        tokens
            .iter()
            .map(|(token, tenant)| format!("{token}\t{tenant}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .env("CONN_IDLE_ROLE", "client")
        .env("CONN_IDLE_ADDR", address.to_string())
        .env("CONN_IDLE_TOKENS", &tokens_path)
        .env(
            "CONN_IDLE_CLIENT_SECONDS",
            (warmup + seconds + 600).to_string(),
        )
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (lines_tx, mut lines) = tokio::sync::mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });
    let connect_started = Instant::now();
    loop {
        let line = lines.recv().await.expect("client exited before READY");
        if line.starts_with("READY") {
            eprintln!(
                "conn_idle: client {line} after {:?}",
                connect_started.elapsed()
            );
            break;
        }
    }
    tokio::time::sleep(Duration::from_secs(warmup)).await;

    unsafe { libc::kill(child.id() as i32, libc::SIGUSR2) };
    let polls_before = poll_counts();
    let threads_before = thread_cpu();
    let usage_before = usage();
    let wall = Instant::now();
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let elapsed = wall.elapsed().as_secs_f64();
    let usage_after = usage();
    let threads_after = thread_cpu();
    let polls_after = poll_counts();

    // Client stats, then stop it.
    unsafe { libc::kill(child.id() as i32, libc::SIGUSR1) };
    let mut client_stats = Value::Null;
    if let Ok(Some(line)) = tokio::time::timeout(Duration::from_secs(10), lines.recv()).await {
        client_stats = serde_json::from_str(&line).unwrap_or(Value::String(line));
    }
    let _ = child.kill();
    let _ = child.wait();

    let cpu_us = usage_after.cpu_us - usage_before.cpu_us;
    let per_conn_us_s = cpu_us as f64 / elapsed / connections as f64;
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    // Threads that exited during the window are not counted here.
    let mut by_name = BTreeMap::<String, u64>::new();
    for ((name, tid), after) in &threads_after {
        let before = threads_before
            .get(&(name.clone(), tid.clone()))
            .copied()
            .unwrap_or(0);
        *by_name.entry(name.clone()).or_default() += after.saturating_sub(before);
    }
    let mut threads = serde_json::Map::new();
    for (name, delta) in by_name {
        if delta > 0 {
            threads.insert(
                name,
                json!((delta as f64 / ticks / elapsed * 1000.0).round() / 1000.0),
            );
        }
    }
    let mut polls = serde_json::Map::new();
    for (name, (count, sum)) in &polls_after {
        let (count0, sum0) = polls_before.get(name).copied().unwrap_or_default();
        let count = count - count0;
        if count > 0 {
            polls.insert(
                name.clone(),
                json!({
                    "perConnPerS": (count as f64 / elapsed / connections as f64 * 1000.0).round() / 1000.0,
                    "meanUs": ((sum - sum0) as f64 / count as f64 / 100.0).round() / 10.0,
                }),
            );
        }
    }
    let result = json!({
        "bench": "conn_idle",
        "label": std::env::var("CONN_IDLE_LABEL").unwrap_or_default(),
        "connections": connections,
        "tenants": counts,
        "seconds": (elapsed * 10.0).round() / 10.0,
        "transitionMeanS": env::<f64>("CONN_IDLE_TRANSITION_S", 0.0),
        "reconnectPerS": env::<f64>("CONN_IDLE_RECONNECT_PER_S", 0.0),
        "batchFrames": env::<u8>("CONN_IDLE_BATCH_FRAMES", 0),
        "presenceTickMs": runtime_presence.0,
        "presenceLeadingEdge": runtime_presence.1,
        "cores": (cpu_us as f64 / elapsed / 1e6 * 10000.0).round() / 10000.0,
        "usPerConnPerS": (per_conn_us_s * 100.0).round() / 100.0,
        "voluntaryCsPerS": ((usage_after.voluntary - usage_before.voluntary) as f64 / elapsed).round(),
        "involuntaryCsPerS": ((usage_after.involuntary - usage_before.involuntary) as f64 / elapsed).round(),
        "threadCores": threads,
        "polls": polls,
        "client": client_stats,
    });
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    if let Ok(path) = std::env::var("BENCH_JSON_OUT") {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{result}").unwrap();
    }
    runtime.shutdown().await;
    std::process::exit(0);
}

// ---------------------------------------------------------------- client ---

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Default)]
struct ClientStats {
    /// Set by SIGUSR2 at the start of the measured window.
    measuring: std::sync::atomic::AtomicBool,
    window_frames: AtomicU64,
    window_presence_frames: AtomicU64,
    window_presence_changes: AtomicU64,
    window_transitions: AtomicU64,
    /// Per Member index: the latest state it sent (true = idle) and when.
    sent: std::sync::Mutex<Vec<Option<(bool, Instant)>>>,
    /// Delivery latencies in µs, recorded while measuring.
    latencies: std::sync::Mutex<Vec<u32>>,
    frames: AtomicU64,
    presence_frames: AtomicU64,
    pings: AtomicU64,
    transitions: AtomicU64,
    reconnects: AtomicU64,
    auth_errors: AtomicU64,
    closed: AtomicU64,
}

fn client_main() {
    // Run on every CPU except the server's.
    unsafe {
        let mut parent: libc::cpu_set_t = std::mem::zeroed();
        libc::sched_getaffinity(
            libc::getppid(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &mut parent,
        );
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        let cpus = libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as usize;
        let mut any = false;
        for cpu in 0..cpus {
            if !libc::CPU_ISSET(cpu, &parent) {
                libc::CPU_SET(cpu, &mut set);
                any = true;
            }
        }
        if any {
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(client_run());
}

async fn connect(address: &str, token: &str, tenant: &str, stats: &ClientStats) -> Option<Socket> {
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
        .await
        .ok()?;
    let mut authed = false;
    while let Some(Ok(frame)) = socket.next().await {
        let Message::Text(text) = frame else { continue };
        let value: Value = serde_json::from_str(&text).ok()?;
        match value["type"].as_str() {
            Some("session.ready") => {
                let mut capabilities = json!({"preserveSubscriptions":1,"presence":1});
                if env::<u8>("CONN_IDLE_BATCH_FRAMES", 0) == 1 {
                    capabilities["replicaBatchFrames"] = json!(1);
                }
                let auth = json!({"type":"auth","id":"auth","token":token,"project":"project","tenant":tenant,
                    "capabilities":capabilities});
                socket
                    .send(Message::Text(auth.to_string().into()))
                    .await
                    .ok()?;
            }
            Some("auth.result") => {
                authed = true;
                break;
            }
            Some("auth.error") => {
                stats.auth_errors.fetch_add(1, Relaxed);
                return None;
            }
            _ => {}
        }
    }
    if !authed {
        return None;
    }
    for frame in [
        json!({"type":"presence.set","state":"active"}),
        json!({"type":"presence.subscribe","id":"presence-1"}),
    ] {
        socket
            .send(Message::Text(frame.to_string().into()))
            .await
            .ok()?;
    }
    Some(socket)
}

/// Records the delivery latency of every presence change in a received
/// frame (bare `presence.patch` or `replica.batch` items).
fn record_presence(text: &str, stats: &ClientStats) {
    let received = Instant::now();
    let Ok(frame) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let patches: Vec<&Value> = match frame["type"].as_str() {
        Some("presence.patch") => vec![&frame],
        Some("replica.batch") => frame["items"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter(|item| item["type"] == "presence.patch")
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    if patches.is_empty() {
        return;
    }
    stats.window_presence_frames.fetch_add(1, Relaxed);
    let mut latencies = Vec::new();
    {
        let sent = stats.sent.lock().unwrap();
        for change in patches
            .iter()
            .filter_map(|patch| patch["changes"].as_array())
            .flatten()
        {
            stats.window_presence_changes.fetch_add(1, Relaxed);
            let Some(index) = change["memberId"]
                .as_str()
                .and_then(|id| id.strip_prefix("member_"))
                .and_then(|id| id.parse::<usize>().ok())
            else {
                continue;
            };
            let idle = match change["state"].as_str() {
                Some("idle") => true,
                Some("active") => false,
                _ => continue,
            };
            if let Some(Some((sent_idle, at))) = sent.get(index) {
                if *sent_idle == idle {
                    latencies.push(received.saturating_duration_since(*at).as_micros() as u32);
                }
            }
        }
    }
    stats.latencies.lock().unwrap().extend(latencies);
}

fn percentile_ms(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    (sorted[index] as f64 / 100.0).round() / 10.0
}

/// Exponential delay with the given mean (seconds), from a per-task LCG.
fn exp_delay(state: &mut u64, mean: f64) -> Duration {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let uniform = ((*state >> 11) as f64 / (1u64 << 53) as f64).max(1e-9);
    Duration::from_secs_f64(-uniform.ln() * mean)
}

async fn client_run() {
    let address = std::env::var("CONN_IDLE_ADDR").unwrap();
    let tokens: Vec<(String, String)> =
        std::fs::read_to_string(std::env::var("CONN_IDLE_TOKENS").unwrap())
            .unwrap()
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .map(|(token, tenant)| (token.to_owned(), tenant.to_owned()))
            .collect();
    let transition_mean: f64 = env("CONN_IDLE_TRANSITION_S", 0.0);
    let reconnect_total: f64 = env("CONN_IDLE_RECONNECT_PER_S", 0.0);
    let reconnect_mean = (reconnect_total > 0.0).then(|| tokens.len() as f64 / reconnect_total);
    let stats = Arc::new(ClientStats::default());
    *stats.sent.lock().unwrap() = vec![None; tokens.len()];
    let ready = Arc::new(tokio::sync::Semaphore::new(64));
    let (up_tx, mut up_rx) = tokio::sync::mpsc::unbounded_channel::<bool>();
    for (index, (token, tenant)) in tokens.iter().cloned().enumerate() {
        let address = address.clone();
        let stats = stats.clone();
        let ready = ready.clone();
        let up_tx = up_tx.clone();
        tokio::spawn(async move {
            let mut rng =
                0x9e37_79b9_7f4a_7c15u64 ^ (index as u64 + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            let mut first = true;
            loop {
                let socket = {
                    let _permit = ready.acquire().await.unwrap();
                    let connecting = Instant::now();
                    let socket = connect(&address, &token, &tenant, &stats).await;
                    if socket.is_some() {
                        // `connect` announced `active`.
                        stats.sent.lock().unwrap()[index] = Some((false, connecting));
                    }
                    socket
                };
                if first {
                    let _ = up_tx.send(socket.is_some());
                    first = false;
                } else {
                    stats.reconnects.fetch_add(1, Relaxed);
                }
                let Some(mut socket) = socket else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                let mut state_idle = false;
                let far = Duration::from_secs(1 << 30);
                let mut transition_at = tokio::time::Instant::now()
                    + if transition_mean > 0.0 {
                        exp_delay(&mut rng, transition_mean)
                    } else {
                        far
                    };
                let reconnect_at = tokio::time::Instant::now()
                    + reconnect_mean.map_or(far, |mean| exp_delay(&mut rng, mean));
                loop {
                    tokio::select! {
                        frame = socket.next() => match frame {
                            Some(Ok(Message::Text(text))) => {
                                stats.frames.fetch_add(1, Relaxed);
                                if text.contains("\"presence.") {
                                    stats.presence_frames.fetch_add(1, Relaxed);
                                }
                                if stats.measuring.load(Relaxed) {
                                    stats.window_frames.fetch_add(1, Relaxed);
                                    if text.contains("\"presence.patch\"") {
                                        record_presence(&text, &stats);
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(_))) => { stats.pings.fetch_add(1, Relaxed); }
                            Some(Ok(_)) => {}
                            _ => { stats.closed.fetch_add(1, Relaxed); break; }
                        },
                        _ = tokio::time::sleep_until(transition_at) => {
                            state_idle = !state_idle;
                            let frame = json!({"type":"presence.set","state": if state_idle { "idle" } else { "active" }});
                            stats.sent.lock().unwrap()[index] = Some((state_idle, Instant::now()));
                            if socket.send(Message::Text(frame.to_string().into())).await.is_err() { break; }
                            if stats.measuring.load(Relaxed) {
                                stats.window_transitions.fetch_add(1, Relaxed);
                            }
                            stats.transitions.fetch_add(1, Relaxed);
                            transition_at = tokio::time::Instant::now() + exp_delay(&mut rng, transition_mean);
                        }
                        _ = tokio::time::sleep_until(reconnect_at) => {
                            let _ = socket.close(None).await;
                            break;
                        }
                    }
                }
            }
        });
    }
    let mut up = 0usize;
    let mut failed = 0usize;
    while up + failed < tokens.len() {
        match up_rx.recv().await {
            Some(true) => up += 1,
            Some(false) => failed += 1,
            None => break,
        }
    }
    println!("READY up={up} failed={failed}");
    let _ = std::io::stdout().flush();
    let mut signal =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1()).unwrap();
    let mut start =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2()).unwrap();
    let seconds: u64 = env("CONN_IDLE_CLIENT_SECONDS", 3600);
    let deadline = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);
    let mut window_started = None;
    loop {
        tokio::select! {
            _ = start.recv() => {
                stats.measuring.store(true, Relaxed);
                window_started = Some(Instant::now());
            }
            _ = signal.recv() => break,
            _ = &mut deadline => break,
        }
    }
    stats.measuring.store(false, Relaxed);
    let window_s = window_started
        .map_or(0.0, |at| at.elapsed().as_secs_f64())
        .max(1e-9);
    let mut latencies = std::mem::take(&mut *stats.latencies.lock().unwrap());
    latencies.sort_unstable();
    let load = |counter: &AtomicU64| counter.load(Relaxed);
    let per_s = |counter: &AtomicU64| (load(counter) as f64 / window_s * 10.0).round() / 10.0;
    let latency = json!({
        "count": latencies.len(),
        "p50": percentile_ms(&latencies, 0.50),
        "p95": percentile_ms(&latencies, 0.95),
        "p99": percentile_ms(&latencies, 0.99),
        "max": percentile_ms(&latencies, 1.0),
        "mean": if latencies.is_empty() { 0.0 } else {
            (latencies.iter().map(|v| *v as f64).sum::<f64>() / latencies.len() as f64 / 100.0).round() / 10.0
        },
    });
    println!(
        "{}",
        json!({
            "up": up, "failed": failed,
            "frames": load(&stats.frames), "presenceFrames": load(&stats.presence_frames),
            "pings": load(&stats.pings), "transitions": load(&stats.transitions),
            "reconnects": load(&stats.reconnects), "authErrors": load(&stats.auth_errors),
            "closed": load(&stats.closed),
            "windowS": (window_s * 10.0).round() / 10.0,
            "framesPerS": per_s(&stats.window_frames),
            "presenceFramesPerS": per_s(&stats.window_presence_frames),
            "presenceChangesPerS": per_s(&stats.window_presence_changes),
            "transitionsPerS": per_s(&stats.window_transitions),
            "latencyMs": latency,
        })
    );
    let _ = std::io::stdout().flush();
    std::process::exit(0);
}
