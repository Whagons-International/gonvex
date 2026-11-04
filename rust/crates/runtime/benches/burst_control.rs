//! Burst micro-driver: overload behaviour of one runtime pinned to one CPU.
//!
//! Real WebSockets through the runtime's router into the perf fixture (9,000
//! task tenant, fake module host). The runtime (its tokio worker, the offload
//! pool, SQLite threads) is pinned to one CPU (`BURST_SERVER_CPU`, default 2),
//! so `available_parallelism` is 1 as in the 1-vCPU container (2 snapshot
//! permits, 1 offload thread). Clients run on other CPUs (`BURST_CLIENT_CPUS`).
//!
//! Users are e2e test members; `BURST_ADMIN_SHARE` (default 0.1) of them get
//! the fixture Admin's role and permissions (they see all 9,000 tasks, so
//! their login snapshots are heavy); the others see no task rows.
//!
//! Load (the 1k lab pattern in miniature): `BURST_USERS` active users hold the
//! login set (10 collections) and loop: think (exponential, mean
//! `BURST_THINK_MS`), then either open a task detail window (8 keyed
//! collections) *and* save that task right away (as a "record view" does), or
//! only save. `BURST_OBSERVERS` idle users hold the login set and only
//! receive. At 30 % of the run `BURST_RECONNECT` of the active users drop
//! their socket and sign in again (10 cold opens each: the reconnect storm).
//!
//! Measured (client side, milliseconds): save = call sent -> result and a
//! covering watermark (what the SDK waits for); delivery = a commit's result
//! -> watermark >= its revision on every other socket (idle observers and
//! active users separately); open = detail window sent -> all 8 ready;
//! reconnect = sign-in sent -> 10 ready. Plus server stages from the profiler.
//! Watermark-only batches flush at the tick (`GONVEX_REPLICA_BATCH_IDLE_MS=50`)
//! so the watermark measures delivery, not the idle delay.
//!
//! `BURST_JSON_OUT=path` appends one JSON line per run.
//! `cargo bench -p gonvex-runtime --features perf-fixture --bench burst_control`
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::http::Request;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use gonvex_runtime::accept::{AcceptedSocket, TunedListener};
use gonvex_runtime::control::ControlConnection;
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberKind, SqlValue, WHAGONS_DETAIL_WINDOW, WHAGONS_SUBSCRIPTIONS,
};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const ADMIN_KEY: &str = "burst-control-admin-key";
const PASSWORD: &str = "correct horse battery staple";

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

/// (busy, total) jiffies of `cpu` from /proc/stat.
fn cpu_jiffies(cpu: usize) -> (u64, u64) {
    let stat = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let prefix = format!("cpu{cpu} ");
    let Some(line) = stat.lines().find(|line| line.starts_with(&prefix)) else {
        return (0, 0);
    };
    let values: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    let total: u64 = values.iter().take(8).sum();
    let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
    (total - idle, total)
}

/// CPU seconds (user + system) of this process's threads allowed only on
/// `cpu` (the runtime's threads: they inherit the server pinning).
fn server_threads_cpu_s(cpu: usize) -> f64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let wanted = cpu.to_string();
    let mut total = 0u64;
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0.0;
    };
    for task in tasks.flatten() {
        let path = task.path();
        let Ok(status) = std::fs::read_to_string(path.join("status")) else {
            continue;
        };
        let allowed = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
            .map(str::trim);
        if allowed != Some(wanted.as_str()) {
            continue;
        }
        let Ok(stat) = std::fs::read_to_string(path.join("stat")) else {
            continue;
        };
        // Fields after the parenthesised name: utime is 14th, stime 15th.
        let after = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
        let fields: Vec<&str> = after.split_whitespace().collect();
        let utime: u64 = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
        let stime: u64 = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
        total += utime + stime;
    }
    total as f64 / ticks
}

fn pin_to(cpus: &[usize]) {
    // SAFETY: plain affinity syscall on the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set),
            0,
            "sched_setaffinity {cpus:?}"
        );
    }
}

/// Small deterministic PRNG (xorshift64*), one per user.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn exp_ms(&mut self, mean: f64) -> Duration {
        Duration::from_secs_f64(-(1.0 - self.unit()).ln() * mean / 1000.0)
    }
}

#[derive(Default)]
struct SocketState {
    watermark: u64,
    streams: HashMap<String, String>,
    ready: HashSet<String>,
    errors: HashSet<String>,
    results: HashMap<String, Result<u64, String>>,
    /// (watermark, ms since start) whenever the watermark advanced.
    marks: Vec<(u64, f64)>,
    resets: usize,
    closed: bool,
}

struct Connection {
    sink: tokio::sync::Mutex<SplitSink<Socket, Message>>,
    state: Mutex<SocketState>,
    changed: tokio::sync::Notify,
    started_ms: f64,
    reader: tokio::task::JoinHandle<()>,
}

/// One socket generation of a user: (connection, from ms, until ms).
type Span = (Arc<Connection>, f64, f64);

struct Clock(Instant);
impl Clock {
    fn ms(&self) -> f64 {
        self.0.elapsed().as_secs_f64() * 1000.0
    }
}

fn frames_of(text: &str) -> Vec<Value> {
    let Ok(frame) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    if frame["type"] != "replica.batch" {
        return vec![frame];
    }
    let mut frames: Vec<Value> = frame["items"].as_array().cloned().unwrap_or_default();
    if let Some(revision) = frame["watermark"].as_u64() {
        frames.push(json!({"type":"replica.watermark","revision":revision}));
    }
    frames
}

fn apply(state: &mut SocketState, frame: &Value, now: f64) {
    let id = |state: &SocketState| -> Option<String> {
        let id = frame["id"].as_str()?;
        Some(
            state
                .streams
                .get(id)
                .cloned()
                .unwrap_or_else(|| id.to_owned()),
        )
    };
    match frame["type"].as_str().unwrap_or("") {
        "replica.stream" => {
            if let (Some(id), Some(stream)) = (frame["id"].as_str(), frame["stream"].as_str()) {
                state.streams.insert(stream.to_owned(), id.to_owned());
            }
        }
        "replica.ready" => {
            if let Some(id) = id(state) {
                state.ready.insert(id);
            }
        }
        "replica.readyMany" => {
            for item in frame["ready"].as_array().into_iter().flatten() {
                if let Some(id) = item["id"].as_str() {
                    let id = state
                        .streams
                        .get(id)
                        .cloned()
                        .unwrap_or_else(|| id.to_owned());
                    state.ready.insert(id);
                }
            }
        }
        "replica.error" => {
            if let Some(id) = id(state) {
                state.errors.insert(id);
            }
        }
        "replica.reset" => state.resets += 1,
        "replica.watermark" => {
            if let Some(revision) = frame["revision"].as_u64() {
                if revision > state.watermark {
                    state.watermark = revision;
                    state.marks.push((revision, now));
                }
            }
        }
        "reducer.result" => {
            if let Some(id) = frame["id"].as_str() {
                let revision = frame["committedRevision"].as_u64().unwrap_or(0);
                state.results.insert(id.to_owned(), Ok(revision));
            }
        }
        "reducer.error" => {
            if let Some(id) = frame["id"].as_str() {
                state
                    .results
                    .insert(id.to_owned(), Err(frame["error"].to_string()));
            }
        }
        _ => {}
    }
}

impl Connection {
    async fn connect(
        address: std::net::SocketAddr,
        token: &str,
        project: &str,
        tenant: &str,
        clock: Arc<Clock>,
    ) -> Arc<Self> {
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
            .await
            .expect("websocket connect");
        let (mut sink, mut stream): (_, SplitStream<Socket>) = socket.split();
        sink.send(Message::Text(
            json!({"type":"auth","id":"auth","token":token,"project":project,"tenant":tenant,
                   "capabilities":{"preserveSubscriptions":1,"replicaBatchFrames":1,"replicaWatermark":1}})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        // Wait for auth.result before handing the socket out.
        loop {
            let Some(Ok(Message::Text(text))) = stream.next().await else {
                panic!("socket closed before auth")
            };
            let frames = frames_of(&text);
            if frames.iter().any(|f| f["type"] == "auth.error") {
                panic!("auth failed: {text}");
            }
            if frames.iter().any(|f| f["type"] == "auth.result") {
                break;
            }
        }
        let started_ms = clock.ms();
        let connection = Arc::new_cyclic(|weak: &std::sync::Weak<Connection>| {
            let weak = weak.clone();
            let clock = clock.clone();
            let reader = tokio::spawn(async move {
                while let Some(Ok(message)) = stream.next().await {
                    let Message::Text(text) = message else {
                        continue;
                    };
                    let Some(connection) = weak.upgrade() else {
                        return;
                    };
                    let now = clock.ms();
                    {
                        let mut state = connection.state.lock().unwrap();
                        for frame in frames_of(&text) {
                            apply(&mut state, &frame, now);
                        }
                    }
                    connection.changed.notify_waiters();
                }
                if let Some(connection) = weak.upgrade() {
                    connection.state.lock().unwrap().closed = true;
                    connection.changed.notify_waiters();
                }
            });
            Connection {
                sink: tokio::sync::Mutex::new(sink),
                state: Mutex::new(SocketState::default()),
                changed: tokio::sync::Notify::new(),
                started_ms,
                reader,
            }
        });
        connection
    }

    async fn send(&self, value: Value) {
        let _ = self
            .sink
            .lock()
            .await
            .send(Message::Text(value.to_string().into()))
            .await;
    }

    /// Waits until `done` holds (true) or `timeout` passes (false).
    async fn until(&self, timeout: Duration, mut done: impl FnMut(&SocketState) -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().unwrap();
                if done(&state) {
                    return true;
                }
                if state.closed {
                    return false;
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    async fn open_all(&self, opens: &[(String, String, Value)]) -> bool {
        {
            let mut state = self.state.lock().unwrap();
            for (id, _, _) in opens {
                state.ready.remove(id);
                state.errors.remove(id);
            }
        }
        for (id, path, args) in opens {
            self.send(json!({"type":"replica.open","id":id,"path":path,"args":args}))
                .await;
        }
        let ids: Vec<String> = opens.iter().map(|(id, _, _)| id.clone()).collect();
        self.until(Duration::from_secs(30), |state| {
            ids.iter()
                .all(|id| state.ready.contains(id) || state.errors.contains(id))
        })
        .await
    }

    async fn close_all(&self, ids: &[String]) {
        for id in ids {
            self.send(json!({"type":"replica.close","id":id})).await;
        }
        let mut state = self.state.lock().unwrap();
        for id in ids {
            state.ready.remove(id);
        }
    }

    /// Sends a save and waits for its result and a covering watermark.
    /// Returns (latency ms, committed revision, result time) on success.
    async fn save(&self, id: String, task: &str, clock: &Clock) -> Option<(f64, u64, f64)> {
        let started = clock.ms();
        self.send(
            json!({"type":"reducer.call","id":id,"path":"tasks.updateDescription",
                         "args":{"taskId":task,"description":format!("burst {id}")},
                         "scope":"tenant","idempotencyKey":id}),
        )
        .await;
        let mut result_at = None;
        let mut revision = None;
        let ok = self
            .until(Duration::from_secs(60), |state| {
                match state.results.get(&id) {
                    Some(Ok(committed)) => {
                        revision = Some(*committed);
                        if result_at.is_none() {
                            result_at = Some(clock.ms());
                        }
                        state.watermark >= *committed
                    }
                    Some(Err(_)) => true,
                    None => false,
                }
            })
            .await;
        let result = self.state.lock().unwrap().results.remove(&id);
        match (ok, result) {
            (true, Some(Ok(_))) => Some((clock.ms() - started, revision?, result_at?)),
            _ => None,
        }
    }
}

#[derive(Default)]
struct Samples {
    save: Vec<f64>,
    /// Saves sent together with a detail window open.
    save_window: Vec<f64>,
    save_errors: usize,
    open: Vec<f64>,
    open_timeouts: usize,
    open_errors: usize,
    reconnect: Vec<f64>,
    reconnect_timeouts: usize,
    /// (revision, result time ms)
    commits: Vec<(u64, f64)>,
}

fn quantiles(values: &mut [f64]) -> Value {
    if values.is_empty() {
        return json!({"n":0});
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let at =
        |q: f64| values[((q * values.len() as f64).ceil() as usize).clamp(1, values.len()) - 1];
    json!({"n":values.len(),"p50":at(0.5).round(),"p95":at(0.95).round(),"p99":at(0.99).round(),"max":at(1.0).round()})
}

/// Delivery latencies of `commits` on a socket's watermark marks, for
/// commits made while it was connected (from `from_ms`, up to `until_ms`).
fn delivery(
    marks: &[(u64, f64)],
    commits: &[(u64, f64)],
    from_ms: f64,
    until_ms: f64,
    out: &mut Vec<f64>,
    missed: &mut usize,
) {
    for (revision, at) in commits {
        if *at < from_ms + 500.0 || *at > until_ms {
            continue;
        }
        let index = marks.partition_point(|(mark, _)| mark < revision);
        match marks.get(index) {
            Some((_, seen)) => out.push((seen - at).max(0.0)),
            None => *missed += 1,
        }
    }
}

async fn provision(fixture: &Fixture, email: &str, admin: bool) -> String {
    let route = &fixture.tenants[0].route;
    let response = fixture
        .runtime
        .router()
        .oneshot(
            Request::post("/dev/internal/e2e/members")
                .header("authorization", format!("Bearer {ADMIN_KEY}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"projectId": route.project_id, "tenantId": route.tenant_id, "email": email,
                           "name": email, "password": PASSWORD})
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 << 10).await.unwrap();
    assert!(status.is_success(), "{}", String::from_utf8_lossy(&body));
    if admin {
        // Sees every task, like the fixture's Admins (heavy login snapshots).
        let member_id = serde_json::from_slice::<Value>(&body).unwrap()["memberId"]
            .as_str()
            .unwrap()
            .to_owned();
        let template = fixture.members_of_kind(0, MemberKind::Admin)[0];
        let template = &fixture.tenants[0].members[template];
        fixture
            .commit(
                0,
                "UPDATE members SET role = $1, permissions = $2 WHERE id = $3",
                vec![
                    SqlValue::Text(template.role.clone()),
                    SqlValue::Text(template.permissions.to_string()),
                    SqlValue::Text(member_id),
                ],
            )
            .await
            .unwrap();
    }
    let login = fixture
        .runtime
        .execute_control_action(
            &ControlConnection {
                project_id: route.project_id.clone(),
                ..ControlConnection::default()
            },
            "control.auth.passwordLogin",
            &json!({"email": email, "password": PASSWORD}),
            &format!("login-{email}"),
        )
        .await
        .unwrap();
    login["accessToken"].as_str().unwrap().to_owned()
}

fn login_set(generation: usize) -> Vec<(String, String, Value)> {
    WHAGONS_SUBSCRIPTIONS
        .iter()
        .map(|path| (format!("{path}#{generation}"), path.to_string(), json!({})))
        .collect()
}

fn main() {
    let server_cpu: usize = env("BURST_SERVER_CPU", 2);
    let client_cpus: Vec<usize> = std::env::var("BURST_CLIENT_CPUS")
        .unwrap_or_else(|_| "3,4,5".into())
        .split(',')
        .filter_map(|cpu| cpu.trim().parse().ok())
        .collect();
    let users: usize = env("BURST_USERS", 60);
    let observers: usize = env("BURST_OBSERVERS", 40);
    let seconds: f64 = env("BURST_SECONDS", 30.0);
    let think_ms: f64 = env("BURST_THINK_MS", 2000.0);
    let window_share: f64 = env("BURST_WINDOW_SHARE", 0.5);
    let reconnect: f64 = env("BURST_RECONNECT", 0.5);
    let seed: u64 = env("BURST_SEED", 7);
    let admin_share: f64 = env("BURST_ADMIN_SHARE", 0.1);
    let label: String = env("BURST_LABEL", "run".to_owned());
    for (name, value) in [
        ("GONVEX_REPLICA_BATCH_IDLE_MS", "50"),
        ("GONVEX_SQLITE_SYNCHRONOUS", "normal"),
        ("GONVEX_OFFLOAD_NICE", "10"),
        ("GONVEX_SQLITE_READER_NICE", "10"),
    ] {
        if std::env::var_os(name).is_none() {
            std::env::set_var(name, value);
        }
    }
    // Everything the server creates inherits this thread's affinity.
    pin_to(&[server_cpu]);
    gonvex_profiler::set_enabled(true);
    let server = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (fixture, address, tokens) = server.block_on(async {
        let mut options = FixtureOptions::whagons();
        for (name, value) in [
            ("GONVEX_REQUIRE_AUTH", "true"),
            ("GONVEX_ADMIN_KEY", ADMIN_KEY),
            (
                "GONVEX_DASHBOARD_SESSION_SECRET",
                "burst-control-secret-long-enough-xx",
            ),
            ("GONVEX_AUTH_URL", "http://localhost:8080"),
            ("GONVEX_PENDING_CONNECTIONS_PER_IP", "100000"),
            ("GONVEX_PUBLIC_CONNECTIONS_PER_IP", "100000"),
        ] {
            options.config.insert(name.to_owned(), value.to_owned());
        }
        let fixture = Fixture::build(options).await.unwrap();
        let mut tokens = Vec::new();
        for user in 0..users + observers {
            // Every 1/share-th user (active and observers alike) is an Admin.
            let admin =
                admin_share > 0.0 && user % ((1.0 / admin_share).round() as usize).max(1) == 0;
            tokens.push(provision(&fixture, &format!("burst-{user}@example.test"), admin).await);
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = fixture.runtime.router();
        tokio::spawn(async move {
            axum::serve(
                TunedListener::new(listener, Default::default()),
                router.into_make_service_with_connect_info::<AcceptedSocket>(),
            )
            .await
            .unwrap();
        });
        (fixture, address, tokens)
    });
    let route = fixture.tenants[0].route.clone();
    let tasks: Arc<Vec<String>> = Arc::new(
        fixture.tenants[0]
            .tasks
            .iter()
            .map(|t| t.id.clone())
            .collect(),
    );
    // Keep the server runtime driving on its own thread while clients run.
    let (stop_server, server_stopped) = tokio::sync::oneshot::channel::<()>();
    let server_thread = std::thread::spawn(move || {
        server.block_on(async {
            let _ = server_stopped.await;
        });
        (server, fixture)
    });

    let client_cpus_for_threads = client_cpus.clone();
    let client = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(client_cpus.len().max(1))
        .enable_all()
        .on_thread_start(move || pin_to(&client_cpus_for_threads))
        .build()
        .unwrap();
    pin_to(&client_cpus);
    let report = client.block_on(async move {
        let clock = Arc::new(Clock(Instant::now()));
        let project = route.project_id.clone();
        let tenant = route.tenant_id.clone();
        // Sign everyone in (the initial login burst, not measured).
        let mut connections = Vec::new();
        let mut handles = Vec::new();
        for token in tokens.iter() {
            let (token, project, tenant, clock) = (token.clone(), project.clone(), tenant.clone(), clock.clone());
            handles.push(tokio::spawn(async move {
                let connection = Connection::connect(address, &token, &project, &tenant, clock).await;
                assert!(connection.open_all(&login_set(0)).await, "initial login timed out");
                connection
            }));
        }
        for handle in handles {
            connections.push(handle.await.unwrap());
        }
        let signed_in_ms = clock.ms();
        let baseline: Vec<_> = gonvex_profiler::snapshot();
        let cpu_before = cpu_jiffies(server_cpu);
        let threads_before = server_threads_cpu_s(server_cpu);
        let shares_before = gonvex_runtime::feed_share::metrics();
        let decisions_before = gonvex_runtime::feed_share::shared_decisions();
        let batches_before = gonvex_runtime::delivery::lag_batches();
        let samples = Arc::new(Mutex::new(Samples::default()));
        let start = clock.ms();
        let end = start + seconds * 1000.0;
        let reconnect_at = start + seconds * 300.0;
        // Active users. The observers are the connections after `users`.
        let mut tasks_running = Vec::new();
        let generations: Arc<Mutex<Vec<Vec<Span>>>> = Arc::new(Mutex::new(vec![Vec::new(); users]));
        for user in 0..users {
            let mut connection = connections[user].clone();
            let (token, project, tenant, clock, samples, tasks, generations) = (
                tokens[user].clone(), project.clone(), tenant.clone(), clock.clone(), samples.clone(), tasks.clone(), generations.clone(),
            );
            let reconnects = (user as f64) < reconnect * users as f64;
            tasks_running.push(tokio::spawn(async move {
                let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9).wrapping_add(user as u64 * 7919 + 1));
                let mut reconnected = false;
                let mut generation_start = connection.started_ms;
                let mut call = 0usize;
                while clock.ms() < end {
                    let think = rng.exp_ms(think_ms);
                    tokio::time::sleep(think).await;
                    if clock.ms() >= end {
                        break;
                    }
                    if reconnects && !reconnected && clock.ms() >= reconnect_at {
                        reconnected = true;
                        generations.lock().unwrap()[user].push((connection.clone(), generation_start, clock.ms()));
                        connection.reader.abort();
                        let _ = connection.sink.lock().await.close().await;
                        let sent = clock.ms();
                        connection = Connection::connect(address, &token, &project, &tenant, clock.clone()).await;
                        generation_start = connection.started_ms;
                        let ok = connection.open_all(&login_set(1)).await;
                        let mut samples = samples.lock().unwrap();
                        if ok { samples.reconnect.push(clock.ms() - sent) } else { samples.reconnect_timeouts += 1 }
                        continue;
                    }
                    let task = tasks[(rng.next() % tasks.len() as u64) as usize].clone();
                    call += 1;
                    let call_id = format!("u{user}-c{call}");
                    if rng.unit() < window_share {
                        let opens: Vec<(String, String, Value)> = WHAGONS_DETAIL_WINDOW
                            .iter()
                            .map(|(path, arg)| (format!("{path}@{call}"), path.to_string(), json!({ *arg: task })))
                            .collect();
                        let ids: Vec<String> = opens.iter().map(|(id, _, _)| id.clone()).collect();
                        let sent = clock.ms();
                        let (opened, saved) = tokio::join!(connection.open_all(&opens), connection.save(call_id, &task, &clock));
                        let open_ms = clock.ms() - sent;
                        {
                            let mut samples = samples.lock().unwrap();
                            if opened {
                                let errors = ids.iter().filter(|id| connection.state.lock().unwrap().errors.contains(*id)).count();
                                if errors > 0 { samples.open_errors += 1 } else { samples.open.push(open_ms) }
                            } else {
                                samples.open_timeouts += 1;
                            }
                            match saved {
                                Some((ms, revision, at)) => { samples.save.push(ms); samples.save_window.push(ms); samples.commits.push((revision, at)); }
                                None => samples.save_errors += 1,
                            }
                        }
                        // Dwell a little, then close the window.
                        tokio::time::sleep(rng.exp_ms(think_ms / 2.0)).await;
                        connection.close_all(&ids).await;
                    } else {
                        let saved = connection.save(call_id, &task, &clock).await;
                        let mut samples = samples.lock().unwrap();
                        match saved {
                            Some((ms, revision, at)) => { samples.save.push(ms); samples.commits.push((revision, at)); }
                            None => samples.save_errors += 1,
                        }
                    }
                }
                generations.lock().unwrap()[user].push((connection.clone(), generation_start, clock.ms()));
            }));
        }
        for task in tasks_running {
            task.await.unwrap();
        }
        let finished = clock.ms();
        let cpu_after = cpu_jiffies(server_cpu);
        let threads_cpu = server_threads_cpu_s(server_cpu) - threads_before;
        let shares_after = gonvex_runtime::feed_share::metrics();
        let shares_created = shares_after["created"].as_u64().unwrap_or(0) - shares_before["created"].as_u64().unwrap_or(0);
        let shares_found = shares_after["found"].as_u64().unwrap_or(0) - shares_before["found"].as_u64().unwrap_or(0);
        let decisions = gonvex_runtime::feed_share::shared_decisions() - decisions_before;
        let batches = gonvex_runtime::delivery::lag_batches();
        let (lag_batches, lag_batched) = (batches.0 - batches_before.0, batches.1 - batches_before.1);
        let server_cpu_busy = (cpu_after.0 - cpu_before.0) as f64 / (cpu_after.1 - cpu_before.1).max(1) as f64;
        // Let in-flight deliveries land before reading the marks.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after = gonvex_profiler::snapshot();
        let mut samples = std::mem::take(&mut *samples.lock().unwrap());
        samples.commits.sort_by_key(|commit| commit.0);
        let mut observer_delivery = Vec::new();
        let mut observer_missed = 0;
        let mut resets = 0;
        for connection in &connections[users..] {
            let state = connection.state.lock().unwrap();
            resets += state.resets;
            delivery(&state.marks, &samples.commits, start, finished, &mut observer_delivery, &mut observer_missed);
        }
        let mut active_delivery = Vec::new();
        let mut active_missed = 0;
        for spans in generations.lock().unwrap().iter() {
            for (connection, from, until) in spans {
                let state = connection.state.lock().unwrap();
                resets += state.resets;
                delivery(&state.marks, &samples.commits, (*from).max(start), *until, &mut active_delivery, &mut active_missed);
            }
        }
        let stage = |name: &str| -> Value {
            let find = |rows: &[gonvex_profiler::StageSnapshot]| rows.iter().filter(|row| row.name() == name).max_by_key(|row| row.count).cloned();
            let Some(now) = find(&after) else { return json!(null) };
            let before = find(&baseline);
            let mut buckets: HashMap<u16, u64> = now.buckets.iter().copied().collect();
            if let Some(before) = &before {
                for (index, hits) in &before.buckets {
                    *buckets.get_mut(index).unwrap() -= hits;
                }
            }
            let mut buckets: Vec<(u16, u64)> = buckets.into_iter().filter(|(_, hits)| *hits > 0).collect();
            buckets.sort();
            let count = now.count - before.as_ref().map_or(0, |row| row.count);
            let q = |q: f64| (gonvex_profiler::quantile(&buckets, count, now.max, q) as f64 / 1e6 * 10.0).round() / 10.0;
            let sum_s = now.sum.wrapping_sub(before.as_ref().map_or(0, |row| row.sum)) as f64 / 1e9;
            json!({"n":count,"p50":q(0.5),"p90":q(0.9),"p99":q(0.99),"sum_s":(sum_s * 100.0).round() / 100.0})
        };
        let report = json!({
            "label": label,
            "users": users, "observers": observers, "admin_share": admin_share, "seconds": seconds, "think_ms": think_ms,
            "sign_in_ms": signed_in_ms.round(),
            "save": quantiles(&mut samples.save), "save_errors": samples.save_errors,
            "save_window": quantiles(&mut samples.save_window),
            "delivery_observers": quantiles(&mut observer_delivery), "delivery_observers_missed": observer_missed,
            "delivery_active": quantiles(&mut active_delivery), "delivery_active_missed": active_missed,
            "open": quantiles(&mut samples.open), "open_timeouts": samples.open_timeouts, "open_errors": samples.open_errors,
            "reconnect": quantiles(&mut samples.reconnect), "reconnect_timeouts": samples.reconnect_timeouts,
            "resets": resets,
            "server_cpu_busy": (server_cpu_busy * 1000.0).round() / 1000.0,
            // Runtime threads' CPU over the measured window, and per completed save.
            "server_cpu_s": (threads_cpu * 100.0).round() / 100.0,
            "cpu_ms_per_save": (threads_cpu * 1000.0 / samples.save.len().max(1) as f64 * 100.0).round() / 100.0,
            "shares": {"created": shares_created, "found": shares_found,
                       "found_per_created": ((shares_found as f64 / shares_created.max(1) as f64) * 10.0).round() / 10.0,
                       "shared_decisions": decisions},
            "lag_batches": {"batches": lag_batches, "events": lag_batched},
            "server": {
                "snapshot.permit_wait": stage("snapshot.permit_wait"),
                "snapshot.permit_hold": stage("snapshot.permit_hold"),
                "snapshot.grant_lag": stage("snapshot.grant_lag"),
                "snapshot.slot_hold": stage("snapshot.slot_hold"),
                "snapshot.budget_wait": stage("snapshot.budget_wait"),
                "snapshot.build": stage("snapshot.build"),
                "delivery.queue_wait": stage("delivery.queue_wait"),
                "delivery.total": stage("delivery.total"),
                "offload.queue_wait": stage("offload.queue_wait"),
                "fn.total": stage("fn.total"),
                "ws.inbound_wait": stage("ws.inbound_wait"),
                "tokio.sched_lag": stage("tokio.sched_lag"),
            },
        });
        for connection in &connections {
            connection.reader.abort();
        }
        report
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Ok(path) = std::env::var("BURST_JSON_OUT") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{}", serde_json::to_string(&report).unwrap()).unwrap();
    }
    let _ = stop_server.send(());
    let (server, fixture) = server_thread.join().unwrap();
    server.block_on(fixture.shutdown());
}
