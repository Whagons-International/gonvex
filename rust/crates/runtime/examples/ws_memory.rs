//! Per-connection WebSocket memory micro-driver (not a user simulator).
//!
//! `ws_memory server --mode transport|runtime --compression on|off --port-file F`
//!   transport: the runtime's `gonvex_websocket::upgrade` path with an echo task.
//!   runtime:   the full runtime router; clients sign in with a real native session.
//!   Both run under a counting wrapper around mimalloc and serve `/__mem`.
//! `ws_memory client --port P --server-pid PID --mode M --deflate on|off --n N --sizes 2048,65536`
//!   opens N connections, then sends one message of each size per connection and
//!   samples server heap, RSS, cgroup memory.stat and `ss -tm` skmem after each phase.
//! `ws_memory compress-cost` measures building one per-message compressor.
//!
//! See docs/performance/websocket-memory.md.
use std::{
    alloc::{GlobalAlloc, Layout},
    sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    time::{Duration, Instant},
};

struct Counting;
static LIVE: AtomicI64 = AtomicI64::new(0);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);
const BUCKETS: usize = 32;
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicI64 = AtomicI64::new(0);
static BUCKET_COUNT: [AtomicI64; BUCKETS] = [ZERO; BUCKETS];
static BUCKET_BYTES: [AtomicI64; BUCKETS] = [ZERO; BUCKETS];

fn record(size: usize, sign: i64) {
    let bucket = ((usize::BITS - size.leading_zeros()) as usize).min(BUCKETS - 1);
    let live = LIVE.fetch_add(sign * size as i64, Relaxed) + sign * size as i64;
    BUCKET_COUNT[bucket].fetch_add(sign, Relaxed);
    BUCKET_BYTES[bucket].fetch_add(sign * size as i64, Relaxed);
    if sign > 0 {
        ALLOCS.fetch_add(1, Relaxed);
        PEAK.fetch_max(live, Relaxed);
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size(), 1);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size(), 1);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(layout.size(), -1);
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            record(layout.size(), -1);
            record(new_size, 1);
        }
        new
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn arg(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .unwrap_or_else(|| default.to_owned())
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let workers = arg(&args, "--workers", "1").parse().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();
    match args.first().map(String::as_str) {
        Some("server") => runtime.block_on(server::run(&args)),
        Some("client") => runtime.block_on(client::run(&args)),
        Some("compress-cost") => compress_cost(),
        _ => eprintln!("usage: ws_memory server|client|compress-cost ..."),
    }
}

fn mem_json() -> String {
    let buckets = (0..BUCKETS)
        .map(|i| {
            format!(
                "[{},{}]",
                BUCKET_COUNT[i].load(Relaxed),
                BUCKET_BYTES[i].load(Relaxed)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let rss = std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .unwrap_or(0)
        * 1024;
    format!(
        r#"{{"live":{},"allocs":{},"rss":{},"buckets":[{}]}}"#,
        LIVE.load(Relaxed),
        ALLOCS.load(Relaxed),
        rss,
        buckets
    )
}

/// Time and allocation of one fresh per-message compressor (the current
/// `compress_text`), as the baseline for a shared "compress once" encoder.
fn compress_cost() {
    let mut seed = 7u32;
    for size in [256usize, 2048, 16384, 65536] {
        let payload = filler(size, &mut seed);
        let rounds = (4_000_000 / size).clamp(50, 20_000);
        let allocs = ALLOCS.load(Relaxed);
        let mut transient = 0i64;
        let started = Instant::now();
        let mut out = 0usize;
        for _ in 0..rounds {
            let before = LIVE.load(Relaxed);
            PEAK.store(before, Relaxed);
            let encoded = gonvex_websocket::bench_compress_text(payload.as_bytes(), 12).unwrap();
            transient = transient.max(PEAK.load(Relaxed) - before);
            out = encoded.map_or(size, |e| e.len());
        }
        let elapsed = started.elapsed();
        println!(
            "compress size={size} out={out} ratio={:.2} ns/msg={} allocs/msg={:.1} peak_transient={}B",
            size as f64 / out as f64,
            elapsed.as_nanos() / rounds as u128,
            (ALLOCS.load(Relaxed) - allocs) as f64 / rounds as f64,
            transient,
        );
    }
    // Compressor state size: allocate one flate2 compressor and hold it.
    let before = LIVE.load(Relaxed);
    let allocs = ALLOCS.load(Relaxed);
    let started = Instant::now();
    let codec = flate2::Compress::new_with_window_bits(flate2::Compression::new(1), false, 12);
    let build = started.elapsed();
    println!(
        "deflate state window=12 level=1: {} bytes in {} allocations, built in {} ns",
        LIVE.load(Relaxed) - before,
        ALLOCS.load(Relaxed) - allocs,
        build.as_nanos()
    );
    drop(codec);
    for bits in [9u8, 15] {
        let before = LIVE.load(Relaxed);
        let codec = flate2::Decompress::new_with_window_bits(false, bits);
        println!(
            "inflate state window={bits}: {} bytes (window allocated lazily on first output)",
            LIVE.load(Relaxed) - before
        );
        drop(codec);
    }
    let before = LIVE.load(Relaxed);
    let codec = flate2::Compress::new_with_window_bits(flate2::Compression::new(1), false, 15);
    println!(
        "deflate state window=15 level=1 (not negotiated, reference): {} bytes",
        LIVE.load(Relaxed) - before
    );
    drop(codec);
}

/// Pseudo-JSON identifier text: words and numbers, no characters that need escaping.
fn filler(size: usize, seed: &mut u32) -> String {
    const WORDS: [&str; 32] = [
        "task",
        "status",
        "open",
        "closed",
        "team",
        "spot",
        "member",
        "title",
        "priority",
        "high",
        "low",
        "medium",
        "due",
        "created",
        "updated",
        "owner",
        "workspace",
        "tenant",
        "category",
        "form",
        "field",
        "value",
        "approval",
        "pending",
        "done",
        "note",
        "user",
        "tag",
        "template",
        "shift",
        "board",
        "lane",
    ];
    let mut out = String::with_capacity(size + 16);
    while out.len() < size {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 17;
        *seed ^= *seed << 5;
        out.push_str(WORDS[(*seed % 32) as usize]);
        out.push('-');
        if *seed & 3 == 0 {
            out.push_str(&(*seed % 100_000).to_string());
            out.push('-');
        }
    }
    out.truncate(size);
    out
}

mod server {
    use super::*;
    use axum::{
        extract::{ws::Message, Request, State},
        response::Response,
        routing::get,
        Router,
    };

    pub async fn run(args: &[String]) {
        let mode = arg(args, "--mode", "transport");
        let compression = arg(args, "--compression", "on") == "on";
        let port_file = arg(args, "--port-file", "");
        let data = arg(args, "--data", "");
        // Production socket options and outbound limits, with overrides:
        // a 0 buffer/lowat keeps the platform default (the old behaviour).
        let mut connection = gonvex_runtime::config::ConnectionConfig::default();
        let bytes = |name: &str, default: Option<usize>| -> Option<usize> {
            let value: usize = arg(args, name, &default.unwrap_or(0).to_string()).parse().unwrap();
            (value > 0).then_some(value)
        };
        connection.socket.send_buffer = bytes("--sndbuf", connection.socket.send_buffer);
        connection.socket.receive_buffer = bytes("--rcvbuf", connection.socket.receive_buffer);
        connection.socket.notsent_lowat =
            bytes("--notsent", connection.socket.notsent_lowat.map(|v| v as usize)).map(|v| v as u32);
        connection.outbound.stall_timeout = Duration::from_secs(
            arg(args, "--stall-seconds", &connection.outbound.stall_timeout.as_secs().to_string())
                .parse()
                .unwrap(),
        );
        eprintln!("ws_memory server connection config: {connection:?}");
        let mem = Router::new().route("/__mem", get(|| async { mem_json() }));
        let (app, _guard) = match mode.as_str() {
            "transport" => (
                Router::new()
                    .route("/ws", get(transport))
                    .with_state((compression, connection.outbound))
                    .merge(mem),
                None,
            ),
            "runtime" => {
                let (runtime, token) = runtime_fixture(&data, compression, connection).await;
                std::fs::write(format!("{port_file}.token"), &token).unwrap();
                (runtime.router().merge(mem), Some(runtime))
            }
            other => panic!("unknown mode {other}"),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        std::fs::write(&port_file, port.to_string()).unwrap();
        eprintln!("ws_memory server mode={mode} compression={compression} port={port}");
        axum::serve(
            gonvex_runtime::accept::TunedListener::new(listener, connection.socket),
            app.into_make_service_with_connect_info::<gonvex_runtime::accept::AcceptedSocket>(),
        )
        .await
        .unwrap();
    }

    async fn transport(
        State((compression, outbound)): State<(bool, gonvex_websocket::OutboundLimits)>,
        accepted: Option<axum::Extension<axum::extract::ConnectInfo<gonvex_runtime::accept::AcceptedSocket>>>,
        request: Request,
    ) -> Response {
        let options = gonvex_websocket::UpgradeOptions {
            compression,
            threshold: 1024,
            max_message: 64 << 20,
            max_frame: 16 << 20,
            outbound,
            socket_fd: accepted.and_then(|accepted| accepted.0 .0.fd),
        };
        gonvex_websocket::upgrade(request, options, |mut socket| async move {
            while let Some(Ok(message)) = socket.next().await {
                match message {
                    Message::Text(text) => {
                        if socket.send(Message::Text(text)).await.is_err() {
                            return;
                        }
                    }
                    Message::Close(_) => return,
                    _ => {}
                }
            }
        })
        .await
    }

    async fn runtime_fixture(
        data: &str,
        compression: bool,
        connection: gonvex_runtime::config::ConnectionConfig,
    ) -> (gonvex_runtime::Runtime, String) {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        use gonvex_runtime::config::{Config, ModuleHostConfig, SandboxConfig, StorageConfig};
        use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
        use pbkdf2::pbkdf2_hmac;
        use sha2::Sha256;
        use std::collections::BTreeMap;
        let dir = std::path::Path::new(data);
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let url = gonvex_store::database_url(&dir.join("control.db"));
        let pools = PoolRegistry::new(PoolLimits::default());
        let fixture = pools.pool(&url).await.unwrap();
        let control = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        control
            .clone()
            .provision_tenant_database(
                TenantRoute {
                    project_id: "project".into(),
                    tenant_id: "tenant".into(),
                    database_url: url.clone(),
                },
                Vec::new(),
            )
            .await
            .unwrap();
        for sql in [
            "INSERT INTO gonvex_runtime_projects(id,name,status,auth_mode) VALUES('project','Project','active','gonvex-native')",
            "INSERT INTO accounts(id,auth_realm_id,email,name) VALUES('account','project','person@example.test','Person')",
            "INSERT INTO members(id,account_id,status) VALUES('account','account','active')",
            "INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status) VALUES('account','tenant','account','active')",
        ] {
            sqlx::query(sql).execute(&fixture).await.unwrap();
        }
        sqlx::query("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES('tenant','project','tenant','Tenant',$1,'active')").bind(&url).execute(&fixture).await.unwrap();
        let mut hash = [0u8; 32];
        pbkdf2_hmac::<Sha256>(b"password", b"salt", 1, &mut hash);
        let encoded = format!(
            "pbkdf2_sha256$1${}${}",
            URL_SAFE_NO_PAD.encode(b"salt"),
            URL_SAFE_NO_PAD.encode(hash)
        );
        sqlx::query("INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES('project','account',$1)").bind(&encoded).execute(&fixture).await.unwrap();
        let config = Config {
            addr: "127.0.0.1:0".parse().unwrap(),
            control_plane_database_url: Some(url.clone()),
            default_database_url: Some(gonvex_store::database_url(dir)),
            tenant_database_urls: BTreeMap::new(),
            project_database_urls: BTreeMap::new(),
            require_auth: true,
            websocket_compression: compression,
            websocket_compression_threshold: 1024,
            control_secret: Some("bench-control-secret-that-is-long-enough".to_owned()),
            auth_public_url: Some("http://localhost:8080".to_owned()),
            admin_key: Some("bench-admin-key".to_owned()),
            dev_sync_key: None,
            dashboard_account: None,
            dashboard_password: None,
            dashboard_auth_project_id: Some("project".to_owned()),
            google_client_id: None,
            google_client_secret: None,
            database_max_total_connections: 8,
            database_max_connections: 4,
            database_max_idle_connections: 1,
            database_idle_timeout: Duration::from_secs(60),
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
            runtime_version: "bench".to_owned(),
            sandbox: SandboxConfig::default(),
            storage: StorageConfig::default(),
            service_principals: Vec::new(),
            public_functions: Default::default(),
            presence: Default::default(),
            connection,
        };
        let runtime = gonvex_runtime::Runtime::new(config);
        runtime.start().await.unwrap();
        let login = runtime
            .execute_control_action(
                &gonvex_runtime::control::ControlConnection {
                    project_id: "project".into(),
                    ..Default::default()
                },
                "control.auth.passwordLogin",
                &serde_json::json!({"email":"person@example.test","password":"password"}),
                "bench-login",
            )
            .await
            .unwrap();
        let token = login["accessToken"].as_str().unwrap().to_owned();
        drop(control);
        fixture.close().await;
        (runtime, token)
    }
}

mod client {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::{broadcast, Semaphore},
    };

    #[derive(Clone, Debug)]
    enum Command {
        Send(usize),
        /// Send `count` messages of `size` without reading any reply: a client
        /// that stopped reading. Server replies back up into socket buffers.
        Stall(usize, usize),
        Close,
    }

    struct Shared {
        port: u16,
        deflate: bool,
        token: Option<String>,
        done: AtomicUsize,
        failed: AtomicUsize,
        negotiated: AtomicUsize,
        gate: Semaphore,
    }

    pub async fn run(args: &[String]) {
        let port: u16 = arg(args, "--port", "0").parse().unwrap();
        let pid: u32 = arg(args, "--server-pid", "0").parse().unwrap();
        let mode = arg(args, "--mode", "transport");
        let deflate = arg(args, "--deflate", "on") == "on";
        let n: usize = arg(args, "--n", "1000").parse().unwrap();
        let label = arg(args, "--label", "run");
        let settle = Duration::from_millis(arg(args, "--settle-ms", "1500").parse().unwrap());
        let sizes = arg(args, "--sizes", "2048,65536")
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<usize>().unwrap())
            .collect::<Vec<_>>();
        let token = (mode == "runtime")
            .then(|| std::fs::read_to_string(arg(args, "--token-file", "")).unwrap());
        let shared = Arc::new(Shared {
            port,
            deflate,
            token,
            done: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            negotiated: AtomicUsize::new(0),
            gate: Semaphore::new(48),
        });
        tokio::time::sleep(settle).await;
        let base = Sample::take(port, pid).await;
        report(&label, "baseline", n, &base, &base);
        let (commands, _) = broadcast::channel::<Command>(8);
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(n);
        for index in 0..n {
            let shared = Arc::clone(&shared);
            let rx = commands.subscribe();
            tasks.push(tokio::spawn(connection(index, shared, rx)));
        }
        wait(&shared, n).await;
        eprintln!(
            "{label}: {n} connections ready in {:?} (deflate negotiated on {}, failed {})",
            started.elapsed(),
            shared.negotiated.load(Ordering::Relaxed),
            shared.failed.load(Ordering::Relaxed)
        );
        tokio::time::sleep(settle).await;
        report(&label, "idle", n, &base, &Sample::take(port, pid).await);
        for size in sizes {
            shared.done.store(0, Ordering::Relaxed);
            commands.send(Command::Send(size)).unwrap();
            wait(&shared, n).await;
            tokio::time::sleep(settle).await;
            report(
                &label,
                &format!("after_{size}"),
                n,
                &base,
                &Sample::take(port, pid).await,
            );
        }
        let stall: usize = arg(args, "--stall", "0").parse().unwrap();
        if stall > 0 {
            shared.done.store(0, Ordering::Relaxed);
            commands.send(Command::Stall(65536, stall)).unwrap();
            wait(&shared, n).await;
            tokio::time::sleep(settle).await;
            report(
                &label,
                &format!("stalled_{stall}x65536"),
                n,
                &base,
                &Sample::take(port, pid).await,
            );
        }
        shared.done.store(0, Ordering::Relaxed);
        commands.send(Command::Close).unwrap();
        for task in tasks {
            let _ = task.await;
        }
        tokio::time::sleep(settle * 2).await;
        report(&label, "closed", n, &base, &Sample::take(port, pid).await);
    }

    async fn wait(shared: &Shared, n: usize) {
        loop {
            let done = shared.done.load(Ordering::Relaxed) + shared.failed.load(Ordering::Relaxed);
            if done >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn connection(
        index: usize,
        shared: Arc<Shared>,
        mut commands: broadcast::Receiver<Command>,
    ) {
        let permit = shared.gate.acquire().await.unwrap();
        let mut socket = match open(&shared, index).await {
            Ok(socket) => socket,
            Err(error) => {
                eprintln!("connection {index}: {error}");
                shared.failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        drop(permit);
        shared.done.fetch_add(1, Ordering::Relaxed);
        let mut seed = index as u32 * 2654435761 + 1;
        let mut sequence = 0;
        while let Ok(command) = commands.recv().await {
            match command {
                Command::Send(size) => {
                    let permit = shared.gate.acquire().await.unwrap();
                    sequence += 1;
                    let marker = format!("m-{index}-{sequence}-");
                    let body = filler(size.saturating_sub(160), &mut seed);
                    let text = match &shared.token {
                        Some(token) => format!(
                            r#"{{"type":"auth","id":"{marker}{body}","token":"{token}","project":"project","tenant":"tenant"}}"#
                        ),
                        None => format!(r#"{{"type":"echo","id":"{marker}","data":"{body}"}}"#),
                    };
                    let ok = socket.send(0x1, text.as_bytes()).await.is_ok()
                        && socket.until(&marker).await.is_ok();
                    drop(permit);
                    if ok {
                        shared.done.fetch_add(1, Ordering::Relaxed);
                    } else {
                        shared.failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
                Command::Stall(size, count) => {
                    for _ in 0..count {
                        sequence += 1;
                        let body = filler(size.saturating_sub(160), &mut seed);
                        let text = match &shared.token {
                            Some(token) => format!(
                                r#"{{"type":"auth","id":"s-{index}-{sequence}-{body}","token":"{token}","project":"project","tenant":"tenant"}}"#
                            ),
                            None => format!(
                                r#"{{"type":"echo","id":"s-{index}-{sequence}","data":"{body}"}}"#
                            ),
                        };
                        let write = socket.send(0x1, text.as_bytes());
                        if tokio::time::timeout(Duration::from_secs(2), write)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    shared.done.fetch_add(1, Ordering::Relaxed);
                }
                Command::Close => {
                    let _ = socket.send(0x8, &1000u16.to_be_bytes()).await;
                    let _ = tokio::time::timeout(Duration::from_secs(2), socket.drain()).await;
                    return;
                }
            }
        }
    }

    struct Socket {
        stream: TcpStream,
        deflate: bool,
    }

    async fn open(shared: &Shared, index: usize) -> std::io::Result<Socket> {
        let mut stream = TcpStream::connect(("127.0.0.1", shared.port)).await?;
        stream.set_nodelay(true)?;
        let extension = if shared.deflate {
            "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n"
        } else {
            ""
        };
        stream
            .write_all(format!("GET /ws HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n{extension}\r\n", shared.port).as_bytes())
            .await?;
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            response.push(stream.read_u8().await?);
        }
        let response = String::from_utf8_lossy(&response).to_lowercase();
        if !response.starts_with("http/1.1 101") {
            return Err(std::io::Error::other(format!(
                "handshake {index}: {response}"
            )));
        }
        let negotiated = response.contains("permessage-deflate");
        if negotiated {
            shared.negotiated.fetch_add(1, Ordering::Relaxed);
        }
        let mut socket = Socket {
            stream,
            deflate: negotiated,
        };
        if let Some(token) = &shared.token {
            socket.until("session.ready").await?;
            let id = format!("auth-{index}");
            let auth = format!(
                r#"{{"type":"auth","id":"{id}","token":"{token}","project":"project","tenant":"tenant"}}"#
            );
            socket.send(0x1, auth.as_bytes()).await?;
            let reply = socket.until(&format!("\"{id}\"")).await?;
            if !reply.contains("auth.result") {
                return Err(std::io::Error::other(format!("auth {index}: {reply}")));
            }
        }
        Ok(socket)
    }

    impl Socket {
        async fn send(&mut self, opcode: u8, payload: &[u8]) -> std::io::Result<()> {
            let mask = [0x11, 0x22, 0x33, 0x44];
            let mut frame = Vec::with_capacity(payload.len() + 14);
            frame.push(0x80 | opcode);
            match payload.len() {
                n if n < 126 => frame.push(0x80 | n as u8),
                n if n <= 0xffff => {
                    frame.push(0x80 | 126);
                    frame.extend_from_slice(&(n as u16).to_be_bytes());
                }
                n => {
                    frame.push(0x80 | 127);
                    frame.extend_from_slice(&(n as u64).to_be_bytes());
                }
            }
            frame.extend_from_slice(&mask);
            frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
            self.stream.write_all(&frame).await
        }

        /// Read text messages until one contains `marker`.
        async fn until(&mut self, marker: &str) -> std::io::Result<String> {
            loop {
                let (opcode, payload) = self.message().await?;
                if opcode == 0x8 {
                    return Err(std::io::Error::other("closed by server"));
                }
                if opcode == 0x1 {
                    let text = String::from_utf8_lossy(&payload);
                    if text.contains(marker) {
                        return Ok(text.into_owned());
                    }
                }
            }
        }

        async fn drain(&mut self) {
            while let Ok((opcode, _)) = self.message().await {
                if opcode == 0x8 {
                    return;
                }
            }
        }

        async fn message(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
            let mut payload = Vec::new();
            let mut first = None;
            loop {
                let b0 = self.stream.read_u8().await?;
                let b1 = self.stream.read_u8().await?;
                let len = match b1 & 0x7f {
                    126 => self.stream.read_u16().await? as usize,
                    127 => self.stream.read_u64().await? as usize,
                    n => n as usize,
                };
                let mut part = vec![0; len];
                self.stream.read_exact(&mut part).await?;
                let opcode = b0 & 0x0f;
                if opcode >= 0x8 {
                    if first.is_none() {
                        return Ok((opcode, part));
                    }
                    continue;
                }
                if first.is_none() {
                    first = Some((opcode, b0 & 0x40 != 0));
                }
                payload.extend_from_slice(&part);
                if b0 & 0x80 != 0 {
                    break;
                }
            }
            let (opcode, compressed) = first.unwrap();
            if compressed && self.deflate {
                payload.extend_from_slice(&[0, 0, 255, 255]);
                let mut codec = flate2::Decompress::new(false);
                let mut out = Vec::with_capacity(payload.len() * 4);
                loop {
                    out.reserve(64 << 10);
                    let before = codec.total_in() as usize;
                    codec
                        .decompress_vec(&payload[before..], &mut out, flate2::FlushDecompress::Sync)
                        .map_err(std::io::Error::other)?;
                    if codec.total_in() as usize == payload.len() {
                        break;
                    }
                }
                return Ok((opcode, out));
            }
            Ok((opcode, payload))
        }
    }

    pub struct Sample {
        heap: i64,
        rss: i64,
        allocs: i64,
        buckets: Vec<(i64, i64)>,
        stat: std::collections::BTreeMap<String, i64>,
        skmem: [i64; 4],
        sockets: i64,
    }

    impl Sample {
        async fn take(port: u16, pid: u32) -> Sample {
            let body = http_get(port, "/__mem").await;
            let value: serde_json::Value = serde_json::from_str(&body).unwrap();
            let buckets = value["buckets"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| (b[0].as_i64().unwrap(), b[1].as_i64().unwrap()))
                .collect();
            let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
                .unwrap_or_default()
                .lines()
                .find_map(|line| line.strip_prefix("0::").map(str::to_owned))
                .unwrap_or_default();
            let stat = std::fs::read_to_string(format!("/sys/fs/cgroup{cgroup}/memory.stat"))
                .unwrap_or_default()
                .lines()
                .filter_map(|line| {
                    let (key, value) = line.split_once(' ')?;
                    Some((key.to_owned(), value.parse().ok()?))
                })
                .collect();
            let (skmem, sockets) = skmem(port);
            Sample {
                heap: value["live"].as_i64().unwrap(),
                rss: value["rss"].as_i64().unwrap(),
                allocs: value["allocs"].as_i64().unwrap(),
                buckets,
                stat,
                skmem,
                sockets,
            }
        }
    }

    /// Sum server-side `ss -tm` skmem: r (rmem_alloc), t (wmem_alloc),
    /// f (forward alloc), w (wmem_queued) for established sockets on `port`.
    fn skmem(port: u16) -> ([i64; 4], i64) {
        let output = std::process::Command::new("ss")
            .args([
                "-tmnH",
                "state",
                "established",
                &format!("( sport = :{port} )"),
            ])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
            .unwrap_or_default();
        let mut sums = [0i64; 4];
        let mut sockets = 0;
        for line in output.lines() {
            let Some(start) = line.find("skmem:(") else {
                continue;
            };
            sockets += 1;
            let inner = &line[start + 7..];
            let inner = &inner[..inner.find(')').unwrap_or(inner.len())];
            for field in inner.split(',') {
                let digits = field.trim_start_matches(|c: char| c.is_ascii_alphabetic());
                let key = &field[..field.len() - digits.len()];
                let Ok(value) = digits.parse::<i64>() else {
                    continue;
                };
                match key {
                    "r" => sums[0] += value,
                    "t" => sums[1] += value,
                    "f" => sums[2] += value,
                    "w" => sums[3] += value,
                    _ => {}
                }
            }
        }
        (sums, sockets)
    }

    async fn http_get(port: u16, path: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or_default()
    }

    fn report(label: &str, phase: &str, n: usize, base: &Sample, now: &Sample) {
        let n = n as i64;
        let stat = |key: &str| {
            now.stat.get(key).copied().unwrap_or(0) - base.stat.get(key).copied().unwrap_or(0)
        };
        let slab = stat("slab");
        let largest = now
            .buckets
            .iter()
            .zip(&base.buckets)
            .enumerate()
            .map(|(i, (a, b))| (i, a.0 - b.0, a.1 - b.1))
            .filter(|(_, count, bytes)| *count != 0 || *bytes != 0)
            .map(|(i, count, bytes)| {
                format!(
                    "<{}:{:.2}x{}",
                    if i == 0 { 1 } else { 1u64 << i },
                    count as f64 / n as f64,
                    bytes / n
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        println!(
            "{label} {phase}: heap/conn={} rss/conn={} allocs={} cg_anon/conn={} cg_sock/conn={} cg_slab/conn={} cg_kernel/conn={} skmem r/t/f/w per conn={}/{}/{}/{} sockets={}",
            (now.heap - base.heap) / n,
            (now.rss - base.rss) / n,
            now.allocs - base.allocs,
            stat("anon") / n,
            stat("sock") / n,
            slab / n,
            stat("kernel") / n,
            now.skmem[0] / n,
            now.skmem[1] / n,
            now.skmem[2] / n,
            now.skmem[3] / n,
            now.sockets,
        );
        println!("{label} {phase}: live-bucket delta per conn [<size:count x bytes]: {largest}");
    }
}
