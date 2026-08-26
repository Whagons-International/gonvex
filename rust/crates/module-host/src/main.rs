//! The Gonvex module host.
//!
//! One process, one listener, every project. The Rust runtime keeps ownership of
//! HTTP, Postgres, tenancy, and identity; this host owns module generations and
//! the isolates that run them, and reaches back into the trusted runtime for anything a module
//! asks for. It is deliberately a local-only service: a Unix domain socket
//! where one exists, loopback TCP as the portable fallback.
//!
//! Nothing here is per tenant. Spawning a process per tenant would multiply V8
//! heaps by the number of tenants for no isolation the invocation context does
//! not already provide.

use std::io::{Read, Write};
#[cfg(unix)]
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use gonvex_module_host::framing::DEFAULT_MAX_FRAME_BYTES;
use gonvex_module_host::protocol::PROTOCOL_VERSION;
use gonvex_module_host::session;
use gonvex_module_host::session::{log, HostConfig, HostState};
use gonvex_module_runtime_v8::{initialize_v8_platform, V8Config};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;

const USAGE: &str = "\
gonvex-module-host — executes Gonvex TypeScript modules for the Rust runtime

  --listen <endpoint>          unix:/path/to.sock | tcp:127.0.0.1:7787 (required)
  --max-frame-bytes <n>        largest accepted protocol frame
  --max-concurrent <n>         concurrent module invocations
  --isolate-pool <n>           live isolates per module generation
  --max-heap-mb <n>            V8 heap ceiling per isolate
  --execution-timeout-ms <n>   ceiling on one invocation
  --max-host-calls <n>         host operations one invocation may make
  --max-result-bytes <n>       largest result one invocation may return
  --recycle-after <n>          calls an isolate serves before retirement
  --drain-ms <n>               how long a retired generation may finish calls
  --shutdown-ms <n>            bound on shutdown
  --exit-on-stdin-eof          exit when the parent closes stdin
";

enum Endpoint {
    #[cfg(unix)]
    Unix(PathBuf),
    Tcp(String),
}

impl Endpoint {
    fn parse(value: &str) -> Result<Self, String> {
        let value = value.trim();
        if value.is_empty() {
            return Err("--listen requires an endpoint".to_owned());
        }
        let (scheme, rest) = match value.split_once("://") {
            Some((scheme, rest)) => (scheme, rest),
            None => match value.split_once(':') {
                // `127.0.0.1:7787` has a colon but no scheme; a scheme is only
                // a scheme when it is one of the two names we accept.
                Some((scheme, rest)) if scheme == "unix" || scheme == "tcp" => (scheme, rest),
                _ => ("", value),
            },
        };
        match scheme {
            "unix" => unix_endpoint(rest),
            "tcp" => Ok(Self::Tcp(rest.to_owned())),
            // A bare path is a socket path; anything else is an address.
            _ if rest.starts_with('/') || rest.starts_with('.') => unix_endpoint(rest),
            _ => Ok(Self::Tcp(rest.to_owned())),
        }
    }
}

#[cfg(unix)]
fn unix_endpoint(path: &str) -> Result<Endpoint, String> {
    Ok(Endpoint::Unix(PathBuf::from(path)))
}

#[cfg(not(unix))]
fn unix_endpoint(_path: &str) -> Result<Endpoint, String> {
    Err(
        "unix domain sockets are not available on this platform; use tcp:127.0.0.1:<port>"
            .to_owned(),
    )
}

enum Listener {
    #[cfg(unix)]
    Unix(UnixListener),
    Tcp(TcpListener),
}

struct Options {
    endpoint: Endpoint,
    config: HostConfig,
    exit_on_stdin_eof: bool,
}

fn parse_options() -> Result<Option<Options>, String> {
    let mut endpoint: Option<String> = std::env::var("GONVEX_MODULE_HOST_ENDPOINT").ok();
    let mut exit_on_stdin_eof = false;
    let mut config = HostConfig {
        max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        max_concurrent_calls: 32,
        drain_timeout: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(15),
        v8: V8Config::default(),
    };

    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("{argument} requires a value"))
        };
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--listen" => endpoint = Some(value()?),
            "--max-frame-bytes" => config.max_frame_bytes = number(&value()?)?,
            "--max-concurrent" => config.max_concurrent_calls = number(&value()?)?,
            "--isolate-pool" => config.v8.isolate_pool_size = number(&value()?)?,
            "--max-heap-mb" => config.v8.max_heap_bytes = number::<usize>(&value()?)? * 1024 * 1024,
            "--execution-timeout-ms" => {
                config.v8.execution_timeout = Duration::from_millis(number(&value()?)?)
            }
            "--max-host-calls" => config.v8.max_host_calls = number(&value()?)?,
            "--max-result-bytes" => config.v8.max_result_bytes = number(&value()?)?,
            "--recycle-after" => config.v8.recycle_after_calls = number(&value()?)?,
            "--drain-ms" => config.drain_timeout = Duration::from_millis(number(&value()?)?),
            "--shutdown-ms" => config.shutdown_timeout = Duration::from_millis(number(&value()?)?),
            "--exit-on-stdin-eof" => exit_on_stdin_eof = true,
            other => return Err(format!("unknown argument {other}")),
        }
    }

    let endpoint = endpoint.ok_or_else(|| "--listen is required".to_owned())?;
    Ok(Some(Options {
        endpoint: Endpoint::parse(&endpoint)?,
        config,
        exit_on_stdin_eof,
    }))
}

fn number<T: std::str::FromStr>(value: &str) -> Result<T, String> {
    value
        .trim()
        .parse::<T>()
        .map_err(|_| format!("{value} is not a valid number"))
}

fn main() -> ExitCode {
    // Every isolate thread must share the V8 platform initialized by this
    // process thread. It must happen before Tokio creates its worker threads;
    // otherwise isolate threads spawned by different workers do not have the
    // common initialized parent V8 11.6 requires.
    initialize_v8_platform();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("gonvex-module-host: failed to start async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run())
}

async fn run() -> ExitCode {
    let options = match parse_options() {
        Ok(Some(options)) => options,
        Ok(None) => {
            print!("{}", USAGE);
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("gonvex-module-host: {}\n\n{}", message, USAGE);
            return ExitCode::FAILURE;
        }
    };

    let (listener, endpoint) = match bind(&options.endpoint).await {
        Ok(bound) => bound,
        Err(message) => {
            eprintln!("gonvex-module-host: {message}");
            return ExitCode::FAILURE;
        }
    };

    let state = HostState::new(options.config.clone());
    if options.exit_on_stdin_eof {
        watch_parent_exit(Arc::clone(&state));
    }
    watch_signals(Arc::clone(&state));

    // The readiness line is the one thing on stdout, so a supervisor can wait
    // for it instead of racing the listener.
    let mut stdout = std::io::stdout();
    let _ = writeln!(
        stdout,
        "{{\"ready\":true,\"protocol\":{},\"endpoint\":\"{}\",\"pid\":{},\"version\":\"{}\"}}",
        PROTOCOL_VERSION,
        endpoint,
        std::process::id(),
        env!("CARGO_PKG_VERSION")
    );
    let _ = stdout.flush();
    log(&format!(
        "listening on {} (frames <= {} bytes, {} concurrent calls)",
        endpoint, options.config.max_frame_bytes, options.config.max_concurrent_calls
    ));

    accept_loop(listener, Arc::clone(&state)).await;

    // Bounded shutdown: in-flight calls get the grace window, and whatever is
    // still running past it is reported rather than silently abandoned.
    let draining = Arc::clone(&state);
    let grace = options.config.shutdown_timeout;
    let drained = tokio::task::spawn_blocking(move || draining.modules.shutdown(Some(grace)))
        .await
        .unwrap_or(false);
    if !drained {
        log("shutdown deadline passed with module calls still running");
    }
    cleanup(&options.endpoint);
    ExitCode::SUCCESS
}

async fn accept_loop(listener: Listener, state: Arc<HostState>) {
    let mut shutdown = state.shutdown_signal();
    loop {
        if state.is_shutting_down() {
            break;
        }
        match &listener {
            #[cfg(unix)]
            Listener::Unix(listener) => {
                let accepted = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    accepted = listener.accept() => accepted,
                };
                match accepted {
                    Ok((stream, _)) => {
                        tokio::spawn(session::serve(stream, Arc::clone(&state)));
                    }
                    Err(err) => {
                        log(&format!("accept failed: {err}"));
                        break;
                    }
                }
            }
            Listener::Tcp(listener) => {
                let accepted = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    accepted = listener.accept() => accepted,
                };
                match accepted {
                    Ok((stream, _)) => {
                        let _ = stream.set_nodelay(true);
                        tokio::spawn(session::serve(stream, Arc::clone(&state)));
                    }
                    Err(err) => {
                        log(&format!("accept failed: {err}"));
                        break;
                    }
                }
            }
        }
    }
}

/// Binds the listener and reports the endpoint it actually got, which is not
/// the requested one when a caller asked for port 0.
async fn bind(endpoint: &Endpoint) -> Result<(Listener, String), String> {
    match endpoint {
        #[cfg(unix)]
        Endpoint::Unix(path) => {
            if path.exists() {
                // A socket file outlives a crashed host. Connecting is the only
                // way to tell a stale file from a live one, so a running host
                // is never silently displaced.
                if tokio::net::UnixStream::connect(path).await.is_ok() {
                    return Err(format!(
                        "another module host is already listening on {}",
                        path.display()
                    ));
                }
                let _ = std::fs::remove_file(path);
            }
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let listener = UnixListener::bind(path)
                .map_err(|err| format!("failed to listen on {}: {err}", path.display()))?;
            Ok((Listener::Unix(listener), format!("unix:{}", path.display())))
        }
        Endpoint::Tcp(address) => {
            let listener = TcpListener::bind(address.as_str())
                .await
                .map_err(|err| format!("failed to listen on {address}: {err}"))?;
            let resolved = match listener.local_addr() {
                Ok(local) => format!("tcp:{local}"),
                Err(_) => format!("tcp:{address}"),
            };
            Ok((Listener::Tcp(listener), resolved))
        }
    }
}

fn cleanup(endpoint: &Endpoint) {
    match endpoint {
        #[cfg(unix)]
        Endpoint::Unix(path) => {
            let _ = std::fs::remove_file(path);
        }
        Endpoint::Tcp(_) => {}
    }
}

/// A managed host is started with a pipe on stdin it never reads. EOF on that
/// pipe means the parent runtime is gone, which is the one orphan signal that works
/// the same way on every platform.
fn watch_parent_exit(state: Arc<HostState>) {
    let spawned = std::thread::Builder::new()
        .name("gonvex-parent-watch".to_owned())
        .spawn(move || {
            let mut stdin = std::io::stdin().lock();
            let mut buffer = [0u8; 256];
            loop {
                match stdin.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            log("parent closed stdin; shutting down");
            state.begin_shutdown();
        });
    if spawned.is_err() {
        log("failed to watch the parent process; continuing without an orphan guard");
    }
}

fn watch_signals(state: Arc<HostState>) {
    #[cfg(unix)]
    {
        let terminate_state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut terminate =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(signal) => signal,
                    Err(err) => {
                        log(&format!("failed to watch SIGTERM: {err}"));
                        return;
                    }
                };
            terminate.recv().await;
            log("received SIGTERM; shutting down");
            terminate_state.begin_shutdown();
        });
    }
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            log("received interrupt; shutting down");
            state.begin_shutdown();
        }
    });
}
