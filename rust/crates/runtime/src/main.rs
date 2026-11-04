use std::process::ExitCode;

// `system-alloc` keeps the system allocator so heap profilers that hook
// malloc (heaptrack) see every allocation. Production uses mimalloc.
//
// Every allocator is wrapped in `heap_stats::Counting` (live Rust heap bytes
// for `/dev/metrics` `memory.allocator`).
#[cfg(not(any(feature = "system-alloc", feature = "jemalloc-prof")))]
#[global_allocator]
static GLOBAL: gonvex_runtime::heap_stats::Counting<mimalloc::MiMalloc> =
    gonvex_runtime::heap_stats::Counting(mimalloc::MiMalloc);

#[cfg(all(feature = "system-alloc", not(feature = "jemalloc-prof")))]
#[global_allocator]
static GLOBAL: gonvex_runtime::heap_stats::Counting<std::alloc::System> =
    gonvex_runtime::heap_stats::Counting(std::alloc::System);

#[cfg(not(any(feature = "system-alloc", feature = "jemalloc-prof")))]
mod mimalloc_state {
    // Exported by the statically linked mimalloc (libmimalloc-sys); declared
    // here because the crate's `extended` bindings are not enabled.
    extern "C" {
        fn mi_process_info(
            elapsed_msecs: *mut usize,
            user_msecs: *mut usize,
            system_msecs: *mut usize,
            current_rss: *mut usize,
            peak_rss: *mut usize,
            current_commit: *mut usize,
            peak_commit: *mut usize,
            page_faults: *mut usize,
        );
        fn mi_collect(force: bool);
    }

    /// mimalloc's committed and resident bytes (`memory.allocator.state`).
    pub fn info() -> serde_json::Value {
        let mut values = [0usize; 8];
        let [a, b, c, d, e, f, g, h] = &mut values;
        // SAFETY: eight valid out-pointers.
        unsafe { mi_process_info(a, b, c, d, e, f, g, h) };
        serde_json::json!({
            "rssBytes": values[3], "peakRssBytes": values[4],
            "commitBytes": values[5], "peakCommitBytes": values[6],
        })
    }

    /// Forced collection of this thread's heap, abandoned segments and
    /// delayed arena purges.
    pub fn collect() {
        // SAFETY: no arguments besides the flag.
        unsafe { mi_collect(true) };
    }
}

/// `GONVEX_ALLOC_TRIM_SECS=N`: every N seconds, a forced mimalloc collect
/// (from this thread: delayed arena purges and abandoned segments) and glibc
/// `malloc_trim(0)` (SQLite and V8 allocate through glibc). Off by default.
fn start_allocator_trim() {
    let Some(seconds) = std::env::var("GONVEX_ALLOC_TRIM_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
    else {
        return;
    };
    std::thread::Builder::new()
        .name("gonvex-alloc-trim".into())
        .spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(seconds));
            #[cfg(not(any(feature = "system-alloc", feature = "jemalloc-prof")))]
            mimalloc_state::collect();
            // SAFETY: malloc_trim takes no pointers.
            #[cfg(target_os = "linux")]
            unsafe {
                libc::malloc_trim(0);
            }
        })
        .ok();
}

fn register_allocator_info() {
    #[cfg(not(any(feature = "system-alloc", feature = "jemalloc-prof")))]
    gonvex_runtime::heap_stats::set_allocator_info("mimalloc", mimalloc_state::info);
    #[cfg(feature = "jemalloc-prof")]
    gonvex_runtime::heap_stats::set_allocator_info("jemalloc", || {
        let _ = tikv_jemalloc_ctl::epoch::advance();
        serde_json::json!({
            "allocatedBytes": tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0),
            "residentBytes": tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0),
            "retainedBytes": tikv_jemalloc_ctl::stats::retained::read().unwrap_or(0),
        })
    });
    #[cfg(all(feature = "system-alloc", not(feature = "jemalloc-prof")))]
    gonvex_runtime::heap_stats::set_allocator_info("system", || serde_json::Value::Null);
}

// `jemalloc-prof`: sampled heap profiles for attributing live heap by call
// site. Run with `_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:17` and
// `GONVEX_HEAP_DUMP_PREFIX=/dir/heap` (+ `GONVEX_HEAP_DUMP_SECS`, default 30):
// a dump every interval, read with `jeprof --text <binary> <dump>`.
#[cfg(feature = "jemalloc-prof")]
#[global_allocator]
static GLOBAL: gonvex_runtime::heap_stats::Counting<tikv_jemallocator::Jemalloc> =
    gonvex_runtime::heap_stats::Counting(tikv_jemallocator::Jemalloc);

#[cfg(feature = "jemalloc-prof")]
fn start_heap_dumps() {
    let Ok(prefix) = std::env::var("GONVEX_HEAP_DUMP_PREFIX") else {
        return;
    };
    let seconds = std::env::var("GONVEX_HEAP_DUMP_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
        .max(1);
    std::thread::Builder::new()
        .name("gonvex-heap-dump".into())
        .spawn(move || {
            for sequence in 0u64.. {
                std::thread::sleep(std::time::Duration::from_secs(seconds));
                let at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_secs());
                let path = format!("{prefix}.{at}.{sequence:04}.heap\0");
                // SAFETY: prof.dump takes a NUL-terminated file name.
                let result = unsafe {
                    tikv_jemalloc_ctl::raw::write(b"prof.dump\0", path.as_ptr() as *const std::ffi::c_char)
                };
                if let Err(error) = result {
                    eprintln!("gonvex-runtime: heap dump failed: {error}");
                    return;
                }
            }
        })
        .ok();
}

use gonvex_runtime::config::Config;
use gonvex_runtime::Runtime;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    #[cfg(feature = "jemalloc-prof")]
    start_heap_dumps();
    register_allocator_info();
    start_allocator_trim();
    // In-process module isolates need the V8 platform initialized on this
    // thread before Tokio starts its workers (see gonvex-module-host main).
    gonvex_module_runtime_v8::initialize_v8_platform();
    // Base priority and the thread spawner (parent of isolate, checkpoint and
    // scheduled-lane threads), after V8 and before any thread is niced
    // (gonvex_profiler::thread_priority).
    gonvex_profiler::thread_priority::init();
    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Network workers (and their blocking threads) at base priority.
        .on_thread_start(|| {
            gonvex_profiler::thread_priority::set_current_target(0);
        })
        .build()
    {
        Ok(runtime) => {
            // Writer pools open here, never on a niced offload thread.
            gonvex_store::reader_priority::set_home_runtime(runtime.handle().clone());
            runtime.block_on(run())
        }
        Err(error) => {
            eprintln!("gonvex-runtime: failed to start async runtime: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let verify = args
        .first()
        .is_some_and(|value| value == "verify-module-artifact");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    if verify {
        // stdout carries only the verified hash; diagnostics such as the
        // legacy-visibility deprecation warning go to stderr.
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    if verify {
        let path = args
            .windows(2)
            .find_map(|values| (values[0] == "--file").then_some(values[1].as_str()));
        let Some(path) = path else {
            error!("verify-module-artifact requires --file");
            return ExitCode::FAILURE;
        };
        let result = tokio::fs::read(path)
            .await
            .map_err(|error| error.to_string())
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
            .and_then(|artifact| {
                gonvex_runtime::modules::verify_standalone_module_artifact(artifact)
                    .map_err(|error| error.to_string())
            });
        return match result {
            Ok(hash) => {
                println!("{hash}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                error!(%error, "module artifact verification failed");
                ExitCode::FAILURE
            }
        };
    }

    raise_open_file_limit();
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            error!(%error, "invalid Gonvex runtime configuration");
            return ExitCode::FAILURE;
        }
    };
    let addr = config.addr;
    // One runtime process owns a data directory. Hold the exclusive lock for
    // the life of the process; a second runtime on the same volume refuses to
    // start instead of opening the SQLite files concurrently.
    let _data_dir_lock = match config.data_dir() {
        Some(data_dir) => match lock_data_dir(&data_dir) {
            Ok(lock) => Some(lock),
            Err(error) => {
                error!(%error, data_dir = %data_dir.display(), "data directory is unavailable");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    let runtime_socket_tuning = config.connection.socket;
    let runtime = Runtime::new(config);
    if let Err(error) = runtime.start().await {
        error!(%error, "Gonvex runtime startup failed");
        return ExitCode::FAILURE;
    }
    let listener = match gonvex_runtime::accept::bind(addr) {
        Ok(listener) => listener,
        Err(error) => {
            error!(%error, %addr, "failed to bind Gonvex runtime");
            runtime.shutdown().await;
            return ExitCode::FAILURE;
        }
    };

    info!(%addr, "starting Gonvex Rust runtime");
    // Nagle off plus per-socket kernel buffer caps on every accepted socket.
    let listener = gonvex_runtime::accept::TunedListener::new(listener, runtime_socket_tuning);
    let server = axum::serve(
        listener,
        runtime
            .router()
            .into_make_service_with_connect_info::<gonvex_runtime::accept::AcceptedSocket>(),
    )
    .with_graceful_shutdown(shutdown_signal());
    // After the signal the listener stops accepting. In-flight HTTP requests
    // get a bounded drain; upgraded WebSockets are not waited for. Then the
    // runtime records support-session end times and closes its pools, all
    // well inside the container's stop grace (Docker default 10 s; the
    // production compose sets 30 s).
    let server = tokio::spawn(async move { server.await });
    let result = {
        let mut server = server;
        tokio::select! {
            joined = &mut server => joined.unwrap_or_else(|error| Err(std::io::Error::other(error))),
            _ = async {
                shutdown_signal().await;
                tokio::time::sleep(SHUTDOWN_DRAIN).await;
            } => {
                info!("HTTP drain timed out; shutting down with connections open");
                server.abort();
                Ok(())
            }
        }
    };
    runtime.shutdown().await;
    if let Err(error) = result {
        error!(%error, "Gonvex Rust runtime stopped");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Maximum wait for in-flight HTTP requests after a stop signal.
const SHUTDOWN_DRAIN: std::time::Duration = std::time::Duration::from_secs(4);

/// SIGINT (Ctrl-C) or SIGTERM (`docker stop`, Coolify redeploy). As PID 1 in
/// the container the runtime receives SIGTERM with no default handler, so
/// without this a stop waited out the grace period and ended in SIGKILL.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate = match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(_) => {
                let _ = signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}

/// Every WebSocket holds a descriptor. Some Docker daemons start containers
/// with a soft `nofile` of 1024 (hard 524288), which caps a runtime at about a
/// thousand connections; raise the soft limit to the hard limit.
fn raise_open_file_limit() {
    #[cfg(unix)]
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 && limit.rlim_cur < limit.rlim_max
        {
            let previous = limit.rlim_cur;
            limit.rlim_cur = limit.rlim_max;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0 {
                info!(
                    from = previous,
                    to = limit.rlim_cur,
                    "raised the open file limit"
                );
            }
        }
    }
}

/// Takes the store's exclusive `{data}/.gonvex.lock`, held until dropped.
fn lock_data_dir(data_dir: &std::path::Path) -> Result<gonvex_store::DataDirLock, String> {
    gonvex_store::DataDir::new(data_dir)
        .lock()
        .map_err(|error| match error {
            gonvex_store::DatabaseError::DataDirLocked(path) => {
                format!("another Gonvex runtime holds {path}; stop it before starting this one")
            }
            other => other.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_runtime_cannot_lock_the_same_data_directory() {
        let data = tempfile::tempdir().unwrap();
        let first = lock_data_dir(data.path()).unwrap();
        assert!(lock_data_dir(data.path()).is_err());
        drop(first);
        assert!(lock_data_dir(data.path()).is_ok());
    }
}
