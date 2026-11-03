use std::process::ExitCode;

// `system-alloc` keeps the system allocator so heap profilers that hook
// malloc (heaptrack) see every allocation. Production uses mimalloc.
#[cfg(not(feature = "system-alloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use axum::serve::ListenerExt;
use gonvex_runtime::config::Config;
use gonvex_runtime::Runtime;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    // In-process module isolates need the V8 platform initialized on this
    // thread before Tokio starts its workers (see gonvex-module-host main).
    gonvex_module_runtime_v8::initialize_v8_platform();
    match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime.block_on(run()),
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
    let runtime = Runtime::new(config);
    if let Err(error) = runtime.start().await {
        error!(%error, "Gonvex runtime startup failed");
        return ExitCode::FAILURE;
    }
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(error) => {
            error!(%error, %addr, "failed to bind Gonvex runtime");
            runtime.shutdown().await;
            return ExitCode::FAILURE;
        }
    };

    info!(%addr, "starting Gonvex Rust runtime");
    let server = axum::serve(
        listener.tap_io(disable_nagle),
        runtime
            .router()
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = signal::ctrl_c().await;
    });
    let result = server.await;
    runtime.shutdown().await;
    if let Err(error) = result {
        error!(%error, "Gonvex Rust runtime stopped");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
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

fn disable_nagle(stream: &mut tokio::net::TcpStream) {
    // Protocol frames already batch their headers and payloads. Holding a
    // following small delta/ready frame for a delayed ACK adds a network stall.
    if let Err(error) = stream.set_nodelay(true) {
        warn!(%error, "failed to enable TCP_NODELAY");
    }
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

    #[tokio::test]
    async fn accepted_connections_disable_nagle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap());
        let (client, accepted) = tokio::join!(client, listener.accept());
        let _client = client.unwrap();
        let (mut stream, _) = accepted.unwrap();
        stream.set_nodelay(false).unwrap();
        disable_nagle(&mut stream);
        assert!(stream.nodelay().unwrap());
    }
}
