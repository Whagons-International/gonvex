use std::process::ExitCode;

use gonvex_runtime::config::Config;
use gonvex_runtime::Runtime;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_some_and(|value| value == "verify-module-artifact")
    {
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
    let server = axum::serve(listener, runtime.router()).with_graceful_shutdown(async {
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
