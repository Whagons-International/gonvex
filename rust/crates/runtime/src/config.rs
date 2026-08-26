use std::collections::BTreeMap;
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

const DEFAULT_ADDR: &str = "0.0.0.0:8080";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub addr: SocketAddr,
    pub control_plane_database_url: Option<String>,
    pub default_database_url: Option<String>,
    pub tenant_database_urls: BTreeMap<String, String>,
    pub project_database_urls: BTreeMap<String, String>,
    pub require_auth: bool,
    pub control_secret: Option<String>,
    pub auth_public_url: Option<String>,
    pub admin_key: Option<String>,
    pub dev_sync_key: Option<String>,
    pub dashboard_account: Option<String>,
    pub dashboard_password: Option<String>,
    pub dashboard_auth_project_id: Option<String>,
    pub google_client_id: Option<String>,
    pub google_client_secret: Option<String>,
    pub database_max_total_connections: usize,
    pub database_max_connections: u32,
    pub database_max_idle_connections: u32,
    pub module_host: ModuleHostConfig,
    pub runtime_version: String,
    pub sandbox: SandboxConfig,
    pub storage: StorageConfig,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub public_base_url: String,
}

impl StorageConfig {
    pub fn configured(&self) -> bool {
        !self.endpoint.is_empty()
            && !self.bucket.is_empty()
            && !self.access_key_id.is_empty()
            && !self.secret_access_key.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxConfig {
    pub enabled: bool,
    pub worker_binary: Option<PathBuf>,
    pub root: PathBuf,
    pub allow_unconfined: bool,
    pub concurrency: usize,
    pub max_per_account: usize,
    pub max_total: usize,
    pub max_executions: usize,
    pub default_ttl: Duration,
    pub max_ttl: Duration,
    pub default_timeout: Duration,
    pub max_timeout: Duration,
    pub max_code_bytes: usize,
    pub max_file_bytes: u64,
    pub max_workspace_bytes: u64,
    pub max_output_bytes: usize,
    pub max_rows: usize,
    pub max_heap_bytes: u64,
    pub duckdb_memory_bytes: u64,
    pub worker_uid: u32,
    pub worker_gid: u32,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            worker_binary: None,
            root: env::temp_dir().join("gonvex-sandboxes"),
            allow_unconfined: false,
            concurrency: 2,
            max_per_account: 4,
            max_total: 128,
            max_executions: 16,
            default_ttl: Duration::from_secs(30 * 60),
            max_ttl: Duration::from_secs(2 * 60 * 60),
            default_timeout: Duration::from_secs(30),
            max_timeout: Duration::from_secs(120),
            max_code_bytes: 512 << 10,
            max_file_bytes: 64 << 20,
            max_workspace_bytes: 256 << 20,
            max_output_bytes: 8 << 20,
            max_rows: 500,
            max_heap_bytes: 64 << 20,
            duckdb_memory_bytes: 128 << 20,
            worker_uid: 65_534,
            worker_gid: 65_534,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleHostConfig {
    pub enabled: bool,
    pub binary: Option<PathBuf>,
    pub endpoint: Option<String>,
    pub start_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub max_frame_bytes: usize,
    pub max_concurrent_calls: usize,
    pub isolate_pool_size: usize,
    pub execution_timeout: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{name} must be a socket address, got {value:?}")]
    Address { name: &'static str, value: String },
    #[error("{name} must be true or false, got {value:?}")]
    Boolean { name: &'static str, value: String },
    #[error("{name} must be a positive integer, got {value:?}")]
    Integer { name: &'static str, value: String },
    #[error("{name} must be a JSON object of string values")]
    StringMap { name: &'static str },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let addr_value = lookup("GONVEX_ADDR").unwrap_or_else(|| DEFAULT_ADDR.to_owned());
        let addr = normalize_addr(&addr_value).ok_or_else(|| ConfigError::Address {
            name: "GONVEX_ADDR",
            value: addr_value.clone(),
        })?;
        let runtime_version = lookup("SOURCE_COMMIT")
            .filter(|value| is_git_sha(value))
            .or_else(|| non_empty(lookup("GONVEX_RUNTIME_VERSION")))
            .unwrap_or_else(|| "development".to_owned());

        Ok(Self {
            addr,
            control_plane_database_url: non_empty(lookup("GONVEX_CONTROL_PLANE_DATABASE_URL")),
            default_database_url: non_empty(lookup("DATABASE_URL")),
            tenant_database_urls: string_map(
                "GONVEX_TENANT_DATABASE_URLS",
                lookup("GONVEX_TENANT_DATABASE_URLS"),
            )?,
            project_database_urls: string_map(
                "GONVEX_PROJECT_DATABASE_URLS",
                lookup("GONVEX_PROJECT_DATABASE_URLS"),
            )?,
            require_auth: boolean("GONVEX_REQUIRE_AUTH", lookup("GONVEX_REQUIRE_AUTH"), false)?,
            control_secret: non_empty(lookup("GONVEX_DASHBOARD_SESSION_SECRET")),
            auth_public_url: non_empty(lookup("GONVEX_AUTH_URL"))
                .or_else(|| non_empty(lookup("GONVEX_PUBLIC_URL"))),
            admin_key: non_empty(lookup("GONVEX_ADMIN_KEY")),
            dev_sync_key: non_empty(lookup("GONVEX_DEV_SYNC_KEY")),
            dashboard_account: non_empty(lookup("DASHBOARD_AUTH_ACCOUNT"))
                .map(|value| value.to_lowercase()),
            dashboard_password: lookup("DASHBOARD_AUTH_PASSWORD"),
            dashboard_auth_project_id: non_empty(lookup("GONVEX_DASHBOARD_AUTH_PROJECT_ID")),
            google_client_id: non_empty(lookup("GONVEX_GOOGLE_CLIENT_ID")),
            google_client_secret: non_empty(lookup("GONVEX_GOOGLE_CLIENT_SECRET")),
            database_max_total_connections: integer(
                "GONVEX_DB_MAX_TOTAL_CONNS",
                lookup("GONVEX_DB_MAX_TOTAL_CONNS"),
                20,
            )?,
            database_max_connections: integer(
                "GONVEX_DB_MAX_OPEN_CONNS",
                lookup("GONVEX_DB_MAX_OPEN_CONNS"),
                16,
            )? as u32,
            database_max_idle_connections: integer_allow_zero(
                "GONVEX_DB_MAX_IDLE_CONNS",
                lookup("GONVEX_DB_MAX_IDLE_CONNS"),
                1,
            )? as u32,
            module_host: ModuleHostConfig {
                enabled: boolean(
                    "GONVEX_MODULE_HOST_ENABLED",
                    lookup("GONVEX_MODULE_HOST_ENABLED"),
                    true,
                )?,
                binary: non_empty(lookup("GONVEX_MODULE_HOST_BINARY")).map(PathBuf::from),
                endpoint: non_empty(lookup("GONVEX_MODULE_HOST_ENDPOINT")),
                start_timeout: duration_ms(
                    "GONVEX_MODULE_HOST_START_TIMEOUT_MS",
                    lookup("GONVEX_MODULE_HOST_START_TIMEOUT_MS"),
                    30_000,
                )?,
                shutdown_timeout: duration_ms(
                    "GONVEX_MODULE_HOST_SHUTDOWN_TIMEOUT_MS",
                    lookup("GONVEX_MODULE_HOST_SHUTDOWN_TIMEOUT_MS"),
                    10_000,
                )?,
                max_frame_bytes: integer(
                    "GONVEX_MODULE_HOST_MAX_FRAME_BYTES",
                    lookup("GONVEX_MODULE_HOST_MAX_FRAME_BYTES"),
                    64 << 20,
                )?,
                max_concurrent_calls: integer(
                    "GONVEX_MODULE_HOST_MAX_CONCURRENT_CALLS",
                    lookup("GONVEX_MODULE_HOST_MAX_CONCURRENT_CALLS"),
                    32,
                )?,
                isolate_pool_size: integer(
                    "GONVEX_MODULE_HOST_ISOLATE_POOL_SIZE",
                    lookup("GONVEX_MODULE_HOST_ISOLATE_POOL_SIZE"),
                    4,
                )?,
                execution_timeout: duration_ms(
                    "GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS",
                    lookup("GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS"),
                    10_000,
                )?,
            },
            runtime_version,
            sandbox: SandboxConfig {
                enabled: boolean(
                    "GONVEX_SANDBOX_ENABLED",
                    lookup("GONVEX_SANDBOX_ENABLED"),
                    false,
                )?,
                worker_binary: non_empty(lookup("GONVEX_SANDBOX_WORKER_BINARY")).map(PathBuf::from),
                root: non_empty(lookup("GONVEX_SANDBOX_ROOT"))
                    .map(PathBuf::from)
                    .unwrap_or_else(|| env::temp_dir().join("gonvex-sandboxes")),
                allow_unconfined: boolean(
                    "GONVEX_SANDBOX_ALLOW_UNCONFINED",
                    lookup("GONVEX_SANDBOX_ALLOW_UNCONFINED"),
                    false,
                )?,
                concurrency: integer(
                    "GONVEX_SANDBOX_CONCURRENCY",
                    lookup("GONVEX_SANDBOX_CONCURRENCY"),
                    2,
                )?,
                max_per_account: integer(
                    "GONVEX_SANDBOX_MAX_PER_ACCOUNT",
                    lookup("GONVEX_SANDBOX_MAX_PER_ACCOUNT"),
                    4,
                )?,
                max_total: integer(
                    "GONVEX_SANDBOX_MAX_TOTAL",
                    lookup("GONVEX_SANDBOX_MAX_TOTAL"),
                    128,
                )?,
                max_executions: integer(
                    "GONVEX_SANDBOX_MAX_EXECUTIONS",
                    lookup("GONVEX_SANDBOX_MAX_EXECUTIONS"),
                    16,
                )?,
                default_ttl: duration_ms(
                    "GONVEX_SANDBOX_DEFAULT_TTL_MS",
                    lookup("GONVEX_SANDBOX_DEFAULT_TTL_MS"),
                    30 * 60 * 1_000,
                )?,
                max_ttl: duration_ms(
                    "GONVEX_SANDBOX_MAX_TTL_MS",
                    lookup("GONVEX_SANDBOX_MAX_TTL_MS"),
                    2 * 60 * 60 * 1_000,
                )?,
                default_timeout: duration_ms(
                    "GONVEX_SANDBOX_DEFAULT_TIMEOUT_MS",
                    lookup("GONVEX_SANDBOX_DEFAULT_TIMEOUT_MS"),
                    30_000,
                )?,
                max_timeout: duration_ms(
                    "GONVEX_SANDBOX_MAX_TIMEOUT_MS",
                    lookup("GONVEX_SANDBOX_MAX_TIMEOUT_MS"),
                    120_000,
                )?,
                max_code_bytes: integer(
                    "GONVEX_SANDBOX_MAX_CODE_BYTES",
                    lookup("GONVEX_SANDBOX_MAX_CODE_BYTES"),
                    512 << 10,
                )?,
                max_file_bytes: integer(
                    "GONVEX_SANDBOX_MAX_FILE_BYTES",
                    lookup("GONVEX_SANDBOX_MAX_FILE_BYTES"),
                    64 << 20,
                )? as u64,
                max_workspace_bytes: integer(
                    "GONVEX_SANDBOX_MAX_WORKSPACE_BYTES",
                    lookup("GONVEX_SANDBOX_MAX_WORKSPACE_BYTES"),
                    256 << 20,
                )? as u64,
                max_output_bytes: integer(
                    "GONVEX_SANDBOX_MAX_OUTPUT_BYTES",
                    lookup("GONVEX_SANDBOX_MAX_OUTPUT_BYTES"),
                    8 << 20,
                )?,
                max_rows: integer(
                    "GONVEX_SANDBOX_MAX_ROWS",
                    lookup("GONVEX_SANDBOX_MAX_ROWS"),
                    500,
                )?,
                max_heap_bytes: integer(
                    "GONVEX_SANDBOX_MAX_HEAP_BYTES",
                    lookup("GONVEX_SANDBOX_MAX_HEAP_BYTES"),
                    64 << 20,
                )? as u64,
                duckdb_memory_bytes: integer(
                    "GONVEX_SANDBOX_DUCKDB_MEMORY_BYTES",
                    lookup("GONVEX_SANDBOX_DUCKDB_MEMORY_BYTES"),
                    128 << 20,
                )? as u64,
                worker_uid: integer(
                    "GONVEX_SANDBOX_WORKER_UID",
                    lookup("GONVEX_SANDBOX_WORKER_UID"),
                    65_534,
                )? as u32,
                worker_gid: integer(
                    "GONVEX_SANDBOX_WORKER_GID",
                    lookup("GONVEX_SANDBOX_WORKER_GID"),
                    65_534,
                )? as u32,
            },
            storage: StorageConfig {
                endpoint: non_empty(lookup("S3_ENDPOINT")).unwrap_or_default(),
                region: non_empty(lookup("S3_REGION")).unwrap_or_else(|| "us-east-1".to_owned()),
                bucket: non_empty(lookup("S3_BUCKET")).unwrap_or_default(),
                access_key_id: non_empty(lookup("S3_ACCESS_KEY_ID")).unwrap_or_default(),
                secret_access_key: non_empty(lookup("S3_SECRET_ACCESS_KEY")).unwrap_or_default(),
                force_path_style: boolean(
                    "S3_FORCE_PATH_STYLE",
                    lookup("S3_FORCE_PATH_STYLE"),
                    true,
                )?,
                public_base_url: non_empty(lookup("GONVEX_PUBLIC_URL")).unwrap_or_default(),
            },
        })
    }
}

fn normalize_addr(value: &str) -> Option<SocketAddr> {
    let value = value.trim();
    if let Ok(addr) = value.parse() {
        return Some(addr);
    }
    value
        .strip_prefix(':')
        .and_then(|port| port.parse().ok())
        .map(|port| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn boolean(name: &'static str, value: Option<String>, default: bool) -> Result<bool, ConfigError> {
    let Some(value) = non_empty(value) else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::Boolean { name, value }),
    }
}

fn integer(
    name: &'static str,
    value: Option<String>,
    default: usize,
) -> Result<usize, ConfigError> {
    let Some(value) = non_empty(value) else {
        return Ok(default);
    };
    value
        .parse::<usize>()
        .ok()
        .filter(|number| *number > 0)
        .ok_or(ConfigError::Integer { name, value })
}

fn duration_ms(
    name: &'static str,
    value: Option<String>,
    default: usize,
) -> Result<Duration, ConfigError> {
    integer(name, value, default).map(|value| Duration::from_millis(value as u64))
}

fn integer_allow_zero(
    name: &'static str,
    value: Option<String>,
    default: usize,
) -> Result<usize, ConfigError> {
    let Some(value) = non_empty(value) else {
        return Ok(default);
    };
    value
        .parse::<usize>()
        .map_err(|_| ConfigError::Integer { name, value })
}

fn string_map(
    name: &'static str,
    value: Option<String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let Some(value) = non_empty(value) else {
        return Ok(BTreeMap::new());
    };
    let parsed: BTreeMap<String, String> =
        serde_json::from_str(&value).map_err(|_| ConfigError::StringMap { name })?;
    Ok(parsed
        .into_iter()
        .filter_map(|(key, value)| {
            let key = key.trim().to_owned();
            let value = value.trim().to_owned();
            (!key.is_empty() && !value.is_empty()).then_some((key, value))
        })
        .collect())
}

fn is_git_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_lookup(|name| {
            values
                .iter()
                .find_map(|(key, value)| (*key == name).then(|| (*value).to_owned()))
        })
    }

    #[test]
    fn reads_the_existing_runtime_environment_contract() {
        let config = config(&[
            ("GONVEX_ADDR", ":9090"),
            ("GONVEX_CONTROL_PLANE_DATABASE_URL", "postgres://control"),
            ("DATABASE_URL", "postgres://default"),
            ("GONVEX_REQUIRE_AUTH", "true"),
            (
                "GONVEX_TENANT_DATABASE_URLS",
                r#"{"tenant-a":"postgres://a"}"#,
            ),
            ("GONVEX_MODULE_HOST_BINARY", "/bin/gonvex-module-host"),
            ("SOURCE_COMMIT", "0123456789abcdef0123456789abcdef01234567"),
        ])
        .expect("config");

        assert_eq!(config.addr, "0.0.0.0:9090".parse().unwrap());
        assert_eq!(
            config.control_plane_database_url.as_deref(),
            Some("postgres://control")
        );
        assert_eq!(config.tenant_database_urls["tenant-a"], "postgres://a");
        assert!(config.require_auth);
        assert_eq!(config.database_max_total_connections, 20);
        assert_eq!(
            config.runtime_version,
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn rejects_invalid_security_configuration() {
        assert!(matches!(
            config(&[("GONVEX_REQUIRE_AUTH", "sometimes")]),
            Err(ConfigError::Boolean { .. })
        ));
        assert!(matches!(
            config(&[("GONVEX_MODULE_HOST_MAX_FRAME_BYTES", "0")]),
            Err(ConfigError::Integer { .. })
        ));
    }
}
