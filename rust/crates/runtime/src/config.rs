use std::collections::BTreeMap;
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

const DEFAULT_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_DATA_DIR: &str = "./data";
/// Postgres-era variables. Setting one now fails startup instead of silently
/// running against an empty data directory.
const REMOVED_DATABASE_VARIABLES: [&str; 2] = ["GONVEX_CONTROL_PLANE_DATABASE_URL", "DATABASE_URL"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub addr: SocketAddr,
    /// `sqlite://{GONVEX_DATA_DIR}/control.db`. `None` runs without a Control
    /// Plane.
    pub control_plane_database_url: Option<String>,
    /// `sqlite://{GONVEX_DATA_DIR}` (absolute; default `./data`): the data
    /// directory that holds every SQLite database and under which
    /// runtime-created project and tenant files are provisioned. One runtime
    /// process owns it at a time. `None` disables provisioning.
    pub default_database_url: Option<String>,
    /// Explicit `sqlite://` overrides (`GONVEX_TENANT_DATABASE_URLS`), keyed by
    /// tenant or `project:tenant`.
    pub tenant_database_urls: BTreeMap<String, String>,
    /// Explicit `sqlite://` overrides (`GONVEX_PROJECT_DATABASE_URLS`).
    pub project_database_urls: BTreeMap<String, String>,
    pub require_auth: bool,
    pub websocket_compression: bool,
    pub websocket_compression_threshold: usize,
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
    pub database_idle_timeout: Duration,
    pub module_host: ModuleHostConfig,
    pub runtime_version: String,
    pub sandbox: SandboxConfig,
    pub storage: StorageConfig,
    /// Trusted backend services that may open tenant-scoped service sessions.
    /// Parsed from `GONVEX_SERVICE_PRINCIPALS`; empty disables the feature.
    pub service_principals: Vec<ServicePrincipalConfig>,
    pub public_functions: crate::public_functions::PublicLimits,
    /// Presence v2 (`GONVEX_PRESENCE_*`); see `presence.rs`.
    pub presence: crate::presence::PresenceConfig,
    /// Per-connection socket caps and outbound bounds (`GONVEX_TCP_*`,
    /// `GONVEX_WS_*`): a client that stops reading pins a fixed amount of
    /// memory and is disconnected.
    pub connection: ConnectionConfig,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub socket: crate::accept::SocketTuning,
    pub outbound: gonvex_websocket::OutboundLimits,
}

/// One trusted backend service credential. Only the SHA-256 digest of the
/// credential is configured, so the runtime environment never holds the
/// secret itself. See `service_principal.rs` for the capability model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServicePrincipalConfig {
    pub id: String,
    pub token_sha256: String,
    pub projects: Vec<String>,
    pub functions: Vec<String>,
    pub max_delegation: Duration,
    /// Whether the credential may read `/dev/manifest` for its projects. The
    /// response omits the schema and visibility plans.
    pub manifest: bool,
}

pub const SERVICE_PRINCIPAL_DEFAULT_DELEGATION_SECONDS: u64 = 900;
pub const SERVICE_PRINCIPAL_MAX_DELEGATION_SECONDS: u64 = 3600;

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
    /// Run the V8 isolates inside the runtime process. Host calls are then
    /// direct function calls instead of JSON frames on a socket; `binary` and
    /// `endpoint` are ignored. `GONVEX_MODULE_HOST_IN_PROCESS=false` keeps the
    /// separate module-host process.
    pub in_process: bool,
    pub binary: Option<PathBuf>,
    pub endpoint: Option<String>,
    pub start_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub max_frame_bytes: usize,
    pub max_concurrent_calls: usize,
    pub isolate_pool_size: usize,
    pub execution_timeout: Duration,
    pub action_execution_timeout: Duration,
    /// V8 heap ceiling per isolate (in-process mode; the separate host takes
    /// its own `--max-heap-mb`). Crossing it terminates the call and retires
    /// the isolate.
    pub max_heap_bytes: usize,
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
    #[error("{name} values must be sqlite:// database paths, got {value:?}")]
    DatabaseRoute { name: &'static str, value: String },
    #[error("{0} is no longer supported; databases are SQLite files under GONVEX_DATA_DIR")]
    RemovedDatabaseVariable(&'static str),
    #[error("GONVEX_DATA_DIR {0:?} cannot be resolved to an absolute path")]
    DataDir(String),
    #[error("GONVEX_SERVICE_PRINCIPALS is invalid: {0}")]
    ServicePrincipals(String),
    #[error("trusted proxy variables must be comma-separated CIDRs or IP addresses")]
    PublicProxies,
    #[error("GONVEX_CLIENT_IP_HEADER must be a valid HTTP header name")]
    ClientIpHeader,
}

impl Config {
    /// The absolute data directory (`GONVEX_DATA_DIR`), when configured.
    pub fn data_dir(&self) -> Option<PathBuf> {
        self.default_database_url
            .as_deref()
            .map(gonvex_store::database_path)
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    /// Production defaults with explicit overrides, for the benchmark fixture.
    #[cfg(feature = "perf-fixture")]
    pub(crate) fn perf_from_lookup(
        lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        Self::from_lookup(lookup)
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

        for name in REMOVED_DATABASE_VARIABLES {
            if non_empty(lookup(name)).is_some() {
                return Err(ConfigError::RemovedDatabaseVariable(name));
            }
        }
        let data_dir = data_dir(lookup("GONVEX_DATA_DIR"))?;

        Ok(Self {
            addr,
            control_plane_database_url: Some(gonvex_store::database_url(
                &data_dir.join("control.db"),
            )),
            default_database_url: Some(gonvex_store::database_url(&data_dir)),
            tenant_database_urls: sqlite_routes(
                "GONVEX_TENANT_DATABASE_URLS",
                lookup("GONVEX_TENANT_DATABASE_URLS"),
            )?,
            project_database_urls: sqlite_routes(
                "GONVEX_PROJECT_DATABASE_URLS",
                lookup("GONVEX_PROJECT_DATABASE_URLS"),
            )?,
            websocket_compression: boolean(
                "GONVEX_WS_COMPRESSION",
                lookup("GONVEX_WS_COMPRESSION"),
                true,
            )?,
            websocket_compression_threshold: integer(
                "GONVEX_WS_COMPRESSION_THRESHOLD",
                lookup("GONVEX_WS_COMPRESSION_THRESHOLD"),
                1024,
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
            // Concurrent SQL jobs across every database file (CPU fairness).
            database_max_total_connections: integer(
                "GONVEX_DB_MAX_TOTAL_CONNS",
                lookup("GONVEX_DB_MAX_TOTAL_CONNS"),
                64,
            )?,
            // Read-only connections per database file; writes always use the
            // file's single writer connection.
            // Each SQLite connection holds its own parsed schema (~4 MB for
            // the Whagons tenant schema), so idle readers are not free.
            database_max_connections: integer(
                "GONVEX_DB_MAX_OPEN_CONNS",
                lookup("GONVEX_DB_MAX_OPEN_CONNS"),
                default_readers_per_database(),
            )? as u32,
            database_max_idle_connections: integer_allow_zero(
                "GONVEX_DB_MAX_IDLE_CONNS",
                lookup("GONVEX_DB_MAX_IDLE_CONNS"),
                4,
            )? as u32,
            database_idle_timeout: duration_ms(
                "GONVEX_DB_IDLE_TIMEOUT_MS",
                lookup("GONVEX_DB_IDLE_TIMEOUT_MS"),
                15_000,
            )?,
            module_host: ModuleHostConfig {
                enabled: boolean(
                    "GONVEX_MODULE_HOST_ENABLED",
                    lookup("GONVEX_MODULE_HOST_ENABLED"),
                    true,
                )?,
                in_process: boolean(
                    "GONVEX_MODULE_HOST_IN_PROCESS",
                    lookup("GONVEX_MODULE_HOST_IN_PROCESS"),
                    true,
                )?,
                max_heap_bytes: integer(
                    "GONVEX_MODULE_HOST_MAX_HEAP_MB",
                    lookup("GONVEX_MODULE_HOST_MAX_HEAP_MB"),
                    64,
                )? << 20,
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
                    default_isolate_pool_size(),
                )?,
                execution_timeout: duration_ms(
                    "GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS",
                    lookup("GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS"),
                    10_000,
                )?,
                action_execution_timeout: duration_ms(
                    "GONVEX_MODULE_HOST_ACTION_EXECUTION_TIMEOUT_MS",
                    lookup("GONVEX_MODULE_HOST_ACTION_EXECUTION_TIMEOUT_MS"),
                    900_000,
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
            presence: presence_config(&mut lookup)?,
            connection: connection_config(&mut lookup)?,
            public_functions: crate::public_functions::PublicLimits {
                pending_per_ip: integer(
                    "GONVEX_PENDING_CONNECTIONS_PER_IP",
                    lookup("GONVEX_PENDING_CONNECTIONS_PER_IP"),
                    100,
                )?,
                auth_timeout: Duration::from_secs(integer(
                    "GONVEX_AUTH_TIMEOUT_SECONDS",
                    lookup("GONVEX_AUTH_TIMEOUT_SECONDS"),
                    10,
                )? as u64),
                action_timeout: Duration::from_secs(integer(
                    "GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS",
                    lookup("GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS"),
                    60,
                )? as u64),
                connection_calls: integer(
                    "GONVEX_PUBLIC_CALLS_PER_CONNECTION",
                    lookup("GONVEX_PUBLIC_CALLS_PER_CONNECTION"),
                    30,
                )?,
                ip_calls: integer(
                    "GONVEX_PUBLIC_CALLS_PER_IP",
                    lookup("GONVEX_PUBLIC_CALLS_PER_IP"),
                    120,
                )?,
                payload_bytes: integer(
                    "GONVEX_PUBLIC_MAX_PAYLOAD_BYTES",
                    lookup("GONVEX_PUBLIC_MAX_PAYLOAD_BYTES"),
                    64 << 10,
                )?,
                connections_per_ip: integer(
                    "GONVEX_PUBLIC_CONNECTIONS_PER_IP",
                    lookup("GONVEX_PUBLIC_CONNECTIONS_PER_IP"),
                    20,
                )?,
                trusted_proxies: [
                    lookup("GONVEX_TRUSTED_PROXY_CIDRS"),
                    lookup("GONVEX_PUBLIC_TRUSTED_PROXY_IPS"),
                ]
                .into_iter()
                .filter_map(non_empty)
                .flat_map(|value| {
                    value
                        .split(',')
                        .map(|entry| {
                            let entry = entry.trim();
                            entry
                                .parse::<ipnet::IpNet>()
                                .or_else(|_| entry.parse::<IpAddr>().map(ipnet::IpNet::from))
                                .map_err(|_| ConfigError::PublicProxies)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
                client_ip_header: non_empty(lookup("GONVEX_CLIENT_IP_HEADER"))
                    .unwrap_or_else(|| "x-real-ip".to_owned())
                    .parse()
                    .map_err(|_| ConfigError::ClientIpHeader)?,
            },
            service_principals: service_principals(lookup("GONVEX_SERVICE_PRINCIPALS"))?,
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

/// One V8 isolate per available CPU (cgroup quota aware), at most 4. Each
/// isolate holds the whole application bundle (~290 MB for Whagons), and
/// isolates beyond the CPU count add memory without adding throughput.
/// Readers per database file: one per CPU, at least 2 (a long snapshot read
/// must not block other reads) and at most 4.
fn default_readers_per_database() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(2)
        .clamp(2, 4)
}

fn default_isolate_pool_size() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .clamp(1, 4)
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

fn path_list(value: Option<String>) -> Vec<String> {
    non_empty(value)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Zero (or unset, for the receive buffer) keeps the platform default.
fn connection_config(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<ConnectionConfig, ConfigError> {
    let socket = crate::accept::SocketTuning::default();
    let outbound = gonvex_websocket::OutboundLimits::default();
    let optional = |value: usize| (value > 0).then_some(value);
    let max_bytes = integer(
        "GONVEX_WS_OUTBOUND_MAX_BYTES",
        lookup("GONVEX_WS_OUTBOUND_MAX_BYTES"),
        outbound.max_bytes,
    )?;
    Ok(ConnectionConfig {
        socket: crate::accept::SocketTuning {
            send_buffer: optional(integer_allow_zero(
                "GONVEX_TCP_SEND_BUFFER_BYTES",
                lookup("GONVEX_TCP_SEND_BUFFER_BYTES"),
                socket.send_buffer.unwrap_or(0),
            )?),
            receive_buffer: optional(integer_allow_zero(
                "GONVEX_TCP_RECEIVE_BUFFER_BYTES",
                lookup("GONVEX_TCP_RECEIVE_BUFFER_BYTES"),
                socket.receive_buffer.unwrap_or(0),
            )?),
            notsent_lowat: optional(integer_allow_zero(
                "GONVEX_TCP_NOTSENT_LOWAT_BYTES",
                lookup("GONVEX_TCP_NOTSENT_LOWAT_BYTES"),
                socket.notsent_lowat.unwrap_or(0) as usize,
            )?)
            .map(|bytes| bytes.min(u32::MAX as usize) as u32),
        },
        outbound: gonvex_websocket::OutboundLimits {
            max_bytes,
            max_frames: integer(
                "GONVEX_WS_OUTBOUND_MAX_FRAMES",
                lookup("GONVEX_WS_OUTBOUND_MAX_FRAMES"),
                outbound.max_frames,
            )?,
            low_water: outbound.low_water.min(max_bytes),
            stall_timeout: Duration::from_secs(integer(
                "GONVEX_WS_SEND_STALL_TIMEOUT_SECONDS",
                lookup("GONVEX_WS_SEND_STALL_TIMEOUT_SECONDS"),
                outbound.stall_timeout.as_secs() as usize,
            )? as u64),
        },
    })
}

fn presence_config(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<crate::presence::PresenceConfig, ConfigError> {
    let defaults = crate::presence::PresenceConfig::default();
    let ping_interval = Duration::from_secs(integer(
        "GONVEX_PRESENCE_PING_SECONDS",
        lookup("GONVEX_PRESENCE_PING_SECONDS"),
        defaults.ping_interval.as_secs() as usize,
    )? as u64);
    let liveness_timeout = Duration::from_secs(integer(
        "GONVEX_PRESENCE_LIVENESS_SECONDS",
        lookup("GONVEX_PRESENCE_LIVENESS_SECONDS"),
        defaults.liveness_timeout.as_secs() as usize,
    )? as u64)
    // At least two missed Pings before a connection is declared dead.
    .max(ping_interval * 2);
    Ok(crate::presence::PresenceConfig {
        enabled: boolean(
            "GONVEX_PRESENCE_V2",
            lookup("GONVEX_PRESENCE_V2"),
            defaults.enabled,
        )?,
        tick: duration_ms(
            "GONVEX_PRESENCE_TICK_MS",
            lookup("GONVEX_PRESENCE_TICK_MS"),
            defaults.tick.as_millis() as usize,
        )?,
        leading_edge: boolean(
            "GONVEX_PRESENCE_LEADING_EDGE",
            lookup("GONVEX_PRESENCE_LEADING_EDGE"),
            defaults.leading_edge,
        )?,
        offline_grace: Duration::from_millis(integer_allow_zero(
            "GONVEX_PRESENCE_OFFLINE_GRACE_MS",
            lookup("GONVEX_PRESENCE_OFFLINE_GRACE_MS"),
            defaults.offline_grace.as_millis() as usize,
        )? as u64),
        ping_interval,
        liveness_timeout,
        legacy_beat_paths: path_list(lookup("GONVEX_PRESENCE_LEGACY_BEAT_PATHS")),
        legacy_transition_paths: path_list(lookup("GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS")),
        max_pending_batches: defaults.max_pending_batches,
        support_refresh: Duration::from_secs(integer(
            "GONVEX_SUPPORT_LAST_SEEN_REFRESH_SECONDS",
            lookup("GONVEX_SUPPORT_LAST_SEEN_REFRESH_SECONDS"),
            defaults.support_refresh.as_secs() as usize,
        )? as u64),
    })
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

/// Resolves `GONVEX_DATA_DIR` (default `./data`) to an absolute path.
fn data_dir(value: Option<String>) -> Result<PathBuf, ConfigError> {
    let raw = non_empty(value).unwrap_or_else(|| DEFAULT_DATA_DIR.to_owned());
    let path = PathBuf::from(&raw);
    let path = if path.is_absolute() {
        path
    } else {
        env::current_dir()
            .map_err(|_| ConfigError::DataDir(raw.clone()))?
            .join(path)
    };
    // Normalize `.` components so routes are stable across spellings.
    Ok(path
        .components()
        .filter(|component| !matches!(component, std::path::Component::CurDir))
        .collect())
}

/// A JSON object of `sqlite://` routes, normalized to absolute file paths.
fn sqlite_routes(
    name: &'static str,
    value: Option<String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    string_map(name, value)?
        .into_iter()
        .map(|(key, value)| {
            if !value.starts_with("sqlite:") {
                return Err(ConfigError::DatabaseRoute { name, value });
            }
            let route = gonvex_store::database_url(&gonvex_store::database_path(&value));
            Ok((key, route))
        })
        .collect()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServicePrincipalEntry {
    id: String,
    token_sha256: String,
    projects: Vec<String>,
    #[serde(default)]
    functions: Vec<String>,
    #[serde(default)]
    max_delegation_seconds: Option<u64>,
    #[serde(default)]
    manifest: bool,
}

/// Parses `GONVEX_SERVICE_PRINCIPALS`, a JSON array such as
/// `[{"id":"api-gateway","tokenSha256":"<64 hex>","projects":["p1"],
///    "functions":["apiKeys.authenticate"],"maxDelegationSeconds":900,
///    "manifest":true}]`.
/// Invalid entries fail startup instead of silently disabling a credential.
fn service_principals(value: Option<String>) -> Result<Vec<ServicePrincipalConfig>, ConfigError> {
    let Some(value) = non_empty(value) else {
        return Ok(Vec::new());
    };
    let invalid = |message: String| ConfigError::ServicePrincipals(message);
    let entries: Vec<ServicePrincipalEntry> =
        serde_json::from_str(&value).map_err(|error| invalid(error.to_string()))?;
    let mut principals: Vec<ServicePrincipalConfig> = Vec::with_capacity(entries.len());
    for entry in entries {
        let id = entry.id.trim().to_owned();
        if id.is_empty()
            || id.len() > 64
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(invalid(format!(
                "principal id {id:?} must be 1-64 of [A-Za-z0-9_-]"
            )));
        }
        if principals.iter().any(|principal| principal.id == id) {
            return Err(invalid(format!("principal id {id:?} is duplicated")));
        }
        let token_sha256 = entry.token_sha256.trim().to_ascii_lowercase();
        if token_sha256.len() != 64 || !token_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid(format!(
                "principal {id:?} tokenSha256 must be 64 hex characters"
            )));
        }
        if principals
            .iter()
            .any(|principal| principal.token_sha256 == token_sha256)
        {
            return Err(invalid(format!(
                "principal {id:?} reuses another principal's credential"
            )));
        }
        let projects: Vec<String> = entry
            .projects
            .iter()
            .map(|project| project.trim().to_owned())
            .filter(|project| !project.is_empty())
            .collect();
        if projects.is_empty() {
            return Err(invalid(format!(
                "principal {id:?} must list at least one project"
            )));
        }
        let functions: Vec<String> = entry
            .functions
            .iter()
            .map(|function| function.trim().to_owned())
            .filter(|function| !function.is_empty())
            .collect();
        if functions
            .iter()
            .any(|function| function.starts_with("control.") || function.contains('*'))
        {
            return Err(invalid(format!(
                "principal {id:?} functions must be exact tenant module paths"
            )));
        }
        let seconds = entry
            .max_delegation_seconds
            .unwrap_or(SERVICE_PRINCIPAL_DEFAULT_DELEGATION_SECONDS);
        if !(60..=SERVICE_PRINCIPAL_MAX_DELEGATION_SECONDS).contains(&seconds) {
            return Err(invalid(format!(
                "principal {id:?} maxDelegationSeconds must be between 60 and {SERVICE_PRINCIPAL_MAX_DELEGATION_SECONDS}"
            )));
        }
        principals.push(ServicePrincipalConfig {
            id,
            token_sha256,
            projects,
            functions,
            max_delegation: Duration::from_secs(seconds),
            manifest: entry.manifest,
        });
    }
    Ok(principals)
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
    fn connection_limits_config() {
        let defaults = config(&[]).unwrap().connection;
        assert_eq!(defaults.socket.send_buffer, Some(256 << 10));
        assert_eq!(defaults.socket.notsent_lowat, Some(128 << 10));
        assert_eq!(defaults.socket.receive_buffer, None);
        assert_eq!(defaults.outbound, gonvex_websocket::OutboundLimits::default());
        assert_eq!(defaults.outbound.stall_timeout, Duration::from_secs(30));
        let custom = config(&[
            ("GONVEX_TCP_SEND_BUFFER_BYTES", "0"),
            ("GONVEX_TCP_RECEIVE_BUFFER_BYTES", "65536"),
            ("GONVEX_TCP_NOTSENT_LOWAT_BYTES", "0"),
            ("GONVEX_WS_OUTBOUND_MAX_BYTES", "32768"),
            ("GONVEX_WS_OUTBOUND_MAX_FRAMES", "16"),
            ("GONVEX_WS_SEND_STALL_TIMEOUT_SECONDS", "5"),
        ])
        .unwrap()
        .connection;
        assert_eq!(custom.socket.send_buffer, None);
        assert_eq!(custom.socket.receive_buffer, Some(65536));
        assert_eq!(custom.socket.notsent_lowat, None);
        assert_eq!(custom.outbound.max_bytes, 32768);
        assert_eq!(custom.outbound.low_water, 32768);
        assert_eq!(custom.outbound.max_frames, 16);
        assert_eq!(custom.outbound.stall_timeout, Duration::from_secs(5));
        assert!(config(&[("GONVEX_WS_SEND_STALL_TIMEOUT_SECONDS", "0")]).is_err());
    }

    #[test]
    fn presence_v2_config() {
        let defaults = config(&[]).unwrap().presence;
        assert!(defaults.enabled);
        assert_eq!(defaults.tick, Duration::from_millis(250));
        assert!(defaults.leading_edge);
        assert_eq!(defaults.offline_grace, Duration::from_secs(10));
        assert!(defaults.legacy_beat_paths.is_empty());
        assert_eq!(defaults.support_refresh, Duration::from_secs(300));
        assert_eq!(
            config(&[("GONVEX_SUPPORT_LAST_SEEN_REFRESH_SECONDS", "60")])
                .unwrap()
                .presence
                .support_refresh,
            Duration::from_secs(60)
        );
        let custom = config(&[
            ("GONVEX_PRESENCE_V2", "off"),
            ("GONVEX_PRESENCE_TICK_MS", "1000"),
            ("GONVEX_PRESENCE_LEADING_EDGE", "off"),
            ("GONVEX_PRESENCE_OFFLINE_GRACE_MS", "0"),
            ("GONVEX_PRESENCE_PING_SECONDS", "40"),
            ("GONVEX_PRESENCE_LIVENESS_SECONDS", "30"),
            (
                "GONVEX_PRESENCE_LEGACY_BEAT_PATHS",
                "presence.beat, other.beat",
            ),
            (
                "GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS",
                "presence.transition",
            ),
        ])
        .unwrap()
        .presence;
        assert!(!custom.enabled);
        assert_eq!(custom.tick, Duration::from_millis(1000));
        assert!(!custom.leading_edge);
        assert_eq!(custom.offline_grace, Duration::ZERO);
        // Liveness never drops below two Ping intervals.
        assert_eq!(custom.liveness_timeout, Duration::from_secs(80));
        assert_eq!(
            custom.legacy_beat_paths,
            vec!["presence.beat", "other.beat"]
        );
        assert_eq!(custom.legacy_transition_paths, vec!["presence.transition"]);
        assert!(config(&[("GONVEX_PRESENCE_TICK_MS", "0")]).is_err());
    }

    #[test]
    fn database_idle_timeout_is_configurable() {
        assert_eq!(
            config(&[]).unwrap().database_idle_timeout,
            Duration::from_secs(15)
        );
        assert_eq!(
            config(&[("GONVEX_DB_IDLE_TIMEOUT_MS", "1500")])
                .unwrap()
                .database_idle_timeout,
            Duration::from_millis(1500)
        );
        assert!(config(&[("GONVEX_DB_IDLE_TIMEOUT_MS", "0")]).is_err());
    }

    #[test]
    fn websocket_compression_config() {
        let defaults = config(&[]).unwrap();
        assert!(defaults.websocket_compression);
        assert!(
            !config(&[("GONVEX_WS_COMPRESSION", "false")])
                .unwrap()
                .websocket_compression
        );
        assert_eq!(defaults.websocket_compression_threshold, 1024);
        let enabled = config(&[
            ("GONVEX_WS_COMPRESSION", "true"),
            ("GONVEX_WS_COMPRESSION_THRESHOLD", "2048"),
        ])
        .unwrap();
        assert!(enabled.websocket_compression);
        assert_eq!(enabled.websocket_compression_threshold, 2048);
        assert!(matches!(
            config(&[("GONVEX_WS_COMPRESSION", "maybe")]),
            Err(ConfigError::Boolean { .. })
        ));
        assert!(matches!(
            config(&[("GONVEX_WS_COMPRESSION_THRESHOLD", "0")]),
            Err(ConfigError::Integer { .. })
        ));
    }

    #[test]
    fn public_proxy_config_merges_cidrs_and_legacy_ips() {
        let limits = config(&[
            (
                "GONVEX_TRUSTED_PROXY_CIDRS",
                "10.0.0.0/8, 2001:db8::/32, 192.0.2.1",
            ),
            (
                "GONVEX_PUBLIC_TRUSTED_PROXY_IPS",
                "127.0.0.1, 172.16.0.0/12",
            ),
            ("GONVEX_CLIENT_IP_HEADER", "CF-Connecting-IP"),
        ])
        .unwrap()
        .public_functions;
        assert_eq!(limits.trusted_proxies.len(), 5);
        for ip in [
            "10.123.4.5",
            "2001:db8:1::1",
            "192.0.2.1",
            "127.0.0.1",
            "172.31.1.1",
        ] {
            assert!(limits
                .trusted_proxies
                .iter()
                .any(|net| net.contains(&ip.parse::<IpAddr>().unwrap())));
        }
        assert_eq!(limits.client_ip_header, "cf-connecting-ip");
        let legacy = config(&[("GONVEX_PUBLIC_TRUSTED_PROXY_IPS", "127.0.0.1")]).unwrap();
        assert_eq!(
            legacy.public_functions.trusted_proxies,
            vec!["127.0.0.1/32".parse::<ipnet::IpNet>().unwrap()]
        );
        assert_eq!(
            config(&[]).unwrap().public_functions.client_ip_header,
            "x-real-ip"
        );
        for name in [
            "GONVEX_TRUSTED_PROXY_CIDRS",
            "GONVEX_PUBLIC_TRUSTED_PROXY_IPS",
        ] {
            for value in ["10.0.0.0/33", "garbage", "127.0.0.1,"] {
                assert_eq!(
                    config(&[(name, value)]).unwrap_err(),
                    ConfigError::PublicProxies
                );
            }
        }
        assert_eq!(
            config(&[("GONVEX_CLIENT_IP_HEADER", "bad header")]).unwrap_err(),
            ConfigError::ClientIpHeader
        );
    }

    #[test]
    fn parses_database_idle_timeout_without_changing_pool_defaults() {
        let defaults = config(&[]).expect("defaults");
        assert_eq!(defaults.database_idle_timeout, Duration::from_secs(15));
        let tuned = config(&[("GONVEX_DB_IDLE_TIMEOUT_MS", "1000")]).expect("timeout");
        assert_eq!(tuned.database_idle_timeout, Duration::from_secs(1));
        assert_eq!(
            tuned.database_max_connections,
            defaults.database_max_connections
        );
        assert!(config(&[("GONVEX_DB_IDLE_TIMEOUT_MS", "bad")]).is_err());
    }

    #[test]
    fn reads_the_existing_runtime_environment_contract() {
        let config = config(&[
            ("GONVEX_ADDR", ":9090"),
            ("GONVEX_DATA_DIR", "/var/lib/gonvex/data"),
            ("GONVEX_REQUIRE_AUTH", "true"),
            (
                "GONVEX_TENANT_DATABASE_URLS",
                r#"{"tenant-a":"sqlite:///srv/a.db"}"#,
            ),
            ("GONVEX_MODULE_HOST_BINARY", "/bin/gonvex-module-host"),
            ("SOURCE_COMMIT", "0123456789abcdef0123456789abcdef01234567"),
        ])
        .expect("config");

        assert_eq!(config.addr, "0.0.0.0:9090".parse().unwrap());
        assert_eq!(
            config.data_dir(),
            Some(PathBuf::from("/var/lib/gonvex/data"))
        );
        assert_eq!(
            config.control_plane_database_url.as_deref(),
            Some("sqlite:///var/lib/gonvex/data/control.db")
        );
        assert_eq!(
            config.default_database_url.as_deref(),
            Some("sqlite:///var/lib/gonvex/data")
        );
        assert_eq!(
            config.tenant_database_urls["tenant-a"],
            "sqlite:///srv/a.db"
        );
        assert!(config.require_auth);
        assert_eq!(config.database_max_total_connections, 64);
        assert_eq!(
            config.runtime_version,
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn data_dir_defaults_to_an_absolute_data_directory() {
        let data_dir = config(&[]).expect("config").data_dir().unwrap();
        assert!(data_dir.is_absolute());
        assert!(data_dir.ends_with("data"));
        assert!(!data_dir.to_string_lossy().contains("/./"));
        let relative = config(&[("GONVEX_DATA_DIR", "state/db")])
            .expect("config")
            .data_dir()
            .unwrap();
        assert!(relative.is_absolute());
        assert!(relative.ends_with("state/db"));
    }

    #[test]
    fn postgres_database_variables_fail_startup() {
        for name in ["DATABASE_URL", "GONVEX_CONTROL_PLANE_DATABASE_URL"] {
            assert_eq!(
                config(&[(name, "postgres://example")]).unwrap_err(),
                ConfigError::RemovedDatabaseVariable(name)
            );
        }
        assert!(matches!(
            config(&[("GONVEX_PROJECT_DATABASE_URLS", r#"{"p":"postgres://x"}"#)]).unwrap_err(),
            ConfigError::DatabaseRoute { .. }
        ));
    }

    #[test]
    fn parses_service_principals() {
        let hash = "AB".repeat(32);
        let config = config(&[(
            "GONVEX_SERVICE_PRINCIPALS",
            &format!(
                r#"[{{"id":"api-gateway","tokenSha256":"{hash}","projects":[" p1 "],"functions":["keys.authenticate"]}}]"#
            ),
        )])
        .expect("config");
        assert_eq!(config.service_principals.len(), 1);
        let principal = &config.service_principals[0];
        assert_eq!(principal.id, "api-gateway");
        assert_eq!(principal.token_sha256, "ab".repeat(32));
        assert_eq!(principal.projects, vec!["p1".to_owned()]);
        assert_eq!(principal.functions, vec!["keys.authenticate".to_owned()]);
        assert_eq!(principal.max_delegation, Duration::from_secs(900));
        assert!(!principal.manifest, "manifest access is opt-in");
        assert!(config_default_has_no_principals());
    }

    fn config_default_has_no_principals() -> bool {
        config(&[]).expect("config").service_principals.is_empty()
    }

    #[test]
    fn parses_service_principal_manifest_access() {
        let hash = "ab".repeat(32);
        for (flag, expected) in [("true", true), ("false", false)] {
            let config = config(&[(
                "GONVEX_SERVICE_PRINCIPALS",
                &format!(
                    r#"[{{"id":"gateway","tokenSha256":"{hash}","projects":["p1"],"manifest":{flag}}}]"#
                ),
            )])
            .expect("config");
            assert_eq!(config.service_principals[0].manifest, expected, "{flag}");
        }
        for flag in [r#""true""#, "1", "null", "{}"] {
            let value = format!(
                r#"[{{"id":"gateway","tokenSha256":"{hash}","projects":["p1"],"manifest":{flag}}}]"#
            );
            assert!(
                matches!(
                    config(&[("GONVEX_SERVICE_PRINCIPALS", &value)]),
                    Err(ConfigError::ServicePrincipals(_))
                ),
                "manifest {flag} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_invalid_service_principals() {
        let hash = "ab".repeat(32);
        for value in [
            "not json".to_owned(),
            format!(r#"[{{"id":"","tokenSha256":"{hash}","projects":["p"]}}]"#),
            r#"[{"id":"a","tokenSha256":"short","projects":["p"]}]"#.to_owned(),
            format!(r#"[{{"id":"a","tokenSha256":"{hash}","projects":[]}}]"#),
            format!(
                r#"[{{"id":"a","tokenSha256":"{hash}","projects":["p"],"functions":["control.x"]}}]"#
            ),
            format!(
                r#"[{{"id":"a","tokenSha256":"{hash}","projects":["p"],"maxDelegationSeconds":5}}]"#
            ),
            format!(r#"[{{"id":"a","tokenSha256":"{hash}","projects":["p"],"extra":true}}]"#),
            format!(
                r#"[{{"id":"a","tokenSha256":"{hash}","projects":["p"]}},{{"id":"b","tokenSha256":"{hash}","projects":["p"]}}]"#
            ),
        ] {
            assert!(
                matches!(
                    config(&[("GONVEX_SERVICE_PRINCIPALS", &value)]),
                    Err(ConfigError::ServicePrincipals(_))
                ),
                "{value} should be rejected"
            );
        }
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
