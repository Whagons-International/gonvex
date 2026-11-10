//! SQLite database ownership: one writer connection and a read-only pool per
//! database file, an in-process notification channel replacing LISTEN/NOTIFY,
//! and keyed async locks replacing Postgres advisory locks.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use tokio::sync::{broadcast, Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use crate::DatabaseError;

const DEFAULT_MAX_TOTAL_CONNECTIONS: usize = 64;
const MAX_TOTAL_CONNECTIONS: usize = 256;
const DEFAULT_READERS_PER_DATABASE: u32 = 4;
const NOTIFY_CAPACITY: usize = 4096;
const DEFAULT_WARM_READER_MS: u64 = 10 * 60 * 1000;
const DEFAULT_READER_PATIENCE_MS: u64 = 25;
const DEFAULT_WARM_READERS: u64 = 1;
const DEFAULT_READER_OPENS: u64 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolLimits {
    /// Concurrent SQL jobs across every database (CPU fairness, not sockets).
    pub max_total_connections: usize,
    /// Read-only connections per database file. Writes always use one writer.
    pub max_connections_per_database: u32,
    pub max_idle_connections_per_database: u32,
    /// A database's readers beyond the warm one close once none of them has
    /// been used for this long.
    pub idle_timeout: Duration,
    /// One reader stays open (warm) while the database had a read within this
    /// window, so crons and page loads after a quiet spell do not pay a
    /// connection open plus a full schema parse (2.9 MB, ~30 ms of CPU for
    /// the Whagons tenant schema). `GONVEX_DB_WARM_READER_MS`, default 10
    /// minutes; zero closes every reader after `idle_timeout`. Warm readers
    /// count against `GONVEX_SQLITE_CACHE_BUDGET_MB` (see `sweep_readers`).
    pub warm_reader_window: Duration,
    /// A read transaction that finds every open reader busy waits up to this
    /// long for one to be released before opening another
    /// (`GONVEX_DB_READER_PATIENCE_MS`, default 25; zero opens at once). An
    /// open parses the whole schema (~30 ms of CPU, seconds when a burst of
    /// opens shares one vCPU), while most reads take well under a
    /// millisecond, so a page load's burst of reads shares the warm reader.
    pub reader_patience: Duration,
    /// Readers kept open while a database was read within
    /// `warm_reader_window` (`GONVEX_DB_WARM_READERS`, default 1).
    pub warm_readers: u32,
    /// Reader connections opened at once across the process
    /// (`GONVEX_DB_READER_OPENS`, default 2). Each open parses the whole
    /// schema; concurrent parses contend on SQLite's global memory mutex and
    /// on the CPU, so 8 at once took 1.1-2.5 s each on staging (2026-11-04)
    /// where one alone takes ~30 ms. With a limit, a database also opens one
    /// reader at a time: a burst of reads waits for the reader being opened
    /// (or for any reader released meanwhile) instead of opening up to
    /// `max_connections_per_database` in parallel. Zero: no limit (opens
    /// race, the previous behaviour).
    pub reader_opens: u32,
    pub acquire_timeout: Duration,
}

impl Default for PoolLimits {
    fn default() -> Self {
        Self {
            max_total_connections: DEFAULT_MAX_TOTAL_CONNECTIONS,
            max_connections_per_database: DEFAULT_READERS_PER_DATABASE,
            max_idle_connections_per_database: DEFAULT_READERS_PER_DATABASE,
            idle_timeout: Duration::from_secs(600),
            warm_reader_window: Duration::from_millis(env_u64(
                "GONVEX_DB_WARM_READER_MS",
                DEFAULT_WARM_READER_MS,
            )),
            reader_patience: Duration::from_millis(env_u64(
                "GONVEX_DB_READER_PATIENCE_MS",
                DEFAULT_READER_PATIENCE_MS,
            )),
            warm_readers: env_u64("GONVEX_DB_WARM_READERS", DEFAULT_WARM_READERS)
                .min(u64::from(u32::MAX)) as u32,
            reader_opens: env_u64("GONVEX_DB_READER_OPENS", DEFAULT_READER_OPENS)
                .min(u64::from(u32::MAX)) as u32,
            acquire_timeout: Duration::from_secs(30),
        }
    }
}

impl PoolLimits {
    pub fn bounded(mut self) -> Self {
        self.max_total_connections = self.max_total_connections.clamp(1, MAX_TOTAL_CONNECTIONS);
        self.max_connections_per_database = self.max_connections_per_database.clamp(1, 64);
        self.max_idle_connections_per_database = self
            .max_idle_connections_per_database
            .min(self.max_connections_per_database);
        self.warm_readers = self.warm_readers.min(self.max_connections_per_database);
        self
    }
}

/// Durability of a commit. `Full` syncs the WAL on every commit; `Normal`
/// syncs at checkpoints and can lose the last commits only on power loss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    Full,
    Normal,
}

impl Durability {
    pub fn from_env() -> Self {
        match std::env::var("GONVEX_SQLITE_SYNCHRONOUS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "normal" => Self::Normal,
            _ => Self::Full,
        }
    }
}

/// Private page cache per connection in KiB (`GONVEX_SQLITE_CACHE_KIB`,
/// default 8 MiB).
fn sqlite_cache_kib() -> u64 {
    env_u64("GONVEX_SQLITE_CACHE_KIB", 8 * 1024)
}

/// Process-wide budget for SQLite's heap in MiB
/// (`GONVEX_SQLITE_CACHE_BUDGET_MB`, default 128; 0 disables). Every
/// connection of every tenant file allocates its page cache, parsed schema
/// and prepared statements from one heap; past the budget SQLite recycles
/// page-cache pages instead of growing (`sqlite3_soft_heap_limit64`), so the
/// total stays near the budget however many files and readers are open.
/// It is a soft limit: allocations never fail because of it.
pub fn sqlite_cache_budget_mib() -> u64 {
    env_u64("GONVEX_SQLITE_CACHE_BUDGET_MB", 128)
}

/// Applies [`sqlite_cache_budget_mib`] once per process.
fn apply_cache_budget() {
    static APPLIED: std::sync::Once = std::sync::Once::new();
    APPLIED.call_once(|| {
        let budget = sqlite_cache_budget_mib();
        if budget > 0 {
            unsafe {
                libsqlite3_sys::sqlite3_soft_heap_limit64((budget * 1024 * 1024) as i64);
            }
        }
    });
}

/// Run on each writer connection when it opens: `0x10002` re-analyzes every
/// table whose row count moved ~10x since its statistics (or that has none
/// while it has indexes), bounded by `analysis_limit` rows per index.
const OPTIMIZE_AT_OPEN: &str = "PRAGMA analysis_limit=400; PRAGMA optimize=0x10002;";
/// Periodic form (tables used since the connection opened).
const OPTIMIZE_PERIODIC: &str = "PRAGMA analysis_limit=400; PRAGMA optimize;";

async fn optimize_at_open(connection: &mut sqlx::SqliteConnection) -> Result<(), sqlx::Error> {
    for statement in OPTIMIZE_AT_OPEN.split_inclusive(';') {
        sqlx::query(statement.trim())
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

static READER_OPENS: AtomicU64 = AtomicU64::new(0);

/// Reader connections opened since start (each parses the file's schema).
pub fn sqlite_reader_opens() -> u64 {
    READER_OPENS.load(Ordering::Relaxed)
}

/// SQLite heap in use by this process, in bytes.
pub fn sqlite_memory_used() -> i64 {
    unsafe { libsqlite3_sys::sqlite3_memory_used() }
}

/// Shared memory-mapped window per database file in MiB
/// (`GONVEX_SQLITE_MMAP_MIB`, default 256; 0 disables mmap).
fn sqlite_mmap_mib() -> u64 {
    env_u64("GONVEX_SQLITE_MMAP_MIB", 256)
}

/// Prepared statements cached per connection
/// (`GONVEX_SQLITE_STATEMENT_CACHE`, default 128).
fn sqlite_statement_cache() -> usize {
    env_u64("GONVEX_SQLITE_STATEMENT_CACHE", 128) as usize
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

/// Milliseconds since this process first asked (monotonic).
fn now_ms() -> u64 {
    static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let elapsed = EPOCH.get_or_init(std::time::Instant::now).elapsed();
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Minimum time between two budget closures of the same busy database's
/// extra readers (`GONVEX_DB_BUSY_BUDGET_CLOSE_MS`, default 60,000; 0 = every
/// sweep, the previous behaviour). Under steady load the writers plus one warm
/// reader per database can exceed the allowance by themselves; closing busy
/// databases' extra readers every few seconds then only churns them (1.5
/// reopens/s at 1k users, statement re-preparation 4-10 % of runtime CPU).
fn busy_budget_close_interval_ms() -> u64 {
    static INTERVAL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *INTERVAL.get_or_init(|| env_u64("GONVEX_DB_BUSY_BUDGET_CLOSE_MS", 60_000))
}

fn sweep_interval(limits: &PoolLimits) -> Duration {
    let shortest = if limits.warm_reader_window.is_zero() {
        limits.idle_timeout
    } else {
        limits.idle_timeout.min(limits.warm_reader_window)
    };
    (shortest / 4).clamp(Duration::from_millis(50), Duration::from_secs(5))
}

/// SQLite heap that closing connections would free but page-cache eviction
/// would not: parsed schemas and prepared statements (2.9 MB of schema per
/// Whagons tenant connection). Page-cache bytes are excluded; past the soft
/// heap limit SQLite recycles those by itself.
pub fn sqlite_connection_heap() -> i64 {
    let (mut cache, mut high) = (0i64, 0i64);
    unsafe {
        libsqlite3_sys::sqlite3_status64(
            libsqlite3_sys::SQLITE_STATUS_PAGECACHE_OVERFLOW,
            &mut cache,
            &mut high,
            0,
        );
    }
    sqlite_memory_used().saturating_sub(cache)
}

/// Closes idle readers of `pool` until at most `keep` connections remain.
async fn close_idle_readers(pool: &SqlitePool, keep: usize) {
    while pool.num_idle() > 0 && pool.size() as usize > keep {
        // Never opens a connection: `None` when none is idle.
        let Some(connection) = pool.try_acquire() else {
            return;
        };
        let _ = connection.close().await;
    }
}

/// Reader retention, run every [`sweep_interval`]:
///
/// * read within `idle_timeout`: up to `max_idle_connections_per_database`;
/// * read within `warm_reader_window`: `warm_readers` warm readers (1);
/// * otherwise: none.
///
/// Warm readers count against the SQLite budget: while connection heap
/// (schemas and statements) exceeds three quarters of
/// `GONVEX_SQLITE_CACHE_BUDGET_MB`, the least recently read databases lose
/// their warm reader first, then busy databases their idle extra readers.
/// The remaining quarter is left to page caches.
async fn sweep_readers(databases: &[Database], limits: &PoolLimits) {
    let now = now_ms();
    let busy_ms = u64::try_from(limits.idle_timeout.as_millis()).unwrap_or(u64::MAX);
    let warm_ms = u64::try_from(limits.warm_reader_window.as_millis()).unwrap_or(u64::MAX);
    let mut warm = Vec::new();
    let mut busy = Vec::new();
    for database in databases {
        let used = database.reader_used.load(Ordering::Relaxed);
        let idle_for = now.saturating_sub(used);
        let keep = if idle_for < busy_ms {
            limits.max_idle_connections_per_database as usize
        } else if idle_for < warm_ms {
            limits.warm_readers as usize
        } else {
            0
        };
        close_idle_readers(&database.reader, keep).await;
        if database.reader.num_idle() > 0 {
            if idle_for < busy_ms {
                busy.push((used, database));
            } else {
                warm.push((used, database));
            }
        }
    }
    let budget = sqlite_cache_budget_mib().saturating_mul(1024 * 1024);
    if budget == 0 || (warm.is_empty() && busy.is_empty()) {
        return;
    }
    let allowance = i64::try_from(budget / 4 * 3).unwrap_or(i64::MAX);
    if sqlite_connection_heap() <= allowance {
        return;
    }
    warm.sort_by_key(|(used, _)| *used);
    for (_, database) in warm {
        close_idle_readers(&database.reader, 0).await;
        if sqlite_connection_heap() <= allowance {
            return;
        }
    }
    busy.sort_by_key(|(used, _)| *used);
    let interval = busy_budget_close_interval_ms();
    for (_, database) in busy {
        // A busy database reopens closed readers at once (schema parse and a
        // cold statement cache each time) and the heap returns; close them at
        // most once per interval instead of on every sweep.
        let last = database.budget_closed.load(Ordering::Relaxed);
        if interval > 0 && last > 0 && now.saturating_sub(last) < interval {
            continue;
        }
        database.budget_closed.store(now.max(1), Ordering::Relaxed);
        close_idle_readers(&database.reader, 1).await;
        if sqlite_connection_heap() <= allowance {
            return;
        }
    }
}

/// A notification published on one database's in-process channel.
#[derive(Clone, Debug)]
pub struct Notification {
    channel: Arc<str>,
    payload: Arc<str>,
}

impl Notification {
    pub fn channel(&self) -> &str {
        &self.channel
    }
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

/// Subscription to a database's notifications, filtered to listened channels.
pub struct DatabaseListener {
    receiver: broadcast::Receiver<Notification>,
    channels: HashSet<String>,
}

impl DatabaseListener {
    pub async fn listen(&mut self, channel: &str) -> Result<(), DatabaseError> {
        self.channels.insert(channel.to_owned());
        Ok(())
    }

    /// Waits for the next notification on a listened channel. A lagged
    /// receiver reports `Lagged` so callers can repair from durable state,
    /// exactly as a reconnecting Postgres listener had to.
    pub async fn recv(&mut self) -> Result<Notification, DatabaseError> {
        loop {
            match self.receiver.recv().await {
                Ok(notification) if self.channels.contains(notification.channel()) => {
                    return Ok(notification)
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    return Err(DatabaseError::ListenerLagged)
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(DatabaseError::ListenerClosed)
                }
            }
        }
    }
}

/// Shared SQL-job admission held for the whole job.
pub struct AdmissionPermit {
    _shared: OwnedSemaphorePermit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolRegistrySnapshot {
    pub pools: usize,
    pub open_connections: u32,
    pub in_use: u32,
    pub idle: usize,
    /// Pools whose non-atomic SQLx idle sample was outside 0..=size.
    /// Their idle contribution is unavailable (excluded), so in_use is an upper bound.
    pub idle_samples_unavailable: usize,
    pub max_open_connections: u32,
    pub listener_connections: u32,
    pub admission_limit: usize,
    pub admission_active: usize,
    /// Completed admission attempts, including uncontended successes and timeouts.
    pub admission_count: u64,
    pub admission_wait_us: u64,
    pub admission_wait_max_us: u64,
}

#[derive(Default)]
struct ConnectionCounts {
    open: u32,
    idle: usize,
    unavailable_idle_samples: usize,
}

fn connection_counts(samples: impl IntoIterator<Item = (u32, usize)>) -> ConnectionCounts {
    let mut counts = ConnectionCounts::default();
    for (open, idle) in samples {
        counts.open += open;
        // SQLx 0.8.6 publishes a returned connection/permit before increasing
        // num_idle. A concurrent checkout can briefly expose usize::MAX.
        // Size and idle are also separate samples. Neither an underflow value
        // nor idle > size is a physical idle count: expose it as unavailable,
        // rather than adding it or reporting the pool as entirely idle.
        if idle <= open as usize {
            counts.idle += idle;
        } else {
            counts.unavailable_idle_samples += 1;
        }
    }
    counts
}

/// Per-database single-writer queue statistics, updated with relaxed atomics
/// on the transaction path (no registry lock).
#[derive(Default)]
pub struct WriterStats {
    waiting: std::sync::atomic::AtomicU32,
    acquisitions: AtomicU64,
    wait_ns: AtomicU64,
    wait_max_ns: AtomicU64,
    /// Set once the database is known to carry the change feed (it is never
    /// removed), so commits skip the schema lookup.
    pub(crate) has_feed: std::sync::atomic::AtomicBool,
}

/// Holds a writer-queue slot while a transaction waits for its database's
/// single writer.
pub struct WriterWaiting<'a> {
    stats: &'a WriterStats,
    _global: gonvex_profiler::GaugeGuard,
}

impl WriterStats {
    pub fn enter(&self) -> WriterWaiting<'_> {
        self.waiting.fetch_add(1, Ordering::Relaxed);
        WriterWaiting {
            stats: self,
            _global: gonvex_profiler::gauge(gonvex_profiler::GaugeId::WriterWaiting).guard(),
        }
    }

    pub fn acquired(&self, wait_ns: u64) {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.wait_ns.fetch_add(wait_ns, Ordering::Relaxed);
        if wait_ns > self.wait_max_ns.load(Ordering::Relaxed) {
            self.wait_max_ns.fetch_max(wait_ns, Ordering::Relaxed);
        }
    }
}

impl Drop for WriterWaiting<'_> {
    fn drop(&mut self) {
        self.stats.waiting.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One database's writer queue, for the profiler dump.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriterQueue {
    pub database: String,
    pub waiting: u32,
    pub acquisitions: u64,
    pub wait_ns: u64,
    pub wait_max_ns: u64,
}

#[derive(Clone)]
pub(crate) struct Database {
    /// Prepared statements of the writer connection (see `direct`). Declared
    /// before `writer`: when the last handle drops, they are finalized while
    /// the connection is still open.
    pub(crate) statements: Arc<crate::direct::WriterStatements>,
    pub(crate) writer: SqlitePool,
    pub(crate) writer_stats: Arc<WriterStats>,
    pub(crate) reader: SqlitePool,
    /// When a reader connection was last released ([`now_ms`]).
    reader_used: Arc<AtomicU64>,
    /// One reader open at a time for this database (see
    /// [`PoolLimits::reader_opens`]).
    reader_open: Arc<Semaphore>,
    /// When the budget sweep last closed this busy database's extra readers
    /// (`now_ms`; 0 = never). See [`busy_budget_close_interval_ms`].
    budget_closed: Arc<AtomicU64>,
    notify: broadcast::Sender<Notification>,
    locks: Arc<std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// Batched WAL syncs for `Durability::Full` (see `sync_barrier`).
    sync: Arc<crate::sync_barrier::SyncBarrier>,
    /// Background WAL checkpoints (see `checkpoint`); `None` when SQLite's
    /// automatic checkpoint runs inside the writer's commits.
    wal: Option<Arc<crate::checkpoint::WalState>>,
}

#[derive(Clone)]
pub struct PoolRegistry {
    limits: PoolLimits,
    durability: Durability,
    admission: Arc<Semaphore>,
    /// Reader opens in flight across databases ([`PoolLimits::reader_opens`]);
    /// `None` when unlimited.
    reader_opens: Option<Arc<Semaphore>>,
    admission_count: Arc<AtomicU64>,
    admission_wait_us: Arc<AtomicU64>,
    admission_wait_max_us: Arc<AtomicU64>,
    // Per-file initializers. The map lock is held only to find or insert an
    // initializer, never across file I/O, so a slow or suspended open of one
    // file cannot block lookups of every other database.
    databases: Arc<std::sync::Mutex<BTreeMap<String, Arc<tokio::sync::OnceCell<Database>>>>>,
    /// Set once the reader sweeper task runs (started by the first open).
    sweeper: Arc<std::sync::atomic::AtomicBool>,
}

/// Guard for a keyed in-process lock (replaces `pg_advisory_lock`).
pub struct KeyedLock {
    _guard: OwnedMutexGuard<()>,
}

/// Normalizes `sqlite://…`, `sqlite:…` or a bare path to an absolute file path.
pub fn database_path(database_url: &str) -> PathBuf {
    let trimmed = database_url.trim();
    let path = trimmed
        .strip_prefix("sqlite://")
        .or_else(|| trimmed.strip_prefix("sqlite:"))
        .unwrap_or(trimmed);
    let path = path.split('?').next().unwrap_or(path);
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|dir| dir.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

/// Canonical route string for a database file.
pub fn database_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

fn key(database_url: &str) -> String {
    database_path(database_url).display().to_string()
}

impl PoolRegistry {
    pub fn new(limits: PoolLimits) -> Self {
        Self::with_durability(limits, Durability::from_env())
    }

    pub fn with_durability(limits: PoolLimits, durability: Durability) -> Self {
        let limits = limits.bounded();
        apply_cache_budget();
        crate::statement_profile::install();
        crate::reader_priority::install();
        Self {
            admission: Arc::new(Semaphore::new(limits.max_total_connections)),
            reader_opens: (limits.reader_opens > 0)
                .then(|| Arc::new(Semaphore::new(limits.reader_opens as usize))),
            admission_count: Arc::default(),
            admission_wait_us: Arc::default(),
            admission_wait_max_us: Arc::default(),
            databases: Arc::default(),
            sweeper: Arc::default(),
            durability,
            limits,
        }
    }

    fn options(&self, path: &Path, read_only: bool) -> Result<SqliteConnectOptions, DatabaseError> {
        let url = format!("sqlite://{}", path.display());
        let mut options = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(!read_only)
            .read_only(read_only);
        // WAL is a persistent property of the file, set once by the writer.
        // `PRAGMA journal_mode` takes a lock, so issuing it on every reader
        // open made reader opens wait behind the writer's transactions.
        if !read_only {
            // Serialized mode: direct sessions finalize their cached
            // statements on the writer from any thread (see `direct`).
            options = options
                .journal_mode(SqliteJournalMode::Wal)
                .serialized(true);
            // COMMIT never syncs while holding the single writer; Full
            // durability is provided by waiting on the database's batched
            // WAL sync before a commit is acknowledged (`durable`). Readers
            // never commit, so they skip it.
            options = options.synchronous(SqliteSynchronous::Normal);
        }
        // sqlx sends every pragma below in one batch when a connection
        // opens. The first one that needs the schema (any but
        // `foreign_keys`) parses the whole schema of the file: that parse is
        // nearly all of a connection's setup cost, hence warm readers.
        let options = options
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5))
            // Private page cache per connection. Many tenants times several
            // connections each makes a large private cache expensive, so the
            // shared memory map below holds hot pages once for all
            // connections (file-backed, reclaimable by the kernel).
            .pragma("cache_size", format!("-{}", sqlite_cache_kib()))
            .pragma("mmap_size", (sqlite_mmap_mib() * 1024 * 1024).to_string())
            .pragma("temp_store", "MEMORY")
            // Module SQL may use `col REGEXP $n` (Rust `regex` syntax).
            .with_regexp()
            .statement_cache_capacity(sqlite_statement_cache());
        Ok(options)
    }

    pub(crate) async fn database(&self, database_url: &str) -> Result<Database, DatabaseError> {
        let key = key(database_url);
        let cell = self
            .databases
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone();
        // Concurrent callers for the same file share one open; a failed open
        // leaves the cell empty so the next caller retries.
        // The initializer is constructed only on a cold miss. Keep its connection
        // setup state out of every warm registry/commit/feed future.
        cell.get_or_try_init(|| Box::pin(self.open(&key)))
            .await
            .cloned()
    }

    async fn open(&self, key: &str) -> Result<Database, DatabaseError> {
        let path = PathBuf::from(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| DatabaseError::Io(error.to_string()))?;
        }
        let wal = crate::checkpoint::background_enabled()
            .then(|| crate::checkpoint::WalState::new(&path));
        let writer_wal = wal.clone();
        // The writer creates the file and switches it to WAL before any reader
        // opens it; a read-only connection cannot change the journal mode.
        let writer_options = self.options(&path, false)?;
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            // A local SQLite connection does not drop like a socket; the
            // default ping on every acquire was one more worker round trip
            // per transaction.
            .test_before_acquire(false)
            .acquire_timeout(self.limits.acquire_timeout)
            // Change capture reads row values through the pre-update hook,
            // installed on the writer only (readers never write).
            .after_connect(move |connection, _| {
                let wal = writer_wal.clone();
                Box::pin(async move {
                    // Planner statistics: ANALYZE the tables whose size
                    // changed a lot since their last statistics (stale
                    // `sqlite_stat1` rows made indexed lookups full scans).
                    optimize_at_open(connection).await?;
                    crate::capture::install(connection)
                        .await
                        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
                    // Checkpoints leave the writer's commits (see `checkpoint`).
                    if let Some(wal) = wal {
                        crate::checkpoint::install(connection, wal)
                            .await
                            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
                    }
                    Ok(())
                })
            })
            .connect_with(writer_options);
        // The writer's sqlx worker thread is created by whichever thread polls
        // the connect; open it at base priority (see `reader_priority`).
        let writer = crate::reader_priority::at_base_priority(writer).await?;
        let reader_used = Arc::new(AtomicU64::new(now_ms()));
        let released = reader_used.clone();
        // Readers close through `sweep_readers`, which knows when the
        // database as a whole was last read; sqlx's per-connection idle
        // timer would close the warm reader too.
        let reader = SqlitePoolOptions::new()
            .max_connections(self.limits.max_connections_per_database)
            .min_connections(0)
            .idle_timeout(None)
            .max_lifetime(None)
            .test_before_acquire(false)
            .acquire_timeout(self.limits.acquire_timeout)
            .after_release(move |_, _| {
                released.store(now_ms(), Ordering::Relaxed);
                Box::pin(async { Ok(true) })
            })
            .after_connect(|_, _| {
                READER_OPENS.fetch_add(1, Ordering::Relaxed);
                Box::pin(async { Ok(()) })
            })
            .connect_lazy_with(self.options(&path, true)?);
        self.start_sweeper();
        let (notify, _) = broadcast::channel(NOTIFY_CAPACITY);
        let database = Database {
            statements: crate::direct::WriterStatements::new(),
            writer,
            writer_stats: Arc::default(),
            reader,
            reader_used,
            reader_open: Arc::new(Semaphore::new(1)),
            budget_closed: Arc::default(),
            notify,
            locks: Arc::default(),
            sync: crate::sync_barrier::SyncBarrier::new(&path),
            wal,
        };
        Ok(database)
    }

    fn start_sweeper(&self) {
        if self.sweeper.swap(true, Ordering::AcqRel) {
            return;
        }
        let databases = Arc::downgrade(&self.databases);
        let limits = self.limits.clone();
        tokio::spawn(async move {
            let interval = sweep_interval(&limits);
            let optimize_every =
                Duration::from_secs(env_u64("GONVEX_SQLITE_OPTIMIZE_SECS", 3600).max(60));
            // First pass about a minute after start (indexes provisioning
            // created have no statistics yet), then every `optimize_every`.
            let mut last_optimize = std::time::Instant::now()
                .checked_sub(optimize_every.saturating_sub(Duration::from_secs(60)))
                .unwrap_or_else(std::time::Instant::now);
            loop {
                tokio::time::sleep(interval).await;
                let Some(map) = databases.upgrade() else {
                    return;
                };
                let open: Vec<Database> = map
                    .lock()
                    .unwrap()
                    .values()
                    .filter_map(|cell| cell.get().cloned())
                    .collect();
                drop(map);
                sweep_readers(&open, &limits).await;
                if last_optimize.elapsed() >= optimize_every {
                    last_optimize = std::time::Instant::now();
                    for database in &open {
                        // Cheap when nothing changed (it only compares row
                        // counts with the recorded statistics).
                        for statement in OPTIMIZE_PERIODIC.split_inclusive(';') {
                            let _ = sqlx::query(statement.trim())
                                .execute(&database.writer)
                                .await;
                        }
                    }
                }
            }
        });
    }

    fn open_databases(&self) -> Vec<Database> {
        self.databases
            .lock()
            .unwrap()
            .values()
            .filter_map(|cell| cell.get().cloned())
            .collect()
    }

    /// Waits until everything committed on `database_url` so far is on disk
    /// (no-op with `Durability::Normal`).
    pub async fn durable(&self, database_url: &str) -> Result<(), DatabaseError> {
        if self.durability == Durability::Normal {
            return Ok(());
        }
        self.database(database_url).await?.sync.wait().await
    }

    /// WAL sync attempts for this database, isolated from other tenants and
    /// registries. Use this when measuring a tenant's group-commit behavior;
    /// the process-wide WAL_SYNCS counter includes unrelated concurrent work.
    pub async fn wal_sync_count(&self, database_url: &str) -> Result<u64, DatabaseError> {
        Ok(self.database(database_url).await?.sync.sync_count())
    }

    /// The writer pool (one connection). Every write must use it.
    pub async fn pool(&self, database_url: &str) -> Result<SqlitePool, DatabaseError> {
        Ok(self.database(database_url).await?.writer)
    }

    /// Prepared-statement counters of the writer's direct sessions.
    pub async fn writer_statement_stats(
        &self,
        database_url: &str,
    ) -> Result<crate::direct::StatementStats, DatabaseError> {
        Ok(self.database(database_url).await?.statements.stats())
    }

    /// Opens a read transaction on `database`, preferring an open reader
    /// released within [`PoolLimits::reader_patience`] over opening another,
    /// and opening readers one at a time per database (at most
    /// [`PoolLimits::reader_opens`] across databases).
    pub(crate) async fn begin_read(
        &self,
        database: &Database,
    ) -> Result<sqlx::Transaction<'static, sqlx::Sqlite>, DatabaseError> {
        let reader = &database.reader;
        let patience = self.limits.reader_patience;
        let started = std::time::Instant::now();
        let mut patient_until = started + patience;
        let give_up = started + self.limits.acquire_timeout;
        loop {
            // `try_acquire` never opens a connection.
            if let Some(connection) = reader.try_acquire() {
                return Ok(sqlx::Transaction::begin(connection, None).await?);
            }
            // `size` counts readers being opened too.
            let size = reader.size();
            if size >= self.limits.max_connections_per_database {
                // Every reader exists and is busy: sqlx queues for a release.
                break;
            }
            let now = std::time::Instant::now();
            // A short read usually releases a reader within the patience,
            // far sooner than another one opens.
            let wait_for_release = !patience.is_zero() && size > 0 && now < patient_until;
            if !wait_for_release {
                let Some(opens) = &self.reader_opens else {
                    break;
                };
                if let Ok(_local) = database.reader_open.clone().try_acquire_owned() {
                    let _global = opens.clone().acquire_owned().await;
                    // Another reader may have been released while this open
                    // waited for a process-wide slot.
                    let connection = match reader.try_acquire() {
                        Some(connection) => connection,
                        None => reader.acquire().await?,
                    };
                    return Ok(sqlx::Transaction::begin(connection, None).await?);
                }
                // This database is opening a reader already: wait for it (or
                // for a release) rather than parsing the schema in parallel.
                // The patience restarts once it is open: its first read is
                // usually done long before another open would be.
                if now >= give_up {
                    break;
                }
                patient_until = now + patience;
            }
            // A released connection becomes idle after sqlx's release
            // ping, a worker round trip; poll rather than chase it.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(reader.begin().await?)
    }

    /// The read-only pool. Each transaction sees one WAL snapshot.
    pub async fn reader(&self, database_url: &str) -> Result<SqlitePool, DatabaseError> {
        Ok(self.database(database_url).await?.reader)
    }

    /// The writer pool with its queue statistics (one registry lookup).
    pub async fn writer_with_stats(
        &self,
        database_url: &str,
    ) -> Result<(SqlitePool, Arc<WriterStats>), DatabaseError> {
        let database = self.database(database_url).await?;
        Ok((database.writer, database.writer_stats))
    }

    /// Writer queues of open databases, longest queue (then most total wait)
    /// first, at most `limit`. Read path only: takes the registry lock.
    pub fn writer_queues(&self, limit: usize) -> Vec<WriterQueue> {
        let databases: Vec<(String, Arc<WriterStats>)> = self
            .databases
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(key, cell)| {
                cell.get()
                    .map(|database| (key.clone(), database.writer_stats.clone()))
            })
            .collect();
        let mut queues: Vec<WriterQueue> = databases
            .into_iter()
            .map(|(database, stats)| WriterQueue {
                database,
                waiting: stats.waiting.load(Ordering::Relaxed),
                acquisitions: stats.acquisitions.load(Ordering::Relaxed),
                wait_ns: stats.wait_ns.load(Ordering::Relaxed),
                wait_max_ns: stats.wait_max_ns.load(Ordering::Relaxed),
            })
            .collect();
        queues.sort_by(|a, b| b.waiting.cmp(&a.waiting).then(b.wait_ns.cmp(&a.wait_ns)));
        queues.truncate(limit);
        queues
    }

    pub async fn admit(&self, database_url: &str) -> Result<AdmissionPermit, DatabaseError> {
        let _ = database_url;
        let started = std::time::Instant::now();
        let waiting = gonvex_profiler::gauge(gonvex_profiler::GaugeId::AdmissionWaiting).guard();
        let result = tokio::time::timeout(
            self.limits.acquire_timeout,
            self.admission.clone().acquire_owned(),
        )
        .await
        .map_err(|_| DatabaseError::AdmissionTimeout)
        .and_then(|result| result.map_err(|_| DatabaseError::AdmissionTimeout));
        drop(waiting);
        let waited = started.elapsed();
        gonvex_profiler::record_duration(
            gonvex_profiler::Stage::DbAdmission,
            gonvex_profiler::Label::None,
            waited,
        );
        let elapsed = u64::try_from(waited.as_micros()).unwrap_or(u64::MAX);
        self.admission_count.fetch_add(1, Ordering::Relaxed);
        self.admission_wait_us.fetch_add(elapsed, Ordering::Relaxed);
        self.admission_wait_max_us
            .fetch_max(elapsed, Ordering::Relaxed);
        result.map(|shared| AdmissionPermit { _shared: shared })
    }

    pub async fn listener(&self, database_url: &str) -> Result<DatabaseListener, DatabaseError> {
        let database = self.database(database_url).await?;
        Ok(DatabaseListener {
            receiver: database.notify.subscribe(),
            channels: HashSet::new(),
        })
    }

    /// Publishes to every listener of `database_url` (replaces `pg_notify`).
    pub async fn notify(
        &self,
        database_url: &str,
        channel: &str,
        payload: &str,
    ) -> Result<(), DatabaseError> {
        let database = self.database(database_url).await?;
        let _ = database.notify.send(Notification {
            channel: channel.into(),
            payload: payload.into(),
        });
        Ok(())
    }

    /// Holds an in-process lock on `key` within `database_url` (replaces
    /// session and transaction advisory locks: one process owns the file).
    pub async fn lock(&self, database_url: &str, key: &str) -> Result<KeyedLock, DatabaseError> {
        let database = self.database(database_url).await?;
        let mutex = database
            .locks
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default()
            .clone();
        Ok(KeyedLock {
            _guard: mutex.lock_owned().await,
        })
    }

    pub async fn snapshot(&self) -> PoolRegistrySnapshot {
        let databases = self.open_databases();
        let connections = connection_counts(databases.iter().flat_map(|database| {
            [
                (database.writer.size(), database.writer.num_idle()),
                (database.reader.size(), database.reader.num_idle()),
            ]
        }));
        let open_connections = connections.open;
        let idle = connections.idle;
        PoolRegistrySnapshot {
            listener_connections: 0,
            pools: databases.len(),
            open_connections,
            in_use: open_connections.saturating_sub(u32::try_from(idle).unwrap_or(u32::MAX)),
            idle,
            idle_samples_unavailable: connections.unavailable_idle_samples,
            max_open_connections: u32::try_from(databases.len())
                .unwrap_or(u32::MAX)
                .saturating_mul(self.limits.max_connections_per_database + 1),
            admission_limit: self.limits.max_total_connections,
            admission_active: self
                .limits
                .max_total_connections
                .saturating_sub(self.admission.available_permits()),
            admission_count: self.admission_count.load(Ordering::Relaxed),
            admission_wait_us: self.admission_wait_us.load(Ordering::Relaxed),
            admission_wait_max_us: self.admission_wait_max_us.load(Ordering::Relaxed),
        }
    }

    pub async fn close(&self) {
        let cells = std::mem::take(&mut *self.databases.lock().unwrap());
        for database in cells.into_values().filter_map(|cell| cell.get().cloned()) {
            database.reader.close().await;
            database.statements.close();
            database.writer.close().await;
            if let Some(wal) = &database.wal {
                wal.close().await;
            }
        }
    }

    pub async fn close_database(&self, database_url: &str) {
        let cell = self.databases.lock().unwrap().remove(&key(database_url));
        if let Some(database) = cell.and_then(|cell| cell.get().cloned()) {
            database.reader.close().await;
            database.statements.close();
            database.writer.close().await;
            if let Some(wal) = &database.wal {
                wal.close().await;
            }
        }
    }
}

#[cfg(test)]
mod idle_snapshot_regression {
    use super::*;

    #[test]
    fn invalid_sqlx_idle_sample_is_unavailable_not_a_connection_count() {
        let counts = connection_counts([(1, usize::MAX), (2, 1)]);
        assert_eq!(counts.open, 3);
        assert_eq!(counts.idle, 1);
        assert_eq!(counts.unavailable_idle_samples, 1);
        let inconsistent = connection_counts([(1, 2), (0, 0)]);
        assert_eq!(inconsistent.idle, 0);
        assert_eq!(inconsistent.unavailable_idle_samples, 1);
    }

    #[test]
    fn valid_idle_samples_and_recovery_keep_their_exact_counts() {
        let counts = connection_counts([(1, 0), (2, 2)]);
        assert_eq!(counts.open, 3);
        assert_eq!(counts.idle, 2);
        assert_eq!(counts.unavailable_idle_samples, 0);
        let empty = connection_counts([]);
        assert_eq!(
            (empty.open, empty.idle, empty.unavailable_idle_samples),
            (0, 0, 0)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_survives_sqlx_connection_handoffs() {
        let directory =
            std::env::temp_dir().join(format!("gonvex-idle-handoff-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let url = format!("sqlite://{}", directory.join("handoff.db").display());
        let registry = Arc::new(PoolRegistry::new(PoolLimits {
            max_connections_per_database: 1,
            ..PoolLimits::default()
        }));
        let database = registry.database(&url).await.unwrap();
        let held_reader = database.reader.acquire().await.unwrap();
        let other_url = format!("sqlite://{}", directory.join("idle.db").display());
        let other = registry.database(&other_url).await.unwrap();
        drop(other.writer.acquire().await.unwrap());
        tokio::time::timeout(Duration::from_secs(5), async {
            while other.writer.num_idle() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let start = Arc::new(std::sync::Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let pool = database.writer.clone();
            let start = start.clone();
            workers.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                start.wait();
                runtime.block_on(async move {
                    for _ in 0..20_000 {
                        drop(pool.acquire().await.unwrap());
                        tokio::task::yield_now().await;
                    }
                });
            }));
        }
        start.wait();
        for _ in 0..200_000 {
            let snapshot = registry.snapshot().await;
            assert!(snapshot.idle <= snapshot.open_connections as usize);
            tokio::task::yield_now().await;
        }
        tokio::task::spawn_blocking(move || {
            for worker in workers {
                worker.join().unwrap();
            }
        })
        .await
        .unwrap();
        drop(held_reader);
        registry.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }
}
