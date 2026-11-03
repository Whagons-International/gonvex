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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolLimits {
    /// Concurrent SQL jobs across every database (CPU fairness, not sockets).
    pub max_total_connections: usize,
    /// Read-only connections per database file. Writes always use one writer.
    pub max_connections_per_database: u32,
    pub max_idle_connections_per_database: u32,
    pub idle_timeout: Duration,
    pub acquire_timeout: Duration,
}

impl Default for PoolLimits {
    fn default() -> Self {
        Self {
            max_total_connections: DEFAULT_MAX_TOTAL_CONNECTIONS,
            max_connections_per_database: DEFAULT_READERS_PER_DATABASE,
            max_idle_connections_per_database: DEFAULT_READERS_PER_DATABASE,
            idle_timeout: Duration::from_secs(600),
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
    pub max_open_connections: u32,
    pub listener_connections: u32,
    pub admission_limit: usize,
    pub admission_active: usize,
    /// Completed admission attempts, including uncontended successes and timeouts.
    pub admission_count: u64,
    pub admission_wait_us: u64,
    pub admission_wait_max_us: u64,
}

/// Per-database single-writer queue statistics, updated with relaxed atomics
/// on the transaction path (no registry lock).
#[derive(Default)]
pub struct WriterStats {
    waiting: std::sync::atomic::AtomicU32,
    acquisitions: AtomicU64,
    wait_ns: AtomicU64,
    wait_max_ns: AtomicU64,
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
    pub(crate) writer: SqlitePool,
    pub(crate) writer_stats: Arc<WriterStats>,
    pub(crate) reader: SqlitePool,
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
    admission_count: Arc<AtomicU64>,
    admission_wait_us: Arc<AtomicU64>,
    admission_wait_max_us: Arc<AtomicU64>,
    // Per-file initializers. The map lock is held only to find or insert an
    // initializer, never across file I/O, so a slow or suspended open of one
    // file cannot block lookups of every other database.
    databases: Arc<std::sync::Mutex<BTreeMap<String, Arc<tokio::sync::OnceCell<Database>>>>>,
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
        Self {
            admission: Arc::new(Semaphore::new(limits.max_total_connections)),
            admission_count: Arc::default(),
            admission_wait_us: Arc::default(),
            admission_wait_max_us: Arc::default(),
            databases: Arc::default(),
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
            options = options.journal_mode(SqliteJournalMode::Wal);
        }
        let options = options
            // COMMIT never syncs while holding the single writer; Full
            // durability is provided by waiting on the database's batched
            // WAL sync before a commit is acknowledged (`durable`).
            .synchronous(SqliteSynchronous::Normal)
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
        cell.get_or_try_init(|| self.open(&key)).await.cloned()
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
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .acquire_timeout(self.limits.acquire_timeout)
            // Change capture reads row values through the pre-update hook,
            // installed on the writer only (readers never write).
            .after_connect(move |connection, _| {
                let wal = writer_wal.clone();
                Box::pin(async move {
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
            .connect_with(self.options(&path, false)?)
            .await?;
        let reader = SqlitePoolOptions::new()
            .max_connections(self.limits.max_connections_per_database)
            .min_connections(0)
            .idle_timeout(Some(self.limits.idle_timeout))
            .max_lifetime(None)
            .acquire_timeout(self.limits.acquire_timeout)
            .connect_lazy_with(self.options(&path, true)?);
        let (notify, _) = broadcast::channel(NOTIFY_CAPACITY);
        let database = Database {
            writer,
            writer_stats: Arc::default(),
            reader,
            notify,
            locks: Arc::default(),
            sync: crate::sync_barrier::SyncBarrier::new(&path),
            wal,
        };
        Ok(database)
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

    /// The writer pool (one connection). Every write must use it.
    pub async fn pool(&self, database_url: &str) -> Result<SqlitePool, DatabaseError> {
        Ok(self.database(database_url).await?.writer)
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
        let open_connections = databases
            .iter()
            .map(|database| database.writer.size() + database.reader.size())
            .sum::<u32>();
        let idle = databases
            .iter()
            .map(|database| database.writer.num_idle() + database.reader.num_idle())
            .sum::<usize>();
        PoolRegistrySnapshot {
            listener_connections: 0,
            pools: databases.len(),
            open_connections,
            in_use: open_connections.saturating_sub(u32::try_from(idle).unwrap_or(u32::MAX)),
            idle,
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
            database.writer.close().await;
            if let Some(wal) = &database.wal {
                wal.close().await;
            }
        }
    }
}
