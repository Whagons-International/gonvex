//! SQLite database ownership: one writer connection and a read-only pool per
//! database file, an in-process notification channel replacing LISTEN/NOTIFY,
//! and keyed async locks replacing Postgres advisory locks.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use tokio::sync::{broadcast, Mutex, OwnedMutexGuard, OwnedSemaphorePermit, RwLock, Semaphore};

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
}

#[derive(Clone)]
pub(crate) struct Database {
    pub(crate) writer: SqlitePool,
    pub(crate) reader: SqlitePool,
    notify: broadcast::Sender<Notification>,
    locks: Arc<std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

#[derive(Clone)]
pub struct PoolRegistry {
    limits: PoolLimits,
    durability: Durability,
    admission: Arc<Semaphore>,
    databases: Arc<RwLock<BTreeMap<String, Database>>>,
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
        Self {
            admission: Arc::new(Semaphore::new(limits.max_total_connections)),
            databases: Arc::new(RwLock::new(BTreeMap::new())),
            durability,
            limits,
        }
    }

    fn options(&self, path: &Path, read_only: bool) -> Result<SqliteConnectOptions, DatabaseError> {
        let url = format!("sqlite://{}", path.display());
        let options = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(!read_only)
            .read_only(read_only)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(match self.durability {
                Durability::Full => SqliteSynchronous::Full,
                Durability::Normal => SqliteSynchronous::Normal,
            })
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5))
            .pragma("cache_size", "-65536")
            .pragma("temp_store", "MEMORY")
            // Module SQL may use `col REGEXP $n` (Rust `regex` syntax).
            .with_regexp()
            .statement_cache_capacity(512);
        Ok(options)
    }

    pub(crate) async fn database(&self, database_url: &str) -> Result<Database, DatabaseError> {
        let key = key(database_url);
        if let Some(database) = self.databases.read().await.get(&key).cloned() {
            return Ok(database);
        }
        let mut databases = self.databases.write().await;
        if let Some(database) = databases.get(&key).cloned() {
            return Ok(database);
        }
        let path = PathBuf::from(&key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| DatabaseError::Io(error.to_string()))?;
        }
        // The writer creates the file and switches it to WAL before any reader
        // opens it; a read-only connection cannot change the journal mode.
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .acquire_timeout(self.limits.acquire_timeout)
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
            reader,
            notify,
            locks: Arc::default(),
        };
        databases.insert(key, database.clone());
        Ok(database)
    }

    /// The writer pool (one connection). Every write must use it.
    pub async fn pool(&self, database_url: &str) -> Result<SqlitePool, DatabaseError> {
        Ok(self.database(database_url).await?.writer)
    }

    /// The read-only pool. Each transaction sees one WAL snapshot.
    pub async fn reader(&self, database_url: &str) -> Result<SqlitePool, DatabaseError> {
        Ok(self.database(database_url).await?.reader)
    }

    pub async fn admit(&self, database_url: &str) -> Result<AdmissionPermit, DatabaseError> {
        let _ = database_url;
        tokio::time::timeout(
            self.limits.acquire_timeout,
            self.admission.clone().acquire_owned(),
        )
        .await
        .map_err(|_| DatabaseError::AdmissionTimeout)?
        .map(|shared| AdmissionPermit { _shared: shared })
        .map_err(|_| DatabaseError::AdmissionTimeout)
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
        let databases = self.databases.read().await;
        let open_connections = databases
            .values()
            .map(|database| database.writer.size() + database.reader.size())
            .sum::<u32>();
        let idle = databases
            .values()
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
        }
    }

    pub async fn close(&self) {
        let databases: Vec<Database> = self
            .databases
            .write()
            .await
            .split_off("")
            .into_values()
            .collect();
        for database in databases {
            database.reader.close().await;
            database.writer.close().await;
        }
    }

    pub async fn close_database(&self, database_url: &str) {
        let database = self.databases.write().await.remove(&key(database_url));
        if let Some(database) = database {
            database.reader.close().await;
            database.writer.close().await;
        }
    }
}
