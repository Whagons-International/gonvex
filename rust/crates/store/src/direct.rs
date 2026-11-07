//! Direct SQLite access on a transaction's own connection.
//!
//! sqlx runs every SQLite connection on a worker thread and hands each
//! statement to it through a channel: two thread switches per statement plus
//! sqlx's per-statement argument, row and cache machinery (about 110 µs per
//! module read on one CPU, workstream 8b). A [`DirectSession`] instead takes
//! the transaction's connection out of the worker for as long as the
//! transaction uses it (`LockedSqliteHandle`; the worker thread parks on the
//! connection lock) and runs statements on the calling thread through the C
//! API, with prepared statements kept across transactions.
//!
//! Ownership stays with sqlx: the session is a lease on the connection of one
//! `TenantTransaction`, released before any sqlx use of that transaction and
//! before it commits, rolls back or drops. Statements never cross into V8; the
//! host's capability-checked handlers are the only users.
//!
//! Statement cache. The writer connection of a database is long-lived (pool of
//! one, no idle timeout), so its prepared statements live in
//! [`WriterStatements`] across transactions, LRU-bounded
//! (`GONVEX_SQLITE_DIRECT_STATEMENTS`, default 256). They are finalized before
//! the connection can close (sqlx panics on `sqlite3_close` with unfinalized
//! statements): on `PoolRegistry::close`/`close_database`, and when the last
//! owner drops, which is ordered before the pool's connection. Writer
//! connections are opened in SQLite's serialized mode so that this finalize is
//! safe even while sqlx runs an unrelated statement on the same connection.
//! A schema change (`PRAGMA schema_version`, checked when a session starts,
//! and after DDL through a session) clears the cache, so column metadata read
//! from cached statements is never stale. Reader connections close when idle,
//! so their sessions finalize their statements when they end.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::{c_int, CStr};
use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, MutexGuard};

use libsqlite3_sys as ffi;
use serde_json::Value;

use crate::rows::JsonKind;

/// A SQLite error, with the same text and code sqlx reports, so callers and
/// error classification cannot tell which path ran the statement.
#[derive(Clone, Debug)]
pub struct SqliteError {
    code: c_int,
    message: String,
}

impl SqliteError {
    unsafe fn from_db(db: *mut ffi::sqlite3) -> Self {
        let code = ffi::sqlite3_extended_errcode(db);
        let message = CStr::from_ptr(ffi::sqlite3_errmsg(db))
            .to_string_lossy()
            .into_owned();
        Self { code, message }
    }

    fn from_code(code: c_int) -> Self {
        let message = unsafe { CStr::from_ptr(ffi::sqlite3_errstr(code)) }
            .to_string_lossy()
            .into_owned();
        Self { code, message }
    }

    /// Extended result code.
    pub fn code(&self) -> c_int {
        self.code
    }
}

impl fmt::Display for SqliteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(code: {}) {}", self.code, self.message)
    }
}

impl std::error::Error for SqliteError {}

impl sqlx::error::DatabaseError for SqliteError {
    fn message(&self) -> &str {
        &self.message
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Owned(self.code.to_string()))
    }

    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> sqlx::error::ErrorKind {
        use sqlx::error::ErrorKind;
        match self.code {
            ffi::SQLITE_CONSTRAINT_UNIQUE | ffi::SQLITE_CONSTRAINT_PRIMARYKEY => {
                ErrorKind::UniqueViolation
            }
            ffi::SQLITE_CONSTRAINT_FOREIGNKEY => ErrorKind::ForeignKeyViolation,
            ffi::SQLITE_CONSTRAINT_NOTNULL => ErrorKind::NotNullViolation,
            ffi::SQLITE_CONSTRAINT_CHECK => ErrorKind::CheckViolation,
            _ => ErrorKind::Other,
        }
    }
}

fn database_error(error: SqliteError) -> sqlx::Error {
    sqlx::Error::Database(Box::new(error))
}

unsafe fn last_error(db: *mut ffi::sqlite3) -> sqlx::Error {
    database_error(SqliteError::from_db(db))
}

/// A bound parameter value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Bind<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
}

/// An owned parameter value, for statements run on another thread.
#[derive(Clone, Debug, PartialEq)]
pub enum OwnedBind {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl OwnedBind {
    pub fn as_bind(&self) -> Bind<'_> {
        match self {
            Self::Null => Bind::Null,
            Self::Integer(value) => Bind::Integer(*value),
            Self::Real(value) => Bind::Real(*value),
            Self::Text(value) => Bind::Text(value),
        }
    }
}

/// Prepared statements per writer connection
/// (`GONVEX_SQLITE_DIRECT_STATEMENTS`, default 256; 0 disables reuse).
fn writer_statement_capacity() -> usize {
    std::env::var("GONVEX_SQLITE_DIRECT_STATEMENTS")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(256)
}

struct Cached {
    statement: NonNull<ffi::sqlite3_stmt>,
    used: u64,
}

/// Prepared statements of one connection, keyed by SQL text.
pub(crate) struct StatementCache {
    db: usize,
    entries: HashMap<Box<str>, Cached>,
    capacity: usize,
    tick: u64,
    schema_version: Option<i64>,
    prepared: u64,
    reused: u64,
}

// SAFETY: statements are only used while the owning session holds the
// connection; the cache itself is plain data guarded by a mutex.
unsafe impl Send for StatementCache {}

impl StatementCache {
    fn new(capacity: usize) -> Self {
        Self {
            db: 0,
            entries: HashMap::new(),
            capacity,
            tick: 0,
            schema_version: None,
            prepared: 0,
            reused: 0,
        }
    }

    /// Binds the cache to `db`. Statements prepared on another connection
    /// are forgotten without finalizing: that connection is gone (closing it
    /// with them alive would have failed first), so finalizing would touch
    /// freed memory.
    fn attach(&mut self, db: *mut ffi::sqlite3) {
        let db = db as usize;
        if self.db != db {
            if self.db != 0 {
                self.entries.clear();
            }
            self.db = db;
            self.schema_version = None;
        }
    }

    fn clear(&mut self) {
        for (_, cached) in self.entries.drain() {
            unsafe {
                ffi::sqlite3_finalize(cached.statement.as_ptr());
            }
        }
    }

    fn take(&mut self, sql: &str) -> Option<NonNull<ffi::sqlite3_stmt>> {
        self.entries.remove(sql).map(|cached| {
            self.reused += 1;
            cached.statement
        })
    }

    fn put(&mut self, sql: &str, statement: NonNull<ffi::sqlite3_stmt>) {
        if self.capacity == 0 {
            unsafe {
                ffi::sqlite3_finalize(statement.as_ptr());
            }
            return;
        }
        if self.entries.len() >= self.capacity {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.used)
                .map(|(sql, _)| sql.clone())
            {
                if let Some(cached) = self.entries.remove(&oldest) {
                    unsafe {
                        ffi::sqlite3_finalize(cached.statement.as_ptr());
                    }
                }
            }
        }
        self.tick += 1;
        let used = self.tick;
        if let Some(previous) = self.entries.insert(sql.into(), Cached { statement, used }) {
            unsafe {
                ffi::sqlite3_finalize(previous.statement.as_ptr());
            }
        }
    }
}

impl Drop for StatementCache {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Statement cache counters, for tests and benchmarks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatementStats {
    pub cached: usize,
    pub prepared: u64,
    pub reused: u64,
}

/// The long-lived statement cache of one database's writer connection.
pub(crate) struct WriterStatements {
    cache: Mutex<StatementCache>,
}

impl WriterStatements {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            cache: Mutex::new(StatementCache::new(writer_statement_capacity())),
        })
    }

    fn lock(&self) -> MutexGuard<'_, StatementCache> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Called with exclusive ownership of the pooled writer, before schema
    /// work retires it. Finalize raw statements while their handle is alive;
    /// the replacement connection keeps the configured cache capacity.
    pub(crate) fn retire(&self) {
        let mut cache = self.lock();
        cache.clear();
        cache.db = 0;
        cache.schema_version = None;
    }

    /// Finalizes every statement and stops caching: the connection closes next.
    pub(crate) fn close(&self) {
        let mut cache = self.lock();
        cache.clear();
        cache.capacity = 0;
    }

    pub(crate) fn stats(&self) -> StatementStats {
        let cache = self.lock();
        StatementStats {
            cached: cache.entries.len(),
            prepared: cache.prepared,
            reused: cache.reused,
        }
    }
}

struct RawDb(NonNull<ffi::sqlite3>);

// SAFETY: the connection is used by one thread at a time, under the session
// mutex, while sqlx's worker is parked on the connection lock.
unsafe impl Send for RawDb {}

enum Statements {
    Writer(Arc<WriterStatements>),
    Local(StatementCache),
}

struct SessionInner {
    db: Option<RawDb>,
    statements: Option<Statements>,
}

/// A lease on one transaction's connection for direct statements. Cheap to
/// clone and `Send`: the host hands a clone to the thread that runs module
/// code. After the transaction releases it (before any sqlx use, commit,
/// rollback or drop) [`DirectSession::run`] returns `None`.
#[derive(Clone)]
pub struct DirectSession {
    inner: Arc<Mutex<SessionInner>>,
    /// The owning transaction's failure mark (`TenantTransaction::mark_failed`).
    failure: Arc<Mutex<Option<String>>>,
}

impl DirectSession {
    pub(crate) fn new(
        db: NonNull<ffi::sqlite3>,
        writer: Option<Arc<WriterStatements>>,
        failure: Arc<Mutex<Option<String>>>,
    ) -> Self {
        let statements = match writer {
            Some(writer) => Statements::Writer(writer),
            None => Statements::Local(StatementCache::new(64)),
        };
        Self {
            inner: Arc::new(Mutex::new(SessionInner {
                db: Some(RawDb(db)),
                statements: Some(statements),
            })),
            failure,
        }
    }

    /// Marks the owning transaction failed: it will refuse to commit, as
    /// `TenantTransaction::mark_failed` does.
    pub fn mark_failed(&self, reason: impl Into<String>) {
        let mut failure = self
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failure.is_none() {
            *failure = Some(reason.into());
        }
    }

    /// Clears the mark after a `ROLLBACK TO` (`TenantTransaction::clear_failure`).
    pub fn clear_failure(&self) {
        *self
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    fn lock(&self) -> MutexGuard<'_, SessionInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether the transaction still lends its connection to this session.
    pub fn active(&self) -> bool {
        self.lock().db.is_some()
    }

    /// Runs `work` with the connection, on the calling thread. `None` once the
    /// session was released.
    pub fn run<R>(&self, work: impl FnOnce(&mut Conn<'_>) -> R) -> Option<R> {
        let mut inner = self.lock();
        let SessionInner { db, statements } = &mut *inner;
        let db = db.as_ref()?.0.as_ptr();
        match statements.as_mut()? {
            Statements::Writer(writer) => {
                let mut cache = writer.lock();
                cache.attach(db);
                Some(work(&mut Conn {
                    db,
                    cache: &mut *cache,
                }))
            }
            Statements::Local(cache) => {
                cache.attach(db);
                Some(work(&mut Conn { db, cache }))
            }
        }
    }

    /// Ends the lease. Waits for a statement in progress on another thread.
    /// Statements of a reader session are finalized here, while the
    /// connection is still locked out of sqlx.
    pub(crate) fn release(&self) {
        let mut inner = self.lock();
        let statements = inner.statements.take();
        drop(statements);
        inner.db = None;
    }

    pub fn statement_stats(&self) -> Option<StatementStats> {
        let inner = self.lock();
        match inner.statements.as_ref()? {
            Statements::Writer(writer) => Some(writer.stats()),
            Statements::Local(cache) => Some(StatementStats {
                cached: cache.entries.len(),
                prepared: cache.prepared,
                reused: cache.reused,
            }),
        }
    }
}

/// The connection inside [`DirectSession::run`].
pub struct Conn<'a> {
    db: *mut ffi::sqlite3,
    cache: &'a mut StatementCache,
}

impl Conn<'_> {
    /// A cached prepared statement for `sql` (one statement; trailing text
    /// after it is ignored). It is reset and returned to the cache on drop.
    pub fn prepare(&mut self, sql: &str) -> Result<Stmt<'_>, sqlx::Error> {
        let statement = match self.cache.take(sql) {
            Some(statement) => statement,
            None => {
                self.cache.prepared += 1;
                self.prepare_raw(sql, ffi::SQLITE_PREPARE_PERSISTENT as u32)?
            }
        };
        Ok(Stmt {
            statement,
            db: self.db,
            sql: Some(sql.into()),
            cache: Some(self.cache),
            _conn: PhantomData,
        })
    }

    /// A one-off statement, finalized on drop (large ad-hoc SQL).
    pub fn prepare_once(&mut self, sql: &str) -> Result<Stmt<'_>, sqlx::Error> {
        let statement = self.prepare_raw(sql, 0)?;
        Ok(Stmt {
            statement,
            db: self.db,
            sql: None,
            cache: None,
            _conn: PhantomData,
        })
    }

    fn prepare_raw(
        &mut self,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<ffi::sqlite3_stmt>, sqlx::Error> {
        let length = c_int::try_from(sql.len())
            .map_err(|_| database_error(SqliteError::from_code(ffi::SQLITE_TOOBIG)))?;
        let mut statement = std::ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v3(
                self.db,
                sql.as_ptr().cast(),
                length,
                flags,
                &mut statement,
                std::ptr::null_mut(),
            )
        };
        if rc != ffi::SQLITE_OK {
            if !statement.is_null() {
                unsafe { ffi::sqlite3_finalize(statement) };
            }
            return Err(unsafe { last_error(self.db) });
        }
        NonNull::new(statement)
            .ok_or_else(|| sqlx::Error::Protocol("the statement is empty".to_owned()))
    }

    /// Runs a statement to completion and returns the rows it changed.
    pub fn execute(&mut self, sql: &str, binds: &[Bind<'_>]) -> Result<u64, sqlx::Error> {
        let mut statement = self.prepare(sql)?;
        statement.bind(binds)?;
        while statement.step()? {}
        Ok(statement.changes())
    }

    /// First column of the first row, if any, as an integer.
    pub fn query_i64(&mut self, sql: &str, binds: &[Bind<'_>]) -> Result<Option<i64>, sqlx::Error> {
        let mut statement = self.prepare(sql)?;
        statement.bind(binds)?;
        if statement.step()? && statement.column_type(0) != ffi::SQLITE_NULL {
            Ok(Some(statement.int64(0)))
        } else {
            Ok(None)
        }
    }

    /// Drops every cached statement (after DDL on this connection).
    pub fn schema_changed(&mut self) {
        self.cache.clear();
        self.cache.schema_version = None;
    }

    /// Clears the cache when the schema changed since it was filled.
    pub(crate) fn check_schema(&mut self) -> Result<(), sqlx::Error> {
        let version = self.query_i64("PRAGMA schema_version", &[])?;
        if self.cache.schema_version.is_some() && self.cache.schema_version != version {
            self.cache.clear();
        }
        self.cache.schema_version = version;
        Ok(())
    }

    pub fn raw(&self) -> *mut ffi::sqlite3 {
        self.db
    }
}

/// A prepared statement borrowed from a [`Conn`]. Reset (and returned to the
/// cache, if cached) on drop.
pub struct Stmt<'c> {
    statement: NonNull<ffi::sqlite3_stmt>,
    db: *mut ffi::sqlite3,
    sql: Option<Box<str>>,
    cache: Option<&'c mut StatementCache>,
    _conn: PhantomData<&'c mut ()>,
}

impl Drop for Stmt<'_> {
    fn drop(&mut self) {
        let statement = self.statement.as_ptr();
        match (self.cache.take(), self.sql.take()) {
            (Some(cache), Some(sql)) => unsafe {
                ffi::sqlite3_reset(statement);
                ffi::sqlite3_clear_bindings(statement);
                cache.put(&sql, self.statement);
            },
            _ => unsafe {
                ffi::sqlite3_finalize(statement);
            },
        }
    }
}

/// Leading decimal digits of `text`, as sqlx reads `?NNN` / `$NNN`.
fn leading_number(text: &str) -> Option<usize> {
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    text[..digits].parse().ok()
}

impl Stmt<'_> {
    /// Binds `binds` with sqlx's rules: `$N`/`?N` take the Nth value, a bare
    /// `?` the next one; parameters past the last value stay NULL.
    pub fn bind(&mut self, binds: &[Bind<'_>]) -> Result<(), sqlx::Error> {
        let statement = self.statement.as_ptr();
        let count = unsafe { ffi::sqlite3_bind_parameter_count(statement) };
        let mut sequential = 0usize;
        for parameter in 1..=count {
            let name = unsafe { ffi::sqlite3_bind_parameter_name(statement, parameter) };
            let index = if name.is_null() {
                sequential += 1;
                sequential
            } else {
                let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
                let number = if let Some(rest) = name.strip_prefix('?') {
                    leading_number(rest)
                } else if let Some(rest) = name.strip_prefix('$') {
                    Some(leading_number(rest).ok_or_else(|| {
                        sqlx::Error::Protocol(format!(
                            "parameters with non-integer names are not currently supported: {rest}"
                        ))
                    })?)
                } else {
                    return Err(sqlx::Error::Protocol(format!(
                        "unsupported SQL parameter format: {name}"
                    )));
                };
                match number {
                    Some(number) if number > 0 => number,
                    _ => {
                        return Err(sqlx::Error::Protocol(format!(
                            "invalid SQL parameter: {name}"
                        )))
                    }
                }
            };
            let Some(value) = binds.get(index - 1) else {
                break;
            };
            let rc = unsafe {
                match value {
                    Bind::Null => ffi::sqlite3_bind_null(statement, parameter),
                    Bind::Integer(value) => ffi::sqlite3_bind_int64(statement, parameter, *value),
                    Bind::Real(value) => ffi::sqlite3_bind_double(statement, parameter, *value),
                    Bind::Text(value) => ffi::sqlite3_bind_text64(
                        statement,
                        parameter,
                        value.as_ptr().cast(),
                        value.len() as u64,
                        ffi::SQLITE_TRANSIENT(),
                        ffi::SQLITE_UTF8 as u8,
                    ),
                }
            };
            if rc != ffi::SQLITE_OK {
                return Err(unsafe { last_error(self.db) });
            }
        }
        Ok(())
    }

    /// Advances to the next row: `true` when a row is available.
    pub fn step(&mut self) -> Result<bool, sqlx::Error> {
        match unsafe { ffi::sqlite3_step(self.statement.as_ptr()) } {
            ffi::SQLITE_ROW => Ok(true),
            ffi::SQLITE_DONE => Ok(false),
            _ => Err(unsafe { last_error(self.db) }),
        }
    }

    /// Rows changed by the last completed INSERT/UPDATE/DELETE.
    pub fn changes(&self) -> u64 {
        unsafe { ffi::sqlite3_changes64(self.db) }.max(0) as u64
    }

    pub fn column_count(&self) -> usize {
        unsafe { ffi::sqlite3_column_count(self.statement.as_ptr()) }.max(0) as usize
    }

    pub fn column_name(&self, index: usize) -> &str {
        let name = unsafe { ffi::sqlite3_column_name(self.statement.as_ptr(), index as c_int) };
        if name.is_null() {
            return "";
        }
        unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("")
    }

    /// Declared type of a result column; `None` for an expression.
    pub fn column_decltype(&self, index: usize) -> Option<&str> {
        let declared =
            unsafe { ffi::sqlite3_column_decltype(self.statement.as_ptr(), index as c_int) };
        (!declared.is_null()).then(|| unsafe { CStr::from_ptr(declared) }.to_str().unwrap_or(""))
    }

    /// Storage class of the current row's value (`SQLITE_INTEGER`, ...).
    pub fn column_type(&self, index: usize) -> c_int {
        unsafe { ffi::sqlite3_column_type(self.statement.as_ptr(), index as c_int) }
    }

    /// Whether the current row's value is TEXT.
    pub fn is_text(&self, index: usize) -> bool {
        self.column_type(index) == ffi::SQLITE_TEXT
    }

    pub fn int64(&self, index: usize) -> i64 {
        unsafe { ffi::sqlite3_column_int64(self.statement.as_ptr(), index as c_int) }
    }

    pub fn double(&self, index: usize) -> f64 {
        unsafe { ffi::sqlite3_column_double(self.statement.as_ptr(), index as c_int) }
    }

    /// Text bytes of the current row's value (empty for NULL).
    pub fn text(&self, index: usize) -> &[u8] {
        unsafe {
            let text = ffi::sqlite3_column_text(self.statement.as_ptr(), index as c_int);
            let length = ffi::sqlite3_column_bytes(self.statement.as_ptr(), index as c_int);
            if text.is_null() || length <= 0 {
                &[]
            } else {
                std::slice::from_raw_parts(text, length as usize)
            }
        }
    }

    pub fn blob(&self, index: usize) -> &[u8] {
        unsafe {
            let blob = ffi::sqlite3_column_blob(self.statement.as_ptr(), index as c_int);
            let length = ffi::sqlite3_column_bytes(self.statement.as_ptr(), index as c_int);
            if blob.is_null() || length <= 0 {
                &[]
            } else {
                std::slice::from_raw_parts(blob.cast::<u8>(), length as usize)
            }
        }
    }

    /// Text of the current row's value as `str`.
    pub fn str(&self, index: usize) -> Result<&str, String> {
        std::str::from_utf8(self.text(index))
            .map_err(|error| format!("value decode failed: invalid UTF-8 text: {error}"))
    }

    /// Writes the current row's value as JSON, interpreted through `kind`
    /// exactly as [`crate::rows::decode_value`] decodes it.
    pub fn write_json(
        &self,
        index: usize,
        kind: JsonKind,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        match (kind, self.column_type(index)) {
            (_, ffi::SQLITE_NULL) => out.extend_from_slice(b"null"),
            (JsonKind::Boolean, ffi::SQLITE_INTEGER) => {
                out.extend_from_slice(if self.int64(index) != 0 {
                    b"true"
                } else {
                    b"false"
                })
            }
            (JsonKind::Boolean, ffi::SQLITE_FLOAT) => {
                out.extend_from_slice(if self.double(index) != 0.0 {
                    b"true"
                } else {
                    b"false"
                })
            }
            (JsonKind::Json, ffi::SQLITE_TEXT) => {
                let text = self.str(index)?;
                match serde_json::from_str::<Value>(text) {
                    Ok(value) => write_value(out, &value),
                    Err(_) => write_str(out, text),
                }
            }
            (_, ffi::SQLITE_INTEGER) => {
                let mut buffer = itoa_buffer();
                out.extend_from_slice(format_i64(&mut buffer, self.int64(index)));
            }
            (_, ffi::SQLITE_FLOAT) => write_f64(out, self.double(index)),
            (_, ffi::SQLITE_TEXT) => write_str(out, self.str(index)?),
            (_, ffi::SQLITE_BLOB) => write_hex(out, self.blob(index)),
            _ => write_str(out, self.str(index)?),
        }
        Ok(())
    }
}

fn itoa_buffer() -> [u8; 20] {
    [0; 20]
}

fn format_i64(buffer: &mut [u8; 20], value: i64) -> &[u8] {
    let mut index = buffer.len();
    let negative = value < 0;
    let mut magnitude = value.unsigned_abs();
    loop {
        index -= 1;
        buffer[index] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }
    if negative {
        // i64::MIN has 19 digits, so there is room for the sign.
        index -= 1;
        buffer[index] = b'-';
    }
    &buffer[index..]
}

/// JSON text of a string, escaped as serde_json escapes it.
pub fn write_str(out: &mut Vec<u8>, text: &str) {
    serde_json::to_writer(&mut *out, text).expect("writing to a Vec cannot fail");
}

pub fn write_value(out: &mut Vec<u8>, value: &Value) {
    serde_json::to_writer(&mut *out, value).expect("writing to a Vec cannot fail");
}

/// A float as `serde_json::Number` writes it; non-finite values are `null`.
pub fn write_f64(out: &mut Vec<u8>, value: f64) {
    if value.is_finite() {
        serde_json::to_writer(&mut *out, &value).expect("writing to a Vec cannot fail");
    } else {
        out.extend_from_slice(b"null");
    }
}

/// A BLOB as a lowercase hex JSON string (`rows::decode_value`).
pub fn write_hex(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2 + 2);
    out.push(b'"');
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 15) as usize]);
    }
    out.push(b'"');
}

pub use crate::json_rows::JsonRowBatch;

const BATCH_ROWS: usize = 128;
const BATCH_BYTES: usize = 256 * 1024;

/// Streams the first column of every row of `sql` as JSON text batches.
/// Runs on the calling (blocking) thread; stops when `send` refuses a batch.
pub(crate) fn stream_json_rows(
    conn: &mut Conn<'_>,
    sql: &str,
    binds: &[OwnedBind],
    stop: &std::sync::atomic::AtomicBool,
    mut send: impl FnMut(JsonRowBatch) -> bool,
) -> Result<(), sqlx::Error> {
    unsafe extern "C" fn stopped(flag: *mut std::ffi::c_void) -> c_int {
        let flag = &*(flag as *const std::sync::atomic::AtomicBool);
        c_int::from(flag.load(std::sync::atomic::Ordering::Relaxed))
    }
    /// Removes the progress handler however the query ends.
    struct Interruptible(*mut ffi::sqlite3);
    impl Drop for Interruptible {
        fn drop(&mut self) {
            unsafe { ffi::sqlite3_progress_handler(self.0, 0, None, std::ptr::null_mut()) };
        }
    }
    // A dropped stream interrupts the statement within ~1,000 VM steps, so
    // the transaction's release never waits behind a long scan.
    let _interruptible = Interruptible(conn.db);
    unsafe {
        ffi::sqlite3_progress_handler(
            conn.db,
            1000,
            Some(stopped),
            stop as *const std::sync::atomic::AtomicBool as *mut std::ffi::c_void,
        );
    }
    let binds = binds.iter().map(OwnedBind::as_bind).collect::<Vec<_>>();
    let mut statement = conn.prepare_once(sql)?;
    statement.bind(&binds)?;
    let mut batch = JsonRowBatch::with_capacity(BATCH_ROWS, 64 * 1024);
    while statement.step()? {
        if statement.column_type(0) == ffi::SQLITE_NULL {
            continue;
        }
        batch.push(statement.text(0));
        if batch.len() >= BATCH_ROWS || batch.bytes() >= BATCH_BYTES {
            let full = std::mem::replace(
                &mut batch,
                JsonRowBatch::with_capacity(BATCH_ROWS, 64 * 1024),
            );
            if !send(full) {
                return Ok(());
            }
        }
    }
    if !batch.is_empty() {
        send(batch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_format_like_serde() {
        for value in [0i64, 7, -7, 1234567890, i64::MAX, i64::MIN] {
            let mut buffer = itoa_buffer();
            assert_eq!(
                std::str::from_utf8(format_i64(&mut buffer, value)).unwrap(),
                value.to_string()
            );
        }
    }

    #[test]
    fn sqlite_errors_read_like_sqlx() {
        let error = database_error(SqliteError {
            code: ffi::SQLITE_CONSTRAINT_UNIQUE,
            message: "UNIQUE constraint failed: t.id".into(),
        });
        assert_eq!(
            error.to_string(),
            "error returned from database: (code: 2067) UNIQUE constraint failed: t.id"
        );
        assert!(error.as_database_error().unwrap().is_unique_violation());
        assert_eq!(error.as_database_error().unwrap().code().unwrap(), "2067");
    }
}
