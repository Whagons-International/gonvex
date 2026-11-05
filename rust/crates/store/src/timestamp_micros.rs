//! Explicit precision-preserving timestamp storage. No untyped unit inference.
//! All registered clocks are native; the statement trace captures BEGIN before
//! execution and retains its wall-clock value until the transaction ends. It
//! also gives autocommit statements one stable `now`, including trigger bodies.
use chrono::{DateTime, SecondsFormat, Utc};
use libsqlite3_sys as ffi;
use std::ffi::{c_char, c_int, c_uint, c_void};
use std::sync::{Arc, Mutex, Once};

pub const DECLARED_TYPE: &str = "TIMESTAMP_MICROS";
pub const ENCODER_FUNCTION: &str = "gonvex_timestamp_micros_rfc3339";

pub fn encode(value: i64) -> Result<String, String> {
    DateTime::<Utc>::from_timestamp_micros(value)
        .map(|date| date.to_rfc3339_opts(SecondsFormat::AutoSi, true))
        .ok_or_else(|| "TIMESTAMP_MICROS outside chrono RFC3339 range".into())
}
/// Only an explicitly timestamp-typed caller may use this binder. More than six
/// fraction digits and leap-second inputs need a PostgreSQL rounding oracle;
/// rejecting them is deliberate, rather than silently truncating/normalizing.
pub fn parse_rfc3339(value: &str) -> Result<i64, String> {
    if let Some((_, fraction)) = value.split_once('.') {
        if fraction.bytes().take_while(u8::is_ascii_digit).count() > 6 {
            return Err("timestamp precision above six digits is not qualified".into());
        }
    }
    let date = DateTime::parse_from_rfc3339(value)
        .map_err(|_| "timestamp needs explicit RFC3339 offset".to_owned())?;
    if date.timestamp_subsec_nanos() >= 1_000_000_000 || date.timestamp_subsec_nanos() % 1000 != 0 {
        return Err("timestamp leap-second/submicrosecond normalization is not qualified".into());
    }
    let micros = date.timestamp_micros();
    encode(micros)?;
    Ok(micros)
}
fn now() -> Result<i64, String> {
    // chrono reads the native system clock; no JS Number or millisecond clock.
    Ok(Utc::now().timestamp_micros())
}
#[derive(Default)]
struct State {
    transaction_start: Option<i64>,
    statement: Option<usize>,
}
struct Context {
    db: usize,
    state: Mutex<State>,
}
unsafe fn error(context: *mut ffi::sqlite3_context, message: &str) {
    ffi::sqlite3_result_error(context, message.as_ptr().cast(), message.len() as c_int);
}
unsafe extern "C" fn destroy(data: *mut c_void) {
    drop(Box::from_raw(data as *mut Arc<Context>));
}
unsafe extern "C" fn clock(
    context: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    let owner = &*(ffi::sqlite3_user_data(context) as *const Arc<Context>);
    let value = owner
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .transaction_start;
    match value {
        Some(value) => ffi::sqlite3_result_int64(context, value),
        None => error(context, "native transaction clock is not initialized"),
    }
}
unsafe extern "C" fn wall_clock(
    context: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    match now() {
        Ok(value) => ffi::sqlite3_result_int64(context, value),
        Err(message) => error(context, &message),
    }
}
unsafe extern "C" fn encoder(
    context: *mut ffi::sqlite3_context,
    _: c_int,
    args: *mut *mut ffi::sqlite3_value,
) {
    let value = *args;
    match ffi::sqlite3_value_type(value) {
        ffi::SQLITE_NULL => ffi::sqlite3_result_null(context),
        ffi::SQLITE_INTEGER => match encode(ffi::sqlite3_value_int64(value)) {
            Ok(text) => ffi::sqlite3_result_text(
                context,
                text.as_ptr().cast(),
                text.len() as c_int,
                ffi::SQLITE_TRANSIENT(),
            ),
            Err(message) => error(context, &message),
        },
        _ => error(context, "TIMESTAMP_MICROS requires INTEGER storage or NULL"),
    }
}
unsafe extern "C" fn trace(
    kind: c_uint,
    data: *mut c_void,
    statement: *mut c_void,
    elapsed: *mut c_void,
) -> c_int {
    if kind == ffi::SQLITE_TRACE_CLOSE as c_uint {
        return 0;
    }
    let context = &*(data as *const Context);
    if kind == ffi::SQLITE_TRACE_STMT as c_uint {
        let mut state = context.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.statement.is_none() {
            if ffi::sqlite3_get_autocommit(context.db as *mut ffi::sqlite3) != 0 {
                state.transaction_start = now().ok();
            }
            state.statement = Some(statement as usize);
        }
    } else if kind == ffi::SQLITE_TRACE_PROFILE as c_uint {
        {
            let mut state = context.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.statement == Some(statement as usize) {
                state.statement = None;
            }
        }
        if crate::statement_profile::enabled() {
            crate::statement_profile::on_trace(kind, std::ptr::null_mut(), statement, elapsed);
        }
    }
    0
}
/// Registers the auto-extension before connections open, including standalone
/// SQLx/direct connections created after a registry. Profiling is delegated,
/// not disabled; pre-update/commit/rollback/WAL hooks are untouched.
pub fn install() {
    static ONCE: Once = Once::new();
    crate::statement_profile::install();
    ONCE.call_once(|| unsafe {
        ffi::sqlite3_auto_extension(Some(entry));
    });
}
unsafe extern "C" fn entry(
    db: *mut ffi::sqlite3,
    _error: *mut *mut c_char,
    _api: *const ffi::sqlite3_api_routines,
) -> c_int {
    register(db)
}
/// # Safety
/// `db` must be a live SQLite connection exclusively owned by this caller.
unsafe fn register(db: *mut ffi::sqlite3) -> c_int {
    let context = Arc::new(Context {
        db: db as usize,
        state: Mutex::new(State::default()),
    });
    for (name, argc, callback, flags) in [
        (
            b"pg_now_micros\0".as_slice(),
            0,
            clock as unsafe extern "C" fn(_, _, _),
            ffi::SQLITE_UTF8,
        ),
        (
            b"pg_clock_timestamp_micros\0".as_slice(),
            0,
            wall_clock as unsafe extern "C" fn(_, _, _),
            ffi::SQLITE_UTF8,
        ),
        (
            b"gonvex_timestamp_micros_rfc3339\0".as_slice(),
            1,
            encoder as unsafe extern "C" fn(_, _, _),
            ffi::SQLITE_UTF8 | ffi::SQLITE_DETERMINISTIC,
        ),
    ] {
        let owner = Box::into_raw(Box::new(context.clone()));
        // SQLite calls the destructor even when registration fails.
        let rc = ffi::sqlite3_create_function_v2(
            db,
            name.as_ptr().cast(),
            argc,
            flags,
            owner.cast(),
            Some(callback),
            None,
            None,
            Some(destroy),
        );
        if rc != ffi::SQLITE_OK {
            return rc;
        }
    }
    let rc = ffi::sqlite3_trace_v2(
        db,
        (ffi::SQLITE_TRACE_STMT | ffi::SQLITE_TRACE_PROFILE | ffi::SQLITE_TRACE_CLOSE) as c_uint,
        Some(trace),
        Arc::as_ptr(&context) as *mut c_void,
    );
    if rc != ffi::SQLITE_OK {
        return rc;
    }
    // Connection-owned client data retains the trace context independently of
    // replaceable UDFs. Destruction never calls trace_v2, so it cannot remove a
    // later unrelated native observer. Registration is exclusive at open.
    ffi::sqlite3_set_clientdata(
        db,
        c"gonvex.timestamp-micros.clock-v1".as_ptr(),
        Box::into_raw(Box::new(context)).cast(),
        Some(destroy),
    )
}
/// Additional negative eligibility guard for timestamp-typed source columns.
/// A true result grants no permission capability: callers must still prove the
/// full actual module/source/index authority in their same transaction.
pub async fn source_domain(
    connection: &mut sqlx::SqliteConnection,
    table: &str,
    columns: &[String],
) -> Result<bool, crate::DatabaseError> {
    if columns.is_empty() {
        return Ok(true);
    }
    {
        let mut handle = connection.lock_handle().await?;
        if unsafe {
            ffi::sqlite3_get_clientdata(
                handle.as_raw_handle().as_ptr(),
                c"gonvex.timestamp-micros.clock-v1".as_ptr(),
            )
        }
        .is_null()
        {
            return Ok(false);
        }
    }
    let minimum = DateTime::<Utc>::MIN_UTC.timestamp_micros();
    let maximum = DateTime::<Utc>::MAX_UTC.timestamp_micros();
    let invalid = columns
        .iter()
        .map(|column| {
            let col = crate::rows::quote_identifier(column);
            format!("({col} IS NOT NULL AND (typeof({col})<>'integer' OR {col}<?1 OR {col}>?2))")
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM {} WHERE {invalid})",
        crate::rows::quote_identifier(table)
    );
    Ok(sqlx::query_scalar::<_, i64>(&sql)
        .bind(minimum)
        .bind(maximum)
        .fetch_one(connection)
        .await?
        == 0)
}

#[cfg(test)]
#[path = "timestamp_micros_tests.rs"]
mod tests;
