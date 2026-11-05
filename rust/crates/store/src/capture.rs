//! Row-image capture through SQLite's pre-update hook.
//!
//! Change capture used to build each row image in SQL inside per-table
//! triggers (`json_object` over every column, twice for updates). Those
//! trigger bodies dominated the schema (2.4 MB for the Whagons tenant), and
//! SQLite keeps a parsed schema in every connection: 27 MB per connection
//! instead of 2 MB.
//!
//! Now each capture trigger is a few hundred bytes. It calls
//! `gonvex_capture(table, rowid, op)`, which returns `[old, new, changed]`
//! for the row being changed. The values come from the pre-update hook,
//! which SQLite invokes with the exact old and new column values just before
//! each row change on this connection. The trigger still inserts the change
//! row in the same statement, so statement and savepoint rollback keep
//! working exactly as before.
//!
//! The hook and function are installed on writer connections only. A
//! connection without them cannot write a captured table ("no such function:
//! gonvex_capture"), which also keeps foreign writers from bypassing capture.

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::sync::{Arc, Mutex, RwLock};

use libsqlite3_sys as ffi;
use serde_json::{Map, Number, Value};
use sqlx::SqliteConnection;

use crate::rows::JsonKind;
use crate::DatabaseError;

pub const CAPTURE_FUNCTION: &str = "gonvex_capture";

extern "C" {
    fn sqlite3_preupdate_hook(
        db: *mut ffi::sqlite3,
        callback: Option<
            unsafe extern "C" fn(
                *mut c_void,
                *mut ffi::sqlite3,
                c_int,
                *const c_char,
                *const c_char,
                i64,
                i64,
            ),
        >,
        context: *mut c_void,
    ) -> *mut c_void;
    fn sqlite3_preupdate_old(
        db: *mut ffi::sqlite3,
        index: c_int,
        value: *mut *mut ffi::sqlite3_value,
    ) -> c_int;
    fn sqlite3_preupdate_new(
        db: *mut ffi::sqlite3,
        index: c_int,
        value: *mut *mut ffi::sqlite3_value,
    ) -> c_int;
    fn sqlite3_preupdate_count(db: *mut ffi::sqlite3) -> c_int;
}

/// Whether the linked SQLite was built with the pre-update hook.
pub fn supported() -> bool {
    unsafe { ffi::sqlite3_compileoption_used(c"ENABLE_PREUPDATE_HOOK".as_ptr()) != 0 }
}

#[derive(Clone, Debug, PartialEq)]
enum Cell {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug)]
struct Event {
    table: String,
    rowid: i64,
    op: c_int,
    old: Option<Vec<Cell>>,
    new: Option<Vec<Cell>>,
}

/// Captured table: column names and kinds in declaration order, plus the
/// index of an `INTEGER PRIMARY KEY` (rowid alias), whose value the
/// pre-update hook reports through the rowid argument.
#[derive(Debug, Default)]
struct TableShape {
    columns: Vec<(String, JsonKind)>,
    rowid_alias: Option<usize>,
}

/// Captured tables of one schema, shared by every connection whose schema
/// has the same digest (every tenant of a module, typically).
// Keyed by the tenant schema's table names, looked up per captured row.
type Shapes = Arc<HashMap<String, Arc<TableShape>, foldhash::fast::RandomState>>;

#[derive(Default)]
struct Context {
    tables: RwLock<Shapes>,
    /// Digest of the definitions `tables` was built from.
    digest: Mutex<[u8; 32]>,
    events: Mutex<Vec<Event>>,
}

/// Contexts of open writer connections, keyed by `sqlite3*`.
fn contexts() -> &'static Mutex<HashMap<usize, std::sync::Weak<Context>>> {
    static CONTEXTS: std::sync::OnceLock<Mutex<HashMap<usize, std::sync::Weak<Context>>>> =
        std::sync::OnceLock::new();
    CONTEXTS.get_or_init(Default::default)
}

unsafe fn value_cell(value: *mut ffi::sqlite3_value) -> Cell {
    if value.is_null() {
        return Cell::Null;
    }
    match ffi::sqlite3_value_type(value) {
        ffi::SQLITE_INTEGER => Cell::Integer(ffi::sqlite3_value_int64(value)),
        ffi::SQLITE_FLOAT => Cell::Real(ffi::sqlite3_value_double(value)),
        ffi::SQLITE_TEXT => {
            let text = ffi::sqlite3_value_text(value);
            let len = ffi::sqlite3_value_bytes(value).max(0) as usize;
            if text.is_null() {
                Cell::Text(String::new())
            } else {
                Cell::Text(
                    String::from_utf8_lossy(std::slice::from_raw_parts(text, len)).into_owned(),
                )
            }
        }
        ffi::SQLITE_BLOB => {
            let blob = ffi::sqlite3_value_blob(value).cast::<u8>();
            let len = ffi::sqlite3_value_bytes(value).max(0) as usize;
            Cell::Blob(if blob.is_null() {
                Vec::new()
            } else {
                std::slice::from_raw_parts(blob, len).to_vec()
            })
        }
        _ => Cell::Null,
    }
}

unsafe extern "C" fn preupdate(
    context: *mut c_void,
    db: *mut ffi::sqlite3,
    op: c_int,
    database: *const c_char,
    table: *const c_char,
    old_rowid: i64,
    new_rowid: i64,
) {
    let context = &*(context as *const Context);
    if database.is_null() || table.is_null() || CStr::from_ptr(database).to_bytes() != b"main" {
        return;
    }
    let table = String::from_utf8_lossy(CStr::from_ptr(table).to_bytes()).into_owned();
    let Some(shape) = context.tables.read().unwrap().get(&table).cloned() else {
        return;
    };
    let count = sqlite3_preupdate_count(db).max(0);
    let read = |getter: unsafe extern "C" fn(
        *mut ffi::sqlite3,
        c_int,
        *mut *mut ffi::sqlite3_value,
    ) -> c_int,
                rowid: i64| {
        let mut cells = Vec::with_capacity(count as usize);
        for index in 0..count {
            let mut value = std::ptr::null_mut();
            let cell = if getter(db, index, &mut value) == ffi::SQLITE_OK {
                value_cell(value)
            } else {
                Cell::Null
            };
            cells.push(
                if shape.rowid_alias == Some(index as usize) && cell == Cell::Null {
                    Cell::Integer(rowid)
                } else {
                    cell
                },
            );
        }
        cells
    };
    let (old, new, rowid) = match op {
        ffi::SQLITE_INSERT => (
            None,
            Some(read(sqlite3_preupdate_new, new_rowid)),
            new_rowid,
        ),
        ffi::SQLITE_DELETE => (
            Some(read(sqlite3_preupdate_old, old_rowid)),
            None,
            old_rowid,
        ),
        ffi::SQLITE_UPDATE => (
            Some(read(sqlite3_preupdate_old, old_rowid)),
            Some(read(sqlite3_preupdate_new, new_rowid)),
            new_rowid,
        ),
        _ => return,
    };
    context.events.lock().unwrap().push(Event {
        table,
        rowid,
        op,
        old,
        new,
    });
}

/// Events left unconsumed (a statement that failed after the hook fired)
/// end with the transaction.
unsafe extern "C" fn transaction_end(context: *mut c_void) -> c_int {
    (*(context as *const Context))
        .events
        .lock()
        .unwrap()
        .clear();
    0
}

unsafe extern "C" fn transaction_rollback(context: *mut c_void) {
    (*(context as *const Context))
        .events
        .lock()
        .unwrap()
        .clear();
}

fn cell_json(cell: &Cell, kind: JsonKind) -> Result<Value, String> {
    Ok(match (cell, kind) {
        (Cell::Null, _) => Value::Null,
        (Cell::Integer(value), JsonKind::TimestampMicros) => {
            Value::String(crate::timestamp_micros::encode(*value)?)
        }
        (_, JsonKind::TimestampMicros) => {
            return Err("TIMESTAMP_MICROS requires INTEGER storage or NULL".into())
        }
        (Cell::Integer(value), JsonKind::Boolean) => Value::Bool(*value != 0),
        (Cell::Real(value), JsonKind::Boolean) => Value::Bool(*value != 0.0),
        (Cell::Text(text), JsonKind::Json) => {
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.clone()))
        }
        (Cell::Integer(value), _) => Value::Number((*value).into()),
        (Cell::Real(value), _) => Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        (Cell::Text(text), _) => Value::String(text.clone()),
        (Cell::Blob(bytes), _) => {
            Value::String(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
        }
    })
}

fn image(shape: &TableShape, cells: &[Cell]) -> Result<Value, String> {
    let mut object = Map::new();
    for (index, (name, kind)) in shape.columns.iter().enumerate() {
        let cell = cells.get(index).unwrap_or(&Cell::Null);
        object.insert(name.clone(), cell_json(cell, *kind)?);
    }
    Ok(Value::Object(object))
}

/// `gonvex_capture(table, rowid, op)`: `[old, new, changed]` JSON for the
/// matching pending event, or NULL when an update changed no column.
unsafe extern "C" fn capture_function(
    context: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    let fail = |message: &str| {
        ffi::sqlite3_result_error(context, message.as_ptr().cast(), message.len() as c_int);
    };
    if argc != 3 {
        return fail("gonvex_capture(table, rowid, op) takes three arguments");
    }
    let state = &*(ffi::sqlite3_user_data(context) as *const Arc<Context>);
    let args = std::slice::from_raw_parts(argv, 3);
    let table = match value_cell(args[0]) {
        Cell::Text(table) => table,
        _ => return fail("gonvex_capture: table must be text"),
    };
    let rowid = ffi::sqlite3_value_int64(args[1]);
    let op = match ffi::sqlite3_value_int64(args[2]) {
        0 => ffi::SQLITE_INSERT,
        1 => ffi::SQLITE_UPDATE,
        _ => ffi::SQLITE_DELETE,
    };
    let Some(shape) = state.tables.read().unwrap().get(&table).cloned() else {
        return fail("gonvex_capture: table is not captured on this connection");
    };
    let event = {
        let mut events = state.events.lock().unwrap();
        let Some(position) = events
            .iter()
            .rposition(|event| event.table == table && event.rowid == rowid && event.op == op)
        else {
            drop(events);
            return fail(
                "gonvex_capture: no pending row change (writer without the capture hook?)",
            );
        };
        events.remove(position)
    };
    let old = match event
        .old
        .as_deref()
        .map(|cells| image(&shape, cells))
        .transpose()
    {
        Ok(value) => value,
        Err(message) => return fail(&message),
    };
    let new = match event
        .new
        .as_deref()
        .map(|cells| image(&shape, cells))
        .transpose()
    {
        Ok(value) => value,
        Err(message) => return fail(&message),
    };
    let changed = match (&event.old, &event.new) {
        (Some(old), Some(new)) => {
            let names = shape
                .columns
                .iter()
                .enumerate()
                .filter(|(index, _)| old.get(*index) != new.get(*index))
                .map(|(_, (name, _))| Value::String(name.clone()))
                .collect::<Vec<_>>();
            if names.is_empty() {
                ffi::sqlite3_result_null(context);
                return;
            }
            Value::Array(names)
        }
        _ => Value::Array(Vec::new()),
    };
    let text = Value::Array(vec![
        old.unwrap_or(Value::Null),
        new.unwrap_or(Value::Null),
        changed,
    ])
    .to_string();
    ffi::sqlite3_result_text(
        context,
        text.as_ptr().cast(),
        text.len() as c_int,
        ffi::SQLITE_TRANSIENT(),
    );
}

unsafe extern "C" fn destroy_context(data: *mut c_void) {
    drop(Box::from_raw(data as *mut Arc<Context>));
}

/// Installs the hook and `gonvex_capture` on a writer connection and loads
/// the captured-table catalog. Call again after DDL changes captured tables.
pub async fn install(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    if !supported() {
        return Err(DatabaseError::Decode(
            "the linked SQLite lacks SQLITE_ENABLE_PREUPDATE_HOOK; build with rust/.cargo/config.toml".into(),
        ));
    }
    let digest = catalog_digest(connection).await?;
    let shapes = load_shapes(connection, digest).await?;
    let mut handle = connection.lock_handle().await?;
    let db = handle.as_raw_handle().as_ptr();
    let key = db as usize;
    let existing = contexts()
        .lock()
        .unwrap()
        .get(&key)
        .and_then(std::sync::Weak::upgrade);
    let context = match existing {
        Some(context) => context,
        None => {
            let context = Arc::new(Context::default());
            let data = Box::into_raw(Box::new(context.clone()));
            let raw = Arc::as_ptr(&context) as *mut c_void;
            unsafe {
                let name = std::ffi::CString::new(CAPTURE_FUNCTION).unwrap();
                let rc = ffi::sqlite3_create_function_v2(
                    db,
                    name.as_ptr(),
                    3,
                    ffi::SQLITE_UTF8,
                    data.cast(),
                    Some(capture_function),
                    None,
                    None,
                    Some(destroy_context),
                );
                if rc != ffi::SQLITE_OK {
                    drop(Box::from_raw(data));
                    return Err(DatabaseError::Decode(format!(
                        "register {CAPTURE_FUNCTION}: code {rc}"
                    )));
                }
                sqlite3_preupdate_hook(db, Some(preupdate), raw);
                ffi::sqlite3_commit_hook(db, Some(transaction_end), raw);
                ffi::sqlite3_rollback_hook(db, Some(transaction_rollback), raw);
            }
            let mut map = contexts().lock().unwrap();
            map.retain(|_, weak| weak.strong_count() > 0);
            map.insert(key, Arc::downgrade(&context));
            context
        }
    };
    *context.tables.write().unwrap() = shapes;
    *context.digest.lock().unwrap() = digest;
    Ok(())
}

/// Reloads the captured-table catalog of this connection, if it captures.
/// Only a digest is read when the captured tables did not change (scripts
/// re-run at every boot mostly leave them untouched).
pub async fn refresh(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    let key = {
        let mut handle = connection.lock_handle().await?;
        handle.as_raw_handle().as_ptr() as usize
    };
    let context = contexts()
        .lock()
        .unwrap()
        .get(&key)
        .and_then(std::sync::Weak::upgrade);
    if let Some(context) = context {
        let digest = catalog_digest(connection).await?;
        if digest == *context.digest.lock().unwrap() {
            return Ok(());
        }
        let shapes = load_shapes(connection, digest).await?;
        *context.tables.write().unwrap() = shapes;
        *context.digest.lock().unwrap() = digest;
    }
    Ok(())
}

/// Digest of the `sqlite_schema` rows the capture catalog is a function of:
/// every table definition and every capture trigger (one row).
async fn catalog_digest(connection: &mut SqliteConnection) -> Result<[u8; 32], DatabaseError> {
    use sha2::{Digest, Sha256};
    let definitions: Option<String> = sqlx::query_scalar(
        r#"SELECT group_concat(type || char(31) || name || char(31) || tbl_name || char(31)
                               || coalesce(sql, ''), char(30) ORDER BY type, name)
           FROM sqlite_schema
           WHERE type = 'table'
              OR (type = 'trigger' AND sql LIKE '%' || $1 || '(%')"#,
    )
    .bind(CAPTURE_FUNCTION)
    .fetch_one(connection)
    .await?;
    Ok(Sha256::digest(definitions.unwrap_or_default().as_bytes()).into())
}

/// Catalogs by schema digest. Every tenant file of a module has the same
/// schema, so a boot over N tenants builds the catalog once instead of N
/// times (each build is a 4,303-row `pragma_table_info` join for Whagons).
fn shape_cache() -> &'static Mutex<HashMap<[u8; 32], Shapes>> {
    static CACHE: std::sync::OnceLock<Mutex<HashMap<[u8; 32], Shapes>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// Distinct schemas kept in [`shape_cache`]; more are rebuilt on demand.
const SHAPE_CACHE_ENTRIES: usize = 16;

/// Tables that carry capture triggers, with their column shapes, for the
/// schema whose [`catalog_digest`] is `digest` (from the cache when any
/// connection already built it).
async fn load_shapes(
    connection: &mut SqliteConnection,
    digest: [u8; 32],
) -> Result<Shapes, DatabaseError> {
    if let Some(shapes) = shape_cache().lock().unwrap().get(&digest).cloned() {
        return Ok(shapes);
    }
    // One JSON row instead of one row per column: each row crosses sqlx's
    // worker thread separately, which made this the slowest boot statement.
    let encoded: Option<String> = sqlx::query_scalar(
        r#"WITH captured(name) AS MATERIALIZED (
             SELECT DISTINCT tbl_name FROM sqlite_schema
             WHERE type = 'trigger' AND sql LIKE '%' || $1 || '(%')
           SELECT json_group_array(json_array(m.name, p.name, p.type, p.pk)
                                   ORDER BY m.name, p.cid)
           FROM sqlite_schema AS m JOIN pragma_table_info(m.name) AS p
           WHERE m.type = 'table' AND m.name IN captured"#,
    )
    .bind(CAPTURE_FUNCTION)
    .fetch_one(connection)
    .await?;
    let rows: Vec<(String, String, String, i64)> =
        serde_json::from_str(encoded.as_deref().unwrap_or("[]"))
            .map_err(|error| DatabaseError::Decode(format!("capture catalog: {error}")))?;
    let mut shapes: HashMap<String, (TableShape, usize)> = HashMap::new();
    for (table, name, declared, pk) in rows {
        let (shape, pk_columns) = shapes.entry(table).or_default();
        if pk > 0 {
            *pk_columns += 1;
        }
        if pk == 1 && declared.trim().eq_ignore_ascii_case("INTEGER") {
            shape.rowid_alias = Some(shape.columns.len());
        }
        shape
            .columns
            .push((name, JsonKind::from_declared(&declared)));
    }
    let shapes: Shapes = Arc::new(
        shapes
            .into_iter()
            .map(|(table, (mut shape, pk_columns))| {
                // Only a single-column INTEGER PRIMARY KEY aliases the rowid.
                if pk_columns != 1 {
                    shape.rowid_alias = None;
                }
                (table, Arc::new(shape))
            })
            .collect(),
    );
    let mut cache = shape_cache().lock().unwrap();
    if cache.len() >= SHAPE_CACHE_ENTRIES {
        cache.clear();
    }
    cache.insert(digest, shapes.clone());
    Ok(shapes)
}

#[cfg(test)]
mod tests {
    use crate::feed::{capture_triggers_sql, finalize, Provenance, SYNC_INFRASTRUCTURE_SQL};
    use crate::rows::table_columns;
    use serde_json::{json, Value};
    use sqlx::{Connection, Row, SqliteConnection};

    async fn connection_with(table_sql: &str, table: &str, key: &str) -> SqliteConnection {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(SYNC_INFRASTRUCTURE_SQL)
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::raw_sql(table_sql)
            .execute(&mut connection)
            .await
            .unwrap();
        let columns = table_columns(&mut connection, table).await.unwrap();
        sqlx::raw_sql(&capture_triggers_sql(table, key, &columns))
            .execute(&mut connection)
            .await
            .unwrap();
        super::install(&mut connection).await.unwrap();
        connection
    }

    async fn changes(connection: &mut SqliteConnection) -> Vec<(String, Value, Value, Value)> {
        sqlx::query("SELECT operation, old_value, new_value, changed_columns FROM _gonvex_sync_changes ORDER BY event_id")
            .fetch_all(connection)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                let json = |index: usize| {
                    row.get::<Option<String>, _>(index)
                        .map(|text| serde_json::from_str(&text).unwrap())
                        .unwrap_or(Value::Null)
                };
                (row.get(0), json(1), json(2), json(3))
            })
            .collect()
    }

    #[tokio::test]
    async fn nested_same_row_trigger_updates_keep_their_own_images() {
        // Like Whagons' member timestamps: an app trigger updates the row it fired for.
        let mut connection = connection_with(
            r#"CREATE TABLE m (id TEXT PRIMARY KEY, name TEXT, updated_at INTEGER);
               CREATE TRIGGER stamp AFTER UPDATE OF name ON m BEGIN
                 UPDATE m SET updated_at = 99 WHERE id = NEW.id;
               END;"#,
            "m",
            "id",
        )
        .await;
        let mut tx = connection.begin().await.unwrap();
        sqlx::query("INSERT INTO m VALUES ('a', 'old', 1)")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE m SET name = 'new' WHERE id = 'a'")
            .execute(&mut *tx)
            .await
            .unwrap();
        finalize(&mut tx, &Provenance::default())
            .await
            .unwrap()
            .unwrap();
        tx.commit().await.unwrap();
        let rows = changes(&mut connection).await;
        assert_eq!(rows[0].0, "insert");
        assert_eq!(rows[0].2, json!({"id":"a","name":"old","updated_at":1}));
        let updates = rows
            .iter()
            .filter(|row| row.0 == "update")
            .collect::<Vec<_>>();
        assert_eq!(updates.len(), 2, "{rows:?}");
        let name_change = updates.iter().find(|row| row.3 == json!(["name"])).unwrap();
        assert_eq!(name_change.1, json!({"id":"a","name":"old","updated_at":1}));
        assert_eq!(name_change.2, json!({"id":"a","name":"new","updated_at":1}));
        let stamp = updates
            .iter()
            .find(|row| row.3 == json!(["updated_at"]))
            .unwrap();
        assert_eq!(stamp.1, json!({"id":"a","name":"new","updated_at":1}));
        assert_eq!(stamp.2, json!({"id":"a","name":"new","updated_at":99}));
    }

    #[tokio::test]
    async fn failed_statement_leaves_no_change_and_later_changes_capture_correctly() {
        let mut connection = connection_with(
            r#"CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER NOT NULL CHECK (n < 10))"#,
            "t",
            "id",
        )
        .await;
        let mut tx = connection.begin().await.unwrap();
        sqlx::query("INSERT INTO t VALUES ('a', 1), ('c', 4)")
            .execute(&mut *tx)
            .await
            .unwrap();
        // One statement: 'a' becomes 3 (hook fires, its change row is
        // written), then 'c' becomes 12 and fails the CHECK. SQLite rolls the
        // whole statement back, including the change row already written.
        let failed = sqlx::query("UPDATE t SET n = n * 3")
            .execute(&mut *tx)
            .await;
        assert!(failed.is_err());
        sqlx::query("UPDATE t SET n = 2 WHERE id = 'a'")
            .execute(&mut *tx)
            .await
            .unwrap();
        finalize(&mut tx, &Provenance::default())
            .await
            .unwrap()
            .unwrap();
        tx.commit().await.unwrap();
        let rows = changes(&mut connection).await;
        let last = rows.last().unwrap();
        assert_eq!(last.0, "update");
        assert_eq!(last.2, json!({"id":"a","n":2}));
        assert!(
            rows.iter().all(|row| row.2 != json!({"id":"a","n":3})),
            "{rows:?}"
        );
        assert_eq!(
            rows.iter().filter(|row| row.0 == "update").count(),
            1,
            "{rows:?}"
        );
        assert_eq!(last.1, json!({"id":"a","n":1}));
    }
}
