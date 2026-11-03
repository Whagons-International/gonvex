//! Declared-type aware row encoding. SQLite values carry a storage class, not
//! a type, so the declared column type (`pragma_table_info`) decides whether
//! an INTEGER is a boolean and whether TEXT holds JSON.

use serde_json::{Map, Number, Value};
use sqlx::sqlite::{SqliteRow, SqliteValueRef};
use sqlx::{Column, Row, SqliteConnection, TypeInfo, ValueRef};

use crate::DatabaseError;

/// How a column is encoded as JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonKind {
    Json,
    Boolean,
    Integer,
    Real,
    Text,
}

impl JsonKind {
    pub fn from_declared(declared: &str) -> Self {
        let declared = declared.to_ascii_uppercase();
        if declared.contains("JSON") {
            Self::Json
        } else if declared.contains("BOOL") {
            Self::Boolean
        } else if declared.contains("INT") {
            Self::Integer
        } else if declared.contains("REAL")
            || declared.contains("DOUB")
            || declared.contains("FLOA")
            || declared.contains("NUMERIC")
            || declared.contains("DECIMAL")
        {
            Self::Real
        } else {
            Self::Text
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub declared: String,
    pub not_null: bool,
    pub primary_key: bool,
}

impl ColumnInfo {
    pub fn kind(&self) -> JsonKind {
        JsonKind::from_declared(&self.declared)
    }
}

/// Columns of `table` in declaration order.
pub async fn table_columns(
    connection: &mut SqliteConnection,
    table: &str,
) -> Result<Vec<ColumnInfo>, DatabaseError> {
    let rows =
        sqlx::query("SELECT name, type, \"notnull\", pk FROM pragma_table_info($1) ORDER BY cid")
            .bind(table)
            .fetch_all(connection)
            .await?;
    Ok(rows
        .into_iter()
        .map(|row| ColumnInfo {
            name: row.get(0),
            declared: row.get::<String, _>(1),
            not_null: row.get::<i64, _>(2) != 0,
            primary_key: row.get::<i64, _>(3) != 0,
        })
        .collect())
}

pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// One column's JSON expression for `json_object`, e.g. `json(r."meta")`.
pub fn json_value_expr(qualified: &str, kind: JsonKind) -> String {
    match kind {
        JsonKind::Json => format!(
            "CASE WHEN {qualified} IS NULL THEN NULL WHEN json_valid({qualified}) THEN json({qualified}) ELSE {qualified} END"
        ),
        JsonKind::Boolean => format!(
            "CASE WHEN {qualified} IS NULL THEN NULL WHEN {qualified} THEN json('true') ELSE json('false') END"
        ),
        // json_object renders REAL with 15 significant digits, which loses
        // precision: row images (decoded in Rust) carry the exact double, so
        // two encodings of one row disagreed. 17 digits round-trip.
        JsonKind::Integer | JsonKind::Real => format!(
            "CASE WHEN {} THEN {} ELSE {qualified} END",
            finite_real(qualified),
            exact_real(qualified)
        ),
        // json_object rejects BLOB values. Text-like columns (including
        // declared BLOB/BYTEA) encode a BLOB as lowercase hex, exactly as
        // `decode_value` does, so row images and decoded rows agree.
        JsonKind::Text => format!(
            "CASE WHEN typeof({qualified}) = 'blob' THEN lower(hex({qualified})) WHEN {} THEN {} ELSE {qualified} END",
            finite_real(qualified),
            exact_real(qualified)
        ),
    }
}

fn finite_real(qualified: &str) -> String {
    format!("typeof({qualified}) = 'real' AND abs({qualified}) <= 1.7976931348623157e308")
}

fn exact_real(qualified: &str) -> String {
    format!("json(printf('%!.17g', {qualified}))")
}

/// `json_object('a', r."a", …)` for the given columns. Bundled SQLite (3.46)
/// limits one function call to 127 arguments, so wide rows build one object
/// per 60 columns and splice their members together. (`json_patch` cannot
/// merge them: a merge patch deletes keys whose value is null.)
pub fn json_object_expr(alias: &str, columns: &[(String, JsonKind)]) -> String {
    const PAIRS_PER_CALL: usize = 60;
    let pairs = columns
        .iter()
        .map(|(name, kind)| {
            let qualified = if alias.is_empty() {
                quote_identifier(name)
            } else {
                format!("{alias}.{}", quote_identifier(name))
            };
            format!(
                "'{}', {}",
                name.replace('\'', "''"),
                json_value_expr(&qualified, *kind)
            )
        })
        .collect::<Vec<_>>();
    let chunks = pairs
        .chunks(PAIRS_PER_CALL)
        .map(|chunk| format!("json_object({})", chunk.join(", ")))
        .collect::<Vec<_>>();
    match chunks.len() {
        0 => "json_object()".to_owned(),
        1 => chunks.into_iter().next().unwrap(),
        _ => {
            let members = chunks
                .iter()
                .map(|chunk| format!("substr({chunk}, 2, length({chunk}) - 2)"))
                .collect::<Vec<_>>()
                .join(" || ',' || ");
            format!("json('{{' || {members} || '}}')")
        }
    }
}

/// Decodes one cell by storage class, interpreted through its declared kind.
pub fn decode_value(value: SqliteValueRef<'_>, kind: JsonKind) -> Result<Value, DatabaseError> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let storage = value.type_info().name().to_ascii_uppercase();
    let owned = ValueRef::to_owned(&value);
    let decode_error = |error: sqlx::Error| DatabaseError::Decode(error.to_string());
    let integer = || sqlx::Value::try_decode::<i64>(&owned).map_err(decode_error);
    let real = || sqlx::Value::try_decode::<f64>(&owned).map_err(decode_error);
    let text = || sqlx::Value::try_decode::<String>(&owned).map_err(decode_error);
    Ok(match (kind, storage.as_str()) {
        (JsonKind::Boolean, "INTEGER") => Value::Bool(integer()? != 0),
        (JsonKind::Boolean, "REAL") => Value::Bool(real()? != 0.0),
        (JsonKind::Json, "TEXT") => {
            let text = text()?;
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        }
        (_, "INTEGER") => Value::Number(integer()?.into()),
        (_, "REAL") => Number::from_f64(real()?)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        (_, "TEXT") => Value::String(text()?),
        (_, "BLOB") => {
            let bytes = sqlx::Value::try_decode::<Vec<u8>>(&owned).map_err(decode_error)?;
            // Lowercase hex, matching json_value_expr's encoding in row images.
            Value::String(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
        }
        _ => Value::String(text()?),
    })
}

/// Decodes a whole row to a JSON object. `kinds` maps column names to kinds;
/// unknown columns are decoded by storage class.
pub fn row_to_object(
    row: &SqliteRow,
    kind_of: impl Fn(&str) -> Option<JsonKind>,
) -> Result<Map<String, Value>, DatabaseError> {
    let mut object = Map::new();
    for (index, column) in row.columns().iter().enumerate() {
        let name = column.name();
        let declared = column.type_info().name();
        let kind = kind_of(name).unwrap_or_else(|| JsonKind::from_declared(declared));
        let value = decode_value(row.try_get_raw(index)?, kind)?;
        object.insert(name.to_owned(), value);
    }
    Ok(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_follow_declared_type() {
        assert_eq!(JsonKind::from_declared("JSONB"), JsonKind::Json);
        assert_eq!(JsonKind::from_declared("BOOLEAN"), JsonKind::Boolean);
        assert_eq!(JsonKind::from_declared("BIGINT"), JsonKind::Integer);
        assert_eq!(JsonKind::from_declared("DOUBLE PRECISION"), JsonKind::Real);
        assert_eq!(JsonKind::from_declared("TEXT"), JsonKind::Text);
        assert_eq!(JsonKind::from_declared(""), JsonKind::Text);
    }

    #[test]
    fn wide_rows_splice_json_object_chunks() {
        let columns = (0..900)
            .map(|index| (format!("c{index}"), JsonKind::Text))
            .collect::<Vec<_>>();
        let expr = json_object_expr("r", &columns);
        assert_eq!(expr.matches("json_object(").count(), 2 * 15);
        assert!(!expr.contains("json_patch"));
    }
}

/// Declared kinds of a statement's result columns, read with
/// `sqlite3_column_decltype` on a separately prepared copy of the statement.
/// sqlx parses declared types into its own enum and reports unknown names
/// such as `JSON` as NULL, which loses the JSON/boolean distinction; this
/// reads them verbatim. `None` marks an expression column (no declared type).
pub async fn result_column_kinds(
    connection: &mut sqlx::SqliteConnection,
    sql: &str,
) -> Result<std::sync::Arc<Vec<Option<JsonKind>>>, DatabaseError> {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<HashMap<(usize, u64, String), Arc<Vec<Option<JsonKind>>>>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let mut handle = connection.lock_handle().await?;
    let raw = handle.as_raw_handle();
    let epoch = RESULT_KINDS_EPOCH.load(std::sync::atomic::Ordering::Relaxed);
    let key = (raw.as_ptr() as usize, epoch, sql.to_owned());
    if let Some(kinds) = cache.lock().unwrap().get(&key).cloned() {
        return Ok(kinds);
    }
    let kinds = unsafe { declared_kinds(raw.as_ptr(), sql) }?;
    let kinds = Arc::new(kinds);
    let mut cache = cache.lock().unwrap();
    if cache.len() > 4096 {
        cache.clear();
    }
    cache.insert(key, kinds.clone());
    Ok(kinds)
}

/// Invalidates cached result kinds; called after migrations change columns.
pub fn clear_result_column_kinds() {
    RESULT_KINDS_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

static RESULT_KINDS_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

unsafe fn declared_kinds(
    db: *mut libsqlite3_sys::sqlite3,
    sql: &str,
) -> Result<Vec<Option<JsonKind>>, DatabaseError> {
    use libsqlite3_sys as ffi;
    let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(
        db,
        sql.as_ptr().cast(),
        i32::try_from(sql.len()).map_err(|_| DatabaseError::Decode("statement too long".into()))?,
        &mut stmt,
        std::ptr::null_mut(),
    );
    if rc != ffi::SQLITE_OK || stmt.is_null() {
        let message = std::ffi::CStr::from_ptr(ffi::sqlite3_errmsg(db))
            .to_string_lossy()
            .into_owned();
        if !stmt.is_null() {
            ffi::sqlite3_finalize(stmt);
        }
        return Err(DatabaseError::Decode(format!("describe statement: {message}")));
    }
    let count = ffi::sqlite3_column_count(stmt);
    let mut kinds = Vec::with_capacity(count.max(0) as usize);
    for index in 0..count {
        let declared = ffi::sqlite3_column_decltype(stmt, index);
        kinds.push(if declared.is_null() {
            None
        } else {
            Some(JsonKind::from_declared(
                &std::ffi::CStr::from_ptr(declared).to_string_lossy(),
            ))
        });
    }
    ffi::sqlite3_finalize(stmt);
    Ok(kinds)
}

#[cfg(test)]
mod result_kind_tests {
    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn result_columns_keep_json_and_boolean_declared_types() {
        let mut connection = sqlx::SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::query(r#"CREATE TABLE t (id TEXT PRIMARY KEY, meta JSON, done BOOLEAN, n BIGINT, note TEXT)"#)
            .execute(&mut connection)
            .await
            .unwrap();
        let kinds = result_column_kinds(
            &mut connection,
            r#"SELECT id, meta, done, n, note, n + 1 AS expr FROM t AS r WHERE id = $1"#,
        )
        .await
        .unwrap();
        assert_eq!(
            kinds.as_slice(),
            &[
                Some(JsonKind::Text),
                Some(JsonKind::Json),
                Some(JsonKind::Boolean),
                Some(JsonKind::Integer),
                Some(JsonKind::Text),
                None
            ]
        );
        let returning = result_column_kinds(
            &mut connection,
            r#"INSERT INTO t (id, meta) VALUES ($1, $2) RETURNING *"#,
        )
        .await
        .unwrap();
        assert_eq!(returning.get(1).copied().flatten(), Some(JsonKind::Json));
        // A JSON-looking TEXT column stays text.
        sqlx::query(r#"INSERT INTO t (id, note) VALUES ('a', '{"not":"json"}')"#)
            .execute(&mut connection)
            .await
            .unwrap();
        let row = sqlx::query("SELECT note FROM t WHERE id = 'a'")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        let kinds = result_column_kinds(&mut connection, "SELECT note FROM t WHERE id = 'a'")
            .await
            .unwrap();
        let value = decode_value(sqlx::Row::try_get_raw(&row, 0).unwrap(), kinds[0].unwrap()).unwrap();
        assert_eq!(value, serde_json::json!(r#"{"not":"json"}"#));
    }
}

#[cfg(test)]
mod blob_tests {
    use super::*;
    use sqlx::{Connection, Row};

    #[tokio::test]
    async fn blob_values_encode_identically_in_sql_images_and_decoded_rows() {
        let mut connection = sqlx::SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE f (id TEXT PRIMARY KEY, data BLOB, note TEXT)")
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO f VALUES ('a', x'00ff10', x'ab')")
            .execute(&mut connection)
            .await
            .unwrap();
        let kinds = vec![
            ("id".to_owned(), JsonKind::Text),
            ("data".to_owned(), JsonKind::from_declared("BLOB")),
            ("note".to_owned(), JsonKind::Text),
        ];
        let image: String = sqlx::query_scalar(&format!(
            "SELECT {} FROM f AS r",
            json_object_expr("r", &kinds)
        ))
        .fetch_one(&mut connection)
        .await
        .unwrap();
        let image: Value = serde_json::from_str(&image).unwrap();
        let row = sqlx::query("SELECT id, data, note FROM f").fetch_one(&mut connection).await.unwrap();
        let decoded = Value::Object(row_to_object(&row, |_| None).unwrap());
        assert_eq!(image, serde_json::json!({"id":"a","data":"00ff10","note":"ab"}));
        assert_eq!(decoded, image);
        let _ = row.get::<String, _>(0);
    }
}
