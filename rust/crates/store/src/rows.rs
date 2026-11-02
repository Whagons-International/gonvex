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
    let rows = sqlx::query(
        "SELECT name, type, \"notnull\", pk FROM pragma_table_info($1) ORDER BY cid",
    )
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
        JsonKind::Integer | JsonKind::Real | JsonKind::Text => qualified.to_owned(),
    }
}

/// `json_object('a', r."a", …)` for the given columns. SQLite limits one
/// function call to 1,000 arguments, so wide rows are merged with `json_patch`.
pub fn json_object_expr(alias: &str, columns: &[(String, JsonKind)]) -> String {
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
        .chunks(400)
        .map(|chunk| format!("json_object({})", chunk.join(", ")))
        .collect::<Vec<_>>();
    match chunks.len() {
        0 => "json_object()".to_owned(),
        1 => chunks.into_iter().next().unwrap(),
        _ => chunks
            .into_iter()
            .reduce(|left, right| format!("json_patch({left}, {right})"))
            .unwrap(),
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
        (_, "REAL") => Number::from_f64(real()?).map(Value::Number).unwrap_or(Value::Null),
        (_, "TEXT") => Value::String(text()?),
        (_, "BLOB") => {
            let bytes = sqlx::Value::try_decode::<Vec<u8>>(&owned).map_err(decode_error)?;
            Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                bytes,
            ))
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
    fn wide_rows_merge_json_object_chunks() {
        let columns = (0..900)
            .map(|index| (format!("c{index}"), JsonKind::Text))
            .collect::<Vec<_>>();
        let expr = json_object_expr("r", &columns);
        assert_eq!(expr.matches("json_patch").count(), 2);
    }
}
