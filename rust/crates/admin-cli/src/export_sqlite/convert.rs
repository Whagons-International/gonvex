//! Postgres value -> SQLite value conversion, decided by the SQLite column's
//! declared type (`pragma_table_info`), plus the canonical JSON form used to
//! compare a converted source row with the stored SQLite row.

use chrono::{DateTime, NaiveDate, SecondsFormat};
use serde_json::{Number, Value};

const MS_PER_DAY: i64 = 86_400_000;

/// How a target column stores values, from its declared type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetKind {
    Json,
    Boolean,
    Integer,
    Real,
    Text,
    Blob,
    /// No declared type: values keep their natural storage class.
    Any,
}

impl TargetKind {
    /// Same precedence as `gonvex_store::rows::JsonKind::from_declared`, with
    /// `BLOB` and the empty declaration distinguished.
    pub fn from_declared(declared: &str) -> Self {
        let declared = declared.trim().to_ascii_uppercase();
        if declared.is_empty() {
            Self::Any
        } else if declared.contains("JSON") {
            Self::Json
        } else if declared.contains("BOOL") {
            Self::Boolean
        } else if declared.contains("INT") {
            Self::Integer
        } else if declared.contains("BLOB") {
            Self::Blob
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

/// How a Postgres column is read. Each family has one SELECT expression and
/// one decoded Rust shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceFamily {
    Bool,
    Int,
    Float,
    /// `numeric` / `money`, read as exact text.
    Numeric,
    /// `timestamp[tz]`, read as integer microseconds since the Unix epoch (UTC).
    Timestamp,
    /// `date`, read as days since 1970-01-01.
    Date,
    /// `json`, `jsonb` and arrays, read as JSON text.
    Json,
    Bytes,
    /// Everything else (text, varchar, uuid, enums, interval, inet, ...),
    /// read through `::text`.
    Text,
}

impl SourceFamily {
    /// From `information_schema.columns.data_type` / `udt_name`.
    pub fn from_postgres(data_type: &str, udt_name: &str) -> Self {
        match data_type {
            "boolean" => Self::Bool,
            "smallint" | "integer" | "bigint" => Self::Int,
            "real" | "double precision" => Self::Float,
            "numeric" | "money" => Self::Numeric,
            "timestamp with time zone" | "timestamp without time zone" => Self::Timestamp,
            "date" => Self::Date,
            "json" | "jsonb" | "ARRAY" => Self::Json,
            "bytea" => Self::Bytes,
            _ if udt_name.starts_with('_') => Self::Json,
            _ => Self::Text,
        }
    }

    /// SELECT expression for an already quoted column. Timestamps without a
    /// time zone are interpreted as UTC (Postgres `extract(epoch …)` semantics).
    pub fn select_expr(self, column: &str, data_type: &str) -> String {
        match self {
            Self::Bool => column.to_owned(),
            Self::Int => format!("{column}::int8"),
            Self::Float => format!("{column}::float8"),
            Self::Numeric => format!("{column}::numeric::text"),
            Self::Timestamp => format!("floor(extract(epoch FROM {column}) * 1000000)::int8"),
            Self::Date => format!("({column} - DATE '1970-01-01')::int8"),
            Self::Json if data_type == "ARRAY" => format!("array_to_json({column})::text"),
            Self::Json => format!("{column}::text"),
            Self::Bytes => column.to_owned(),
            Self::Text => format!("{column}::text"),
        }
    }
}

/// One decoded Postgres cell.
#[derive(Clone, Debug, PartialEq)]
pub enum SourceValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Numeric(String),
    TimestampMicros(i64),
    DateDays(i64),
    Json(String),
    Text(String),
    Bytes(Vec<u8>),
}

/// One SQLite value to bind.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

fn fail(value: &SourceValue, kind: TargetKind, why: &str) -> String {
    let shown = match value {
        SourceValue::Text(text) | SourceValue::Json(text) | SourceValue::Numeric(text)
            if text.chars().count() > 60 =>
        {
            format!("{:?}…", text.chars().take(60).collect::<String>())
        }
        SourceValue::Bytes(bytes) => format!("<{} bytes>", bytes.len()),
        other => format!("{other:?}"),
    };
    format!("cannot store {shown} in a {kind:?} column: {why}")
}

fn millis_from_micros(micros: i64) -> i64 {
    micros.div_euclid(1000)
}

fn rfc3339_from_micros(micros: i64) -> Option<String> {
    DateTime::from_timestamp_micros(micros)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

fn date_text(days: i64) -> Option<String> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?
        .checked_add_signed(chrono::Duration::days(days))
        .map(|date| date.format("%Y-%m-%d").to_string())
}

/// Exact `f64` for an integer, or `None` if it would round.
fn exact_f64(value: i64) -> Option<f64> {
    let float = value as f64;
    (float.abs() < 9_007_199_254_740_992.0 && float as i64 == value).then_some(float)
}

/// Integral `f64` as `i64`, or `None`.
fn integral_i64(value: f64) -> Option<i64> {
    (value.is_finite() && value.fract() == 0.0 && value.abs() < 9.2e18).then_some(value as i64)
}

/// `"12"`, `"12.000"` or `"-3e2"` as an exact integer.
fn integral_numeric(text: &str) -> Option<i64> {
    let text = text.trim();
    if let Ok(value) = text.parse::<i64>() {
        return Some(value);
    }
    let (whole, fraction) = text.split_once('.')?;
    if !fraction.bytes().all(|byte| byte == b'0') {
        return None;
    }
    whole.parse::<i64>().ok()
}

fn parse_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "y" | "yes" | "on" => Some(true),
        "f" | "false" | "0" | "n" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_timestamp_millis(text: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(text.trim())
        .ok()
        .map(|value| value.timestamp_millis())
}

fn float_json(value: f64) -> Option<String> {
    Number::from_f64(value).map(|number| number.to_string())
}

/// Converts one Postgres cell for a column of `kind`. Never rounds silently:
/// a value that cannot be stored exactly (a fractional numeric in an
/// INTEGER column, an unparsable text in a BOOLEAN column, …) is an error.
pub fn convert(value: &SourceValue, kind: TargetKind) -> Result<SqlValue, String> {
    use SourceValue as S;
    use SqlValue as V;
    use TargetKind as K;
    let err = |why: &str| Err(fail(value, kind, why));
    Ok(match (value, kind) {
        (S::Null, _) => V::Null,

        (S::Bool(flag), K::Boolean | K::Integer | K::Any) => V::Integer(i64::from(*flag)),
        (S::Bool(flag), K::Real) => V::Real(if *flag { 1.0 } else { 0.0 }),
        (S::Bool(flag), K::Json | K::Text) => V::Text(flag.to_string()),

        (S::Int(number), K::Integer | K::Any) => V::Integer(*number),
        (S::Int(number), K::Boolean) => match number {
            0 | 1 => V::Integer(*number),
            _ => return err("only 0 and 1 are booleans"),
        },
        (S::Int(number), K::Real) => match exact_f64(*number) {
            Some(float) => V::Real(float),
            None => return err("integer is not exactly representable as REAL"),
        },
        (S::Int(number), K::Json | K::Text) => V::Text(number.to_string()),

        (S::Float(float), _) if float.is_nan() => return err("NaN is not storable"),
        (S::Float(float), K::Real | K::Any) => V::Real(*float),
        (S::Float(float), K::Integer) => match integral_i64(*float) {
            Some(number) => V::Integer(number),
            None => return err("fractional value would be truncated"),
        },
        (S::Float(float), K::Boolean) => {
            if *float == 0.0 || *float == 1.0 {
                V::Integer(*float as i64)
            } else {
                return err("only 0 and 1 are booleans");
            }
        }
        (S::Float(float), K::Text) => V::Text(float.to_string()),
        (S::Float(float), K::Json) => match float_json(*float) {
            Some(text) => V::Text(text),
            None => return err("infinity is not JSON"),
        },

        (S::Numeric(text), K::Integer) => match integral_numeric(text) {
            Some(number) => V::Integer(number),
            None => return err("fractional or out-of-range numeric would be truncated"),
        },
        (S::Numeric(text), K::Real | K::Any) => match text.trim().parse::<f64>() {
            Ok(float) if float.is_finite() => V::Real(float),
            _ => return err("numeric is not a finite number"),
        },
        (S::Numeric(text), K::Boolean) => match integral_numeric(text) {
            Some(number @ (0 | 1)) => V::Integer(number),
            _ => return err("only 0 and 1 are booleans"),
        },
        (S::Numeric(text), K::Text) => V::Text(text.clone()),
        (S::Numeric(text), K::Json) => match serde_json::from_str::<Value>(text) {
            Ok(Value::Number(_)) => V::Text(text.trim().to_owned()),
            _ => return err("numeric is not a JSON number"),
        },

        (S::TimestampMicros(micros), K::Integer | K::Any | K::Json) => {
            V::Integer(millis_from_micros(*micros))
        }
        (S::TimestampMicros(micros), K::Real) => V::Real(*micros as f64 / 1000.0),
        (S::TimestampMicros(micros), K::Text) => match rfc3339_from_micros(*micros) {
            Some(text) => V::Text(text),
            None => return err("timestamp is out of range"),
        },

        (S::DateDays(days), K::Integer | K::Any | K::Json) => match days.checked_mul(MS_PER_DAY) {
            Some(millis) => V::Integer(millis),
            None => return err("date is out of range"),
        },
        (S::DateDays(days), K::Real) => V::Real(*days as f64 * MS_PER_DAY as f64),
        (S::DateDays(days), K::Text) => match date_text(*days) {
            Some(text) => V::Text(text),
            None => return err("date is out of range"),
        },

        (S::Json(text), K::Json | K::Text | K::Any) => match serde_json::from_str::<Value>(text) {
            Ok(_) => V::Text(text.clone()),
            Err(error) => return err(&format!("invalid JSON: {error}")),
        },
        (S::Json(text), K::Integer | K::Real | K::Boolean) => {
            match serde_json::from_str::<Value>(text) {
                Ok(Value::Bool(flag)) => return convert(&S::Bool(flag), kind),
                Ok(Value::Number(number)) => match (number.as_i64(), number.as_f64()) {
                    (Some(integer), _) => return convert(&S::Int(integer), kind),
                    (None, Some(float)) => return convert(&S::Float(float), kind),
                    _ => return err("JSON number out of range"),
                },
                _ => return err("only a JSON boolean or number fits a scalar column"),
            }
        }

        (S::Text(text), K::Text | K::Any) => V::Text(text.clone()),
        (S::Text(text), K::Integer) => match text.trim().parse::<i64>() {
            Ok(number) => V::Integer(number),
            Err(_) => match parse_timestamp_millis(text) {
                Some(millis) => V::Integer(millis),
                None => return err("text is neither an integer nor an RFC 3339 timestamp"),
            },
        },
        (S::Text(text), K::Real) => match text.trim().parse::<f64>() {
            Ok(float) if !float.is_nan() => V::Real(float),
            _ => return err("text is not a number"),
        },
        (S::Text(text), K::Boolean) => match parse_bool(text) {
            Some(flag) => V::Integer(i64::from(flag)),
            None => return err("text is not a boolean"),
        },
        // A text column becoming JSON keeps valid JSON as is and stores any
        // other text as a JSON string.
        (S::Text(text), K::Json) => match serde_json::from_str::<Value>(text) {
            Ok(_) => V::Text(text.clone()),
            Err(_) => V::Text(Value::String(text.clone()).to_string()),
        },
        (S::Text(text), K::Blob) => V::Blob(text.as_bytes().to_vec()),

        (S::Bytes(bytes), K::Blob | K::Any) => V::Blob(bytes.clone()),

        (_, K::Blob) => return err("only bytea or text can become a BLOB"),
        (S::Bytes(_), _) => return err("bytea can only become a BLOB"),
        (S::TimestampMicros(_) | S::DateDays(_), K::Boolean) => {
            return err("a date or timestamp is not a boolean")
        }
    })
}

fn number_value(value: f64) -> Value {
    Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(value.to_string()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Canonical JSON of a stored (or about to be stored) value, reflecting the
/// target column's affinity, so that a converted source row and the row read
/// back from SQLite compare equal exactly when they hold the same data.
pub fn canonical(value: &SqlValue, kind: TargetKind) -> Value {
    use SqlValue as V;
    use TargetKind as K;
    match (value, kind) {
        (V::Null, _) => Value::Null,
        (V::Blob(bytes), _) => Value::String(format!("x'{}'", hex(bytes))),
        (V::Text(text), K::Json) => {
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.clone()))
        }
        (V::Integer(number), K::Boolean) => Value::Bool(*number != 0),
        (V::Real(float), K::Boolean) => Value::Bool(*float != 0.0),
        (V::Integer(number), K::Real) => number_value(*number as f64),
        (V::Real(float), K::Real) => number_value(*float),
        (V::Real(float), K::Integer) => match integral_i64(*float) {
            Some(number) => Value::from(number),
            None => number_value(*float),
        },
        (V::Integer(number), K::Text) => Value::String(number.to_string()),
        (V::Real(float), K::Text) => Value::String(float.to_string()),
        (V::Integer(number), _) => Value::from(*number),
        (V::Real(float), _) => number_value(*float),
        (V::Text(text), _) => Value::String(text.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use SourceValue as S;
    use SqlValue as V;
    use TargetKind as K;

    #[test]
    fn declared_types_follow_the_storage_contract() {
        for (declared, kind) in [
            ("JSON", K::Json),
            ("json", K::Json),
            ("BOOLEAN", K::Boolean),
            ("INTEGER", K::Integer),
            ("BIGINT", K::Integer),
            ("REAL", K::Real),
            ("DOUBLE PRECISION", K::Real),
            ("NUMERIC", K::Real),
            ("DECIMAL(10,2)", K::Real),
            ("TEXT", K::Text),
            ("VARCHAR(20)", K::Text),
            ("BLOB", K::Blob),
            ("", K::Any),
        ] {
            assert_eq!(TargetKind::from_declared(declared), kind, "{declared}");
        }
    }

    #[test]
    fn postgres_types_map_to_families() {
        for (data_type, udt, family) in [
            ("boolean", "bool", SourceFamily::Bool),
            ("bigint", "int8", SourceFamily::Int),
            ("integer", "int4", SourceFamily::Int),
            ("double precision", "float8", SourceFamily::Float),
            ("numeric", "numeric", SourceFamily::Numeric),
            (
                "timestamp with time zone",
                "timestamptz",
                SourceFamily::Timestamp,
            ),
            (
                "timestamp without time zone",
                "timestamp",
                SourceFamily::Timestamp,
            ),
            ("date", "date", SourceFamily::Date),
            ("jsonb", "jsonb", SourceFamily::Json),
            ("json", "json", SourceFamily::Json),
            ("ARRAY", "_text", SourceFamily::Json),
            ("bytea", "bytea", SourceFamily::Bytes),
            ("uuid", "uuid", SourceFamily::Text),
            ("text", "text", SourceFamily::Text),
            ("USER-DEFINED", "status_enum", SourceFamily::Text),
        ] {
            assert_eq!(
                SourceFamily::from_postgres(data_type, udt),
                family,
                "{data_type}"
            );
        }
        assert_eq!(
            SourceFamily::Json.select_expr("\"tags\"", "ARRAY"),
            "array_to_json(\"tags\")::text"
        );
        assert_eq!(
            SourceFamily::Timestamp.select_expr("\"at\"", "timestamp with time zone"),
            "floor(extract(epoch FROM \"at\") * 1000000)::int8"
        );
    }

    #[test]
    fn timestamps_and_dates_become_unix_milliseconds() {
        // 2024-01-02T03:04:05.678901Z
        let micros = 1_704_164_645_678_901;
        assert_eq!(
            convert(&S::TimestampMicros(micros), K::Integer).unwrap(),
            V::Integer(1_704_164_645_678)
        );
        // Floor, not truncation toward zero, before the epoch.
        assert_eq!(
            convert(&S::TimestampMicros(-1), K::Integer).unwrap(),
            V::Integer(-1)
        );
        assert_eq!(
            convert(&S::TimestampMicros(micros), K::Text).unwrap(),
            V::Text("2024-01-02T03:04:05.678901Z".into())
        );
        assert_eq!(
            convert(&S::DateDays(19_724), K::Integer).unwrap(),
            V::Integer(19_724 * MS_PER_DAY)
        );
        assert_eq!(
            convert(&S::DateDays(19_724), K::Text).unwrap(),
            V::Text("2024-01-02".into())
        );
        assert_eq!(
            convert(&S::Text("2024-01-02T03:04:05.678Z".into()), K::Integer).unwrap(),
            V::Integer(1_704_164_645_678)
        );
    }

    #[test]
    fn booleans_become_zero_or_one() {
        assert_eq!(convert(&S::Bool(true), K::Boolean).unwrap(), V::Integer(1));
        assert_eq!(convert(&S::Bool(false), K::Boolean).unwrap(), V::Integer(0));
        assert_eq!(convert(&S::Bool(true), K::Integer).unwrap(), V::Integer(1));
        assert_eq!(
            convert(&S::Text("t".into()), K::Boolean).unwrap(),
            V::Integer(1)
        );
        assert!(convert(&S::Int(2), K::Boolean).is_err());
        assert!(convert(&S::Text("maybe".into()), K::Boolean).is_err());
        assert_eq!(canonical(&V::Integer(1), K::Boolean), json!(true));
    }

    #[test]
    fn json_and_arrays_become_json_text() {
        assert_eq!(
            convert(&S::Json(r#"{"a": [1, 2]}"#.into()), K::Json).unwrap(),
            V::Text(r#"{"a": [1, 2]}"#.into())
        );
        assert_eq!(
            convert(&S::Json(r#"["x","y"]"#.into()), K::Json).unwrap(),
            V::Text(r#"["x","y"]"#.into())
        );
        assert!(convert(&S::Json("{".into()), K::Json).is_err());
        // Text that is not JSON is stored as a JSON string.
        assert_eq!(
            convert(&S::Text("plain".into()), K::Json).unwrap(),
            V::Text("\"plain\"".into())
        );
        assert_eq!(
            canonical(&V::Text(r#"{"b":1,"a":[true]}"#.into()), K::Json),
            json!({"a": [true], "b": 1})
        );
        assert_eq!(
            convert(&S::Json("true".into()), K::Boolean).unwrap(),
            V::Integer(1)
        );
    }

    #[test]
    fn numerics_never_round_silently() {
        assert_eq!(
            convert(&S::Numeric("12.50".into()), K::Real).unwrap(),
            V::Real(12.5)
        );
        assert_eq!(
            convert(&S::Numeric("12.50".into()), K::Text).unwrap(),
            V::Text("12.50".into())
        );
        assert_eq!(
            convert(&S::Numeric("12.000".into()), K::Integer).unwrap(),
            V::Integer(12)
        );
        assert!(convert(&S::Numeric("12.5".into()), K::Integer).is_err());
        assert!(convert(&S::Numeric("NaN".into()), K::Real).is_err());
        assert!(convert(&S::Float(1.5), K::Integer).is_err());
        assert!(convert(&S::Float(f64::NAN), K::Real).is_err());
        assert!(convert(&S::Int(9_007_199_254_740_993), K::Real).is_err());
        assert_eq!(convert(&S::Int(7), K::Real).unwrap(), V::Real(7.0));
    }

    #[test]
    fn uuids_text_and_bytes() {
        let id = "4985bbc5-74e7-4c82-b3aa-fbadc49c8090";
        assert_eq!(
            convert(&S::Text(id.into()), K::Text).unwrap(),
            V::Text(id.into())
        );
        assert_eq!(
            convert(&S::Bytes(vec![0, 255]), K::Blob).unwrap(),
            V::Blob(vec![0, 255])
        );
        assert!(convert(&S::Bytes(vec![1]), K::Text).is_err());
        assert!(convert(&S::Int(1), K::Blob).is_err());
        assert_eq!(convert(&S::Null, K::Blob).unwrap(), V::Null);
        assert_eq!(canonical(&V::Blob(vec![0, 255]), K::Blob), json!("x'00ff'"));
    }

    #[test]
    fn canonical_form_absorbs_sqlite_affinity() {
        // REAL affinity stores an integer as REAL; INTEGER affinity stores an
        // integral REAL as INTEGER; TEXT affinity stores numbers as text.
        assert_eq!(
            canonical(&V::Integer(3), K::Real),
            canonical(&V::Real(3.0), K::Real)
        );
        assert_eq!(
            canonical(&V::Real(3.0), K::Integer),
            canonical(&V::Integer(3), K::Integer)
        );
        assert_eq!(
            canonical(&V::Integer(3), K::Text),
            canonical(&V::Text("3".into()), K::Text)
        );
        assert_ne!(
            canonical(&V::Integer(3), K::Integer),
            canonical(&V::Integer(4), K::Integer)
        );
    }
}
