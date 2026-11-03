use gonvex_store::rows::JsonKind;
use serde_json::Value;
use sqlx::{query::Query, sqlite::SqliteArguments, Sqlite};

/// One SQLite parameter. SQLite binds by value, so a statement's cached plan
/// does not depend on parameter types: arrays and objects bind as JSON text
/// (read them with `json_each($n)`), booleans as `0`/`1`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Parameter {
    Null,
    Text(String),
    Integer(i64),
    Real(f64),
    /// Serialized JSON text.
    Json(String),
}

impl Parameter {
    /// Binds a JSON value by its own type.
    pub(crate) fn scalar(value: &Value) -> Result<Self, String> {
        Ok(match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Integer(i64::from(*value)),
            Value::String(value) => Self::Text(value.clone()),
            Value::Number(value) if value.is_i64() => Self::Integer(
                value
                    .as_i64()
                    .ok_or_else(|| "integer is outside the SQLite integer range".to_owned())?,
            ),
            Value::Number(value) if value.is_u64() => Self::Integer(
                value
                    .as_u64()
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or_else(|| "integer is outside the SQLite integer range".to_owned())?,
            ),
            Value::Number(value) => Self::Real(
                value
                    .as_f64()
                    .ok_or_else(|| "number is not representable as a float".to_owned())?,
            ),
            Value::Array(_) | Value::Object(_) => Self::Json(value.to_string()),
        })
    }

    /// Binds a JSON value for a column of the given declared kind: a JSON
    /// column always stores serialized JSON (so a string stays a JSON string),
    /// a BOOLEAN column stores `0`/`1`.
    pub(crate) fn column(value: &Value, kind: JsonKind) -> Result<Self, String> {
        match (kind, value) {
            (_, Value::Null) => Ok(Self::Null),
            (JsonKind::Json, value) => Ok(Self::Json(value.to_string())),
            (JsonKind::Boolean, Value::Bool(value)) => Ok(Self::Integer(i64::from(*value))),
            _ => Self::scalar(value),
        }
    }

    /// The value as a direct-session bind (`gonvex_store::direct`).
    pub(crate) fn as_bind(&self) -> gonvex_store::direct::Bind<'_> {
        use gonvex_store::direct::Bind;
        match self {
            Self::Null => Bind::Null,
            Self::Text(value) | Self::Json(value) => Bind::Text(value),
            Self::Integer(value) => Bind::Integer(*value),
            Self::Real(value) => Bind::Real(*value),
        }
    }

    pub(crate) fn into_owned_bind(self) -> gonvex_store::direct::OwnedBind {
        use gonvex_store::direct::OwnedBind;
        match self {
            Self::Null => OwnedBind::Null,
            Self::Text(value) | Self::Json(value) => OwnedBind::Text(value),
            Self::Integer(value) => OwnedBind::Integer(value),
            Self::Real(value) => OwnedBind::Real(value),
        }
    }

    pub(crate) fn bind<'q>(
        self,
        query: Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> Query<'q, Sqlite, SqliteArguments<'q>> {
        match self {
            Self::Null => query.bind(None::<String>),
            Self::Text(value) | Self::Json(value) => query.bind(value),
            Self::Integer(value) => query.bind(value),
            Self::Real(value) => query.bind(value),
        }
    }
}

pub(crate) struct Parameters(Vec<Parameter>);

impl Parameters {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(values: Vec<Parameter>) -> Self {
        Self(values)
    }

    pub(crate) fn scalars<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<Self, String> {
        values
            .into_iter()
            .map(Parameter::scalar)
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    pub(crate) fn into_owned_binds(self) -> Vec<gonvex_store::direct::OwnedBind> {
        self.0.into_iter().map(Parameter::into_owned_bind).collect()
    }

    pub(crate) fn bind<'q>(
        self,
        mut query: Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> Query<'q, Sqlite, SqliteArguments<'q>> {
        for value in self.0 {
            query = value.bind(query);
        }
        query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Connection, Row, SqliteConnection};

    async fn connection() -> SqliteConnection {
        SqliteConnection::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite")
    }

    async fn row(
        connection: &mut SqliteConnection,
        statement: &str,
        parameters: Vec<Parameter>,
    ) -> sqlx::sqlite::SqliteRow {
        Parameters::new(parameters)
            .bind(sqlx::query(statement))
            .fetch_one(connection)
            .await
            .expect("typed query")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn changing_json_parameter_types_preserves_values_on_one_connection() {
        let mut connection = connection().await;
        let statement = "SELECT $1 AS cache_probe, typeof($1) AS kind";
        let first = row(
            &mut connection,
            statement,
            vec![Parameter::scalar(&Value::from(0)).unwrap()],
        )
        .await;
        assert_eq!(first.try_get::<i64, _>(0).unwrap(), 0);
        assert_eq!(first.try_get::<String, _>(1).unwrap(), "integer");
        let second = row(
            &mut connection,
            statement,
            vec![Parameter::scalar(&Value::from("abcdefgh")).unwrap()],
        )
        .await;
        assert_eq!(second.try_get::<String, _>(0).unwrap(), "abcdefgh");
        let object = serde_json::json!({"hello": "world"});
        let third = row(
            &mut connection,
            statement,
            vec![Parameter::scalar(&object).unwrap()],
        )
        .await;
        let text: String = third.try_get(0).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), object);
        let fourth = row(
            &mut connection,
            statement,
            vec![Parameter::scalar(&Value::Bool(true)).unwrap()],
        )
        .await;
        assert_eq!(fourth.try_get::<i64, _>(0).unwrap(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn array_parameters_bind_as_json_for_json_each() {
        let mut connection = connection().await;
        let values = serde_json::json!([" a ", "b"]);
        let rows = Parameters::scalars([&values])
            .unwrap()
            .bind(sqlx::query("SELECT value FROM json_each($1) ORDER BY key"))
            .fetch_all(&mut connection)
            .await
            .unwrap();
        let values = rows
            .iter()
            .map(|row| row.get::<String, _>(0))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![" a ", "b"]);
    }

    #[test]
    fn column_binding_follows_declared_kind() {
        assert_eq!(
            Parameter::column(&Value::from("created"), JsonKind::Json).unwrap(),
            Parameter::Json("\"created\"".to_owned())
        );
        assert_eq!(
            Parameter::column(&Value::Bool(false), JsonKind::Boolean).unwrap(),
            Parameter::Integer(0)
        );
        assert_eq!(
            Parameter::column(&Value::Null, JsonKind::Json).unwrap(),
            Parameter::Null
        );
        assert!(Parameter::scalar(&serde_json::json!(u64::MAX)).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn changing_crud_id_types_cannot_commit_unrelated_row_writes() {
        // SQLite compares an INTEGER key with a TEXT parameter as unequal, so
        // re-running a statement with a different id type matches no rows
        // instead of coercing to an unrelated row.
        let mut connection = connection().await;
        for operation in ["delete", "update", "delete_many"] {
            let table = format!("gonvex_parameter_{operation}");
            sqlx::query(&format!(
                "CREATE TEMP TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"
            ))
            .execute(&mut connection)
            .await
            .unwrap();
            sqlx::query(&format!("INSERT INTO {table} VALUES (0,'seed'),(1,'seed'),(7017280452245743464,'seed'),(4702394921427289928,'seed')"))
                .execute(&mut connection).await.unwrap();
            let (statement, first, second) = match operation {
                "delete" => (
                    format!("DELETE FROM {table} WHERE id=$1"),
                    vec![Value::from(0)],
                    vec![Value::from("abcdefgh")],
                ),
                "update" => (
                    format!("UPDATE {table} SET v=$1 WHERE id=$2 RETURNING *"),
                    vec![Value::from("valid"), Value::from(0)],
                    vec![Value::from("wrong"), Value::from("abcdefgh")],
                ),
                _ => (
                    format!("DELETE FROM {table} WHERE id IN ($1,$2)"),
                    vec![Value::from(0), Value::from(1)],
                    vec![Value::from("abcdefgh"), Value::from("ABCDEFGH")],
                ),
            };
            let mut transaction = connection.begin().await.unwrap();
            Parameters::scalars(&first)
                .unwrap()
                .bind(sqlx::query(&statement))
                .execute(&mut *transaction)
                .await
                .unwrap();
            let second = Parameters::scalars(&second)
                .unwrap()
                .bind(sqlx::query(&statement))
                .execute(&mut *transaction)
                .await
                .unwrap();
            assert_eq!(second.rows_affected(), 0, "{operation}");
            transaction.rollback().await.unwrap();
            let rows = sqlx::query(&format!("SELECT id,v FROM {table} ORDER BY id"))
                .fetch_all(&mut connection)
                .await
                .unwrap();
            assert_eq!(rows.len(), 4);
            assert!(rows
                .iter()
                .all(|row| row.try_get::<String, _>("v").unwrap() == "seed"));
        }
    }
}
