use serde_json::Value;
use sqlx::{postgres::PgArguments, query::Query, types::Json, Postgres, Type};
use uuid::Uuid;

// Each variant owns exactly the value sent by SQLx. Its cache discriminator
// comes from that same Rust type's PostgreSQL type_info, rather than JSON text.
pub(crate) enum Parameter {
    Text(Option<String>),
    Bool(bool),
    Int8(i64),
    Float8(f64),
    Jsonb(Value),
    TextArray(Vec<String>),
    Int2Array(Vec<i16>),
    Int4Array(Vec<i32>),
    Int8Array(Vec<i64>),
    Float4Array(Vec<f32>),
    Float8Array(Vec<f64>),
    BoolArray(Vec<bool>),
    UuidArray(Vec<Uuid>),
}
fn oid<T: Type<Postgres>>() -> u32 {
    T::type_info()
        .oid()
        .expect("built-in PostgreSQL parameter type has an OID")
        .0
}
impl Parameter {
    pub(crate) fn scalar(value: &Value) -> Result<Self, String> {
        Ok(match value {
            Value::Null => Self::Text(None),
            Value::Bool(value) => Self::Bool(*value),
            Value::String(value) => Self::Text(Some(value.clone())),
            Value::Number(value) if value.is_i64() => Self::Int8(
                value
                    .as_i64()
                    .ok_or_else(|| "integer is outside PostgreSQL bigint range".to_owned())?,
            ),
            Value::Number(value) if value.is_u64() => {
                let value = value
                    .as_u64()
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or_else(|| "integer is outside PostgreSQL bigint range".to_owned())?;
                Self::Int8(value)
            }
            Value::Number(value) => Self::Float8(
                value
                    .as_f64()
                    .ok_or_else(|| "number is not representable as a float".to_owned())?,
            ),
            Value::Array(_) | Value::Object(_) => Self::Jsonb(value.clone()),
        })
    }
    pub(crate) fn for_query(value: &Value, postgres_type: Option<&str>) -> Result<Self, String> {
        let Value::Array(values) = value else {
            return Self::scalar(value);
        };
        let invalid = |expected: &str| {
            format!(
                "PostgreSQL expects {expected}, but the JSON array contains an incompatible value"
            )
        };
        Ok(match postgres_type {
            Some("_TEXT" | "_VARCHAR" | "_BPCHAR" | "TEXT[]" | "VARCHAR[]" | "CHAR[]") => {
                Self::TextArray(
                    values
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| invalid("a text array"))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            Some("_INT2" | "INT2[]" | "SMALLINT[]") => Self::Int2Array(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_i64()
                            .and_then(|value| i16::try_from(value).ok())
                            .ok_or_else(|| invalid("a smallint array"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some("_INT4" | "INT4[]" | "INTEGER[]") => Self::Int4Array(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_i64()
                            .and_then(|value| i32::try_from(value).ok())
                            .ok_or_else(|| invalid("an integer array"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some("_INT8" | "INT8[]" | "BIGINT[]") => Self::Int8Array(
                values
                    .iter()
                    .map(|value| value.as_i64().ok_or_else(|| invalid("a bigint array")))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some("_FLOAT4" | "FLOAT4[]" | "REAL[]") => Self::Float4Array(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_f64()
                            .map(|value| value as f32)
                            .ok_or_else(|| invalid("a real array"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some("_FLOAT8" | "FLOAT8[]" | "DOUBLE PRECISION[]") => Self::Float8Array(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_f64()
                            .ok_or_else(|| invalid("a double precision array"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some("_BOOL" | "BOOL[]" | "BOOLEAN[]") => Self::BoolArray(
                values
                    .iter()
                    .map(|value| value.as_bool().ok_or_else(|| invalid("a boolean array")))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some("_UUID" | "UUID[]") => Self::UuidArray(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .ok_or_else(|| invalid("a UUID array"))
                            .and_then(|value| {
                                Uuid::parse_str(value).map_err(|_| invalid("a UUID array"))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some("JSON" | "JSONB") | None => Self::Jsonb(value.clone()),
            Some(postgres_type)
                if postgres_type.starts_with('_') || postgres_type.ends_with("[]") =>
            {
                return Err(format!(
                    "PostgreSQL array parameter type {postgres_type} is not supported"
                ));
            }
            Some(postgres_type) => {
                return Err(format!("PostgreSQL expects scalar parameter type {postgres_type}, but received a JSON array"));
            }
        })
    }
    pub(crate) fn oid(&self) -> u32 {
        match self {
            Self::Text(_) => oid::<String>(),
            Self::Bool(_) => oid::<bool>(),
            Self::Int8(_) => oid::<i64>(),
            Self::Float8(_) => oid::<f64>(),
            Self::Jsonb(_) => oid::<Json<Value>>(),
            Self::TextArray(_) => oid::<Vec<String>>(),
            Self::Int2Array(_) => oid::<Vec<i16>>(),
            Self::Int4Array(_) => oid::<Vec<i32>>(),
            Self::Int8Array(_) => oid::<Vec<i64>>(),
            Self::Float4Array(_) => oid::<Vec<f32>>(),
            Self::Float8Array(_) => oid::<Vec<f64>>(),
            Self::BoolArray(_) => oid::<Vec<bool>>(),
            Self::UuidArray(_) => oid::<Vec<Uuid>>(),
        }
    }
    pub(crate) fn bind<'q>(
        self,
        query: Query<'q, Postgres, PgArguments>,
    ) -> Query<'q, Postgres, PgArguments> {
        match self {
            Self::Text(value) => query.bind(value),
            Self::Bool(value) => query.bind(value),
            Self::Int8(value) => query.bind(value),
            Self::Float8(value) => query.bind(value),
            Self::Jsonb(value) => query.bind(Json(value)),
            Self::TextArray(value) => query.bind(value),
            Self::Int2Array(value) => query.bind(value),
            Self::Int4Array(value) => query.bind(value),
            Self::Int8Array(value) => query.bind(value),
            Self::Float4Array(value) => query.bind(value),
            Self::Float8Array(value) => query.bind(value),
            Self::BoolArray(value) => query.bind(value),
            Self::UuidArray(value) => query.bind(value),
        }
    }
}

// Keep the values used for cache identity and encoding in one owned plan.
pub(crate) struct Parameters(Vec<Parameter>);

impl Parameters {
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

    pub(crate) fn statement<'a>(&self, sql: &'a str) -> std::borrow::Cow<'a, str> {
        let oids = self.0.iter().map(Parameter::oid).collect::<Vec<_>>();
        crate::sql_statement_key::statement(sql, &oids)
    }

    pub(crate) fn bind<'q>(
        self,
        mut query: Query<'q, Postgres, PgArguments>,
    ) -> Query<'q, Postgres, PgArguments> {
        for value in self.0 {
            query = value.bind(query);
        }
        query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Connection, Either, Executor, PgConnection, Row, TypeInfo};

    async fn connection() -> Option<PgConnection> {
        let Ok(url) = std::env::var("GONVEX_TEST_POSTGRES_URL") else {
            eprintln!("PostgreSQL regression skipped: set GONVEX_TEST_POSTGRES_URL");
            return None;
        };
        Some(
            PgConnection::connect(&url)
                .await
                .expect("test PostgreSQL connection"),
        )
    }

    async fn row(
        connection: &mut PgConnection,
        statement: &str,
        parameters: Vec<Parameter>,
    ) -> sqlx::postgres::PgRow {
        let parameters = Parameters::new(parameters);
        let key = parameters.statement(statement);
        parameters
            .bind(sqlx::query(key.as_ref()))
            .fetch_one(connection)
            .await
            .expect("typed query")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn changing_json_parameter_types_preserves_values_on_one_connection() {
        let Some(mut connection) = connection().await else {
            return;
        };
        let statement = "SELECT $1 AS cache_probe";
        let first = row(
            &mut connection,
            statement,
            vec![Parameter::scalar(&Value::from(0)).unwrap()],
        )
        .await;
        assert_eq!(first.try_get::<i64, _>(0).unwrap(), 0);
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
        assert_eq!(third.try_get::<Json<Value>, _>(0).unwrap().0, object);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn description_does_not_poison_text_array_binary_bindings() {
        let Some(mut connection) = connection().await else {
            return;
        };
        for (cast, expected) in [("varchar[]", vec![" a ", "b"]), ("char[]", vec![" ", "b"])] {
            let statement = format!("SELECT $1::{cast} AS values");
            let key = crate::sql_statement_key::description(&statement);
            let description = (&mut connection).describe(&key).await.unwrap();
            let Some(Either::Left(types)) = description.parameters() else {
                panic!("PostgreSQL did not describe the parameter");
            };
            let values = serde_json::json!([" a ", "b"]);
            let parameter = Parameter::for_query(&values, Some(types[0].name())).unwrap();
            let result = row(&mut connection, &statement, vec![parameter]).await;
            assert_eq!(result.try_get::<Vec<String>, _>(0).unwrap(), expected);
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn changing_crud_id_types_cannot_commit_unrelated_row_writes() {
        let Some(mut connection) = connection().await else {
            return;
        };
        for operation in ["delete", "update", "delete_many"] {
            let table = format!("gonvex_parameter_{operation}");
            sqlx::query(&format!(
                "CREATE TEMP TABLE {table} (id bigint PRIMARY KEY,v text)"
            ))
            .execute(&mut connection)
            .await
            .unwrap();
            sqlx::query(&format!("INSERT INTO {table} VALUES (0,'seed'),(1,'seed'),(7017280452245743464,'seed'),(4702394921427289928,'seed')"))
                .execute(&mut connection).await.unwrap();
            let (statement, first, second) = match operation {
                "delete" => (format!("DELETE FROM {table} WHERE id=$1"), vec![Value::from(0)], vec![Value::from("abcdefgh")]),
                "update" => (format!("UPDATE {table} AS target SET v=input.v FROM jsonb_populate_record(NULL::{table}, $1::jsonb) AS input WHERE target.id=$2 RETURNING target.*"), vec![serde_json::json!({"v":"valid"}),Value::from(0)], vec![serde_json::json!({"v":"wrong"}),Value::from("abcdefgh")]),
                _ => (format!("DELETE FROM {table} WHERE id IN ($1,$2)"), vec![Value::from(0),Value::from(1)], vec![Value::from("abcdefgh"),Value::from("ABCDEFGH")]),
            };
            let mut transaction = connection.begin().await.unwrap();
            let parameters = Parameters::scalars(&first).unwrap();
            let key = parameters.statement(&statement);
            parameters
                .bind(sqlx::query(key.as_ref()))
                .execute(&mut *transaction)
                .await
                .unwrap();
            let parameters = Parameters::scalars(&second).unwrap();
            let key = parameters.statement(&statement);
            let error = parameters
                .bind(sqlx::query(key.as_ref()))
                .execute(&mut *transaction)
                .await
                .unwrap_err();
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("42883")
            );
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
