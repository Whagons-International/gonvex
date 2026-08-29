//! Trusted host operations for TypeScript module invocations.
//!
//! The V8 isolate can ask for database work, but it never receives a pool,
//! transaction, URL, or credential. Query calls share one read-only snapshot;
//! Reducer calls share one host-owned transaction that the caller commits only
//! after the JavaScript handler returns successfully.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use gonvex_module_host::protocol::HostCallFrame;
use gonvex_postgres::TenantTransaction;
use serde_json::{Map, Value};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::types::Json;
use sqlx::{Column, Either, Executor, Postgres, Row, TypeInfo, ValueRef};
use uuid::Uuid;

use crate::module_host::HostCallHandler;

const DEFAULT_KEY: &str = "id";
pub(crate) const SCHEDULE_OUTBOX_PATH: &str = "_gonvex.scheduler.enqueue";

#[derive(Clone, Debug)]
struct TableKey {
    column: String,
    data_type: Option<String>,
    database_generated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseCapability {
    Query,
    Reducer,
}

pub struct DatabaseHostCalls {
    transaction: Option<TenantTransaction>,
    capability: DatabaseCapability,
    schema: Value,
    table_keys: BTreeMap<String, TableKey>,
    schedulable_functions: BTreeSet<String>,
    actor_account_id: String,
    actor_email: String,
    provenance: Value,
}

impl DatabaseHostCalls {
    pub fn new(transaction: TenantTransaction, capability: DatabaseCapability) -> Self {
        Self {
            transaction: Some(transaction),
            capability,
            schema: Value::Null,
            table_keys: BTreeMap::new(),
            schedulable_functions: BTreeSet::new(),
            actor_account_id: String::new(),
            actor_email: String::new(),
            provenance: Value::Null,
        }
    }

    pub fn with_provenance(
        mut self,
        provenance: &gonvex_module_runtime::InvocationProvenance,
    ) -> Self {
        self.provenance = serde_json::to_value(provenance).unwrap_or(Value::Null);
        self
    }

    pub fn with_actor(mut self, account_id: &str, email: &str) -> Self {
        self.actor_account_id = account_id.to_owned();
        self.actor_email = email.to_owned();
        self
    }

    pub fn with_schema(mut self, schema: &Value) -> Self {
        self.schema = schema.clone();
        self
    }

    pub fn with_schedulable_functions(
        mut self,
        functions: impl IntoIterator<Item = String>,
    ) -> Self {
        self.schedulable_functions.extend(functions);
        self
    }

    pub async fn finish(mut self, success: bool) -> Result<(), String> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| "the invocation transaction is already closed".to_owned())?;
        if self.capability == DatabaseCapability::Reducer && success {
            transaction
                .commit()
                .await
                .map_err(|error| error.to_string())
        } else {
            transaction
                .rollback()
                .await
                .map_err(|error| error.to_string())
        }
    }

    fn transaction(&mut self) -> Result<&mut TenantTransaction, String> {
        self.transaction
            .as_mut()
            .ok_or_else(|| "the invocation transaction is closed".to_owned())
    }

    pub fn transaction_mut(&mut self) -> &mut TenantTransaction {
        self.transaction
            .as_mut()
            .expect("invocation transaction stays open until finish")
    }

    fn require_write(&self) -> Result<(), String> {
        if self.capability == DatabaseCapability::Reducer {
            Ok(())
        } else {
            Err("a Query cannot modify application tables".to_owned())
        }
    }
}

#[async_trait]
impl HostCallHandler for DatabaseHostCalls {
    async fn handle(&mut self, call: HostCallFrame) -> Result<Value, String> {
        match call {
            HostCallFrame::DbQuery {
                statement,
                parameters,
            } => self.query(&statement, parameters).await,
            HostCallFrame::DbInsert { table, row } => {
                self.require_write()?;
                self.insert(&table, row).await
            }
            HostCallFrame::DbUpdate {
                table,
                key,
                id,
                patch,
            } => {
                self.require_write()?;
                self.update(&table, &key, id, patch).await
            }
            HostCallFrame::DbDelete { table, key, id } => {
                self.require_write()?;
                self.delete(&table, &key, id).await
            }
            HostCallFrame::DbDeleteMany { table, key, ids } => {
                self.require_write()?;
                self.delete_many(&table, &key, ids).await
            }
            HostCallFrame::ActionEnqueue { function, args } => {
                self.require_write()?;
                let function = function.trim();
                if function.is_empty() {
                    return Err("actions.enqueue requires an Action path".to_owned());
                }
                if function == SCHEDULE_OUTBOX_PATH {
                    return Err(
                        "actions.enqueue cannot invoke a Gonvex system operation".to_owned()
                    );
                }
                let account_id = self.actor_account_id.clone();
                let email = self.actor_email.clone();
                let provenance = self.provenance.clone();
                let id = self
                    .transaction()?
                    .enqueue_action(function, &args, &account_id, &email, &provenance)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(Value::String(id))
            }
            HostCallFrame::ScheduleAfter {
                delay_ms,
                function,
                args,
            } => {
                self.require_write()?;
                let delay = i64::try_from(delay_ms)
                    .map_err(|_| "scheduler delayMs is outside the supported range".to_owned())?;
                let run_at = Utc::now()
                    .checked_add_signed(chrono::Duration::milliseconds(delay))
                    .ok_or_else(|| "scheduler delayMs is outside the supported range".to_owned())?;
                self.enqueue_scheduled(&function, args, run_at).await
            }
            HostCallFrame::ScheduleAt {
                at_unix_ms,
                function,
                args,
            } => {
                self.require_write()?;
                let run_at = i64::try_from(at_unix_ms)
                    .ok()
                    .and_then(DateTime::from_timestamp_millis)
                    .ok_or_else(|| {
                        "scheduler atUnixMs is outside the supported range".to_owned()
                    })?;
                self.enqueue_scheduled(&function, args, run_at).await
            }
            _ => Err(match self.capability {
                DatabaseCapability::Query => {
                    "a Query may only use its read-only database capability".to_owned()
                }
                DatabaseCapability::Reducer => {
                    "this Reducer host capability is not implemented in the Rust runtime yet"
                        .to_owned()
                }
            }),
        }
    }
}

impl DatabaseHostCalls {
    async fn enqueue_scheduled(
        &mut self,
        function: &str,
        args: Value,
        run_at: DateTime<Utc>,
    ) -> Result<Value, String> {
        let function = function.trim();
        if !self.schedulable_functions.contains(function) {
            return Err(format!(
                "scheduled function {function:?} is not a registered Reducer or Action"
            ));
        }
        let job_id = format!("job_{}", Uuid::new_v4());
        let payload = serde_json::json!({
            "jobId": job_id,
            "function": function,
            "args": args,
            "runAtUnixMs": run_at.timestamp_millis(),
        });
        let account_id = self.actor_account_id.clone();
        let email = self.actor_email.clone();
        let provenance = self.provenance.clone();
        self.transaction()?
            .enqueue_action(
                SCHEDULE_OUTBOX_PATH,
                &payload,
                &account_id,
                &email,
                &provenance,
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(Value::String(job_id))
    }

    async fn query(&mut self, statement: &str, parameters: Value) -> Result<Value, String> {
        require_single_statement(statement)?;
        if self.capability == DatabaseCapability::Query {
            require_read_statement(statement)?;
        }
        let parameters = match parameters {
            Value::Null => Vec::new(),
            Value::Array(values) => values,
            _ => return Err("query parameters must be an array".to_owned()),
        };
        let parameter_types = if parameters.iter().any(Value::is_array) {
            let description = (&mut **self.transaction()?.transaction())
                .describe(statement.trim())
                .await
                .map_err(|error| {
                    format!("could not resolve PostgreSQL parameter types: {error}")
                })?;
            match description.parameters() {
                Some(Either::Left(types)) => types
                    .iter()
                    .map(|type_info| type_info.name().to_owned())
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        };
        let mut query = sqlx::query(statement.trim());
        for (index, value) in parameters.iter().enumerate() {
            query = bind_query_value(query, value, parameter_types.get(index).map(String::as_str))
                .map_err(|error| format!("parameter ${}: {error}", index + 1))?;
        }
        let transaction = self.transaction()?.transaction();
        let rows = query
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| error.to_string())?;
        rows_to_json(rows)
    }

    async fn insert(&mut self, table: &str, row: Value) -> Result<Value, String> {
        let row = object(row, "row")?;
        if row.is_empty() {
            return Err("an insert requires at least one column".to_owned());
        }
        let mut values: BTreeMap<String, Value> = row.into_iter().collect();
        let key = self.catalog_table_key(table).await?;
        if !values.contains_key(&key.column) && !key.database_generated {
            match key.data_type.as_deref() {
                Some("text" | "character varying" | "character") => {
                    values.insert(
                        key.column.clone(),
                        Value::String(Uuid::new_v4().to_string()),
                    );
                }
                Some(data_type) => {
                    return Err(format!(
                        "insert into {table:?} requires primary key {:?}; Gonvex can only allocate omitted text keys, but this column is {data_type}",
                        key.column,
                    ));
                }
                None => {}
            }
        }
        let table = quote_identifier(table)?;
        let columns = values
            .keys()
            .map(|column| quote_identifier(column))
            .collect::<Result<Vec<_>, _>>()?;
        let select_columns = columns
            .iter()
            .map(|column| format!("input.{column}"))
            .collect::<Vec<_>>();
        let statement = format!(
            "INSERT INTO {table} ({}) SELECT {} FROM jsonb_populate_record(NULL::{table}, $1::jsonb) AS input RETURNING *",
            columns.join(", "),
            select_columns.join(", ")
        );
        let payload = Value::Object(values.into_iter().collect());
        let query = sqlx::query(&statement).bind(Json(payload));
        let transaction = self.transaction()?.transaction();
        let row = query
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| error.to_string())?;
        row.map(row_to_json)
            .transpose()
            .map(|row| row.unwrap_or(Value::Null))
    }

    async fn update(
        &mut self,
        table: &str,
        key: &str,
        id: Value,
        patch: Value,
    ) -> Result<Value, String> {
        let key = self.resolve_table_key(table, key).await?;
        let table = quote_identifier(table)?;
        require_row_id(&id)?;
        let values: BTreeMap<String, Value> = object(patch, "patch")?.into_iter().collect();
        if values.is_empty() {
            return Err("an update requires at least one column".to_owned());
        }
        let assignments = values
            .keys()
            .map(|column| {
                let column = quote_identifier(column)?;
                Ok(format!("{column} = input.{column}"))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let statement = format!(
            "UPDATE {table} AS target SET {} FROM jsonb_populate_record(NULL::{table}, $1::jsonb) AS input WHERE target.{key} = $2 RETURNING target.*",
            assignments.join(", ")
        );
        let payload = Value::Object(values.into_iter().collect());
        let mut query = sqlx::query(&statement).bind(Json(payload));
        query = bind_value(query, &id)?;
        let transaction = self.transaction()?.transaction();
        let row = query
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| error.to_string())?;
        row.map(row_to_json)
            .transpose()
            .map(|row| row.unwrap_or(Value::Null))
    }

    async fn delete(&mut self, table: &str, key: &str, id: Value) -> Result<Value, String> {
        let key = self.resolve_table_key(table, key).await?;
        let table = quote_identifier(table)?;
        require_row_id(&id)?;
        let statement = format!("DELETE FROM {table} WHERE {key} = $1");
        let query = bind_value(sqlx::query(&statement), &id)?;
        let transaction = self.transaction()?.transaction();
        let result = query
            .execute(&mut **transaction)
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({ "deleted": result.rows_affected() }))
    }

    async fn delete_many(&mut self, table: &str, key: &str, ids: Value) -> Result<Value, String> {
        let ids = ids
            .as_array()
            .ok_or_else(|| "deleteMany ids must be an array".to_owned())?;
        if ids.len() > 10_000 {
            return Err("deleteMany cannot delete more than 10000 rows per call".to_owned());
        }
        if ids.is_empty() {
            return Ok(serde_json::json!({ "deleted": 0 }));
        }
        for id in ids {
            require_row_id(id)?;
        }
        let key = self.resolve_table_key(table, key).await?;
        let table = quote_identifier(table)?;
        let parameters = (1..=ids.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!("DELETE FROM {table} WHERE {key} IN ({parameters})");
        let mut query = sqlx::query(&statement);
        for id in ids {
            query = bind_value(query, id)?;
        }
        let transaction = self.transaction()?.transaction();
        let result = query
            .execute(&mut **transaction)
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({ "deleted": result.rows_affected() }))
    }

    async fn resolve_table_key(&mut self, table: &str, requested: &str) -> Result<String, String> {
        let requested = requested.trim();
        let declared = declared_table_key(&self.schema, table)?.map(str::to_owned);
        if !requested.is_empty() {
            if declared
                .as_deref()
                .is_some_and(|declared| requested != declared)
            {
                return Err(format!(
                    "table {table:?} declares primary key {:?}, not {requested:?}",
                    declared.as_deref().unwrap_or_default(),
                ));
            }
            return quote_identifier(requested);
        }
        if let Some(declared) = declared {
            return quote_identifier(&declared);
        }
        if let Some(cached) = self.table_keys.get(table) {
            return quote_identifier(&cached.column);
        }

        let key = self.catalog_table_key(table).await?;
        quote_identifier(&key.column)
    }

    async fn catalog_table_key(&mut self, table: &str) -> Result<TableKey, String> {
        if let Some(cached) = self.table_keys.get(table) {
            return Ok(cached.clone());
        }

        let (schema_name, table_name) = table_catalog_parts(table)?;
        let statement = r#"
            SELECT
              attribute.attname,
              format_type(attribute.atttypid, attribute.atttypmod),
              (attribute.attidentity <> '' OR attribute.atthasdef)
            FROM pg_catalog.pg_index AS index
            JOIN pg_catalog.pg_class AS relation ON relation.oid = index.indrelid
            JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            JOIN unnest(index.indkey) WITH ORDINALITY AS key(attnum, position) ON TRUE
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = relation.oid AND attribute.attnum = key.attnum
            WHERE namespace.nspname = COALESCE($1, current_schema())
              AND relation.relname = $2
              AND index.indisprimary
            ORDER BY key.position
        "#;
        let keys = sqlx::query_as::<_, (String, String, bool)>(statement)
            .bind(schema_name)
            .bind(table_name)
            .fetch_all(&mut **self.transaction()?.transaction())
            .await
            .map_err(|error| error.to_string())?;
        let key = match keys.as_slice() {
            [] => TableKey {
                column: DEFAULT_KEY.to_owned(),
                data_type: None,
                database_generated: true,
            },
            [(column, data_type, database_generated)] => TableKey {
                column: column.clone(),
                data_type: Some(data_type.clone()),
                database_generated: *database_generated,
            },
            _ => {
                return Err(format!(
                    "table {table:?} has a composite primary key, which db.update/delete do not support"
                ));
            }
        };
        self.table_keys.insert(table.to_owned(), key.clone());
        Ok(key)
    }
}

pub(crate) fn bind_value<'query>(
    query: Query<'query, Postgres, PgArguments>,
    value: &Value,
) -> Result<Query<'query, Postgres, PgArguments>, String> {
    Ok(match value {
        Value::Null => query.bind(Option::<String>::None),
        Value::Bool(value) => query.bind(*value),
        Value::String(value) => query.bind(value.clone()),
        Value::Number(value) if value.is_i64() => query.bind(
            value
                .as_i64()
                .ok_or_else(|| "integer is outside PostgreSQL bigint range".to_owned())?,
        ),
        Value::Number(value) if value.is_u64() => {
            let value = value
                .as_u64()
                .and_then(|value| i64::try_from(value).ok())
                .ok_or_else(|| "integer is outside PostgreSQL bigint range".to_owned())?;
            query.bind(value)
        }
        Value::Number(value) => query.bind(
            value
                .as_f64()
                .ok_or_else(|| "number is not representable as a float".to_owned())?,
        ),
        Value::Array(_) | Value::Object(_) => query.bind(Json(value.clone())),
    })
}

fn bind_query_value<'query>(
    query: Query<'query, Postgres, PgArguments>,
    value: &Value,
    postgres_type: Option<&str>,
) -> Result<Query<'query, Postgres, PgArguments>, String> {
    let Value::Array(values) = value else {
        return bind_value(query, value);
    };
    let invalid = |expected: &str| {
        format!("PostgreSQL expects {expected}, but the JSON array contains an incompatible value")
    };
    Ok(match postgres_type {
        Some("_TEXT" | "_VARCHAR" | "_BPCHAR" | "TEXT[]" | "VARCHAR[]" | "CHAR[]") => query.bind(
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("a text array"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some("_INT2" | "SMALLINT[]") => query.bind(
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
        Some("_INT4" | "INTEGER[]") => query.bind(
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
        Some("_INT8" | "BIGINT[]") => query.bind(
            values
                .iter()
                .map(|value| value.as_i64().ok_or_else(|| invalid("a bigint array")))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some("_FLOAT4" | "REAL[]") => query.bind(
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
        Some("_FLOAT8" | "DOUBLE PRECISION[]") => query.bind(
            values
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .ok_or_else(|| invalid("a double precision array"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some("_BOOL" | "BOOLEAN[]") => query.bind(
            values
                .iter()
                .map(|value| value.as_bool().ok_or_else(|| invalid("a boolean array")))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some("_UUID" | "UUID[]") => query.bind(
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
        Some("JSON" | "JSONB") | None => query.bind(Json(value.clone())),
        Some(postgres_type) if postgres_type.starts_with('_') || postgres_type.ends_with("[]") => {
            return Err(format!(
                "PostgreSQL array parameter type {postgres_type} is not supported"
            ));
        }
        Some(postgres_type) => {
            return Err(format!("PostgreSQL expects scalar parameter type {postgres_type}, but received a JSON array"));
        }
    })
}

fn rows_to_json(rows: Vec<PgRow>) -> Result<Value, String> {
    rows.into_iter()
        .map(row_to_json)
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn row_to_json(row: PgRow) -> Result<Value, String> {
    let mut result = Map::new();
    for (index, column) in row.columns().iter().enumerate() {
        result.insert(
            column.name().to_owned(),
            cell_to_json(&row, index, column.type_info().name())?,
        );
    }
    Ok(Value::Object(result))
}

fn cell_to_json(row: &PgRow, index: usize, type_name: &str) -> Result<Value, String> {
    if row
        .try_get_raw(index)
        .map_err(|error| error.to_string())?
        .is_null()
    {
        return Ok(Value::Null);
    }
    let value = match type_name {
        "BOOL" => Value::Bool(row.try_get::<bool, _>(index).map_err(db_decode)?),
        "INT2" => Value::from(row.try_get::<i16, _>(index).map_err(db_decode)?),
        "INT4" => Value::from(row.try_get::<i32, _>(index).map_err(db_decode)?),
        "INT8" => Value::from(row.try_get::<i64, _>(index).map_err(db_decode)?),
        "FLOAT4" => json_float(row.try_get::<f32, _>(index).map_err(db_decode)? as f64)?,
        "FLOAT8" => json_float(row.try_get::<f64, _>(index).map_err(db_decode)?)?,
        "NUMERIC" => Value::String(
            row.try_get::<BigDecimal, _>(index)
                .map_err(db_decode)?
                .to_string(),
        ),
        "JSON" | "JSONB" => row.try_get::<Json<Value>, _>(index).map_err(db_decode)?.0,
        "UUID" => Value::String(
            row.try_get::<Uuid, _>(index)
                .map_err(db_decode)?
                .to_string(),
        ),
        "TIMESTAMPTZ" => Value::String(
            row.try_get::<DateTime<Utc>, _>(index)
                .map_err(db_decode)?
                .to_rfc3339(),
        ),
        "TIMESTAMP" => Value::String(
            row.try_get::<NaiveDateTime, _>(index)
                .map_err(db_decode)?
                .format("%Y-%m-%dT%H:%M:%S%.f")
                .to_string(),
        ),
        "DATE" => Value::String(
            row.try_get::<NaiveDate, _>(index)
                .map_err(db_decode)?
                .to_string(),
        ),
        "TIME" => Value::String(
            row.try_get::<NaiveTime, _>(index)
                .map_err(db_decode)?
                .to_string(),
        ),
        "BYTEA" => {
            Value::String(STANDARD.encode(row.try_get::<Vec<u8>, _>(index).map_err(db_decode)?))
        }
        "_TEXT" | "_VARCHAR" | "_BPCHAR" | "TEXT[]" | "VARCHAR[]" | "CHAR[]" => {
            serde_json::to_value(row.try_get::<Vec<String>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_BOOL" | "BOOLEAN[]" => {
            serde_json::to_value(row.try_get::<Vec<bool>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_INT2" | "SMALLINT[]" => {
            serde_json::to_value(row.try_get::<Vec<i16>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_INT4" | "INTEGER[]" => {
            serde_json::to_value(row.try_get::<Vec<i32>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_INT8" | "BIGINT[]" => {
            serde_json::to_value(row.try_get::<Vec<i64>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_FLOAT4" | "REAL[]" => {
            serde_json::to_value(row.try_get::<Vec<f32>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_FLOAT8" | "DOUBLE PRECISION[]" => {
            serde_json::to_value(row.try_get::<Vec<f64>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_UUID" | "UUID[]" => {
            serde_json::to_value(row.try_get::<Vec<Uuid>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        _ => Value::String(row.try_get::<String, _>(index).map_err(db_decode)?),
    };
    Ok(value)
}

fn db_decode(error: sqlx::Error) -> String {
    error.to_string()
}

fn json_float(value: f64) -> Result<Value, String> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| "PostgreSQL returned a non-finite float".to_owned())
}

fn object(value: Value, name: &str) -> Result<Map<String, Value>, String> {
    match value {
        Value::Object(value) => Ok(value),
        Value::Null => Ok(Map::new()),
        _ => Err(format!("{name} must be an object")),
    }
}

fn require_row_id(value: &Value) -> Result<(), String> {
    if matches!(value, Value::String(_) | Value::Number(_)) {
        Ok(())
    } else {
        Err("a row id must be a string or number".to_owned())
    }
}

fn declared_table_key<'a>(schema: &'a Value, table: &str) -> Result<Option<&'a str>, String> {
    let Some(schema) = schema.as_object() else {
        return Ok(None);
    };
    let table_definition = ["tables", "tenantTables", "controlPlaneTables"]
        .iter()
        .filter_map(|scope| schema.get(*scope).and_then(Value::as_object))
        .find_map(|tables| tables.get(table).and_then(Value::as_object));
    let Some(table_definition) = table_definition else {
        return Ok(None);
    };
    let Some(columns) = table_definition.get("columns").and_then(Value::as_object) else {
        return Ok(None);
    };
    let keys = columns
        .iter()
        .filter_map(|(name, column)| {
            column
                .as_object()
                .and_then(|column| column.get("primaryKey"))
                .and_then(Value::as_bool)
                .filter(|primary| *primary)
                .map(|_| name.as_str())
        })
        .collect::<Vec<_>>();
    match keys.as_slice() {
        [] => Ok(None),
        [key] => Ok(Some(key)),
        _ => Err(format!(
            "table {table:?} declares a composite primary key, which db.update/delete do not support"
        )),
    }
}

fn table_catalog_parts(table: &str) -> Result<(Option<String>, String), String> {
    quote_identifier(table)?;
    let parts = table.trim().split('.').collect::<Vec<_>>();
    Ok(match parts.as_slice() {
        [table] => (None, (*table).to_owned()),
        [schema, table] => (Some((*schema).to_owned()), (*table).to_owned()),
        _ => return Err(format!("identifier {table:?} is not a table name")),
    })
}

fn quote_identifier(name: &str) -> Result<String, String> {
    let name = name.trim();
    let parts = name.split('.').collect::<Vec<_>>();
    if name.is_empty() || parts.len() > 2 || parts.iter().any(|part| !valid_identifier(part)) {
        return Err(format!("identifier {name:?} is not a table or column name"));
    }
    Ok(parts
        .into_iter()
        .map(|part| format!("\"{part}\""))
        .collect::<Vec<_>>()
        .join("."))
}

fn valid_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }
    name.chars().enumerate().all(|(index, character)| {
        character.is_ascii_alphabetic()
            || character == '_'
            || (index > 0 && (character.is_ascii_digit() || character == '$'))
    })
}

fn require_read_statement(statement: &str) -> Result<(), String> {
    let head = statement
        .trim_start()
        .split(|character: char| character.is_whitespace() || character == '(')
        .find(|part| !part.is_empty())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if matches!(
        head.as_str(),
        "select" | "with" | "values" | "table" | "explain" | "show"
    ) {
        Ok(())
    } else {
        Err(format!(
            "a Query may only read; {head:?} is not a read statement"
        ))
    }
}

fn require_single_statement(statement: &str) -> Result<(), String> {
    let statement = statement.trim();
    if statement.is_empty() {
        return Err("a database statement is required".to_owned());
    }
    let mut single = false;
    let mut double = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let characters = statement.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < characters.len() {
        let current = characters[index];
        let next = characters.get(index + 1).copied().unwrap_or('\0');
        if line_comment {
            line_comment = current != '\n';
        } else if block_comment {
            if current == '*' && next == '/' {
                block_comment = false;
                index += 1;
            }
        } else if single {
            if current == '\'' {
                if next == '\'' {
                    index += 1;
                } else {
                    single = false;
                }
            }
        } else if double {
            if current == '"' {
                double = false;
            }
        } else if current == '-' && next == '-' {
            line_comment = true;
            index += 1;
        } else if current == '/' && next == '*' {
            block_comment = true;
            index += 1;
        } else if current == '\'' {
            single = true;
        } else if current == '"' {
            double = true;
        } else if current == ';'
            && characters[index + 1..]
                .iter()
                .collect::<String>()
                .trim()
                .is_empty()
        {
            // One trailing semicolon is allowed.
        } else if current == ';' {
            return Err("a database statement may not contain more than one statement".to_owned());
        }
        index += 1;
    }
    if single || double || block_comment {
        return Err("a database statement contains an unterminated literal or comment".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry};
    use sqlx::postgres::PgPoolOptions;

    fn scoped_url(base: &str, schema: &str) -> String {
        let separator = if base.contains('?') { '&' } else { '?' };
        format!("{base}{separator}options=-csearch_path%3D{schema}")
    }

    #[test]
    fn identifiers_are_plain_and_bounded() {
        assert_eq!(
            quote_identifier("public.tasks").unwrap(),
            "\"public\".\"tasks\""
        );
        assert!(quote_identifier("tasks; DROP TABLE tasks").is_err());
        assert!(quote_identifier("a.b.c").is_err());
    }

    #[test]
    fn query_statement_rejects_writes_and_batches() {
        assert!(require_read_statement("SELECT * FROM tasks").is_ok());
        assert!(require_read_statement("UPDATE tasks SET title = 'x'").is_err());
        assert!(require_single_statement("SELECT ';'; SELECT 2").is_err());
        assert!(require_single_statement("SELECT ';'").is_ok());
    }

    #[tokio::test]
    async fn resolves_declared_non_id_primary_key_for_omitted_host_key() {
        let schema = serde_json::json!({
            "tables": {
                "tasks": {
                    "columns": {
                        "_id": {"type": "text", "nullable": false, "primaryKey": true},
                        "title": {"type": "text", "nullable": false, "primaryKey": false}
                    },
                    "indexes": {}
                }
            }
        });
        assert_eq!(declared_table_key(&schema, "tasks").unwrap(), Some("_id"));

        let calls = DatabaseHostCalls {
            transaction: None,
            capability: DatabaseCapability::Reducer,
            schema,
            table_keys: BTreeMap::new(),
            schedulable_functions: BTreeSet::new(),
            actor_account_id: String::new(),
            actor_email: String::new(),
            provenance: Value::Null,
        };
        let mut calls = calls;
        assert_eq!(
            calls.resolve_table_key("tasks", "").await.unwrap(),
            "\"_id\""
        );
    }

    #[tokio::test]
    async fn rejects_explicit_key_that_conflicts_with_declared_primary_key() {
        let schema = serde_json::json!({
            "tenantTables": {
                "tasks": {
                    "columns": {
                        "_id": {"type": "text", "nullable": false, "primaryKey": true}
                    }
                }
            }
        });
        let calls = DatabaseHostCalls {
            transaction: None,
            capability: DatabaseCapability::Reducer,
            schema,
            table_keys: BTreeMap::new(),
            schedulable_functions: BTreeSet::new(),
            actor_account_id: String::new(),
            actor_email: String::new(),
            provenance: Value::Null,
        };
        let mut calls = calls;
        assert!(calls.resolve_table_key("tasks", "id").await.is_err());
    }

    #[tokio::test]
    async fn binds_values_by_destination_column_type_and_deletes_many_rows() {
        let Some(base_url) = std::env::var("GONVEX_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping array binding test");
            return;
        };
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let schema = format!("gonvex_array_parameters_{nonce}");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await
            .unwrap();
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query(&format!(
            r#"CREATE TABLE "{schema}"."tasks" ("_id" text PRIMARY KEY, "title" text NOT NULL, "metadata" jsonb, "tags" text[], "score" integer)"#
        ))
        .execute(&admin)
        .await
        .unwrap();
        let database_url = scoped_url(&base_url, &schema);
        let control = ControlPlane::connect(
            &database_url,
            PoolRegistry::new(PoolLimits::default()),
            BTreeMap::new(),
        )
        .await
        .unwrap();
        let transaction = control.begin_control_transaction(false).await.unwrap();
        let mut calls = DatabaseHostCalls::new(transaction, DatabaseCapability::Reducer)
            .with_schema(&serde_json::json!({
                "tables": {
                    "tasks": {
                        "columns": {
                            "_id": {"type": "text", "nullable": false, "primaryKey": true},
                            "title": {"type": "text", "nullable": false, "primaryKey": false},
                            "metadata": {"type": "jsonb", "nullable": true, "primaryKey": false},
                            "tags": {"type": "text[]", "nullable": true, "primaryKey": false},
                            "score": {"type": "integer", "nullable": true, "primaryKey": false}
                        }
                    }
                }
            }));

        let array_rows = calls
            .query(
                "SELECT value FROM unnest($1::text[]) AS value ORDER BY value",
                serde_json::json!([["second", "first"]]),
            )
            .await
            .unwrap();
        assert_eq!(
            array_rows,
            serde_json::json!([{"value":"first"},{"value":"second"}])
        );
        let json_row = calls
            .query(
                "SELECT $1::jsonb AS value",
                serde_json::json!([["second", "first"]]),
            )
            .await
            .unwrap();
        assert_eq!(json_row, serde_json::json!([{"value":["second","first"]}]));
        let inserted = calls
            .insert(
                "tasks",
                serde_json::json!({
                    "_id": "first",
                    "title": "first",
                    "metadata": "created",
                    "tags": ["one", "two"],
                    "score": 1
                }),
            )
            .await
            .unwrap();
        assert_eq!(inserted["metadata"], serde_json::json!("created"));
        assert_eq!(inserted["tags"], serde_json::json!(["one", "two"]));
        assert_eq!(inserted["score"], serde_json::json!(1));

        let updated = calls
            .update(
                "tasks",
                "",
                serde_json::json!("first"),
                serde_json::json!({
                    "metadata": false,
                    "tags": ["updated"],
                    "score": 2
                }),
            )
            .await
            .unwrap();
        assert_eq!(updated["metadata"], serde_json::json!(false));
        assert_eq!(updated["tags"], serde_json::json!(["updated"]));
        assert_eq!(updated["score"], serde_json::json!(2));

        for id in ["second", "third"] {
            calls
                .insert("tasks", serde_json::json!({"_id": id, "title": id}))
                .await
                .unwrap();
        }
        assert_eq!(
            calls
                .delete_many("tasks", "", serde_json::json!(["first", "third"]))
                .await
                .unwrap(),
            serde_json::json!({"deleted": 2})
        );
        assert_eq!(
            calls
                .query(
                    "SELECT \"_id\" FROM \"tasks\" ORDER BY \"_id\"",
                    serde_json::json!([]),
                )
                .await
                .unwrap(),
            serde_json::json!([{"_id": "second"}])
        );
        calls.finish(false).await.unwrap();

        sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin)
            .await
            .unwrap();
    }
}
