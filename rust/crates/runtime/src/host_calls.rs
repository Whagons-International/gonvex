//! Trusted host operations for TypeScript module invocations.
//!
//! The V8 isolate can ask for database work, but it never receives a pool,
//! transaction, URL, or credential. Query calls share one read-only snapshot;
//! Reducer calls share one host-owned transaction that the caller commits only
//! after the JavaScript handler returns successfully.

use std::collections::BTreeMap;

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
use sqlx::{Column, Postgres, Row, TypeInfo, ValueRef};
use uuid::Uuid;

use crate::module_host::HostCallHandler;

const DEFAULT_KEY: &str = "id";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseCapability {
    Query,
    Reducer,
}

pub struct DatabaseHostCalls {
    transaction: Option<TenantTransaction>,
    capability: DatabaseCapability,
    actor_account_id: String,
    actor_email: String,
    provenance: Value,
}

impl DatabaseHostCalls {
    pub fn new(transaction: TenantTransaction, capability: DatabaseCapability) -> Self {
        Self {
            transaction: Some(transaction),
            capability,
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
            HostCallFrame::ActionEnqueue { function, args } => {
                self.require_write()?;
                let function = function.trim();
                if function.is_empty() {
                    return Err("actions.enqueue requires an Action path".to_owned());
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
        let mut query = sqlx::query(statement.trim());
        for (index, value) in parameters.iter().enumerate() {
            query = bind_value(query, value)
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
        let table = quote_identifier(table)?;
        let row = object(row, "row")?;
        if row.is_empty() {
            return Err("an insert requires at least one column".to_owned());
        }
        let values: BTreeMap<String, Value> = row.into_iter().collect();
        let columns = values
            .keys()
            .map(|column| quote_identifier(column))
            .collect::<Result<Vec<_>, _>>()?;
        let placeholders = (1..=values.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>();
        let statement = format!(
            "INSERT INTO {table} ({}) VALUES ({}) RETURNING *",
            columns.join(", "),
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&statement);
        for value in values.values() {
            query = bind_value(query, value)?;
        }
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
        let table = quote_identifier(table)?;
        let key = quote_identifier(if key.trim().is_empty() {
            DEFAULT_KEY
        } else {
            key
        })?;
        require_row_id(&id)?;
        let values: BTreeMap<String, Value> = object(patch, "patch")?.into_iter().collect();
        if values.is_empty() {
            return Err("an update requires at least one column".to_owned());
        }
        let assignments = values
            .keys()
            .enumerate()
            .map(|(index, column)| {
                quote_identifier(column).map(|column| format!("{column} = ${}", index + 1))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let statement = format!(
            "UPDATE {table} SET {} WHERE {key} = ${} RETURNING *",
            assignments.join(", "),
            values.len() + 1
        );
        let mut query = sqlx::query(&statement);
        for value in values.values() {
            query = bind_value(query, value)?;
        }
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
        let table = quote_identifier(table)?;
        let key = quote_identifier(if key.trim().is_empty() {
            DEFAULT_KEY
        } else {
            key
        })?;
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
        "_TEXT" | "_VARCHAR" => {
            serde_json::to_value(row.try_get::<Vec<String>, _>(index).map_err(db_decode)?)
                .map_err(|error| error.to_string())?
        }
        "_INT4" => serde_json::to_value(row.try_get::<Vec<i32>, _>(index).map_err(db_decode)?)
            .map_err(|error| error.to_string())?,
        "_INT8" => serde_json::to_value(row.try_get::<Vec<i64>, _>(index).map_err(db_decode)?)
            .map_err(|error| error.to_string())?,
        "_UUID" => serde_json::to_value(row.try_get::<Vec<Uuid>, _>(index).map_err(db_decode)?)
            .map_err(|error| error.to_string())?,
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
}
