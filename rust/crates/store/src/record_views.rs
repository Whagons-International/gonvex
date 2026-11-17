//! Logical record snapshots over normalized physical storage. BEFORE triggers
//! save the first public image; finalization reads the committed candidate after
//! all SQL consequences. Both pending images and the change feed are transactional.
use crate::capture::{cell_json, value_cell};
use crate::rows::{quote_identifier, JsonKind};
use crate::DatabaseError;
use libsqlite3_sys as ffi;
use serde_json::{Map, Value};
use sqlx::{Row, SqliteConnection};
use std::collections::HashMap;
use std::ffi::{c_int, CStr, CString};

pub(crate) const VALUE_FUNCTION: &str = "gonvex_record_value";
#[derive(Clone, Debug)]
pub(crate) struct RecordDescriptor {
    pub name: String,
    pub storage_table: String,
    pub key_column: String,
    pub schema: Value,
}
pub(crate) async fn descriptors(
    connection: &mut SqliteConnection,
) -> Result<HashMap<String, RecordDescriptor>, DatabaseError> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_views' AND type='table')").fetch_one(&mut *connection).await?;
    if !exists {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(
        "SELECT name,storage_table,key_column,schema FROM _gonvex_record_views ORDER BY name",
    )
    .fetch_all(connection)
    .await?;
    rows.into_iter()
        .map(|row| {
            let schema = serde_json::from_str(&row.get::<String, _>(3))
                .map_err(|error| DatabaseError::Decode(error.to_string()))?;
            let record = RecordDescriptor {
                name: row.get(0),
                storage_table: row.get(1),
                key_column: row.get(2),
                schema,
            };
            Ok((record.storage_table.clone(), record))
        })
        .collect()
}
struct Statement(*mut ffi::sqlite3_stmt);
impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_finalize(self.0);
        }
    }
}

/// Called by a SQL function in a BEFORE trigger, never from a pre-update hook.
/// Statements end before returning so they cannot keep the connection alive.
pub(crate) unsafe fn image(
    db: *mut ffi::sqlite3,
    record: &RecordDescriptor,
    key: &str,
) -> Result<Option<Value>, String> {
    let columns = record.schema["columns"]
        .as_object()
        .ok_or("record schema has no columns")?;
    let kinds = columns
        .iter()
        .map(|(name, definition)| {
            Ok((
                name.clone(),
                JsonKind::from_declared(
                    definition["type"]
                        .as_str()
                        .ok_or("record field has no declared type")?,
                ),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let selected = kinds
        .iter()
        .map(|(name, kind)| {
            if *kind == JsonKind::Json {
                format!("json({})", quote_identifier(name))
            } else {
                quote_identifier(name)
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let public_key = record.schema["key"]
        .as_str()
        .ok_or("record schema has no public key")?;
    let sql = CString::new(format!(
        "SELECT {selected} FROM {} WHERE {}=?1",
        quote_identifier(&record.name),
        quote_identifier(public_key)
    ))
    .map_err(|_| "record SQL contains NUL")?;
    let mut raw = std::ptr::null_mut();
    let result = ffi::sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut raw, std::ptr::null_mut());
    let statement = Statement(raw);
    let error = || {
        CStr::from_ptr(ffi::sqlite3_errmsg(db))
            .to_string_lossy()
            .into_owned()
    };
    if result != ffi::SQLITE_OK {
        return Err(error());
    }
    if ffi::sqlite3_stmt_readonly(raw) == 0 {
        return Err("record snapshots must be read-only".into());
    }
    let result = ffi::sqlite3_bind_text(
        raw,
        1,
        key.as_ptr().cast(),
        key.len()
            .try_into()
            .map_err(|_| "record key is too large")?,
        ffi::SQLITE_TRANSIENT(),
    );
    if result != ffi::SQLITE_OK {
        return Err(error());
    }
    match ffi::sqlite3_step(raw) {
        ffi::SQLITE_DONE => return Ok(None),
        ffi::SQLITE_ROW => {}
        _ => return Err(error()),
    }
    let mut object = Map::new();
    for (index, (name, kind)) in kinds.into_iter().enumerate() {
        object.insert(
            name,
            cell_json(
                &value_cell(ffi::sqlite3_column_value(raw, index as c_int)),
                kind,
            ),
        );
    }
    let result = ffi::sqlite3_step(raw);
    if result != ffi::SQLITE_DONE {
        return Err(if result == ffi::SQLITE_ROW {
            "a logical record view returned duplicate keys".into()
        } else {
            error()
        });
    }
    drop(statement);
    Ok(Some(Value::Object(object)))
}

/// One first-before image per logical record, even when several owned tables
/// change. The deferred guard makes a missing finalization fail at COMMIT.
pub(crate) fn capture_triggers_sql(
    storage: &str,
    key: &str,
    name: &str,
    integer_key: bool,
) -> String {
    let [insert, update, delete] =
        crate::feed::capture_trigger_names(storage).map(|name| quote_identifier(&name));
    let public = |row: &str| {
        if integer_key {
            format!(
                "(SELECT value FROM _gonvex_identity_keys WHERE _id={row}.{})",
                quote_identifier(key)
            )
        } else {
            format!("CAST({row}.{} AS TEXT)", quote_identifier(key))
        }
    };
    let literal = name.replace('\'', "''");
    let stage = |row: &str| {
        let key = public(row);
        format!("INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT '{literal}',{key},{VALUE_FUNCTION}('{literal}',{key}) WHERE NOT EXISTS(SELECT 1 FROM _gonvex_record_pending WHERE table_name='{literal}' AND row_id={key});")
    };
    format!("DROP TRIGGER IF EXISTS {insert};\nDROP TRIGGER IF EXISTS {update};\nDROP TRIGGER IF EXISTS {delete};\nCREATE TRIGGER {insert} BEFORE INSERT ON {} BEGIN\n{}\nEND;\nCREATE TRIGGER {update} BEFORE UPDATE ON {} BEGIN\n{}\nEND;\nCREATE TRIGGER {delete} BEFORE DELETE ON {} BEGIN\n{}\nEND;\n",quote_identifier(storage),stage("NEW"),quote_identifier(storage),stage("OLD"),quote_identifier(storage),stage("OLD"))
}

pub(crate) const FLUSH_SQL: &str = r#"
INSERT INTO _gonvex_sync_changes(table_name,row_id,operation,new_value)
WITH resolved AS MATERIALIZED (
 SELECT table_name,row_id,old_value,gonvex_record_value(table_name,row_id) new_value FROM _gonvex_record_pending
)
SELECT table_name,row_id,CASE WHEN old_value IS NULL THEN 'insert' WHEN new_value IS NULL THEN 'delete' ELSE 'update' END,
 json_array(json(old_value),json(new_value),json(CASE WHEN old_value IS NULL OR new_value IS NULL THEN '[]' ELSE
 (SELECT json_group_array(k.key) FROM json_each(new_value) k
  WHERE json_type(old_value,'$.'||json_quote(k.key)) IS NOT json_type(new_value,'$.'||json_quote(k.key))
     OR json_extract(old_value,'$.'||json_quote(k.key)) IS NOT json_extract(new_value,'$.'||json_quote(k.key))) END))
FROM resolved WHERE old_value IS NOT new_value;
DELETE FROM _gonvex_record_pending;
"#;

pub(crate) async fn flush(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_pending' AND type='table')").fetch_one(&mut *connection).await?;
    if exists {
        crate::execute_script(connection, FLUSH_SQL).await?;
    }
    Ok(())
}

pub(crate) fn flush_direct(connection: &mut crate::direct::Conn<'_>) -> Result<(), sqlx::Error> {
    if connection.query_i64("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_pending' AND type='table')",&[])?==Some(1){
  let insert=FLUSH_SQL.strip_suffix("DELETE FROM _gonvex_record_pending;\n").expect("record flush has a cleanup statement");
  connection.execute(insert,&[])?;
  connection.execute("DELETE FROM _gonvex_record_pending",&[])?;
 }
    Ok(())
}

pub(crate) fn owned_capture_triggers_sql(
    source: &str,
    source_key: &str,
    owner: &RecordDescriptor,
    column: &str,
    inbound: bool,
    integer_key: bool,
) -> String {
    owned_capture_triggers_sql_named(
        source,
        source_key,
        owner,
        column,
        inbound,
        integer_key,
        None,
    )
}
pub(crate) fn owned_capture_triggers_sql_named(
    source: &str,
    source_key: &str,
    owner: &RecordDescriptor,
    column: &str,
    inbound: bool,
    integer_key: bool,
    dependency: Option<(&str, &[String])>,
) -> String {
    let (namespace, watch) =
        dependency.map_or((None, &[][..]), |(name, watch)| (Some(name), watch));
    let names = match namespace {
        None => crate::feed::capture_trigger_names(source),
        Some(namespace) => ["insert", "update", "delete"]
            .map(|event| format!("gonvex_record_dependency_{namespace}_{event}")),
    }
    .map(|name| quote_identifier(&name));
    let literal = owner.name.replace('\'', "''");
    let stage = |row: &str| {
        let private = if inbound {
            format!("r.{}", quote_identifier(&owner.key_column))
        } else {
            format!("{row}.{}", quote_identifier(column))
        };
        let public = if integer_key {
            format!("(SELECT value FROM _gonvex_identity_keys WHERE _id={private})")
        } else {
            format!("CAST({private} AS TEXT)")
        };
        let from = if inbound {
            format!(" FROM {} r", quote_identifier(&owner.storage_table))
        } else {
            String::new()
        };
        let predicate = if inbound {
            format!(
                "r.{}={row}.{} AND ",
                quote_identifier(column),
                quote_identifier(source_key)
            )
        } else {
            String::new()
        };
        format!("INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT '{literal}',{public},{VALUE_FUNCTION}('{literal}',{public}){from} WHERE {private} IS NOT NULL AND {predicate}NOT EXISTS(SELECT 1 FROM _gonvex_record_pending WHERE table_name='{literal}' AND row_id={public});")
    };
    let update = if watch.is_empty() {
        "UPDATE".to_owned()
    } else {
        format!(
            "UPDATE OF {}",
            watch
                .iter()
                .map(|c| quote_identifier(c))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let update_when = if watch.is_empty() {
        String::new()
    } else {
        format!(
            " WHEN {}",
            watch
                .iter()
                .map(|c| format!("NEW.{0} IS NOT OLD.{0}", quote_identifier(c)))
                .collect::<Vec<_>>()
                .join(" OR ")
        )
    };
    let events = [
        ("INSERT".to_owned(), stage("NEW")),
        (update, format!("{} {}", stage("OLD"), stage("NEW"))),
        ("DELETE".to_owned(), stage("OLD")),
    ];
    events.into_iter().zip(names).map(|((event,body),name)|format!("DROP TRIGGER IF EXISTS {name};\nCREATE TRIGGER {name} BEFORE {event} ON {}{} BEGIN\n{body}\nEND;\n",quote_identifier(source),if event.starts_with("UPDATE"){update_when.as_str()}else{""})).collect()
}
