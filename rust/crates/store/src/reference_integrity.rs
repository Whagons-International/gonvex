//! Tenant-local declared reference integrity on SQLite.
//!
//! Applications own the catalog and deletion policy. Every declared table gets
//! `AFTER INSERT/UPDATE/DELETE` triggers that stage row images in
//! `_gonvex_reference_pending`. [`flush`] (called by `TenantTransaction`
//! before `COMMIT` and by the migration runner) evaluates the declarations in
//! Rust, maintains the edge and target-token ledger, and lets SQLite's native
//! deferred foreign key from edges to targets reject a dangling reference at
//! `COMMIT`. Staged rows reference an always-empty guard table through another
//! deferred FK, so a writer that never flushes cannot commit at all.
//!
//! Migrations declare references with
//! `INSERT INTO _gonvex_reference_requests(declaration) VALUES('{…}')`; the
//! runner applies requests in order inside the migration transaction. Rust
//! callers can use [`declare`] directly. `SELECT * FROM
//! gonvex_reference_violations` lists inventoried legacy violations.

mod jsonpath;

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};
use sqlx::{Connection, Row, SqliteConnection};

use self::jsonpath::JsonPath;
use crate::provision::{application_tables, column_exists, table_exists, BoxFuture};
use crate::rows::{json_object_expr, quote_identifier, table_columns, ColumnInfo};
use crate::DatabaseError;

const STATES: [&str; 2] = ["exists", "live"];
const CONFIGURATION_PREFIX: &str = "$configuration:";
const EXCLUSION_KINDS: [&str; 8] = [
    "external",
    "enum",
    "local",
    "content",
    "snapshot",
    "derived",
    "lifecycle",
    "identity",
];

fn violation(message: impl Into<String>) -> DatabaseError {
    DatabaseError::ReferenceIntegrity(message.into())
}

/// Creates the ledger tables. Idempotent.
pub(crate) fn install(
    connection: &mut SqliteConnection,
) -> BoxFuture<'_, Result<(), DatabaseError>> {
    Box::pin(async move {
        let mut transaction = connection.begin().await?;
        crate::execute_script(
            &mut *transaction,
            include_str!("reference_integrity/schema.sql"),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    })
}

/// Runs after each tenant migration script inside its transaction: applies
/// declaration requests, regenerates staging triggers for changed columns,
/// processes staged rows and enforces opted-in storage coverage.
pub(crate) fn after_migration(
    connection: &mut SqliteConnection,
) -> BoxFuture<'_, Result<(), DatabaseError>> {
    Box::pin(after_migration_inner(connection))
}

async fn after_migration_inner(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    let requests =
        sqlx::query("SELECT seq, declaration FROM _gonvex_reference_requests ORDER BY seq")
            .fetch_all(&mut *connection)
            .await?;
    for request in &requests {
        let text: String = request.get(1);
        let catalog: Value = serde_json::from_str(&text)
            .map_err(|error| violation(format!("invalid reference catalog JSON: {error}")))?;
        declare(connection, &catalog).await?;
    }
    if !requests.is_empty() {
        sqlx::query("DELETE FROM _gonvex_reference_requests")
            .execute(&mut *connection)
            .await?;
    }
    refresh_triggers(connection).await?;
    flush(connection).await?;
    let catalog: Option<String> = sqlx::query_scalar(
        "SELECT declaration FROM _gonvex_reference_catalog WHERE json_type(declaration,'$.exclusions') IS NOT NULL",
    )
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(catalog) = catalog {
        let catalog: Value =
            serde_json::from_str(&catalog).map_err(|error| violation(error.to_string()))?;
        check_coverage(connection, &catalog).await?;
    }
    Ok(())
}

/// Processes staged row changes. Call inside the writing transaction before
/// `COMMIT`; `TenantTransaction::commit` does. A no-op on databases without a
/// reference ledger or with nothing staged.
pub fn flush(connection: &mut SqliteConnection) -> BoxFuture<'_, Result<(), DatabaseError>> {
    Box::pin(flush_inner(connection))
}

async fn flush_inner(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    if !table_exists(connection, "_gonvex_reference_pending").await? {
        return Ok(());
    }
    let pending = sqlx::query(
        "SELECT seq, table_name, old_doc, new_doc FROM _gonvex_reference_pending ORDER BY seq",
    )
    .fetch_all(&mut *connection)
    .await?;
    let Some(last) = pending.last().map(|row| row.get::<i64, _>(0)) else {
        return Ok(());
    };
    let ledger = Ledger::load(connection).await?;
    for row in &pending {
        let table: String = row.get(1);
        let parse = |index: usize| -> Result<Option<Value>, DatabaseError> {
            row.get::<Option<String>, _>(index)
                .map(|text| serde_json::from_str(&text))
                .transpose()
                .map_err(|error| violation(format!("invalid staged row image: {error}")))
        };
        let (old, new) = (parse(2)?, parse(3)?);
        ledger.changed(connection, &table, old, new).await?;
    }
    sqlx::query("DELETE FROM _gonvex_reference_pending WHERE seq <= $1")
        .bind(last)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// Installs (or replaces) the reference catalog. Runs in a savepoint, so it
/// may be called inside an open transaction; the whole declaration applies or
/// none of it does.
pub fn declare<'a>(
    connection: &'a mut SqliteConnection,
    catalog: &'a Value,
) -> BoxFuture<'a, Result<(), DatabaseError>> {
    Box::pin(async move {
        let mut savepoint = connection.begin().await?;
        declare_in(&mut savepoint, catalog).await?;
        savepoint.commit().await?;
        Ok(())
    })
}

fn text_of(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

fn str_field<'a>(object: &'a Value, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str)
}

/// `->>` text of a field, including non-string scalars.
fn field_text(object: &Value, key: &str) -> Option<String> {
    text_of(object.get(key))
}

fn reason_ok(object: &Value, key: &str) -> bool {
    field_text(object, key).is_some_and(|reason| reason.trim().chars().count() >= 12)
}

fn parse_path(source: &str) -> Result<JsonPath, DatabaseError> {
    JsonPath::parse(source).map_err(|error| violation(format!("invalid JSON path: {error}")))
}

fn binding_id(row_id: &str, version: Option<i64>) -> String {
    Value::Array(vec![
        Value::String(row_id.to_owned()),
        version.map_or(Value::Null, Value::from),
    ])
    .to_string()
}

fn configuration(document: &Value, columns: &[String]) -> Value {
    Value::Object(
        columns
            .iter()
            .map(|column| {
                (
                    column.clone(),
                    document.get(column).cloned().unwrap_or(Value::Null),
                )
            })
            .collect(),
    )
}

struct TableDecl {
    key: String,
    live: JsonPath,
}

struct LookupStep {
    binding: String,
    from: String,
    key: JsonPath,
    when: Option<JsonPath>,
    required: bool,
}

struct Rule {
    name: String,
    source: String,
    state: String,
    declaration: Value,
    at: JsonPath,
    paths: Vec<JsonPath>,
    source_when: Option<JsonPath>,
    lookup: Option<Vec<LookupStep>>,
    discriminator: Option<JsonPath>,
}

impl Rule {
    fn compile(declaration: &Value) -> Result<Self, DatabaseError> {
        let name = field_text(declaration, "name").unwrap_or_default();
        let paths = match declaration.get("paths") {
            Some(Value::Array(paths)) => paths
                .iter()
                .map(|path| {
                    text_of(Some(path))
                        .ok_or_else(|| violation(format!("reference {name} has a null path")))
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => vec![field_text(declaration, "path")
                .ok_or_else(|| violation(format!("reference {name} has a null path")))?],
        };
        let lookup = match declaration.get("lookup") {
            None | Some(Value::Null) => None,
            Some(lookup) => {
                let steps = match lookup {
                    Value::Array(steps) => steps.clone(),
                    other => vec![other.clone()],
                };
                Some(
                    steps
                        .iter()
                        .map(|step| {
                            Ok(LookupStep {
                                binding: field_text(step, "binding").unwrap_or_default(),
                                from: field_text(step, "from")
                                    .unwrap_or_else(|| "container".to_owned()),
                                key: parse_path(&field_text(step, "key").unwrap_or_default())?,
                                when: field_text(step, "when")
                                    .map(|when| parse_path(&when))
                                    .transpose()?,
                                required: step.get("required") == Some(&Value::Bool(true)),
                            })
                        })
                        .collect::<Result<Vec<_>, DatabaseError>>()?,
                )
            }
        };
        Ok(Self {
            source: field_text(declaration, "source").unwrap_or_default(),
            state: field_text(declaration, "state").unwrap_or_default(),
            at: parse_path(&field_text(declaration, "at").unwrap_or_else(|| "$".to_owned()))?,
            paths: paths
                .iter()
                .map(|path| parse_path(path))
                .collect::<Result<_, _>>()?,
            source_when: field_text(declaration, "sourceWhen")
                .map(|path| parse_path(&path))
                .transpose()?,
            lookup,
            discriminator: field_text(declaration, "discriminator")
                .map(|path| parse_path(&path))
                .transpose()?,
            declaration: declaration.clone(),
            name,
        })
    }

    fn allows_numbers(&self) -> bool {
        self.declaration.get("allowNumbers") == Some(&Value::Bool(true))
    }

    fn accepts(&self, identifier: &Value) -> bool {
        identifier.is_string() || identifier.is_number() && self.allows_numbers()
    }
}

struct Binding {
    name: String,
    source: String,
    columns: Vec<String>,
}

/// The installed declaration, loaded once per flush.
struct Ledger {
    tables: BTreeMap<String, TableDecl>,
    rules: Vec<Rule>,
    keys: Vec<(String, String, String)>,
    bindings: Vec<Binding>,
}

impl Ledger {
    async fn load(connection: &mut SqliteConnection) -> Result<Self, DatabaseError> {
        let mut tables = BTreeMap::new();
        for row in
            sqlx::query("SELECT table_name, key_column, live_when FROM _gonvex_reference_tables")
                .fetch_all(&mut *connection)
                .await?
        {
            tables.insert(
                row.get::<String, _>(0),
                TableDecl {
                    key: row.get(1),
                    live: parse_path(&row.get::<String, _>(2))?,
                },
            );
        }
        let mut rules = Vec::new();
        for row in sqlx::query("SELECT declaration FROM _gonvex_reference_rules ORDER BY name")
            .fetch_all(&mut *connection)
            .await?
        {
            let declaration: Value = serde_json::from_str(&row.get::<String, _>(0))
                .map_err(|error| violation(error.to_string()))?;
            rules.push(Rule::compile(&declaration)?);
        }
        let keys = sqlx::query_as(
            "SELECT table_name, token_name, key_column FROM _gonvex_reference_keys ORDER BY table_name, token_name",
        )
        .fetch_all(&mut *connection)
        .await?;
        let mut bindings = Vec::new();
        for row in sqlx::query(
            "SELECT name, source_table, columns_ FROM _gonvex_reference_bindings ORDER BY name",
        )
        .fetch_all(&mut *connection)
        .await?
        {
            bindings.push(Binding {
                name: row.get(0),
                source: row.get(1),
                columns: serde_json::from_str(&row.get::<String, _>(2))
                    .map_err(|error| violation(error.to_string()))?,
            });
        }
        Ok(Self {
            tables,
            rules,
            keys,
            bindings,
        })
    }

    /// One staged row change (the former `_gonvex_reference_changed` trigger).
    async fn changed(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        old: Option<Value>,
        new: Option<Value>,
    ) -> Result<(), DatabaseError> {
        let Some(declared) = self.tables.get(table) else {
            return Ok(());
        };
        let old_id = old.as_ref().and_then(|old| field_text(old, &declared.key));
        let new_id = new.as_ref().and_then(|new| field_text(new, &declared.key));
        let update = old.is_some() && new.is_some();
        if new.is_none() || update && old_id != new_id {
            self.sync_row(connection, table, old_id.as_deref(), None, old.as_ref())
                .await?;
        }
        if new.is_some() {
            let previous = if update && old_id == new_id {
                old.as_ref()
            } else {
                None
            };
            self.sync_row(connection, table, new_id.as_deref(), new.as_ref(), previous)
                .await?;
        }
        Ok(())
    }

    async fn sync_row(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        row_id: Option<&str>,
        document: Option<&Value>,
        previous: Option<&Value>,
    ) -> Result<(), DatabaseError> {
        let desired = self.desired(connection, table, document).await?;
        if let Some(row_id) = row_id {
            // Preserve unchanged edges: an inventoried legacy violation must
            // not be silently repaired or re-enforced by an unrelated update.
            let existing: Vec<(String, String, String, String)> = sqlx::query_as(
                r#"SELECT rule_name, target_table, target_id, target_state FROM _gonvex_reference_edges
                   WHERE source_table=$1 AND source_id=$2"#,
            )
            .bind(table)
            .bind(row_id)
            .fetch_all(&mut *connection)
            .await?;
            for edge in existing.iter().filter(|edge| !desired.contains(*edge)) {
                sqlx::query(
                    r#"DELETE FROM _gonvex_reference_edges WHERE rule_name=$1 AND source_id=$2
                       AND target_table=$3 AND target_id=$4 AND target_state=$5"#,
                )
                .bind(&edge.0)
                .bind(row_id)
                .bind(&edge.1)
                .bind(&edge.2)
                .bind(&edge.3)
                .execute(&mut *connection)
                .await?;
            }
            for edge in &desired {
                sqlx::query(
                    r#"INSERT INTO _gonvex_reference_edges
                       (rule_name,source_table,source_id,target_table,target_id,target_state,
                        enforced_table,enforced_id,enforced_state)
                       VALUES($1,$2,$3,$4,$5,$6,$4,$5,$6) ON CONFLICT DO NOTHING"#,
                )
                .bind(&edge.0)
                .bind(table)
                .bind(row_id)
                .bind(&edge.1)
                .bind(&edge.2)
                .bind(&edge.3)
                .execute(&mut *connection)
                .await?;
            }
        }
        self.sync_targets(connection, table, previous, document)
            .await
    }

    /// Desired edges of one row: (rule, target table, target id, state).
    async fn desired(
        &self,
        connection: &mut SqliteConnection,
        source: &str,
        document: Option<&Value>,
    ) -> Result<BTreeSet<(String, String, String, String)>, DatabaseError> {
        let mut desired = BTreeSet::new();
        let (Some(document), Some(table)) = (document, self.tables.get(source)) else {
            return Ok(desired);
        };
        for rule in self.rules.iter().filter(|rule| rule.source == source) {
            let active = rule
                .source_when
                .as_ref()
                .unwrap_or(&table.live)
                .matches(document, None)
                == Some(true);
            if !active {
                continue;
            }
            for (target_table, target_id) in self.values(connection, document, rule).await? {
                desired.insert((
                    rule.name.clone(),
                    target_table,
                    target_id,
                    rule.state.clone(),
                ));
            }
        }
        Ok(desired)
    }

    /// Selected target identities of one rule (the former
    /// `_gonvex_reference_values`).
    async fn values(
        &self,
        connection: &mut SqliteConnection,
        document: &Value,
        rule: &Rule,
    ) -> Result<Vec<(String, String)>, DatabaseError> {
        let name = &rule.name;
        let mut document = document.clone();
        // Some consumers accept both JSON values and JSON serialized in a
        // text column. Decoding is declared per reference, never guessed.
        if str_field(&rule.declaration, "encoding") == Some("json-or-value") {
            if let Some(column) = str_field(&rule.declaration, "column") {
                if let Some(Value::String(serialized)) = document.get(column) {
                    let parsed: Value = serde_json::from_str(serialized).map_err(|_| {
                        violation(format!("reference {name} has invalid serialized JSON"))
                    })?;
                    document[column] = parsed;
                }
            }
        }
        let mut selected = Vec::new();
        'containers: for container in rule.at.query(&document, None) {
            let mut vars = Map::new();
            vars.insert("entry".to_owned(), container.clone());
            vars.insert("document".to_owned(), document.clone());
            let mut configuration: Option<Value> = None;
            if let Some(steps) = &rule.lookup {
                for step in steps {
                    let lookup_document = match step.from.as_str() {
                        "document" => Some(&document),
                        "configuration" => configuration.as_ref(),
                        _ => Some(&container),
                    };
                    let identifier = lookup_document
                        .and_then(|lookup| step.key.query_first(lookup, Some(&vars)))
                        .filter(|identifier| !identifier.is_null());
                    let Some(identifier) = identifier else {
                        if step.required {
                            return Err(violation(format!(
                                "reference {name} needs a configuration key"
                            )));
                        }
                        continue 'containers;
                    };
                    if !rule.accepts(&identifier) {
                        return Err(violation(format!(
                            "reference {name} needs a string configuration key"
                        )));
                    }
                    let parent_id = text_of(Some(&identifier)).unwrap_or_default();
                    let row = sqlx::query(
                        "SELECT document, version FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2",
                    )
                    .bind(&step.binding)
                    .bind(&parent_id)
                    .fetch_optional(&mut *connection)
                    .await?;
                    let (found, version) = match row {
                        Some(row) => (
                            serde_json::from_str::<Value>(&row.get::<String, _>(0)).ok(),
                            Some(row.get::<i64, _>(1)),
                        ),
                        None => (None, None),
                    };
                    selected.push((
                        format!("{CONFIGURATION_PREFIX}{}", step.binding),
                        binding_id(&parent_id, version),
                    ));
                    configuration = found;
                    // Missing legacy parents remain auditable violations. New
                    // writes fail the native FK, including stale configuration.
                    let applies = configuration.as_ref().is_some_and(|configuration| {
                        step.when
                            .as_ref()
                            .map_or(Some(true), |when| when.matches(configuration, Some(&vars)))
                            == Some(true)
                    });
                    if !applies {
                        continue 'containers;
                    }
                }
            }
            for path in &rule.paths {
                for item in path.query(&container, None) {
                    for identifier in decode(item, str_field(&rule.declaration, "normalize"))? {
                        if identifier.is_null() {
                            continue;
                        }
                        if !rule.accepts(&identifier) {
                            return Err(violation(format!(
                                "reference {name} must contain string IDs"
                            )));
                        }
                        let target_id = text_of(Some(&identifier)).unwrap_or_default();
                        if target_id.is_empty() {
                            if field_text(&rule.declaration, "allowEmpty").as_deref()
                                == Some("true")
                            {
                                continue;
                            }
                            return Err(violation(format!(
                                "reference {name} contains an empty ID"
                            )));
                        }
                        let ignored = rule
                            .declaration
                            .get("ignoreValues")
                            .and_then(Value::as_array)
                            .is_some_and(|values| {
                                values
                                    .iter()
                                    .any(|value| value.as_str() == Some(target_id.as_str()))
                            });
                        if ignored {
                            continue;
                        }
                        let mut destination = field_text(&rule.declaration, "target");
                        if let Some(targets) = rule.declaration.get("targets") {
                            let scope = if rule.lookup.is_some() {
                                configuration.as_ref()
                            } else {
                                Some(&container)
                            };
                            let discriminator = match (scope, &rule.discriminator) {
                                (Some(scope), Some(path)) => path.query_first(scope, Some(&vars)),
                                _ => None,
                            };
                            let Some(Value::String(discriminator)) = discriminator else {
                                return Err(violation(format!(
                                    "reference {name} requires a target discriminator"
                                )));
                            };
                            destination = field_text(targets, &discriminator);
                        }
                        let Some(destination) = destination else {
                            return Err(violation(format!(
                                "reference {name} has an undeclared target type"
                            )));
                        };
                        selected.push((destination, target_id));
                    }
                }
            }
        }
        Ok(selected)
    }

    /// Target tokens a row owns (the former `_gonvex_reference_row_targets`).
    async fn row_targets(
        &self,
        connection: &mut SqliteConnection,
        source: &str,
        document: Option<&Value>,
    ) -> Result<Vec<(String, String, String)>, DatabaseError> {
        let mut targets = Vec::new();
        let (Some(document), Some(table)) = (document, self.tables.get(source)) else {
            return Ok(targets);
        };
        let live = table.live.matches(document, None) == Some(true);
        let states = || STATES.iter().filter(|state| **state == "exists" || live);
        for (_, token, column) in self.keys.iter().filter(|(owner, _, _)| owner == source) {
            if let Some(value) = field_text(document, column) {
                for state in states() {
                    targets.push((token.clone(), value.clone(), (*state).to_owned()));
                }
            }
        }
        let Some(row_id) = field_text(document, &table.key) else {
            return Ok(targets);
        };
        for binding in self
            .bindings
            .iter()
            .filter(|binding| binding.source == source)
        {
            let row = sqlx::query(
                "SELECT document, version FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2",
            )
            .bind(&binding.name)
            .bind(&row_id)
            .fetch_optional(&mut *connection)
            .await?;
            let Some(row) = row else { continue };
            let stored = serde_json::from_str::<Value>(&row.get::<String, _>(0)).ok();
            if stored.as_ref() != Some(&configuration(document, &binding.columns)) {
                continue;
            }
            let id = binding_id(&row_id, Some(row.get(1)));
            for state in states() {
                targets.push((
                    format!("{CONFIGURATION_PREFIX}{}", binding.name),
                    id.clone(),
                    (*state).to_owned(),
                ));
            }
        }
        Ok(targets)
    }

    async fn sync_targets(
        &self,
        connection: &mut SqliteConnection,
        source: &str,
        previous: Option<&Value>,
        document: Option<&Value>,
    ) -> Result<(), DatabaseError> {
        let Some(table) = self.tables.get(source) else {
            return Ok(());
        };
        // Snapshot old tokens before updating the parent projection.
        // Configuration versions never repeat; comparing JSON retains the
        // version across unrelated parent edits.
        let previous_targets = self.row_targets(connection, source, previous).await?;
        let previous_id = previous.and_then(|previous| field_text(previous, &table.key));
        let document_id = document.and_then(|document| field_text(document, &table.key));
        let bindings = self
            .bindings
            .iter()
            .filter(|binding| binding.source == source)
            .collect::<Vec<_>>();
        if let Some(previous_id) = &previous_id {
            if document.is_none() || document_id.as_ref() != Some(previous_id) {
                for binding in &bindings {
                    sqlx::query(
                        "DELETE FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2",
                    )
                    .bind(&binding.name)
                    .bind(previous_id)
                    .execute(&mut *connection)
                    .await?;
                }
            }
        }
        if let (Some(document), Some(document_id)) = (document, &document_id) {
            if previous.is_none() {
                // Installing a formerly missing parent must not reinterpret
                // legacy values whose domain was unknown at inventory.
                for binding in &bindings {
                    let legacy: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM _gonvex_reference_edges WHERE target_table=$1 AND target_id=$2)",
                    )
                    .bind(format!("{CONFIGURATION_PREFIX}{}", binding.name))
                    .bind(binding_id(document_id, None))
                    .fetch_one(&mut *connection)
                    .await?;
                    if legacy {
                        return Err(violation("configuration has unresolved legacy dependents"));
                    }
                }
            }
            for binding in &bindings {
                let projected = configuration(document, &binding.columns);
                let stored: Option<String> = sqlx::query_scalar(
                    "SELECT document FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2",
                )
                .bind(&binding.name)
                .bind(document_id)
                .fetch_optional(&mut *connection)
                .await?;
                let unchanged = stored
                    .as_deref()
                    .and_then(|stored| serde_json::from_str::<Value>(stored).ok())
                    .is_some_and(|stored| stored == projected);
                if !unchanged {
                    let version = next_version(connection).await?;
                    sqlx::query(
                        r#"INSERT INTO _gonvex_reference_configurations(binding_name,row_id,document,version)
                           VALUES($1,$2,$3,$4) ON CONFLICT(binding_name,row_id)
                           DO UPDATE SET document=excluded.document,version=excluded.version"#,
                    )
                    .bind(&binding.name)
                    .bind(document_id)
                    .bind(projected.to_string())
                    .bind(version)
                    .execute(&mut *connection)
                    .await?;
                }
            }
        }
        let mut deltas: BTreeMap<(String, String, String), i64> = BTreeMap::new();
        for target in self.row_targets(connection, source, document).await? {
            *deltas.entry(target).or_default() += 1;
        }
        for target in previous_targets {
            *deltas.entry(target).or_default() -= 1;
        }
        for ((token, row_id, state), delta) in deltas {
            if delta != 0 {
                apply_target_delta(connection, &token, &row_id, &state, delta).await?;
            }
        }
        Ok(())
    }
}

async fn next_version(connection: &mut SqliteConnection) -> Result<i64, DatabaseError> {
    Ok(sqlx::query_scalar(
        "UPDATE _gonvex_reference_sequence SET last_version=last_version+1 WHERE singleton=1 RETURNING last_version",
    )
    .fetch_one(connection)
    .await?)
}

async fn apply_target_delta(
    connection: &mut SqliteConnection,
    token: &str,
    row_id: &str,
    state: &str,
    delta: i64,
) -> Result<(), DatabaseError> {
    if delta > 0 {
        sqlx::query(
            r#"INSERT INTO _gonvex_reference_targets(table_name,row_id,state,owner_count)
               VALUES($1,$2,$3,$4) ON CONFLICT(table_name,row_id,state)
               DO UPDATE SET owner_count=_gonvex_reference_targets.owner_count+excluded.owner_count"#,
        )
        .bind(token)
        .bind(row_id)
        .bind(state)
        .bind(delta)
        .execute(&mut *connection)
        .await?;
        // An inventoried edge whose target now exists becomes enforced, as a
        // NOT VALID foreign key still guards deletion of a restored parent.
        sqlx::query(
            r#"UPDATE _gonvex_reference_edges
               SET enforced_table=target_table,enforced_id=target_id,enforced_state=target_state
               WHERE target_table=$1 AND target_id=$2 AND target_state=$3 AND enforced_table IS NULL"#,
        )
        .bind(token)
        .bind(row_id)
        .bind(state)
        .execute(&mut *connection)
        .await?;
        return Ok(());
    }
    let remaining: Option<i64> = sqlx::query_scalar(
        r#"UPDATE _gonvex_reference_targets SET owner_count=owner_count+$4
           WHERE table_name=$1 AND row_id=$2 AND state=$3 RETURNING owner_count"#,
    )
    .bind(token)
    .bind(row_id)
    .bind(state)
    .bind(delta)
    .fetch_optional(&mut *connection)
    .await?;
    match remaining {
        None => Err(violation("reference target ledger is inconsistent")),
        Some(0) => {
            sqlx::query(
                "DELETE FROM _gonvex_reference_targets WHERE table_name=$1 AND row_id=$2 AND state=$3 AND owner_count=0",
            )
            .bind(token)
            .bind(row_id)
            .bind(state)
            .execute(&mut *connection)
            .await?;
            Ok(())
        }
        Some(_) => Ok(()),
    }
}

/// Explicit adapters for existing selector encodings (the former
/// `_gonvex_reference_decode`). Plain content never opts in.
fn decode(value: Value, normalization: Option<&str>) -> Result<Vec<Value>, DatabaseError> {
    let Some(normalization) = normalization else {
        return Ok(vec![value]);
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    match normalization {
        "task-relation" => {
            return Ok(match value {
                Value::Object(mut object) => object.remove("taskId").into_iter().collect(),
                other => vec![other],
            })
        }
        "csv-or-json-list" => {}
        _ => return Err(violation("unsupported selector normalization")),
    }
    let mut decoded = value.clone();
    if let Value::String(raw) = &value {
        let raw = raw.trim();
        if raw.starts_with('[') {
            if let Ok(parsed @ Value::Array(_)) = serde_json::from_str::<Value>(raw) {
                decoded = parsed;
            }
        }
    }
    let scalar = |item: &Value, container: &str| -> Result<Option<Value>, DatabaseError> {
        match item {
            Value::String(_) | Value::Number(_) | Value::Bool(_) => {
                let raw = text_of(Some(item)).unwrap_or_default();
                let raw = raw.trim();
                Ok((!raw.is_empty()).then(|| Value::String(raw.to_owned())))
            }
            _ => Err(violation(format!(
                "selector {container} needs scalar values"
            ))),
        }
    };
    let mut output = Vec::new();
    match &decoded {
        Value::Array(items) => {
            for item in items {
                output.extend(scalar(item, "list")?);
            }
        }
        Value::Object(items) => {
            for item in items.values() {
                output.extend(scalar(item, "map")?);
            }
        }
        other => {
            for part in text_of(Some(other)).unwrap_or_default().split(',') {
                let part = part.trim();
                if !part.is_empty() {
                    output.push(Value::String(part.to_owned()));
                }
            }
        }
    }
    Ok(output)
}

fn staging_trigger_names(table: &str) -> [String; 3] {
    ["insert", "update", "delete"].map(|operation| format!("_gonvex_reference_{table}_{operation}"))
}

/// The three `CREATE TRIGGER` statements staging `table`'s row images, as
/// SQLite stores them in `sqlite_schema.sql`.
fn staging_triggers(table: &str, columns: &[ColumnInfo]) -> [String; 3] {
    let quoted = quote_identifier(table);
    let literal = table.replace('\'', "''");
    let kinds = columns
        .iter()
        .map(|column| (column.name.clone(), column.kind()))
        .collect::<Vec<_>>();
    let new_image = json_object_expr("NEW", &kinds);
    let old_image = json_object_expr("OLD", &kinds);
    let any_change = crate::feed::any_column_changed_expr(columns);
    let [insert, update, delete] = staging_trigger_names(table).map(|name| quote_identifier(&name));
    [
        format!(
            "CREATE TRIGGER {insert} AFTER INSERT ON {quoted} BEGIN\n  INSERT INTO _gonvex_reference_pending(table_name,old_doc,new_doc) VALUES('{literal}',NULL,{new_image});\nEND"
        ),
        format!(
            "CREATE TRIGGER {update} AFTER UPDATE ON {quoted} WHEN {any_change} BEGIN\n  INSERT INTO _gonvex_reference_pending(table_name,old_doc,new_doc) VALUES('{literal}',{old_image},{new_image});\nEND"
        ),
        format!(
            "CREATE TRIGGER {delete} AFTER DELETE ON {quoted} BEGIN\n  INSERT INTO _gonvex_reference_pending(table_name,old_doc,new_doc) VALUES('{literal}',{old_image},NULL);\nEND"
        ),
    ]
}

async fn drop_staging_triggers(
    connection: &mut SqliteConnection,
    table: &str,
) -> Result<(), DatabaseError> {
    for name in staging_trigger_names(table) {
        sqlx::query(&format!(
            "DROP TRIGGER IF EXISTS {}",
            quote_identifier(&name)
        ))
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

/// (Re)installs staging triggers whose definition no longer matches the
/// table's columns. Unchanged triggers cause no DDL.
async fn install_staging_triggers(
    connection: &mut SqliteConnection,
    table: &str,
) -> Result<(), DatabaseError> {
    let columns = table_columns(connection, table).await?;
    let statements = staging_triggers(table, &columns);
    let mut current = true;
    for (name, statement) in staging_trigger_names(table).iter().zip(&statements) {
        let installed: Option<String> = sqlx::query_scalar(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=$1 AND tbl_name=$2",
        )
        .bind(name)
        .bind(table)
        .fetch_optional(&mut *connection)
        .await?;
        current &= installed.as_deref() == Some(statement.as_str());
    }
    if !current {
        drop_staging_triggers(connection, table).await?;
        for statement in &statements {
            sqlx::query(statement).execute(&mut *connection).await?;
        }
    }
    Ok(())
}

async fn refresh_triggers(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    let tables: Vec<String> = sqlx::query_scalar("SELECT table_name FROM _gonvex_reference_tables")
        .fetch_all(&mut *connection)
        .await?;
    for table in tables {
        if table_exists(connection, &table).await? {
            install_staging_triggers(connection, &table).await?;
        }
    }
    Ok(())
}

async fn read_documents(
    connection: &mut SqliteConnection,
    table: &str,
) -> Result<Vec<Value>, DatabaseError> {
    let kinds = table_columns(connection, table)
        .await?
        .into_iter()
        .map(|column| {
            let kind = column.kind();
            (column.name, kind)
        })
        .collect::<Vec<_>>();
    let rows: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT {} FROM {}",
        json_object_expr("", &kinds),
        quote_identifier(table)
    ))
    .fetch_all(connection)
    .await?;
    rows.iter()
        .map(|row| serde_json::from_str(row).map_err(|error| violation(error.to_string())))
        .collect()
}

async fn primary_key(
    connection: &mut SqliteConnection,
    table: &str,
) -> Result<Vec<String>, DatabaseError> {
    Ok(
        sqlx::query_scalar("SELECT name FROM pragma_table_info($1) WHERE pk>0 ORDER BY pk")
            .bind(table)
            .fetch_all(connection)
            .await?,
    )
}

/// Whether `column` alone is unique: the single-column primary key or a
/// non-partial single-column unique index.
async fn unique_column(
    connection: &mut SqliteConnection,
    table: &str,
    column: &str,
) -> Result<bool, DatabaseError> {
    if primary_key(connection, table).await? == [column] {
        return Ok(true);
    }
    Ok(sqlx::query_scalar(
        r#"SELECT EXISTS(SELECT 1 FROM pragma_index_list($1) list
             WHERE list."unique"=1 AND list.partial=0
               AND (SELECT count(*) FROM pragma_index_info(list.name))=1
               AND (SELECT name FROM pragma_index_info(list.name))=$2)"#,
    )
    .bind(table)
    .bind(column)
    .fetch_one(connection)
    .await?)
}

fn is_framework_table(name: &str) -> bool {
    name.starts_with("_gonvex") || name.starts_with("gonvex_")
}

async fn declare_in(
    connection: &mut SqliteConnection,
    catalog: &Value,
) -> Result<(), DatabaseError> {
    let empty = Vec::new();
    let tables = catalog.get("tables").and_then(Value::as_array);
    let references = catalog.get("references").and_then(Value::as_array);
    let (Some(tables), Some(references)) = (tables, references) else {
        return Err(violation("invalid reference catalog"));
    };
    if field_text(catalog, "version").as_deref() != Some("1") {
        return Err(violation("invalid reference catalog"));
    }
    let previous_tables: Vec<String> =
        sqlx::query_scalar("SELECT table_name FROM _gonvex_reference_tables")
            .fetch_all(&mut *connection)
            .await?;
    let mut names = previous_tables
        .iter()
        .cloned()
        .map(Some)
        .collect::<Vec<_>>();
    names.extend(tables.iter().map(|entry| field_text(entry, "name")));
    for name in &names {
        match name {
            Some(name) if !is_framework_table(name) => {}
            _ => return Err(violation("invalid application reference table")),
        }
    }
    let table_names = tables
        .iter()
        .filter_map(|entry| field_text(entry, "name"))
        .collect::<BTreeSet<_>>();
    for entry in tables {
        let name = field_text(entry, "name").unwrap_or_default();
        let key = field_text(entry, "key");
        let actual = primary_key(connection, &name).await?;
        if key.is_none() || actual.len() != 1 || key.as_deref() != Some(actual[0].as_str()) {
            return Err(violation(format!(
                "reference table {name} must declare its single primary key"
            )));
        }
        let aliases = match entry.get("targetKeys") {
            None => &empty,
            Some(Value::Array(aliases)) => aliases,
            Some(_) => return Err(violation(format!("invalid target key aliases for {name}"))),
        };
        for alias in aliases {
            let alias_name = field_text(alias, "name").unwrap_or_default();
            let column = field_text(alias, "column").unwrap_or_default();
            if alias_name.is_empty()
                || alias_name.starts_with(CONFIGURATION_PREFIX)
                || table_names.contains(&alias_name)
                || column.is_empty()
                || !column_exists(connection, &name, &column).await?
            {
                return Err(violation(format!(
                    "target alias for {name} needs an existing key column and a separate namespace"
                )));
            }
            if !unique_column(connection, &name, &column).await?
                && (alias.get("multipleOwners") != Some(&Value::Bool(true))
                    || !reason_ok(alias, "reason"))
            {
                return Err(violation(format!(
                    "nonunique target alias for {name} needs an explicit multiple-owner policy and reason"
                )));
            }
        }
        match field_text(entry, "live") {
            Some(live) => {
                parse_path(&live)?;
            }
            None => {
                return Err(violation(format!(
                    "reference table {name} needs a lifecycle"
                )))
            }
        }
    }

    // A prior statement may have staged changes. Apply them against the old
    // declaration and refuse to rebuild over an enforced violation.
    flush(connection).await?;
    let dangling: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pragma_foreign_key_check('_gonvex_reference_edges')",
    )
    .fetch_one(&mut *connection)
    .await?;
    if dangling > 0 {
        return Err(violation(
            "FOREIGN KEY constraint failed: a declared reference would dangle",
        ));
    }
    for table in &previous_tables {
        drop_staging_triggers(connection, table).await?;
    }
    crate::execute_script(
        &mut *connection,
        r#"DELETE FROM _gonvex_reference_edges;
           DELETE FROM _gonvex_reference_targets;
           DELETE FROM _gonvex_reference_rules;
           DELETE FROM _gonvex_reference_keys;
           DELETE FROM _gonvex_reference_configurations;
           DELETE FROM _gonvex_reference_bindings;
           DELETE FROM _gonvex_reference_tables;"#,
    )
    .await?;
    for entry in tables {
        let name = field_text(entry, "name").unwrap_or_default();
        let key = field_text(entry, "key").unwrap_or_default();
        sqlx::query("INSERT INTO _gonvex_reference_tables(table_name,key_column,live_when) VALUES($1,$2,$3)")
            .bind(&name)
            .bind(&key)
            .bind(field_text(entry, "live").unwrap_or_default())
            .execute(&mut *connection)
            .await?;
        let mut keys = vec![(name.clone(), key)];
        for alias in entry
            .get("targetKeys")
            .and_then(Value::as_array)
            .unwrap_or(&empty)
        {
            keys.push((
                field_text(alias, "name").unwrap_or_default(),
                field_text(alias, "column").unwrap_or_default(),
            ));
        }
        for (token, column) in keys {
            sqlx::query("INSERT INTO _gonvex_reference_keys(table_name,token_name,key_column) VALUES($1,$2,$3)")
                .bind(&name)
                .bind(token)
                .bind(column)
                .execute(&mut *connection)
                .await?;
        }
    }

    let bindings = match catalog.get("bindings") {
        None => &empty,
        Some(Value::Array(bindings)) => bindings,
        Some(_) => return Err(violation("invalid configuration bindings")),
    };
    for entry in bindings {
        let name = field_text(entry, "name").unwrap_or_default();
        let source = field_text(entry, "source").unwrap_or_default();
        let columns = match entry.get("columns") {
            Some(Value::Array(columns)) if !columns.is_empty() => columns,
            _ => return Err(violation("invalid configuration binding")),
        };
        if name.is_empty() || !table_names.contains(&source) {
            return Err(violation("invalid configuration binding"));
        }
        let mut column_names = Vec::new();
        for column in columns {
            let column = text_of(Some(column)).unwrap_or_default();
            if !column_exists(connection, &source, &column).await? {
                return Err(violation(format!(
                    "configuration binding {name} needs existing columns"
                )));
            }
            column_names.push(Value::String(column));
        }
        sqlx::query(
            "INSERT INTO _gonvex_reference_bindings(name,source_table,columns_) VALUES($1,$2,$3)",
        )
        .bind(&name)
        .bind(&source)
        .bind(Value::Array(column_names).to_string())
        .execute(&mut *connection)
        .await?;
    }

    for rule in references {
        validate_rule(connection, rule, &table_names).await?;
        sqlx::query(
            "INSERT INTO _gonvex_reference_rules(name,source_table,declaration) VALUES($1,$2,$3)",
        )
        .bind(field_text(rule, "name"))
        .bind(field_text(rule, "source"))
        .bind(rule.to_string())
        .execute(&mut *connection)
        .await?;
    }

    // Inventory: configurations and target tokens for every table first, then
    // edges, so a backfilled edge sees every existing target.
    let ledger = Ledger::load(connection).await?;
    let mut documents = BTreeMap::new();
    for entry in tables {
        let name = field_text(entry, "name").unwrap_or_default();
        let rows = read_documents(connection, &name).await?;
        let key = &ledger.tables[&name].key;
        for binding in ledger
            .bindings
            .iter()
            .filter(|binding| binding.source == name)
        {
            for row in &rows {
                let Some(row_id) = field_text(row, key) else {
                    continue;
                };
                let version = next_version(connection).await?;
                sqlx::query(
                    "INSERT INTO _gonvex_reference_configurations(binding_name,row_id,document,version) VALUES($1,$2,$3,$4)",
                )
                .bind(&binding.name)
                .bind(row_id)
                .bind(configuration(row, &binding.columns).to_string())
                .bind(version)
                .execute(&mut *connection)
                .await?;
            }
        }
        let mut counts: BTreeMap<(String, String, String), i64> = BTreeMap::new();
        for row in &rows {
            for target in ledger.row_targets(connection, &name, Some(row)).await? {
                *counts.entry(target).or_default() += 1;
            }
        }
        for ((token, row_id, state), count) in counts {
            apply_target_delta(connection, &token, &row_id, &state, count).await?;
        }
        install_staging_triggers(connection, &name).await?;
        documents.insert(name, rows);
    }
    for (name, rows) in &documents {
        let key = ledger.tables[name].key.clone();
        for row in rows {
            let Some(row_id) = field_text(row, &key) else {
                continue;
            };
            for (rule, target_table, target_id, state) in
                ledger.desired(connection, name, Some(row)).await?
            {
                // Inventory keeps legacy violations visible without enforcing
                // them (PostgreSQL NOT VALID); new edges are enforced.
                sqlx::query(
                    r#"INSERT INTO _gonvex_reference_edges
                       (rule_name,source_table,source_id,target_table,target_id,target_state,
                        enforced_table,enforced_id,enforced_state)
                       SELECT $1,$2,$3,$4,$5,$6,target.table_name,target.row_id,target.state
                       FROM (SELECT 1) LEFT JOIN _gonvex_reference_targets target
                         ON target.table_name=$4 AND target.row_id=$5 AND target.state=$6"#,
                )
                .bind(&rule)
                .bind(name)
                .bind(&row_id)
                .bind(&target_table)
                .bind(&target_id)
                .bind(&state)
                .execute(&mut *connection)
                .await?;
            }
        }
    }
    if catalog.get("exclusions").is_some() {
        check_coverage(connection, catalog).await?;
    }
    sqlx::query(
        r#"INSERT INTO _gonvex_reference_catalog(singleton,declaration) VALUES(1,$1)
           ON CONFLICT(singleton) DO UPDATE SET declaration=excluded.declaration"#,
    )
    .bind(catalog.to_string())
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn validate_rule(
    connection: &mut SqliteConnection,
    rule: &Value,
    table_names: &BTreeSet<String>,
) -> Result<(), DatabaseError> {
    let name = field_text(rule, "name");
    let label = name.clone().unwrap_or_default();
    let has = |key: &str| rule.get(key).is_some();
    if name.is_none()
        || !matches!(
            field_text(rule, "state").as_deref(),
            Some("exists" | "live")
        )
        || !field_text(rule, "source").is_some_and(|source| table_names.contains(&source))
        || has("target") == has("targets")
    {
        return Err(violation(format!("invalid reference declaration {label}")));
    }
    let paths_invalid = has("paths")
        && !matches!(rule.get("paths"), Some(Value::Array(paths)) if !paths.is_empty());
    if has("path") == has("paths") || paths_invalid {
        return Err(violation(format!(
            "reference {label} needs exactly one path or nonempty paths array"
        )));
    }
    if has("ignoreValues")
        && !(matches!(rule.get("ignoreValues"), Some(Value::Array(values)) if values.iter().all(Value::is_string))
            && reason_ok(rule, "ignoreReason"))
    {
        return Err(violation(format!(
            "reference {label} needs declared string sentinels and their meaning"
        )));
    }
    if has("allowNumbers")
        && (rule.get("allowNumbers") != Some(&Value::Bool(true))
            || !reason_ok(rule, "numberReason"))
    {
        return Err(violation(format!(
            "reference {label} needs an explicit numeric selector policy and reason"
        )));
    }
    if has("normalize")
        && (!matches!(
            str_field(rule, "normalize"),
            Some("csv-or-json-list" | "task-relation")
        ) || !reason_ok(rule, "normalizeReason"))
    {
        return Err(violation(format!(
            "reference {label} needs a supported selector decoder and reason"
        )));
    }
    if has("encoding")
        && (str_field(rule, "encoding") != Some("json-or-value")
            || field_text(rule, "column").is_none())
    {
        return Err(violation(format!(
            "reference {label} has unsupported storage encoding"
        )));
    }
    if let Some(lookup) = rule.get("lookup") {
        let steps = match lookup {
            Value::Array(steps) if !steps.is_empty() => steps.clone(),
            Value::Object(_) => vec![lookup.clone()],
            _ => {
                return Err(violation(format!(
                    "reference {label} needs configuration lookup steps"
                )))
            }
        };
        for step in &steps {
            let binding = field_text(step, "binding").unwrap_or_default();
            let declared: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM _gonvex_reference_bindings WHERE name=$1)",
            )
            .bind(&binding)
            .fetch_one(&mut *connection)
            .await?;
            let from = field_text(step, "from").unwrap_or_else(|| "container".to_owned());
            if !declared
                || field_text(step, "key").is_none()
                || !matches!(from.as_str(), "container" | "document" | "configuration")
            {
                return Err(violation(format!(
                    "reference {label} needs a declared configuration binding and key"
                )));
            }
        }
    }
    if has("targets")
        && (field_text(rule, "discriminator").is_none()
            || !matches!(rule.get("targets"), Some(Value::Object(targets)) if !targets.is_empty()))
    {
        return Err(violation(format!(
            "reference {label} needs discriminator variants"
        )));
    }
    // Parses every JSON path (`at`, paths, sourceWhen, lookup keys, discriminator).
    let compiled = Rule::compile(rule)?;
    let mut targets = Vec::new();
    if has("target") {
        targets.push(field_text(rule, "target"));
    }
    if let Some(Value::Object(variants)) = rule.get("targets") {
        targets.extend(variants.values().map(|target| text_of(Some(target))));
    }
    for target in targets {
        let known: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM _gonvex_reference_keys WHERE token_name=$1)",
        )
        .bind(&target)
        .fetch_one(&mut *connection)
        .await?;
        if !known {
            return Err(violation(format!(
                "reference {} has unknown target {}",
                compiled.name,
                target.unwrap_or_default()
            )));
        }
    }
    Ok(())
}

/// Opt-in storage coverage: every text or JSON column of an application table
/// needs a declared reference, a target key or a reasoned exclusion.
pub fn check_coverage<'a>(
    connection: &'a mut SqliteConnection,
    catalog: &'a Value,
) -> BoxFuture<'a, Result<(), DatabaseError>> {
    Box::pin(check_coverage_inner(connection, catalog))
}

async fn check_coverage_inner(
    connection: &mut SqliteConnection,
    catalog: &Value,
) -> Result<(), DatabaseError> {
    let Some(exclusions) = catalog.get("exclusions").and_then(Value::as_array) else {
        return Err(violation("reference coverage needs explicit exclusions"));
    };
    let empty = Vec::new();
    let references = catalog
        .get("references")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    for rule in references {
        let label = field_text(rule, "name").unwrap_or_default();
        let source = field_text(rule, "source").unwrap_or_default();
        let Some(column) = field_text(rule, "column") else {
            return Err(violation(format!(
                "reference {label} needs an existing source column"
            )));
        };
        if !column_exists(connection, &source, &column).await? {
            return Err(violation(format!(
                "reference {label} needs an existing source column"
            )));
        }
        // Every selection must be anchored to the declared storage column; a
        // sibling path cannot count as coverage for another column.
        let at = field_text(rule, "at")
            .map(|at| parse_path(&at))
            .transpose()?
            .filter(|at| *at != parse_path("$").expect("root path"));
        let selections = match at {
            Some(at) => vec![at],
            None => match rule.get("paths") {
                Some(Value::Array(paths)) => paths
                    .iter()
                    .map(|path| parse_path(&text_of(Some(path)).unwrap_or_default()))
                    .collect::<Result<_, _>>()?,
                _ => vec![parse_path(&field_text(rule, "path").unwrap_or_default())?],
            },
        };
        if selections
            .iter()
            .any(|selection| selection.root_member() != Some(column.as_str()))
        {
            return Err(violation(format!(
                "reference {label} selection must start at its declared column"
            )));
        }
    }
    for entry in exclusions {
        let source = field_text(entry, "source").unwrap_or_default();
        let column = field_text(entry, "column").unwrap_or_default();
        let valid = reason_ok(entry, "reason")
            && field_text(entry, "kind")
                .is_some_and(|kind| EXCLUSION_KINDS.contains(&kind.as_str()))
            && column_exists(connection, &source, &column).await?;
        if !valid {
            return Err(violation(format!(
                "invalid reference exclusion for {source}.{column}"
            )));
        }
    }
    let keys: Vec<(String, String)> =
        sqlx::query_as("SELECT table_name, key_column FROM _gonvex_reference_keys")
            .fetch_all(&mut *connection)
            .await?;
    let classified = |list: &[Value], table: &str, column: &str| {
        list.iter().any(|item| {
            str_field(item, "source") == Some(table) && str_field(item, "column") == Some(column)
        })
    };
    for table in application_tables(connection).await? {
        for column in table_columns(connection, &table).await? {
            let declared = column.declared.to_ascii_uppercase();
            let candidate = !column.primary_key
                && ["TEXT", "CHAR", "CLOB", "JSON"]
                    .iter()
                    .any(|kind| declared.contains(kind));
            if candidate
                && !classified(references, &table, &column.name)
                && !keys
                    .iter()
                    .any(|(owner, key)| owner == &table && key == &column.name)
                && !classified(exclusions, &table, &column.name)
            {
                return Err(violation(format!(
                    "unclassified reference candidate {table}.{}",
                    column.name
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn selector_decoders_match_their_declared_consumers() {
        let decode = |value: Value, mode| decode(value, Some(mode)).unwrap();
        assert_eq!(
            decode(json!(" [ 1, \"selected\", \" \" ] "), "csv-or-json-list"),
            [json!("1"), json!("selected")]
        );
        assert_eq!(
            decode(json!(" selected, other ,, "), "csv-or-json-list"),
            [json!("selected"), json!("other")]
        );
        assert_eq!(
            decode(
                json!({"taskId":"selected","label":"A label"}),
                "task-relation"
            ),
            [json!("selected")]
        );
        assert!(super::decode(json!([{"nested":1}]), Some("csv-or-json-list")).is_err());
        assert_eq!(
            super::decode(json!(null), Some("task-relation")).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[test]
    fn binding_ids_and_configurations_are_stable() {
        assert_eq!(binding_id("manual", Some(3)), r#"["manual",3]"#);
        assert_eq!(binding_id("absent", None), r#"["absent",null]"#);
        assert_eq!(
            configuration(
                &json!({"options":{"a":1},"label":"x"}),
                &["options".to_owned()]
            ),
            json!({"options":{"a":1}})
        );
    }
}
