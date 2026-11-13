//! Tenant-local declared reference integrity on SQLite.
//!
//! Applications own the catalog and deletion policy. Captured tables share one
//! change-feed trigger that queues event IDs in `_gonvex_reference_pending`;
//! it reuses the row images the feed already owns. Uncaptured tables retain
//! three row-image staging triggers. [`flush`] (called by `TenantTransaction`
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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
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
            &mut transaction,
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
    Box::pin(flush_inner(connection, None))
}

type CatalogProgram = tokio::sync::OnceCell<Arc<Ledger>>;

/// Immutable programs shared by equal declaration digests within one pool
/// registry. Weak entries retain no policy after its last open database closes.
#[derive(Default)]
pub(crate) struct CompiledCatalogPrograms(Mutex<BTreeMap<String, Weak<CatalogProgram>>>);

impl CompiledCatalogPrograms {
    fn program(&self, digest: &str) -> Arc<CatalogProgram> {
        let mut programs = self.0.lock().unwrap_or_else(|error| error.into_inner());
        programs.retain(|_, program| program.strong_count() > 0);
        if let Some(program) = programs.get(digest).and_then(Weak::upgrade) {
            return program;
        }
        let program = Arc::new(CatalogProgram::new());
        programs.insert(digest.to_owned(), Arc::downgrade(&program));
        program
    }
}

/// Each database checks its own SQL digest and holds its current program alive.
/// The program contains declarations only; targets, edges and configurations
/// are always read from the transaction's tenant-local SQLite connection.
#[derive(Default)]
pub(crate) struct CompiledCatalogCache {
    current: Mutex<Option<(String, Arc<CatalogProgram>)>>,
    programs: Arc<CompiledCatalogPrograms>,
}

impl CompiledCatalogCache {
    pub(crate) fn new(programs: Arc<CompiledCatalogPrograms>) -> Self {
        Self {
            current: Mutex::default(),
            programs,
        }
    }

    async fn load(&self, connection: &mut SqliteConnection) -> Result<Arc<Ledger>, DatabaseError> {
        // Older opt-in declarations remain valid until redeclared. Do not add
        // runtime-upgrade DDL to every tenant just to cache an unused catalog.
        if !table_exists(connection, "_gonvex_reference_catalog_revision").await? {
            return Ok(Arc::new(Ledger::load(connection).await?));
        }
        let digest: Option<String> = sqlx::query_scalar(
            "SELECT digest FROM _gonvex_reference_catalog_revision WHERE singleton=1",
        )
        .fetch_optional(&mut *connection)
        .await?;
        let Some(digest) = digest else {
            return Ok(Arc::new(Ledger::load(connection).await?));
        };
        let program = {
            let mut cached = self
                .current
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match cached.as_ref() {
                Some((current, program)) if current == &digest => program.clone(),
                _ => {
                    let program = self.programs.program(&digest);
                    *cached = Some((digest, program.clone()));
                    program
                }
            }
        };
        // Only one initializer fetches/compiles a digest during a burst of
        // first writes. Cancellation or failure leaves the cell retryable.
        let ledger = program
            .get_or_try_init(|| async {
                let mut timer = gonvex_profiler::Timer::start();
                let ledger = Arc::new(Ledger::load(connection).await?);
                timer.lap(
                    gonvex_profiler::Stage::ReferenceCompile,
                    gonvex_profiler::Label::None,
                );
                Ok::<_, DatabaseError>(ledger)
            })
            .await?;
        Ok(ledger.clone())
    }
}

pub(crate) async fn flush_cached(
    connection: &mut SqliteConnection,
    cache: &CompiledCatalogCache,
) -> Result<(), DatabaseError> {
    flush_inner(connection, Some(cache)).await
}

async fn flush_inner(
    connection: &mut SqliteConnection,
    cache: Option<&CompiledCatalogCache>,
) -> Result<(), DatabaseError> {
    if !table_exists(connection, "_gonvex_reference_pending").await? {
        return Ok(());
    }
    let last: Option<i64> = sqlx::query_scalar("SELECT max(seq) FROM _gonvex_reference_pending")
        .fetch_one(&mut *connection)
        .await?;
    let Some(last) = last else {
        return Ok(());
    };
    let feed_capture =
        column_exists(connection, "_gonvex_reference_pending", "change_event_id").await?;
    let staged_rows = if feed_capture {
        "SELECT p.seq,p.table_name,
         CASE WHEN p.change_event_id IS NULL THEN p.old_doc
              WHEN json_type(c.new_value)='array' THEN nullif(c.new_value -> '$[0]','null')
              ELSE c.old_value END,
         CASE WHEN p.change_event_id IS NULL THEN p.new_doc
              WHEN json_type(c.new_value)='array' THEN nullif(c.new_value -> '$[1]','null')
              ELSE c.new_value END
         FROM _gonvex_reference_pending p LEFT JOIN _gonvex_sync_changes c ON c.event_id=p.change_event_id
         WHERE p.seq <= $1 AND p.seq > $2 ORDER BY p.seq LIMIT 256"
    } else {
        "SELECT seq,table_name,old_doc,new_doc FROM _gonvex_reference_pending
         WHERE seq <= $1 AND seq > $2 ORDER BY seq LIMIT 256"
    };
    let ledger = match cache {
        Some(cache) => cache.load(connection).await?,
        None => Arc::new(Ledger::load(connection).await?),
    };
    crate::execute_script(connection,
        "CREATE TEMP TABLE IF NOT EXISTS _gonvex_reference_dirty_sources(table_name TEXT NOT NULL,row_id TEXT NOT NULL,PRIMARY KEY(table_name,row_id)) WITHOUT ROWID; DELETE FROM temp._gonvex_reference_dirty_sources;")
        .await?;
    let mut after = i64::MIN;
    // Establish final target/configuration state first. Keep the original
    // deferred guards until every dependent has been checked successfully.
    loop {
        let pending = sqlx::query(staged_rows)
            .bind(last)
            .bind(after)
            .fetch_all(&mut *connection)
            .await?;
        let Some(batch_last) = pending.last().map(|row| row.get::<i64, _>(0)) else {
            break;
        };
        for row in &pending {
            let table: String = row.get(1);
            let parse = |index: usize| -> Result<Option<Value>, DatabaseError> {
                row.get::<Option<String>, _>(index)
                    .map(|text| serde_json::from_str(&text))
                    .transpose()
                    .map_err(|error| violation(format!("invalid staged row image: {error}")))
            };
            let (old, new) = (parse(2)?, parse(3)?);
            let Some(declaration) = ledger.tables.get(&table) else {
                continue;
            };
            if !declaration.rules.is_empty() {
                for document in [old.as_ref(), new.as_ref()].into_iter().flatten() {
                    if let Some(id) = field_text(document, &declaration.key) {
                        sqlx::query(
                            "INSERT OR IGNORE INTO temp._gonvex_reference_dirty_sources VALUES($1,$2)",
                        )
                        .bind(&table)
                        .bind(id)
                        .execute(&mut *connection)
                        .await?;
                    }
                }
            }
            ledger
                .sync_targets(connection, &table, old.as_ref(), new.as_ref())
                .await?;
        }
        after = batch_last;
    }
    let mut point_queries = BTreeMap::new();
    loop {
        let dirty: Vec<(String,String)> = sqlx::query_as("SELECT table_name,row_id FROM temp._gonvex_reference_dirty_sources ORDER BY table_name,row_id LIMIT 256")
            .fetch_all(&mut *connection).await?;
        if dirty.is_empty() {
            break;
        }
        for (table, id) in dirty {
            if !point_queries.contains_key(&table) {
                let scan = DocumentScan::new(connection, &table, &ledger).await?;
                point_queries.insert(
                    table.clone(),
                    format!("{} WHERE {}=$1", scan.select, quote_identifier(&scan.key)),
                );
            }
            let document: Option<String> = sqlx::query_scalar(&point_queries[&table])
                .bind(&id)
                .fetch_optional(&mut *connection)
                .await?;
            let document = document
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()
                .map_err(|error| violation(error.to_string()))?;
            ledger
                .sync_edges(connection, &table, &id, document.as_ref())
                .await?;
            sqlx::query("DELETE FROM temp._gonvex_reference_dirty_sources WHERE table_name=$1 AND row_id=$2")
                .bind(&table).bind(&id).execute(&mut *connection).await?;
        }
    }
    sqlx::query("DELETE FROM _gonvex_reference_pending WHERE seq <= $1")
        .bind(last)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// Whether `flush` has staged rows to process, checked directly on the
/// connection (the common answer, "no", then costs no sqlx round trip).
pub(crate) fn staged_direct(connection: &mut crate::direct::Conn<'_>) -> Result<bool, sqlx::Error> {
    let ledger = connection.query_i64(
        "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_reference_pending'",
        &[],
    )?;
    if ledger.is_none() {
        return Ok(false);
    }
    Ok(connection
        .query_i64("SELECT 1 FROM _gonvex_reference_pending LIMIT 1", &[])?
        .is_some())
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

fn configuration(document: &Value, paths: &[Vec<String>]) -> Value {
    fn insert(output: &mut Value, path: &[String], value: Value) {
        if !output.is_object() {
            *output = Value::Object(Map::new());
        }
        if path.len() == 1 {
            output[&path[0]] = value;
        } else {
            insert(
                output
                    .as_object_mut()
                    .unwrap()
                    .entry(path[0].clone())
                    .or_insert(Value::Null),
                &path[1..],
                value,
            );
        }
    }
    let mut result = Value::Object(Map::new());
    for path in paths {
        let value = path
            .iter()
            .try_fold(document, |value, member| value.get(member));
        insert(&mut result, path, value.cloned().unwrap_or(Value::Null));
    }
    result
}

struct TableDecl {
    key: String,
    live: JsonPath,
    rules: Vec<Rule>,
    keys: Vec<(String, String)>,
    bindings: Vec<Binding>,
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
    assertion: Option<JsonPath>,
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
            _ if declaration.get("assert").is_some() => Vec::new(),
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
            assertion: field_text(declaration, "assert")
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
    paths: Vec<Vec<String>>,
}

fn binding_paths(value: &Value) -> Result<Vec<Vec<String>>, DatabaseError> {
    let Some(entries) = value.as_array().filter(|entries| !entries.is_empty()) else {
        return Err(violation("invalid configuration projection"));
    };
    let mut paths: Vec<Vec<String>> = Vec::new();
    for entry in entries {
        // Strings are the original whole-column format. Arrays are the v2
        // exact member paths; storing segments avoids reparsing on each row.
        let path = match entry {
            Value::String(column) if !column.is_empty() => vec![column.clone()],
            Value::Array(segments) if !segments.is_empty() => segments
                .iter()
                .map(|segment| {
                    segment
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| violation("invalid configuration member"))
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err(violation("invalid configuration projection")),
        };
        if paths
            .iter()
            .any(|existing| existing.starts_with(&path) || path.starts_with(existing))
        {
            return Err(violation("overlapping configuration projections"));
        }
        paths.push(path);
    }
    Ok(paths)
}

/// The compiled declaration, grouped by source so an edit visits only its
/// table's policy. There is no second mutable representation of that policy.
struct Ledger {
    tables: BTreeMap<String, TableDecl>,
    single_owner_targets: BTreeSet<String>,
    target_states: BTreeMap<String, bool>,
    binding_sources: BTreeMap<String, String>,
    input_columns: BTreeMap<String, Option<BTreeSet<String>>>,
}

impl Ledger {
    fn columns_for(&self, source: &str) -> Option<BTreeSet<String>> {
        self.input_columns.get(source).cloned().flatten()
    }

    fn derive_columns(&self, source: &str) -> Option<BTreeSet<String>> {
        let table = self.tables.get(source)?;
        let mut columns = BTreeSet::from([table.key.clone()]);
        columns.extend(table.live.document_members(true)?);
        columns.extend(table.keys.iter().map(|(_, column)| column.clone()));
        for binding in &table.bindings {
            columns.extend(binding.paths.iter().map(|path| path[0].clone()));
        }
        for rule in &table.rules {
            if let Some(column) = str_field(&rule.declaration, "column") {
                columns.insert(column.to_owned());
            }
            if let Some(when) = &rule.source_when {
                columns.extend(when.document_members(true)?);
            }
            let at_root = rule.at.is_root();
            if !at_root {
                columns.extend(rule.at.document_members(true)?);
            }
            for path in &rule.paths {
                columns.extend(path.document_members(at_root)?);
            }
            if let Some(steps) = &rule.lookup {
                for step in steps {
                    columns.extend(step.key.document_members(
                        step.from == "document" || step.from == "container" && at_root,
                    )?);
                    if let Some(when) = &step.when {
                        columns.extend(when.document_members(false)?);
                    }
                }
            }
            if let Some(discriminator) = &rule.discriminator {
                columns.extend(discriminator.document_members(rule.lookup.is_none() && at_root)?);
            }
            if let Some(assertion) = &rule.assertion {
                columns.extend(assertion.document_members(false)?);
            }
        }
        Some(columns)
    }

    async fn load(connection: &mut SqliteConnection) -> Result<Self, DatabaseError> {
        let mut tables = BTreeMap::new();
        let mut single_owner_targets = BTreeSet::new();
        let mut target_states = BTreeMap::<String, bool>::new();
        let mut binding_sources = BTreeMap::new();
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
                    rules: Vec::new(),
                    keys: Vec::new(),
                    bindings: Vec::new(),
                },
            );
        }
        for row in sqlx::query("SELECT declaration FROM _gonvex_reference_rules ORDER BY name")
            .fetch_all(&mut *connection)
            .await?
        {
            let declaration: Value = serde_json::from_str(&row.get::<String, _>(0))
                .map_err(|error| violation(error.to_string()))?;
            let rule = Rule::compile(&declaration)?;
            let mut targets = Vec::new();
            if let Some(target) = str_field(&declaration, "target") {
                targets.push(target.to_owned());
            }
            if let Some(variants) = declaration.get("targets").and_then(Value::as_object) {
                targets.extend(
                    variants
                        .values()
                        .filter_map(Value::as_str)
                        .map(str::to_owned),
                );
            }
            if let Some(steps) = &rule.lookup {
                targets.extend(
                    steps
                        .iter()
                        .map(|step| format!("{CONFIGURATION_PREFIX}{}", step.binding)),
                );
            }
            for target in targets {
                *target_states.entry(target).or_default() |= rule.state == "live";
            }

            if declaration.get("singleOwner") == Some(&Value::Bool(true)) {
                if let Some(target) = str_field(&declaration, "target") {
                    single_owner_targets.insert(target.to_owned());
                }
                if let Some(targets) = declaration.get("targets").and_then(Value::as_object) {
                    single_owner_targets.extend(
                        targets
                            .values()
                            .filter_map(Value::as_str)
                            .map(str::to_owned),
                    );
                }
            }
            let table = tables
                .get_mut(&rule.source)
                .ok_or_else(|| violation("reference rule has an undeclared source"))?;
            table.rules.push(rule);
        }
        let keys: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT table_name, token_name, key_column FROM _gonvex_reference_keys ORDER BY table_name, token_name",
        )
        .fetch_all(&mut *connection)
        .await?;
        for (source, token, column) in keys {
            let table = tables
                .get_mut(&source)
                .ok_or_else(|| violation("reference key has an undeclared source"))?;
            table.keys.push((token, column));
        }
        for row in sqlx::query(
            "SELECT name, source_table, columns_ FROM _gonvex_reference_bindings ORDER BY name",
        )
        .fetch_all(&mut *connection)
        .await?
        {
            let source: String = row.get(1);
            let table = tables
                .get_mut(&source)
                .ok_or_else(|| violation("reference binding has an undeclared source"))?;
            let name: String = row.get(0);
            binding_sources.insert(name.clone(), source);
            table.bindings.push(Binding {
                name,
                paths: binding_paths(
                    &serde_json::from_str::<Value>(&row.get::<String, _>(2))
                        .map_err(|error| violation(error.to_string()))?,
                )?,
            });
        }
        let mut ledger = Self {
            tables,
            single_owner_targets,
            target_states,
            binding_sources,
            input_columns: BTreeMap::new(),
        };
        ledger.input_columns = ledger
            .tables
            .keys()
            .map(|source| (source.clone(), ledger.derive_columns(source)))
            .collect();
        Ok(ledger)
    }

    fn states(&self, token: &str, live: bool) -> impl Iterator<Item = &'static str> {
        let needed = self.target_states.get(token).copied();
        // Existence also distinguishes deleted from missing in diagnostics.
        STATES.into_iter().filter(move |state| {
            needed.is_some() && (*state == "exists" || live && needed == Some(true))
        })
    }

    async fn configuration_for(
        &self,
        connection: &mut SqliteConnection,
        binding_name: &str,
        row_id: &str,
    ) -> Result<Option<(Value, i64)>, DatabaseError> {
        if let Some(row)=sqlx::query("SELECT document,version FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2")
            .bind(binding_name).bind(row_id).fetch_optional(&mut *connection).await? {
            return Ok(Some((serde_json::from_str(&row.get::<String,_>(0)).map_err(|error|violation(error.to_string()))?,row.get(1))));
        }
        let source = self
            .binding_sources
            .get(binding_name)
            .ok_or_else(|| violation("undeclared configuration binding"))?;
        let table = &self.tables[source];
        let binding = table
            .bindings
            .iter()
            .find(|binding| binding.name == binding_name)
            .ok_or_else(|| violation("undeclared configuration binding"))?;
        let scan = DocumentScan::new(connection, source, self).await?;
        let sql = format!("{} WHERE {}=$1", scan.select, quote_identifier(&scan.key));
        let Some(text) = sqlx::query_scalar::<_, String>(&sql)
            .bind(row_id)
            .fetch_optional(&mut *connection)
            .await?
        else {
            return Ok(None);
        };
        let document: Value =
            serde_json::from_str(&text).map_err(|error| violation(error.to_string()))?;
        // Do not let SQLite affinity invent an alternate public identity.
        if field_text(&document, &table.key).as_deref() != Some(row_id) {
            return Ok(None);
        }
        let projected = configuration(&document, &binding.paths);
        let version = next_version(connection).await?;
        sqlx::query("INSERT INTO _gonvex_reference_configurations(binding_name,row_id,document,version) VALUES($1,$2,$3,$4)")
            .bind(binding_name).bind(row_id).bind(projected.to_string()).bind(version).execute(&mut *connection).await?;
        let token = format!("{CONFIGURATION_PREFIX}{binding_name}");
        let id = binding_id(row_id, Some(version));
        let live = table.live.matches(&document, None) == Some(true);
        for state in self.states(&token, live) {
            apply_target_delta(connection, &token, &id, state, 1).await?;
        }
        Ok(Some((projected, version)))
    }

    async fn sync_edges(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        row_id: &str,
        document: Option<&Value>,
    ) -> Result<(), DatabaseError> {
        let desired = self.desired(connection, table, document).await?;
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
        let existing = existing.into_iter().collect::<BTreeSet<_>>();
        let mut unused_configurations = BTreeSet::new();
        for edge in existing.iter().filter(|edge| !desired.contains(*edge)) {
            if let Some(binding) = edge.1.strip_prefix(CONFIGURATION_PREFIX) {
                if let Some(parent) = serde_json::from_str::<Value>(&edge.2)
                    .ok()
                    .and_then(|id| id.get(0)?.as_str().map(str::to_owned))
                {
                    unused_configurations.insert((binding.to_owned(), parent));
                }
            }
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
        for edge in desired.iter().filter(|edge| !existing.contains(*edge)) {
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
        // Only after inserting the final edges: otherwise an old rule could
        // delete the configuration a replacement rule is about to reference.
        for (binding, parent) in unused_configurations {
            let version: Option<i64> = sqlx::query_scalar("SELECT version FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2")
                .bind(&binding).bind(&parent).fetch_optional(&mut *connection).await?;
            let Some(version) = version else { continue };
            let token = format!("{CONFIGURATION_PREFIX}{binding}");
            let id = binding_id(&parent, Some(version));
            let used: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_reference_edges WHERE target_table=$1 AND target_id=$2)")
                .bind(&token).bind(&id).fetch_one(&mut *connection).await?;
            if !used {
                sqlx::query(
                    "DELETE FROM _gonvex_reference_targets WHERE table_name=$1 AND row_id=$2",
                )
                .bind(&token)
                .bind(&id)
                .execute(&mut *connection)
                .await?;
                sqlx::query("DELETE FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2")
                    .bind(&binding).bind(&parent).execute(&mut *connection).await?;
            }
        }
        Ok(())
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
        for rule in &table.rules {
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
        let mut document = Cow::Borrowed(document);
        // Some consumers accept both JSON values and JSON serialized in a
        // text column. Decoding is declared per reference, never guessed.
        if str_field(&rule.declaration, "encoding") == Some("json-or-value") {
            if let Some(column) = str_field(&rule.declaration, "column") {
                if let Some(Value::String(serialized)) = document.get(column) {
                    let parsed: Value = serde_json::from_str(serialized).map_err(|_| {
                        violation(format!("reference {name} has invalid serialized JSON"))
                    })?;
                    document.to_mut()[column] = parsed;
                }
            }
        }
        let mut selected = Vec::new();
        let containers: Cow<'_, [Value]> = if rule.at.is_root() {
            Cow::Borrowed(std::slice::from_ref(document.as_ref()))
        } else {
            Cow::Owned(rule.at.query(&document, None))
        };
        'containers: for container in containers.iter() {
            let vars = (rule.lookup.is_some() || rule.discriminator.is_some()).then(|| {
                Map::from_iter([
                    ("entry".to_owned(), container.clone()),
                    ("document".to_owned(), document.as_ref().clone()),
                ])
            });
            let mut configuration: Option<Value> = None;
            if let Some(steps) = &rule.lookup {
                for step in steps {
                    let lookup_document = match step.from.as_str() {
                        "document" => Some(document.as_ref()),
                        "configuration" => configuration.as_ref(),
                        _ => Some(container),
                    };
                    let identifier = lookup_document
                        .and_then(|lookup| step.key.query_first(lookup, vars.as_ref()))
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
                    let (found, version) = match self
                        .configuration_for(connection, &step.binding, &parent_id)
                        .await?
                    {
                        Some((document, version)) => (Some(document), Some(version)),
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
                        step.when.as_ref().map_or(Some(true), |when| {
                            when.matches(configuration, vars.as_ref())
                        }) == Some(true)
                    });
                    if !applies {
                        continue 'containers;
                    }
                }
            }
            if let Some(assertion) = &rule.assertion {
                if !configuration.as_ref().is_some_and(|configuration| {
                    assertion.matches(configuration, vars.as_ref()) == Some(true)
                }) {
                    return Err(violation(format!(
                        "reference {name} fails its configuration assertion"
                    )));
                }
            }
            for path in &rule.paths {
                for item in path.query(container, None) {
                    let normalization = str_field(&rule.declaration, "normalize").or_else(|| {
                        (str_field(&rule.declaration, "encoding") == Some("json-or-value"))
                            .then_some("csv-or-json-list")
                    });
                    for mut identifier in decode(item, normalization)? {
                        if identifier.is_null() {
                            continue;
                        }
                        if let Some(prefix) = str_field(&rule.declaration, "valuePrefix") {
                            let Some(value) = identifier
                                .as_str()
                                .and_then(|value| value.strip_prefix(prefix))
                            else {
                                continue;
                            };
                            identifier = Value::String(value.to_owned());
                        }
                        if let (Some(value), Some(Value::Array(markers))) = (
                            identifier.as_str(),
                            rule.declaration.get("ignoreContainingAll"),
                        ) {
                            if markers.iter().all(|marker| {
                                marker.as_str().is_some_and(|marker| value.contains(marker))
                            }) {
                                continue;
                            }
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
                                Some(container)
                            };
                            let discriminator = match (scope, &rule.discriminator) {
                                (Some(scope), Some(path)) => path.query_first(scope, vars.as_ref()),
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
                        if rule.declaration.get("singleOwner") == Some(&Value::Bool(true)) {
                            let owners: Option<i64> = sqlx::query_scalar("SELECT owner_count FROM _gonvex_reference_targets WHERE table_name=$1 AND row_id=$2 AND state=$3")
                                .bind(&destination).bind(&target_id).bind(&rule.state).fetch_optional(&mut *connection).await?;
                            if owners.is_some_and(|owners| owners > 1) {
                                return Err(violation(format!(
                                    "reference {name} has an ambiguous target"
                                )));
                            }
                        }
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
        for (token, column) in &table.keys {
            if let Some(value) = field_text(document, column) {
                for state in self.states(token, live) {
                    targets.push((token.clone(), value.clone(), state.to_owned()));
                }
            }
        }
        let Some(row_id) = field_text(document, &table.key) else {
            return Ok(targets);
        };
        for binding in &table.bindings {
            let row = sqlx::query(
                "SELECT document, version FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2",
            )
            .bind(&binding.name)
            .bind(&row_id)
            .fetch_optional(&mut *connection)
            .await?;
            let Some(row) = row else { continue };
            let stored = serde_json::from_str::<Value>(&row.get::<String, _>(0)).ok();
            if stored.as_ref() != Some(&configuration(document, &binding.paths)) {
                continue;
            }
            let id = binding_id(&row_id, Some(row.get(1)));
            let token = format!("{CONFIGURATION_PREFIX}{}", binding.name);
            for state in self.states(&token, live) {
                targets.push((token.clone(), id.clone(), state.to_owned()));
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
        let bindings = &table.bindings;
        if let Some(previous_id) = &previous_id {
            if document.is_none() || document_id.as_ref() != Some(previous_id) {
                for binding in bindings {
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
            if previous_id.as_ref() != Some(document_id) {
                // Installing a formerly missing parent must not reinterpret
                // legacy values whose domain was unknown at inventory.
                for binding in bindings {
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
            for binding in bindings {
                let projected = configuration(document, &binding.paths);
                let stored: Option<String> = sqlx::query_scalar(
                    "SELECT document FROM _gonvex_reference_configurations WHERE binding_name=$1 AND row_id=$2",
                )
                .bind(&binding.name)
                .bind(document_id)
                .fetch_optional(&mut *connection)
                .await?;
                let Some(stored) = stored else { continue };
                let unchanged = Some(stored.as_str())
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
                if (delta < 0 && token.starts_with(CONFIGURATION_PREFIX))
                    || self.single_owner_targets.contains(&token)
                {
                    // Only dependents of the changed configuration enter the
                    // work queue. Their final values may still be perfectly valid.
                    sqlx::query("INSERT OR IGNORE INTO temp._gonvex_reference_dirty_sources SELECT source_table,source_id FROM _gonvex_reference_edges WHERE target_table=$1 AND target_id=$2 AND target_state=$3")
                        .bind(&token).bind(&row_id).bind(&state).execute(&mut *connection).await?;
                }
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
    let parsed = match &value {
        Value::String(raw) => serde_json::from_str::<Value>(raw)
            .ok()
            .filter(|value| value.is_array() || value.is_object()),
        _ => None,
    };
    let value = parsed.unwrap_or(value);
    match normalization {
        "task-relation" => {
            return Ok(match value {
                Value::Object(mut object) => object
                    .remove("taskId")
                    .filter(|value| !value.is_null())
                    .or_else(|| object.remove("task_id"))
                    .into_iter()
                    .collect(),
                other => vec![other],
            })
        }
        "csv-or-json-list" => {}
        _ => return Err(violation("unsupported selector normalization")),
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
    match &value {
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

const FEED_STAGING_TRIGGER: &str = "CREATE TRIGGER _gonvex_reference_from_feed AFTER INSERT ON _gonvex_sync_changes
WHEN NEW.revision IS NULL AND EXISTS (
  SELECT 1 FROM _gonvex_reference_tables r WHERE r.table_name=NEW.table_name AND r.capture_columns IS NOT NULL
    AND (NEW.operation<>'update' OR json_type(r.capture_columns)='null' OR EXISTS (
      SELECT 1 FROM json_each(NEW.new_value -> '$[2]') changed
      JOIN json_each(r.capture_columns) needed ON needed.value=changed.value)))
BEGIN
  INSERT INTO _gonvex_reference_pending(table_name,change_event_id) VALUES(NEW.table_name,NEW.event_id);
END";

async fn captured_tables(
    connection: &mut SqliteConnection,
) -> Result<BTreeSet<String>, DatabaseError> {
    let rows:Vec<(String,String)>=sqlx::query_as("SELECT tbl_name,name FROM sqlite_schema WHERE type='trigger' AND sql LIKE '%gonvex_capture(%'")
        .fetch_all(connection).await?;
    let mut names = BTreeMap::<String, BTreeSet<String>>::new();
    for (table, name) in rows {
        names.entry(table).or_default().insert(name);
    }
    Ok(names
        .into_iter()
        .filter_map(|(table, names)| {
            crate::feed::capture_trigger_names(&table)
                .iter()
                .all(|name| names.contains(name))
                .then_some(table)
        })
        .collect())
}

async fn install_feed_staging(connection: &mut SqliteConnection) -> Result<(), DatabaseError> {
    // These are compiled inputs and a transaction-local pointer, not another
    // application representation. Rebuild both from the authoritative catalog.
    if !column_exists(connection, "_gonvex_reference_tables", "capture_columns").await? {
        sqlx::query("ALTER TABLE _gonvex_reference_tables ADD COLUMN capture_columns JSON")
            .execute(&mut *connection)
            .await?;
    }
    if !column_exists(connection, "_gonvex_reference_pending", "change_event_id").await? {
        sqlx::query("ALTER TABLE _gonvex_reference_pending ADD COLUMN change_event_id INTEGER REFERENCES _gonvex_sync_changes(event_id) DEFERRABLE INITIALLY DEFERRED")
            .execute(&mut *connection).await?;
    }
    let current: Option<String> = sqlx::query_scalar(
        "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='_gonvex_reference_from_feed'",
    )
    .fetch_optional(&mut *connection)
    .await?;
    if current.as_deref() != Some(FEED_STAGING_TRIGGER) {
        sqlx::query("DROP TRIGGER IF EXISTS _gonvex_reference_from_feed")
            .execute(&mut *connection)
            .await?;
        sqlx::query(FEED_STAGING_TRIGGER)
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

/// Reconcile reference staging after capture bootstrap (including repaired or
/// newly captured tables). Reuse its transactional receipt store; a native
/// schema cookie plus catalog digest invalidates this derived plan on DDL,
/// redeclaration and rollback, without compiling every policy at each boot.
pub(crate) async fn refresh_from_change_feed(
    connection: &mut SqliteConnection,
) -> Result<(), DatabaseError> {
    if !table_exists(connection, "_gonvex_reference_catalog").await? {
        return Ok(());
    }
    let digest = if table_exists(connection, "_gonvex_reference_catalog_revision").await? {
        sqlx::query_scalar::<_, String>(
            "SELECT digest FROM _gonvex_reference_catalog_revision WHERE singleton=1",
        )
        .fetch_optional(&mut *connection)
        .await?
    } else {
        sqlx::query_scalar::<_, String>(
            "SELECT declaration FROM _gonvex_reference_catalog WHERE singleton=1",
        )
        .fetch_optional(&mut *connection)
        .await?
        .map(|text| format!("{:x}", Sha256::digest(text.as_bytes())))
    };
    let Some(digest) = digest else { return Ok(()) };
    let cookie: i64 = sqlx::query_scalar("PRAGMA main.schema_version")
        .fetch_one(&mut *connection)
        .await?;
    let signature = |cookie| {
        format!(
            "reference-feed-v1:{cookie}:{digest}:{:x}",
            Sha256::digest(FEED_STAGING_TRIGGER.as_bytes())
        )
    };
    let receipt: Option<String> = sqlx::query_scalar(
        "SELECT digest FROM _gonvex_sync_bootstrap WHERE name='reference-capture'",
    )
    .fetch_optional(&mut *connection)
    .await?;
    if receipt.as_deref() == Some(signature(cookie).as_str()) {
        return Ok(());
    }
    refresh_triggers(connection).await?;
    let cookie: i64 = sqlx::query_scalar("PRAGMA main.schema_version")
        .fetch_one(&mut *connection)
        .await?;
    sqlx::query("INSERT INTO _gonvex_sync_bootstrap(name,digest) VALUES('reference-capture',$1) ON CONFLICT(name) DO UPDATE SET digest=excluded.digest")
        .bind(signature(cookie)).execute(&mut *connection).await?;
    Ok(())
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
    ledger: &Ledger,
    captured: bool,
) -> Result<(), DatabaseError> {
    if column_exists(connection, "_gonvex_reference_tables", "capture_columns").await? {
        let inputs = captured.then(|| {
            ledger
                .columns_for(table)
                .map_or(Value::Null, |columns| {
                    Value::Array(columns.into_iter().map(Value::String).collect())
                })
                .to_string()
        });
        sqlx::query("UPDATE _gonvex_reference_tables SET capture_columns=$2 WHERE table_name=$1 AND capture_columns IS NOT $2")
            .bind(table).bind(inputs).execute(&mut *connection).await?;
    }
    if captured {
        drop_staging_triggers(connection, table).await?;
        return Ok(());
    }
    let mut columns = table_columns(connection, table).await?;
    if let Some(needed) = ledger.columns_for(table) {
        columns.retain(|column| needed.contains(&column.name));
    }
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
    let ledger = Ledger::load(connection).await?;
    if ledger.tables.is_empty() {
        return Ok(());
    }
    let captured = captured_tables(connection).await?;
    if ledger.tables.keys().any(|table| captured.contains(table)) {
        install_feed_staging(connection).await?;
    }
    for (table, declaration) in &ledger.tables {
        if !table_exists(connection, table).await?
            || primary_key(connection, table).await? != [declaration.key.as_str()]
        {
            return Err(violation(format!("declared reference table {table} lost its public key; update its declaration in the same migration")));
        }
        let columns = table_columns(connection, table).await?;
        let required = declaration
            .keys
            .iter()
            .map(|(_, column)| column.as_str())
            .chain(
                declaration
                    .bindings
                    .iter()
                    .flat_map(|binding| binding.paths.iter().map(|path| path[0].as_str())),
            )
            .chain(
                declaration
                    .rules
                    .iter()
                    .filter_map(|rule| str_field(&rule.declaration, "column")),
            );
        for column in required {
            if !columns.iter().any(|actual| actual.name == column) {
                return Err(violation(format!("declared reference column {table}.{column} was removed; update its declaration in the same migration")));
            }
        }
        install_staging_triggers(connection, table, &ledger, captured.contains(table)).await?;
    }
    Ok(())
}

/// Keyset batches keep inventory memory bounded. An OFFSET scan would make
/// each successive batch revisit the rows already processed.
struct DocumentScan {
    select: String,
    key: String,
    after: Option<String>,
}

impl DocumentScan {
    async fn new(
        connection: &mut SqliteConnection,
        table: &str,
        ledger: &Ledger,
    ) -> Result<Self, DatabaseError> {
        let needed = ledger.columns_for(table);
        let kinds = table_columns(connection, table)
            .await?
            .into_iter()
            .filter(|column| {
                needed
                    .as_ref()
                    .is_none_or(|needed| needed.contains(&column.name))
            })
            .map(|column| {
                let kind = column.kind();
                (column.name, kind)
            })
            .collect::<Vec<_>>();
        Ok(Self {
            select: format!(
                "SELECT {} FROM {}",
                json_object_expr("", &kinds),
                quote_identifier(table)
            ),
            key: ledger.tables[table].key.clone(),
            after: None,
        })
    }

    async fn next(
        &mut self,
        connection: &mut SqliteConnection,
    ) -> Result<Vec<Value>, DatabaseError> {
        let key = quote_identifier(&self.key);
        let sql = format!(
            "{}{} ORDER BY {key} LIMIT 256",
            self.select,
            if self.after.is_some() {
                format!(" WHERE {key} > $1")
            } else {
                String::new()
            }
        );
        let query = sqlx::query_scalar::<_, String>(&sql);
        let query = match &self.after {
            Some(after) => query.bind(after),
            None => query,
        };
        let rows: Vec<Value> = query
            .fetch_all(connection)
            .await?
            .into_iter()
            .map(|row| serde_json::from_str(&row).map_err(|error| violation(error.to_string())))
            .collect::<Result<_, _>>()?;
        if let Some(last) = rows.last() {
            self.after = Some(
                field_text(last, &self.key)
                    .ok_or_else(|| violation("reference inventory needs non-null row keys"))?,
            );
        }
        Ok(rows)
    }
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
    let version = field_text(catalog, "version");
    if !matches!(version.as_deref(), Some("1" | "2")) {
        return Err(violation("invalid reference catalog"));
    }
    if version.as_deref() == Some("1")
        && (catalog.get("existing").is_some()
            || references
                .iter()
                .any(|rule| rule.get("assert").is_some() || rule.get("singleOwner").is_some())
            || catalog
                .get("bindings")
                .and_then(Value::as_array)
                .is_some_and(|bindings| {
                    bindings
                        .iter()
                        .any(|binding| binding.get("paths").is_some())
                }))
    {
        return Err(violation("configuration assertions, member projections and single-owner references require catalog version 2"));
    }
    if catalog.get("existing").is_some()
        && !matches!(str_field(catalog, "existing"), Some("reject" | "inventory"))
    {
        return Err(violation(
            "existing reference policy must be reject or inventory",
        ));
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
        let columns = match (entry.get("columns"), entry.get("paths")) {
            (Some(Value::Array(columns)), None)
                if !columns.is_empty() && columns.iter().all(Value::is_string) =>
            {
                Value::Array(columns.clone())
            }
            (None, Some(Value::Array(paths))) if !paths.is_empty() => Value::Array(
                paths
                    .iter()
                    .map(|path| {
                        let path = path.as_str().ok_or_else(|| {
                            violation("configuration projection needs a member path")
                        })?;
                        parse_path(path)?
                            .member_path()
                            .map(|segments| {
                                Value::Array(segments.into_iter().map(Value::String).collect())
                            })
                            .ok_or_else(|| {
                                violation(
                                    "configuration projection needs only exact object members",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            _ => return Err(violation("invalid configuration binding")),
        };
        if name.is_empty() || !table_names.contains(&source) {
            return Err(violation("invalid configuration binding"));
        }
        for path in binding_paths(&columns)? {
            if !column_exists(connection, &source, &path[0]).await? {
                return Err(violation(format!(
                    "configuration binding {name} needs existing columns"
                )));
            }
        }
        sqlx::query(
            "INSERT INTO _gonvex_reference_bindings(name,source_table,columns_) VALUES($1,$2,$3)",
        )
        .bind(&name)
        .bind(&source)
        .bind(columns.to_string())
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

    // Inventory target identities first. Source edges then materialize only
    // the configurations they actually use, never a copy of every parent row.
    let ledger = Ledger::load(connection).await?;
    for entry in tables {
        let name = field_text(entry, "name").unwrap_or_default();
        let mut scan = DocumentScan::new(connection, &name, &ledger).await?;
        loop {
            let rows = scan.next(connection).await?;
            if rows.is_empty() {
                break;
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
        }
    }
    for entry in tables {
        let name = field_text(entry, "name").unwrap_or_default();
        let key = ledger.tables[&name].key.clone();
        let mut scan = DocumentScan::new(connection, &name, &ledger).await?;
        loop {
            let rows = scan.next(connection).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let Some(row_id) = field_text(row, &key) else {
                    continue;
                };
                for (rule, target_table, target_id, state) in
                    ledger.desired(connection, &name, Some(row)).await?
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
                    .bind(&name)
                    .bind(&row_id)
                    .bind(&target_table)
                    .bind(&target_id)
                    .bind(&state)
                    .execute(&mut *connection)
                    .await?;
                }
            }
        }
    }
    if str_field(catalog, "existing") == Some("reject") {
        let invalid: Option<(String, String, String)> = sqlx::query_as(
            "SELECT rule_name,source_table,source_id FROM gonvex_reference_violations LIMIT 1",
        )
        .fetch_optional(&mut *connection)
        .await?;
        if let Some((rule, table, id)) = invalid {
            return Err(violation(format!(
                "reference {rule} on {table}/{id} has an invalid existing target"
            )));
        }
    }
    if catalog.get("exclusions").is_some() {
        check_coverage(connection, catalog).await?;
    }
    let serialized = catalog.to_string();
    sqlx::query(
        r#"INSERT INTO _gonvex_reference_catalog(singleton,declaration) VALUES(1,$1)
           ON CONFLICT(singleton) DO UPDATE SET declaration=excluded.declaration"#,
    )
    .bind(&serialized)
    .execute(&mut *connection)
    .await?;
    // Install cache identity only for an explicitly installed declaration.
    // The declaration's content identity is transactional, including rollback.
    sqlx::query("CREATE TABLE IF NOT EXISTS _gonvex_reference_catalog_revision (singleton INTEGER PRIMARY KEY CHECK(singleton=1),digest TEXT NOT NULL)")
        .execute(&mut *connection).await?;
    sqlx::query(
        "INSERT INTO _gonvex_reference_catalog_revision(singleton,digest) VALUES(1,$1) \
         ON CONFLICT(singleton) DO UPDATE SET digest=excluded.digest",
    )
    .bind(format!("{:x}", Sha256::digest(serialized.as_bytes())))
    .execute(&mut *connection)
    .await?;
    refresh_triggers(connection).await?;
    // A declaration replaces a static catalog and inventories its targets.
    // Install their bounded planner statistics in that same transaction.
    // Otherwise the next open analyzes these newly populated tables, touching
    // the whole catalog again even on a boot with no pending migrations.
    crate::registry::optimize_planner_statistics(connection).await?;
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
    const FIELDS: &[&str] = &[
        "name",
        "source",
        "column",
        "state",
        "target",
        "targets",
        "at",
        "path",
        "paths",
        "sourceWhen",
        "lookup",
        "discriminator",
        "allowEmpty",
        "ignoreValues",
        "ignoreReason",
        "allowNumbers",
        "numberReason",
        "normalize",
        "normalizeReason",
        "encoding",
        "valuePrefix",
        "prefixReason",
        "ignoreContainingAll",
        "assert",
        "singleOwner",
        "ownerReason",
    ];
    if let Some(object) = rule.as_object() {
        if let Some(unknown) = object
            .keys()
            .find(|field| !FIELDS.contains(&field.as_str()))
        {
            return Err(violation(format!(
                "reference {label} has unsupported policy {unknown}"
            )));
        }
    }
    if name.is_none()
        || !matches!(
            field_text(rule, "state").as_deref(),
            Some("exists" | "live")
        )
        || !field_text(rule, "source").is_some_and(|source| table_names.contains(&source))
        || if has("assert") {
            has("target") || has("targets") || !has("lookup") || has("path") || has("paths")
        } else {
            has("target") == has("targets")
        }
    {
        return Err(violation(format!("invalid reference declaration {label}")));
    }
    let paths_invalid = has("paths")
        && !matches!(rule.get("paths"), Some(Value::Array(paths)) if !paths.is_empty());
    if !has("assert") && (has("path") == has("paths") || paths_invalid) {
        return Err(violation(format!(
            "reference {label} needs exactly one path or nonempty paths array"
        )));
    }
    if has("assert")
        && str_field(rule, "assert").is_none_or(|assertion| assertion.trim().is_empty())
    {
        return Err(violation(format!(
            "reference {label} needs a configuration assertion"
        )));
    }
    if has("singleOwner")
        && (has("assert")
            || rule.get("singleOwner") != Some(&Value::Bool(true))
            || !reason_ok(rule, "ownerReason"))
    {
        return Err(violation(format!(
            "reference {label} needs a single-owner policy and reason"
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
    if has("ignoreContainingAll")
        && !(matches!(rule.get("ignoreContainingAll"),Some(Value::Array(markers))
        if !markers.is_empty() && markers.iter().all(|marker| marker.as_str().is_some_and(|marker| !marker.is_empty())))
            && reason_ok(rule, "ignoreReason"))
    {
        return Err(violation(format!(
            "reference {label} needs nonempty literal markers and their meaning"
        )));
    }
    if has("valuePrefix")
        && !(str_field(rule, "valuePrefix").is_some_and(|prefix| !prefix.is_empty())
            && reason_ok(rule, "prefixReason"))
    {
        return Err(violation(format!(
            "reference {label} needs a nonempty selector prefix and its meaning"
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

    #[tokio::test]
    async fn uncaptured_tables_keep_their_native_commit_guard() {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        crate::execute_script(&mut connection,"PRAGMA foreign_keys=ON; CREATE TABLE parents(_id TEXT PRIMARY KEY NOT NULL); CREATE TABLE children(_id TEXT PRIMARY KEY NOT NULL,parentId TEXT)").await.unwrap();
        install(&mut connection).await.unwrap();
        declare(&mut connection,&json!({"version":1,"tables":[
            {"name":"parents","key":"_id","live":"true"},
            {"name":"children","key":"_id","live":"true"}],
            "references":[{"name":"parent","source":"children","column":"parentId","path":"$.parentId","target":"parents","state":"exists","allowEmpty":true}]
        })).await.unwrap();
        assert!(!table_exists(&mut connection, "_gonvex_sync_changes")
            .await
            .unwrap());
        assert!(
            sqlx::query("INSERT INTO children VALUES('bad','absent')")
                .execute(&mut connection)
                .await
                .is_err(),
            "a raw writer that omits flush must fail closed without change capture"
        );
        let mut transaction = connection.begin().await.unwrap();
        crate::execute_script(
            &mut transaction,
            "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p')",
        )
        .await
        .unwrap();
        flush(&mut transaction).await.unwrap();
        transaction.commit().await.unwrap();
        assert!(sqlx::query("DELETE FROM parents WHERE _id='p'")
            .execute(&mut connection)
            .await
            .is_err());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM parents")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(count, 1);
        connection.close().await.unwrap();
    }

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
        assert_eq!(
            decode(json!("{\"task_id\":\"selected\"}"), "task-relation"),
            [json!("selected")]
        );
        assert_eq!(
            decode(json!({"taskId":null,"task_id":"fallback"}), "task-relation"),
            [json!("fallback")]
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
                &[vec!["options".to_owned()]]
            ),
            json!({"options":{"a":1}})
        );
        assert_eq!(
            configuration(
                &json!({"data":{"poll":{"options":[{"id":"a"}],"tallies":{"a":7}},"body":"large"}}),
                &[vec!["data".into(), "poll".into(), "options".into()]]
            ),
            json!({"data":{"poll":{"options":[{"id":"a"}]}}})
        );
    }
}
