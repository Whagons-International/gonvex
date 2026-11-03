//! `gonvex-admin export-sqlite`: moves a Postgres deployment (Control Plane
//! plus every project/tenant database) into a SQLite data directory.
//!
//! The target schema is built by the runtime's own provisioning
//! (`ControlPlane::connect`, `provision_tenant_database`), so every file is
//! exactly what the runtime would create; only rows are copied. See
//! docs/sqlite-storage.md, "Moving existing data".

pub mod convert;
pub mod plan;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use futures_util::TryStreamExt;
use gonvex_store::{
    ControlPlane, DataDir, MigrationScope, PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
};
use serde::Serialize;
use serde_json::{Map, Value};
use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqliteRow, SqliteSynchronous,
};
use sqlx::{Connection, Row, TypeInfo, ValueRef};

use self::convert::{canonical, convert, SourceFamily, SourceValue, SqlValue, TargetKind};
use self::plan::{
    load_migrations, map_source_url, plan_routes, redact_url, DatabaseKind, DatabasePlan,
    RouteRewrite, SourceProject, SourceTenant,
};
use crate::identity::Result;

pub const DEFAULT_SAMPLE_ROWS: usize = 200;

/// Target tables never copied from Postgres:
/// - the migration ledger records the SQLite migrations that built the file;
/// - change-feed history is not carried over (the feed restarts at the source
///   revision with `retained_revision = revision`, so clients resync by
///   snapshot instead of replaying Postgres-encoded row images);
/// - `_gonvex_sync_clock` is set explicitly;
/// - the reference ledger is rebuilt from the catalog by the store.
fn never_copied(table: &str) -> bool {
    matches!(
        table,
        "gonvex_migrations"
            | "_gonvex_sync_bootstrap"
            | "_gonvex_sync_changes"
            | "_gonvex_sync_transactions"
            | "_gonvex_sync_clock"
    ) || table.starts_with("_gonvex_reference_")
}

/// Runtime-created table copied when the source has it. Mirrors
/// `FILES_TABLE_DDL` in rust/crates/runtime/src/storage.rs.
const FILES_TABLE_DDL: &str = r#"CREATE TABLE IF NOT EXISTS _gonvex_files (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  owner_id TEXT NOT NULL DEFAULT '',
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  content_type TEXT NOT NULL DEFAULT '',
  size_bytes INTEGER NOT NULL DEFAULT 0,
  checksum TEXT NOT NULL DEFAULT '',
  visibility TEXT NOT NULL DEFAULT 'private',
  status TEXT NOT NULL DEFAULT 'pending',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  uploaded_at INTEGER,
  deleted_at INTEGER
)"#;

/// Framework triggers that would turn copied rows into change-feed rows,
/// reference staging rows or membership projections. They are dropped for the
/// copy and restored verbatim (so the store's install receipts still match).
const FRAMEWORK_TRIGGERS: &str = r#"SELECT name, sql FROM sqlite_schema
  WHERE type='trigger' AND (name LIKE '\_gonvex\_sync\_%' ESCAPE '\'
    OR name LIKE '\_gonvex\_reference\_%' ESCAPE '\'
    OR name LIKE 'gonvex\_queue\_control\_plane\_membership\_%' ESCAPE '\')
  ORDER BY name"#;

/// Application triggers (everything but the framework triggers above).
const APPLICATION_TRIGGERS: &str = r#"SELECT tbl_name, sql FROM sqlite_schema
  WHERE type='trigger' AND sql IS NOT NULL
    AND name NOT LIKE '\_gonvex\_sync\_%' ESCAPE '\'
    AND name NOT LIKE '\_gonvex\_reference\_%' ESCAPE '\'
    AND name NOT LIKE 'gonvex\_queue\_control\_plane\_membership\_%' ESCAPE '\'
  ORDER BY name"#;

/// Tokens of a trigger body: upper-cased bare words and quoted identifiers
/// (unquoted, case kept). String literals and comments are skipped.
fn sql_tokens(sql: &str) -> Vec<(bool, String)> {
    let chars = sql.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' && chars.get(i + 1) == Some(&'\'') {
                    i += 2;
                } else if chars[i] == '\'' {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2;
        } else if c == '"' || c == '`' || c == '[' {
            let close = if c == '[' { ']' } else { c };
            let mut ident = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == close && chars.get(i + 1) == Some(&close) && close != ']' {
                    ident.push(close);
                    i += 2;
                } else if chars[i] == close {
                    i += 1;
                    break;
                } else {
                    ident.push(chars[i]);
                    i += 1;
                }
            }
            tokens.push((true, ident));
        } else if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            tokens.push((false, chars[start..i].iter().collect::<String>()));
        } else {
            i += 1;
        }
    }
    tokens
}

/// Tables a trigger body writes: `INSERT [OR x] INTO t`, `REPLACE INTO t`,
/// `UPDATE [OR x] t`, `DELETE FROM t`. Only the body after `BEGIN` counts, so
/// the trigger's own `AFTER UPDATE OF … ON t` header is not a write.
fn trigger_writes(sql: &str) -> Vec<String> {
    let tokens = sql_tokens(sql);
    let is = |token: Option<&(bool, String)>, word: &str| matches!(token, Some((false, w)) if w.eq_ignore_ascii_case(word));
    let Some(begin) = tokens.iter().position(|t| is(Some(t), "BEGIN")) else {
        return Vec::new();
    };
    let mut writes = Vec::new();
    let mut i = begin + 1;
    while i < tokens.len() {
        let mut target = None;
        if is(tokens.get(i), "INSERT") || is(tokens.get(i), "REPLACE") {
            let mut j = i + 1;
            if is(tokens.get(j), "OR") {
                j += 2;
            }
            if is(tokens.get(j), "INTO") {
                target = Some(j + 1);
            }
        } else if is(tokens.get(i), "UPDATE") {
            let mut j = i + 1;
            if is(tokens.get(j), "OR") {
                j += 2;
            }
            // `ON CONFLICT … DO UPDATE SET` names no table.
            if !is(tokens.get(j), "SET") {
                target = Some(j);
            }
        } else if is(tokens.get(i), "DELETE") && is(tokens.get(i + 1), "FROM") {
            target = Some(i + 2);
        }
        // SQLite forbids schema-qualified names inside trigger bodies.
        if let Some((_, name)) = target.and_then(|j| tokens.get(j)) {
            if !writes.contains(name) {
                writes.push(name.clone());
            }
        }
        i += 1;
    }
    writes
}

/// Stable order in which every table that application triggers write into is
/// copied after the tables whose triggers write it. `copy_rows` deletes before
/// inserting, so such a table ends with exactly its source rows. Tables in a
/// trigger cycle keep their original relative order.
fn trigger_copy_order(
    names: &[String],
    triggers: &[(String, String)],
) -> (Vec<usize>, Vec<String>) {
    let index = names
        .iter()
        .enumerate()
        .map(|(i, name)| (name.as_str(), i))
        .collect::<BTreeMap<_, _>>();
    // after[b] = tables whose triggers write b (b must follow them).
    let mut after = vec![BTreeSet::new(); names.len()];
    for (table, sql) in triggers {
        let Some(&writer) = index.get(table.as_str()) else {
            continue;
        };
        for written in trigger_writes(sql) {
            if let Some(&target) = index.get(written.as_str()) {
                if target != writer {
                    after[target].insert(writer);
                }
            }
        }
    }
    let mut placed = vec![false; names.len()];
    let mut order = Vec::with_capacity(names.len());
    loop {
        let next = (0..names.len()).find(|&i| !placed[i] && after[i].iter().all(|&w| placed[w]));
        match next {
            Some(i) => {
                placed[i] = true;
                order.push(i);
            }
            None => match (0..names.len()).find(|&i| !placed[i]) {
                // A cycle: place the earliest remaining table as-is.
                Some(i) => {
                    placed[i] = true;
                    order.push(i);
                }
                None => break,
            },
        }
    }
    let adjustments = (0..names.len())
        .filter(|&i| !after[i].is_empty())
        .map(|i| {
            format!(
                "{} <- [{}]",
                names[i],
                after[i]
                    .iter()
                    .map(|&w| names[w].as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect();
    (order, adjustments)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationDirs {
    pub default: Option<PathBuf>,
    pub per_project: BTreeMap<String, PathBuf>,
}

impl MigrationDirs {
    fn for_project(&self, project_id: &str) -> Option<&Path> {
        self.per_project
            .get(project_id)
            .or(self.default.as_ref())
            .map(PathBuf::as_path)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportOptions {
    pub control_plane_url: String,
    pub data_dir: PathBuf,
    pub migrations: MigrationDirs,
    pub projects: Vec<String>,
    pub tenants: Vec<String>,
    pub force: bool,
    pub allow_drop_columns: bool,
    pub allow_drop_tables: bool,
    pub keep_epoch: bool,
    pub no_control_plane: bool,
    pub sample_rows: usize,
    pub source_url_maps: Vec<(String, String)>,
    pub report_file: Option<PathBuf>,
}

pub fn usage() -> &'static str {
    "gonvex-admin export-sqlite --control-plane-url POSTGRES_URL --data-dir DIR \\
      --migrations DIR | --migrations PROJECT_ID=DIR ...
      [--project ID ...] [--tenant ID ...] [--force] [--no-control-plane]
      [--allow-drop-columns] [--allow-drop-tables] [--keep-epoch]
      [--sample-rows N] [--source-url-map FROM=TO ...] [--report FILE]"
}

pub fn parse_options(args: &[String]) -> Result<ExportOptions> {
    let mut options = ExportOptions {
        control_plane_url: String::new(),
        data_dir: PathBuf::new(),
        migrations: MigrationDirs::default(),
        projects: Vec::new(),
        tenants: Vec::new(),
        force: false,
        allow_drop_columns: false,
        allow_drop_tables: false,
        keep_epoch: false,
        no_control_plane: false,
        sample_rows: DEFAULT_SAMPLE_ROWS,
        source_url_maps: Vec::new(),
        report_file: None,
    };
    let mut index = 0;
    while index < args.len() {
        let flag = args[index].as_str();
        let mut value = || -> Result<String> {
            index += 1;
            args.get(index)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| format!("{flag} requires a value").into())
        };
        match flag {
            "--force" => options.force = true,
            "--allow-drop-columns" => options.allow_drop_columns = true,
            "--allow-drop-tables" => options.allow_drop_tables = true,
            "--keep-epoch" => options.keep_epoch = true,
            "--no-control-plane" => options.no_control_plane = true,
            "--control-plane-url" => options.control_plane_url = value()?,
            "--data-dir" => options.data_dir = PathBuf::from(value()?),
            "--report" => options.report_file = Some(PathBuf::from(value()?)),
            "--project" => options.projects.push(value()?),
            "--tenant" => options.tenants.push(value()?),
            "--sample-rows" => {
                options.sample_rows = value()?
                    .parse()
                    .map_err(|_| "--sample-rows must be a non-negative integer")?
            }
            "--source-url-map" => {
                let mapping = value()?;
                let (from, to) = mapping
                    .split_once('=')
                    .ok_or("--source-url-map must be FROM=TO")?;
                options
                    .source_url_maps
                    .push((from.to_owned(), to.to_owned()));
            }
            "--migrations" => {
                let raw = value()?;
                match raw.split_once('=') {
                    Some((project, directory))
                        if !project.is_empty() && !project.contains(['/', '\\']) =>
                    {
                        if options
                            .migrations
                            .per_project
                            .insert(project.to_owned(), PathBuf::from(directory))
                            .is_some()
                        {
                            return Err(format!("--migrations for {project:?} given twice").into());
                        }
                    }
                    _ => {
                        if options
                            .migrations
                            .default
                            .replace(PathBuf::from(raw))
                            .is_some()
                        {
                            return Err("--migrations DIR may be given only once".into());
                        }
                    }
                }
            }
            flag if flag.starts_with('-') => return Err(format!("unknown option {flag}").into()),
            value => return Err(format!("unexpected argument {value:?}").into()),
        }
        index += 1;
    }
    if options.control_plane_url.is_empty() {
        options.control_plane_url = std::env::var("GONVEX_CONTROL_PLANE_DATABASE_URL")
            .unwrap_or_default()
            .trim()
            .to_owned();
    }
    if !(options.control_plane_url.starts_with("postgres://")
        || options.control_plane_url.starts_with("postgresql://"))
    {
        return Err(
            "--control-plane-url must be the Postgres Control Plane URL (postgres://…), or set GONVEX_CONTROL_PLANE_DATABASE_URL"
                .into(),
        );
    }
    if options.data_dir.as_os_str().is_empty() {
        return Err("--data-dir is required".into());
    }
    if options.migrations.default.is_none() && options.migrations.per_project.is_empty() {
        return Err(
            "--migrations is required: the application's SQLite migrations directory (DIR or PROJECT_ID=DIR)"
                .into(),
        );
    }
    Ok(options)
}

// ---------------------------------------------------------------------------
// Report

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub ok: bool,
    pub data_dir: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_plane: Option<DatabaseReport>,
    pub databases: Vec<DatabaseReport>,
    /// Databases routed in the Control Plane but outside `--project/--tenant`.
    pub not_exported: Vec<String>,
    pub findings: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseReport {
    pub kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub project_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tenant_ids: Vec<String>,
    pub source: String,
    pub target: String,
    pub tables: Vec<TableReport>,
    /// Source tables intentionally not copied, with the reason.
    pub skipped_source_tables: BTreeMap<String, String>,
    /// Target tables with no source table (left as provisioning created them).
    pub target_only_tables: Vec<String>,
    /// Tables moved later in the copy because application triggers on other
    /// copied tables write into them (`table <- [writers]`); their source rows
    /// then replace whatever the triggers derived mid-copy.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub copy_order_adjustments: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock: Option<ClockReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_ledger: Option<ReferenceReport>,
    pub integrity_check: String,
    pub foreign_key_violations: Vec<String>,
    pub findings: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableReport {
    pub table: String,
    pub source_table: String,
    pub source_rows: i64,
    pub target_rows: i64,
    pub sampled_rows: usize,
    pub sample_mismatches: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub dropped_columns: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub defaulted_columns: Vec<String>,
    /// Schema mismatches accepted only because the source table had no rows
    /// in the export snapshot (nothing could be lost or invented).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub relaxed_for_empty_source: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClockReport {
    pub source_epoch: Option<String>,
    pub source_revision: i64,
    pub epoch: String,
    pub revision: i64,
    pub retained_revision: i64,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceReport {
    /// `target` (declared by the SQLite migrations), `source` (the Postgres
    /// catalog, re-declared) or `none`.
    pub catalog: String,
    pub source_edges: Option<i64>,
    pub target_edges: i64,
    pub violations: i64,
}

// ---------------------------------------------------------------------------
// Schema inspection

#[derive(Clone, Debug)]
struct SourceColumn {
    name: String,
    data_type: String,
    udt_name: String,
}

type SourceSchema = BTreeMap<String, Vec<SourceColumn>>;

async fn source_schema(connection: &mut PgConnection) -> Result<SourceSchema> {
    let rows = sqlx::query(
        r#"SELECT c.table_name::text, c.column_name::text, c.data_type::text, c.udt_name::text
           FROM information_schema.columns c
           JOIN information_schema.tables t
             ON t.table_schema = c.table_schema AND t.table_name = c.table_name
           WHERE c.table_schema = current_schema() AND t.table_type = 'BASE TABLE'
           ORDER BY c.table_name, c.ordinal_position"#,
    )
    .fetch_all(&mut *connection)
    .await?;
    let mut schema = SourceSchema::new();
    for row in rows {
        schema
            .entry(row.get::<String, _>(0))
            .or_default()
            .push(SourceColumn {
                name: row.get(1),
                data_type: row.get(2),
                udt_name: row.get(3),
            });
    }
    Ok(schema)
}

#[derive(Clone, Debug)]
struct TargetColumn {
    name: String,
    declared: String,
    not_null: bool,
    has_default: bool,
    /// 1-based position in the primary key, 0 if not part of it.
    pk: i64,
}

#[derive(Clone, Debug)]
struct TargetTable {
    name: String,
    columns: Vec<TargetColumn>,
}

impl TargetTable {
    fn primary_key(&self) -> Vec<&TargetColumn> {
        let mut key = self.columns.iter().filter(|c| c.pk > 0).collect::<Vec<_>>();
        key.sort_by_key(|column| column.pk);
        key
    }
}

async fn target_tables(connection: &mut SqliteConnection) -> Result<Vec<TargetTable>> {
    let names: Vec<String> = sqlx::query_scalar(
        r#"SELECT list.name FROM pragma_table_list list
           WHERE list.schema='main' AND list.type='table'
             AND list.name NOT LIKE 'sqlite\_%' ESCAPE '\'
             AND NOT EXISTS (
               SELECT 1 FROM sqlite_schema virtual
               WHERE virtual.type='table' AND virtual.sql LIKE 'CREATE VIRTUAL TABLE%'
                 AND list.name IN (virtual.name, virtual.name||'_data', virtual.name||'_idx',
                   virtual.name||'_content', virtual.name||'_docsize', virtual.name||'_config'))
           ORDER BY list.name"#,
    )
    .fetch_all(&mut *connection)
    .await?;
    let mut tables = Vec::new();
    for name in names {
        let columns = sqlx::query(
            r#"SELECT name, type, "notnull", dflt_value IS NOT NULL, pk FROM pragma_table_info($1) ORDER BY cid"#,
        )
        .bind(&name)
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .map(|row| TargetColumn {
            name: row.get(0),
            declared: row.get(1),
            not_null: row.get::<i64, _>(2) != 0,
            has_default: row.get::<i64, _>(3) != 0,
            pk: row.get(4),
        })
        .collect();
        tables.push(TargetTable { name, columns });
    }
    Ok(tables)
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Row copy

/// Per-row Control Plane routing rewrite: `key_column` value -> new values.
struct Rewrite<'a> {
    key_column: &'static str,
    routes: &'a BTreeMap<String, RouteRewrite>,
}

struct ColumnCopy {
    name: String,
    kind: TargetKind,
    family: SourceFamily,
    select: String,
}

struct TablePlan {
    columns: Vec<ColumnCopy>,
    dropped: Vec<String>,
    defaulted: Vec<String>,
    relaxed: Vec<String>,
}

/// `source_empty`: the source table has no rows in the export snapshot. Then a
/// required SQLite column missing from Postgres, or a Postgres column missing
/// from SQLite, cannot lose or invent data, so the mismatch is recorded in
/// `relaxed` instead of stopping the export. With rows, both still fail.
fn plan_table(
    target: &TargetTable,
    source_name: &str,
    source: &[SourceColumn],
    allow_drop_columns: bool,
    source_empty: bool,
) -> Result<TablePlan> {
    let by_name = source
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<BTreeMap<_, _>>();
    let mut columns = Vec::new();
    let mut defaulted = Vec::new();
    let mut relaxed = Vec::new();
    for column in &target.columns {
        match by_name.get(column.name.as_str()) {
            Some(source) => {
                let family = SourceFamily::from_postgres(&source.data_type, &source.udt_name);
                columns.push(ColumnCopy {
                    name: column.name.clone(),
                    kind: TargetKind::from_declared(&column.declared),
                    family,
                    select: family.select_expr(&quote(&source.name), &source.data_type),
                });
            }
            None => {
                let rowid_alias = column.pk == 1
                    && column.declared.eq_ignore_ascii_case("INTEGER")
                    && target.columns.iter().filter(|c| c.pk > 0).count() == 1;
                if column.not_null && !column.has_default && !rowid_alias {
                    if source_empty {
                        relaxed.push(format!(
                            "{}: NOT NULL without a default, missing from Postgres (source table empty)",
                            column.name
                        ));
                        continue;
                    }
                    return Err(format!(
                        "{}.{}: NOT NULL without a default and missing from Postgres table {source_name}",
                        target.name, column.name
                    )
                    .into());
                }
                defaulted.push(column.name.clone());
            }
        }
    }
    let target_names = target
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let dropped = source
        .iter()
        .filter(|column| !target_names.contains(column.name.as_str()))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if !dropped.is_empty() && !allow_drop_columns && source_empty {
        relaxed.extend(dropped.iter().map(|name| {
            format!("{name}: Postgres column with no SQLite column (source table empty)")
        }));
    } else if !dropped.is_empty() && !allow_drop_columns {
        return Err(format!(
            "Postgres table {source_name} has columns missing from SQLite table {}: {}; fix the SQLite schema or pass --allow-drop-columns",
            target.name,
            dropped.join(", ")
        )
        .into());
    }
    if columns.is_empty() {
        return Err(format!("{}: no columns in common with {source_name}", target.name).into());
    }
    Ok(TablePlan {
        columns,
        dropped,
        defaulted,
        relaxed,
    })
}

fn decode_source(
    row: &sqlx::postgres::PgRow,
    index: usize,
    family: SourceFamily,
) -> Result<SourceValue> {
    Ok(match family {
        SourceFamily::Bool => row
            .try_get::<Option<bool>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::Bool),
        SourceFamily::Int => row
            .try_get::<Option<i64>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::Int),
        SourceFamily::Float => row
            .try_get::<Option<f64>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::Float),
        SourceFamily::Timestamp => row
            .try_get::<Option<i64>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::TimestampMicros),
        SourceFamily::Date => row
            .try_get::<Option<i64>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::DateDays),
        SourceFamily::Numeric => row
            .try_get::<Option<String>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::Numeric),
        SourceFamily::Json => row
            .try_get::<Option<String>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::Json),
        SourceFamily::Text => row
            .try_get::<Option<String>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::Text),
        SourceFamily::Bytes => row
            .try_get::<Option<Vec<u8>>, _>(index)?
            .map_or(SourceValue::Null, SourceValue::Bytes),
    })
}

/// Converts one source row into target values (with routing rewrites).
fn convert_row(
    table: &str,
    plan: &TablePlan,
    row: &sqlx::postgres::PgRow,
    rewrite: Option<&Rewrite<'_>>,
) -> Result<Vec<SqlValue>> {
    let mut values = Vec::with_capacity(plan.columns.len());
    for (index, column) in plan.columns.iter().enumerate() {
        let source = decode_source(row, index, column.family)
            .map_err(|error| format!("{table}.{}: {error}", column.name))?;
        values.push(
            convert(&source, column.kind)
                .map_err(|error| format!("{table}.{}: {error}", column.name))?,
        );
    }
    if let Some(rewrite) = rewrite {
        let key_index = plan
            .columns
            .iter()
            .position(|column| column.name == rewrite.key_column)
            .ok_or_else(|| format!("{table}: routing key {} missing", rewrite.key_column))?;
        let SqlValue::Text(key) = &values[key_index] else {
            return Err(format!("{table}: routing key is not text").into());
        };
        let route = rewrite.routes.get(key).ok_or_else(|| {
            format!("{table} row {key:?} appeared during the export; stop the runtime and rerun")
        })?;
        for (index, column) in plan.columns.iter().enumerate() {
            match column.name.as_str() {
                "database_url" => values[index] = SqlValue::Text(route.database_url.clone()),
                "database_name" => values[index] = SqlValue::Text(route.database_name.clone()),
                _ => {}
            }
        }
    }
    Ok(values)
}

fn bind_values<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    values: Vec<SqlValue>,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for value in values {
        query = match value {
            SqlValue::Null => query.bind(Option::<i64>::None),
            SqlValue::Integer(number) => query.bind(number),
            SqlValue::Real(float) => query.bind(float),
            SqlValue::Text(text) => query.bind(text),
            SqlValue::Blob(bytes) => query.bind(bytes),
        };
    }
    query
}

fn stored_value(row: &SqliteRow, index: usize) -> Result<SqlValue> {
    let raw = row.try_get_raw(index)?;
    if raw.is_null() {
        return Ok(SqlValue::Null);
    }
    let storage = raw.type_info().name().to_ascii_uppercase();
    Ok(match storage.as_str() {
        "INTEGER" => SqlValue::Integer(row.try_get_unchecked::<i64, _>(index)?),
        "REAL" => SqlValue::Real(row.try_get_unchecked::<f64, _>(index)?),
        "BLOB" => SqlValue::Blob(row.try_get_unchecked::<Vec<u8>, _>(index)?),
        _ => SqlValue::Text(row.try_get_unchecked::<String, _>(index)?),
    })
}

fn select_sql(plan: &TablePlan, source_table: &str) -> String {
    format!(
        "SELECT {} FROM {}",
        plan.columns
            .iter()
            .map(|column| column.select.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        quote(source_table)
    )
}

async fn copy_rows(
    source: &mut PgConnection,
    target: &mut SqliteConnection,
    table: &TargetTable,
    source_table: &str,
    plan: &TablePlan,
    rewrite: Option<&Rewrite<'_>>,
) -> Result<i64> {
    sqlx::query(&format!("DELETE FROM {}", quote(&table.name)))
        .execute(&mut *target)
        .await?;
    let insert = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote(&table.name),
        plan.columns
            .iter()
            .map(|column| quote(&column.name))
            .collect::<Vec<_>>()
            .join(", "),
        plan.columns
            .iter()
            .map(|column| if column.kind == TargetKind::Json {
                "json(?)"
            } else {
                "?"
            })
            .collect::<Vec<_>>()
            .join(", ")
    );
    let select = select_sql(plan, source_table);
    let mut rows = sqlx::query(&select).fetch(&mut *source);
    let mut copied = 0_i64;
    while let Some(row) = rows.try_next().await? {
        let values = convert_row(&table.name, plan, &row, rewrite)?;
        bind_values(sqlx::query(&insert), values)
            .execute(&mut *target)
            .await
            .map_err(|error| format!("insert into {}: {error}", table.name))?;
        copied += 1;
    }
    Ok(copied)
}

/// Compares `limit` pseudo-randomly chosen rows (stable order by the hash of
/// the primary key) after conversion with the stored SQLite rows, as
/// canonical JSON. Returns (sampled, mismatches, example descriptions).
async fn sample_compare(
    source: &mut PgConnection,
    target: &mut SqliteConnection,
    table: &TargetTable,
    source_table: &str,
    plan: &TablePlan,
    rewrite: Option<&Rewrite<'_>>,
    limit: usize,
) -> Result<(usize, usize, Vec<String>)> {
    let key = table.primary_key();
    if limit == 0 || key.is_empty() {
        return Ok((0, 0, Vec::new()));
    }
    let key_positions = key
        .iter()
        .map(|column| {
            plan.columns
                .iter()
                .position(|copy| copy.name == column.name)
                .ok_or_else(|| format!("{}: primary key {} not copied", table.name, column.name))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let order = key
        .iter()
        .map(|column| format!("{}::text", quote(&column.name)))
        .collect::<Vec<_>>()
        .join(" || '\u{1f}' || ");
    let select = format!(
        "{} ORDER BY md5({order}), {} LIMIT {limit}",
        select_sql(plan, source_table),
        key.iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let rows = sqlx::query(&select).fetch_all(&mut *source).await?;
    let lookup = format!(
        "SELECT {} FROM {} WHERE {}",
        plan.columns
            .iter()
            .map(|column| quote(&column.name))
            .collect::<Vec<_>>()
            .join(", "),
        quote(&table.name),
        key.iter()
            .map(|column| format!("{} = ?", quote(&column.name)))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    let canonical_row = |values: &[SqlValue]| -> Map<String, Value> {
        plan.columns
            .iter()
            .zip(values)
            .map(|(column, value)| (column.name.clone(), canonical(value, column.kind)))
            .collect()
    };
    let mut mismatches = 0;
    let mut examples = Vec::new();
    for row in &rows {
        let expected = convert_row(&table.name, plan, row, rewrite)?;
        let key_values = key_positions
            .iter()
            .map(|position| expected[*position].clone())
            .collect::<Vec<_>>();
        let stored = bind_values(sqlx::query(&lookup), key_values.clone())
            .fetch_all(&mut *target)
            .await?;
        let describe_key = || {
            key_values
                .iter()
                .zip(&key)
                .map(|(value, column)| {
                    format!(
                        "{}={}",
                        column.name,
                        canonical(value, TargetKind::from_declared(&column.declared))
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        };
        let [stored] = stored.as_slice() else {
            mismatches += 1;
            if examples.len() < 3 {
                examples.push(format!(
                    "{} [{}]: {} stored rows",
                    table.name,
                    describe_key(),
                    stored.len()
                ));
            }
            continue;
        };
        let actual = (0..plan.columns.len())
            .map(|index| stored_value(stored, index))
            .collect::<Result<Vec<_>>>()?;
        let (expected, actual) = (canonical_row(&expected), canonical_row(&actual));
        if expected != actual {
            mismatches += 1;
            if examples.len() < 3 {
                let columns = expected
                    .iter()
                    .filter(|(name, value)| actual.get(*name) != Some(value))
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                examples.push(format!(
                    "{} [{}]: columns differ: {columns}",
                    table.name,
                    describe_key()
                ));
            }
        }
    }
    Ok((rows.len(), mismatches, examples))
}

// ---------------------------------------------------------------------------
// One database

struct DatabaseJob<'a> {
    report: DatabaseReport,
    is_control_plane: bool,
    target_url: String,
    allow_drop_columns: bool,
    allow_drop_tables: bool,
    keep_epoch: bool,
    sample_rows: usize,
    /// Source tables to ignore silently (the Control Plane's own tables when a
    /// tenant shares the Control Plane database).
    ignore_source: BTreeSet<String>,
    rewrites: BTreeMap<&'static str, Rewrite<'a>>,
}

async fn open_target(url: &str) -> Result<SqliteConnection> {
    let path = gonvex_store::database_path(url);
    let options = SqliteConnectOptions::from_str(&gonvex_store::database_url(&path))?
        .create_if_missing(false)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        // Checked explicitly (PRAGMA foreign_key_check) after the copy, so the
        // table order does not matter and violations are reported by row.
        .foreign_keys(false)
        .busy_timeout(std::time::Duration::from_secs(5))
        .pragma("cache_size", "-262144")
        .pragma("temp_store", "MEMORY")
        .with_regexp();
    Ok(SqliteConnection::connect_with(&options).await?)
}

async fn begin_snapshot(pool: &PgPool) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await?;
    Ok(transaction)
}

/// Source table feeding `target` (the legacy receipt table name is accepted).
fn source_for<'s>(target: &str, schema: &'s SourceSchema) -> Option<(&'s str, &'s [SourceColumn])> {
    let candidates: &[&str] = match target {
        "_gonvex_reducer_idempotency" => &[
            "_gonvex_reducer_idempotency",
            "_gonvex_mutation_idempotency",
        ],
        _ => std::slice::from_ref(&target),
    };
    candidates.iter().find_map(|name| {
        schema
            .get_key_value(*name)
            .map(|(name, columns)| (name.as_str(), columns.as_slice()))
    })
}

async fn copy_database(pool: &PgPool, job: &mut DatabaseJob<'_>) -> Result<()> {
    let mut source = begin_snapshot(pool).await?;
    let schema = source_schema(&mut source).await?;
    let mut target = open_target(&job.target_url).await?;
    if !job.is_control_plane && schema.contains_key("_gonvex_files") {
        gonvex_store::execute_script(&mut target, FILES_TABLE_DDL).await?;
    }
    let tables = target_tables(&mut target).await?;
    let target_names = tables
        .iter()
        .map(|t| t.name.clone())
        .collect::<BTreeSet<_>>();

    // Source tables that will not be copied must be accounted for.
    let mut consumed = BTreeSet::new();
    let mut work = Vec::new();
    for table in &tables {
        if never_copied(&table.name) {
            continue;
        }
        match source_for(&table.name, &schema) {
            Some((source_name, columns)) => {
                consumed.insert(source_name.to_owned());
                let source_empty: bool = !sqlx::query_scalar::<_, bool>(&format!(
                    "SELECT EXISTS (SELECT 1 FROM {})",
                    quote(source_name)
                ))
                .fetch_one(&mut *source)
                .await?;
                let plan = plan_table(
                    table,
                    source_name,
                    columns,
                    job.allow_drop_columns,
                    source_empty,
                )?;
                work.push((table, source_name.to_owned(), plan));
            }
            None => job.report.target_only_tables.push(table.name.clone()),
        }
    }
    let mut dropped_tables = Vec::new();
    for name in schema.keys() {
        if consumed.contains(name) {
            continue;
        }
        let reason = if job.ignore_source.contains(name) {
            Some("Control Plane table (copied into control.db)")
        } else if name == "_gonvex_sync_changes" || name == "_gonvex_sync_transactions" {
            Some("change-feed history is not carried over; clients resync by snapshot")
        } else if name == "_gonvex_sync_clock" {
            Some("revision carried over; epoch set explicitly")
        } else if name == "gonvex_migrations" {
            Some("Postgres migration ledger; the SQLite ledger records the SQLite migrations")
        } else if name.starts_with("_gonvex_reference_") {
            Some("reference ledger rebuilt from the catalog")
        } else if name == "_gonvex_mutation_idempotency"
            && target_names.contains("_gonvex_reducer_idempotency")
        {
            Some("legacy receipts; _gonvex_reducer_idempotency was copied instead")
        } else {
            None
        };
        match reason {
            Some(reason) => {
                job.report
                    .skipped_source_tables
                    .insert(name.clone(), reason.to_owned());
            }
            None => dropped_tables.push(name.clone()),
        }
    }
    if !dropped_tables.is_empty() {
        if !job.allow_drop_tables {
            return Err(format!(
                "{}: Postgres tables with no SQLite table: {}; add them to the SQLite migrations or pass --allow-drop-tables",
                job.report.target,
                dropped_tables.join(", ")
            )
            .into());
        }
        for name in dropped_tables {
            job.report
                .skipped_source_tables
                .insert(name, "no SQLite table (--allow-drop-tables)".to_owned());
        }
    }

    // Copy tables that application triggers write into after their writers.
    let application_triggers: Vec<(String, String)> = sqlx::query_as(APPLICATION_TRIGGERS)
        .fetch_all(&mut target)
        .await?;
    let names = work
        .iter()
        .map(|(table, _, _)| table.name.clone())
        .collect::<Vec<_>>();
    let (order, adjustments) = trigger_copy_order(&names, &application_triggers);
    job.report.copy_order_adjustments = adjustments;
    let mut slots = work.into_iter().map(Some).collect::<Vec<_>>();
    let work = order
        .into_iter()
        .map(|i| slots[i].take().expect("each table placed once"))
        .collect::<Vec<_>>();

    let mut transaction = target.begin().await?;
    let triggers: Vec<(String, String)> = sqlx::query_as(FRAMEWORK_TRIGGERS)
        .fetch_all(&mut *transaction)
        .await?;
    for (name, _) in &triggers {
        sqlx::query(&format!("DROP TRIGGER {}", quote(name)))
            .execute(&mut *transaction)
            .await?;
    }
    let mut copied = BTreeMap::new();
    for (table, source_name, plan) in &work {
        let rewrite = job.rewrites.get(table.name.as_str());
        let rows = copy_rows(
            &mut source,
            &mut transaction,
            table,
            source_name,
            plan,
            rewrite,
        )
        .await?;
        copied.insert(table.name.clone(), rows);
    }

    if !job.is_control_plane {
        // Change feed: no history, clock at the source revision.
        let source_clock: Option<(String, i64)> = if schema.contains_key("_gonvex_sync_clock") {
            sqlx::query_as("SELECT epoch, revision FROM _gonvex_sync_clock LIMIT 1")
                .fetch_optional(&mut *source)
                .await?
        } else {
            None
        };
        let revision = source_clock.as_ref().map_or(0, |(_, revision)| *revision);
        gonvex_store::execute_script(
            &mut transaction,
            "DELETE FROM _gonvex_sync_changes; DELETE FROM _gonvex_sync_transactions;",
        )
        .await?;
        let epoch = match (&source_clock, job.keep_epoch) {
            (Some((epoch, _)), true) => epoch.clone(),
            _ => {
                sqlx::query_scalar("SELECT lower(hex(randomblob(16)))")
                    .fetch_one(&mut *transaction)
                    .await?
            }
        };
        sqlx::query(
            "UPDATE _gonvex_sync_clock SET epoch=?, revision=?, retained_revision=? WHERE singleton=1",
        )
        .bind(&epoch)
        .bind(revision)
        .bind(revision)
        .execute(&mut *transaction)
        .await?;
        job.report.clock = Some(ClockReport {
            source_epoch: source_clock.map(|(epoch, _)| epoch),
            source_revision: revision,
            epoch,
            revision,
            retained_revision: revision,
        });
    }

    for (_, sql) in &triggers {
        gonvex_store::execute_script(&mut transaction, sql).await?;
    }

    if !job.is_control_plane {
        // Reference ledger: rebuilt by the store's declaration (inventory of
        // the copied rows), never copied, so it matches the SQLite engine.
        let target_catalog: Option<String> = sqlx::query_scalar(
            "SELECT declaration FROM _gonvex_reference_catalog WHERE singleton=1",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        let (source_catalog, source_edges) = if schema.contains_key("_gonvex_reference_catalog") {
            let catalog: Option<String> = sqlx::query_scalar(
                "SELECT declaration::text FROM _gonvex_reference_catalog LIMIT 1",
            )
            .fetch_optional(&mut *source)
            .await?;
            let edges: Option<i64> = if schema.contains_key("_gonvex_reference_edges") {
                Some(
                    sqlx::query_scalar("SELECT count(*) FROM _gonvex_reference_edges")
                        .fetch_one(&mut *source)
                        .await?,
                )
            } else {
                None
            };
            (catalog, edges)
        } else {
            (None, None)
        };
        let (origin, catalog) = match (target_catalog, source_catalog) {
            (Some(catalog), _) => ("target", Some(catalog)),
            (None, Some(catalog)) => ("source", Some(catalog)),
            (None, None) => ("none", None),
        };
        if let Some(catalog) = catalog {
            let catalog: Value = serde_json::from_str(&catalog)
                .map_err(|error| format!("reference catalog is not JSON: {error}"))?;
            gonvex_store::reference_integrity::declare(&mut transaction, &catalog)
                .await
                .map_err(|error| {
                    format!("{}: rebuild reference ledger: {error}", job.report.target)
                })?;
        }
        let target_edges: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_reference_edges")
            .fetch_one(&mut *transaction)
            .await?;
        let violations: i64 =
            sqlx::query_scalar("SELECT count(*) FROM gonvex_reference_violations")
                .fetch_one(&mut *transaction)
                .await?;
        if let Some(source_edges) = source_edges {
            if origin != "none" && source_edges != target_edges {
                job.report.findings.push(format!(
                    "warning: reference edges differ (Postgres {source_edges}, rebuilt {target_edges}); review if the catalogs differ"
                ));
            }
        }
        job.report.reference_ledger = Some(ReferenceReport {
            catalog: origin.to_owned(),
            source_edges,
            target_edges,
            violations,
        });
    }
    transaction.commit().await?;

    // Verification against the same Postgres snapshot.
    for (table, source_name, plan) in &work {
        let source_rows: i64 =
            sqlx::query_scalar(&format!("SELECT count(*) FROM {}", quote(source_name)))
                .fetch_one(&mut *source)
                .await?;
        let target_rows: i64 =
            sqlx::query_scalar(&format!("SELECT count(*) FROM {}", quote(&table.name)))
                .fetch_one(&mut target)
                .await?;
        let rewrite = job.rewrites.get(table.name.as_str());
        let (sampled, mismatches, examples) = sample_compare(
            &mut source,
            &mut target,
            table,
            source_name,
            plan,
            rewrite,
            job.sample_rows,
        )
        .await?;
        if source_rows != target_rows || copied.get(&table.name) != Some(&source_rows) {
            job.report.findings.push(format!(
                "{}: row count mismatch (Postgres {source_rows}, SQLite {target_rows})",
                table.name
            ));
        }
        job.report.findings.extend(examples);
        job.report.tables.push(TableReport {
            table: table.name.clone(),
            source_table: source_name.clone(),
            source_rows,
            target_rows,
            sampled_rows: sampled,
            sample_mismatches: mismatches,
            dropped_columns: plan.dropped.clone(),
            defaulted_columns: plan.defaulted.clone(),
            relaxed_for_empty_source: plan.relaxed.clone(),
        });
    }
    source.rollback().await?;

    let integrity: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(&mut target)
        .await?;
    job.report.integrity_check = integrity.join("; ");
    if job.report.integrity_check != "ok" {
        job.report
            .findings
            .push(format!("integrity_check: {}", job.report.integrity_check));
    }
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut target)
        .await?;
    for row in violations.iter().take(20) {
        let table: String = row.try_get_unchecked(0)?;
        let rowid: Option<i64> = row.try_get_unchecked(1)?;
        let parent: String = row.try_get_unchecked(2)?;
        job.report.foreign_key_violations.push(format!(
            "{table} rowid {} -> {parent}",
            rowid.map_or("?".to_owned(), |id| id.to_string())
        ));
    }
    if !violations.is_empty() {
        job.report
            .findings
            .push(format!("{} foreign key violation(s)", violations.len()));
    }
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut target)
        .await?;
    target.close().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Orchestration

fn remove_database_file(path: &Path) -> Result<()> {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        match std::fs::remove_file(&name) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!("remove {}: {error}", Path::new(&name).display()).into())
            }
        }
    }
    Ok(())
}

async fn connect_source(url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect(url)
        .await
        .map_err(|error| format!("connect {}: {error}", redact_url(url)).into())
}

async fn source_routes(pool: &PgPool) -> Result<(Vec<SourceProject>, Vec<SourceTenant>)> {
    let projects =
        sqlx::query("SELECT id::text, database_url::text FROM gonvex_runtime_projects ORDER BY id")
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| SourceProject {
                id: row.get(0),
                database_url: row.get::<Option<String>, _>(1).unwrap_or_default(),
            })
            .collect();
    let tenants = sqlx::query(
        r#"SELECT relationship_id::text, project_id::text, tenant_id::text, database_url::text,
                  (deleted_at IS NOT NULL OR status = 'deleted') AS deleted
           FROM gonvex_runtime_tenants ORDER BY project_id, tenant_id"#,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| SourceTenant {
        relationship_id: row.get(0),
        project_id: row.get(1),
        tenant_id: row.get(2),
        database_url: row.get::<Option<String>, _>(3).unwrap_or_default(),
        deleted: row.get(4),
    })
    .collect();
    Ok((projects, tenants))
}

fn migrations_for(
    options: &ExportOptions,
    cache: &mut BTreeMap<PathBuf, Vec<SqlMigration>>,
    project_id: &str,
) -> Result<Vec<SqlMigration>> {
    let directory = options.migrations.for_project(project_id).ok_or_else(|| {
        format!("no --migrations directory for project {project_id:?} (pass --migrations {project_id}=DIR or a default --migrations DIR)")
    })?;
    if !cache.contains_key(directory) {
        let migrations = load_migrations(directory)?;
        cache.insert(directory.to_path_buf(), migrations);
    }
    Ok(cache[directory].clone())
}

fn registry() -> PoolRegistry {
    PoolRegistry::new(PoolLimits::default())
}

fn route_of(database: &DatabasePlan) -> TenantRoute {
    TenantRoute {
        project_id: database.project_id.clone(),
        tenant_id: database
            .tenant_ids
            .first()
            .cloned()
            .unwrap_or_else(|| database.project_id.clone()),
        database_url: database.target_url.clone(),
    }
}

pub async fn export(options: &ExportOptions) -> Result<Report> {
    let data = DataDir::new(&options.data_dir);
    let _lock = data.lock().map_err(|error| format!("{error}"))?;
    let control_source = connect_source(&options.control_plane_url).await?;
    let (projects, tenants) = source_routes(&control_source).await?;
    let routes = plan_routes(&data, &projects, &tenants)?;
    let mut report = Report {
        data_dir: data.root().display().to_string(),
        source: redact_url(&options.control_plane_url),
        ..Report::default()
    };
    for filter in &options.projects {
        if !projects.iter().any(|project| &project.id == filter) {
            return Err(format!("--project {filter:?} is not in the Control Plane").into());
        }
    }
    for filter in &options.tenants {
        if !routes
            .databases
            .iter()
            .any(|database| database.tenant_ids.contains(filter))
        {
            return Err(
                format!("--tenant {filter:?} has no live database in the Control Plane").into(),
            );
        }
    }
    let (selected, skipped): (Vec<_>, Vec<_>) = routes
        .databases
        .iter()
        .partition(|database| database.selected(&options.projects, &options.tenants));
    report.not_exported = skipped
        .iter()
        .map(|database| database.target_url.clone())
        .collect();

    // Preflight: never overwrite without --force.
    let control_path = data.control_path();
    if options.no_control_plane {
        if !control_path.exists() {
            return Err(format!(
                "--no-control-plane needs an existing {}",
                control_path.display()
            )
            .into());
        }
    } else if control_path.exists() {
        if !options.force {
            return Err(format!(
                "{} already exists; pass --force to replace it",
                control_path.display()
            )
            .into());
        }
        remove_database_file(&control_path)?;
    }
    for database in &selected {
        if database.target_path.exists() {
            if !options.force {
                return Err(format!(
                    "{} already exists; pass --force to replace it",
                    database.target_path.display()
                )
                .into());
            }
            remove_database_file(&database.target_path)?;
        }
    }

    // 1. Schema, built by the runtime's own provisioning.
    let mut cache = BTreeMap::new();
    let mut database_migrations = Vec::new();
    for database in &selected {
        database_migrations.push(migrations_for(options, &mut cache, &database.project_id)?);
    }
    let control_url = data.control_url();
    {
        let control = ControlPlane::connect(&control_url, registry(), BTreeMap::new()).await?;
        if !options.no_control_plane {
            let mut seen = BTreeSet::new();
            let control_migrations = cache
                .values()
                .flatten()
                .filter(|migration| migration.scope == MigrationScope::ControlPlane)
                .filter(|migration| seen.insert(migration.name.clone()))
                .cloned()
                .collect::<Vec<_>>();
            control
                .clone()
                .apply_control_migrations(control_migrations)
                .await?;
        }
        for (database, migrations) in selected.iter().zip(&database_migrations) {
            let route = route_of(database);
            control
                .clone()
                .provision_tenant_database(route.clone(), migrations.clone())
                .await
                .map_err(|error| format!("provision {}: {error}", database.target_url))?;
            control.ensure_reducer_idempotency(&route).await?;
        }
        control.pools().close().await;
    }

    // 2. Control Plane rows.
    if !options.no_control_plane {
        let mut job = DatabaseJob {
            report: DatabaseReport {
                kind: "controlPlane".to_owned(),
                source: redact_url(&options.control_plane_url),
                target: control_url.clone(),
                ..DatabaseReport::default()
            },
            is_control_plane: true,
            target_url: control_url.clone(),
            allow_drop_columns: options.allow_drop_columns,
            allow_drop_tables: options.allow_drop_tables,
            keep_epoch: options.keep_epoch,
            sample_rows: options.sample_rows,
            ignore_source: BTreeSet::new(),
            rewrites: BTreeMap::from([
                (
                    "gonvex_runtime_projects",
                    Rewrite {
                        key_column: "id",
                        routes: &routes.projects,
                    },
                ),
                (
                    "gonvex_runtime_tenants",
                    Rewrite {
                        key_column: "relationship_id",
                        routes: &routes.tenants,
                    },
                ),
            ]),
        };
        copy_database(&control_source, &mut job).await?;
        report.control_plane = Some(job.report);
    }

    // Control Plane table names, ignored in a tenant database that shares the
    // Control Plane's Postgres database (development setups).
    let control_tables = {
        let mut connection = open_target(&control_url).await?;
        let names = target_tables(&mut connection)
            .await?
            .into_iter()
            .map(|table| table.name)
            .collect::<BTreeSet<_>>();
        connection.close().await?;
        names
    };

    // 3. Each project/tenant database.
    for database in &selected {
        let source_url = map_source_url(&database.source_url, &options.source_url_maps);
        let pool = if source_url == options.control_plane_url {
            control_source.clone()
        } else {
            connect_source(&source_url).await?
        };
        let mut job = DatabaseJob {
            report: DatabaseReport {
                kind: match database.kind {
                    DatabaseKind::Project => "project".to_owned(),
                    DatabaseKind::Tenant => "tenant".to_owned(),
                },
                project_id: database.project_id.clone(),
                tenant_ids: database.tenant_ids.clone(),
                source: redact_url(&source_url),
                target: database.target_url.clone(),
                ..DatabaseReport::default()
            },
            is_control_plane: false,
            target_url: database.target_url.clone(),
            allow_drop_columns: options.allow_drop_columns,
            allow_drop_tables: options.allow_drop_tables,
            keep_epoch: options.keep_epoch,
            sample_rows: options.sample_rows,
            ignore_source: if source_url == options.control_plane_url {
                control_tables.clone()
            } else {
                BTreeSet::new()
            },
            rewrites: BTreeMap::new(),
        };
        copy_database(&pool, &mut job).await?;
        if source_url != options.control_plane_url {
            pool.close().await;
        }
        report.databases.push(job.report);
    }
    control_source.close().await;

    // 4. The store's installer must accept every file as current (change
    //    capture and membership triggers installed, nothing staged).
    {
        let control = ControlPlane::connect(&control_url, registry(), BTreeMap::new()).await?;
        for ((database, migrations), database_report) in selected
            .iter()
            .zip(&database_migrations)
            .zip(report.databases.iter_mut())
        {
            control
                .clone()
                .provision_tenant_database(route_of(database), migrations.clone())
                .await
                .map_err(|error| format!("reinstall {}: {error}", database.target_url))?;
            let pool = control.pools().pool(&database.target_url).await?;
            let staged: i64 = sqlx::query_scalar("SELECT count(*) FROM _gonvex_sync_changes")
                .fetch_one(&pool)
                .await?;
            if staged != 0 {
                database_report
                    .findings
                    .push(format!("{staged} change-feed row(s) exist after the copy"));
            }
            let routed: Option<String> = sqlx::query_scalar(
                "SELECT database_url FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2",
            )
            .bind(&database.project_id)
            .bind(database.tenant_ids.first().map(String::as_str).unwrap_or(""))
            .fetch_optional(&control.pools().pool(&control_url).await?)
            .await?;
            if let Some(routed) = routed {
                if routed != database.target_url {
                    database_report.findings.push(format!(
                        "Control Plane routes tenant to {routed:?}, not {:?}",
                        database.target_url
                    ));
                }
            }
        }
        control.pools().close().await;
    }

    let mut findings = Vec::new();
    for database in report.control_plane.iter().chain(&report.databases) {
        for finding in &database.findings {
            if !finding.starts_with("warning:") {
                findings.push(format!("{}: {finding}", database.target));
            }
        }
    }
    report.findings = findings;
    report.ok = report.findings.is_empty();
    Ok(report)
}

/// CLI entry: runs the export, prints the JSON report, and fails when the
/// verification found any mismatch.
pub async fn run(args: &[String]) -> Result<()> {
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
    {
        println!("{}", usage());
        return Ok(());
    }
    let options = parse_options(args)?;
    let report = export(&options).await?;
    let text = serde_json::to_string_pretty(&report)?;
    if let Some(path) = &options.report_file {
        std::fs::write(path, format!("{text}\n"))
            .map_err(|error| format!("write {}: {error}", path.display()))?;
    }
    println!("{text}");
    if !report.ok {
        return Err(format!(
            "export-sqlite verification failed with {} finding(s)",
            report.findings.len()
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn parser_reads_filters_and_migration_dirs() {
        let options = parse_options(&args(&[
            "--control-plane-url",
            "postgres://u:p@db/control",
            "--data-dir",
            "/srv/data",
            "--migrations",
            "./migrations",
            "--migrations",
            "p2=/apps/p2/migrations",
            "--project",
            "p1",
            "--tenant",
            "t1",
            "--force",
            "--allow-drop-columns",
            "--sample-rows",
            "10",
            "--source-url-map",
            "postgres://u:p@postgres:5432=postgres://u:p@127.0.0.1:15432",
        ]))
        .unwrap();
        assert_eq!(
            options.migrations.for_project("p1"),
            Some(Path::new("./migrations"))
        );
        assert_eq!(
            options.migrations.for_project("p2"),
            Some(Path::new("/apps/p2/migrations"))
        );
        assert_eq!(options.projects, vec!["p1"]);
        assert_eq!(options.tenants, vec!["t1"]);
        assert!(options.force && options.allow_drop_columns && !options.allow_drop_tables);
        assert_eq!(options.sample_rows, 10);
        assert_eq!(options.source_url_maps.len(), 1);
    }

    #[test]
    fn parser_requires_postgres_source_data_dir_and_migrations() {
        let base = ["--data-dir", "/srv/data", "--migrations", "m"];
        let error = parse_options(&args(
            &[
                &["--control-plane-url", "sqlite:///srv/data/control.db"][..],
                &base[..],
            ]
            .concat(),
        ))
        .unwrap_err();
        assert!(error.to_string().contains("Postgres"));
        let error = parse_options(&args(&[
            "--control-plane-url",
            "postgres://db/c",
            "--migrations",
            "m",
        ]))
        .unwrap_err();
        assert!(error.to_string().contains("--data-dir"));
        let error = parse_options(&args(&[
            "--control-plane-url",
            "postgres://db/c",
            "--data-dir",
            "d",
        ]))
        .unwrap_err();
        assert!(error.to_string().contains("--migrations"));
        let error = parse_options(&args(&["--bogus"])).unwrap_err();
        assert!(error.to_string().contains("unknown option"));
    }

    #[test]
    fn framework_history_is_never_copied() {
        for table in [
            "_gonvex_sync_changes",
            "_gonvex_sync_transactions",
            "_gonvex_sync_clock",
            "gonvex_migrations",
            "_gonvex_reference_edges",
        ] {
            assert!(never_copied(table), "{table}");
        }
        for table in [
            "_gonvex_action_outbox",
            "_gonvex_reducer_idempotency",
            "_gonvex_control_plane_membership_outbox",
            "members",
            "gonvex_runtime_tenants",
        ] {
            assert!(!never_copied(table), "{table}");
        }
    }

    #[test]
    fn missing_and_dropped_columns_fail_loudly() {
        let target = TargetTable {
            name: "tasks".into(),
            columns: vec![
                TargetColumn {
                    name: "id".into(),
                    declared: "TEXT".into(),
                    not_null: true,
                    has_default: false,
                    pk: 1,
                },
                TargetColumn {
                    name: "done".into(),
                    declared: "BOOLEAN".into(),
                    not_null: true,
                    has_default: true,
                    pk: 0,
                },
                TargetColumn {
                    name: "title".into(),
                    declared: "TEXT".into(),
                    not_null: true,
                    has_default: false,
                    pk: 0,
                },
            ],
        };
        let column = |name: &str, data_type: &str| SourceColumn {
            name: name.into(),
            data_type: data_type.into(),
            udt_name: data_type.into(),
        };
        let plan = plan_table(
            &target,
            "tasks",
            &[column("id", "uuid"), column("title", "text")],
            false,
            false,
        )
        .unwrap();
        assert_eq!(plan.defaulted, vec!["done"]);
        assert_eq!(plan.columns[0].select, "\"id\"::text");
        let missing = plan_table(&target, "tasks", &[column("id", "uuid")], false, false);
        assert!(missing.err().unwrap().to_string().contains("NOT NULL"));
        let extra = [
            column("id", "uuid"),
            column("title", "text"),
            column("legacy", "text"),
        ];
        assert!(plan_table(&target, "tasks", &extra, false, false)
            .err()
            .unwrap()
            .to_string()
            .contains("--allow-drop-columns"));
        assert_eq!(
            plan_table(&target, "tasks", &extra, true, false)
                .unwrap()
                .dropped,
            vec!["legacy"]
        );

        // An empty source table: a required SQLite column missing from
        // Postgres and a Postgres-only column are recorded, not fatal.
        let renamed = [column("id", "uuid"), column("legacy_title", "text")];
        let relaxed = plan_table(&target, "tasks", &renamed, false, true).unwrap();
        assert_eq!(relaxed.dropped, vec!["legacy_title"]);
        assert_eq!(relaxed.defaulted, vec!["done"]);
        assert_eq!(relaxed.columns.len(), 1);
        assert_eq!(relaxed.relaxed.len(), 2);
        assert!(relaxed.relaxed[0].starts_with("title: NOT NULL"));
        assert!(relaxed.relaxed[1].starts_with("legacy_title: Postgres column"));
        // The same shapes with rows still stop the export.
        let error = plan_table(&target, "tasks", &renamed, false, false)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("title: NOT NULL"), "{error}");
        let error = plan_table(&target, "tasks", &extra, false, false)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("--allow-drop-columns"), "{error}");
    }

    #[test]
    fn trigger_writes_reads_only_the_body() {
        let label = r#"CREATE TRIGGER "task_grid_workspace_search_label_update" AFTER UPDATE OF "name" ON "workspaces" BEGIN
  UPDATE "taskGridRows" SET ("searchText") = (
    SELECT v."searchText" FROM "whTaskGridProjection" AS v WHERE v."taskId" = "taskGridRows"."taskId")
  WHERE "workspaceId" = NEW."_id" AND EXISTS (SELECT 1 FROM "tasks" AS t WHERE t."_id" = "taskGridRows"."taskId");
END"#;
        assert_eq!(trigger_writes(label), vec!["taskGridRows"]);
        let upsert = r#"CREATE TRIGGER t AFTER INSERT ON "tasks" BEGIN
  -- DELETE FROM commented_out
  INSERT INTO "taskGridRows" ("taskId") SELECT 'UPDATE literal' FROM x
    ON CONFLICT ("taskId") DO UPDATE SET "taskId" = excluded."taskId";
  DELETE FROM [taskRelatedSearchDocuments] WHERE 1 = 0;
  INSERT OR REPLACE INTO peopleDirectory SELECT 1;
END"#;
        assert_eq!(
            trigger_writes(upsert),
            vec![
                "taskGridRows",
                "taskRelatedSearchDocuments",
                "peopleDirectory"
            ]
        );
    }

    #[test]
    fn trigger_targets_are_copied_after_their_writers() {
        let names = [
            "taskGridRows",
            "tasks",
            "members",
            "workspaces",
            "peopleDirectory",
        ]
        .map(String::from);
        let triggers = vec![
            (
                "tasks".to_owned(),
                r#"CREATE TRIGGER a AFTER INSERT ON tasks BEGIN INSERT INTO "taskGridRows" SELECT 1; END"#.to_owned(),
            ),
            (
                "workspaces".to_owned(),
                r#"CREATE TRIGGER b AFTER UPDATE OF name ON workspaces BEGIN UPDATE "taskGridRows" SET x = 1; END"#.to_owned(),
            ),
            (
                "members".to_owned(),
                r#"CREATE TRIGGER c AFTER INSERT ON members BEGIN INSERT INTO peopleDirectory SELECT 1; END"#.to_owned(),
            ),
            // Not copied: ignored.
            (
                "elsewhere".to_owned(),
                r#"CREATE TRIGGER d AFTER INSERT ON elsewhere BEGIN DELETE FROM tasks; END"#.to_owned(),
            ),
        ];
        let (order, adjustments) = trigger_copy_order(&names, &triggers);
        let ordered = order.iter().map(|&i| names[i].as_str()).collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec![
                "tasks",
                "members",
                "workspaces",
                "taskGridRows",
                "peopleDirectory"
            ]
        );
        assert_eq!(
            adjustments,
            vec![
                "taskGridRows <- [tasks, workspaces]".to_owned(),
                "peopleDirectory <- [members]".to_owned()
            ]
        );

        // A cycle keeps the original order instead of failing.
        let names = ["a", "b"].map(String::from);
        let triggers = vec![
            (
                "a".to_owned(),
                "CREATE TRIGGER x AFTER INSERT ON a BEGIN INSERT INTO b SELECT 1; END".to_owned(),
            ),
            (
                "b".to_owned(),
                "CREATE TRIGGER y AFTER INSERT ON b BEGIN INSERT INTO a SELECT 1; END".to_owned(),
            ),
        ];
        assert_eq!(trigger_copy_order(&names, &triggers).0, vec![0, 1]);
    }
}
