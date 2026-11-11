//! `gonvex-tenant-stage`: the runtime half of the P03 tenant transfer.
//!
//! ```text
//! gonvex-tenant-stage stage  --source FILE --migrations DIR --out FILE [--keep-epoch]
//!                            [--project ID] [--tenant ID]
//! gonvex-tenant-stage accept --file FILE --migrations DIR [--project ID] [--tenant ID]
//! ```
//!
//! `stage` builds `--out` with the runtime's own provisioning
//! (`ControlPlane::provision_tenant_database` with the app's tenant
//! migrations, loaded by `gonvex-admin`'s loader) and copies the rows of
//! `--source` into it under the rules of docs/sqlite-storage.md, "Moving
//! existing data":
//!
//! - the migration ledger, change-feed history, sync clock and bootstrap, and
//!   the reference ledger are never copied (same list as
//!   `gonvex-admin export-sqlite`, `never_copied`);
//! - the clock keeps the source revision with `retained_revision = revision`,
//!   an empty `_gonvex_sync_changes`, and a new epoch unless `--keep-epoch`;
//! - the reference ledger is rebuilt with `reference_integrity::declare`;
//! - `_gonvex_files` is created with the runtime's own DDL when the source has it.
//!
//! The source is already SQLite, so every row a trigger derived (projections,
//! FTS content) is in the source too. All triggers are therefore suspended for
//! the copy and restored verbatim in the same transaction, and external-content
//! FTS5 indexes are rebuilt from their copied content. The outcome is the doc's
//! (each copied table equals the source exactly) without trigger ordering.
//!
//! `accept` runs the store's installer (`provision_tenant_database`) on a file
//! again and checks that it accepted the file as current: the schema did not
//! change, nothing was staged in the change feed, integrity and foreign keys
//! hold.
//!
//! Both print a JSON report on stdout and exit non-zero on any finding.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, SqlMigration, TenantRoute};
use serde::Serialize;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, Row};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

/// Mirrors `never_copied` in rust/crates/admin-cli/src/export_sqlite/mod.rs.
fn never_copied(table: &str) -> bool {
    matches!(
        table,
        "gonvex_migrations"
            | "_gonvex_sync_bootstrap"
            | "_gonvex_sync_changes"
            | "_gonvex_sync_transactions"
            | "_gonvex_sync_clock"
            | "gonvex_runtime_manifests"
    ) || table.starts_with("_gonvex_reference_")
}

fn skip_reason(table: &str) -> &'static str {
    match table {
        "gonvex_migrations" => "migration ledger records the migrations that built this file",
        "_gonvex_sync_changes" | "_gonvex_sync_transactions" => {
            "change-feed history is not carried over; clients resync by snapshot"
        }
        "_gonvex_sync_clock" => {
            "revision carried over, retained_revision = revision; epoch set explicitly"
        }
        "_gonvex_sync_bootstrap" => "installer receipts of this file's provisioning",
        "gonvex_runtime_manifests" => "module artifacts are redeployed, not copied",
        _ => "reference ledger rebuilt from the catalog",
    }
}

/// `_gonvex_files` is created lazily by the runtime on first upload. These
/// are its statements from rust/crates/runtime/src/storage.rs (`ensure_table`),
/// verbatim, so a staged file holds exactly what the runtime would create.
const FILES_TABLE_STATEMENTS: [&str; 3] = [
    r#"CREATE TABLE IF NOT EXISTS _gonvex_files (
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
)"#,
    "CREATE INDEX IF NOT EXISTS _gonvex_files_tenant_idx ON _gonvex_files (tenant_id)",
    "CREATE INDEX IF NOT EXISTS _gonvex_files_owner_idx ON _gonvex_files (owner_id)",
];

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct StageReport {
    source: String,
    out: String,
    provision_ms: u128,
    copy_ms: u128,
    copied_tables: BTreeMap<String, i64>,
    skipped_tables: BTreeMap<String, String>,
    template_reused: bool,
    lazily_created: Vec<String>,
    triggers_suspended: usize,
    fts_rebuilt: Vec<String>,
    sequences: BTreeMap<String, i64>,
    clock: Option<ClockReport>,
    reference_ledger: Option<ReferenceReport>,
    integrity_check: String,
    foreign_key_violations: Vec<String>,
    accept: Option<AcceptReport>,
    /// Reported only.
    warnings: Vec<String>,
    /// Any finding fails the run.
    findings: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClockReport {
    source_epoch: Option<String>,
    source_revision: i64,
    source_retained_revision: i64,
    epoch: String,
    revision: i64,
    retained_revision: i64,
    source_change_rows: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReferenceReport {
    catalog: &'static str,
    source_edges: Option<i64>,
    edges: i64,
    violations: i64,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct AcceptReport {
    file: String,
    install_ms: u128,
    schema_objects: usize,
    schema_unchanged: bool,
    staged_change_rows: i64,
    integrity_check: String,
    foreign_key_violations: usize,
    migrations: usize,
    findings: Vec<String>,
}

struct Args {
    command: String,
    values: BTreeMap<String, String>,
    flags: BTreeSet<String>,
}

fn parse_args() -> Result<Args> {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_default();
    let mut values = BTreeMap::new();
    let mut flags = BTreeSet::new();
    while let Some(arg) = args.next() {
        let Some(name) = arg.strip_prefix("--") else {
            return Err(format!("unexpected argument {arg:?}").into());
        };
        if name == "keep-epoch" {
            flags.insert(name.to_owned());
        } else {
            let value = args
                .next()
                .ok_or_else(|| format!("--{name} needs a value"))?;
            values.insert(name.to_owned(), value);
        }
    }
    Ok(Args {
        command,
        values,
        flags,
    })
}

impl Args {
    fn path(&self, name: &str) -> Result<PathBuf> {
        let value = self
            .values
            .get(name)
            .ok_or_else(|| format!("--{name} is required"))?;
        Ok(std::path::absolute(value)?)
    }
    fn value(&self, name: &str, default: &str) -> String {
        self.values
            .get(name)
            .cloned()
            .unwrap_or_else(|| default.to_owned())
    }
}

#[tokio::main]
async fn main() {
    let outcome = async {
        let args = parse_args()?;
        match args.command.as_str() {
            "stage" => {
                let report = stage(&args).await?;
                println!("{}", serde_json::to_string_pretty(&report)?);
                Ok::<bool, Error>(report.findings.is_empty())
            }
            "accept" => {
                let report = accept(
                    &args.path("file")?,
                    load_migrations(&args.path("migrations")?)?,
                    &args.value("project", "p03"),
                    &args.value("tenant", "p03"),
                )
                .await?;
                println!("{}", serde_json::to_string_pretty(&report)?);
                Ok(report.findings.is_empty())
            }
            _ => Err("usage: gonvex-tenant-stage stage|accept (see src/main.rs)".into()),
        }
    }
    .await;
    match outcome {
        Ok(true) => {}
        Ok(false) => std::process::exit(2),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn load_migrations(directory: &Path) -> Result<Vec<SqlMigration>> {
    gonvex_admin::export_sqlite::plan::load_migrations(directory)
        .map_err(|error| error.to_string().into())
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn open(path: &Path, foreign_keys: bool) -> Result<SqliteConnection> {
    let options = SqliteConnectOptions::from_str(&gonvex_store::database_url(path))?
        .create_if_missing(false)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        // As in gonvex-admin export-sqlite: checked explicitly
        // (PRAGMA foreign_key_check) after the copy.
        .foreign_keys(foreign_keys)
        .busy_timeout(std::time::Duration::from_secs(5))
        .pragma("cache_size", "-262144")
        .pragma("temp_store", "MEMORY")
        .with_regexp();
    Ok(SqliteConnection::connect_with(&options).await?)
}

/// Runtime steps run on a tenant file through a throwaway Control Plane.
#[derive(Clone, Copy)]
enum Install {
    /// `provision_tenant_database`, twice: the first pass on a new file ends
    /// before the tenant identity triggers are back (the baseline migration
    /// rebuilds `members`), the second is what every later open does. Same
    /// as gonvex-admin export-sqlite, which reinstalls after its copy.
    Provision,
    /// `provision_tenant_database` once (the acceptance check).
    Reinstall,
    /// `ensure_reducer_idempotency`: the runtime creates this table lazily.
    ReducerReceipts,
}

async fn install(
    file: &Path,
    migrations: &[SqlMigration],
    project: &str,
    tenant: &str,
    step: Install,
) -> Result<()> {
    let scratch = file
        .parent()
        .ok_or("file has no parent directory")?
        .join(format!(".stage-control-{}", std::process::id()));
    std::fs::create_dir_all(&scratch)?;
    let result = async {
        let registry = PoolRegistry::new(PoolLimits::default());
        let control = ControlPlane::connect(
            &gonvex_store::database_url(&scratch.join("control.db")),
            registry.clone(),
            BTreeMap::new(),
        )
        .await?;
        let route = TenantRoute {
            project_id: project.to_owned(),
            tenant_id: tenant.to_owned(),
            database_url: gonvex_store::database_url(file),
        };
        let outcome = async {
            match step {
                Install::Provision => {
                    for _ in 0..2 {
                        control
                            .clone()
                            .provision_tenant_database(route.clone(), migrations.to_vec())
                            .await?;
                    }
                }
                Install::Reinstall => {
                    control
                        .clone()
                        .provision_tenant_database(route.clone(), migrations.to_vec())
                        .await?
                }
                Install::ReducerReceipts => control.ensure_reducer_idempotency(&route).await?,
            }
            Ok::<(), Error>(())
        }
        .await;
        registry.close().await;
        outcome?;
        // Fold the WAL into the file so a plain copy of it is complete.
        let mut connection = open(file, false).await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        Ok(())
    }
    .await;
    std::fs::remove_dir_all(&scratch).ok();
    result
}

/// A provisioned, still empty tenant file for `migrations`, built once and
/// reflink-copied for every stage (provisioning runs ~14 MB of migrations).
async fn provisioned_copy(
    out: &Path,
    template: Option<&Path>,
    migrations: &[SqlMigration],
    project: &str,
    tenant: &str,
) -> Result<bool> {
    let Some(template) = template else {
        install(out, migrations, project, tenant, Install::Provision).await?;
        return Ok(false);
    };
    let fingerprint = migrations
        .iter()
        .map(|migration| format!("{}:{}", migration.name, migration.checksum))
        .collect::<Vec<_>>()
        .join("\n");
    let stamp = template.with_extension("migrations");
    let reused = template.is_file()
        && std::fs::read_to_string(&stamp).ok().as_deref() == Some(fingerprint.as_str());
    if !reused {
        for suffix in ["", "-wal", "-shm"] {
            std::fs::remove_file(format!("{}{suffix}", template.display())).ok();
        }
        install(template, migrations, project, tenant, Install::Provision).await?;
        std::fs::write(&stamp, &fingerprint)?;
    }
    // copy_file_range: a reflink on Btrfs/XFS.
    std::fs::copy(template, out)?;
    Ok(reused)
}

#[derive(Debug, PartialEq)]
struct SchemaObject {
    kind: String,
    name: String,
    sql: Option<String>,
}

async fn schema(connection: &mut SqliteConnection, database: &str) -> Result<Vec<SchemaObject>> {
    let rows = sqlx::query(&format!(
        // sqlite_stat1/4 are planner statistics the registry's ANALYZE at open
        // writes; they are not schema.
        "SELECT type, name, sql FROM {database}.sqlite_schema WHERE name NOT LIKE 'sqlite\\_stat%' ESCAPE '\\' ORDER BY type, name"
    ))
    .fetch_all(&mut *connection)
    .await?;
    Ok(rows
        .iter()
        .map(|row| SchemaObject {
            kind: row.get(0),
            name: row.get(1),
            sql: row.get(2),
        })
        .collect())
}

/// Ordinary tables (no virtual, shadow or sqlite_* tables) of a schema.
async fn ordinary_tables(connection: &mut SqliteConnection, database: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT name FROM pragma_table_list WHERE schema = ? AND type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
    )
    .bind(database)
    .fetch_all(&mut *connection)
    .await?)
}

struct Columns {
    names: Vec<String>,
    /// INSERT/SELECT list that carries the rowid of rowid tables whose rowid
    /// is not aliased by an INTEGER PRIMARY KEY column.
    copy_list: String,
}

async fn columns(
    connection: &mut SqliteConnection,
    database: &str,
    table: &str,
) -> Result<Columns> {
    let rows = sqlx::query(&format!(
        "SELECT name, type, pk, hidden FROM {database}.pragma_table_xinfo(?) ORDER BY cid"
    ))
    .bind(table)
    .fetch_all(&mut *connection)
    .await?;
    let ddl: String = sqlx::query_scalar(&format!(
        "SELECT sql FROM {database}.sqlite_schema WHERE type='table' AND name=?"
    ))
    .bind(table)
    .fetch_one(&mut *connection)
    .await?;
    let without_rowid = ddl
        .to_ascii_uppercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .contains("WITHOUT ROWID");
    let names: Vec<String> = rows
        .iter()
        .filter(|row| row.get::<i64, _>(3) == 0)
        .map(|row| row.get(0))
        .collect();
    let pk: Vec<(String, String)> = rows
        .iter()
        .filter(|row| row.get::<i64, _>(2) > 0)
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let aliased = pk.len() == 1 && pk[0].1.eq_ignore_ascii_case("INTEGER");
    let mut list = names.iter().map(|name| quote(name)).collect::<Vec<_>>();
    if !without_rowid && !aliased {
        list.insert(0, "rowid".to_owned());
    }
    Ok(Columns {
        names,
        copy_list: list.join(", "),
    })
}

async fn stage(args: &Args) -> Result<StageReport> {
    let source = args.path("source")?;
    let out = args.path("out")?;
    let migrations = load_migrations(&args.path("migrations")?)?;
    let project = args.value("project", "p03");
    let tenant = args.value("tenant", "p03");
    let keep_epoch = args.flags.contains("keep-epoch");
    if out.exists() {
        return Err(format!("{} exists; stage writes a new file", out.display()).into());
    }
    let mut report = StageReport {
        source: source.display().to_string(),
        out: out.display().to_string(),
        ..StageReport::default()
    };

    let started = Instant::now();
    let template = args.values.get("template").map(PathBuf::from);
    report.template_reused =
        provisioned_copy(&out, template.as_deref(), &migrations, &project, &tenant).await?;
    // Tables the runtime creates on first use, through the runtime's own code.
    {
        let mut probe = open(&source, false).await?;
        let lazy: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name IN ('_gonvex_reducer_idempotency','_gonvex_mutation_idempotency')",
        )
        .fetch_all(&mut probe)
        .await?;
        probe.close().await?;
        if !lazy.is_empty() {
            install(
                &out,
                &migrations,
                &project,
                &tenant,
                Install::ReducerReceipts,
            )
            .await?;
            report
                .lazily_created
                .push("_gonvex_reducer_idempotency".to_owned());
        }
    }
    report.provision_ms = started.elapsed().as_millis();

    let started = Instant::now();
    let mut target = open(&out, false).await?;
    // Read-only attach when URI filenames are enabled; otherwise a plain
    // attach (the copy only ever reads from it).
    let read_only = sqlx::query("ATTACH DATABASE ? AS src")
        .bind(format!("file:{}?mode=ro", source.display()))
        .execute(&mut target)
        .await;
    let attached: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pragma_database_list WHERE name='src' AND file <> ''",
    )
    .fetch_one(&mut target)
    .await?;
    if read_only.is_err() || attached == 0 {
        if read_only.is_ok() {
            sqlx::query("DETACH DATABASE src")
                .execute(&mut target)
                .await?;
        }
        sqlx::query("ATTACH DATABASE ? AS src")
            .bind(source.display().to_string())
            .execute(&mut target)
            .await?;
    }

    let source_tables: BTreeSet<String> = ordinary_tables(&mut target, "src")
        .await?
        .into_iter()
        .collect();
    if source_tables.contains("_gonvex_files") {
        let exists: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM main.sqlite_schema WHERE type='table' AND name='_gonvex_files'",
        )
        .fetch_one(&mut target)
        .await?;
        if exists == 0 {
            for statement in FILES_TABLE_STATEMENTS {
                gonvex_store::execute_script(&mut target, statement).await?;
            }
            report.lazily_created.push("_gonvex_files".to_owned());
        }
    }
    let target_tables = ordinary_tables(&mut target, "main").await?;
    let target_set: BTreeSet<String> = target_tables.iter().cloned().collect();
    for name in &source_tables {
        if !target_set.contains(name) && !never_copied(name) {
            report.findings.push(format!(
                "{name}: source table has no table in the provisioned schema"
            ));
        }
    }

    let mut transaction = target.begin().await?;
    let triggers: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, sql FROM main.sqlite_schema WHERE type='trigger' AND sql IS NOT NULL ORDER BY rowid",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for (name, _) in &triggers {
        sqlx::query(&format!("DROP TRIGGER main.{}", quote(name)))
            .execute(&mut *transaction)
            .await?;
    }
    report.triggers_suspended = triggers.len();

    for table in &target_tables {
        if never_copied(table) {
            report
                .skipped_tables
                .insert(table.clone(), skip_reason(table).to_owned());
            continue;
        }
        if !source_tables.contains(table) {
            report.skipped_tables.insert(
                table.clone(),
                "not in the source; provisioned rows kept".to_owned(),
            );
            continue;
        }
        let target_columns = columns(&mut transaction, "main", table).await?;
        let source_columns = columns(&mut transaction, "src", table).await?;
        let mut left = target_columns.names.clone();
        let mut right = source_columns.names.clone();
        left.sort();
        right.sort();
        let rowid = |list: &str| list.starts_with("rowid");
        if left != right || rowid(&target_columns.copy_list) != rowid(&source_columns.copy_list) {
            report.findings.push(format!(
                "{table}: columns differ (provisioned {:?}, source {:?})",
                target_columns.names, source_columns.names
            ));
            continue;
        }
        let quoted = quote(table);
        sqlx::query(&format!("DELETE FROM main.{quoted}"))
            .execute(&mut *transaction)
            .await?;
        let copied = sqlx::query(&format!(
            "INSERT INTO main.{quoted} ({list}) SELECT {list} FROM src.{quoted}",
            list = target_columns.copy_list
        ))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        report.copied_tables.insert(table.clone(), copied as i64);
    }

    // AUTOINCREMENT high-water marks of copied tables.
    let has_source_sequence: i64 =
        sqlx::query_scalar("SELECT count(*) FROM src.sqlite_schema WHERE name='sqlite_sequence'")
            .fetch_one(&mut *transaction)
            .await?;
    if has_source_sequence > 0 {
        let sequences: Vec<(String, i64)> =
            sqlx::query_as("SELECT name, seq FROM src.sqlite_sequence ORDER BY name")
                .fetch_all(&mut *transaction)
                .await?;
        for (name, seq) in sequences {
            if !report.copied_tables.contains_key(&name) {
                continue;
            }
            sqlx::query("DELETE FROM main.sqlite_sequence WHERE name=?")
                .bind(&name)
                .execute(&mut *transaction)
                .await?;
            sqlx::query("INSERT INTO main.sqlite_sequence(name, seq) VALUES (?, ?)")
                .bind(&name)
                .bind(seq)
                .execute(&mut *transaction)
                .await?;
            report.sequences.insert(name, seq);
        }
    }

    // Change feed: no history, clock at the source revision.
    let source_clock: Option<(String, i64, i64)> = if source_tables.contains("_gonvex_sync_clock") {
        sqlx::query_as("SELECT epoch, revision, retained_revision FROM src._gonvex_sync_clock WHERE singleton=1")
            .fetch_optional(&mut *transaction)
            .await?
    } else {
        None
    };
    let source_change_rows: i64 = if source_tables.contains("_gonvex_sync_changes") {
        sqlx::query_scalar("SELECT count(*) FROM src._gonvex_sync_changes")
            .fetch_one(&mut *transaction)
            .await?
    } else {
        0
    };
    let revision = source_clock.as_ref().map_or(0, |clock| clock.1);
    gonvex_store::execute_script(
        &mut transaction,
        "DELETE FROM main._gonvex_sync_changes; DELETE FROM main._gonvex_sync_transactions;",
    )
    .await?;
    let epoch: String = match (&source_clock, keep_epoch) {
        (Some((epoch, _, _)), true) => epoch.clone(),
        _ => {
            sqlx::query_scalar("SELECT lower(hex(randomblob(16)))")
                .fetch_one(&mut *transaction)
                .await?
        }
    };
    sqlx::query("UPDATE main._gonvex_sync_clock SET epoch=?, revision=?, retained_revision=? WHERE singleton=1")
        .bind(&epoch)
        .bind(revision)
        .bind(revision)
        .execute(&mut *transaction)
        .await?;
    report.clock = Some(ClockReport {
        source_epoch: source_clock.as_ref().map(|clock| clock.0.clone()),
        source_revision: revision,
        source_retained_revision: source_clock.as_ref().map_or(0, |clock| clock.2),
        epoch,
        revision,
        retained_revision: revision,
        source_change_rows,
    });

    for (_, sql) in &triggers {
        gonvex_store::execute_script(&mut transaction, sql).await?;
    }

    // External-content FTS5 indexes: rebuilt from the copied content.
    let virtual_tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT l.name, s.sql FROM pragma_table_list AS l JOIN main.sqlite_schema AS s ON s.name = l.name
         WHERE l.schema='main' AND l.type='virtual' ORDER BY l.name",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for (name, sql) in virtual_tables {
        let lowered = sql.to_ascii_lowercase();
        if lowered.contains("using fts5")
            && lowered.contains("content=")
            && !lowered.contains("content=''")
        {
            sqlx::query(&format!(
                "INSERT INTO main.{q}({q}) VALUES('rebuild')",
                q = quote(&name)
            ))
            .execute(&mut *transaction)
            .await?;
            report.fts_rebuilt.push(name);
        } else {
            report.findings.push(format!(
                "{name}: virtual table that is not external-content FTS5 is not supported"
            ));
        }
    }

    // Reference ledger: rebuilt by the store's declaration (inventory of the
    // copied rows), as gonvex-admin export-sqlite does.
    let target_catalog: Option<String> = sqlx::query_scalar(
        "SELECT declaration FROM main._gonvex_reference_catalog WHERE singleton=1",
    )
    .fetch_optional(&mut *transaction)
    .await?;
    let (source_catalog, source_edges) = if source_tables.contains("_gonvex_reference_catalog") {
        let catalog: Option<String> = sqlx::query_scalar(
            "SELECT declaration FROM src._gonvex_reference_catalog WHERE singleton=1",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        let edges: Option<i64> = if source_tables.contains("_gonvex_reference_edges") {
            Some(
                sqlx::query_scalar("SELECT count(*) FROM src._gonvex_reference_edges")
                    .fetch_one(&mut *transaction)
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
        let catalog: Value = serde_json::from_str(&catalog)?;
        gonvex_store::reference_integrity::declare(&mut transaction, &catalog)
            .await
            .map_err(|error| format!("rebuild reference ledger: {error}"))?;
    }
    let edges: i64 = sqlx::query_scalar("SELECT count(*) FROM main._gonvex_reference_edges")
        .fetch_one(&mut *transaction)
        .await?;
    let violations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM main.gonvex_reference_violations")
            .fetch_one(&mut *transaction)
            .await?;
    if let Some(source_edges) = source_edges {
        if source_edges != edges {
            report.warnings.push(format!(
                "reference edges differ (source {source_edges}, rebuilt {edges})"
            ));
        }
    }
    report.reference_ledger = Some(ReferenceReport {
        catalog: origin,
        source_edges,
        edges,
        violations,
    });
    transaction.commit().await?;
    sqlx::query("DETACH DATABASE src")
        .execute(&mut target)
        .await?;

    let integrity: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(&mut target)
        .await?;
    report.integrity_check = integrity.join("; ");
    if report.integrity_check != "ok" {
        report
            .findings
            .push(format!("integrity_check: {}", report.integrity_check));
    }
    for row in sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut target)
        .await?
    {
        let table: String = row.get(0);
        let rowid: Option<i64> = row.get(1);
        let parent: String = row.get(2);
        report.foreign_key_violations.push(format!(
            "{table} rowid {} -> {parent}",
            rowid.map_or("?".into(), |id| id.to_string())
        ));
    }
    if !report.foreign_key_violations.is_empty() {
        report.findings.push(format!(
            "{} foreign key violation(s)",
            report.foreign_key_violations.len()
        ));
        report.foreign_key_violations.truncate(20);
    }
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut target)
        .await?;
    target.close().await?;
    report.copy_ms = started.elapsed().as_millis();

    let accepted = accept(&out, migrations, &project, &tenant).await?;
    report.findings.extend(
        accepted
            .findings
            .iter()
            .map(|finding| format!("accept: {finding}")),
    );
    report.accept = Some(accepted);
    Ok(report)
}

/// The store's installer must accept the file as current: it runs again and
/// changes nothing.
async fn accept(
    file: &Path,
    migrations: Vec<SqlMigration>,
    project: &str,
    tenant: &str,
) -> Result<AcceptReport> {
    let mut report = AcceptReport {
        file: file.display().to_string(),
        migrations: migrations.len(),
        ..AcceptReport::default()
    };
    if !file.is_file() {
        return Err(format!("{} does not exist", file.display()).into());
    }
    let mut connection = open(file, true).await?;
    let before = schema(&mut connection, "main").await?;
    connection.close().await?;

    let started = Instant::now();
    install(file, &migrations, project, tenant, Install::Reinstall).await?;
    report.install_ms = started.elapsed().as_millis();

    let mut connection = open(file, true).await?;
    let after = schema(&mut connection, "main").await?;
    report.schema_objects = after.len();
    report.schema_unchanged = before == after;
    if !report.schema_unchanged {
        let before_names: BTreeSet<_> = before
            .iter()
            .map(|o| (o.kind.clone(), o.name.clone(), o.sql.clone()))
            .collect();
        let after_names: BTreeSet<_> = after
            .iter()
            .map(|o| (o.kind.clone(), o.name.clone(), o.sql.clone()))
            .collect();
        let added: Vec<_> = after_names
            .difference(&before_names)
            .map(|o| format!("{} {}", o.0, o.1))
            .take(10)
            .collect();
        let removed: Vec<_> = before_names
            .difference(&after_names)
            .map(|o| format!("{} {}", o.0, o.1))
            .take(10)
            .collect();
        report.findings.push(format!(
            "the installer changed the schema (added {added:?}, removed {removed:?})"
        ));
    }
    let applied: BTreeMap<String, String> =
        sqlx::query_as("SELECT name, checksum FROM gonvex_migrations")
            .fetch_all(&mut connection)
            .await?
            .into_iter()
            .collect();
    for migration in &migrations {
        if !applied.contains_key(&migration.name) {
            report
                .findings
                .push(format!("migration {} is not in the ledger", migration.name));
        }
    }
    report.staged_change_rows =
        sqlx::query_scalar("SELECT count(*) FROM _gonvex_sync_changes WHERE revision IS NULL")
            .fetch_one(&mut connection)
            .await?;
    if report.staged_change_rows != 0 {
        report.findings.push(format!(
            "{} unrevisioned change row(s)",
            report.staged_change_rows
        ));
    }
    let integrity: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(&mut connection)
        .await?;
    report.integrity_check = integrity.join("; ");
    if report.integrity_check != "ok" {
        report
            .findings
            .push(format!("integrity_check: {}", report.integrity_check));
    }
    report.foreign_key_violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut connection)
        .await?
        .len();
    if report.foreign_key_violations != 0 {
        report.findings.push(format!(
            "{} foreign key violation(s)",
            report.foreign_key_violations
        ));
    }
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    Ok(report)
}
