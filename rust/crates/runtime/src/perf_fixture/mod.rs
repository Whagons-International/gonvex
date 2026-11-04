//! Shared benchmark fixture: a real [`Runtime`] over Whagons-shaped SQLite
//! tenants, with a fake in-process module host instead of V8.
//!
//! Enabled by the `perf-fixture` cargo feature (never in release builds).
//! See `docs/performance/perf-fixture.md` for how to run and extend benches.
//!
//! ```ignore
//! let fixture = Fixture::build(FixtureOptions::whagons()).await?;
//! let big = 0; // tenant index; FixtureOptions::whagons() puts the 9k tenant first
//! let mut members = fixture.connect_all(big).await?;          // 78 x 10 subscriptions
//! let event = fixture.update_task(big, 17, json!({"description": "x"})).await?;
//! let delivery = fixture.deliver(&mut members, &event, Encode::Json).await?;
//! let reply = fixture.call_reducer(&fixture.session(big, 3), "presence.beat", json!({})).await;
//! ```
//!
//! What is real: `Runtime::start`, module install (Load/Activate over the
//! module-host protocol), the Whagons replica collection definitions and
//! version 2 visibility plans (`whagons_module.json`, extracted from the
//! Whagons manifest), the Whagons table schemas (`whagons_schema.sql`),
//! change capture, `open_replica`, `apply_feed_event`, and the websocket's
//! reducer entry (`call_reducer`). What is not: V8 (reducers are scripted in
//! [`fake_host`]), the websocket transport itself, and the Whagons task-grid
//! projection triggers (`migrations/0001_sqlite_projections.sql`).
//!
//! The SQLite files are built once per [`FIXTURE_VERSION`] and options into
//! `~/.cache/gonvex-perf-fixture/` (or `$GONVEX_PERF_FIXTURE_DIR`) and copied
//! (reflinked on btrfs) into a fresh run directory for every [`Fixture::build`].

pub mod data;
pub mod fake_host;
pub mod harness;
pub mod stalls;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use gonvex_protocol::{ExecutionScope, ReducerCallRequest, ReplicaOpenRequest, ServerMessage};
use gonvex_store::{
    Account, ControlPlane, Member, MigrationScope, PoolLimits, PoolRegistry, RuntimeManifestRecord,
    SessionIdentity, SqlMigration, TenantRoute, TenantSession,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Connection as _;

use crate::change_feed::{self, FeedEvent, LogChange};
use crate::config::Config;
use crate::replica::{ReplicaOpenResult, ReplicaSubscription};
use crate::Runtime;

pub use data::{FixtureMember, FixtureTask, MemberKind, SqlValue};
pub use fake_host::{FakeModuleHost, JsTimeMode};

/// Bump whenever the schema, module JSON or data generator changes: it keys
/// the cached SQLite template.
pub const FIXTURE_VERSION: u32 = 2;
/// Project id of the fixture module.
pub const PROJECT_ID: &str = "whagons";
const SCHEMA_SQL: &str = include_str!("whagons_schema.sql");
const MODULE_JSON: &str = include_str!("whagons_module.json");
const MIGRATION_NAME: &str = "0000_whagons_fixture.sql";

/// The replica collections one Whagons user opens (`bench/whagons/run.ts`),
/// in the order the client opens them.
pub const WHAGONS_SUBSCRIPTIONS: [&str; 10] = [
    "presence.list",
    "workspaces.list",
    "statuses.statuses",
    "tags.tags",
    "members.listMembers",
    "members.listMemberTeams",
    "taskResources.taskUsers",
    "taskResources.taskTags",
    "workspacePreferences.workspacePreferences",
    "tasks.recentWorkspaceTasks",
];

/// A cold login: [`WHAGONS_SUBSCRIPTIONS`] plus the notifications list (the
/// 11 `watchReplica` collections of a Whagons user).
pub const WHAGONS_COLD_LOGIN: [&str; 11] = [
    "presence.list",
    "workspaces.list",
    "statuses.statuses",
    "tags.tags",
    "members.listMembers",
    "members.listMemberTeams",
    "taskResources.taskUsers",
    "taskResources.taskTags",
    "workspacePreferences.workspacePreferences",
    "tasks.recentWorkspaceTasks",
    "notifications.listNotifications",
];

/// The 8 collections of a task detail window (`bench/whagons/run.ts`) and
/// the argument naming the task.
pub const WHAGONS_DETAIL_WINDOW: [(&str, &str); 8] = [
    ("tasks.get", "id"),
    ("taskResources.listTaskNotes", "taskId"),
    ("taskResources.listTaskLogs", "taskId"),
    ("taskResources.listTaskViewsByTaskId", "taskId"),
    ("approvals.listTaskApprovals", "taskId"),
    ("taskFindings.listForTask", "taskId"),
    ("taskResources.listTaskUsers", "taskId"),
    ("customFields.listTaskValues", "taskId"),
];

pub type FixtureResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// One tenant shard of the fixture.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TenantSpec {
    pub id: String,
    pub tasks: usize,
    pub members: usize,
    /// Approximate task `description` length in bytes.
    pub description_bytes: usize,
}

impl TenantSpec {
    pub fn new(id: &str, tasks: usize, members: usize) -> Self {
        Self {
            id: id.to_owned(),
            tasks,
            members,
            description_bytes: 400,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FixtureOptions {
    pub tenants: Vec<TenantSpec>,
    pub seed: u64,
    /// Template cache root (default `$GONVEX_PERF_FIXTURE_DIR` or
    /// `~/.cache/gonvex-perf-fixture`).
    pub cache_dir: Option<PathBuf>,
    /// Runtime configuration overrides by environment variable name (e.g.
    /// `GONVEX_DB_MAX_OPEN_CONNS`). `GONVEX_*` variables from the process
    /// environment apply too; these win.
    pub config: BTreeMap<String, String>,
}

impl FixtureOptions {
    /// The shared shape: one 9,000-task tenant with 78 members, plus three
    /// small tenants (200 tasks, 5 members). Tenant 0 is the big one.
    pub fn whagons() -> Self {
        Self {
            tenants: vec![
                TenantSpec::new("big", 9_000, 78),
                TenantSpec::new("small-1", 200, 5),
                TenantSpec::new("small-2", 200, 5),
                TenantSpec::new("small-3", 200, 5),
            ],
            seed: 0x5eed_2026,
            cache_dir: None,
            config: BTreeMap::new(),
        }
    }

    /// A quick shape for tests: 400 tasks / 12 members and one small tenant.
    pub fn small() -> Self {
        Self {
            tenants: vec![
                TenantSpec::new("big", 400, 12),
                TenantSpec::new("small-1", 50, 5),
            ],
            ..Self::whagons()
        }
    }

    fn cache_root(&self) -> PathBuf {
        if let Some(dir) = &self.cache_dir {
            return dir.clone();
        }
        if let Some(dir) = std::env::var_os("GONVEX_PERF_FIXTURE_DIR") {
            return PathBuf::from(dir);
        }
        let home = std::env::var_os("HOME").unwrap_or_else(|| ".".into());
        PathBuf::from(home).join(".cache/gonvex-perf-fixture")
    }

    fn template_key(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(FIXTURE_VERSION.to_le_bytes());
        hasher.update(self.seed.to_le_bytes());
        hasher.update(SCHEMA_SQL.as_bytes());
        hasher.update(MODULE_JSON.as_bytes());
        for tenant in &self.tenants {
            hasher.update(format!("{tenant:?}").as_bytes());
        }
        let digest = hasher.finalize();
        format!(
            "v{FIXTURE_VERSION}-{}",
            digest[..6]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        )
    }

    fn tenant_seed(&self, index: usize) -> u64 {
        self.seed.wrapping_add(index as u64 * 0x1000_0001)
    }
}

/// A tenant of the running fixture: its route and generated content.
pub struct FixtureTenant {
    pub spec: TenantSpec,
    pub route: TenantRoute,
    pub members: Vec<FixtureMember>,
    pub tasks: Vec<FixtureTask>,
}

/// One member's websocket-equivalent state: its session and open replica
/// subscriptions keyed by subscription id (the function path).
pub struct MemberConnection {
    pub tenant: usize,
    pub member: usize,
    pub session: TenantSession,
    pub subscriptions: BTreeMap<String, ReplicaSubscription>,
    /// Rows delivered by the snapshots that opened `subscriptions`.
    pub snapshot_rows: usize,
    /// Rows per subscription id at open time. Benches should read these
    /// instead of `ReplicaSubscription` internals.
    pub rows: BTreeMap<String, usize>,
}

/// How [`Fixture::deliver`] encodes the messages `apply_feed_event` returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encode {
    /// Only `apply_feed_event`.
    None,
    /// Plus `serde_json::to_string` per message, as `send_json` does.
    Json,
    /// Plus per-message deflate (`compress_text`) above the configured
    /// threshold, as the compressed websocket encoder does.
    JsonDeflate,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Delivery {
    pub connections_with_messages: usize,
    pub messages: usize,
    pub json_bytes: usize,
    pub wire_bytes: usize,
    /// Wall time inside `apply_feed_event`, summed over connections.
    pub apply_ns: u64,
    /// Wall time encoding the resulting messages (JSON, deflate).
    pub encode_ns: u64,
    /// Visibility counter deltas over the delivery (`in_memory_row_checks`,
    /// `visibility_sql_statements`, `collection_snapshots`, ...). Process-wide
    /// counters: deliver nothing concurrently while measuring.
    pub visibility: crate::visibility_metrics::VisibilityMetricsSnapshot,
}

/// The running fixture. Dropping it deletes the run directory (set
/// `GONVEX_PERF_FIXTURE_KEEP=1` to keep it); call [`Fixture::shutdown`] for an
/// orderly runtime shutdown first.
pub struct Fixture {
    pub runtime: Runtime,
    pub host: FakeModuleHost,
    pub tenants: Vec<FixtureTenant>,
    pub run_dir: PathBuf,
    pub template_dir: PathBuf,
    compression_threshold: usize,
}

impl Fixture {
    /// Builds (or reuses) the SQLite template, copies it into a fresh run
    /// directory, starts the fake module host and `Runtime::start()`.
    pub async fn build(options: FixtureOptions) -> FixtureResult<Self> {
        let root = options.cache_root();
        let template_dir = root.join(options.template_key());
        if !template_dir.join("ready").exists() {
            build_template(&options, &template_dir).await?;
        }
        let run_dir = root.join("runs").join(format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(run_dir.join("tenants"))?;
        for entry in walk_files(&template_dir)? {
            let relative = entry.strip_prefix(&template_dir)?;
            let name = relative.to_string_lossy();
            if relative == Path::new("ready") || name.ends_with("-shm") {
                continue;
            }
            // `std::fs::copy` uses copy_file_range: a reflink on btrfs/xfs.
            std::fs::copy(&entry, run_dir.join(relative))?;
        }
        let control_url = gonvex_store::database_url(&run_dir.join("control.db"));
        let mut routes = Vec::new();
        {
            let mut control = sqlx::SqliteConnection::connect(&control_url).await?;
            for spec in &options.tenants {
                let url = tenant_url(&run_dir, &spec.id);
                sqlx::query("UPDATE gonvex_runtime_tenants SET database_url=$1 WHERE project_id=$2 AND tenant_id=$3")
                    .bind(&url)
                    .bind(PROJECT_ID)
                    .bind(&spec.id)
                    .execute(&mut control)
                    .await?;
                routes.push(TenantRoute {
                    project_id: PROJECT_ID.to_owned(),
                    tenant_id: spec.id.clone(),
                    database_url: url,
                });
            }
            control.close().await?;
        }

        let host =
            FakeModuleHost::start(&run_dir.join("host.sock"), fake_host::whagons_functions())?;
        let mut overrides: BTreeMap<String, String> = std::env::vars()
            .filter(|(name, _)| name.starts_with("GONVEX_"))
            .collect();
        overrides.extend(options.config.clone());
        overrides.insert("GONVEX_DATA_DIR".into(), run_dir.display().to_string());
        overrides.insert("GONVEX_MODULE_HOST_ENABLED".into(), "true".into());
        overrides.insert("GONVEX_MODULE_HOST_ENDPOINT".into(), host.endpoint());
        // Builds with the in-process V8 host (`GONVEX_MODULE_HOST_IN_PROCESS`,
        // default true there) would otherwise bypass the fake socket host and
        // run the placeholder bundle, which exports nothing
        // (`function_not_found`). Builds without the flag ignore it.
        overrides.insert("GONVEX_MODULE_HOST_IN_PROCESS".into(), "false".into());
        overrides.remove("GONVEX_MODULE_HOST_BINARY");
        let config = Config::perf_from_lookup(|name| overrides.get(name).cloned())?;
        let compression_threshold = config.websocket_compression_threshold;
        let runtime = Runtime::new(config);
        runtime.start().await?;

        let mut tenants = Vec::new();
        for (index, (spec, route)) in options.tenants.iter().zip(routes).enumerate() {
            let generated = data::generate(
                options.tenant_seed(index),
                spec.tasks,
                spec.members,
                spec.description_bytes,
            );
            tenants.push(FixtureTenant {
                spec: spec.clone(),
                route,
                members: generated.members,
                tasks: generated.tasks,
            });
        }
        let fixture = Self {
            runtime,
            host,
            tenants,
            run_dir,
            template_dir,
            compression_threshold,
        };
        // The template and the regenerated descriptors must agree.
        let first = &fixture.tenants[0];
        let found: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM members WHERE id=$1")
            .bind(&first.members[0].id)
            .fetch_one(&fixture.pools().reader(&first.route.database_url).await?)
            .await?;
        if found != 1 {
            return Err("fixture template is stale: bump FIXTURE_VERSION".into());
        }
        Ok(fixture)
    }

    /// The runtime's own pool registry (the one reducers commit through).
    pub fn pools(&self) -> &PoolRegistry {
        &self.runtime.inner.pools
    }

    pub fn tenant(&self, tenant: usize) -> &FixtureTenant {
        &self.tenants[tenant]
    }

    /// Indexes of a tenant's members of `kind`.
    pub fn members_of_kind(&self, tenant: usize, kind: MemberKind) -> Vec<usize> {
        self.tenants[tenant]
            .members
            .iter()
            .enumerate()
            .filter(|(_, member)| member.kind == kind)
            .map(|(index, _)| index)
            .collect()
    }

    /// An admitted tenant session for `member` (as websocket authentication
    /// produces it).
    pub fn session(&self, tenant: usize, member: usize) -> TenantSession {
        let tenant = &self.tenants[tenant];
        let member = &tenant.members[member];
        TenantSession {
            identity: SessionIdentity {
                project_id: PROJECT_ID.to_owned(),
                account: Account {
                    id: member.account_id.clone(),
                    email: member.email.clone(),
                    email_verified: true,
                    name: member.name.clone(),
                    avatar_url: String::new(),
                    provider: "password".into(),
                },
            },
            route: tenant.route.clone(),
            member: Member {
                id: member.id.clone(),
                account_id: member.account_id.clone(),
                status: "active".into(),
                display_name: member.name.clone(),
                avatar_url: String::new(),
                role: member.role.clone(),
                permissions: member.permissions.clone(),
                membership_revision: 1,
            },
            admission_revision: 0,
            delegation: None,
            developer_session: None,
        }
    }

    /// Opens one replica collection exactly as the websocket path does
    /// (`open_replica_negotiated`, cold open: no cursor or digest).
    pub async fn open_replica(
        &self,
        session: &TenantSession,
        id: &str,
        path: &str,
    ) -> FixtureResult<ReplicaOpenResult> {
        let request: ReplicaOpenRequest =
            serde_json::from_value(json!({"id": id, "path": path, "args": {}}))?;
        Ok(self
            .runtime
            .open_replica_negotiated(session, request, false)
            .await?)
    }

    /// [`Fixture::open_replica`] with arguments.
    pub async fn open_replica_args(
        &self,
        session: &TenantSession,
        id: &str,
        path: &str,
        args: Value,
    ) -> FixtureResult<ReplicaOpenResult> {
        let request: ReplicaOpenRequest =
            serde_json::from_value(json!({"id": id, "path": path, "args": args}))?;
        Ok(self
            .runtime
            .open_replica_negotiated(session, request, false)
            .await?)
    }

    /// Opens a member's full [`WHAGONS_SUBSCRIPTIONS`] set, one after another
    /// (the websocket queue runs up to 8 concurrently).
    pub async fn connect(&self, tenant: usize, member: usize) -> FixtureResult<MemberConnection> {
        self.connect_paths(tenant, member, &WHAGONS_SUBSCRIPTIONS).await
    }

    /// [`Fixture::connect`] opening only `paths`.
    pub async fn connect_paths(
        &self,
        tenant: usize,
        member: usize,
        paths: &[&str],
    ) -> FixtureResult<MemberConnection> {
        let session = self.session(tenant, member);
        let mut subscriptions = BTreeMap::new();
        let mut rows = BTreeMap::new();
        for path in paths {
            let opened = self.open_replica(&session, path, path).await?;
            let subscription = opened
                .subscription
                .ok_or_else(|| format!("{path} did not stay subscribed"))?;
            rows.insert(path.to_string(), subscription_rows(&subscription));
            subscriptions.insert(path.to_string(), subscription);
        }
        Ok(MemberConnection {
            tenant,
            member,
            session,
            subscriptions,
            snapshot_rows: rows.values().sum(),
            rows,
        })
    }

    /// [`Fixture::connect`] for every member of `tenant`.
    pub async fn connect_all(&self, tenant: usize) -> FixtureResult<Vec<MemberConnection>> {
        let mut connections = Vec::new();
        for member in 0..self.tenants[tenant].members.len() {
            connections.push(self.connect(tenant, member).await?);
        }
        Ok(connections)
    }

    /// Commits one statement through a `TenantTransaction` on the runtime's
    /// pools (so the change feed assigns its revision, as for a reducer
    /// commit) and returns the committed transaction as a [`FeedEvent`].
    pub async fn commit(
        &self,
        tenant: usize,
        statement: &str,
        binds: Vec<SqlValue>,
    ) -> FixtureResult<FeedEvent> {
        let route = &self.tenants[tenant].route;
        let pools = self.pools();
        let before = change_feed::read_clock(pools, route).await?.revision;
        let mut transaction =
            gonvex_store::begin_transaction(pools, &route.database_url, false).await?;
        bind_all(sqlx::query(statement), &binds)
            .execute(&mut **transaction.transaction())
            .await?;
        transaction.commit().await?;
        self.feed_since(tenant, before).await
    }

    /// [`Self::commit`] of several statements in one transaction (one
    /// revision, as a reducer that writes several tables).
    pub async fn commit_all(
        &self,
        tenant: usize,
        statements: Vec<(String, Vec<SqlValue>)>,
    ) -> FixtureResult<FeedEvent> {
        let route = &self.tenants[tenant].route;
        let pools = self.pools();
        let before = change_feed::read_clock(pools, route).await?.revision;
        let mut transaction =
            gonvex_store::begin_transaction(pools, &route.database_url, false).await?;
        for (statement, binds) in &statements {
            bind_all(sqlx::query(statement), binds)
                .execute(&mut **transaction.transaction())
                .await?;
        }
        transaction.commit().await?;
        self.feed_since(tenant, before).await
    }

    /// Delivery v4 raw-row hash check: every row of every table of `tenant`,
    /// read as `json_object` over all its declared columns (the snapshot
    /// encoding), hashed by the raw path (`canonical_row`) and by the `Value`
    /// path (`stable_json` of the parsed row). Returns (rows, rows on the
    /// fast path, rows whose hashes differ, a first mismatch).
    pub async fn raw_hash_check(
        &self,
        tenant: usize,
    ) -> FixtureResult<(usize, usize, usize, Option<String>)> {
        let route = &self.tenants[tenant].route;
        let mut transaction =
            gonvex_store::begin_transaction(self.pools(), &route.database_url, true).await?;
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )
        .fetch_all(&mut **transaction.transaction())
        .await?;
        let (mut rows, mut fast, mut mismatched, mut first) = (0, 0, 0, None);
        for table in tables {
            let columns = gonvex_store::rows::table_columns(&mut **transaction.transaction(), &table)
                .await?
                .into_iter()
                .map(|column| (column.name.clone(), column.kind()))
                .collect::<Vec<_>>();
            if columns.is_empty() {
                continue;
            }
            let statement = format!(
                "SELECT {} FROM \"{}\" AS r",
                gonvex_store::rows::json_object_expr("r", &columns),
                table.replace('"', "\"\"")
            );
            let texts: Vec<Option<String>> = sqlx::query_scalar(&statement)
                .fetch_all(&mut **transaction.transaction())
                .await?;
            for text in texts.into_iter().flatten() {
                rows += 1;
                let (raw, value, checked) = crate::canonical_row::hashes(&text)?;
                fast += usize::from(checked);
                if raw != value {
                    mismatched += 1;
                    first.get_or_insert_with(|| format!("{table}: {text}"));
                }
            }
        }
        transaction.commit().await?;
        Ok((rows, fast, mismatched, first))
    }

    /// Everything committed to `tenant` after `revision`, as one event.
    pub async fn feed_since(&self, tenant: usize, revision: u64) -> FixtureResult<FeedEvent> {
        let route = &self.tenants[tenant].route;
        let clock = change_feed::read_clock(self.pools(), route).await?;
        let changes =
            change_feed::read_changes(self.pools(), route, revision, clock.revision).await?;
        Ok(FeedEvent::Transaction {
            database_epoch: clock.epoch,
            revision: clock.revision,
            changes: std::sync::Arc::new(changes),
        })
    }

    /// The tenant's current change-feed revision.
    pub async fn revision(&self, tenant: usize) -> FixtureResult<u64> {
        Ok(
            change_feed::read_clock(self.pools(), &self.tenants[tenant].route)
                .await?
                .revision,
        )
    }

    /// Updates task `task` (index into `tenant.tasks`) with `patch` (a JSON
    /// object of column -> value) plus `updatedAt`, and returns the event.
    pub async fn update_task(
        &self,
        tenant: usize,
        task: usize,
        patch: Value,
    ) -> FixtureResult<FeedEvent> {
        let Value::Object(mut patch) = patch else {
            return Err("patch must be an object".into());
        };
        patch.insert("updatedAt".into(), json!(now_ms()));
        let assignments: Vec<String> = patch
            .keys()
            .enumerate()
            .map(|(i, column)| format!("\"{}\"=${}", column.replace('"', "\"\""), i + 1))
            .collect();
        let statement = format!(
            "UPDATE \"tasks\" SET {} WHERE \"_id\"=${}",
            assignments.join(","),
            patch.len() + 1
        );
        let mut binds: Vec<SqlValue> = patch.into_iter().map(|(_, v)| json_bind(v)).collect();
        binds.push(self.tenants[tenant].tasks[task].id.clone().into());
        self.commit(tenant, &statement, binds).await
    }

    /// The row write of a `presence.beat` for `member`, without the reducer.
    pub async fn beat_presence(&self, tenant: usize, member: usize) -> FixtureResult<FeedEvent> {
        let id = self.tenants[tenant].members[member].id.clone();
        self.commit(
            tenant,
            r#"UPDATE "userPresence" SET "lastSeenAt"=$1, "state"='active' WHERE "userId"=$2"#,
            vec![now_ms().into(), id.into()],
        )
        .await
    }

    /// Delivers `event` to every connection as the websocket loop does:
    /// a clone of the event per connection, `apply_feed_event`, then
    /// (per `encode`) JSON and per-message deflate of each message.
    pub async fn deliver(
        &self,
        connections: &mut [MemberConnection],
        event: &FeedEvent,
        encode: Encode,
    ) -> FixtureResult<Delivery> {
        let mut delivery = Delivery::default();
        let before = self.runtime.visibility_metrics().snapshot();
        for connection in connections.iter_mut() {
            let started = std::time::Instant::now();
            let messages = self
                .runtime
                .apply_feed_event(
                    &connection.session,
                    &mut connection.subscriptions,
                    event.clone(),
                )
                .await?;
            delivery.apply_ns += started.elapsed().as_nanos() as u64;
            if !messages.is_empty() {
                delivery.connections_with_messages += 1;
            }
            delivery.messages += messages.len();
            if encode == Encode::None {
                continue;
            }
            let started = std::time::Instant::now();
            for message in &messages {
                let text = serde_json::to_string(message)?;
                delivery.json_bytes += text.len();
                delivery.wire_bytes += if encode == Encode::JsonDeflate {
                    gonvex_websocket::perf_compress_text(
                        text.as_bytes(),
                        15,
                        self.compression_threshold,
                    )?
                    .map_or(text.len(), |compressed| compressed.len())
                } else {
                    text.len()
                };
            }
            delivery.encode_ns += started.elapsed().as_nanos() as u64;
        }
        let after = self.runtime.visibility_metrics().snapshot();
        delivery.visibility = crate::visibility_metrics::VisibilityMetricsSnapshot {
            visibility_sql_statements: after.visibility_sql_statements
                - before.visibility_sql_statements,
            collection_snapshots: after.collection_snapshots - before.collection_snapshots,
            live_query_reruns: after.live_query_reruns - before.live_query_reruns,
            principal_rebuilds: after.principal_rebuilds - before.principal_rebuilds,
            consistency_rebuilds: after.consistency_rebuilds - before.consistency_rebuilds,
            in_memory_row_checks: after.in_memory_row_checks - before.in_memory_row_checks,
            compiled_row_checks: after.compiled_row_checks - before.compiled_row_checks,
            class_evaluations: after.class_evaluations - before.class_evaluations,
            member_evaluations: after.member_evaluations - before.member_evaluations,
            fallback_row_checks: after.fallback_row_checks - before.fallback_row_checks,
        };
        Ok(delivery)
    }

    /// Calls a tenant reducer through the websocket handler's entry
    /// (`call_reducer`): module dispatch, the fake host round trip with its
    /// host calls, the tenant writer and commit. Uses an idempotency key, as
    /// the SDK does.
    pub async fn call_reducer(
        &self,
        session: &TenantSession,
        path: &str,
        args: Value,
    ) -> ServerMessage {
        let id = uuid::Uuid::new_v4().to_string();
        let call = ReducerCallRequest {
            id: id.clone(),
            path: path.to_owned(),
            args,
            scope: None,
            trace: None,
            idempotency_key: Some(id),
            artifact_hash: None,
            client_contract: None,
            receipt_path: None,
            intent_entropy: None,
        };
        crate::call_reducer(
            &self.runtime,
            Some(session),
            &self.control_connection(session, "perf"),
            call,
        )
        .await
    }

    /// Calls a Control Plane reducer (e.g. `control.support.heartbeat`)
    /// through the same entry, as connection `connection_id`.
    pub async fn call_control_reducer(
        &self,
        session: &TenantSession,
        connection_id: &str,
        path: &str,
        args: Value,
    ) -> ServerMessage {
        let id = uuid::Uuid::new_v4().to_string();
        let call = ReducerCallRequest {
            id: id.clone(),
            path: path.to_owned(),
            args,
            scope: Some(ExecutionScope::Control),
            trace: None,
            idempotency_key: Some(id),
            artifact_hash: None,
            client_contract: None,
            receipt_path: None,
            intent_entropy: None,
        };
        crate::call_reducer(
            &self.runtime,
            Some(session),
            &self.control_connection(session, connection_id),
            call,
        )
        .await
    }

    fn control_connection(
        &self,
        session: &TenantSession,
        connection_id: &str,
    ) -> crate::control::ControlConnection {
        crate::control::ControlConnection {
            connection_id: connection_id.to_owned(),
            project_id: session.identity.project_id.clone(),
            identity: Some(session.identity.clone()),
            tenant: Some(session.clone()),
            ..Default::default()
        }
    }

    /// Reads subscription visibility internals, so it may need updating when
    /// the mask representation changes; `Delivery::visibility` counters are
    /// the stable measure.
    ///
    /// The in-memory visibility work of one change for every connection's
    /// `subscription`: does it concern the member's principal, and does the
    /// new row match the member's mask. Returns how many admit the row.
    pub fn evaluate_visibility(
        connections: &[MemberConnection],
        subscription: &str,
        change: &LogChange,
    ) -> usize {
        let mut admitted = 0;
        for connection in connections {
            let Some(subscription) = connection.subscriptions.get(subscription) else {
                continue;
            };
            let concerns = subscription.visibility.principal_change_concerns(
                &subscription.resolved,
                &change.table,
                &change.operation,
                &change.changed_columns,
                &change.old_value,
                &change.new_value,
            );
            std::hint::black_box(concerns);
            if crate::visibility::row_matches(
                &subscription.visibility,
                &subscription.resolved,
                &change.new_value,
            ) {
                admitted += 1;
            }
        }
        admitted
    }

    /// `evaluate_visibility` through the compiled class programs
    /// (`visibility_class`): change rows decoded once, one default evaluation
    /// per visibility class, members evaluated alone only when a member atom
    /// (grant, context) hits the row. Returns admitted count and the stats.
    pub fn evaluate_visibility_compiled(
        connections: &mut [MemberConnection],
        subscription: &str,
        change: &LogChange,
    ) -> (usize, crate::visibility_class::EvalStats) {
        use crate::visibility_class::MemberHandle;
        let mut memo = crate::visibility_class::EvalMemo::default();
        let mut admitted = 0;
        for connection in connections {
            let database = connection.session.route.database_url.clone();
            let Some(subscription) = connection.subscriptions.get_mut(subscription) else {
                continue;
            };
            let concerns = subscription.visibility.principal_change_concerns(
                &subscription.resolved,
                &change.table,
                &change.operation,
                &change.changed_columns,
                &change.old_value,
                &change.new_value,
            );
            std::hint::black_box(concerns);
            let handle =
                subscription
                    .mask
                    .get(&subscription.visibility, &subscription.resolved, &database);
            let visible = match &handle {
                MemberHandle::Compiled(member) => member.visible_memo(&mut memo, &change.new_value),
                MemberHandle::Fallback => {
                    memo.stats.fallback_checks += 1;
                    crate::visibility::row_matches(
                        &subscription.visibility,
                        &subscription.resolved,
                        &change.new_value,
                    )
                }
            };
            admitted += visible as usize;
        }
        (admitted, memo.stats)
    }

    /// Compiled bindings of every connection's `subscription` (warms the
    /// subscription caches), for class statistics and candidate routing.
    pub fn visibility_handles(
        connections: &mut [MemberConnection],
        subscription: &str,
    ) -> Vec<Option<std::sync::Arc<crate::visibility_class::MemberVisibility>>> {
        connections
            .iter_mut()
            .map(|connection| {
                let database = connection.session.route.database_url.clone();
                let subscription = connection.subscriptions.get_mut(subscription)?;
                match subscription.mask.get(
                    &subscription.visibility,
                    &subscription.resolved,
                    &database,
                ) {
                    crate::visibility_class::MemberHandle::Compiled(member) => Some(member),
                    crate::visibility_class::MemberHandle::Fallback => None,
                }
            })
            .collect()
    }

    /// Approximate heap bytes of one connection's `ResolvedVisibility` for
    /// `subscription` (BTreeSet<String> token sets), for memory comparison.
    pub fn resolved_heap_bytes(connection: &MemberConnection, subscription: &str) -> usize {
        let Some(subscription) = connection.subscriptions.get(subscription) else {
            return 0;
        };
        fn sets(resolved: &crate::visibility::ResolvedVisibility) -> usize {
            resolved
                .sets
                .iter()
                .map(|(name, tokens)| {
                    name.len() + 48 + tokens.iter().map(|token| token.len() + 40).sum::<usize>()
                })
                .sum::<usize>()
                + resolved
                    .inherited
                    .values()
                    .map(|parent| sets(&parent.resolved))
                    .sum::<usize>()
        }
        sets(&subscription.resolved)
    }

    /// Orderly shutdown (runtime first, then the fake host and run files).
    pub async fn shutdown(self) {
        self.runtime.shutdown().await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if std::env::var("GONVEX_PERF_FIXTURE_KEEP").as_deref() != Ok("1") {
            let _ = std::fs::remove_dir_all(&self.run_dir);
        }
    }
}

/// Rows held by a subscription (the one place the fixture reads replica
/// internals for counting; adapt here if `ReplicaSubscription` changes).
fn subscription_rows(subscription: &ReplicaSubscription) -> usize {
    subscription.row_count()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn json_bind(value: Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => b.into(),
        Value::Number(n) => n
            .as_i64()
            .map(SqlValue::Int)
            .unwrap_or_else(|| SqlValue::Real(n.as_f64().unwrap_or_default())),
        Value::String(s) => SqlValue::Text(s),
        other => SqlValue::Text(other.to_string()),
    }
}

fn bind_all<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    binds: &'q [SqlValue],
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for value in binds {
        query = match value {
            SqlValue::Null => query.bind(None::<String>),
            SqlValue::Int(v) => query.bind(*v),
            SqlValue::Real(v) => query.bind(*v),
            SqlValue::Text(v) => query.bind(v.as_str()),
        };
    }
    query
}

fn tenant_url(dir: &Path, tenant: &str) -> String {
    gonvex_store::database_url(&dir.join("tenants").join(format!("{tenant}.db")))
}

fn walk_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(walk_files(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}

fn migration_sql() -> String {
    format!("-- gonvex:scope tenant\n{SCHEMA_SQL}")
}

/// The runtime manifest record of the fixture module: the extracted Whagons
/// functions and visibility plans, the schema as its tenant migration, and a
/// placeholder bundle the fake host never evaluates.
pub fn module_manifest() -> FixtureResult<(Value, String)> {
    let source: Value = serde_json::from_str(MODULE_JSON)?;
    let code = "export {};\n";
    let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let mut module = json!({
        "generation": 1,
        "language": "typescript",
        "entrypoint": "perf-fixture.js",
        "files": {format!("migrations/{MIGRATION_NAME}"): encode(migration_sql().as_bytes())},
        "functions": source["functions"],
        "visibility": source["visibility"],
        "crons": [],
        "invitationAcceptanceReducer": "",
        "javascript": {"path": "perf-fixture.js", "hash": format!("{:x}", Sha256::digest(code))},
    });
    let hash = crate::modules::canonical_artifact_hash(module.as_object().expect("object"))?;
    module["hash"] = json!(hash);
    module["javascript"]["code"] = json!(encode(code.as_bytes()));
    Ok((json!({"project": PROJECT_ID, "module": module}), hash))
}

/// Builds the SQLite template: Control Plane with the project, tenants and
/// module manifest; each tenant provisioned with the schema migration and
/// seeded in one transaction (so it has a real change log), then
/// checkpointed. Built in a temporary directory and renamed into place.
async fn build_template(options: &FixtureOptions, dir: &Path) -> FixtureResult<()> {
    let started = std::time::Instant::now();
    let tmp = dir.with_extension(format!("tmp-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(tmp.join("tenants"))?;
    let pools = PoolRegistry::new(PoolLimits::default());
    let control_url = gonvex_store::database_url(&tmp.join("control.db"));
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new()).await?;
    let control_write = |statement: String, binds: Vec<SqlValue>| {
        let pools = pools.clone();
        let control_url = control_url.clone();
        async move {
            let mut transaction =
                gonvex_store::begin_transaction(&pools, &control_url, false).await?;
            bind_all(sqlx::query(&statement), &binds)
                .execute(&mut **transaction.transaction())
                .await?;
            transaction.commit().await?;
            FixtureResult::<()>::Ok(())
        }
    };
    control_write(
        "INSERT INTO gonvex_runtime_projects(id,name,database_mode,status) VALUES($1,'Whagons perf fixture','multiTenant','active')".into(),
        vec![PROJECT_ID.into()],
    )
    .await?;
    for spec in &options.tenants {
        control_write(
            "INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_url,status) VALUES($1,$2,$1,$3,$4,'active')".into(),
            vec![
                spec.id.clone().into(),
                PROJECT_ID.into(),
                format!("Tenant {}", spec.id).into(),
                tenant_url(&tmp, &spec.id).into(),
            ],
        )
        .await?;
    }
    let (manifest, hash) = module_manifest()?;
    control
        .save_runtime_manifest(&RuntimeManifestRecord {
            project_id: PROJECT_ID.into(),
            module_hash: hash,
            manifest,
        })
        .await?;
    let migration = SqlMigration::new(
        MIGRATION_NAME.to_owned(),
        MigrationScope::Tenant,
        false,
        migration_sql(),
    );
    for (index, spec) in options.tenants.iter().enumerate() {
        let route = TenantRoute {
            project_id: PROJECT_ID.into(),
            tenant_id: spec.id.clone(),
            database_url: tenant_url(&tmp, &spec.id),
        };
        control
            .clone()
            .provision_tenant_database(route.clone(), vec![migration.clone()])
            .await?;
        let generated = data::generate(
            options.tenant_seed(index),
            spec.tasks,
            spec.members,
            spec.description_bytes,
        );
        let mut transaction =
            gonvex_store::begin_transaction(&pools, &route.database_url, false).await?;
        for table in &generated.tables {
            let statement = format!(
                "INSERT INTO \"{}\" ({}) VALUES ({})",
                table.table,
                table
                    .columns
                    .iter()
                    .map(|column| format!("\"{column}\""))
                    .collect::<Vec<_>>()
                    .join(","),
                (1..=table.columns.len())
                    .map(|i| format!("${i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            for row in &table.rows {
                bind_all(sqlx::query(&statement), row)
                    .execute(&mut **transaction.transaction())
                    .await
                    .map_err(|error| format!("seed {}: {error}", table.table))?;
            }
        }
        transaction.commit().await?;
    }
    drop(control);
    pools.close().await;
    for file in walk_files(&tmp)? {
        if file.extension().is_some_and(|ext| ext == "db") {
            let mut connection =
                sqlx::SqliteConnection::connect(&gonvex_store::database_url(&file)).await?;
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(&mut connection)
                .await?;
            connection.close().await?;
        }
    }
    std::fs::write(tmp.join("ready"), format!("{:?}\n", options.tenants))?;
    if std::fs::rename(&tmp, dir).is_err() {
        // Another process finished first; use its template.
        let _ = std::fs::remove_dir_all(&tmp);
    }
    eprintln!(
        "perf fixture: built template {} in {:.1}s",
        dir.display(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
