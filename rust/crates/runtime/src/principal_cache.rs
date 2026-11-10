//! One SQL resolution of a member's principal per (plan, member) shared by
//! every collection and open that needs it.
//!
//! A Whagons task detail window opens 8 collections; 7 inherit `tasks`, so
//! each open used to resolve the member row, the tasks plan's ~30 principal
//! and grant sets and its own grant sets again: ~40 SQL statements and a
//! plan-sized fingerprint per open, for the same answer.
//!
//! An entry is the SQL part of a version 2 resolution ([`Core`]: member row,
//! principal/grant/derived sets) for one account, keyed by the plan's set
//! definitions (so the 7 task-child plans, whose grant sets are identical,
//! share one), valid at `revision`. A plan reuses it only when its active
//! sets for the member's role are the entry's; the fingerprint (which hashes
//! the whole plan) is kept per plan. An open at another revision of the same database
//! reuses it only when the change log proves that no table the resolution
//! read changed in between: one indexed probe of `_gonvex_sync_changes` in
//! the open's own read transaction (the log is written in the committing
//! transaction, and every row after `retained_revision` is kept). Tables
//! without change capture are never cached; nor are logical record storage
//! and owned tables, whose changes are logged under their record's name (the
//! record view itself is cacheable). Parent ACL indexes and inherited
//! parents are attached per open as before; only SQL results are shared.
//!
//! Env: `GONVEX_PRINCIPAL_CACHE=0` disables; `GONVEX_PRINCIPAL_CACHE_ENTRIES`
//! (default 8192) bounds entries; `GONVEX_PRINCIPAL_CACHE_TTL_SECS` (120)
//! drops entries unused for that long.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use gonvex_store::{TenantSession, TenantTransaction};
use serde_json::Value;
use sqlx::Row;

use crate::visibility::{PlanCaches, ResolvedVisibility, VisibilityError, VisibilityPlan};

/// The read transaction's clock.
#[derive(Clone, Copy, Debug)]
pub struct Clock<'a> {
    pub epoch: &'a str,
    pub revision: u64,
    pub retained: u64,
}

/// The SQL-resolved part of a version 2 resolution.
#[derive(Debug)]
pub(crate) struct Core {
    direct: BTreeMap<String, String>,
    role: String,
    permissions: Value,
    sets: BTreeMap<String, BTreeSet<String>>,
}

/// A shared resolution and the requesting plan's fingerprint of it.
pub(crate) struct Shared {
    core: Arc<Core>,
    fingerprint: Arc<str>,
}

impl Shared {
    /// The resolution as a fresh one at `revision` would return it (no
    /// parents or indexes attached yet).
    pub(crate) fn resolved(&self, revision: u64) -> ResolvedVisibility {
        let core = &self.core;
        ResolvedVisibility {
            ephemeral_member: false,
            revision,
            direct: core.direct.clone(),
            role: core.role.clone(),
            permissions: core.permissions.clone(),
            sets: core.sets.clone(),
            fingerprint: self.fingerprint.to_string(),
            inherited: BTreeMap::new(),
            indexes: BTreeMap::new(),
        }
    }
}

impl Core {
    fn of(resolved: &ResolvedVisibility) -> Self {
        Self {
            direct: resolved.direct.clone(),
            role: resolved.role.clone(),
            permissions: resolved.permissions.clone(),
            sets: resolved.sets.clone(),
        }
    }

    fn heap_bytes(&self) -> usize {
        let string = |value: &String| value.len() + 48;
        self.direct
            .iter()
            .map(|(key, value)| string(key) + string(value))
            .sum::<usize>()
            + self
                .sets
                .iter()
                .map(|(name, tokens)| string(name) + tokens.iter().map(string).sum::<usize>())
                .sum::<usize>()
            + self.permissions.to_string().len()
            + self.role.len()
            + 256
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct Key {
    database: Box<str>,
    tenant: Box<str>,
    account: Box<str>,
    /// [`sets_signature`] of the plan.
    sets: [u8; 32],
}

struct Entry {
    epoch: Box<str>,
    /// The entry is known valid at every revision in `low..=revision`.
    low: u64,
    revision: u64,
    core: Arc<Core>,
    /// The sets resolved (the plan's active sets for `core.role`).
    active: Arc<BTreeSet<String>>,
    /// Fingerprint per plan that used the entry (identity, liveness, value).
    fingerprints: Vec<(usize, Weak<PlanCaches>, Arc<str>)>,
    /// Tables the resolution read (base and join tables of its sets and
    /// `members`).
    tables: Arc<[String]>,
    bytes: usize,
    used: Instant,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrincipalCacheStats {
    pub entries: usize,
    pub bytes: usize,
    /// Reused at the entry's own revision range (no SQL).
    pub hits: u64,
    /// Reused after a change-log probe found no change.
    pub probed_hits: u64,
    /// Resolved with SQL: no entry.
    pub misses: u64,
    /// Resolved with SQL: an input table changed since the entry.
    pub invalidated: u64,
    /// Resolved with SQL and not stored: an input table has no change capture.
    pub uncacheable: u64,
    pub evictions: u64,
}

struct Config {
    enabled: bool,
    max_entries: usize,
    ttl: Duration,
}

impl Config {
    fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok();
        Self {
            enabled: var("GONVEX_PRINCIPAL_CACHE")
                .is_none_or(|value| !matches!(value.trim(), "0" | "false" | "off")),
            max_entries: var("GONVEX_PRINCIPAL_CACHE_ENTRIES")
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(8192),
            ttl: Duration::from_secs(
                var("GONVEX_PRINCIPAL_CACHE_TTL_SECS")
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or(120),
            ),
        }
    }
}

/// A database's schema version and its tables with change capture.
type Captured = (i64, Arc<BTreeSet<String>>);

type Flights = HashMap<Key, Weak<tokio::sync::Mutex<()>>>;

pub struct PrincipalCache {
    config: Config,
    enabled: AtomicBool,
    entries: Mutex<HashMap<Key, Entry>>,
    /// One SQL resolution at a time per key: concurrent opens of one member
    /// wait for the first instead of all resolving.
    flights: Mutex<Flights>,
    /// Per database: the schema version the capture check ran at and the
    /// tables with change capture.
    captured: Mutex<HashMap<Box<str>, Captured>>,
    bytes: AtomicUsize,
    hits: AtomicU64,
    probed_hits: AtomicU64,
    misses: AtomicU64,
    invalidated: AtomicU64,
    uncacheable: AtomicU64,
    evictions: AtomicU64,
    inserts: AtomicU64,
}

impl Default for PrincipalCache {
    fn default() -> Self {
        Self::with_config(Config::from_env())
    }
}

/// What [`PrincipalCache::lookup`] found.
pub(crate) enum Lookup {
    Hit(Shared),
    Miss,
}

impl PrincipalCache {
    fn with_config(config: Config) -> Self {
        Self {
            enabled: AtomicBool::new(config.enabled),
            config,
            entries: Mutex::default(),
            flights: Mutex::default(),
            captured: Mutex::default(),
            bytes: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            probed_hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            invalidated: AtomicU64::new(0),
            uncacheable: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
        }
    }

    /// Switches sharing on or off (benchmarks and oracles compare both).
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Relaxed);
    }

    pub fn stats(&self) -> PrincipalCacheStats {
        PrincipalCacheStats {
            entries: self.entries.lock().unwrap().len(),
            bytes: self.bytes.load(Relaxed),
            hits: self.hits.load(Relaxed),
            probed_hits: self.probed_hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            invalidated: self.invalidated.load(Relaxed),
            uncacheable: self.uncacheable.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
        }
    }

    /// The key of `plan` for `session`, or None when this resolution is
    /// never shared (cache off, developer sessions whose principal may be
    /// ephemeral).
    pub(crate) fn key(&self, session: &TenantSession, plan: &VisibilityPlan) -> Option<Key> {
        if !self.enabled.load(Relaxed) || session.developer_session.is_some() {
            return None;
        }
        Some(Key {
            database: session.route.database_url.as_str().into(),
            tenant: session.route.tenant_id.as_str().into(),
            account: session.identity.account.id.as_str().into(),
            sets: sets_signature(plan),
        })
    }

    /// Serializes SQL resolutions of `key`; hold the guard across
    /// [`Self::lookup`] and [`Self::store`].
    pub(crate) async fn flight(&self, key: &Key) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut flights = self.flights.lock().unwrap();
            match flights.get(key).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    if flights.len() >= 1024 {
                        flights.retain(|_, lock| lock.strong_count() > 0);
                    }
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    flights.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }

    /// A shared resolution of `key` valid at `clock`, proving validity with
    /// the change log in `transaction` when the entry is at another revision.
    pub(crate) async fn lookup(
        &self,
        key: &Key,
        plan: &VisibilityPlan,
        transaction: &mut TenantTransaction,
        clock: Clock<'_>,
    ) -> Result<Lookup, VisibilityError> {
        let (core, fingerprint, tables, low, high) = {
            let mut entries = self.entries.lock().unwrap();
            let Some(entry) = entries.get_mut(key) else {
                self.misses.fetch_add(1, Relaxed);
                return Ok(Lookup::Miss);
            };
            // Same set definitions; the plan must also select the same sets
            // for this role (active sets depend on its predicate too).
            let active = crate::visibility::active_set_names(plan, &entry.core.role);
            if *entry.epoch != *clock.epoch
                || !(Arc::ptr_eq(&active, &entry.active) || active == entry.active)
            {
                self.remove(&mut entries, key);
                self.misses.fetch_add(1, Relaxed);
                return Ok(Lookup::Miss);
            }
            entry.used = Instant::now();
            let fingerprint = entry
                .fingerprints
                .iter()
                .find(|(identity, caches, _)| {
                    *identity == plan_identity(plan) && caches.strong_count() > 0
                })
                .map(|(_, _, fingerprint)| fingerprint.clone());
            if (entry.low..=entry.revision).contains(&clock.revision) {
                self.hits.fetch_add(1, Relaxed);
                let core = entry.core.clone();
                drop(entries);
                return Ok(Lookup::Hit(self.shared(key, plan, core, fingerprint)));
            }
            // Changes after `retained` are all in the log.
            let low = entry.revision.min(clock.revision);
            let high = entry.low.max(clock.revision);
            if low < clock.retained {
                self.remove(&mut entries, key);
                self.misses.fetch_add(1, Relaxed);
                return Ok(Lookup::Miss);
            }
            (
                entry.core.clone(),
                fingerprint,
                entry.tables.clone(),
                low,
                high,
            )
        };
        if self.changed(transaction, &tables, low, high).await? {
            self.invalidated.fetch_add(1, Relaxed);
            return Ok(Lookup::Miss);
        }
        // No input changed in (low, high]: the entry holds over the union.
        if let Some(entry) = self.entries.lock().unwrap().get_mut(key) {
            if Arc::ptr_eq(&entry.core, &core) {
                entry.low = entry.low.min(clock.revision);
                entry.revision = entry.revision.max(clock.revision);
            }
        }
        self.probed_hits.fetch_add(1, Relaxed);
        Ok(Lookup::Hit(self.shared(key, plan, core, fingerprint)))
    }

    /// `core` with `plan`'s fingerprint, computing and recording it on the
    /// plan's first use of the entry.
    fn shared(
        &self,
        key: &Key,
        plan: &VisibilityPlan,
        core: Arc<Core>,
        fingerprint: Option<Arc<str>>,
    ) -> Shared {
        if let Some(fingerprint) = fingerprint {
            return Shared { core, fingerprint };
        }
        let fingerprint: Arc<str> = crate::visibility::visibility_fingerprint(
            plan,
            &core.direct,
            &core.role,
            &core.permissions,
            &core.sets,
        )
        .into();
        if let Some(entry) = self.entries.lock().unwrap().get_mut(key) {
            if Arc::ptr_eq(&entry.core, &core) {
                entry
                    .fingerprints
                    .retain(|(_, caches, _)| caches.strong_count() > 0);
                entry.fingerprints.push((
                    plan_identity(plan),
                    Arc::downgrade(&plan.active_sets_cache.0),
                    fingerprint.clone(),
                ));
            }
        }
        Shared { core, fingerprint }
    }

    /// Records a fresh SQL resolution of `plan` at `clock`.
    pub(crate) async fn store(
        &self,
        key: Key,
        plan: &VisibilityPlan,
        transaction: &mut TenantTransaction,
        clock: Clock<'_>,
        resolved: &ResolvedVisibility,
    ) -> Result<(), VisibilityError> {
        if resolved.ephemeral_member {
            return Ok(());
        }
        let tables = input_tables(plan, &resolved.role);
        let captured = self.captured(transaction, &key.database).await?;
        if !tables.iter().all(|table| captured.contains(table)) {
            self.uncacheable.fetch_add(1, Relaxed);
            return Ok(());
        }
        let core = Core::of(resolved);
        let bytes = core.heap_bytes()
            + resolved.fingerprint.len()
            + tables.iter().map(|t| t.len() + 24).sum::<usize>();
        let entry = Entry {
            epoch: clock.epoch.into(),
            low: clock.revision,
            revision: clock.revision,
            active: crate::visibility::active_set_names(plan, &resolved.role),
            fingerprints: vec![(
                plan_identity(plan),
                Arc::downgrade(&plan.active_sets_cache.0),
                resolved.fingerprint.as_str().into(),
            )],
            core: Arc::new(core),
            tables: tables.into(),
            bytes,
            used: Instant::now(),
        };
        let mut entries = self.entries.lock().unwrap();
        self.remove(&mut entries, &key);
        self.bytes.fetch_add(bytes, Relaxed);
        entries.insert(key, entry);
        if self.inserts.fetch_add(1, Relaxed).is_multiple_of(256)
            || entries.len() > self.config.max_entries
        {
            self.sweep(&mut entries);
        }
        Ok(())
    }

    fn remove(&self, entries: &mut HashMap<Key, Entry>, key: &Key) {
        if let Some(old) = entries.remove(key) {
            self.bytes.fetch_sub(old.bytes, Relaxed);
        }
    }

    /// Drops entries unused for the TTL; then, over the bound, the least
    /// recently used eighth.
    fn sweep(&self, entries: &mut HashMap<Key, Entry>) {
        let now = Instant::now();
        let before = entries.len();
        let mut freed = 0;
        entries.retain(|_, entry| {
            let keep = now.duration_since(entry.used) < self.config.ttl;
            if !keep {
                freed += entry.bytes;
            }
            keep
        });
        if entries.len() > self.config.max_entries {
            let mut used = entries.values().map(|entry| entry.used).collect::<Vec<_>>();
            used.sort_unstable();
            let cutoff = used[(entries.len() / 8).max(entries.len() - self.config.max_entries)];
            entries.retain(|_, entry| {
                let keep = entry.used > cutoff;
                if !keep {
                    freed += entry.bytes;
                }
                keep
            });
        }
        self.bytes.fetch_sub(freed, Relaxed);
        self.evictions
            .fetch_add((before - entries.len()) as u64, Relaxed);
    }

    /// Whether any of `tables` has a change in `(low, high]`.
    async fn changed(
        &self,
        transaction: &mut TenantTransaction,
        tables: &[String],
        low: u64,
        high: u64,
    ) -> Result<bool, VisibilityError> {
        if tables.is_empty() || low >= high {
            return Ok(false);
        }
        let statement = probe_statement(tables.len());
        let mut query = sqlx::query(statement.as_str())
            .bind(low as i64)
            .bind(high as i64);
        for table in tables {
            query = query.bind(table.as_str());
        }
        let row = query.fetch_one(&mut **transaction.transaction()).await?;
        Ok(row.get::<i64, _>(0) != 0)
    }

    /// Relations of `database` whose changes are logged under their own name
    /// ([`captured_tables`]: captured tables and logical record views), at
    /// the transaction's schema version.
    async fn captured(
        &self,
        transaction: &mut TenantTransaction,
        database: &str,
    ) -> Result<Arc<BTreeSet<String>>, VisibilityError> {
        let version: i64 = sqlx::query_scalar("PRAGMA schema_version")
            .fetch_one(&mut **transaction.transaction())
            .await?;
        if let Some((at, tables)) = self.captured.lock().unwrap().get(database) {
            if *at == version {
                return Ok(tables.clone());
            }
        }
        let triggers: Vec<(String, String)> = sqlx::query_as(
            r#"SELECT name, coalesce(sql, '') FROM sqlite_master WHERE type = 'trigger' AND name LIKE '\_gonvex\_sync\_%' ESCAPE '\'"#,
        )
        .fetch_all(&mut **transaction.transaction())
        .await?;
        let has_records: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_gonvex_record_views')",
        )
        .fetch_one(&mut **transaction.transaction())
        .await?;
        let records: Vec<(String, String)> = if has_records {
            sqlx::query_as("SELECT name, storage_table FROM _gonvex_record_views")
                .fetch_all(&mut **transaction.transaction())
                .await?
        } else {
            Vec::new()
        };
        let tables = Arc::new(captured_tables(&triggers, &records));
        self.captured
            .lock()
            .unwrap()
            .insert(database.into(), (version, tables.clone()));
        Ok(tables)
    }
}

/// The relations whose changes `_gonvex_sync_changes` logs under their own
/// name, from the `_gonvex_sync_*` triggers (`(name, sql)`) and the logical
/// records (`(name, storage_table)`).
///
/// An ordinary table counts when all three of its triggers are
/// `gonvex_capture` triggers (`gonvex_store::feed::capture_triggers_sql`, the
/// same test as `live_query_projection`). Logical record storage and owned
/// tables carry triggers with the same names that stage the owning record in
/// `_gonvex_record_pending` instead; their changes are logged under the
/// record's name, never their own, so they never count. The record view
/// counts instead, once its storage has all three record-staging triggers.
fn captured_tables(
    triggers: &[(String, String)],
    records: &[(String, String)],
) -> BTreeSet<String> {
    let capture_call = format!("{}(", gonvex_store::CAPTURE_FUNCTION);
    let sql = triggers
        .iter()
        .map(|(name, sql)| (name.as_str(), sql.as_str()))
        .collect::<BTreeMap<_, _>>();
    let triplet = |table: &str, body: &dyn Fn(&str) -> bool| {
        gonvex_store::feed::capture_trigger_names(table)
            .iter()
            .all(|name| sql.get(name.as_str()).is_some_and(|sql| body(sql)))
    };
    let mut tables = sql
        .keys()
        .filter_map(|name| {
            name.strip_prefix("_gonvex_sync_")
                .and_then(|rest| rest.strip_suffix("_insert"))
        })
        .filter(|table| triplet(table, &|sql| sql.contains(&capture_call)))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    tables.extend(
        records
            .iter()
            .filter(|(_, storage)| {
                triplet(storage, &|sql| {
                    sql.contains("_gonvex_record_pending") && !sql.contains(&capture_call)
                })
            })
            .map(|(name, _)| name.clone()),
    );
    tables
}

fn plan_identity(plan: &VisibilityPlan) -> usize {
    Arc::as_ptr(&plan.active_sets_cache.0) as usize
}

/// SHA-256 of the plan's version and set definitions (memoized per plan).
fn sets_signature(plan: &VisibilityPlan) -> [u8; 32] {
    *plan.active_sets_cache.0.sets_signature.get_or_init(|| {
        use sha2::Digest;
        let payload = serde_json::to_vec(&(plan.version, &plan.sets)).unwrap_or_default();
        sha2::Sha256::digest(payload).into()
    })
}

/// `EXISTS` per table, each an index seek on `(table_name, revision)`.
fn probe_statement(tables: usize) -> String {
    let terms = (0..tables)
        .map(|index| {
            format!(
                "EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE table_name = ${} AND revision > $1 AND revision <= $2)",
                index + 3
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("SELECT ({terms})")
}

/// Every table a version 2 resolution of `plan` for `role` reads: the
/// member row and each active set's base and join tables.
pub(crate) fn input_tables(plan: &VisibilityPlan, role: &str) -> Vec<String> {
    let mut tables = BTreeSet::from(["members".to_owned()]);
    for name in crate::visibility::active_set_names(plan, role).iter() {
        let set = &plan.sets[name];
        tables.insert(set.table.clone());
        if set.kind != "derived" {
            tables.extend(set.joins.iter().map(|join| join.table.clone()));
        }
    }
    tables.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use gonvex_store::{
        ControlPlane, MigrationScope, PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
    };

    /// Contract 3 logical records: the storage and owned tables carry
    /// `_gonvex_sync_<table>_*` triggers, but every change is logged under
    /// the record's name. A resolution that read an owned table directly
    /// must never be cached (its probe would never fire); one that read the
    /// record view must be, and a write to an owned table invalidates it.
    #[tokio::test]
    async fn record_storage_and_owned_tables_are_captured_only_through_their_record() {
        let directory = tempfile::tempdir().unwrap();
        // One file holds the Control Plane and the tenant tables.
        let url = gonvex_store::database_url(&directory.path().join("records.db"));
        let pools = PoolRegistry::new(PoolLimits::default());
        let control = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let route = TenantRoute {
            project_id: "records".into(),
            tenant_id: "tenant".into(),
            database_url: url.clone(),
        };
        let schema = r#"
          INSERT INTO _gonvex_id_domains VALUES(1,'tasks');
          INSERT INTO _gonvex_identity_keys VALUES(10,1,'a'),(11,1,'b');
          CREATE TABLE teams(id TEXT PRIMARY KEY,name TEXT);
          CREATE TABLE schedules(_id INTEGER PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE taskStorage(_id INTEGER PRIMARY KEY REFERENCES _gonvex_identity_keys(_id),scheduleId INTEGER REFERENCES schedules(_id));
          CREATE TABLE details(_id INTEGER PRIMARY KEY,taskId INTEGER NOT NULL UNIQUE REFERENCES taskStorage(_id) ON DELETE CASCADE,value TEXT);
          CREATE VIEW tasks AS SELECT i.value _id,s.value schedule,d.value detail FROM taskStorage t JOIN _gonvex_identity_keys i ON i._id=t._id LEFT JOIN schedules s ON s._id=t.scheduleId LEFT JOIN details d ON d.taskId=t._id;
          INSERT INTO _gonvex_record_views VALUES('tasks','taskStorage','_id','{"key":"_id","columns":{"_id":{"type":"text"},"schedule":{"type":"text"},"detail":{"type":"text"}}}');
          INSERT INTO _gonvex_table_options(name,capture,owner_table,owner_column,owner_direction) VALUES('schedules','owned','taskStorage','scheduleId','inbound'),('details','owned','taskStorage','taskId','outbound');
          INSERT INTO schedules VALUES(1,'daily'); INSERT INTO taskStorage VALUES(10,1),(11,1); INSERT INTO details VALUES(1,10,'first');
        "#;
        control
            .provision_tenant_database(
                route,
                vec![SqlMigration::new(
                    "0001_records".into(),
                    MigrationScope::Tenant,
                    false,
                    schema.into(),
                )],
            )
            .await
            .unwrap();
        let cache = PrincipalCache::with_config(Config {
            enabled: true,
            max_entries: 16,
            ttl: Duration::from_secs(60),
        });
        let mut read = gonvex_store::begin_transaction(&pools, &url, true)
            .await
            .unwrap();
        let captured = cache.captured(&mut read, &url).await.unwrap();
        drop(read);
        for table in ["members", "teams", "tasks"] {
            assert!(
                captured.contains(table),
                "{table} is cacheable: {captured:?}"
            );
        }
        for table in ["taskStorage", "schedules", "details"] {
            assert!(
                !captured.contains(table),
                "{table} logs under its record's name: {captured:?}"
            );
        }

        let mut write = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        sqlx::query("UPDATE details SET value='second' WHERE taskId=10")
            .execute(&mut **write.transaction())
            .await
            .unwrap();
        let revision = write.commit_with_revision().await.unwrap().unwrap();
        let mut read = gonvex_store::begin_transaction(&pools, &url, true)
            .await
            .unwrap();
        assert!(
            cache
                .changed(&mut read, &["tasks".to_owned()], revision - 1, revision)
                .await
                .unwrap(),
            "an owned-table write invalidates a resolution that read the record"
        );
        assert!(
            !cache
                .changed(&mut read, &["details".to_owned()], 0, revision)
                .await
                .unwrap(),
            "nothing is logged under the owned table's own name"
        );
        drop(read);
        pools.close().await;
    }

    #[test]
    fn record_views_need_record_triggers_on_their_storage() {
        let plain = |table: &str| {
            gonvex_store::feed::capture_trigger_names(table).map(|name| {
                let sql = format!(
                    "CREATE TRIGGER {name} AFTER INSERT ON {table} BEGIN INSERT INTO _gonvex_sync_changes VALUES ({}('{table}', NEW.rowid, 0)); END",
                    gonvex_store::CAPTURE_FUNCTION
                );
                (name, sql)
            })
        };
        let staged = |table: &str, record: &str| {
            gonvex_store::feed::capture_trigger_names(table).map(|name| {
                let sql = format!(
                    "CREATE TRIGGER {name} BEFORE INSERT ON {table} BEGIN INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT '{record}',NEW._id,gonvex_record_value('{record}',NEW._id); END"
                );
                (name, sql)
            })
        };
        let records = vec![("tasks".to_owned(), "taskStorage".to_owned())];
        // Registered, but its storage still has ordinary capture (before the
        // change-feed install rewrites it): the storage logs under its own
        // name, the record view not at all.
        let triggers = plain("taskStorage").into_iter().collect::<Vec<_>>();
        assert_eq!(
            captured_tables(&triggers, &records),
            BTreeSet::from(["taskStorage".to_owned()])
        );
        // Installed: only the record view.
        let triggers = staged("taskStorage", "tasks")
            .into_iter()
            .chain(staged("details", "tasks"))
            .collect::<Vec<_>>();
        assert_eq!(
            captured_tables(&triggers, &records),
            BTreeSet::from(["tasks".to_owned()])
        );
        // A missing trigger leaves either kind uncaptured.
        let mut triggers = staged("taskStorage", "tasks")
            .into_iter()
            .chain(plain("teams"))
            .collect::<Vec<_>>();
        triggers.retain(|(name, _)| !name.ends_with("_delete"));
        assert!(captured_tables(&triggers, &records).is_empty());
    }
}
