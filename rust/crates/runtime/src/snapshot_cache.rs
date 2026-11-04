//! Shared collection snapshots: the "base" of base + change-log delta.
//!
//! One entry per (tenant database, project, module generation, collection
//! path, arguments) holds every row the collection's own filters admit at one
//! revision `R`, in snapshot order, with each row's projection, canonical hash
//! and byte size. It is principal-free: no visibility predicate went into it.
//! An open (cold snapshot or resume) then
//!
//! 1. reads the source table's change log from `R` to its read clock `C`
//!    (the log is authoritative: rows at `C` = rows at `R` with every logged
//!    key replaced by its last after-image),
//! 2. filters the merged rows with the member's visibility at `C` in memory
//!    (the compiled program for version 2 plans, `visibility::row_matches`
//!    otherwise; parent and derived-set indexes must be exactly at `C`),
//! 3. applies the collection's row and byte budgets in order.
//!
//! The result is the row set a fresh SQL snapshot at `C` returns, row for row
//! and hash for hash (`tests/snapshot_cache_oracle.rs`). Anything the cache
//! cannot answer exactly returns `None` and the caller runs the fresh SQL
//! scan: another epoch, `R` older than the log's retention, `R` ahead of the
//! open's clock, a version 1 bounded plan whose sets stay in SQL, an index not
//! at `C`, a budget that truncates an unordered collection.
//!
//! Staleness policy: an open whose delta is longer than
//! `GONVEX_SNAPSHOT_CACHE_ROLL_CHANGES` log rows or
//! `GONVEX_SNAPSHOT_CACHE_ROLL_REVISIONS` revisions replaces the entry with
//! its merged rows at `C` (a roll-forward from the log: no SQL). An entry
//! older than `GONVEX_SNAPSHOT_CACHE_MAX_AGE_SECS` since its last SQL build is
//! rebuilt from SQL. Builds are single-flight per key (concurrent opens of a
//! missing key wait for the one build) and run inside the open, which runs on
//! the offload pool. Entries are evicted least recently used beyond
//! `GONVEX_SNAPSHOT_CACHE_BYTES`. `GONVEX_SNAPSHOT_CACHE=0` disables it.
//!
//! Principal or visibility changes need no invalidation: entries hold no
//! visibility decision; every open evaluates its own principal at `C`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use gonvex_store::TenantTransaction;
use serde_json::Value;

use crate::change_feed::LogChange;
use crate::modules::ReplicaCollectionDefinition;
use crate::replica::{self, ReplicaError, ReplicaWindow};
use crate::visibility::{self, ResolvedVisibility, VisibilityPlan};

/// Knobs (environment, read once).
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub enabled: bool,
    /// Estimated heap of all entries.
    pub budget_bytes: usize,
    /// Roll an entry forward when an open replays more log rows than this.
    pub roll_changes: usize,
    /// ... or when it is more revisions behind than this.
    pub roll_revisions: u64,
    /// Rebuild from SQL when the last SQL build is older than this.
    pub max_age: Duration,
}

impl CacheConfig {
    pub fn from_env() -> Self {
        let number = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
        };
        Self {
            enabled: std::env::var("GONVEX_SNAPSHOT_CACHE")
                .map(|value| !matches!(value.trim(), "0" | "off" | "false"))
                .unwrap_or(true),
            budget_bytes: number("GONVEX_SNAPSHOT_CACHE_BYTES").unwrap_or(256 << 20) as usize,
            roll_changes: number("GONVEX_SNAPSHOT_CACHE_ROLL_CHANGES").unwrap_or(32) as usize,
            roll_revisions: number("GONVEX_SNAPSHOT_CACHE_ROLL_REVISIONS").unwrap_or(64),
            max_age: Duration::from_secs(
                number("GONVEX_SNAPSHOT_CACHE_MAX_AGE_SECS").unwrap_or(900),
            ),
        }
    }
}

/// One cached row. Clones are reference-count increments.
#[derive(Clone, Debug)]
pub struct CachedRow {
    pub key: Arc<str>,
    /// The collection's projection: what a snapshot sends.
    pub row: Arc<Value>,
    /// Projection plus the visibility/filter/order columns it lacks, when it
    /// lacks any.
    pub eval: Option<Arc<Value>>,
    /// Hex SHA-256 of the projection's canonical JSON.
    pub hash: Arc<str>,
    /// Canonical JSON bytes of the projection (budget accounting).
    pub bytes: u32,
}

impl CachedRow {
    pub(crate) fn eval(&self) -> &Value {
        self.eval.as_deref().unwrap_or(&self.row)
    }

    /// A row from a full row image (SQL or change log).
    pub(crate) fn from_image(
        image: &Value,
        definition: &ReplicaCollectionDefinition,
        extra: &[String],
    ) -> Result<Self, ReplicaError> {
        let projection = replica::project_row(image, definition)?;
        Self::from_projection(projection, image, definition, extra)
    }

    fn from_projection(
        projection: Value,
        image: &Value,
        definition: &ReplicaCollectionDefinition,
        extra: &[String],
    ) -> Result<Self, ReplicaError> {
        let key = replica::row_key(&projection, &definition.key)?;
        let canonical = replica::stable_json(&projection);
        let eval = if extra.is_empty() {
            None
        } else {
            let mut eval = projection.as_object().cloned().unwrap_or_default();
            if let Some(image) = image.as_object() {
                for column in extra {
                    if let Some(value) = image.get(column) {
                        eval.insert(column.clone(), value.clone());
                    }
                }
            }
            Some(Arc::new(Value::Object(eval)))
        };
        Ok(Self {
            key: key.into(),
            hash: replica::hex_digest(canonical.as_bytes()).into(),
            bytes: canonical.len() as u32,
            row: Arc::new(projection),
            eval,
        })
    }
}

/// Rows of one collection at one revision.
pub struct CacheEntry {
    pub epoch: String,
    pub revision: u64,
    /// Last SQL build (roll-forwards keep it).
    built: Instant,
    pub rows: Vec<CachedRow>,
    /// Estimated heap bytes.
    pub heap_bytes: usize,
    /// The rows' ACL columns decoded for the compiled visibility program of
    /// one class scope (built by the first compiled open, row-aligned).
    acl: std::sync::RwLock<Option<DecodedAcl>>,
}

type Acl = Arc<crate::visibility_class::AclRow<'static>>;

struct DecodedAcl {
    scope: Arc<crate::visibility_class::ClassScope>,
    rows: Vec<Acl>,
}

impl CacheEntry {
    /// Runs `evaluate` with this entry's rows decoded for `scope`, decoding
    /// them (once per entry and scope) or refreshing their tokens first.
    pub(crate) fn with_acl<T>(
        &self,
        scope: &Arc<crate::visibility_class::ClassScope>,
        evaluate: impl FnOnce(&[Acl]) -> T,
    ) -> T {
        {
            let mut acl = self.acl.write().unwrap_or_else(|error| error.into_inner());
            match acl.as_mut() {
                Some(decoded) if Arc::ptr_eq(&decoded.scope, scope) => {
                    for row in &mut decoded.rows {
                        if row.stale(scope) {
                            Arc::make_mut(row).refresh(scope);
                        }
                    }
                }
                _ => {
                    *acl = Some(DecodedAcl {
                        scope: scope.clone(),
                        rows: self
                            .rows
                            .iter()
                            .map(|row| Arc::new(scope.decode_owned(row.eval())))
                            .collect(),
                    });
                }
            }
        }
        let acl = self.acl.read().unwrap_or_else(|error| error.into_inner());
        evaluate(&acl.as_ref().expect("decoded above").rows)
    }

    /// Gives a rolled entry the decoded rows of the entry it rolled from
    /// (shared) and decodes only the rows the log replaced.
    pub(crate) fn inherit_acl(&self, from: &CacheEntry, sources: &[Option<usize>]) {
        let from = from.acl.read().unwrap_or_else(|error| error.into_inner());
        let Some(decoded) = from.as_ref() else {
            return;
        };
        let rows = self
            .rows
            .iter()
            .zip(sources)
            .map(|(row, source)| match source {
                Some(index) => decoded.rows[*index].clone(),
                None => Arc::new(decoded.scope.decode_owned(row.eval())),
            })
            .collect();
        *self.acl.write().unwrap_or_else(|error| error.into_inner()) = Some(DecodedAcl {
            scope: decoded.scope.clone(),
            rows,
        });
    }
}

impl CacheEntry {
    fn new(epoch: String, revision: u64, built: Instant, rows: Vec<CachedRow>) -> Self {
        let heap_bytes = rows.iter().map(row_heap_bytes).sum::<usize>()
            + rows.capacity() * std::mem::size_of::<CachedRow>();
        Self::with_heap(epoch, revision, built, rows, heap_bytes)
    }

    fn with_heap(
        epoch: String,
        revision: u64,
        built: Instant,
        rows: Vec<CachedRow>,
        heap_bytes: usize,
    ) -> Self {
        Self {
            epoch,
            revision,
            built,
            rows,
            heap_bytes,
            acl: Default::default(),
        }
    }
}

/// Estimated heap of one row: serde_json objects are a BTreeMap of owned
/// keys and values (node and allocator overhead included). Calibrated
/// against the counting allocator in `benches/snapshot_cache.rs` (el-rey
/// tasks: 8.2 KB per 1.8 KB JSON row, including the decoded ACL columns).
fn row_heap_bytes(row: &CachedRow) -> usize {
    fn heap(json: &Value) -> usize {
        match json {
            Value::String(text) => text.len(),
            Value::Array(values) => values.len() * 32 + values.iter().map(heap).sum::<usize>(),
            Value::Object(map) => map
                .iter()
                .map(|(key, item)| key.len() + 126 + heap(item))
                .sum::<usize>(),
            _ => 0,
        }
    }
    let mut bytes = 16 + row.key.len() + 16 + 64 + 16 + 32 + heap(&row.row);
    if let Some(eval) = &row.eval {
        bytes += 48 + heap(eval);
    }
    bytes
}

type Key = (String, String, u64, String, String);

struct Slot {
    entry: Option<Arc<CacheEntry>>,
    build: Arc<tokio::sync::Mutex<()>>,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub builds: u64,
    pub roll_forwards: u64,
    pub fallbacks: u64,
    pub evictions: u64,
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Default)]
pub struct SnapshotCache {
    slots: Mutex<HashMap<Key, Slot>>,
    tick: AtomicU64,
    hits: AtomicU64,
    builds: AtomicU64,
    roll_forwards: AtomicU64,
    fallbacks: AtomicU64,
    evictions: AtomicU64,
    disabled: std::sync::atomic::AtomicBool,
}

pub fn config() -> &'static CacheConfig {
    static CONFIG: OnceLock<CacheConfig> = OnceLock::new();
    CONFIG.get_or_init(CacheConfig::from_env)
}

/// What an open needs to look up an entry.
pub(crate) struct CacheKey<'a> {
    pub database_url: &'a str,
    pub project: &'a str,
    pub generation: u64,
    pub path: &'a str,
    pub args: &'a Value,
}

impl CacheKey<'_> {
    fn owned(&self) -> Key {
        (
            self.database_url.to_owned(),
            self.project.to_owned(),
            self.generation,
            self.path.to_owned(),
            replica::stable_json(self.args),
        )
    }
}

impl SnapshotCache {
    pub fn stats(&self) -> CacheStats {
        let slots = self.slots.lock().unwrap();
        let entries = slots.values().filter_map(|slot| slot.entry.as_ref());
        let (count, bytes) = entries.fold((0, 0), |(count, bytes), entry| {
            (count + 1, bytes + entry.heap_bytes as u64)
        });
        CacheStats {
            hits: self.hits.load(Relaxed),
            builds: self.builds.load(Relaxed),
            roll_forwards: self.roll_forwards.load(Relaxed),
            fallbacks: self.fallbacks.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
            entries: count,
            bytes,
        }
    }

    /// Whether opens use the cache (config, unless switched off at runtime).
    pub fn enabled(&self) -> bool {
        config().enabled && !self.disabled.load(Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.disabled.store(!enabled, Relaxed);
    }

    pub fn clear(&self) {
        self.slots.lock().unwrap().clear();
    }

    pub(crate) fn record_fallback(&self) {
        self.fallbacks.fetch_add(1, Relaxed);
    }

    fn current(&self, key: &Key) -> Option<Arc<CacheEntry>> {
        let mut slots = self.slots.lock().unwrap();
        let tick = self.tick.fetch_add(1, Relaxed);
        let slot = slots.get_mut(key)?;
        slot.last_used = tick;
        slot.entry.clone()
    }

    /// Drops `key`'s entry if it is still `stale` (the next open rebuilds).
    pub(crate) fn invalidate(&self, key: &CacheKey<'_>, stale: &Arc<CacheEntry>) {
        let mut slots = self.slots.lock().unwrap();
        if let Some(slot) = slots.get_mut(&key.owned()) {
            if slot
                .entry
                .as_ref()
                .is_some_and(|entry| Arc::ptr_eq(entry, stale))
            {
                slot.entry = None;
            }
        }
    }

    /// The entry of `key`, building it (once, however many opens wait) when
    /// missing or past its maximum age. `build` reads SQL.
    pub(crate) async fn entry<F, Fut>(
        &self,
        key: &CacheKey<'_>,
        build: F,
    ) -> Result<Option<Arc<CacheEntry>>, ReplicaError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<CacheEntry>, ReplicaError>>,
    {
        let max_age = config().max_age;
        let owned = key.owned();
        let fresh = |entry: &Arc<CacheEntry>| entry.built.elapsed() <= max_age;
        if let Some(entry) = self.current(&owned).filter(fresh) {
            self.hits.fetch_add(1, Relaxed);
            return Ok(Some(entry));
        }
        let build_lock = {
            let mut slots = self.slots.lock().unwrap();
            slots
                .entry(owned.clone())
                .or_insert_with(|| Slot {
                    entry: None,
                    build: Arc::default(),
                    last_used: 0,
                })
                .build
                .clone()
        };
        let _building = build_lock.lock().await;
        if let Some(entry) = self.current(&owned).filter(fresh) {
            self.hits.fetch_add(1, Relaxed);
            return Ok(Some(entry));
        }
        let Some(entry) = build().await? else {
            return Ok(None);
        };
        self.builds.fetch_add(1, Relaxed);
        let entry = Arc::new(entry);
        self.install(owned, entry.clone());
        Ok(Some(entry))
    }

    /// Replaces `stale` by `rolled` (a roll-forward) unless another open
    /// already replaced it.
    pub(crate) fn roll_forward(
        &self,
        key: &CacheKey<'_>,
        stale: &Arc<CacheEntry>,
        rolled: CacheEntry,
    ) {
        let owned = key.owned();
        {
            let slots = self.slots.lock().unwrap();
            let current = slots.get(&owned).and_then(|slot| slot.entry.as_ref());
            if !current.is_some_and(|entry| Arc::ptr_eq(entry, stale)) {
                return;
            }
        }
        self.roll_forwards.fetch_add(1, Relaxed);
        self.install(owned, Arc::new(rolled));
    }

    fn install(&self, key: Key, entry: Arc<CacheEntry>) {
        self.install_within(key, entry, config().budget_bytes)
    }

    fn install_within(&self, key: Key, entry: Arc<CacheEntry>, budget: usize) {
        let mut slots = self.slots.lock().unwrap();
        if entry.heap_bytes > budget {
            // Too large to keep: this open uses it, nobody else.
            if let Some(slot) = slots.get_mut(&key) {
                slot.entry = None;
            }
            return;
        }
        let tick = self.tick.fetch_add(1, Relaxed);
        let slot = slots.entry(key.clone()).or_insert_with(|| Slot {
            entry: None,
            build: Arc::default(),
            last_used: 0,
        });
        slot.entry = Some(entry);
        slot.last_used = tick;
        let mut total = slots
            .values()
            .filter_map(|slot| slot.entry.as_ref())
            .map(|entry| entry.heap_bytes)
            .sum::<usize>();
        while total > budget {
            let Some(victim) = slots
                .iter()
                .filter(|(other, slot)| **other != key && slot.entry.is_some())
                .min_by_key(|(_, slot)| slot.last_used)
                .map(|(other, _)| other.clone())
            else {
                break;
            };
            let slot = slots.get_mut(&victim).unwrap();
            total -= slot.entry.take().map_or(0, |entry| entry.heap_bytes);
            self.evictions.fetch_add(1, Relaxed);
        }
        slots.retain(|_, slot| slot.entry.is_some() || Arc::strong_count(&slot.build) > 1);
    }
}

/// Columns the cached rows need beyond the projection: visibility, the
/// collection's own filters, and the order column.
pub(crate) fn extra_columns(
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
) -> Vec<String> {
    let projected = definition.columns.iter().collect::<HashSet<_>>();
    let mut needed = visibility::acl_columns(plan);
    needed.extend(definition.exclude_when_set.iter().cloned());
    needed.extend(definition.equal_filters.keys().cloned());
    if !definition.order_by.is_empty() {
        needed.insert(definition.order_by.clone());
    }
    needed.insert(definition.key.clone());
    needed
        .into_iter()
        .filter(|column| !projected.contains(column))
        .collect()
}

/// Reads every row the collection's own filters admit (no visibility, no
/// budget) in snapshot order. Runs in its own read transaction.
pub(crate) async fn build_entry(
    transaction: &mut TenantTransaction,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    args: &Value,
) -> Result<CacheEntry, ReplicaError> {
    let started = Instant::now();
    let clock = replica::clock_in_transaction(transaction).await?;
    let args = args.as_object().ok_or(ReplicaError::InvalidArguments)?;
    let extra = extra_columns(definition, plan);
    let mut parameters = Vec::new();
    let mut predicates = vec!["1".to_owned()];
    for column in &definition.exclude_when_set {
        predicates.push(format!("r.{} IS NULL", replica::quote(column)?));
    }
    for (column, argument) in &definition.equal_filters {
        let value = args
            .get(argument)
            .ok_or(ReplicaError::InvalidArguments)?
            .clone();
        parameters.push(value);
        predicates.push(format!(
            "r.{} = ${}",
            replica::quote(column)?,
            parameters.len()
        ));
    }
    let columns = replica::declared_columns(
        transaction,
        definition,
        definition.columns.iter().chain(extra.iter()),
    )
    .await?;
    let mut statement = format!(
        "SELECT {} FROM {} AS r WHERE {}",
        gonvex_store::rows::json_object_expr("r", &columns),
        replica::quote(&definition.table)?,
        predicates.join(" AND ")
    );
    if !definition.order_by.is_empty() {
        let direction = if definition.order_direction == "asc" {
            "ASC"
        } else {
            "DESC"
        };
        statement.push_str(&format!(
            " ORDER BY r.{} {direction}, r.{} {direction}",
            replica::quote(&definition.order_by)?,
            replica::quote(&definition.key)?
        ));
    }
    let parameters = crate::sql_parameters::Parameters::scalars(&parameters)
        .map_err(ReplicaError::InvalidIdentifier)?;
    let mut stream = transaction
        .stream_json_rows(statement, parameters.into_owned_binds())
        .await?;
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    while let Some(batch) = stream.next_batch().await {
        let batch = batch?;
        for bytes in batch.rows() {
            let image = serde_json::from_slice::<Value>(bytes)
                .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))?;
            let row = if extra.is_empty() {
                // The image is the projection: move it, no second copy.
                CachedRow::from_projection(image, &Value::Null, definition, &extra)?
            } else {
                CachedRow::from_image(&image, definition, &extra)?
            };
            if !seen.insert(row.key.clone()) {
                return Err(ReplicaError::DuplicateKey(row.key.to_string()));
            }
            rows.push(row);
            if rows.len() % 256 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
    drop(stream);
    rows.shrink_to_fit();
    Ok(CacheEntry::new(clock.epoch, clock.revision, started, rows))
}

/// A member's rows at the open's clock.
pub struct View {
    pub rows: Vec<CachedRow>,
    pub window: ReplicaWindow,
    /// Source-table log rows replayed on top of the entry.
    pub replayed: usize,
    /// Every admitted row at the clock (set when the entry should roll).
    pub rolled: Option<CacheEntry>,
    /// For each rolled row, its index in the entry it rolled from (None for
    /// rows from the log).
    pub rolled_sources: Vec<Option<usize>>,
}

/// Rows at `revision` (entry + collapsed source-table log), filtered by
/// `visible`, within the collection's budgets. None when the window would be
/// truncated without an order (SQL's choice of rows is not reproducible).
#[allow(clippy::too_many_arguments)]
pub(crate) fn view(
    entry: &CacheEntry,
    changes: &[LogChange],
    revision: u64,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    args: &Value,
    visible: &mut dyn FnMut(&CachedRow, Option<usize>) -> bool,
) -> Result<Option<View>, ReplicaError> {
    let extra = extra_columns(definition, plan);
    // Keys whose rows the log replaces, and their rows at `revision`.
    let mut replaced = HashSet::<Arc<str>>::new();
    let mut inserted = Vec::new();
    for (id, change) in replica::collapse_changes(changes) {
        if id.0 != definition.table {
            continue;
        }
        for image in [change.old, change.new] {
            if !image.is_null() {
                if let Ok(key) = replica::row_key(image, &definition.key) {
                    replaced.insert(key.into());
                }
            }
        }
        if !change.new.is_null() && replica::collection_filters_admit(change.new, args, definition)
        {
            inserted.push(CachedRow::from_image(change.new, definition, &extra)?);
        }
    }
    let ordered = !definition.order_by.is_empty();
    if ordered {
        inserted.sort_by(|left, right| order(definition, left.eval(), right.eval()));
    }
    let source_rows = changes
        .iter()
        .filter(|change| change.table == definition.table)
        .count();
    let roll = source_rows > 0
        && (source_rows >= config().roll_changes
            || revision.saturating_sub(entry.revision) >= config().roll_revisions);
    let mut all = roll.then(|| Vec::with_capacity(entry.rows.len() + inserted.len()));
    let mut sources = Vec::with_capacity(if roll {
        entry.rows.len() + inserted.len()
    } else {
        0
    });
    let mut window = ReplicaWindow::default();
    let mut rows = Vec::new();
    let mut last: Option<&CachedRow> = None;
    let mut done = false;
    let mut kept = entry
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| replaced.is_empty() || !replaced.contains(&row.key))
        .peekable();
    let mut added = inserted.iter().peekable();
    loop {
        // Two-way merge in snapshot order (the log's rows after the cached
        // rows when unordered: the order of an unbounded set is free).
        let (row, index) = match (kept.peek(), added.peek()) {
            (Some((_, left)), Some(right)) => {
                if ordered && order(definition, right.eval(), left.eval()).is_lt() {
                    (added.next().unwrap(), None)
                } else {
                    let (index, row) = kept.next().unwrap();
                    (row, Some(index))
                }
            }
            (Some(_), None) => {
                let (index, row) = kept.next().unwrap();
                (row, Some(index))
            }
            (None, Some(_)) => (added.next().unwrap(), None),
            (None, None) => break,
        };
        if let Some(all) = &mut all {
            all.push(row.clone());
            sources.push(index);
        }
        if done || !visible(row, index) {
            continue;
        }
        if (definition.max_rows > 0 && window.rows >= definition.max_rows)
            || (definition.max_bytes > 0 && window.bytes + row.bytes as i64 > definition.max_bytes)
        {
            window.truncated = true;
            done = true;
            if all.is_none() {
                break;
            }
            continue;
        }
        window.rows += 1;
        window.bytes += row.bytes as i64;
        last = Some(row);
        rows.push(row.clone());
    }
    if window.truncated {
        if !ordered {
            return Ok(None);
        }
        window.boundary = last.map(|row| {
            let eval = row.eval();
            Box::new((
                eval.get(&definition.order_by)
                    .cloned()
                    .unwrap_or(Value::Null),
                eval.get(&definition.key).cloned().unwrap_or(Value::Null),
            ))
        });
    }
    let rolled = all.map(|all| {
        // Heap of the rolled entry, without walking every row again.
        let removed = entry
            .rows
            .iter()
            .filter(|row| replaced.contains(&row.key))
            .map(row_heap_bytes)
            .sum::<usize>();
        let added = inserted.iter().map(row_heap_bytes).sum::<usize>();
        let heap = (entry.heap_bytes + added).saturating_sub(removed)
            + all.capacity().saturating_sub(entry.rows.capacity())
                * std::mem::size_of::<CachedRow>();
        CacheEntry::with_heap(entry.epoch.clone(), revision, entry.built, all, heap)
    });
    Ok(Some(View {
        rows,
        window,
        replayed: source_rows,
        rolled,
        rolled_sources: sources,
    }))
}

fn order(
    definition: &ReplicaCollectionDefinition,
    left: &Value,
    right: &Value,
) -> std::cmp::Ordering {
    let null = Value::Null;
    let ordering = replica::sql_order(
        left.get(&definition.order_by).unwrap_or(&null),
        right.get(&definition.order_by).unwrap_or(&null),
    )
    .then_with(|| {
        replica::sql_order(
            left.get(&definition.key).unwrap_or(&null),
            right.get(&definition.key).unwrap_or(&null),
        )
    });
    if definition.order_direction == "asc" {
        ordering
    } else {
        ordering.reverse()
    }
}

/// Where the indexes `resolved` evaluates through stand relative to a clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexState {
    /// Every index is exactly at the clock: in-memory visibility equals SQL
    /// visibility at the clock.
    Exact,
    /// Some index is behind (its applier has not reached the clock yet) or
    /// ahead (a later transaction was applied): a read at a newer clock,
    /// after the indexes catch up, can be exact.
    Moving,
    /// Poisoned or another epoch.
    Unusable,
}

fn indexes(
    resolved: &ResolvedVisibility,
) -> Vec<&std::sync::RwLock<crate::visibility_index::ParentAclIndex>> {
    let mut indexes = Vec::new();
    let mut pending = vec![resolved];
    while let Some(resolved) = pending.pop() {
        indexes.extend(resolved.indexes.values().map(|index| &**index));
        for parent in resolved.inherited.values() {
            indexes.push(&*parent.index);
            pending.push(&parent.resolved);
        }
    }
    indexes
}

/// Never waits (callers hold a read connection).
pub(crate) fn index_state(resolved: &ResolvedVisibility, revision: u64, epoch: &str) -> IndexState {
    let mut state = IndexState::Exact;
    for index in indexes(resolved) {
        match crate::visibility_index::usable_revision(index, epoch) {
            None => return IndexState::Unusable,
            Some(at) if at != revision => state = IndexState::Moving,
            Some(_) => {}
        }
    }
    state
}

pub(crate) fn indexes_at(resolved: &ResolvedVisibility, revision: u64, epoch: &str) -> bool {
    indexes(resolved)
        .into_iter()
        .all(|index| crate::visibility_index::exactly_at(index, revision, epoch))
}

/// Source-table log rows after `after` through `through`.
pub(crate) async fn source_changes(
    transaction: &mut TenantTransaction,
    table: &str,
    after: u64,
    through: u64,
) -> Result<Vec<LogChange>, ReplicaError> {
    if after >= through {
        return Ok(Vec::new());
    }
    replica::resume_changes(
        transaction,
        after,
        through,
        std::collections::BTreeSet::from([table.to_owned()]),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(bytes: usize) -> Arc<CacheEntry> {
        Arc::new(CacheEntry {
            epoch: "e".into(),
            revision: 1,
            built: Instant::now(),
            rows: Vec::new(),
            heap_bytes: bytes,
            acl: Default::default(),
        })
    }

    fn key(name: &str) -> Key {
        ("db".into(), "p".into(), 1, name.into(), "{}".into())
    }

    #[test]
    fn evicts_least_recently_used_entries_beyond_the_budget() {
        let cache = SnapshotCache::default();
        cache.install_within(key("a"), entry(40), 100);
        cache.install_within(key("b"), entry(40), 100);
        // Touch a: b becomes the least recently used.
        assert!(cache.current(&key("a")).is_some());
        cache.install_within(key("c"), entry(40), 100);
        assert!(cache.current(&key("a")).is_some());
        assert!(cache.current(&key("b")).is_none());
        assert!(cache.current(&key("c")).is_some());
        let stats = cache.stats();
        assert_eq!((stats.entries, stats.bytes, stats.evictions), (2, 80, 1));
        // An entry larger than the whole budget is never kept.
        cache.install_within(key("huge"), entry(200), 100);
        assert!(cache.current(&key("huge")).is_none());
        assert_eq!(cache.stats().entries, 2);
    }
}
