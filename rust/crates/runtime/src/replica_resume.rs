//! Server-validated resume metadata. Never retains rows or encoded payloads.
use super::*;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct Anchor {
    pub cursor: ReplicaCursor,
    pub digest: ReplicaAggregate,
    pub window: ReplicaWindow,
    pub fingerprint: Box<str>,
}

struct Entry {
    anchor: Anchor,
    until: Instant,
    used: u64,
    bytes: usize,
}

#[derive(Default)]
struct State {
    entries: BTreeMap<String, Entry>,
    bytes: usize,
    tick: u64,
}

pub(crate) struct ValidationCache {
    state: Mutex<State>,
    entries: usize,
    bytes: usize,
    ttl: Duration,
}

impl Default for ValidationCache {
    fn default() -> Self {
        let number = |name: &str, default| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let enabled = !std::env::var("GONVEX_RESUME_VALIDATION")
            .is_ok_and(|s| matches!(s.as_str(), "0" | "false" | "off"));
        Self::with_limits(
            if enabled {
                number("GONVEX_RESUME_VALIDATION_ENTRIES", 8192)
            } else {
                0
            },
            number("GONVEX_RESUME_VALIDATION_BYTES", 8 << 20),
            Duration::from_secs(number("GONVEX_RESUME_VALIDATION_TTL_SECS", 120) as u64),
        )
    }
}

impl ValidationCache {
    fn base_bytes() -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<State>()
            + 11 * std::mem::size_of::<(String, Entry)>()
            + 256
    }
    #[cfg(feature = "perf-fixture")]
    pub(crate) fn stats(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.entries.len(), state.bytes + Self::base_bytes())
    }
    #[cfg(feature = "perf-fixture")]
    pub(crate) fn clear(&self) {
        *self.state.lock().unwrap() = State::default();
    }
    pub(super) fn enabled(&self) -> bool {
        self.entries > 0 && self.bytes > 0
    }
    fn with_limits(entries: usize, bytes: usize, ttl: Duration) -> Self {
        Self {
            state: Mutex::default(),
            entries,
            bytes,
            ttl,
        }
    }

    pub(super) fn candidate(&self, key: &str, cursor: &ReplicaCursor) -> Option<Anchor> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let expired = state
            .entries
            .get(key)
            .is_some_and(|e| Instant::now() >= e.until);
        if expired {
            let e = state.entries.remove(key)?;
            state.bytes -= e.bytes;
            return None;
        }
        state.tick = state.tick.wrapping_add(1);
        let tick = state.tick;
        let entry = state.entries.get_mut(key)?;
        if entry.anchor.cursor.epoch != cursor.epoch
            || entry.anchor.cursor.revision > cursor.revision
        {
            return None;
        }
        entry.used = tick;
        Some(entry.anchor.clone())
    }

    /// Called only after an authoritative validation or fully verified replay
    /// and a successful read commit. Cancellation never installs partial work.
    pub(super) fn remember(&self, key: String, anchor: Anchor) {
        if self.entries == 0 || anchor.window.truncated || anchor.window.boundary.is_some() {
            return;
        }
        // A whole full-capacity BTree node per entry is deliberately charged:
        // at most one node per live entry, including sparse/root nodes and
        // internal child pointers. This overcounts actual node allocation.
        let nodes = 11 * std::mem::size_of::<(String, Entry)>() + 256;
        let bytes = nodes
            .saturating_add(key.capacity())
            .saturating_add(anchor.cursor.epoch.capacity())
            .saturating_add(anchor.fingerprint.len())
            .saturating_add(match &anchor.digest {
                ReplicaAggregate::Legacy(_) => 0,
                ReplicaAggregate::Sum(_) => std::mem::size_of::<ReplicaSetDigest>(),
            });
        if bytes.saturating_add(Self::base_bytes()) > self.bytes {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.entries.get(&key).is_some_and(|e| {
            e.anchor.cursor.epoch == anchor.cursor.epoch
                && e.anchor.cursor.revision > anchor.cursor.revision
        }) {
            return;
        }
        if let Some(old) = state.entries.remove(&key) {
            state.bytes -= old.bytes;
        }
        let now = Instant::now();
        let expired: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, e)| e.until <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            if let Some(old) = state.entries.remove(&key) {
                state.bytes -= old.bytes;
            }
        }
        while state.entries.len() >= self.entries
            || state
                .bytes
                .saturating_add(bytes)
                .saturating_add(Self::base_bytes())
                > self.bytes
        {
            let Some(victim) = state
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
            else {
                return;
            };
            if let Some(old) = state.entries.remove(&victim) {
                state.bytes -= old.bytes;
            }
        }
        state.tick = state.tick.wrapping_add(1);
        let used = state.tick;
        state.bytes += bytes;
        state.entries.insert(
            key,
            Entry {
                anchor,
                until: now + self.ttl,
                used,
                bytes,
            },
        );
    }
}

/// A candidate locator is not an authorization proof: the recursive resolved
/// fingerprint is compared after the admitted read, and replay is validated.
pub(super) fn key(
    session: &TenantSession,
    request: &ReplicaOpenRequest,
    module: &crate::modules::ProjectModule,
    definition: &ReplicaCollectionDefinition,
    scope: &str,
    sum: bool,
) -> String {
    serde_json::json!({"version": "resume-validation-v1", "project": module.project_id,
        "database": session.route.database_url, "tenant": session.route.tenant_id,
        "generation": module.generation, "artifact": module.artifact_hash,
        "path": request.path, "args": stable_json(&request.args), "account": session.identity.account.id,
        "member": session.member.id, "membershipRevision": session.member.membership_revision, "admissionRevision": session.admission_revision, "scope": scope, "window": {"table":definition.table,"key":definition.key,"maxRows":definition.max_rows,"maxBytes":definition.max_bytes,"order":definition.order_by,"direction":definition.order_direction,"mode":definition.mode}, "sum": sum}).to_string()
}

pub(super) fn fingerprint(resolved: &ResolvedVisibility) -> String {
    fn tree(resolved: &ResolvedVisibility) -> Value {
        serde_json::json!({"own": resolved.fingerprint, "parents": resolved.inherited.iter()
            .map(|(table, parent)| (table.clone(), tree(&parent.resolved))).collect::<BTreeMap<_, _>>()})
    }
    serde_json::json!({"version": "resolved-visibility-v1", "value": tree(resolved)}).to_string()
}

/// No base rows are read on success. A failed candidate releases this read and
/// its build permit before the caller enters the original cache-build path.
#[allow(clippy::too_many_arguments)]
pub(super) async fn try_open(
    runtime: &Runtime,
    session: &TenantSession,
    request: &ReplicaOpenRequest,
    definition: &Arc<ReplicaCollectionDefinition>,
    plan: &Arc<VisibilityPlan>,
    locator: &str,
    identity: &(u64, String),
    visibility_scope: &str,
) -> Result<Option<ReplicaOpenResult>, ReplicaError> {
    let Some(requested) = request.cursor.as_ref() else {
        return Ok(None);
    };
    let key = epoch_key(locator, &requested.epoch);
    let Some(anchor) = runtime.inner.resume_validation.candidate(&key, requested) else {
        return Ok(None);
    };
    if anchor.cursor == *requested
        && request.digest.as_deref() != Some(anchor.digest.digest().as_str())
    {
        return Ok(None);
    }
    let mut read = runtime
        .begin_collection_read(session, definition, plan, &request.args)
        .await?;
    let outcome = validate(
        session, request, definition, plan, identity, &anchor, &mut read,
    )
    .await?;
    read.transaction.commit().await?;
    let Some((digest, window, upserts, deleted)) = outcome else {
        // commit consumed/released the transaction; release admission before
        // returning to the caller's original cache-build/read fallback.
        drop(read.permit);
        drop(read._active);
        return Ok(None);
    };
    let cursor = cursor_for(&read.clock, definition, visibility_scope);
    read.permit.observe_json_bytes(window.bytes.max(0) as usize);
    runtime.inner.resume_validation.remember(
        key,
        Anchor {
            cursor: cursor.clone(),
            digest: digest.clone(),
            window: window.clone(),
            fingerprint: fingerprint(&read.resolved).into(),
        },
    );
    let mut messages = Vec::new();
    if !upserts.is_empty() || !deleted.is_empty() || request.cursor.as_ref() != Some(&cursor) {
        messages.push(replica_delta(
            &request.id,
            &request.path,
            cursor.clone(),
            upserts,
            deleted,
            digest.digest(),
            false,
        ));
    }
    messages.push(replica_ready(
        &request.id,
        &request.path,
        definition,
        cursor.clone(),
        digest.digest(),
        false,
    ));
    Ok(Some(ReplicaOpenResult {
        messages,
        subscription: Some(ReplicaSubscription {
            id: request.id.clone(),
            path: request.path.clone(),
            args: request.args.clone(),
            definition: Arc::clone(definition),
            visibility: Arc::clone(plan),
            visibility_scope: visibility_scope.to_owned(),
            cursor,
            resolved: read.resolved,
            digest,
            window,
            mask: Default::default(),
            concern_tables: None,
            parent_acl_columns: None,
            wake_member: None,
            fanout: None,
        }),
    }))
}

type Delta = (ReplicaAggregate, ReplicaWindow, Vec<Value>, Vec<String>);

async fn validate(
    session: &TenantSession,
    request: &ReplicaOpenRequest,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    identity: &(u64, String),
    anchor: &Anchor,
    read: &mut CollectionRead,
) -> Result<Option<Delta>, ReplicaError> {
    let requested = request.cursor.as_ref().expect("candidate requires cursor");
    if read.module_identity.as_ref() != Some(identity)
        || read.resolved.ephemeral_member
        || !cursor_resumable(
            Some(requested),
            &cursor_for(&read.clock, definition, ""),
            read.clock.retained_revision,
        )
        || anchor.cursor.revision < read.clock.retained_revision
        || anchor.cursor.epoch != read.clock.epoch
        || anchor.window.truncated
        || anchor.window.boundary.is_some()
        || anchor.fingerprint.as_ref() != fingerprint(&read.resolved)
        || !crate::snapshot_cache::indexes_at(
            &read.resolved,
            read.clock.revision,
            &read.clock.epoch,
        )
    {
        return Ok(None);
    }
    let active = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS (SELECT 1 FROM members WHERE account_id=$1 AND status='active')",
    )
    .bind(&session.identity.account.id)
    .fetch_one(&mut **read.transaction.transaction())
    .await?;
    if active == 0 {
        return Ok(None);
    }
    let after = anchor.cursor.revision;
    let through = read.clock.revision;
    // Normal retention is atomic with the retained floor. Also reject missing
    // transaction headers, whole revisions and internal ordinal gaps. The
    // schema has no trailing-row checksum: arbitrary storage corruption is not
    // claimed detectable; it is outside the change-log writer contract.
    if through > after && !history_complete(&mut read.transaction, after, through).await? {
        return Ok(None);
    }
    let dependencies = plan.resolved_dependency_columns(&read.resolved);
    let mut tables = dependencies.tables();
    tables.insert(definition.table.clone());
    let changes = if through > after {
        crate::raw_log::read_changes(&mut read.transaction, after, through, tables).await?
    } else {
        Vec::new()
    };
    if table_change_relevance(&definition.table, &dependencies, changes.iter()).1 {
        return Ok(None);
    }
    if changes.iter().any(|c| c.table == definition.table) {
        // Delta membership must have a unique, immutable SQL key. Arbitrary
        // projected keys still use the authoritative duplicate-key validator.
        if definition.key != plan.key {
            return Ok(None);
        }
        if !single_sql_key(&mut read.transaction, &definition.table, &definition.key).await? {
            return Ok(None);
        }
    }
    let split = changes.partition_point(|c| c.revision <= requested.revision);
    let mut state = anchor.clone();
    if apply(
        &mut state,
        &changes[..split],
        request,
        definition,
        plan,
        &read.resolved,
        false,
    )
    .is_none()
        || request.digest.as_deref() != Some(state.digest.digest().as_str())
    {
        return Ok(None);
    }
    let Some((upserts, deleted)) = apply(
        &mut state,
        &changes[split..],
        request,
        definition,
        plan,
        &read.resolved,
        true,
    ) else {
        return Ok(None);
    };
    if !crate::snapshot_cache::indexes_at(&read.resolved, read.clock.revision, &read.clock.epoch) {
        return Ok(None);
    }
    Ok(Some((state.digest, state.window, upserts, deleted)))
}

/// `key` is `table`'s only primary-key column. Through the records-aware
/// catalog: a logical record view (contract 3) is a SQL VIEW, whose columns
/// `pragma_table_info` all reports with `pk=0`; its key is the record's public
/// key, which maps one to one onto its storage table's SQL primary key
/// (directly, or through the immutable `_gonvex_identity_keys`), and every
/// record change is logged under that key.
async fn single_sql_key(
    transaction: &mut TenantTransaction,
    table: &str,
    key: &str,
) -> Result<bool, ReplicaError> {
    let columns = gonvex_store::rows::table_columns(transaction.transaction(), table).await?;
    let mut primary = columns.iter().filter(|column| column.primary_key);
    Ok(matches!((primary.next(), primary.next()), (Some(column), None) if column.name == key))
}

async fn history_complete(
    transaction: &mut TenantTransaction,
    after: u64,
    through: u64,
) -> Result<bool, ReplicaError> {
    const MAX_REPLAY: u64 = 4096;
    if through.saturating_sub(after) > MAX_REPLAY {
        return Ok(false);
    }
    let rows = sqlx::query("SELECT c.revision, COUNT(*) AS n, COUNT(DISTINCT c.ordinal) AS distinct_n, MIN(c.ordinal) AS lo, MAX(c.ordinal) AS hi FROM _gonvex_sync_changes c JOIN _gonvex_sync_transactions t ON t.revision=c.revision WHERE c.revision>$1 AND c.revision<=$2 GROUP BY c.revision ORDER BY c.revision")
        .bind(after as i64).bind(through as i64).fetch_all(&mut **transaction.transaction()).await?;
    if rows.len() as u64 != through - after {
        return Ok(false);
    }
    let mut total = 0i64;
    for (i, row) in rows.iter().enumerate() {
        let n: i64 = row.get("n");
        total = total.saturating_add(n);
        if row.get::<i64, _>("revision") != (after + i as u64 + 1) as i64
            || n <= 0
            || row.get::<i64, _>("distinct_n") != n
            || row.get::<i64, _>("lo") != 1
            || row.get::<i64, _>("hi") != n
            || total > MAX_REPLAY as i64
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn apply(
    anchor: &mut Anchor,
    changes: &[crate::raw_log::RawChange],
    request: &ReplicaOpenRequest,
    definition: &ReplicaCollectionDefinition,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    emit: bool,
) -> Option<(Vec<Value>, Vec<String>)> {
    let image = |image: Option<&crate::raw_log::RawImage>,
                 id: &str|
     -> Option<Option<(Value, String, i64)>> {
        let Some(image) = image else {
            return Some(None);
        };
        if !image.filters_admit(&request.args, definition) {
            return Some(None);
        }
        let _site = crate::raw_sites::raw_site!(ResumeImage);
        let value = image.value();
        if !visible(plan, resolved, &value) {
            return Some(None);
        }
        let row = project_row(&value, definition).ok()?;
        if row_key(&row, &definition.key).ok()?.as_str() != id {
            return None;
        }
        let canonical = stable_json(&row);
        Some(Some((
            row,
            hex_digest(canonical.as_bytes()),
            canonical.len().try_into().ok()?,
        )))
    };
    let mut upserts = BTreeMap::new();
    let mut deleted = BTreeSet::new();
    for (id, change) in crate::raw_log::collapse(changes) {
        if id.0 != definition.table {
            continue;
        }
        let old = image(change.old, id.1)?;
        let new = image(change.new, id.1)?;
        if let Some((_, hash, bytes)) = &old {
            anchor.window.rows = anchor.window.rows.checked_sub(1)?;
            anchor.window.bytes = anchor.window.bytes.checked_sub(*bytes)?;
            if anchor.window.bytes < 0 {
                return None;
            }
            anchor.digest.remove(hash);
        }
        if let Some((_, hash, bytes)) = &new {
            anchor.window.rows = anchor.window.rows.checked_add(1)?;
            anchor.window.bytes = anchor.window.bytes.checked_add(*bytes)?;
            anchor.digest.add(hash);
        }
        if (definition.max_rows > 0 && anchor.window.rows > definition.max_rows)
            || (definition.max_bytes > 0 && anchor.window.bytes > definition.max_bytes)
        {
            return None;
        }
        if emit {
            match (old, new) {
                (Some((_, old, _)), Some((row, new, _))) if old != new => {
                    upserts.insert(id.1.to_owned(), row);
                }
                (None, Some((row, _, _))) => {
                    upserts.insert(id.1.to_owned(), row);
                }
                (Some(_), None) => {
                    deleted.insert(id.1.to_owned());
                }
                _ => {}
            }
        }
    }
    Some((
        upserts.into_values().collect(),
        deleted.into_iter().collect(),
    ))
}

pub(super) fn epoch_key(locator: &str, epoch: &str) -> String {
    format!("{locator}\0{epoch}")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn anchor(revision: u64) -> Anchor {
        Anchor {
            cursor: ReplicaCursor {
                epoch: "epoch".into(),
                revision,
            },
            digest: ReplicaAggregate::new(false),
            window: ReplicaWindow::default(),
            fingerprint: "resolved-v1".into(),
        }
    }
    #[test]
    fn lru_entry_limit_and_old_completion_never_replace_newer_anchor() {
        let cache = ValidationCache::with_limits(2, 1 << 20, Duration::from_secs(120));
        cache.remember("a".into(), anchor(2));
        cache.remember("b".into(), anchor(1));
        assert!(cache.candidate("a", &anchor(2).cursor).is_some());
        cache.remember("c".into(), anchor(1));
        assert!(cache.candidate("b", &anchor(1).cursor).is_none());
        cache.remember("a".into(), anchor(1));
        assert_eq!(
            cache
                .candidate("a", &anchor(2).cursor)
                .unwrap()
                .cursor
                .revision,
            2
        );
        assert!(cache.candidate("a", &anchor(1).cursor).is_none());
    }
    #[test]
    fn ttl_disabled_byte_limit_and_boundaries_are_conservative() {
        let expired = ValidationCache::with_limits(1, 1 << 20, Duration::ZERO);
        expired.remember("a".into(), anchor(1));
        assert!(expired.candidate("a", &anchor(1).cursor).is_none());
        let disabled = ValidationCache::with_limits(0, 1 << 20, Duration::from_secs(10));
        disabled.remember("a".into(), anchor(1));
        assert!(disabled.state.lock().unwrap().entries.is_empty());
        let tiny = ValidationCache::with_limits(1, 1, Duration::from_secs(10));
        tiny.remember("a".into(), anchor(1));
        assert!(tiny.state.lock().unwrap().entries.is_empty());
        let cache = ValidationCache::with_limits(10, 8192, Duration::from_secs(10));
        for i in 0..100 {
            cache.remember(format!("{i:01024}"), anchor(1));
        }
        let state = cache.state.lock().unwrap();
        assert!(state.bytes + ValidationCache::base_bytes() <= 8192);
        drop(state);
        let mut truncated = anchor(1);
        truncated.window.truncated = true;
        cache.remember("truncated".into(), truncated);
        assert!(cache.candidate("truncated", &anchor(1).cursor).is_none());
        let mut boundary = anchor(1);
        boundary.window.boundary = Some(Box::new((Value::Null, Value::Null)));
        cache.remember("boundary".into(), boundary);
        assert!(cache.candidate("boundary", &anchor(1).cursor).is_none());
    }

    /// Log-only resume needs the collection key to be the relation's single
    /// SQL key; a registered logical record view qualifies through its
    /// record schema, although SQLite reports no primary key for a view.
    #[tokio::test]
    async fn delta_key_check_reads_record_views_through_their_schema() {
        let directory = tempfile::tempdir().unwrap();
        let url = gonvex_store::database_url(&directory.path().join("keys.db"));
        let pools = gonvex_store::PoolRegistry::new(gonvex_store::PoolLimits::default());
        let mut connection = pools.pool(&url).await.unwrap().acquire().await.unwrap();
        gonvex_store::execute_script(
            &mut connection,
            r#"CREATE TABLE plain(id TEXT PRIMARY KEY, title TEXT);
               CREATE TABLE pair(a TEXT, b TEXT, PRIMARY KEY (a, b));
               CREATE TABLE keyless(id TEXT, title TEXT);
               CREATE TABLE taskStorage(_id INTEGER PRIMARY KEY, title TEXT);
               CREATE VIEW tasks AS SELECT CAST(_id AS TEXT) _id, title FROM taskStorage;
               CREATE VIEW unregistered AS SELECT id, title FROM plain;
               CREATE TABLE _gonvex_record_views(name TEXT PRIMARY KEY,storage_table TEXT NOT NULL UNIQUE,key_column TEXT NOT NULL,schema TEXT NOT NULL);
               INSERT INTO _gonvex_record_views VALUES('tasks','taskStorage','_id','{"key":"_id","columns":{"_id":{"type":"text"},"title":{"type":"text"}}}');"#,
        )
        .await
        .unwrap();
        drop(connection);
        let mut read = gonvex_store::begin_transaction(&pools, &url, true)
            .await
            .unwrap();
        for (table, key, single) in [
            ("plain", "id", true),
            ("plain", "title", false),
            ("pair", "a", false),
            ("keyless", "id", false),
            ("unregistered", "id", false),
            ("tasks", "_id", true),
            ("tasks", "title", false),
            ("missing", "id", false),
        ] {
            assert_eq!(
                single_sql_key(&mut read, table, key).await.unwrap(),
                single,
                "{table}.{key}"
            );
        }
        drop(read);
        pools.close().await;
    }
}
