//! Per-tenant permission bitsets: one bit per (permission, scope).
//!
//! A module may declare a permission grant projection in its manifest:
//!
//! ```text
//! "permissionGrants": {"table": "memberPermissionGrants", "member": "memberId",
//!   "permission": "permission", "scope": "scope", "whereNull": ["deletedAt"]}
//! ```
//!
//! Each row of `table` grants `member` the permission `permission` in `scope`
//! (an empty or null scope is the tenant-wide scope `""`). The application
//! keeps the projection current inside the same Reducer transaction as the
//! grant itself (team membership, role, role permission, spot assignment...),
//! flattening derived permissions (spot-tree descendants, role defaults) into
//! rows. Rows with a non-null `whereNull` column grant nothing.
//!
//! The runtime holds the projection of each tenant database in memory as
//! bitsets: every distinct (permission, scope) pair gets a bit position, and
//! each member's grants are one dense bitset (a reference count per bit keeps
//! duplicate rows correct). Reads are bit tests: `ctx.permissions.has`.
//!
//! **Commit order.** The index is loaded from SQLite in a read transaction
//! after its applier subscribed to the tenant change feed, and then changed
//! only by that applier, from committed feed transactions in revision order.
//! The feed publishes a transaction only after SQLite committed it, so a
//! rolled-back or crashed Reducer never flips a bit. A reader whose own
//! transaction already sees revision R waits until the index reached R
//! (`PermissionIndex::wait_for`). A Reducer reads the committed state, not its
//! own uncommitted grant rows. Feed lag, reset, epoch change or applier exit
//! poisons the index; the next `ensure` reloads it.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use crate::visibility_class::FxMap;

/// The manifest declaration of a permission grant projection.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PermissionGrantSource {
    pub table: String,
    pub member: String,
    pub permission: String,
    pub scope: String,
    /// Columns that must be null for a row to grant (e.g. `deletedAt`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub where_null: Vec<String>,
    /// Row id column of the table in the change feed (default `_id`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
}

impl PermissionGrantSource {
    pub fn validate(&self) -> Result<(), String> {
        for column in [&self.table, &self.member, &self.permission, &self.scope]
            .into_iter()
            .chain(self.where_null.iter())
            .chain((!self.key.is_empty()).then_some(&self.key))
        {
            crate::visibility::identifier_ok(column).map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn key_column(&self) -> &str {
        if self.key.is_empty() {
            "_id"
        } else {
            &self.key
        }
    }

    /// (member, permission, scope) a projected row grants, if any.
    fn grant(&self, row: &Value) -> Option<(String, String, String)> {
        let row = row.as_object()?;
        if self
            .where_null
            .iter()
            .any(|column| row.get(column).is_some_and(|value| !value.is_null()))
        {
            return None;
        }
        let text = |column: &str| match row.get(column) {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.clone()),
            Some(value) => Some(value.to_string()),
        };
        Some((
            text(&self.member)?,
            text(&self.permission)?,
            text(&self.scope).unwrap_or_default(),
        ))
    }
}

/// One member's bitset plus per-bit reference counts.
#[derive(Clone, Debug, Default)]
struct MemberBits {
    words: Vec<u64>,
    counts: FxMap<u32, u32>,
}

impl MemberBits {
    fn add(&mut self, bit: u32) {
        let count = self.counts.entry(bit).or_default();
        *count += 1;
        if *count == 1 {
            let word = (bit / 64) as usize;
            if self.words.len() <= word {
                self.words.resize(word + 1, 0);
            }
            self.words[word] |= 1 << (bit % 64);
        }
    }

    fn remove(&mut self, bit: u32) {
        let Some(count) = self.counts.get_mut(&bit) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            self.counts.remove(&bit);
            self.words[(bit / 64) as usize] &= !(1 << (bit % 64));
        }
    }

    #[inline]
    fn test(&self, bit: u32) -> bool {
        self.words
            .get((bit / 64) as usize)
            .is_some_and(|word| word & (1 << (bit % 64)) != 0)
    }
}

/// The permission bitsets of one tenant database.
#[derive(Debug)]
pub struct PermissionIndex {
    epoch: String,
    revision: u64,
    poisoned: bool,
    positions: FxMap<Box<str>, u32>,
    members: FxMap<Box<str>, MemberBits>,
    /// Row id → (member, bit) it grants, to undo updates and deletes.
    rows: FxMap<Box<str>, (Box<str>, u32)>,
    signal: tokio::sync::watch::Sender<(u64, bool)>,
}

fn position_key(permission: &str, scope: &str) -> String {
    let mut key = String::with_capacity(permission.len() + scope.len() + 1);
    key.push_str(permission);
    key.push('\u{0}');
    key.push_str(scope);
    key
}

impl PermissionIndex {
    pub fn new(epoch: String, revision: u64) -> Self {
        Self {
            epoch,
            revision,
            poisoned: false,
            positions: FxMap::default(),
            members: FxMap::default(),
            rows: FxMap::default(),
            signal: tokio::sync::watch::channel((revision, false)).0,
        }
    }

    /// Build from loaded (row id, row) pairs.
    pub fn from_rows(
        source: &PermissionGrantSource,
        epoch: String,
        revision: u64,
        rows: impl IntoIterator<Item = (String, Value)>,
    ) -> Self {
        let mut index = Self::new(epoch, revision);
        for (id, row) in rows {
            index.insert_row(source, &id, &row);
        }
        index
    }

    fn insert_row(&mut self, source: &PermissionGrantSource, id: &str, row: &Value) {
        let Some((member, permission, scope)) = source.grant(row) else {
            return;
        };
        let key = position_key(&permission, &scope);
        let next = self.positions.len() as u32;
        let bit = *self.positions.entry(key.into()).or_insert(next);
        let member: Box<str> = member.into();
        self.members.entry(member.clone()).or_default().add(bit);
        self.rows.insert(id.into(), (member, bit));
    }

    fn remove_row(&mut self, id: &str) {
        if let Some((member, bit)) = self.rows.remove(id) {
            if let Some(bits) = self.members.get_mut(&member) {
                bits.remove(bit);
            }
        }
    }

    /// Apply one committed transaction's rows of the grant table.
    pub fn apply(
        &mut self,
        source: &PermissionGrantSource,
        revision: u64,
        changes: &[crate::change_feed::LogChange],
    ) {
        if revision <= self.revision {
            return;
        }
        for change in changes.iter().filter(|change| change.table == source.table) {
            self.remove_row(&change.row_id);
            if !change.new_value.is_null() {
                self.insert_row(source, &change.row_id, &change.new_value);
            }
        }
        self.revision = revision;
        self.signal.send_replace((revision, self.poisoned));
    }

    pub fn poison(&mut self) {
        self.poisoned = true;
        self.signal.send_replace((self.revision, true));
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// The bit position of (permission, scope), if any member ever held it.
    pub fn position(&self, permission: &str, scope: &str) -> Option<u32> {
        self.positions
            .get(position_key(permission, scope).as_str())
            .copied()
    }

    /// Does `member` hold `permission` in `scope` (exact scope)?
    pub fn has(&self, member: &str, permission: &str, scope: &str) -> bool {
        let Some(bit) = self.position(permission, scope) else {
            return false;
        };
        self.members.get(member).is_some_and(|bits| bits.test(bit))
    }

    /// Does `member` hold the bit at `position`?
    #[inline]
    pub fn has_position(&self, member: &str, position: u32) -> bool {
        self.members
            .get(member)
            .is_some_and(|bits| bits.test(position))
    }

    /// The member's whole permission state as bitset words.
    pub fn bits(&self, member: &str) -> &[u64] {
        self.members
            .get(member)
            .map_or(&[], |bits| bits.words.as_slice())
    }

    /// Distinct (permission, scope) bits and members with any grant.
    pub fn size(&self) -> (usize, usize) {
        (self.positions.len(), self.members.len())
    }

    /// Approximate heap bytes.
    pub fn heap_bytes(&self) -> usize {
        self.positions.keys().map(|key| key.len() + 32).sum::<usize>()
            + self
                .members
                .iter()
                .map(|(member, bits)| {
                    member.len() + 64 + bits.words.len() * 8 + bits.counts.len() * 16
                })
                .sum::<usize>()
            + self.rows.keys().map(|id| id.len() * 2 + 48).sum::<usize>()
    }

    /// Wait until the index applied `revision` (or was poisoned). Returns
    /// whether it reached it.
    pub async fn wait_for(index: &SharedPermissions, revision: u64) -> bool {
        let mut receiver = {
            let guard = index.read().unwrap_or_else(|e| e.into_inner());
            if guard.revision >= revision {
                return true;
            }
            if guard.poisoned {
                return false;
            }
            guard.signal.subscribe()
        };
        let reached = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            receiver.wait_for(|(applied, poisoned)| *applied >= revision || *poisoned),
        )
        .await;
        matches!(reached, Ok(Ok(state)) if state.0 >= revision && !state.1)
    }
}

pub type SharedPermissions = Arc<std::sync::RwLock<PermissionIndex>>;

impl crate::Runtime {
    /// The permission bits of `session`'s tenant database, loading them on
    /// first use. Errors when the module declares no `permissionGrants`.
    pub async fn permission_index(
        &self,
        session: &gonvex_store::TenantSession,
    ) -> Result<SharedPermissions, String> {
        let module = self
            .inner
            .modules
            .project(&session.identity.project_id)
            .await
            .ok_or_else(|| "module is not installed".to_owned())?;
        let source = module
            .permission_grants
            .clone()
            .ok_or_else(|| "the module declares no permissionGrants projection".to_owned())?;
        let control = self
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| "control plane is not ready".to_owned())?;
        self.inner
            .permissions
            .ensure(&self.inner.change_feeds, &control, &session.route, &source)
            .await
    }
}

/// One index per tenant database, each with a single applier task.
#[derive(Default)]
pub struct PermissionRegistry {
    indexes: std::sync::Mutex<BTreeMap<String, (PermissionGrantSource, SharedPermissions)>>,
    loading: std::sync::Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl PermissionRegistry {
    fn current(&self, key: &str, source: &PermissionGrantSource) -> Option<SharedPermissions> {
        let indexes = self.indexes.lock().unwrap_or_else(|e| e.into_inner());
        let (shape, index) = indexes.get(key)?;
        (shape == source && index.read().is_ok_and(|index| !index.is_poisoned()))
            .then(|| index.clone())
    }

    /// The tenant's index, loading it (once, single flight) if missing,
    /// poisoned or declared differently.
    pub async fn ensure(
        &self,
        feeds: &crate::change_feed::ChangeFeedHub,
        control: &gonvex_store::ControlPlane,
        route: &gonvex_store::TenantRoute,
        source: &PermissionGrantSource,
    ) -> Result<SharedPermissions, String> {
        let key = route.database_url.clone();
        if let Some(index) = self.current(&key, source) {
            return Ok(index);
        }
        let gate = self
            .loading
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key.clone())
            .or_default()
            .clone();
        let _loading = gate.lock().await;
        if let Some(index) = self.current(&key, source) {
            return Ok(index);
        }
        // Subscribe before the load snapshot: every later commit reaches the
        // applier, and `apply` skips those the snapshot already contains.
        let receiver = feeds.subscribe(route).await;
        let index = Arc::new(std::sync::RwLock::new(
            load(control, route, source).await?,
        ));
        let previous = self
            .indexes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, (source.clone(), index.clone()));
        if let Some((_, previous)) = previous {
            previous
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .poison();
        }
        tokio::spawn(run_applier(receiver, index.clone(), source.clone()));
        Ok(index)
    }
}

async fn load(
    control: &gonvex_store::ControlPlane,
    route: &gonvex_store::TenantRoute,
    source: &PermissionGrantSource,
) -> Result<PermissionIndex, String> {
    source.validate()?;
    let mut transaction = control
        .begin_tenant_transaction(route, true)
        .await
        .map_err(|error| error.to_string())?;
    let clock = sqlx::query("SELECT epoch, revision FROM _gonvex_sync_clock WHERE singleton = 1")
        .fetch_one(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?;
    let epoch: String = clock.try_get("epoch").map_err(|error| error.to_string())?;
    let revision = clock
        .try_get::<i64, _>("revision")
        .map_err(|error| error.to_string())?
        .max(0) as u64;
    let quote = |name: &str| format!("\"{name}\"");
    let mut columns = vec![
        source.member.clone(),
        source.permission.clone(),
        source.scope.clone(),
    ];
    columns.extend(source.where_null.iter().cloned());
    let projection = columns
        .iter()
        .map(|column| format!("'{column}', r.{}", quote(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let statement = format!(
        "SELECT CAST(r.{} AS TEXT), json_object({projection}) FROM {} AS r",
        quote(source.key_column()),
        quote(&source.table)
    );
    let rows = sqlx::query(&statement)
        .fetch_all(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .commit()
        .await
        .map_err(|error| error.to_string())?;
    let mut loaded = Vec::with_capacity(rows.len());
    for row in rows {
        let id: Option<String> = row.try_get(0).map_err(|error| error.to_string())?;
        let value: String = row.try_get(1).map_err(|error| error.to_string())?;
        if let Some(id) = id {
            loaded.push((
                id,
                serde_json::from_str(&value).map_err(|error| error.to_string())?,
            ));
        }
    }
    Ok(PermissionIndex::from_rows(source, epoch, revision, loaded))
}

struct PoisonOnExit(SharedPermissions);

impl Drop for PoisonOnExit {
    fn drop(&mut self) {
        self.0.write().unwrap_or_else(|e| e.into_inner()).poison();
    }
}

/// The only writer of one index: committed feed transactions, in order.
async fn run_applier(
    mut receiver: tokio::sync::broadcast::Receiver<crate::change_feed::FeedEvent>,
    index: SharedPermissions,
    source: PermissionGrantSource,
) {
    let _poison = PoisonOnExit(index.clone());
    let mut signal = index
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .signal
        .subscribe();
    loop {
        let event = tokio::select! {
            biased;
            _ = signal.wait_for(|(_, poisoned)| *poisoned) => return,
            event = receiver.recv() => event,
        };
        let Ok(crate::change_feed::FeedEvent::Transaction {
            database_epoch,
            revision,
            changes,
        }) = event
        else {
            return;
        };
        let mut guard = index.write().unwrap_or_else(|e| e.into_inner());
        if guard.epoch != database_epoch {
            return;
        }
        guard.apply(&source, revision, &changes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source() -> PermissionGrantSource {
        serde_json::from_value(json!({"table": "grants", "member": "memberId",
            "permission": "permission", "scope": "scope", "whereNull": ["deletedAt"]}))
        .unwrap()
    }

    fn change(id: &str, old: Value, new: Value) -> crate::change_feed::LogChange {
        crate::change_feed::LogChange {
            revision: 0,
            ordinal: 0,
            origin_command_id: String::new(),
            table: "grants".to_owned(),
            row_id: id.to_owned(),
            operation: "update".to_owned(),
            changed_columns: Vec::new(),
            old_value: old,
            new_value: new,
            provenance: Default::default(),
        }
    }

    #[test]
    fn grants_are_bits_with_reference_counts_and_soft_deletes() {
        let source = source();
        let grant = |member: &str, permission: &str, scope: Value| {
            json!({"memberId": member, "permission": permission, "scope": scope, "deletedAt": null})
        };
        let mut index = PermissionIndex::from_rows(
            &source,
            "e".into(),
            1,
            [
                ("g1".to_owned(), grant("m1", "edit-tasks", json!("w1"))),
                ("g2".to_owned(), grant("m1", "edit-tasks", json!("w1"))),
                ("g3".to_owned(), grant("m2", "view", Value::Null)),
                (
                    "g4".to_owned(),
                    json!({"memberId": "m2", "permission": "edit-tasks", "scope": "w1", "deletedAt": 5}),
                ),
            ],
        );
        assert!(index.has("m1", "edit-tasks", "w1"));
        assert!(!index.has("m1", "edit-tasks", "w2"));
        assert!(!index.has("m2", "edit-tasks", "w1"), "soft-deleted rows grant nothing");
        assert!(index.has("m2", "view", ""), "null scope is tenant-wide");
        assert_eq!(index.size(), (2, 2));
        // One of two duplicate rows deleted: the bit stays.
        index.apply(&source, 2, &[change("g1", grant("m1", "edit-tasks", json!("w1")), Value::Null)]);
        assert!(index.has("m1", "edit-tasks", "w1"));
        // An update moving the grant to another scope.
        index.apply(
            &source,
            3,
            &[change("g2", grant("m1", "edit-tasks", json!("w1")), grant("m1", "edit-tasks", json!("w2")))],
        );
        assert!(!index.has("m1", "edit-tasks", "w1"));
        assert!(index.has("m1", "edit-tasks", "w2"));
        // Revisions at or below the loaded one are already in the snapshot.
        index.apply(&source, 3, &[change("g9", Value::Null, grant("m3", "x", json!("y")))]);
        assert!(!index.has("m3", "x", "y"));
        // Soft delete then restore.
        let deleted = json!({"memberId": "m2", "permission": "view", "scope": null, "deletedAt": 9});
        index.apply(&source, 4, &[change("g3", grant("m2", "view", Value::Null), deleted.clone())]);
        assert!(!index.has("m2", "view", ""));
        index.apply(&source, 5, &[change("g3", deleted, grant("m2", "view", Value::Null))]);
        assert!(index.has("m2", "view", ""));
        assert_eq!(index.revision(), 5);
    }
}

/// Parity of the bits with SQL over the projection across the fixture's
/// members, through real commits, soft deletes, scope moves and a rollback.
#[cfg(all(test, feature = "perf-fixture"))]
mod fixture_tests {
    use super::*;
    use crate::perf_fixture::{Fixture, FixtureOptions, SqlValue};
    use std::collections::BTreeSet;

    async fn sql_grants(fixture: &Fixture) -> BTreeSet<(String, String, String)> {
        let route = &fixture.tenant(0).route;
        let mut transaction =
            gonvex_store::begin_transaction(fixture.pools(), &route.database_url, true)
                .await
                .unwrap();
        let rows = sqlx::query(
            "SELECT DISTINCT memberId, roleKey, COALESCE(teamId, '') FROM memberTeams WHERE deletedAt IS NULL AND memberId IS NOT NULL AND roleKey IS NOT NULL",
        )
        .fetch_all(&mut **transaction.transaction())
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        rows.into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect()
    }

    /// Team roles as (role, team) bits: `memberTeams` is a captured
    /// permission table of the fixture, so its commits reach the feed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn permission_bits_match_sql_through_commits_and_never_see_rollbacks() {
        let fixture = Fixture::build(FixtureOptions::small()).await.unwrap();
        let tenant = fixture.tenant(0);
        let source: PermissionGrantSource = serde_json::from_value(serde_json::json!({
            "table": "memberTeams", "member": "memberId",
            "permission": "roleKey", "scope": "teamId", "whereNull": ["deletedAt"]
        }))
        .unwrap();
        let control = fixture
            .runtime
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .unwrap();
        let registry = PermissionRegistry::default();
        let ensure = || {
            registry.ensure(
                &fixture.runtime.inner.change_feeds,
                &control,
                &tenant.route,
                &source,
            )
        };
        let index = ensure().await.unwrap();
        let members: Vec<String> = tenant.members.iter().map(|m| m.id.clone()).collect();
        let teams: BTreeSet<String> = tenant
            .members
            .iter()
            .flat_map(|m| m.teams.iter().cloned())
            .chain([String::new(), "nowhere".to_owned()])
            .collect();
        let initial = sql_grants(&fixture).await;
        assert!(!initial.is_empty(), "fixture has team memberships");
        let roles: BTreeSet<String> = initial
            .iter()
            .map(|(_, role, _)| role.clone())
            .chain(["manager".to_owned(), "unknown".to_owned()])
            .collect();
        let check = |label: &str, expected: &BTreeSet<(String, String, String)>| {
            let guard = index.read().unwrap();
            let mut checked = 0;
            for member in &members {
                for role in &roles {
                    for team in &teams {
                        let want = expected.contains(&(member.clone(), role.clone(), team.clone()));
                        assert_eq!(guard.has(member, role, team), want, "{label}: {member} {role}@{team}");
                        checked += 1;
                    }
                }
            }
            checked
        };
        let mut checked = check("loaded", &initial);
        let first = tenant.members[0].id.clone();
        let second = tenant.members[1].id.clone();
        let fresh_team = teams
            .iter()
            .find(|team| !team.is_empty() && *team != "nowhere" && !tenant.members[0].teams.contains(team))
            .cloned()
            .unwrap_or_else(|| "nowhere".to_owned());
        let steps: Vec<(&str, Vec<SqlValue>)> = vec![
            ("INSERT INTO memberTeams (_id, memberId, teamId, roleKey) VALUES ('perm-1', $1, $2, 'manager')", vec![first.as_str().into(), fresh_team.as_str().into()]),
            ("UPDATE memberTeams SET deletedAt = 1 WHERE _id = 'perm-1'", vec![]),
            ("UPDATE memberTeams SET deletedAt = NULL, roleKey = 'lead' WHERE _id = 'perm-1'", vec![]),
            ("UPDATE memberTeams SET teamId = 'nowhere' WHERE _id = 'perm-1'", vec![]),
            ("UPDATE memberTeams SET memberId = $1 WHERE _id = 'perm-1'", vec![second.as_str().into()]),
            ("UPDATE memberTeams SET deletedAt = 7 WHERE memberId = $1 AND _id <> 'perm-1'", vec![second.as_str().into()]),
            ("DELETE FROM memberTeams WHERE _id = 'perm-1'", vec![]),
        ];
        for (number, (statement, binds)) in steps.into_iter().enumerate() {
            fixture.commit(0, statement, binds).await.unwrap();
            let revision = fixture.revision(0).await.unwrap();
            assert!(PermissionIndex::wait_for(&index, revision).await, "applier reached {revision}");
            let expected = sql_grants(&fixture).await;
            checked += check(&format!("step {number}"), &expected);
        }
        assert!(index.read().unwrap().position("lead", "nowhere").is_some(), "steps reached the index");
        // A rolled-back grant never becomes a bit.
        let before = sql_grants(&fixture).await;
        {
            let mut transaction =
                gonvex_store::begin_transaction(fixture.pools(), &tenant.route.database_url, false)
                    .await
                    .unwrap();
            sqlx::query("INSERT INTO memberTeams (_id, memberId, teamId, roleKey) VALUES ('phantom', $1, 'phantom-team', 'manager')")
                .bind(&first)
                .execute(&mut **transaction.transaction())
                .await
                .unwrap();
            assert!(!index.read().unwrap().has(&first, "manager", "phantom-team"));
            transaction.rollback().await.unwrap();
        }
        fixture
            .commit(0, "UPDATE memberTeams SET updatedAt = 1 WHERE memberId = $1", vec![first.as_str().into()])
            .await
            .unwrap();
        let revision = fixture.revision(0).await.unwrap();
        assert!(PermissionIndex::wait_for(&index, revision).await);
        assert!(!index.read().unwrap().has(&first, "manager", "phantom-team"));
        checked += check("after rollback", &before);
        assert!(checked > 500, "{checked}");
        assert!(Arc::ptr_eq(&index, &ensure().await.unwrap()), "a live index is reused");
        drop(index);
        fixture.shutdown().await;
    }
}
