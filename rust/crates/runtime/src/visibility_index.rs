//! Tenant-owned indexes for mask grants and inherited parent ACL values.
//! These structures apply feed rows without database access.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;

/// A contribution belongs to a grant row, not to a connection. Multiple rows
/// may grant the same token; revoking one must preserve the others.
#[derive(Default)]
pub struct GrantIndex {
    rows: BTreeMap<String, BTreeSet<(String, String)>>,
    counts: BTreeMap<String, BTreeMap<String, usize>>,
}

impl GrantIndex {
    /// Replace a row's complete contributions. The returned members are exactly
    /// those whose effective token set changed, including old recipients.
    pub fn replace(
        &mut self,
        row_id: String,
        contributions: BTreeSet<(String, String)>,
    ) -> BTreeSet<String> {
        let previous = self.rows.remove(&row_id).unwrap_or_default();
        let mut affected = BTreeSet::new();
        for (member, token) in previous.difference(&contributions) {
            if let Some(tokens) = self.counts.get_mut(member) {
                if let Some(count) = tokens.get_mut(token) {
                    *count -= 1;
                    if *count == 0 {
                        tokens.remove(token);
                        affected.insert(member.clone());
                    }
                }
                if tokens.is_empty() {
                    self.counts.remove(member);
                }
            }
        }
        for (member, token) in contributions.difference(&previous) {
            let count = self
                .counts
                .entry(member.clone())
                .or_default()
                .entry(token.clone())
                .or_default();
            if *count == 0 {
                affected.insert(member.clone());
            }
            *count += 1;
        }
        if !contributions.is_empty() {
            self.rows.insert(row_id, contributions);
        }
        affected
    }

    pub fn contains(&self, member: &str, token: &str) -> bool {
        self.counts
            .get(member)
            .is_some_and(|tokens| tokens.contains_key(token))
    }
}

/// The applier's published progress. Readers wait on it instead of polling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexSignal {
    pub revision: u64,
    pub poisoned: bool,
}

/// Owned once per tenant, table and key column. Connections borrow Arc rows.
/// A key may name several rows (for example association rows by task), so rows
/// are grouped by key and remembered by their own row id for removal.
///
/// A maintained index is loaded at a REPEATABLE READ snapshot taken after its
/// single applier subscribed to the tenant feed, and only that applier mutates
/// it. Its revision is therefore a completeness certificate: every committed
/// transaction up to `revision` is reflected. A poisoned index (feed reset,
/// lag, epoch change, applier exit, or a replaced projection) certifies
/// nothing; every holder must rebuild through the registry.
#[derive(Debug)]
pub struct ParentAclIndex {
    revision: u64,
    modified_revision: u64,
    rows: BTreeMap<String, BTreeMap<String, Arc<Value>>>,
    keys: BTreeMap<String, String>,
    epoch: String,
    maintained: bool,
    poisoned: bool,
    signal: tokio::sync::watch::Sender<IndexSignal>,
}

impl Default for ParentAclIndex {
    fn default() -> Self {
        Self {
            revision: 0,
            modified_revision: 0,
            rows: BTreeMap::new(),
            keys: BTreeMap::new(),
            epoch: String::new(),
            maintained: false,
            poisoned: false,
            signal: tokio::sync::watch::channel(IndexSignal::default()).0,
        }
    }
}

impl ParentAclIndex {
    /// A unique-key index whose key is the row id.
    pub fn from_snapshot(revision: u64, rows: BTreeMap<String, Value>) -> Self {
        Self::from_rows(
            revision,
            rows.into_iter().map(|(key, row)| (key.clone(), key, row)),
        )
    }

    /// Rows as (row id, key, projected row).
    pub fn from_rows(
        revision: u64,
        rows: impl IntoIterator<Item = (String, String, Value)>,
    ) -> Self {
        let mut index = Self {
            revision,
            modified_revision: revision,
            ..Self::default()
        };
        for (id, key, row) in rows {
            index.insert(id, key, row);
        }
        index.publish();
        index
    }

    /// An index owned by one feed applier, loaded at `revision` of `epoch`.
    pub fn maintained(
        epoch: String,
        revision: u64,
        rows: impl IntoIterator<Item = (String, String, Value)>,
    ) -> Self {
        let mut index = Self::from_rows(revision, rows);
        index.epoch = epoch;
        index.maintained = true;
        index
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Last change to projected ACL rows, independent of unrelated commits.
    pub fn modified_revision(&self) -> u64 {
        self.modified_revision
    }

    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Permanently invalidate this index and release its rows. Holders observe
    /// the poison through `ready_at` and rebuild.
    pub fn poison(&mut self) {
        self.poisoned = true;
        self.rows.clear();
        self.keys.clear();
        self.publish();
    }

    fn publish(&self) {
        self.signal.send_replace(IndexSignal {
            revision: self.revision,
            poisoned: self.poisoned,
        });
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<IndexSignal> {
        self.signal.subscribe()
    }

    fn insert(&mut self, id: String, key: String, row: Value) {
        self.remove(&id);
        self.rows
            .entry(key.clone())
            .or_default()
            .insert(id.clone(), Arc::new(row));
        self.keys.insert(id, key);
    }

    fn remove(&mut self, id: &str) {
        if let Some(key) = self.keys.remove(id) {
            if let Some(bucket) = self.rows.get_mut(&key) {
                bucket.remove(id);
                if bucket.is_empty() {
                    self.rows.remove(&key);
                }
            }
        }
    }

    /// Apply one complete transaction to a unique-key index, once, even if many
    /// connections consume it. None removes a deleted parent and immediately
    /// removes inherited access.
    pub fn apply(
        &mut self,
        revision: u64,
        changes: impl IntoIterator<Item = (String, Option<Value>)>,
    ) -> bool {
        self.apply_rows(
            revision,
            changes
                .into_iter()
                .map(|(key, row)| (key.clone(), row.map(|row| (key, row)))),
        )
    }

    /// Apply one complete transaction as (row id, new (key, row) or None).
    pub fn apply_rows(
        &mut self,
        revision: u64,
        changes: impl IntoIterator<Item = (String, Option<(String, Value)>)>,
    ) -> bool {
        if self.poisoned || revision <= self.revision {
            return false;
        }
        for (id, row) in changes {
            match row {
                Some((key, row)) => {
                    let unchanged = self.keys.get(&id) == Some(&key)
                        && self
                            .rows
                            .get(&key)
                            .and_then(|rows| rows.get(&id))
                            .is_some_and(|current| **current == row);
                    if !unchanged {
                        self.insert(id, key, row);
                        self.modified_revision = revision;
                    }
                }
                None if self.keys.contains_key(&id) => {
                    self.remove(&id);
                    self.modified_revision = revision;
                }
                None => {}
            }
        }
        self.revision = revision;
        self.publish();
        true
    }

    /// One row for a unique key (a parent), or None when it does not exist.
    pub fn get(&self, key: &str) -> Option<Arc<Value>> {
        self.rows.get(key)?.values().next().cloned()
    }

    /// Every row with this key.
    pub fn get_all(&self, key: &str) -> Vec<Arc<Value>> {
        self.rows
            .get(key)
            .map(|bucket| bucket.values().cloned().collect())
            .unwrap_or_default()
    }
}

/// Whether `index` reflects every committed transaction through `revision` of
/// `epoch`, waiting for its applier when it is behind.
///
/// The wait has no timer on purpose. A connection only evaluates a feed event
/// it received from the tenant broadcast, and the applier subscribed to that
/// same broadcast before loading, so the event is already queued for the
/// applier, whose per-event work is in-memory only. The applier either applies
/// it or poisons the index (lag, reset, epoch change, exit; a drop guard covers
/// panics and cancellation), and both wake this wait. An index that no applier
/// maintains, or one at another epoch, is never waited on: it is not ready.
pub async fn ready_at(
    index: &std::sync::RwLock<ParentAclIndex>,
    revision: u64,
    epoch: &str,
) -> bool {
    let mut signal = {
        let Ok(index) = index.read() else {
            return false;
        };
        if index.poisoned || (!index.epoch.is_empty() && index.epoch != epoch) {
            return false;
        }
        if index.revision >= revision {
            return true;
        }
        if !index.maintained {
            return false;
        }
        index.subscribe()
    };
    signal
        .wait_for(|state| state.poisoned || state.revision >= revision)
        .await
        .map(|state| !state.poisoned)
        .unwrap_or(false)
}

/// The revision `index` is complete through, or None when it is poisoned or
/// at another epoch. Never waits.
pub(crate) fn usable_revision(
    index: &std::sync::RwLock<ParentAclIndex>,
    epoch: &str,
) -> Option<u64> {
    let index = index.read().ok()?;
    (!index.poisoned && (index.epoch.is_empty() || index.epoch == epoch)).then_some(index.revision)
}

/// Whether `index` is complete through exactly `revision` of `epoch` (no
/// later transaction applied yet). Never waits.
pub(crate) fn exactly_at(
    index: &std::sync::RwLock<ParentAclIndex>,
    revision: u64,
    epoch: &str,
) -> bool {
    index.read().is_ok_and(|index| {
        !index.poisoned
            && (index.epoch.is_empty() || index.epoch == epoch)
            && index.revision == revision
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(member: &str) -> BTreeSet<(String, String)> {
        BTreeSet::from([(member.to_owned(), "task-1".to_owned())])
    }

    #[test]
    fn duplicate_grants_revoke_only_after_last_contribution() {
        let mut index = GrantIndex::default();
        assert_eq!(
            index.replace("a".into(), grant("alice")),
            BTreeSet::from(["alice".into()])
        );
        assert!(index.replace("b".into(), grant("alice")).is_empty());
        assert!(index.replace("a".into(), BTreeSet::new()).is_empty());
        assert!(index.contains("alice", "task-1"));
        assert_eq!(
            index.replace("b".into(), BTreeSet::new()),
            BTreeSet::from(["alice".into()])
        );
        assert!(!index.contains("alice", "task-1"));
        assert!(index.replace("b".into(), BTreeSet::new()).is_empty());
    }

    #[test]
    fn reassignment_routes_to_old_and_new_members() {
        let mut index = GrantIndex::default();
        index.replace("a".into(), grant("alice"));
        assert_eq!(
            index.replace("a".into(), grant("bob")),
            BTreeSet::from(["alice".into(), "bob".into()])
        );
        assert!(!index.contains("alice", "task-1"));
        assert!(index.contains("bob", "task-1"));
        assert!(index.replace("a".into(), grant("bob")).is_empty());
    }

    #[test]
    fn non_unique_keys_group_rows_and_move_with_their_row() {
        let mut index = ParentAclIndex::from_rows(
            1,
            [
                (
                    "c1".into(),
                    "task-1".into(),
                    serde_json::json!({"workspaceId": "a"}),
                ),
                (
                    "c2".into(),
                    "task-1".into(),
                    serde_json::json!({"workspaceId": "b"}),
                ),
            ],
        );
        assert_eq!(index.get_all("task-1").len(), 2);
        assert!(index.apply_rows(
            2,
            [(
                "c2".into(),
                Some(("task-2".into(), serde_json::json!({"workspaceId": "b"})))
            )]
        ));
        assert_eq!(index.get_all("task-1").len(), 1);
        assert_eq!(index.get_all("task-2").len(), 1);
        assert!(index.apply_rows(3, [("c1".into(), None)]));
        assert!(index.get_all("task-1").is_empty());
        assert!(index.get("task-1").is_none());
    }

    #[test]
    fn parent_transactions_are_shared_and_deletion_removes_access() {
        let mut index = ParentAclIndex::from_snapshot(
            10,
            BTreeMap::from([("task-1".into(), serde_json::json!({"workspaceId":"a"}))]),
        );
        let first = index.get("task-1").unwrap();
        assert!(Arc::ptr_eq(&first, &index.get("task-1").unwrap()));
        assert!(index.apply(
            11,
            [(
                "task-1".into(),
                Some(serde_json::json!({"workspaceId":"b"}))
            )]
        ));
        assert!(!index.apply(11, [("task-1".into(), None)]));
        assert_eq!(index.get("task-1").unwrap()["workspaceId"], "b");
        assert!(index.apply(12, [("task-1".into(), None)]));
        assert!(index.get("task-1").is_none());
        assert!(!index.apply(10, [("task-1".into(), Some((*first).clone()))]));
        assert!(index.get("task-1").is_none());
    }

    #[test]
    fn completeness_advances_without_marking_unchanged_acl_rows_modified() {
        let row = serde_json::json!({"workspaceId":"a"});
        let mut index =
            ParentAclIndex::from_snapshot(10, BTreeMap::from([("task".into(), row.clone())]));
        index.apply_rows(11, []);
        assert_eq!(index.revision(), 11);
        assert_eq!(index.modified_revision(), 10);
        index.apply(12, [("task".into(), Some(row))]);
        assert_eq!(index.modified_revision(), 10);
        index.apply(
            13,
            [("task".into(), Some(serde_json::json!({"workspaceId":"b"})))],
        );
        assert_eq!(index.modified_revision(), 13);
        index.apply(14, [("missing".into(), None)]);
        assert_eq!(index.modified_revision(), 13);
        index.apply(15, [("task".into(), None)]);
        assert_eq!(index.modified_revision(), 15);
    }
}
