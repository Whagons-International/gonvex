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

/// Owned once per tenant and parent table. Connections borrow Arc projections.
/// Admission must initialize this index at a consistent snapshot revision before
/// registering it for feed delivery.
#[derive(Default)]
pub struct ParentAclIndex {
    revision: u64,
    rows: BTreeMap<String, Arc<Value>>,
}

impl ParentAclIndex {
    pub fn from_snapshot(revision: u64, rows: BTreeMap<String, Value>) -> Self {
        Self {
            revision,
            rows: rows
                .into_iter()
                .map(|(key, row)| (key, Arc::new(row)))
                .collect(),
        }
    }

    /// Apply one complete transaction, once, even if many connections consume it.
    /// None removes a deleted parent and immediately removes inherited access.
    pub fn apply(
        &mut self,
        revision: u64,
        changes: impl IntoIterator<Item = (String, Option<Value>)>,
    ) -> bool {
        if revision <= self.revision {
            return false;
        }
        for (key, row) in changes {
            match row {
                Some(row) => {
                    self.rows.insert(key, Arc::new(row));
                }
                None => {
                    self.rows.remove(&key);
                }
            }
        }
        self.revision = revision;
        true
    }

    pub fn get(&self, key: &str) -> Option<Arc<Value>> {
        self.rows.get(key).cloned()
    }
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
}
