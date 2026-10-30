//! Version 2 mask delivery for one Replica subscription.
//!
//! A committed transaction never causes visibility SQL on this connection
//! unless it changes this member's principal (a permission row naming the
//! member, or a grant removal naming the member), or it newly reveals rows
//! this connection has to fetch (a grant or parent that became visible to this
//! member). Every other row is decided in memory against the cached principal.

use super::*;
use crate::visibility::{grant_contribution, GrantContribution, VisibilityExpression};

pub(super) enum MaskStep {
    /// Nothing in the transaction concerns this connection.
    Skip,
    /// Deltas were applied in memory (plus targeted fetches for this member).
    Applied,
    /// This member's principal changed, or the window cannot be maintained
    /// incrementally: re-snapshot this connection only.
    Rebuild,
}

enum GrantDelta {
    None,
    Added(Vec<(String, String)>),
    Rebuild,
}

/// Grant rows decided in memory. Additions patch the set; a removal may still
/// be covered by another row of the member, so it re-resolves this member.
fn grant_delta(plan: &VisibilityPlan, resolved: &ResolvedVisibility, change: &LogChange) -> GrantDelta {
    let mut added = Vec::new();
    for (name, set) in plan.active_grant_sets(resolved) {
        if set.table != change.table {
            continue;
        }
        let old = grant_contribution(name, set, resolved, &change.old_value);
        let new = grant_contribution(name, set, resolved, &change.new_value);
        match (&old, &new) {
            (GrantContribution::Unknown, _) | (_, GrantContribution::Unknown) => {
                return GrantDelta::Rebuild
            }
            _ if old == new => {}
            (GrantContribution::Token(_), _) => return GrantDelta::Rebuild,
            (_, GrantContribution::Token(token)) => added.push((name.clone(), token.clone())),
            _ => {}
        }
    }
    if added.is_empty() {
        GrantDelta::None
    } else {
        GrantDelta::Added(added)
    }
}

/// Columns compared to a set by `inSet`, so newly granted tokens can be fetched.
fn set_columns(expression: &VisibilityExpression, set: &str, output: &mut BTreeSet<String>) {
    if expression.operator == "inSet" && expression.set == set {
        output.insert(expression.column.clone());
    }
    for child in &expression.children {
        set_columns(child, set, output);
    }
    if let Some(fallback) = &expression.on_missing {
        set_columns(fallback, set, output);
    }
}

impl Runtime {
    pub(super) async fn apply_mask_feed(
        &self,
        session: &TenantSession,
        subscription: &mut ReplicaSubscription,
        revision: u64,
        changes: &[LogChange],
        entity_changes: &mut BTreeMap<(String, String), ReplicaChange>,
        messages: &mut Vec<ServerMessage>,
    ) -> Result<MaskStep, ReplicaError> {
        let metrics = &self.inner.visibility_metrics;
        let parents = subscription.visibility.inherited_columns();
        let mut source = Vec::new();
        let mut parent_changes = Vec::new();
        // (scope table, set name, token): own plan or a directly inherited parent.
        let mut granted: Vec<(String, String, String)> = Vec::new();
        for change in changes {
            metrics.record_in_memory_row_check();
            if subscription.visibility.principal_change_concerns(
                &subscription.resolved,
                &change.table,
                &change.operation,
                &change.changed_columns,
                &change.old_value,
                &change.new_value,
            ) {
                return Ok(MaskStep::Rebuild);
            }
            if change.table == subscription.definition.table {
                source.push(change);
            }
            if parents.contains_key(&change.table) {
                parent_changes.push(change);
            }
            match grant_delta(&subscription.visibility, &subscription.resolved, change) {
                GrantDelta::Rebuild => return Ok(MaskStep::Rebuild),
                GrantDelta::Added(tokens) => granted.extend(
                    tokens
                        .into_iter()
                        .map(|(name, token)| (subscription.visibility.table.clone(), name, token)),
                ),
                GrantDelta::None => {}
            }
            for (table, parent) in &subscription.resolved.inherited {
                match grant_delta(&parent.plan, &parent.resolved, change) {
                    GrantDelta::Rebuild => return Ok(MaskStep::Rebuild),
                    GrantDelta::Added(tokens) => granted.extend(
                        tokens.into_iter().map(|(name, token)| (table.clone(), name, token)),
                    ),
                    GrantDelta::None => {}
                }
            }
        }
        if source.is_empty() && parent_changes.is_empty() && granted.is_empty() {
            return Ok(MaskStep::Skip);
        }

        // Rows the member may newly see, to fetch for this connection only:
        // (child column, values). Own grants select rows by the set's column;
        // parent grants and parent flips select children by foreign key.
        let mut fetch: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        // Parents whose visibility to this member flipped to hidden.
        let mut hidden_parents: Vec<(String, String)> = Vec::new();
        for (scope, name, token) in &granted {
            if *scope == subscription.visibility.table {
                subscription
                    .resolved
                    .sets
                    .entry(name.clone())
                    .or_default()
                    .insert(token.clone());
                let mut columns = BTreeSet::new();
                set_columns(&subscription.visibility.predicate, name, &mut columns);
                for column in columns {
                    fetch.entry(column).or_default().insert(token.clone());
                }
            } else if let Some(parent) = subscription.resolved.inherited.get_mut(scope) {
                std::sync::Arc::make_mut(&mut parent.resolved)
                    .sets
                    .entry(name.clone())
                    .or_default()
                    .insert(token.clone());
                let mut columns = BTreeSet::new();
                set_columns(&parent.plan.predicate, name, &mut columns);
                if columns.iter().any(|column| *column != parent.plan.key) {
                    // The token does not name parent keys; resolve this member.
                    return Ok(MaskStep::Rebuild);
                }
                for column in parents.get(scope).into_iter().flatten() {
                    fetch.entry(column.clone()).or_default().insert(token.clone());
                }
            }
        }
        for change in &parent_changes {
            let Some(parent) = subscription.resolved.inherited.get(&change.table) else {
                continue;
            };
            let visible = |row: &Value| {
                !row.is_null() && visibility::row_matches(&parent.plan, &parent.resolved, row)
            };
            let (was, now) = (visible(&change.old_value), visible(&change.new_value));
            if was == now {
                continue;
            }
            for column in parents.get(&change.table).into_iter().flatten() {
                if now && !change.old_value.is_null() {
                    // Hidden → visible on an update: its children may already
                    // exist. A new parent has none yet beyond this transaction.
                    fetch.entry(column.clone()).or_default().insert(change.row_id.clone());
                } else if was {
                    hidden_parents.push((column.clone(), change.row_id.clone()));
                }
            }
        }

        let mut upserts = BTreeMap::new();
        let mut deleted = BTreeSet::new();
        let mut inserted = false;
        let projection_covers_acl = visibility::acl_columns(&subscription.visibility)
            .iter()
            .all(|column| subscription.definition.columns.contains(column));
        for (column, key) in &hidden_parents {
            if !projection_covers_acl {
                // Another grant might still admit a child; ask the database
                // for this member's remaining children of that parent.
                fetch.entry(column.clone()).or_default().insert(key.clone());
                continue;
            }
            let children = subscription
                .rows
                .iter()
                .filter(|(_, row)| row.get(column).map(|value| crate::visibility_mask::scalar(Some(value))) == Some(key.clone()))
                .map(|(id, row)| (id.clone(), row.clone()))
                .collect::<Vec<_>>();
            for (id, row) in children {
                metrics.record_in_memory_row_check();
                if !row_in_collection(
                    &row,
                    &subscription.args,
                    &subscription.definition,
                    &subscription.visibility,
                    &subscription.resolved,
                ) {
                    self.mask_apply_row(subscription, &id, None, &[], entity_changes, &mut upserts, &mut deleted)?;
                }
            }
        }
        for change in source {
            metrics.record_in_memory_row_check();
            let now = row_in_collection(
                &change.new_value,
                &subscription.args,
                &subscription.definition,
                &subscription.visibility,
                &subscription.resolved,
            );
            if !now && !subscription.rows.contains_key(&change.row_id) {
                continue;
            }
            let projection = now
                .then(|| project_row(&change.new_value, &subscription.definition))
                .transpose()?;
            inserted |= now && !subscription.rows.contains_key(&change.row_id);
            self.mask_apply_row(
                subscription,
                &change.row_id,
                projection,
                &change.changed_columns,
                entity_changes,
                &mut upserts,
                &mut deleted,
            )?;
        }
        if !fetch.is_empty() {
            let control = self
                .inner
                .control_plane
                .read()
                .await
                .clone()
                .ok_or(ReplicaError::ModuleMissing)?;
            let mut transaction = control.begin_tenant_transaction(&session.route, true).await?;
            for (column, values) in &fetch {
                metrics.record_visibility_sql_statement();
                let rows = query_rows_where(
                    &mut transaction,
                    &subscription.definition,
                    &subscription.visibility,
                    &subscription.resolved,
                    &subscription.args,
                    Some((column.as_str(), values)),
                )
                .await?;
                let mut present = BTreeSet::new();
                for row in rows {
                    let key = row_key(&row, &subscription.definition.key)?;
                    present.insert(key.clone());
                    inserted |= !subscription.rows.contains_key(&key);
                    self.mask_apply_row(subscription, &key, Some(row), &[], entity_changes, &mut upserts, &mut deleted)?;
                }
                // A re-check of hidden parents: cached children it no longer returns go.
                let stale = subscription
                    .rows
                    .iter()
                    .filter(|(id, row)| {
                        !present.contains(*id)
                            && row
                                .get(column)
                                .is_some_and(|value| values.contains(&crate::visibility_mask::scalar(Some(value))))
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                for id in stale {
                    self.mask_apply_row(subscription, &id, None, &[], entity_changes, &mut upserts, &mut deleted)?;
                }
            }
            transaction.commit().await?;
        }
        if inserted && !self.mask_enforce_budget(subscription, entity_changes, &mut upserts, &mut deleted)? {
            return Ok(MaskStep::Rebuild);
        }
        subscription.cursor.revision = revision;
        if !upserts.is_empty() || !deleted.is_empty() {
            messages.push(replica_delta(
                &subscription.id,
                &subscription.path,
                subscription.cursor.clone(),
                upserts.into_values().collect(),
                deleted.into_iter().collect(),
                subscription_digest(subscription),
                subscription.truncated,
            ));
        }
        Ok(MaskStep::Applied)
    }

    /// Insert, update or remove one cached row and record its entity change.
    #[allow(clippy::too_many_arguments)]
    fn mask_apply_row(
        &self,
        subscription: &mut ReplicaSubscription,
        id: &str,
        projection: Option<Value>,
        changed_columns: &[String],
        entity_changes: &mut BTreeMap<(String, String), ReplicaChange>,
        upserts: &mut BTreeMap<String, Value>,
        deleted: &mut BTreeSet<String>,
    ) -> Result<(), ReplicaError> {
        let old = subscription.rows.get(id).cloned();
        if old.is_none() && projection.is_none() {
            return Ok(());
        }
        if old.is_some() && old == projection {
            return Ok(());
        }
        let new_hash = projection.as_ref().map(row_hash);
        if let Some(aggregate) = &mut subscription.aggregate {
            if let Some(previous) = subscription.hashes.get(id) {
                aggregate.change(previous, true);
            }
            if let Some(hash) = &new_hash {
                aggregate.change(hash, false);
            }
        }
        match (&projection, new_hash) {
            (Some(row), Some(hash)) => {
                subscription.rows.insert(id.to_owned(), row.clone());
                subscription.hashes.insert(id.to_owned(), hash);
            }
            _ => {
                subscription.rows.remove(id);
                subscription.hashes.remove(id);
            }
        }
        accumulate_projection_delta(upserts, deleted, id, projection.as_ref());
        let operation = match (&old, &projection) {
            (Some(_), Some(_)) => "update",
            (None, Some(_)) => "insert",
            _ => "delete",
        };
        let table = subscription.definition.table.clone();
        entity_changes.insert(
            (table.clone(), id.to_owned()),
            ReplicaChange {
                entity: table,
                id: id.to_owned(),
                operation: operation.to_owned(),
                old_value: old,
                new_value: projection,
                changed_columns: changed_columns.to_vec(),
            },
        );
        Ok(())
    }

    /// Keep a budgeted collection within its budgets without SQL. Ordered
    /// windows keep their first rows and become truncated; a window without an
    /// order cannot choose which rows to keep, so it re-snapshots (false).
    fn mask_enforce_budget(
        &self,
        subscription: &mut ReplicaSubscription,
        entity_changes: &mut BTreeMap<(String, String), ReplicaChange>,
        upserts: &mut BTreeMap<String, Value>,
        deleted: &mut BTreeSet<String>,
    ) -> Result<bool, ReplicaError> {
        let definition = &subscription.definition;
        let over_rows = definition.max_rows > 0 && subscription.rows.len() > definition.max_rows;
        let bytes = |rows: &BTreeMap<String, Value>| {
            rows.values()
                .map(|row| serde_json::to_vec(row).unwrap_or_default().len() as i64)
                .sum::<i64>()
        };
        let over_bytes = definition.max_bytes > 0 && bytes(&subscription.rows) > definition.max_bytes;
        if !over_rows && !over_bytes {
            return Ok(true);
        }
        if definition.order_by.is_empty() {
            return Ok(false);
        }
        let order_by = definition.order_by.clone();
        let descending = definition.order_direction != "asc";
        let mut ordered = subscription
            .rows
            .iter()
            .map(|(id, row)| (id.clone(), row.get(&order_by).cloned().unwrap_or(Value::Null)))
            .collect::<Vec<_>>();
        ordered.sort_by(|(left_id, left), (right_id, right)| {
            let order = compare_json(left, right).then_with(|| left_id.cmp(right_id));
            if descending {
                order.reverse()
            } else {
                order
            }
        });
        let max_rows = if definition.max_rows > 0 { definition.max_rows } else { usize::MAX };
        let max_bytes = definition.max_bytes;
        let mut kept_bytes = 0i64;
        let mut evict = Vec::new();
        for (index, (id, _)) in ordered.iter().enumerate() {
            let size = serde_json::to_vec(&subscription.rows[id]).unwrap_or_default().len() as i64;
            if index >= max_rows || (max_bytes > 0 && kept_bytes + size > max_bytes) {
                evict.push(id.clone());
            } else {
                kept_bytes += size;
            }
        }
        for id in evict {
            self.mask_apply_row(subscription, &id, None, &[], entity_changes, upserts, deleted)?;
        }
        subscription.truncated = true;
        Ok(true)
    }
}

/// PostgreSQL-like ordering for JSON scalars: nulls last, numbers numerically,
/// strings lexically; mixed kinds by kind.
fn compare_json(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        _ => left.to_string().cmp(&right.to_string()),
    }
}
