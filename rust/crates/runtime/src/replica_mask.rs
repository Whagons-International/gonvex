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
    /// The principal and data need a single current snapshot after feed lag.
    Refresh,
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
fn grant_delta(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    change: &LogChange,
) -> GrantDelta {
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

/// Child keys linked to one parent through an `inheritsAny` association.
fn children_of(
    resolved: &ResolvedVisibility,
    via: &visibility::VisibilityVia,
    parent: &str,
) -> Vec<String> {
    resolved
        .indexes
        .get(&visibility::via_index_name(via, &via.parent_column))
        .and_then(|index| index.read().ok().map(|index| index.get_all(parent)))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|link| {
            link.get(&via.child_column)
                .filter(|value| !value.is_null())
                .map(|value| crate::visibility_mask::scalar(Some(value)))
        })
        .collect()
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
        // (child key column, association, parent table) per inheritsAny.
        let any_parents = visibility::inherits_any(&subscription.visibility)
            .into_iter()
            .filter_map(|expression| {
                Some((
                    expression.column.clone(),
                    expression.via.clone()?,
                    expression.table.clone(),
                ))
            })
            .collect::<Vec<_>>();
        let mut any_changes = Vec::new();
        let mut source = Vec::new();
        let mut parent_changes = Vec::new();
        // (scope table, set name, token): own plan or a directly inherited parent.
        let mut granted: Vec<(String, String, String)> = Vec::new();
        let mut derived_touched = false;
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
            derived_touched |= visibility::derived_change_concerns(
                &subscription.visibility,
                &subscription.resolved,
                &change.table,
                &change.row_id,
                &change.old_value,
                &change.new_value,
            );
            if visibility::ancestor_derived_change_concerns(
                &subscription.resolved,
                &change.table,
                &change.row_id,
                &change.old_value,
                &change.new_value,
            ) || visibility::ancestor_row_change_concerns(
                &subscription.resolved,
                &change.table,
                &change.old_value,
                &change.new_value,
            ) || subscription.resolved.inherited.values().any(|parent| {
                parent.resolved.inherited.values().any(|ancestor| {
                    visibility::grant_change_concerns(
                        &ancestor.plan,
                        &ancestor.resolved,
                        &change.table,
                        &change.old_value,
                        &change.new_value,
                    )
                })
            }) {
                // A parent's derived grant moved for this member only.
                return Ok(MaskStep::Rebuild);
            }
            if parents.contains_key(&change.table) {
                parent_changes.push(change);
            }
            if any_parents
                .iter()
                .any(|(_, via, table)| via.table == change.table || *table == change.table)
            {
                any_changes.push(change);
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
                        tokens
                            .into_iter()
                            .map(|(name, token)| (table.clone(), name, token)),
                    ),
                    GrantDelta::None => {}
                }
            }
        }
        if source.is_empty()
            && parent_changes.is_empty()
            && any_changes.is_empty()
            && granted.is_empty()
            && !derived_touched
        {
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
            } else {
                let any_children = any_parents
                    .iter()
                    .filter(|(_, _, table)| table == scope)
                    .map(|(column, via, _)| {
                        (
                            column.clone(),
                            children_of(&subscription.resolved, via, token),
                        )
                    })
                    .collect::<Vec<_>>();
                let Some(parent) = subscription.resolved.inherited.get_mut(scope) else {
                    continue;
                };
                std::sync::Arc::make_mut(&mut parent.resolved)
                    .sets
                    .entry(name.clone())
                    .or_default()
                    .insert(token.clone());
                let derived = visibility::recompute_derived(
                    &parent.plan,
                    std::sync::Arc::make_mut(&mut parent.resolved),
                );
                if !derived.is_empty() && any_parents.iter().any(|(_, _, table)| table == scope) {
                    return Ok(MaskStep::Rebuild);
                }
                for (name, added, removed) in derived {
                    let mut derived_columns = BTreeSet::new();
                    set_columns(&parent.plan.predicate, &name, &mut derived_columns);
                    if derived_columns
                        .iter()
                        .any(|column| *column != parent.plan.key)
                    {
                        return Ok(MaskStep::Rebuild);
                    }
                    for token in added.iter().chain(removed.iter()) {
                        for column in parents.get(scope).into_iter().flatten() {
                            fetch
                                .entry(column.clone())
                                .or_default()
                                .insert(token.clone());
                        }
                    }
                }
                let mut columns = BTreeSet::new();
                set_columns(&parent.plan.predicate, name, &mut columns);
                for (column, children) in any_children {
                    fetch.entry(column).or_default().extend(children);
                }
                if columns.iter().any(|column| *column != parent.plan.key) {
                    // The token does not name parent keys; resolve this member.
                    return Ok(MaskStep::Rebuild);
                }
                for column in parents.get(scope).into_iter().flatten() {
                    fetch
                        .entry(column.clone())
                        .or_default()
                        .insert(token.clone());
                }
            }
        }
        if derived_touched
            || granted
                .iter()
                .any(|(scope, _, _)| *scope == subscription.visibility.table)
        {
            // Derived sets follow their source set and the shared index in
            // memory; only tokens that moved are fetched or re-checked.
            for (name, added, removed) in
                visibility::recompute_derived(&subscription.visibility, &mut subscription.resolved)
            {
                let mut columns = BTreeSet::new();
                set_columns(&subscription.visibility.predicate, &name, &mut columns);
                for column in columns {
                    fetch
                        .entry(column)
                        .or_default()
                        .extend(added.iter().chain(removed.iter()).cloned());
                }
            }
        }
        for change in &parent_changes {
            let Some(parent) = subscription.resolved.inherited.get(&change.table) else {
                continue;
            };
            let visible = |row: &Value| {
                subscription.visibility.parent_admits(
                    &subscription.resolved,
                    &change.table,
                    parent,
                    row,
                )
            };
            let (was, now) = (visible(&change.old_value), visible(&change.new_value));
            if was == now {
                continue;
            }
            for column in parents.get(&change.table).into_iter().flatten() {
                if now && !change.old_value.is_null() {
                    // Hidden → visible on an update: its children may already
                    // exist. A new parent has none yet beyond this transaction.
                    fetch
                        .entry(column.clone())
                        .or_default()
                        .insert(change.row_id.clone());
                } else if was {
                    hidden_parents.push((column.clone(), change.row_id.clone()));
                }
            }
        }

        for change in &any_changes {
            for (column, via, table) in &any_parents {
                let Some(parent) = subscription.resolved.inherited.get(table) else {
                    continue;
                };
                if via.table == change.table {
                    // An association moved: re-check its child for this member
                    // if the member sees a linked parent or already holds it.
                    for link in [&change.old_value, &change.new_value] {
                        let Some(child) = link
                            .get(&via.child_column)
                            .filter(|value| !value.is_null())
                            .map(|value| crate::visibility_mask::scalar(Some(value)))
                        else {
                            continue;
                        };
                        let parent_seen = link
                            .get(&via.parent_column)
                            .filter(|value| !value.is_null())
                            .and_then(|value| {
                                parent.row(&crate::visibility_mask::scalar(Some(value)))
                            })
                            .is_some_and(|row| {
                                subscription.visibility.parent_admits(
                                    &subscription.resolved,
                                    table,
                                    parent,
                                    &row,
                                )
                            });
                        let held = subscription.rows.values().any(|row| {
                            row.get(column)
                                .map(|value| crate::visibility_mask::scalar(Some(value)))
                                == Some(child.clone())
                        });
                        if parent_seen || held {
                            fetch.entry(column.clone()).or_default().insert(child);
                        }
                    }
                }
                if *table == change.table {
                    let was = subscription.visibility.parent_admits(
                        &subscription.resolved,
                        table,
                        parent,
                        &change.old_value,
                    );
                    let now = subscription.visibility.parent_admits(
                        &subscription.resolved,
                        table,
                        parent,
                        &change.new_value,
                    );
                    if was != now {
                        let children = children_of(&subscription.resolved, via, &change.row_id);
                        fetch.entry(column.clone()).or_default().extend(children);
                    }
                }
            }
        }
        let projection_covers_acl = visibility::acl_columns(&subscription.visibility)
            .iter()
            .all(|column| subscription.definition.columns.contains(column));
        if !projection_covers_acl {
            for (column, key) in &hidden_parents {
                fetch.entry(column.clone()).or_default().insert(key.clone());
            }
        }
        let hidden_removals = if projection_covers_acl {
            subscription
                .rows
                .iter()
                .filter(|(_, row)| {
                    hidden_parents.iter().any(|(column, key)| {
                        row.get(column)
                            .map(|value| crate::visibility_mask::scalar(Some(value)))
                            == Some(key.clone())
                    }) && !row_in_collection(
                        row,
                        &subscription.args,
                        &subscription.definition,
                        &subscription.visibility,
                        &subscription.resolved,
                    )
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        // Freeze row decisions before checking the index revision. An applier
        // that advanced while making them requires a current SQL snapshot,
        // never an old payload admitted by a newer parent's ACL.
        let source = source
            .into_iter()
            .map(|change| {
                let now = row_in_collection(
                    &change.new_value,
                    &subscription.args,
                    &subscription.definition,
                    &subscription.visibility,
                    &subscription.resolved,
                );
                (change, now)
            })
            .collect::<Vec<_>>();
        if visibility::indexes_ahead(&subscription.resolved, revision)
            && (!fetch.is_empty()
                || !hidden_removals.is_empty()
                || source.iter().any(|(change, now)| {
                    *now || subscription.rows.contains_key(change.row_id.as_str())
                }))
        {
            return Ok(MaskStep::Refresh);
        }
        // Do SQL before mutating delivered rows. Its snapshot must agree with
        // this event's principal. If newer commits exist, rebuild both together.
        let mut fetched = Vec::new();
        if !fetch.is_empty() {
            let control = self
                .inner
                .control_plane
                .read()
                .await
                .clone()
                .ok_or(ReplicaError::ModuleMissing)?;
            let mut transaction = control
                .begin_tenant_transaction(&session.route, true)
                .await?;
            for (column, values) in fetch {
                metrics.record_visibility_sql_statement();
                let rows = query_rows_where(
                    &mut transaction,
                    &subscription.definition,
                    &subscription.visibility,
                    &subscription.resolved,
                    &subscription.args,
                    Some((column.as_str(), &values)),
                    revision,
                )
                .await;
                let rows = match rows {
                    Ok(rows) => rows,
                    Err(ReplicaError::SnapshotAdvanced) => {
                        transaction.rollback().await?;
                        return Ok(MaskStep::Refresh);
                    }
                    Err(error) => return Err(error),
                };
                fetched.push((column, values, rows));
            }
            transaction.commit().await?;
        }
        let mut upserts = BTreeMap::new();
        let mut deleted = BTreeSet::new();
        let mut inserted = false;
        for id in hidden_removals {
            metrics.record_in_memory_row_check();
            self.mask_apply_row(
                subscription,
                &id,
                None,
                &[],
                entity_changes,
                &mut upserts,
                &mut deleted,
            )?;
        }
        for (change, now) in source {
            metrics.record_in_memory_row_check();
            if !now && !subscription.rows.contains_key(change.row_id.as_str()) {
                continue;
            }
            let projection = now
                .then(|| project_row(&change.new_value, &subscription.definition))
                .transpose()?;
            inserted |= now && !subscription.rows.contains_key(change.row_id.as_str());
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
        for (column, values, rows) in fetched {
            let mut present = BTreeSet::new();
            for row in rows {
                let key = row_key(&row, &subscription.definition.key)?;
                present.insert(key.clone());
                inserted |= !subscription.rows.contains_key(key.as_str());
                self.mask_apply_row(
                    subscription,
                    &key,
                    Some(row),
                    &[],
                    entity_changes,
                    &mut upserts,
                    &mut deleted,
                )?;
            }
            // A re-check of hidden parents: cached children it no longer returns go.
            let stale = subscription
                .rows
                .iter()
                .filter(|(id, row)| {
                    !present.contains(id.as_ref())
                        && row.get(&column).is_some_and(|value| {
                            values.contains(&crate::visibility_mask::scalar(Some(value)))
                        })
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            for id in stale {
                self.mask_apply_row(
                    subscription,
                    &id,
                    None,
                    &[],
                    entity_changes,
                    &mut upserts,
                    &mut deleted,
                )?;
            }
        }
        if inserted
            && !self.mask_enforce_budget(
                subscription,
                entity_changes,
                &mut upserts,
                &mut deleted,
            )?
        {
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
        if old.is_some() && old.as_ref().map(|entry| &*entry.row) == projection.as_ref() {
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
                let shared = crate::replica::intern_row(
                    &subscription.database_url,
                    &subscription.definition.table,
                    id,
                    &hash,
                    row.clone(),
                );
                subscription
                    .hashes
                    .insert(shared.key.clone(), shared.hash.clone());
                subscription.rows.insert(shared.key.clone(), shared);
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
                old_value: old.map(|row| (*row).clone()),
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
        let bytes = |rows: &crate::replica::RowMap| {
            rows.values()
                .map(|row| serde_json::to_vec(row).unwrap_or_default().len() as i64)
                .sum::<i64>()
        };
        let over_bytes =
            definition.max_bytes > 0 && bytes(&subscription.rows) > definition.max_bytes;
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
            .map(|(id, row)| {
                (
                    id.clone(),
                    row.get(&order_by).cloned().unwrap_or(Value::Null),
                )
            })
            .collect::<Vec<_>>();
        ordered.sort_by(|(left_id, left), (right_id, right)| {
            let order = compare_json(left, right).then_with(|| left_id.cmp(right_id));
            if descending {
                order.reverse()
            } else {
                order
            }
        });
        let max_rows = if definition.max_rows > 0 {
            definition.max_rows
        } else {
            usize::MAX
        };
        let max_bytes = definition.max_bytes;
        let mut kept_bytes = 0i64;
        let mut evict = Vec::new();
        for (index, (id, _)) in ordered.iter().enumerate() {
            let size = serde_json::to_vec(&subscription.rows[id])
                .unwrap_or_default()
                .len() as i64;
            if index >= max_rows || (max_bytes > 0 && kept_bytes + size > max_bytes) {
                evict.push(id.clone());
            } else {
                kept_bytes += size;
            }
        }
        for id in evict {
            self.mask_apply_row(
                subscription,
                &id,
                None,
                &[],
                entity_changes,
                upserts,
                deleted,
            )?;
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
