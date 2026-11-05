//! Version 2 mask delivery for one Replica subscription.
//!
//! A committed transaction is decided in memory against the cached
//! principal: source rows by their before/after images, and rows whose
//! visibility a grant, derived set or parent flip moved by one targeted read
//! of those rows (no visibility predicate) evaluated before and after. The
//! before state is the principal before this transaction's grants plus the
//! parents' before-images (a thread-local overlay of the shared parent
//! index). Everything else (this member's principal changed, a grant was
//! removed, an `inheritsAny` association moved, an ancestor's grant moved, an
//! index ahead of or behind the event) reconciles the collection with the
//! client's row hashes.

use super::*;
use crate::visibility::{grant_contribution, GrantContribution, VisibilityExpression};

// `Apply` is built once per subscription per transaction and matched in
// place; boxing it would add an allocation on the fan-out hot path.
#[allow(clippy::large_enum_variant)]
pub(super) enum Step {
    /// Nothing in the transaction concerns this subscription.
    Skip,
    /// Decide source rows (and `affected` rows) from their images.
    Apply(ApplyContext),
    /// Ask the client for its row hashes and reconcile against SQL.
    Resync(Resync),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Resync {
    /// This member's principal changed in a way memory cannot follow.
    Principal,
    /// A shared index is not at the event's revision.
    Consistency,
    /// A bounded window moved beyond what its counters can represent.
    Window,
}

/// How to evaluate the before-images of one transaction.
#[derive(Default)]
pub(super) struct ApplyContext {
    /// The principal before this transaction's grants (None: unchanged).
    pub before: Option<ResolvedVisibility>,
    /// Parent rows as they were before this transaction.
    #[allow(clippy::type_complexity)]
    pub overlay: Option<Arc<Vec<(String, String, Option<Arc<Value>>)>>>,
    /// Rows of the source table whose visibility may have moved without the
    /// row changing, as they are at the event's revision.
    pub affected: Vec<(String, Value)>,
}

enum GrantDelta {
    None,
    Added(Vec<(String, String)>),
    Rebuild,
}

/// Grant rows decided in memory. Additions patch the set; a removal may still
/// be covered by another row of the member, so it re-resolves this member.
/// Per inherited parent table, the columns of its tenant-shared ACL index
/// (`visibility::index_columns` for this child and parent plan).
fn parent_acl_columns(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
) -> BTreeMap<String, BTreeSet<String>> {
    resolved
        .inherited
        .iter()
        .map(|(table, parent)| {
            let plans = BTreeMap::from([
                (table.clone(), parent.plan.as_ref().clone()),
                (plan.table.clone(), plan.clone()),
            ]);
            (
                table.clone(),
                visibility::index_columns(&plans, table, &parent.plan.key),
            )
        })
        .collect()
}

/// Every table the visibility of `plan` (as resolved for one member) reads:
/// its own table, every set's table and joins, parent and `inheritsAny`
/// tables, recursively through inherited parents. A change to any other
/// table cannot move a principal, grant, derived set or parent decision.
fn concern_tables(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    out: &mut crate::fast_hash::FastSet<String>,
) {
    out.insert(plan.table.clone());
    for set in plan.sets.values() {
        out.insert(set.table.clone());
        for join in &set.joins {
            out.insert(join.table.clone());
        }
    }
    for table in plan.inherited_columns().keys() {
        out.insert(table.clone());
    }
    for expression in visibility::inherits_any(plan) {
        out.insert(expression.table.clone());
        if let Some(via) = &expression.via {
            out.insert(via.table.clone());
        }
    }
    for (table, parent) in &resolved.inherited {
        out.insert(table.clone());
        concern_tables(&parent.plan, &parent.resolved, out);
    }
}

/// The subscription's concern tables (`concern_tables` plus its source and
/// `members`), cached on it. Shared by the mask step and the fan-out shape.
pub(super) fn subscription_concern_tables(
    subscription: &mut ReplicaSubscription,
) -> Arc<crate::fast_hash::FastSet<String>> {
    Arc::clone(subscription.concern_tables.get_or_insert_with(|| {
        let mut tables = crate::fast_hash::FastSet::default();
        tables.insert(subscription.definition.table.clone());
        tables.insert("members".to_owned());
        concern_tables(
            &subscription.visibility,
            &subscription.resolved,
            &mut tables,
        );
        Arc::new(tables)
    }))
}

/// `parent_acl_columns` of the subscription, cached on it.
pub(super) fn subscription_parent_acl_columns(
    subscription: &mut ReplicaSubscription,
) -> Arc<BTreeMap<String, BTreeSet<String>>> {
    Arc::clone(subscription.parent_acl_columns.get_or_insert_with(|| {
        Arc::new(parent_acl_columns(
            &subscription.visibility,
            &subscription.resolved,
        ))
    }))
}

fn grant_delta(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    change: &LogChange,
) -> GrantDelta {
    let mut added = Vec::new();
    for (name, set) in plan.active_grant_sets_iter(resolved) {
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

/// Columns compared to a set by `inSet`, so moved tokens select their rows.
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

/// Does `set` decide anything other than `inSet` comparisons (a `nonEmpty`
/// test flips every row at once)?
fn set_tested_as_a_whole(expression: &VisibilityExpression, set: &str) -> bool {
    (expression.operator == "nonEmpty" && expression.set == set)
        || expression
            .children
            .iter()
            .any(|child| set_tested_as_a_whole(child, set))
        || expression
            .on_missing
            .as_ref()
            .is_some_and(|fallback| set_tested_as_a_whole(fallback, set))
}

/// Which of the mask step's per-change checks can fire for a change of one
/// table (`replica_fanout` precomputes them per combination of plans).
#[derive(Clone, Copy, Default)]
pub(crate) struct MovingChecks {
    /// `principal_change_concerns`: `members`, or a table of a principal set
    /// or of any non-derived set's joins.
    pub principal: bool,
    /// `derived_change_concerns` / `ancestor_derived_change_concerns`: the
    /// table of a derived set.
    pub derived: bool,
    /// `grant_delta`: the table of a grant set.
    pub grant: bool,
}

/// Whether `change` (of a table the subscription's visibility reads) leaves
/// this member's principal, grants and derived sets as they are: every
/// per-change check of [`Runtime::mask_step`] that would re-resolve the
/// member, patch its sets or re-decide affected rows is false, so the step
/// only decides source rows (and parent flips). `checks` names the checks
/// that can fire for the change's table; the others are false by
/// construction. Only for plans whose parents do not inherit again and
/// without `inheritsAny` (the fan-out's shareable plans), where the
/// ancestor and association checks of the loop are false. Keep in step with
/// the loop in `mask_step`.
pub(super) fn member_unmoved(
    subscription: &ReplicaSubscription,
    change: &LogChange,
    checks: MovingChecks,
) -> bool {
    let plan = &subscription.visibility;
    let resolved = &subscription.resolved;
    let (table, old, new) = (change.table.as_str(), &change.old_value, &change.new_value);
    if checks.principal
        && plan.principal_change_concerns(
            resolved,
            table,
            &change.operation,
            &change.changed_columns,
            old,
            new,
        )
    {
        return false;
    }
    if checks.derived
        && (visibility::derived_change_concerns(plan, resolved, table, &change.row_id, old, new)
            || visibility::ancestor_derived_change_concerns(
                resolved,
                table,
                &change.row_id,
                old,
                new,
            ))
    {
        return false;
    }
    !checks.grant
        || (matches!(grant_delta(plan, resolved, change), GrantDelta::None)
            && resolved.inherited.values().all(|parent| {
                matches!(
                    grant_delta(&parent.plan, &parent.resolved, change),
                    GrantDelta::None
                )
            }))
}

impl Runtime {
    pub(super) async fn mask_step(
        &self,
        session: &TenantSession,
        subscription: &mut ReplicaSubscription,
        revision: u64,
        changes: &[LogChange],
        collapsed: &CollapsedChanges<'_>,
        share: Option<&crate::feed_share::EventShare>,
    ) -> Result<Step, ReplicaError> {
        let metrics = &self.inner.visibility_metrics;
        let plan = Arc::clone(&subscription.visibility);
        let parents = plan.inherited_columns();
        let any_parents = visibility::inherits_any(&subscription.visibility)
            .iter()
            .filter_map(|expression| Some((expression.via.clone()?, expression.table.clone())))
            .collect::<Vec<_>>();
        let tables = subscription_concern_tables(subscription);
        let mut source = false;
        let mut parent_changes = Vec::new();
        // (scope table, set name, token): own plan or a directly inherited parent.
        let mut granted: Vec<(String, String, String)> = Vec::new();
        let mut derived_touched = false;
        for change in changes {
            metrics.record_in_memory_row_check();
            if !tables.contains(change.table.as_str()) {
                // Neither the source nor any table the plan, its sets or its
                // parents read: none of the checks below can fire.
                continue;
            }
            if subscription.visibility.principal_change_concerns(
                &subscription.resolved,
                &change.table,
                &change.operation,
                &change.changed_columns,
                &change.old_value,
                &change.new_value,
            ) {
                return Ok(Step::Resync(Resync::Principal));
            }
            source |= change.table == subscription.definition.table;
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
                return Ok(Step::Resync(Resync::Principal));
            }
            if any_parents
                .iter()
                .any(|(via, table)| via.table == change.table || *table == change.table)
            {
                // `inheritsAny` memberships move through association rows
                // whose before-state the shared link index no longer has.
                return Ok(Step::Resync(Resync::Principal));
            }
            if parents.contains_key(&change.table) {
                parent_changes.push(change);
            }
            match grant_delta(&subscription.visibility, &subscription.resolved, change) {
                GrantDelta::Rebuild => return Ok(Step::Resync(Resync::Principal)),
                GrantDelta::Added(tokens) => granted.extend(
                    tokens
                        .into_iter()
                        .map(|(name, token)| (subscription.visibility.table.clone(), name, token)),
                ),
                GrantDelta::None => {}
            }
            for (table, parent) in &subscription.resolved.inherited {
                match grant_delta(&parent.plan, &parent.resolved, change) {
                    GrantDelta::Rebuild => return Ok(Step::Resync(Resync::Principal)),
                    GrantDelta::Added(tokens) => granted.extend(
                        tokens
                            .into_iter()
                            .map(|(name, token)| (table.clone(), name, token)),
                    ),
                    GrantDelta::None => {}
                }
            }
        }
        if !source && parent_changes.is_empty() && granted.is_empty() && !derived_touched {
            return Ok(Step::Skip);
        }

        let mut context = ApplyContext::default();
        // Source rows a moved token or flipped parent may reveal or hide:
        // (child column, values).
        let mut affected: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        if !granted.is_empty() || derived_touched {
            context.before = Some(subscription.resolved.clone());
        }
        for (scope, name, token) in &granted {
            if *scope == subscription.visibility.table {
                subscription
                    .resolved
                    .sets
                    .entry(name.clone())
                    .or_default()
                    .insert(token.clone());
                if set_tested_as_a_whole(&subscription.visibility.predicate, name) {
                    return Ok(Step::Resync(Resync::Principal));
                }
                let mut columns = BTreeSet::new();
                set_columns(&subscription.visibility.predicate, name, &mut columns);
                for column in columns {
                    affected.entry(column).or_default().insert(token.clone());
                }
            } else {
                let Some(parent) = subscription.resolved.inherited.get_mut(scope) else {
                    continue;
                };
                let parent_resolved = Arc::make_mut(&mut parent.resolved);
                parent_resolved
                    .sets
                    .entry(name.clone())
                    .or_default()
                    .insert(token.clone());
                let derived = visibility::recompute_derived(&parent.plan, parent_resolved);
                let mut columns = BTreeSet::new();
                set_columns(&parent.plan.predicate, name, &mut columns);
                for (derived_name, _, _) in &derived {
                    set_columns(&parent.plan.predicate, derived_name, &mut columns);
                }
                if set_tested_as_a_whole(&parent.plan.predicate, name)
                    || columns.iter().any(|column| *column != parent.plan.key)
                {
                    // The token does not name parent keys; resolve this member.
                    return Ok(Step::Resync(Resync::Principal));
                }
                let tokens = std::iter::once(token.clone()).chain(
                    derived
                        .into_iter()
                        .flat_map(|(_, added, removed)| added.into_iter().chain(removed)),
                );
                let tokens = tokens.collect::<Vec<_>>();
                for column in parents.get(scope).into_iter().flatten() {
                    affected
                        .entry(column.clone())
                        .or_default()
                        .extend(tokens.iter().cloned());
                }
            }
        }
        if derived_touched || !granted.is_empty() {
            // Derived sets follow their source set and the shared index in
            // memory; rows of every token that moved are re-decided.
            for (name, added, removed) in
                visibility::recompute_derived(&subscription.visibility, &mut subscription.resolved)
            {
                if set_tested_as_a_whole(&subscription.visibility.predicate, &name) {
                    return Ok(Step::Resync(Resync::Principal));
                }
                let mut columns = BTreeSet::new();
                set_columns(&subscription.visibility.predicate, &name, &mut columns);
                for column in columns {
                    affected
                        .entry(column)
                        .or_default()
                        .extend(added.iter().chain(removed.iter()).cloned());
                }
            }
        }
        if !parent_changes.is_empty() {
            let mut overlay = Vec::new();
            let acl_columns = subscription_parent_acl_columns(subscription);
            let before = context.before.as_ref().unwrap_or(&subscription.resolved);
            for change in &parent_changes {
                let image = collapsed
                    .get(&(change.table.as_str(), change.row_id.as_str()))
                    .map_or(&*change.old_value, |collapsed| collapsed.old);
                if overlay.iter().any(|(table, key, _): &(String, String, _)| {
                    *table == change.table && *key == change.row_id
                }) {
                    continue;
                }
                overlay.push((
                    change.table.clone(),
                    change.row_id.clone(),
                    (!image.is_null()).then(|| match share {
                        // One copy of the before-image per event, not per
                        // subscription.
                        Some(share) => share.image(image),
                        None => Arc::new(image.clone()),
                    }),
                ));
                let (Some(was_parent), Some(now_parent)) = (
                    before.inherited.get(&change.table),
                    subscription.resolved.inherited.get(&change.table),
                ) else {
                    continue;
                };
                let net = collapsed.get(&(change.table.as_str(), change.row_id.as_str()));
                let last = net.map_or(&*change.new_value, |collapsed| collapsed.new);
                // An update that changed none of the parent's ACL index
                // columns, under an unchanged principal, cannot flip whether
                // the parent admits its children (parent visibility reads
                // only those columns).
                if context.before.is_none() && !image.is_null() && !last.is_null() {
                    let columns = &acl_columns;
                    let unchanged = net.is_some_and(|net| {
                        net.changed_columns().next().is_some()
                            && columns.get(&change.table).is_some_and(|acl| {
                                net.changed_columns().all(|column| !acl.contains(column))
                            })
                    });
                    if unchanged {
                        continue;
                    }
                }
                let was =
                    subscription
                        .visibility
                        .parent_admits(before, &change.table, was_parent, image);
                let now = subscription.visibility.parent_admits(
                    &subscription.resolved,
                    &change.table,
                    now_parent,
                    last,
                );
                // A parent created by this transaction has no children
                // beyond the transaction's own source rows.
                if was != now && !image.is_null() {
                    for column in parents.get(&change.table).into_iter().flatten() {
                        affected
                            .entry(column.clone())
                            .or_default()
                            .insert(change.row_id.clone());
                    }
                }
            }
            context.overlay = Some(Arc::new(overlay));
        }
        // Row decisions read the shared indexes: one that already holds later
        // ACL changes could admit an old payload.
        if visibility::indexes_ahead(&subscription.resolved, revision)
            && (source || !affected.is_empty())
        {
            return Ok(Step::Resync(Resync::Consistency));
        }
        if !affected.is_empty() {
            match self
                .affected_rows(session, subscription, &affected, revision, collapsed)
                .await?
            {
                Some(rows) => context.affected = rows,
                None => return Ok(Step::Resync(Resync::Consistency)),
            }
        }
        Ok(Step::Apply(context))
    }

    /// Rows of the subscription's source table whose `column` is one of the
    /// values, as they were at `revision` (rows changed by this event are
    /// decided from its images instead). No visibility predicate: each row is
    /// decided in memory before and after. None when the change log no
    /// longer covers the revisions between `revision` and this read.
    async fn affected_rows(
        &self,
        session: &TenantSession,
        subscription: &ReplicaSubscription,
        affected: &BTreeMap<String, BTreeSet<String>>,
        revision: u64,
        collapsed: &CollapsedChanges<'_>,
    ) -> Result<Option<Vec<(String, Value)>>, ReplicaError> {
        // The read has no visibility predicate, so every subscriber of the
        // collection that this event touches the same way shares it: one
        // read per event, not one per connection.
        type Shared = Option<Arc<Vec<(String, Value)>>>;
        static RECENT: std::sync::OnceLock<
            std::sync::Mutex<std::collections::VecDeque<(String, Shared)>>,
        > = std::sync::OnceLock::new();
        let key = format!(
            "{}\u{0}{revision}\u{0}{:p}\u{0}{:p}\u{0}{}",
            session.route.database_url,
            Arc::as_ptr(&subscription.definition),
            Arc::as_ptr(&subscription.visibility),
            serde_json::to_string(affected).unwrap_or_default()
        );
        let recent = RECENT.get_or_init(Default::default);
        let cached = recent
            .lock()
            .unwrap()
            .iter()
            .find(|(cached, _)| *cached == key)
            .map(|(_, rows)| rows.clone());
        let rows = match cached {
            Some(rows) => rows,
            None => {
                // One cache miss per shared affected-row read, rather than
                // retaining SQL transaction state in every subscriber's task.
                let rows =
                    Box::pin(self.read_affected_rows(session, subscription, affected, revision))
                        .await?
                        .map(Arc::new);
                let mut recent = recent.lock().unwrap();
                if recent.len() >= 64 {
                    recent.pop_front();
                }
                recent.push_back((key, rows.clone()));
                rows
            }
        };
        let table = subscription.definition.table.as_str();
        Ok(rows.map(|rows| {
            rows.iter()
                .filter(|(id, _)| !collapsed.contains_key(&(table, id.as_str())))
                .cloned()
                .collect()
        }))
    }

    async fn read_affected_rows(
        &self,
        session: &TenantSession,
        subscription: &ReplicaSubscription,
        affected: &BTreeMap<String, BTreeSet<String>>,
        revision: u64,
    ) -> Result<Option<Vec<(String, Value)>>, ReplicaError> {
        let definition = &subscription.definition;
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
        let clock = clock_in_transaction(&mut transaction).await?;
        if clock.revision < revision || clock.retained_revision > revision {
            transaction.rollback().await?;
            return Ok(None);
        }
        let mut wanted = definition
            .columns
            .iter()
            .cloned()
            .chain(visibility::acl_columns(&subscription.visibility))
            .chain(definition.equal_filters.keys().cloned())
            .chain(definition.exclude_when_set.iter().cloned())
            .chain(affected.keys().cloned())
            .collect::<BTreeSet<_>>();
        wanted.insert(definition.key.clone());
        if !definition.order_by.is_empty() {
            wanted.insert(definition.order_by.clone());
        }
        let columns = declared_columns(&mut transaction, definition, wanted.iter()).await?;
        let mut rows = BTreeMap::new();
        for (column, values) in affected {
            self.inner
                .visibility_metrics
                .record_visibility_sql_statement();
            let statement = format!(
                "SELECT {} FROM {} AS r WHERE r.{} IN (SELECT value FROM json_each($1))",
                gonvex_store::rows::json_object_expr("r", &columns),
                quote(&definition.table)?,
                quote(column)?,
            );
            let values = serde_json::to_string(values)
                .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))?;
            let fetched: Vec<Option<String>> = sqlx::query_scalar(&statement)
                .bind(values)
                .fetch_all(&mut **transaction.transaction())
                .await?;
            for raw in fetched.into_iter().flatten() {
                let row: Value = serde_json::from_str(&raw)
                    .map_err(|error| ReplicaError::InvalidIdentifier(error.to_string()))?;
                rows.insert(row_key(&row, &definition.key)?, row);
            }
        }
        if clock.revision > revision {
            // Rows written after the event: their image at the event is the
            // before-image of their first later change.
            let later = sqlx::query(
                "SELECT row_id, old_value FROM _gonvex_sync_changes
                 WHERE table_name = $1 AND revision > $2 AND revision <= $3
                 ORDER BY revision, ordinal",
            )
            .bind(&definition.table)
            .bind(revision as i64)
            .bind(clock.revision as i64)
            .fetch_all(&mut **transaction.transaction())
            .await?;
            let mut first = BTreeSet::new();
            for row in &later {
                let id: String = row.try_get("row_id")?;
                if !first.insert(id.clone()) {
                    continue;
                }
                let old = log_json(row, "old_value")?;
                let matches = affected.iter().any(|(column, values)| {
                    old.get(column).is_some_and(|value| {
                        !value.is_null()
                            && values.contains(&crate::visibility_mask::scalar(Some(value)))
                    })
                });
                if matches {
                    rows.insert(id, old);
                } else {
                    rows.remove(&id);
                }
            }
        }
        transaction.commit().await?;
        Ok(Some(rows.into_iter().collect()))
    }
}
