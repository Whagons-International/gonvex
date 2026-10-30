//! Pure row visibility evaluation. No database handles or asynchronous work.

use crate::visibility::{ResolvedVisibility, VisibilityExpression};
use serde_json::Value;

pub(crate) fn match_expression(
    expression: &VisibilityExpression,
    resolved: &ResolvedVisibility,
    row: &serde_json::Map<String, Value>,
) -> bool {
    match expression.operator.as_str() {
        "public" => true,
        "nonEmpty" => resolved
            .sets
            .get(&expression.set)
            .is_some_and(|set| !set.is_empty()),
        "permission" => expression
            .value
            .as_ref()
            .and_then(Value::as_str)
            .is_some_and(|permission| truthy(resolved.permissions.get(permission))),
        "role" => expression
            .value
            .as_ref()
            .and_then(Value::as_str)
            .is_some_and(|role| resolved.role == role),
        "eq" => expression
            .value
            .as_ref()
            .and_then(|value| value.get("literal"))
            .is_some_and(|literal| row.get(&expression.column) == Some(literal)),
        "eqContext" => scalar(row.get(&expression.column)) == resolved.direct[&expression.context],
        "inSet" => row
            .get(&expression.column)
            .filter(|value| !value.is_null())
            .is_some_and(|value| {
                resolved
                    .sets
                    .get(&expression.set)
                    .is_some_and(|set| set.contains(&scalar(Some(value))))
            }),
        "inherits" => {
            let Some(parent) = resolved.inherited.get(&expression.table) else {
                return false;
            };
            let key = match row.get(&expression.column) {
                None | Some(Value::Null) => return false,
                Some(value) => scalar(Some(value)),
            };
            match parent.row(&key) {
                Some(parent_row) => {
                    crate::visibility::literal_constraints_hold(
                        &expression.parent_where,
                        &parent_row,
                    ) && parent_row.as_object().is_some_and(|parent_row| {
                        match_expression(&parent.plan.predicate, &parent.resolved, parent_row)
                    })
                }
                None => expression
                    .on_missing
                    .as_ref()
                    .is_some_and(|fallback| match_expression(fallback, resolved, row)),
            }
        }
        "inheritsAny" => {
            let (Some(parent), Some(via)) = (
                resolved.inherited.get(&expression.table),
                expression.via.as_ref(),
            ) else {
                return false;
            };
            let key = match row.get(&expression.column) {
                None | Some(Value::Null) => return false,
                Some(value) => scalar(Some(value)),
            };
            let Some(links) = resolved
                .indexes
                .get(&crate::visibility::via_index_name(via, &via.child_column))
            else {
                return false;
            };
            let Ok(links) = links.read() else {
                return false;
            };
            let admitted = links.get_all(&key).into_iter().any(|link| {
                let parent_key = match link.get(&via.parent_column) {
                    None | Some(Value::Null) => return false,
                    Some(value) => scalar(Some(value)),
                };
                parent.row(&parent_key).is_some_and(|parent_row| {
                    crate::visibility::literal_constraints_hold(
                        &expression.parent_where,
                        &parent_row,
                    ) && parent_row.as_object().is_some_and(|parent_row| {
                        match_expression(&parent.plan.predicate, &parent.resolved, parent_row)
                    })
                })
            });
            admitted
        }
        "and" => expression
            .children
            .iter()
            .all(|child| match_expression(child, resolved, row)),
        "or" => expression
            .children
            .iter()
            .any(|child| match_expression(child, resolved, row)),
        "not" => !match_expression(&expression.children[0], resolved, row),
        _ => false,
    }
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => value == "true" || value == "1",
        Some(Value::Number(value)) => value.as_f64().unwrap_or_default() != 0.0,
        _ => false,
    }
}

pub(crate) fn scalar(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "null".to_owned(),
        Some(Value::String(value)) => value.clone(),
        Some(value) => serde_json::to_string(value).unwrap_or_default(),
    }
}
