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
        "inSet" => resolved
            .sets
            .get(&expression.set)
            .is_some_and(|set| set.contains(&scalar(row.get(&expression.column)))),
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
