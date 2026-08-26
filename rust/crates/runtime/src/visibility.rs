//! Centralized structured row visibility.

use std::collections::{BTreeMap, BTreeSet};

use gonvex_postgres::{DatabaseError, TenantSession, TenantTransaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::Row;
use thiserror::Error;

use crate::host_calls::bind_value;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityPlan {
    pub table: String,
    pub key: String,
    #[serde(default)]
    pub sets: BTreeMap<String, VisibilitySet>,
    #[serde(rename = "where")]
    pub predicate: VisibilityExpression,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilitySet {
    pub table: String,
    #[serde(default)]
    pub alias: String,
    pub select: String,
    #[serde(default)]
    pub select_from: String,
    #[serde(default)]
    pub joins: Vec<VisibilityJoin>,
    #[serde(default, rename = "where")]
    pub constraints: Vec<VisibilityConstraint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityJoin {
    pub table: String,
    #[serde(default)]
    pub alias: String,
    #[serde(default)]
    pub left_alias: String,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityConstraint {
    #[serde(default)]
    pub table: String,
    pub column: String,
    pub context: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityExpression {
    pub operator: String,
    #[serde(default)]
    pub column: String,
    #[serde(default)]
    pub context: String,
    #[serde(default)]
    pub set: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub children: Vec<VisibilityExpression>,
}

#[derive(Clone, Debug)]
pub struct ResolvedVisibility {
    pub revision: u64,
    pub direct: BTreeMap<String, String>,
    pub role: String,
    pub permissions: Value,
    pub sets: BTreeMap<String, BTreeSet<String>>,
    pub fingerprint: String,
}

#[derive(Debug, Error)]
pub enum VisibilityError {
    #[error("invalid visibility plan: {0}")]
    Invalid(String),
    #[error("active tenant member is required for visibility")]
    MemberRequired,
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

impl VisibilityPlan {
    pub fn validate(&self) -> Result<(), VisibilityError> {
        identifier(&self.table)?;
        identifier(&self.key)?;
        for (name, set) in &self.sets {
            identifier(name)?;
            validate_set(set)?;
        }
        validate_expression(&self.predicate, &self.sets)
    }

    pub fn dependencies(&self) -> BTreeSet<String> {
        let mut result = BTreeSet::from(["members".to_owned()]);
        for set in self.sets.values() {
            result.insert(set.table.clone());
            for join in &set.joins {
                result.insert(join.table.clone());
            }
        }
        result
    }
}

pub async fn resolve(
    transaction: &mut TenantTransaction,
    session: &TenantSession,
    plan: &VisibilityPlan,
) -> Result<ResolvedVisibility, VisibilityError> {
    plan.validate()?;
    let row = sqlx::query(
        r#"SELECT id, role, permissions
           FROM members WHERE account_id = $1 AND status = 'active'"#,
    )
    .bind(&session.identity.account.id)
    .fetch_optional(&mut **transaction.transaction())
    .await?
    .ok_or(VisibilityError::MemberRequired)?;
    let member_id: String = row.get("id");
    let role: String = row.get("role");
    let permissions = row.try_get::<Json<Value>, _>("permissions")?.0;
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton = true")
            .fetch_one(&mut **transaction.transaction())
            .await?;
    let direct = BTreeMap::from([
        ("account.id".to_owned(), session.identity.account.id.clone()),
        ("member.id".to_owned(), member_id),
        ("tenant.id".to_owned(), session.route.tenant_id.clone()),
    ]);
    let mut sets = BTreeMap::new();
    for (name, set) in &plan.sets {
        let (statement, parameters) = compile_set(set, &direct)?;
        let mut query = sqlx::query(&statement);
        for parameter in &parameters {
            query = bind_value(query, parameter).map_err(VisibilityError::Invalid)?;
        }
        let rows = query.fetch_all(&mut **transaction.transaction()).await?;
        let values = rows
            .into_iter()
            .filter_map(|row| row.try_get::<Option<String>, _>(0).ok().flatten())
            .collect();
        sets.insert(name.clone(), values);
    }
    let fingerprint = visibility_fingerprint(plan, &direct, &role, &permissions, &sets);
    Ok(ResolvedVisibility {
        revision: revision.max(0) as u64,
        direct,
        role,
        permissions,
        sets,
        fingerprint,
    })
}

pub fn compile_predicate(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    row_alias: &str,
    parameters: &mut Vec<Value>,
) -> Result<String, VisibilityError> {
    compile_expression(&plan.predicate, plan, resolved, row_alias, parameters)
}

pub fn row_matches(plan: &VisibilityPlan, resolved: &ResolvedVisibility, row: &Value) -> bool {
    let Some(row) = row.as_object() else {
        return false;
    };
    match_expression(&plan.predicate, resolved, row)
}

fn validate_set(set: &VisibilitySet) -> Result<(), VisibilityError> {
    identifier(&set.table)?;
    identifier(&set.select)?;
    let base_alias = if set.alias.is_empty() {
        &set.table
    } else {
        identifier(&set.alias)?;
        &set.alias
    };
    let mut aliases = BTreeSet::from([base_alias.clone()]);
    let mut physical = BTreeMap::from([(set.table.clone(), 1usize)]);
    for join in &set.joins {
        identifier(&join.table)?;
        identifier(&join.left_column)?;
        identifier(&join.right_column)?;
        let alias = if join.alias.is_empty() {
            &join.table
        } else {
            identifier(&join.alias)?;
            &join.alias
        };
        if !aliases.insert(alias.clone()) {
            return Err(VisibilityError::Invalid(format!(
                "visibility alias {alias:?} is repeated"
            )));
        }
        *physical.entry(join.table.clone()).or_default() += 1;
    }
    if physical.iter().any(|(table, count)| {
        *count > 1
            && ((table == &set.table && set.alias.is_empty())
                || set
                    .joins
                    .iter()
                    .any(|join| &join.table == table && join.alias.is_empty()))
    }) {
        return Err(VisibilityError::Invalid(
            "every occurrence of a repeated visibility table requires an alias".to_owned(),
        ));
    }
    Ok(())
}

fn validate_expression(
    expression: &VisibilityExpression,
    sets: &BTreeMap<String, VisibilitySet>,
) -> Result<(), VisibilityError> {
    match expression.operator.as_str() {
        "public" => {}
        "permission" | "role" if expression.value.trim().is_empty() => {
            return Err(VisibilityError::Invalid(format!(
                "{} requires a value",
                expression.operator
            )))
        }
        "permission" | "role" => {}
        "eqContext" => {
            identifier(&expression.column)?;
            context_key(&expression.context)?;
        }
        "inSet" => {
            identifier(&expression.column)?;
            if !sets.contains_key(&expression.set) {
                return Err(VisibilityError::Invalid(format!(
                    "unknown visibility set {:?}",
                    expression.set
                )));
            }
        }
        "and" | "or" if expression.children.is_empty() => {
            return Err(VisibilityError::Invalid(format!(
                "{} requires children",
                expression.operator
            )))
        }
        "and" | "or" => {}
        "not" if expression.children.len() == 1 => {}
        "not" => {
            return Err(VisibilityError::Invalid(
                "not requires one child".to_owned(),
            ))
        }
        operator => {
            return Err(VisibilityError::Invalid(format!(
                "unsupported operator {operator:?}"
            )))
        }
    }
    for child in &expression.children {
        validate_expression(child, sets)?;
    }
    Ok(())
}

fn compile_expression(
    expression: &VisibilityExpression,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    row_alias: &str,
    parameters: &mut Vec<Value>,
) -> Result<String, VisibilityError> {
    let argument = |parameters: &mut Vec<Value>, value: Value| {
        parameters.push(value);
        format!("${}", parameters.len())
    };
    Ok(match expression.operator.as_str() {
        "public" => "TRUE".to_owned(),
        "permission" => {
            let account = argument(
                parameters,
                Value::String(resolved.direct["account.id"].clone()),
            );
            let permission = argument(parameters, Value::String(expression.value.clone()));
            format!(
                "EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = {account} AND _gonvex_member.status = 'active' AND lower(COALESCE(_gonvex_member.permissions ->> {permission}::text, '')) IN ('true', '1'))"
            )
        }
        "role" => {
            let account = argument(
                parameters,
                Value::String(resolved.direct["account.id"].clone()),
            );
            let role = argument(parameters, Value::String(expression.value.clone()));
            format!(
                "EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = {account} AND _gonvex_member.status = 'active' AND _gonvex_member.role = {role})"
            )
        }
        "eqContext" => {
            let column = quote(&expression.column)?;
            let value = argument(
                parameters,
                Value::String(resolved.direct[&expression.context].clone()),
            );
            format!("{row_alias}.{column} = {value}")
        }
        "inSet" => {
            let column = quote(&expression.column)?;
            let set = plan.sets.get(&expression.set).ok_or_else(|| {
                VisibilityError::Invalid(format!("unknown set {:?}", expression.set))
            })?;
            let subquery = compile_set_with_parameters(set, &resolved.direct, parameters)?;
            format!("{row_alias}.{column} IN ({subquery})")
        }
        "and" | "or" => {
            let mut children = Vec::new();
            for child in &expression.children {
                children.push(format!(
                    "({})",
                    compile_expression(child, plan, resolved, row_alias, parameters)?
                ));
            }
            children.join(if expression.operator == "and" {
                " AND "
            } else {
                " OR "
            })
        }
        "not" => format!(
            "NOT ({})",
            compile_expression(
                &expression.children[0],
                plan,
                resolved,
                row_alias,
                parameters
            )?
        ),
        _ => "FALSE".to_owned(),
    })
}

fn compile_set(
    set: &VisibilitySet,
    direct: &BTreeMap<String, String>,
) -> Result<(String, Vec<Value>), VisibilityError> {
    let mut parameters = Vec::new();
    let query = compile_set_with_parameters(set, direct, &mut parameters)?;
    Ok((query, parameters))
}

fn compile_set_with_parameters(
    set: &VisibilitySet,
    direct: &BTreeMap<String, String>,
    parameters: &mut Vec<Value>,
) -> Result<String, VisibilityError> {
    let base_alias = if set.alias.is_empty() {
        set.table.as_str()
    } else {
        set.alias.as_str()
    };
    let mut aliases = BTreeMap::from([(base_alias.to_owned(), "v0".to_owned())]);
    let mut from = format!("{} AS v0", quote(&set.table)?);
    let mut previous = base_alias.to_owned();
    for (index, join) in set.joins.iter().enumerate() {
        let logical = if join.alias.is_empty() {
            &join.table
        } else {
            &join.alias
        };
        let left_logical = if join.left_alias.is_empty() {
            &previous
        } else {
            &join.left_alias
        };
        let left_alias = aliases.get(left_logical).ok_or_else(|| {
            VisibilityError::Invalid(format!("unknown left alias {left_logical:?}"))
        })?;
        let sql_alias = format!("v{}", index + 1);
        from.push_str(&format!(
            " JOIN {} AS {sql_alias} ON {left_alias}.{} = {sql_alias}.{}",
            quote(&join.table)?,
            quote(&join.left_column)?,
            quote(&join.right_column)?
        ));
        aliases.insert(logical.clone(), sql_alias);
        previous = logical.clone();
    }
    let select_from = if set.select_from.is_empty() {
        base_alias
    } else {
        &set.select_from
    };
    let selected_alias = aliases
        .get(select_from)
        .ok_or_else(|| VisibilityError::Invalid(format!("unknown select alias {select_from:?}")))?;
    let mut query = format!(
        "SELECT DISTINCT {selected_alias}.{}::text FROM {from}",
        quote(&set.select)?
    );
    let mut predicates = Vec::new();
    for constraint in &set.constraints {
        context_key(&constraint.context)?;
        let alias = if constraint.table.is_empty() {
            "v0"
        } else {
            aliases
                .get(&constraint.table)
                .ok_or_else(|| {
                    VisibilityError::Invalid(format!(
                        "unknown constraint alias {:?}",
                        constraint.table
                    ))
                })?
                .as_str()
        };
        parameters.push(Value::String(direct[&constraint.context].clone()));
        predicates.push(format!(
            "{alias}.{} = ${}",
            quote(&constraint.column)?,
            parameters.len()
        ));
    }
    if !predicates.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&predicates.join(" AND "));
    }
    Ok(query)
}

fn match_expression(
    expression: &VisibilityExpression,
    resolved: &ResolvedVisibility,
    row: &serde_json::Map<String, Value>,
) -> bool {
    match expression.operator.as_str() {
        "public" => true,
        "permission" => truthy(resolved.permissions.get(&expression.value)),
        "role" => resolved.role == expression.value,
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

fn scalar(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "null".to_owned(),
        Some(Value::String(value)) => value.clone(),
        Some(value) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn visibility_fingerprint(
    plan: &VisibilityPlan,
    direct: &BTreeMap<String, String>,
    role: &str,
    permissions: &Value,
    sets: &BTreeMap<String, BTreeSet<String>>,
) -> String {
    let payload = serde_json::json!({
        "plan": plan,
        "direct": direct,
        "role": role,
        "permissions": permissions,
        "sets": sets,
    });
    Sha256::digest(serde_json::to_vec(&payload).unwrap_or_default())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn context_key(value: &str) -> Result<(), VisibilityError> {
    if matches!(value, "account.id" | "member.id" | "tenant.id") {
        Ok(())
    } else {
        Err(VisibilityError::Invalid(format!(
            "unsupported context {value:?}"
        )))
    }
}

fn identifier(value: &str) -> Result<&str, VisibilityError> {
    if !value.is_empty()
        && value.len() <= 63
        && value.chars().enumerate().all(|(index, character)| {
            character.is_ascii_alphabetic()
                || character == '_'
                || (index > 0 && character.is_ascii_digit())
        })
    {
        Ok(value)
    } else {
        Err(VisibilityError::Invalid(format!(
            "invalid SQL identifier {value:?}"
        )))
    }
}

fn quote(value: &str) -> Result<String, VisibilityError> {
    identifier(value).map(|value| format!("\"{value}\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_join_requires_aliases_and_compiles_both_occurrences() {
        let set = VisibilitySet {
            table: "memberTeams".to_owned(),
            alias: "viewerTeams".to_owned(),
            select: "memberId".to_owned(),
            select_from: "peerTeams".to_owned(),
            joins: vec![VisibilityJoin {
                table: "memberTeams".to_owned(),
                alias: "peerTeams".to_owned(),
                left_alias: "viewerTeams".to_owned(),
                left_column: "teamId".to_owned(),
                right_column: "teamId".to_owned(),
            }],
            constraints: vec![VisibilityConstraint {
                table: "viewerTeams".to_owned(),
                column: "memberId".to_owned(),
                context: "member.id".to_owned(),
            }],
        };
        validate_set(&set).unwrap();
        let (sql, parameters) = compile_set(
            &set,
            &BTreeMap::from([("member.id".to_owned(), "member-1".to_owned())]),
        )
        .unwrap();
        assert!(sql.contains("\"memberTeams\" AS v0 JOIN \"memberTeams\" AS v1"));
        assert!(sql.contains("v0.\"teamId\" = v1.\"teamId\""));
        assert_eq!(parameters, vec![Value::String("member-1".to_owned())]);
    }
}
