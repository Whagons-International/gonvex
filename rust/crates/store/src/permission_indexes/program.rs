//! Compiler-owned normalized programs maintained on the SAME writer before
//! publication. These are backend typed data, never application SQL/callbacks.
//! Source dirty guards run immediately, so in-transaction reads fail closed
//! until maintenance; ordinary v2 reads/action authorization remain untouched.
use super::{decision::Builder, invalid};
use crate::{DatabaseError, TenantTransaction};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Context {
    Member,
    Account,
    Tenant,
}
impl Context {
    pub fn namespace(&self) -> &'static str {
        match self {
            Self::Member => "context.member.id",
            Self::Account => "context.account.id",
            Self::Tenant => "context.tenant.id",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Policy {
    Admitted,
    RowContext {
        column: String,
        context: Context,
    },
    /// None denotes an explicit null literal, never an absent expression.
    RowLiteral {
        column: String,
        value: Option<String>,
    },
    Permission {
        name: String,
    },
    Role {
        name: String,
    },
    All {
        children: Vec<Policy>,
    },
    Any {
        children: Vec<Policy>,
    },
    Not {
        child: Box<Policy>,
    },
}
impl Policy {
    fn nodes(&self) -> Vec<&Self> {
        let mut out = Vec::new();
        let mut pending = vec![self];
        while let Some(p) = pending.pop() {
            out.push(p);
            match p {
                Self::All { children } | Self::Any { children } => pending.extend(children),
                Self::Not { child } => pending.push(child),
                _ => {}
            }
        }
        out
    }
    pub fn contexts(&self) -> Vec<(&str, &Context)> {
        self.nodes()
            .into_iter()
            .filter_map(|p| match p {
                Self::RowContext { column, context } => Some((column.as_str(), context)),
                _ => None,
            })
            .collect()
    }
    pub fn permissions(&self) -> BTreeSet<&str> {
        self.nodes()
            .into_iter()
            .filter_map(|p| match p {
                Self::Permission { name } => Some(name.as_str()),
                _ => None,
            })
            .collect()
    }
    pub fn roles(&self) -> BTreeSet<&str> {
        self.nodes()
            .into_iter()
            .filter_map(|p| match p {
                Self::Role { name } => Some(name.as_str()),
                _ => None,
            })
            .collect()
    }
    pub fn literals(&self) -> Vec<(&str, &Option<String>)> {
        self.nodes()
            .into_iter()
            .filter_map(|p| match p {
                Self::RowLiteral { column, value } => Some((column.as_str(), value)),
                _ => None,
            })
            .collect()
    }
}
/// Filled only by the deterministic runtime compiler after complete loaded
/// policy/source/projection/schema comparison. No eligibility bool is stored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Program {
    pub format: u64,
    pub table: String,
    pub key: String,
    pub tenant: String,
    pub policy: Policy,
    pub row_schema: String,
    pub member_schema: String,
    pub authority: Vec<u8>,
    pub projection_inputs: Vec<String>,
}
const ROW_SOURCE: &str = "compiled.rows";
const PRINCIPAL_SOURCE: &str = "compiled.principals";
fn q(s: &str) -> Result<String, DatabaseError> {
    if s.is_empty()
        || s.len() > 63
        || !s
            .bytes()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphabetic() || c == b'_' || (i > 0 && c.is_ascii_digit()))
    {
        return Err(invalid("invalid compiled permission identifier"));
    }
    Ok(format!("\"{s}\""))
}
pub(super) fn quoted_column(s: &str) -> Result<String, DatabaseError> {
    q(s)
}
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
async fn ddl(c: &mut SqliteConnection, table: &str) -> Result<String, DatabaseError> {
    sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1")
        .bind(table)
        .fetch_optional(c)
        .await?
        .ok_or_else(|| invalid("compiled permission source missing"))
}
impl Program {
    pub fn validate(&self) -> Result<(), DatabaseError> {
        if self.format != 1 || self.tenant.is_empty() || self.authority.is_empty() {
            return Err(invalid("incomplete compiled permission program"));
        }
        q(&self.table)?;
        q(&self.key)?;
        for column in &self.projection_inputs {
            q(column)?;
        }
        if self.table.starts_with("_gonvex_") {
            return Err(invalid("invalid compiled source table"));
        }
        for (column, _) in self.policy.contexts() {
            q(column)?;
        }
        for (column, _) in self.policy.literals() {
            q(column)?;
        }
        let mut pending = vec![(&self.policy, 0)];
        let mut count = 0;
        while let Some((p, depth)) = pending.pop() {
            count += 1;
            if depth > 64 || count > 8192 {
                return Err(invalid("compiled policy construction budget"));
            }
            match p {
                Policy::All { children } | Policy::Any { children } => {
                    pending.extend(children.iter().map(|c| (c, depth + 1)))
                }
                Policy::Not { child } => pending.push((child, depth + 1)),
                Policy::Permission { name } | Policy::Role { name }
                    if name.is_empty() || name.len() > 1024 =>
                {
                    return Err(invalid("invalid principal policy token"))
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn columns(&self) -> Vec<String> {
        let mut columns = self.projection_inputs.clone();
        if !columns.contains(&self.key) {
            columns.push(self.key.clone());
        }
        for (column, _) in self.policy.contexts() {
            if !columns.iter().any(|c| c == column) {
                columns.push(column.to_owned());
            }
        }
        for (column, _) in self.policy.literals() {
            if !columns.iter().any(|c| c == column) {
                columns.push(column.to_owned());
            }
        }
        columns
    }
    async fn domain(&self, c: &mut SqliteConnection) -> Result<bool, DatabaseError> {
        self.validate()?;
        if ddl(c, &self.table).await? != self.row_schema
            || ddl(c, "members").await? != self.member_schema
        {
            return Ok(false);
        }
        let table = q(&self.table)?;
        let key = q(&self.key)?;
        if sqlx::query_scalar::<_,i64>(&format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE typeof({key})<>'text' OR {key}='' OR instr({key},char(0))<>0)" )).fetch_one(&mut *c).await?!=0{return Ok(false);}
        for (column, context) in self.policy.contexts() {
            let col = q(column)?;
            if sqlx::query_scalar::<_,i64>(&format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {col} IS NOT NULL AND typeof({col})<>'text')")).fetch_one(&mut *c).await?!=0{return Ok(false);}
            // Existing interpreted eqContext scalar(NULL)=="null" disagrees
            // with SQL NULL="null". This domain is NOT silently normalized.
            let null_context=match context{Context::Member=>"SELECT EXISTS(SELECT 1 FROM members WHERE status='active' AND id='null')",Context::Account=>"SELECT EXISTS(SELECT 1 FROM members WHERE status='active' AND account_id='null')",Context::Tenant=>if self.tenant=="null"{"SELECT 1"}else{"SELECT 0"}};
            if sqlx::query_scalar::<_, i64>(null_context)
                .fetch_one(&mut *c)
                .await?
                != 0
            {
                return Ok(false);
            }
        }
        if !self.policy.literals().is_empty() {
            let columns = crate::rows::table_columns(c, &self.table).await?;
            for (column, value) in self.policy.literals() {
                let Some(kind) = columns.iter().find(|c| c.name == column).map(|c| c.kind()) else {
                    return Ok(false);
                };
                let sql = super::literal::sql(column, value)?;
                let memory = super::literal::memory(column, value, kind)?;
                let mismatch=format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE coalesce(({sql}),0)<>coalesce(({memory}),0))");
                if sqlx::query_scalar::<_, i64>(&mismatch)
                    .fetch_one(&mut *c)
                    .await?
                    != 0
                {
                    return Ok(false);
                }
            }
        }
        if !self.policy.roles().is_empty() && sqlx::query_scalar::<_,i64>("SELECT EXISTS(SELECT 1 FROM members WHERE status='active' AND typeof(role)<>'text')").fetch_one(&mut *c).await?!=0 {return Ok(false);}
        if !self.policy.permissions().is_empty() {
            // JSON validity/object shape/duplicate keys are part of the proof.
            // SQLite json_each takes the first duplicate; serde_json takes the
            // last. BOOLEAN/JSON declarations alone prove none of these facts.
            if sqlx::query_scalar::<_,i64>("SELECT EXISTS(SELECT 1 FROM members WHERE status='active' AND (permissions IS NULL OR NOT json_valid(permissions)))").fetch_one(&mut *c).await?!=0 {return Ok(false);}
            if sqlx::query_scalar::<_,i64>("SELECT EXISTS(SELECT 1 FROM members WHERE status='active' AND json_type(permissions)<>'object')").fetch_one(&mut *c).await?!=0 {return Ok(false);}
            for name in self.policy.permissions() {
                let bad:i64=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM members m WHERE status='active' AND ((SELECT count(*) FROM json_each(m.permissions) WHERE key=?1)>1 OR EXISTS(SELECT 1 FROM json_each(m.permissions) j WHERE j.key=?1 AND NOT (j.type IN ('true','false','null','array','object') OR (j.type='integer' AND j.value IN (0,1)) OR (j.type='real' AND j.value=0) OR (j.type='text' AND (lower(j.value) NOT IN ('true','1') OR j.value IN ('true','1')))))))").bind(name).fetch_one(&mut *c).await?;
                if bad != 0 {
                    return Ok(false);
                }
            }
        }
        Ok(sqlx::query_scalar::<_,i64>("SELECT EXISTS(SELECT 1 FROM members WHERE status='active' AND (typeof(id)<>'text' OR id='' OR instr(id,char(0))<>0 OR typeof(account_id)<>'text' OR account_id='' OR instr(account_id,char(0))<>0)) OR EXISTS(SELECT account_id FROM members WHERE status='active' GROUP BY account_id HAVING count(*)>1)").fetch_one(c).await?==0)
    }
}
fn watch_sql(
    scope: i64,
    source: &str,
    table: &str,
    key: &str,
    columns: &[String],
) -> Result<Vec<(String, String)>, DatabaseError> {
    let table = q(table)?;
    let key = q(key)?;
    let source_literal = lit(source);
    ["INSERT","UPDATE","DELETE"].into_iter().map(|op|{
        let old=op!="INSERT";let new=op!="DELETE";
        let key_expr=|alias:&str|format!("CASE WHEN typeof({alias}.{key})='text' THEN {alias}.{key} ELSE NULL END");
        let name=format!("_gonvex_permission_program_{scope}_{source}_{}",op.to_ascii_lowercase()).replace('.',"_");
        let changed=if op=="UPDATE" {format!(" WHEN {}",columns.iter().map(|col|q(col).map(|col|format!("OLD.{col} IS NOT NEW.{col}"))).collect::<Result<Vec<_>,_>>()?.join(" OR "))}else{String::new()};
        let statement=format!("CREATE TRIGGER {} AFTER {op} ON {table}{changed} BEGIN UPDATE _gonvex_permission_scopes SET phase='building',change_serial=change_serial+1 WHERE scope_id={scope}; INSERT INTO _gonvex_permission_program_changes(scope_id,source_id,table_name,old_key,new_key,old_image,new_image) VALUES({scope},{source_literal},{},{},{},{},{}); END",q(&name)?,lit(table.trim_matches('"')),if old{key_expr("OLD")}else{"NULL".into()},if new{key_expr("NEW")}else{"NULL".into()},"NULL","NULL");
        Ok((name,statement))
    }).collect()
}
async fn watches(
    c: &mut SqliteConnection,
    scope: i64,
    p: &Program,
    install: bool,
) -> Result<(), DatabaseError> {
    let mut principal_columns = vec!["id".into(), "account_id".into(), "status".into()];
    if !p.policy.roles().is_empty() {
        principal_columns.push("role".into());
    }
    if !p.policy.permissions().is_empty() {
        principal_columns.push("permissions".into());
    }
    let sources = vec![
        (
            "rows".to_owned(),
            p.table.clone(),
            p.key.clone(),
            p.columns(),
        ),
        (
            "principals".to_owned(),
            "members".to_owned(),
            "id".to_owned(),
            principal_columns,
        ),
    ];
    for (source, table, key, cols) in sources {
        for (name, statement) in watch_sql(scope, &source, &table, &key, &cols)? {
            let actual: Option<String> = sqlx::query_scalar(
                "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?1",
            )
            .bind(name)
            .fetch_optional(&mut *c)
            .await?;
            match actual {
                Some(actual) if actual == statement => {}
                None if install => {
                    sqlx::query(&statement).execute(&mut *c).await?;
                }
                _ => return Err(invalid("compiled permission watch missing/changed")),
            }
        }
        let names = crate::feed::capture_trigger_names(&table);
        let columns = crate::rows::table_columns(c, &table).await?;
        let batch = crate::feed::capture_triggers_sql(&table, &key, &columns);
        let expected = batch
            .split("CREATE TRIGGER ")
            .skip(1)
            .map(|sql| format!("CREATE TRIGGER {}", sql.trim().trim_end_matches(';')))
            .collect::<Vec<_>>();
        if expected.len() != 3 {
            return Err(invalid("invalid native capture compiler"));
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name IN (?1,?2,?3)",
        )
        .bind(&names[0])
        .bind(&names[1])
        .bind(&names[2])
        .fetch_one(&mut *c)
        .await?;
        if count == 0 && install {
            sqlx::Executor::execute(&mut *c, batch.as_str()).await?;
        } else if count != 3 {
            return Err(invalid("incomplete source feed capture"));
        }
        for (name, expected) in names.iter().zip(expected) {
            let actual: Option<String> = sqlx::query_scalar(
                "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?1",
            )
            .bind(name)
            .fetch_optional(&mut *c)
            .await?;
            if actual.as_ref() != Some(&expected) {
                return Err(invalid(
                    "source feed capture differs from native writer contract",
                ));
            }
        }
    }
    Ok(())
}
pub async fn install(
    tx: &mut TenantTransaction,
    scope: i64,
    p: &Program,
) -> Result<(), DatabaseError> {
    let result = install_inner(tx, scope, p).await;
    if result.is_err() {
        tx.mark_failed("compiled permission index installation failed");
    }
    result
}
async fn install_inner(
    tx: &mut TenantTransaction,
    scope: i64,
    p: &Program,
) -> Result<(), DatabaseError> {
    super::writer(tx).await?;
    p.validate()?;
    let state = super::state(tx.transaction(), scope).await?;
    if state.table != p.table {
        return Err(invalid("compiled program scope/table mismatch"));
    }
    super::register_sources_inner(
        tx,
        scope,
        &[
            ("row".into(), ROW_SOURCE.into()),
            ("grant".into(), PRINCIPAL_SOURCE.into()),
        ],
    )
    .await?;
    let bytes = serde_json::to_vec(p).map_err(|e| invalid(&e.to_string()))?;
    sqlx::query("INSERT INTO _gonvex_permission_programs VALUES(?1,?2,?3,?4) ON CONFLICT(scope_id) DO UPDATE SET generation=excluded.generation,program=excluded.program,canonical_authority=excluded.canonical_authority").bind(scope).bind(state.generation).bind(bytes).bind(&p.authority).execute(&mut **tx.transaction()).await?;
    watches(tx.transaction(), scope, p, true).await?;
    crate::capture::install(tx.transaction()).await?;
    if !p.domain(tx.transaction()).await? {
        sqlx::query("UPDATE _gonvex_permission_scopes SET phase='failed' WHERE scope_id=?1")
            .bind(scope)
            .execute(&mut **tx.transaction())
            .await?;
        return Ok(());
    }
    if !rebuild(tx.transaction(), scope, state.generation, p, None, None).await? {
        return Ok(());
    }
    super::seal_inner(tx, scope).await
}
/// Read-only verification used by a sealed runtime certificate in the same TX.
pub async fn verify(
    c: &mut SqliteConnection,
    scope: i64,
    p: &Program,
) -> Result<(), DatabaseError> {
    super::require_local(c).await?;
    p.validate()?;
    let row=sqlx::query("SELECT p.program,p.canonical_authority FROM _gonvex_permission_programs p JOIN _gonvex_permission_scopes s ON s.scope_id=p.scope_id AND s.generation=p.generation WHERE p.scope_id=?1").bind(scope).fetch_one(&mut *c).await?;
    let bytes: Vec<u8> = row.try_get(0)?;
    let authority: Vec<u8> = row.try_get(1)?;
    if bytes != serde_json::to_vec(p).map_err(|e| invalid(&e.to_string()))?
        || authority != p.authority
    {
        return Err(invalid(
            "compiled program differs from loaded source authority",
        ));
    }
    let sources=sqlx::query("SELECT kind,source_id FROM _gonvex_permission_sources WHERE scope_id=?1 AND generation=(SELECT generation FROM _gonvex_permission_scopes WHERE scope_id=?1) ORDER BY kind,source_id").bind(scope).fetch_all(&mut *c).await?.into_iter().map(|r|Ok((r.try_get::<String,_>(0)?,r.try_get::<String,_>(1)?))).collect::<Result<Vec<_>,sqlx::Error>>()?;
    if sources
        != vec![
            ("grant".into(), PRINCIPAL_SOURCE.into()),
            ("row".into(), ROW_SOURCE.into()),
        ]
    {
        return Err(invalid("compiled source declarations missing or changed"));
    }
    watches(c, scope, p, false).await
}
async fn intern(
    c: &mut SqliteConnection,
    scope: i64,
    generation: i64,
    key: &[u8],
) -> Result<i64, DatabaseError> {
    Ok(sqlx::query_scalar("INSERT INTO _gonvex_permission_atoms(scope_id,generation,atom_id,canonical_key) SELECT ?1,?2,coalesce(max(atom_id),0)+1,?3 FROM _gonvex_permission_atoms WHERE scope_id=?1 AND generation=?2 ON CONFLICT(scope_id,generation,canonical_key) DO UPDATE SET canonical_key=excluded.canonical_key RETURNING atom_id").bind(scope).bind(generation).bind(key).fetch_one(c).await?)
}
fn atom_key(namespace: &str, resource: &str) -> Vec<u8> {
    let mut b = Vec::new();
    for value in [namespace, resource] {
        b.extend_from_slice(&(value.len() as u64).to_be_bytes());
        b.extend_from_slice(value.as_bytes());
    }
    b.push(0);
    b
}
async fn rebuild(
    c: &mut SqliteConnection,
    scope: i64,
    generation: i64,
    p: &Program,
    row_keys: Option<&BTreeSet<String>>,
    principal_keys: Option<&BTreeSet<String>>,
) -> Result<bool, DatabaseError> {
    sqlx::query("SAVEPOINT gonvex_permission_build")
        .execute(&mut *c)
        .await?;
    let outcome = rebuild_inner(c, scope, generation, p, row_keys, principal_keys).await;
    if !matches!(outcome, Ok(true)) {
        sqlx::query("ROLLBACK TO gonvex_permission_build")
            .execute(&mut *c)
            .await?;
    }
    sqlx::query("RELEASE gonvex_permission_build")
        .execute(&mut *c)
        .await?;
    if matches!(outcome, Ok(false)) {
        // Resource expansion makes the NEW provider ineligible. The original
        // v2 source mutation remains committed and is read by its normal path.
        sqlx::query("UPDATE _gonvex_permission_scopes SET phase='failed' WHERE scope_id=?1")
            .bind(scope)
            .execute(c)
            .await?;
    }
    outcome
}
async fn rebuild_inner(
    c: &mut SqliteConnection,
    scope: i64,
    generation: i64,
    p: &Program,
    row_keys: Option<&BTreeSet<String>>,
    principal_keys: Option<&BTreeSet<String>>,
) -> Result<bool, DatabaseError> {
    let mut builder = Builder::load(c, scope, generation).await?;
    let filter = |keys: Option<&BTreeSet<String>>, column: &str| match keys {
        Some(keys) => format!(
            " WHERE {column} IN (SELECT value FROM json_each({}))",
            lit(&serde_json::to_string(keys).expect("keys"))
        ),
        None => String::new(),
    };
    let mut select = vec![format!("{} AS row_key", q(&p.key)?)];
    let contexts = p
        .policy
        .contexts()
        .into_iter()
        .map(|(c, _)| c)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for (i, column) in contexts.iter().enumerate() {
        select.push(format!("{} AS __permission_context_{i}", q(column)?));
    }
    for (i, (column, value)) in p.policy.literals().iter().enumerate() {
        select.push(format!(
            "CASE WHEN ({}) THEN 1 ELSE 0 END AS __permission_literal_{i}",
            super::literal::sql(column, value)?
        ));
    }
    let rows = sqlx::query(&format!(
        "SELECT {} FROM {}{}",
        select.join(","),
        q(&p.table)?,
        filter(row_keys, &q(&p.key)?)
    ))
    .fetch_all(&mut *c)
    .await?;
    let affected:Vec<String>=match row_keys {Some(keys)=>keys.iter().cloned().collect(),None=>sqlx::query_scalar("SELECT row_key FROM _gonvex_permission_program_rows WHERE scope_id=?1 AND generation=?2").bind(scope).bind(generation).fetch_all(&mut *c).await?};
    for key in &affected {
        sqlx::query("DELETE FROM _gonvex_permission_program_rows WHERE scope_id=?1 AND generation=?2 AND row_key=?3").bind(scope).bind(generation).bind(key).execute(&mut *c).await?;
        sqlx::query("DELETE FROM _gonvex_permission_row_supports WHERE scope_id=?1 AND generation=?2 AND source_id=?3 AND row_key=?4").bind(scope).bind(generation).bind(ROW_SOURCE).bind(key).execute(&mut *c).await?;
    }
    for row in rows {
        let key: String = row.try_get("row_key")?;
        let mut pending = vec![(&p.policy, false)];
        let mut values = Vec::new();
        while let Some((policy, visited)) = pending.pop() {
            let children = match policy {
                Policy::All { children } | Policy::Any { children } => children.as_slice(),
                _ => &[],
            };
            if !visited && (!children.is_empty() || matches!(policy, Policy::Not { .. })) {
                pending.push((policy, true));
                if let Policy::Not { child } = policy {
                    pending.push((child, false));
                } else {
                    pending.extend(children.iter().rev().map(|c| (c, false)));
                }
                continue;
            }
            let value = match policy {
                Policy::Admitted => 1,
                Policy::RowContext { column, context } => {
                    match row.try_get::<Option<String>, _>(
                        format!(
                            "__permission_context_{}",
                            contexts
                                .iter()
                                .position(|c| *c == column)
                                .expect("declared context")
                        )
                        .as_str(),
                    )? {
                        None => 0,
                        Some(value) => builder.atom(
                            intern(c, scope, generation, &atom_key(context.namespace(), &value))
                                .await?,
                        ),
                    }
                }
                Policy::RowLiteral { column, value } => {
                    let i = p
                        .policy
                        .literals()
                        .iter()
                        .position(|(c, v)| *c == column && *v == value)
                        .expect("declared literal");
                    row.try_get::<i64, _>(format!("__permission_literal_{i}").as_str())?
                }
                Policy::Permission { name } => builder.atom(
                    intern(
                        c,
                        scope,
                        generation,
                        &atom_key("principal.permission", name),
                    )
                    .await?,
                ),
                Policy::Role { name } => builder
                    .atom(intern(c, scope, generation, &atom_key("principal.role", name)).await?),
                Policy::All { .. } | Policy::Any { .. } => {
                    let and = matches!(policy, Policy::All { .. });
                    let inputs = values.split_off(values.len() - children.len());
                    inputs
                        .into_iter()
                        .fold(i64::from(and), |a, b| builder.combine(and, a, b))
                }
                Policy::Not { .. } => {
                    builder.negate(values.pop().expect("validated expression operand"))
                }
            };
            values.push(value);
        }
        if builder.exhausted() {
            return Ok(false);
        }
        let root = values.pop().expect("validated policy root");
        if !builder.register_root(c, scope, generation, root).await? {
            return Ok(false);
        }
        sqlx::query("INSERT INTO _gonvex_permission_program_rows VALUES(?1,?2,?3,?4)")
            .bind(scope)
            .bind(generation)
            .bind(&key)
            .bind(root)
            .execute(&mut *c)
            .await?;
        sqlx::query("INSERT INTO _gonvex_permission_row_supports(scope_id,generation,source_id,table_id,universe_id,row_key) VALUES(?1,?2,?3,?4,?4,?5) ON CONFLICT DO NOTHING").bind(scope).bind(generation).bind(ROW_SOURCE).bind(&p.table).bind(key).execute(&mut *c).await?;
    }
    builder.persist(c, scope, generation).await?;
    // These are principal inputs only, not per-user scans of resource rows.
    match principal_keys {
        Some(keys) => {
            for key in keys {
                sqlx::query("DELETE FROM _gonvex_permission_supports WHERE scope_id=?1 AND generation=?2 AND source_id=?3 AND target_id=?4").bind(scope).bind(generation).bind(PRINCIPAL_SOURCE).bind(key).execute(&mut *c).await?;
            }
        }
        None => {
            sqlx::query("DELETE FROM _gonvex_permission_supports WHERE scope_id=?1 AND generation=?2 AND source_id=?3").bind(scope).bind(generation).bind(PRINCIPAL_SOURCE).execute(&mut *c).await?;
        }
    }
    match principal_keys {
        Some(keys) => {
            for key in keys {
                sqlx::query("DELETE FROM _gonvex_permission_program_principals WHERE scope_id=?1 AND generation=?2 AND principal=?3").bind(scope).bind(generation).bind(key).execute(&mut *c).await?;
            }
        }
        None => {
            sqlx::query("DELETE FROM _gonvex_permission_program_principals WHERE scope_id=?1 AND generation=?2").bind(scope).bind(generation).execute(&mut *c).await?;
        }
    }
    let predicate = filter(principal_keys, "id");
    let predicate = if predicate.is_empty() {
        " WHERE status='active'".into()
    } else {
        format!("{predicate} AND status='active'")
    };
    let extras = format!(
        "{}{}",
        if p.policy.roles().is_empty() {
            ""
        } else {
            ",role"
        },
        if p.policy.permissions().is_empty() {
            ""
        } else {
            ",permissions"
        }
    );
    for row in sqlx::query(&format!(
        "SELECT id,account_id{extras} FROM members{predicate}"
    ))
    .fetch_all(&mut *c)
    .await?
    {
        let id: String = row.try_get(0)?;
        sqlx::query("INSERT INTO _gonvex_permission_program_principals VALUES(?1,?2,?3)")
            .bind(scope)
            .bind(generation)
            .bind(&id)
            .execute(&mut *c)
            .await?;
        let mut atoms = BTreeSet::new();
        for (_, context) in p.policy.contexts() {
            let value = match context {
                Context::Member => id.clone(),
                Context::Account => row.try_get(1)?,
                Context::Tenant => p.tenant.clone(),
            };
            atoms.insert(
                intern(c, scope, generation, &atom_key(context.namespace(), &value)).await?,
            );
        }
        if !p.policy.roles().is_empty() {
            let role: String = row.try_get("role")?;
            atoms.insert(intern(c, scope, generation, &atom_key("principal.role", &role)).await?);
        }
        if !p.policy.permissions().is_empty() {
            let raw: String = row.try_get("permissions")?;
            let permissions: serde_json::Value =
                serde_json::from_str(&raw).map_err(|e| invalid(&e.to_string()))?;
            for name in p.policy.permissions() {
                let yes = match permissions.get(name) {
                    Some(serde_json::Value::Bool(v)) => *v,
                    Some(serde_json::Value::Number(n)) => n.as_f64().is_some_and(|n| n != 0.0),
                    Some(serde_json::Value::String(s)) => s == "true" || s == "1",
                    _ => false,
                };
                if yes {
                    atoms.insert(
                        intern(
                            c,
                            scope,
                            generation,
                            &atom_key("principal.permission", name),
                        )
                        .await?,
                    );
                }
            }
        }
        for atom in atoms {
            sqlx::query("INSERT INTO _gonvex_permission_supports(scope_id,generation,source_id,target_kind,target_id,atom_id) VALUES(?1,?2,?3,'member',?4,?5) ON CONFLICT DO NOTHING").bind(scope).bind(generation).bind(PRINCIPAL_SOURCE).bind(&id).bind(atom).execute(&mut *c).await?;
        }
    }
    sqlx::query("UPDATE _gonvex_permission_sources SET complete=1 WHERE scope_id=?1 AND generation=?2 AND source_id IN (?3,?4)").bind(scope).bind(generation).bind(ROW_SOURCE).bind(PRINCIPAL_SOURCE).execute(c).await?;
    Ok(true)
}
/// Called after the feed assigns this transaction's clock, before COMMIT and
/// publication. Source dirty guards already prevent read-your-writes staleness.
pub(crate) async fn maintain(c: &mut SqliteConnection) -> Result<(), DatabaseError> {
    let installed:i64=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_permission_programs')").fetch_one(&mut *c).await?;
    if installed == 0 {
        return Ok(());
    }
    let scopes: Vec<i64> =
        sqlx::query_scalar("SELECT DISTINCT scope_id FROM _gonvex_permission_program_changes")
            .fetch_all(&mut *c)
            .await?;
    for scope in scopes {
        let row = sqlx::query(
            "SELECT generation,program FROM _gonvex_permission_programs WHERE scope_id=?1",
        )
        .bind(scope)
        .fetch_one(&mut *c)
        .await?;
        let mut generation: i64 = row.try_get(0)?;
        let bytes: Vec<u8> = row.try_get(1)?;
        let p: Program = serde_json::from_slice(&bytes).map_err(|e| invalid(&e.to_string()))?;
        verify(c, scope, &p).await?;
        // Ambiguity is an eligibility failure, NOT a source-write denial.
        // The original v2 writer commits; new reads use its normal fallback.
        if !p.domain(c).await? {
            sqlx::query("UPDATE _gonvex_permission_scopes SET phase='failed' WHERE scope_id=?1")
                .bind(scope)
                .execute(&mut *c)
                .await?;
            continue;
        }
        let changes=sqlx::query("SELECT source_id,old_key,new_key FROM _gonvex_permission_program_changes WHERE scope_id=?1 ORDER BY event_id").bind(scope).fetch_all(&mut *c).await?;
        let mut rows = BTreeSet::new();
        let mut principals = BTreeSet::new();
        for row in changes {
            let source: String = row.try_get(0)?;
            let target = if source == "rows" {
                &mut rows
            } else {
                &mut principals
            };
            for i in [1, 2] {
                if let Some(key) = row.try_get::<Option<String>, _>(i)? {
                    target.insert(key);
                }
            }
        }
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM _gonvex_sync_clock WHERE singleton=1")
                .fetch_one(&mut *c)
                .await?;
        let covered: i64 = sqlx::query_scalar(
            "SELECT covered_revision FROM _gonvex_permission_scopes WHERE scope_id=?1",
        )
        .bind(scope)
        .fetch_one(&mut *c)
        .await?;
        let initialized:i64=sqlx::query_scalar("SELECT count(*) FROM _gonvex_permission_sources WHERE scope_id=?1 AND generation=?2 AND complete=1 AND ((kind='row' AND source_id=?3) OR (kind='grant' AND source_id=?4))").bind(scope).bind(generation).bind(ROW_SOURCE).bind(PRINCIPAL_SOURCE).fetch_one(&mut *c).await?;
        if initialized != 2 || covered < revision - 1 {
            generation = generation
                .checked_add(1)
                .ok_or_else(|| invalid("index generation overflow"))?;
            sqlx::query("UPDATE _gonvex_permission_scopes SET generation=?2 WHERE scope_id=?1")
                .bind(scope)
                .bind(generation)
                .execute(&mut *c)
                .await?;
            sqlx::query("UPDATE _gonvex_permission_programs SET generation=?2 WHERE scope_id=?1")
                .bind(scope)
                .bind(generation)
                .execute(&mut *c)
                .await?;
            for (kind, source) in [("row", ROW_SOURCE), ("grant", PRINCIPAL_SOURCE)] {
                sqlx::query("INSERT INTO _gonvex_permission_sources(scope_id,generation,kind,source_id) VALUES(?1,?2,?3,?4)").bind(scope).bind(generation).bind(kind).bind(source).execute(&mut *c).await?;
            }
            if !rebuild(c, scope, generation, &p, None, None).await? {
                continue;
            }
            sqlx::query("INSERT INTO _gonvex_permission_program_resets VALUES(?1,?2,?3)")
                .bind(scope)
                .bind(generation)
                .bind(revision)
                .execute(&mut *c)
                .await?;
            sqlx::query("UPDATE _gonvex_permission_scopes SET phase='pending',covered_revision=?2 WHERE scope_id=?1").bind(scope).bind(revision).execute(&mut *c).await?;
            sqlx::query("DELETE FROM _gonvex_permission_program_changes WHERE scope_id=?1")
                .bind(scope)
                .execute(&mut *c)
                .await?;
            continue;
        }
        let mut before = std::collections::BTreeMap::new();
        for principal in &principals {
            let mut atoms: BTreeSet<i64>=sqlx::query_scalar("SELECT atom_id FROM _gonvex_permission_set_atoms WHERE scope_id=?1 AND generation=?2 AND set_id=?3").bind(scope).bind(generation).bind(principal).fetch_all(&mut *c).await?.into_iter().collect();
            let admitted:i64=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_permission_program_principals WHERE scope_id=?1 AND generation=?2 AND principal=?3)").bind(scope).bind(generation).bind(principal).fetch_one(&mut *c).await?;
            if admitted != 0 {
                atoms.insert(0);
            }
            before.insert(principal.clone(), atoms);
        }
        for key in &rows {
            sqlx::query("INSERT INTO _gonvex_permission_program_row_deltas SELECT ?1,?2,?3,?4,(SELECT root_id FROM _gonvex_permission_program_rows WHERE scope_id=?1 AND generation=?2 AND row_key=?4),NULL ON CONFLICT DO NOTHING").bind(scope).bind(generation).bind(revision).bind(key).execute(&mut *c).await?;
        }
        if !rebuild(c, scope, generation, &p, Some(&rows), Some(&principals)).await? {
            continue;
        }
        for principal in &principals {
            let mut after:BTreeSet<i64>=sqlx::query_scalar("SELECT atom_id FROM _gonvex_permission_set_atoms WHERE scope_id=?1 AND generation=?2 AND set_id=?3").bind(scope).bind(generation).bind(principal).fetch_all(&mut *c).await?.into_iter().collect();
            let admitted:i64=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_permission_program_principals WHERE scope_id=?1 AND generation=?2 AND principal=?3)").bind(scope).bind(generation).bind(principal).fetch_one(&mut *c).await?;
            if admitted != 0 {
                after.insert(0);
            }
            let before = &before[principal];
            for atom in before.symmetric_difference(&after) {
                sqlx::query("INSERT INTO _gonvex_permission_program_principal_deltas VALUES(?1,?2,?3,?4,?5,?6,?7)").bind(scope).bind(generation).bind(revision).bind(principal).bind(atom).bind(i64::from(before.contains(atom))).bind(i64::from(after.contains(atom))).execute(&mut *c).await?;
            }
        }
        for key in &rows {
            sqlx::query("UPDATE _gonvex_permission_program_row_deltas SET after_root=(SELECT root_id FROM _gonvex_permission_program_rows WHERE scope_id=?1 AND generation=?2 AND row_key=?4) WHERE scope_id=?1 AND generation=?2 AND revision=?3 AND row_key=?4").bind(scope).bind(generation).bind(revision).bind(key).execute(&mut *c).await?;
        }
        sqlx::query("UPDATE _gonvex_permission_scopes SET phase='pending',covered_revision=?2 WHERE scope_id=?1").bind(scope).bind(revision).execute(&mut *c).await?;
        sqlx::query("DELETE FROM _gonvex_permission_program_changes WHERE scope_id=?1")
            .bind(scope)
            .execute(&mut *c)
            .await?;
    }
    sqlx::query("UPDATE _gonvex_permission_scopes SET covered_revision=(SELECT revision FROM _gonvex_sync_clock WHERE singleton=1) WHERE phase='ready' AND scope_id IN (SELECT scope_id FROM _gonvex_permission_programs) AND NOT EXISTS(SELECT 1 FROM _gonvex_permission_program_changes x WHERE x.scope_id=_gonvex_permission_scopes.scope_id)").execute(c).await?;
    Ok(())
}

#[allow(dead_code)]
fn future_is_send(c: &mut SqliteConnection, p: &Program) {
    fn check<T: Send>(_: T) {}
    check(maintain(c));
    check(rebuild(c, 1, 1, p, None, None));
    check(watches(c, 1, p, false));
    check(p.domain(c));
}
