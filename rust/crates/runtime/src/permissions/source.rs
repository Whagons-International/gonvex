//! First relational source adapter: plain TEXT owner rows and persisted-member
//! grants, no joins/roles/parent/JSON coercion. Explicit LOCAL rebuild only.
//! A source is replaced as a complete logical batch; this is not an incremental
//! occurrence/candidate implementation and does not publish a live feed.
use super::{
    compile::{self, Expr},
    model::Atom,
    query::Error,
};
use gonvex_store::{permission_indexes as indexes, TenantTransaction};
use serde::Deserialize;
use serde_json::Value;
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};

fn reject() -> Error {
    Error::Rejected("unsupported or invalid relational-v1 source binding")
}
fn quote(value: &str) -> Result<String, Error> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .bytes()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphabetic() || c == b'_' || (i > 0 && c.is_ascii_digit()))
    {
        return Err(reject());
    }
    Ok(format!("\"{value}\""))
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    version: u64,
    declaration: Declaration,
    dependencies: BTreeMap<String, Vec<Dependency>>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Declaration {
    version: u64,
    atoms: BTreeMap<String, Atom>,
    providers: BTreeMap<String, Provider>,
    collections: BTreeMap<String, Collection>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Provider {
    adapter: String,
    version: String,
    sources: Vec<Value>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Collection {
    pub provider: String,
    pub table: String,
    pub key: String,
    pub universe: String,
    policy: Policy,
}
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Dependency {
    source: String,
    table: String,
    key: String,
    alias: String,
    columns: Vec<String>,
    images: String,
}
#[derive(Clone, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
enum Policy {
    Deny,
    Slot {
        slot: String,
    },
    Has {
        atom: String,
    },
    All {
        children: Vec<Policy>,
    },
    Any {
        children: Vec<Policy>,
    },
    Except {
        universe: String,
        allow: Box<Policy>,
        remove: Box<Policy>,
    },
}
impl Policy {
    fn lower(&self, atoms: &BTreeMap<String, Atom>, depth: usize) -> Result<Expr, Error> {
        if depth > 64 {
            return Err(reject());
        }
        Ok(match self {
            Self::Deny => Expr::Deny,
            Self::Slot { slot } if !slot.is_empty() => Expr::SlotAny(slot.clone()),
            Self::Has { atom } => Expr::HasAtom(atoms.get(atom).ok_or_else(reject)?.clone()),
            Self::All { children } => Expr::All(
                children
                    .iter()
                    .map(|x| x.lower(atoms, depth + 1))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Any { children } => Expr::Any(
                children
                    .iter()
                    .map(|x| x.lower(atoms, depth + 1))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Except {
                universe,
                allow,
                remove,
            } => Expr::Subtract {
                universe: universe.clone(),
                allow: Box::new(allow.lower(atoms, depth + 1)?),
                remove: Box::new(remove.lower(atoms, depth + 1)?),
            },
            _ => return Err(reject()),
        })
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    table: String,
    key: String,
    #[serde(rename = "as")]
    alias: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Field {
    alias: String,
    column: String,
}
#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum Text {
    Literal(Literal),
    Field(ProjectedField),
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Literal {
    literal: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectedField {
    field: Field,
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "onNull")]
    on_null: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectedAtom {
    namespace: String,
    resource: Text,
    principal: Option<Text>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RowAtom {
    slot: String,
    atom: ProjectedAtom,
}
#[derive(Clone, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
enum Filter {
    IsNull { field: Field },
    IsNotNull { field: Field },
    TextEq { left: Text, right: Text },
}
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum Subject {
    Member { id: Text },
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grant {
    id: String,
    kind: String,
    from: Table,
    joins: Vec<Value>,
    #[serde(rename = "where")]
    filters: Vec<Filter>,
    target: Subject,
    atoms: Vec<ProjectedAtom>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Rows {
    id: String,
    kind: String,
    from: Table,
    joins: Vec<Value>,
    #[serde(rename = "where")]
    filters: Vec<Filter>,
    table: String,
    key: Text,
    universe: String,
    atoms: Vec<RowAtom>,
}
#[derive(Clone)]
pub(crate) enum Source {
    Grant(Grant),
    Rows(Rows),
}
impl Text {
    fn validate(&self, table: &Table, columns: &mut BTreeSet<String>) -> Result<(), Error> {
        if let Self::Field(f) = self {
            if f.kind != "text"
                || !matches!(f.on_null.as_str(), "omitFact" | "reject")
                || f.field.alias != table.alias
            {
                return Err(reject());
            }
            quote(&f.field.column)?;
            columns.insert(f.field.column.clone());
        }
        Ok(())
    }
    fn sql(&self, binds: &mut Vec<String>) -> Result<String, Error> {
        Ok(match self {
            Self::Literal(x) => {
                binds.push(x.literal.clone());
                format!("?{}", binds.len())
            }
            Self::Field(x) => format!("r.{}", quote(&x.field.column)?),
        })
    }
    fn read(&self, row: &sqlx::sqlite::SqliteRow) -> Result<Option<String>, Error> {
        match self {
            Self::Literal(x) => Ok(Some(x.literal.clone())),
            Self::Field(x) => {
                let value: Option<String> = row.try_get(x.field.column.as_str())?;
                if value.is_none() && x.on_null == "reject" {
                    return Err(reject());
                }
                Ok(value)
            }
        }
    }
}
impl ProjectedAtom {
    fn validate(&self, table: &Table, columns: &mut BTreeSet<String>) -> Result<(), Error> {
        if self.namespace.is_empty() {
            return Err(reject());
        }
        self.resource.validate(table, columns)?;
        if let Some(principal) = &self.principal {
            principal.validate(table, columns)?;
        }
        Ok(())
    }
    fn read(&self, row: &sqlx::sqlite::SqliteRow) -> Result<Option<Vec<u8>>, Error> {
        let Some(resource) = self.resource.read(row)? else {
            return Ok(None);
        };
        let principal = match &self.principal {
            Some(x) => {
                let Some(x) = x.read(row)? else {
                    return Ok(None);
                };
                Some(x)
            }
            None => None,
        };
        Ok(Some(
            Atom {
                namespace: self.namespace.clone(),
                resource,
                principal,
            }
            .canonical()
            .map_err(|_| reject())?,
        ))
    }
}
impl Source {
    pub fn id(&self) -> &str {
        match self {
            Self::Grant(x) => &x.id,
            Self::Rows(x) => &x.id,
        }
    }
    pub fn kind(&self) -> &str {
        match self {
            Self::Grant(_) => "grant",
            Self::Rows(_) => "row",
        }
    }
    fn table(&self) -> &Table {
        match self {
            Self::Grant(x) => &x.from,
            Self::Rows(x) => &x.from,
        }
    }
    fn filters(&self) -> &[Filter] {
        match self {
            Self::Grant(x) => &x.filters,
            Self::Rows(x) => &x.filters,
        }
    }
    pub fn source_table(&self) -> &str {
        &self.table().table
    }
    fn dependency(&self) -> Result<Dependency, Error> {
        let table = self.table();
        quote(&table.table)?;
        quote(&table.key)?;
        quote(&table.alias)?;
        let mut columns = BTreeSet::from([table.key.clone()]);
        for filter in self.filters() {
            match filter {
                Filter::IsNull { field } | Filter::IsNotNull { field } => {
                    if field.alias != table.alias {
                        return Err(reject());
                    }
                    quote(&field.column)?;
                    columns.insert(field.column.clone());
                }
                Filter::TextEq { left, right } => {
                    left.validate(table, &mut columns)?;
                    right.validate(table, &mut columns)?;
                }
            }
        }
        match self {
            Self::Grant(x) => {
                if x.kind != "grant" || !x.joins.is_empty() {
                    return Err(reject());
                }
                let Subject::Member { id } = &x.target;
                id.validate(table, &mut columns)?;
                for atom in &x.atoms {
                    atom.validate(table, &mut columns)?;
                }
            }
            Self::Rows(x) => {
                if x.kind != "row" || !x.joins.is_empty() || x.universe.is_empty() {
                    return Err(reject());
                }
                quote(&x.table)?;
                x.key.validate(table, &mut columns)?;
                for atom in &x.atoms {
                    if atom.slot.is_empty() {
                        return Err(reject());
                    }
                    atom.atom.validate(table, &mut columns)?;
                }
            }
        }
        Ok(Dependency {
            source: self.id().to_owned(),
            table: table.table.clone(),
            key: table.key.clone(),
            alias: table.alias.clone(),
            columns: columns.into_iter().collect(),
            images: "oldAndNew".into(),
        })
    }
    /// Conservative complete logical-source rebuild. SQL filters/projections,
    /// then typed fact ingestion; never an application authorization callback.
    pub async fn load(&self, tx: &mut TenantTransaction) -> Result<indexes::SourceChange, Error> {
        indexes::require_local(tx.transaction()).await?;
        let dependency = self.dependency()?;
        let schema: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name=?1")
                .bind(&dependency.table)
                .fetch_optional(&mut **tx.transaction())
                .await?
                .ok_or_else(reject)?;
        if schema.to_ascii_uppercase().contains("COLLATE")
            || schema.to_ascii_uppercase().contains("VIRTUAL")
        {
            return Err(reject());
        }
        let declared =
            gonvex_store::rows::table_columns(tx.transaction(), &dependency.table).await?;
        if declared.iter().filter(|c| c.primary_key).count() != 1
            || !declared
                .iter()
                .any(|c| c.name == dependency.key && c.primary_key)
        {
            return Err(reject());
        }
        for column in &dependency.columns {
            if !declared
                .iter()
                .any(|c| c.name == *column && c.declared.eq_ignore_ascii_case("TEXT"))
            {
                return Err(reject());
            }
        }
        let mut reject_null = Vec::new();
        for filter in self.filters() {
            if let Filter::TextEq { left, right } = filter {
                for text in [left, right] {
                    if let Text::Field(field) = text {
                        if field.on_null == "reject" {
                            reject_null.push(format!("{} IS NULL", quote(&field.field.column)?));
                        }
                    }
                }
            }
        }
        if !reject_null.is_empty() {
            let bad: i64 = sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE {})",
                quote(&dependency.table)?,
                reject_null.join(" OR ")
            ))
            .fetch_one(&mut **tx.transaction())
            .await?;
            if bad != 0 {
                return Err(reject());
            }
        }
        let invalid = dependency
            .columns
            .iter()
            .map(|c| {
                Ok(format!(
                    "({0} IS NOT NULL AND typeof({0})<>'text')",
                    quote(c)?
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?
            .join(" OR ");
        let invalid: i64 = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE {})",
            quote(&dependency.table)?,
            invalid
        ))
        .fetch_one(&mut **tx.transaction())
        .await?;
        if invalid != 0 {
            return Err(reject());
        }
        let key = quote(&dependency.key)?;
        let invalid_key:i64=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {} WHERE {key} IS NULL OR {key}='' OR instr({key},char(0))<>0)",quote(&dependency.table)?)).fetch_one(&mut **tx.transaction()).await?;
        if invalid_key != 0 {
            return Err(reject());
        }
        let mut binds = Vec::new();
        let mut predicates = Vec::new();
        for filter in self.filters() {
            predicates.push(match filter {
                Filter::IsNull { field } => format!("r.{} IS NULL", quote(&field.column)?),
                Filter::IsNotNull { field } => format!("r.{} IS NOT NULL", quote(&field.column)?),
                Filter::TextEq { left, right } => format!(
                    "{} COLLATE BINARY = {} COLLATE BINARY",
                    left.sql(&mut binds)?,
                    right.sql(&mut binds)?
                ),
            });
        }
        let sql = format!(
            "SELECT {} FROM {} r WHERE {} ORDER BY r.{key} COLLATE BINARY",
            dependency
                .columns
                .iter()
                .map(|c| quote(c))
                .collect::<Result<Vec<_>, _>>()?
                .join(","),
            quote(&dependency.table)?,
            if predicates.is_empty() {
                "1".into()
            } else {
                predicates.join(" AND ")
            }
        );
        let mut query = sqlx::query(&sql);
        for value in binds {
            query = query.bind(value);
        }
        let rows = query.fetch_all(&mut **tx.transaction()).await?;
        match self {
            Self::Grant(source) => {
                let mut facts = Vec::new();
                let Subject::Member { id } = &source.target;
                for row in rows {
                    let Some(target) = id.read(&row)? else {
                        continue;
                    };
                    if target.is_empty() || target.contains('\0') {
                        return Err(reject());
                    }
                    for atom in &source.atoms {
                        if let Some(atom) = atom.read(&row)? {
                            facts.push(indexes::GrantFact {
                                target: indexes::Subject::Member(target.clone()),
                                atom,
                            });
                        }
                    }
                }
                Ok(indexes::SourceChange::Grants {
                    source: source.id.clone(),
                    facts,
                })
            }
            Self::Rows(source) => {
                let mut facts = Vec::new();
                for row in rows {
                    let Some(key) = source.key.read(&row)? else {
                        continue;
                    };
                    if key.is_empty() || key.contains('\0') {
                        return Err(reject());
                    }
                    let mut atoms = Vec::new();
                    for atom in &source.atoms {
                        if let Some(value) = atom.atom.read(&row)? {
                            atoms.push((atom.slot.clone(), value));
                        }
                    }
                    facts.push(indexes::RowFact {
                        key,
                        universe: source.universe.clone(),
                        atoms,
                    });
                }
                Ok(indexes::SourceChange::Rows {
                    source: source.id.clone(),
                    facts,
                })
            }
        }
    }
}

pub(crate) struct BoundSourceProgram {
    pub collection: Collection,
    pub expression: Expr,
    pub sources: Vec<Source>,
}
impl Manifest {
    pub fn parse(value: &Value) -> Result<Self, Error> {
        serde_json::from_value(value.clone()).map_err(|_| reject())
    }
    pub fn bind(&self, path: &str) -> Result<BoundSourceProgram, Error> {
        if self.version != 1 || self.declaration.version != 1 {
            return Err(reject());
        }
        for (id, atom) in &self.declaration.atoms {
            if id.is_empty() {
                return Err(reject());
            }
            atom.canonical().map_err(|_| reject())?;
        }
        let collection = self
            .declaration
            .collections
            .get(path)
            .ok_or_else(reject)?
            .clone();
        let provider = self
            .declaration
            .providers
            .get(&collection.provider)
            .ok_or_else(reject)?;
        if provider.adapter != "relational-v1" || provider.version.is_empty() {
            return Err(reject());
        }
        let mut sources = Vec::new();
        let mut dependencies = Vec::new();
        let mut ids = BTreeSet::new();
        let mut slots = BTreeSet::new();
        let mut universe = false;
        for value in &provider.sources {
            let source = match value.get("kind").and_then(Value::as_str) {
                Some("grant") => {
                    Source::Grant(serde_json::from_value(value.clone()).map_err(|_| reject())?)
                }
                Some("row") => {
                    Source::Rows(serde_json::from_value(value.clone()).map_err(|_| reject())?)
                }
                _ => return Err(reject()),
            };
            if source.id().is_empty() || !ids.insert(source.id().to_owned()) {
                return Err(reject());
            }
            dependencies.push(source.dependency()?);
            let needed = match &source {
                Source::Grant(_) => true,
                Source::Rows(row) => {
                    let needed =
                        row.table == collection.table && row.universe == collection.universe;
                    if needed {
                        universe = true;
                        slots.extend(row.atoms.iter().map(|a| a.slot.clone()));
                    }
                    needed
                }
            };
            if needed {
                sources.push(source);
            }
        }
        if self.dependencies.get(&collection.provider) != Some(&dependencies) || !universe {
            return Err(reject());
        }
        let expression = collection.policy.lower(&self.declaration.atoms, 0)?;
        fn bound_slots(expr: &Expr, slots: &BTreeSet<String>) -> bool {
            match expr {
                Expr::SlotAny(x) => slots.contains(x),
                Expr::All(xs) | Expr::Any(xs) => xs.iter().all(|x| bound_slots(x, slots)),
                Expr::Subtract { allow, remove, .. } => {
                    bound_slots(allow, slots) && bound_slots(remove, slots)
                }
                _ => true,
            }
        }
        if !bound_slots(&expression, &slots) {
            return Err(reject());
        }
        compile::compile(&collection.universe, &expression).map_err(|_| reject())?;
        Ok(BoundSourceProgram {
            collection,
            expression,
            sources,
        })
    }
}
