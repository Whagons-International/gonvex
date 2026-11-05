//! Complete-for-collection proof for ONE exact supported shared-team policy.
//! No caller-provided completeness boolean and no inference from index receipts.
use crate::{
    modules::ReplicaCollectionDefinition, permissions::source::Manifest, visibility::VisibilityPlan,
};
use serde_json::{json, Value};

#[derive(Clone)]
pub(crate) struct CompleteProvider {
    manifest: Vec<u8>,
    plan: Vec<u8>,
    definition: Vec<u8>,
    grant_table: String,
    grant_columns: std::collections::BTreeSet<String>,
}
impl CompleteProvider {
    pub(crate) fn manifest(&self) -> &[u8] {
        &self.manifest
    }
    pub(crate) fn plan(&self) -> &[u8] {
        &self.plan
    }
    pub(crate) fn definition(&self) -> &[u8] {
        &self.definition
    }
    pub(crate) fn grant_table(&self) -> &str {
        &self.grant_table
    }
    pub(crate) fn grant_columns(&self) -> &std::collections::BTreeSet<String> {
        &self.grant_columns
    }
}
fn field(v: &Value, alias: &str) -> Option<String> {
    if v.get("type")?.as_str()? != "text"
        || v.get("onNull")?.as_str()? != "omitFact"
        || v.get("field")?.get("alias")?.as_str()? != alias
    {
        return None;
    }
    Some(v.get("field")?.get("column")?.as_str()?.into())
}
fn strip_null_principal(v: &mut Value) {
    match v {
        Value::Object(m) => {
            if m.get("principal") == Some(&Value::Null) {
                m.remove("principal");
            }
            for x in m.values_mut() {
                strip_null_principal(x);
            }
        }
        Value::Array(a) => {
            for x in a {
                strip_null_principal(x);
            }
        }
        _ => {}
    }
}
pub(crate) fn prove(
    raw: &Value,
    path: &str,
    definition: &ReplicaCollectionDefinition,
    actual: &VisibilityPlan,
) -> Option<CompleteProvider> {
    if !matches!(definition.mode.as_str(), "eager" | "progressive")
        || !definition.equal_filters.is_empty()
        || !definition.exclude_when_set.is_empty()
        || definition.max_bytes < 0
    {
        return None;
    }
    Manifest::parse(raw).ok()?.bind(path).ok()?;
    let collection = raw.pointer("/declaration/collections")?.get(path)?;
    if collection.get("table")?.as_str()? != definition.table
        || collection.get("key")?.as_str()? != definition.key
        || collection.get("policy")? != &json!({"op":"slot","slot":"read"})
    {
        return None;
    }
    if raw.pointer("/declaration/atoms")?.as_object()?.len() != 0 {
        return None;
    }
    let provider = raw
        .pointer("/declaration/providers")?
        .get(collection.get("provider")?.as_str()?)?;
    if provider.get("version")?.as_str()? != "starter-shared-team-v1" {
        return None;
    }
    let sources = provider.get("sources")?.as_array()?;
    if sources.len() != 2 {
        return None;
    }
    let grant = sources
        .iter()
        .find(|s| s.get("kind") == Some(&json!("grant")))?;
    let rows = sources
        .iter()
        .find(|s| s.get("kind") == Some(&json!("row")))?;
    let ga = grant.pointer("/from/as")?.as_str()?;
    let ra = rows.pointer("/from/as")?.as_str()?;
    let gt = grant.pointer("/from/table")?.as_str()?;
    let gk = grant.pointer("/from/key")?.as_str()?;
    let rid = rows.pointer("/from/key")?.as_str()?;
    if rows.pointer("/from/table")?.as_str()? != definition.table
        || field(rows.get("key")?, ra)? != definition.key
        || rid != definition.key
    {
        return None;
    }
    let member = field(grant.pointer("/target/id")?, ga)?;
    let gatoms = grant.get("atoms")?.as_array()?;
    let ratoms = rows.get("atoms")?.as_array()?;
    if gatoms.len() != 1 || ratoms.len() != 1 {
        return None;
    }
    let namespace = gatoms[0].get("namespace")?.as_str()?;
    let team = field(gatoms[0].get("resource")?, ga)?;
    let rowteam = field(ratoms[0].pointer("/atom/resource")?, ra)?;
    if namespace != "team" || gt == "members" || team == member || rowteam == definition.key {
        return None;
    }
    let text = |alias: &str, column: &str| json!({"field":{"alias":alias,"column":column},"type":"text","onNull":"omitFact"});
    let expected_grant = json!({"id":grant.get("id")?,"kind":"grant","from":{"table":gt,"key":gk,"as":ga},"joins":[],"where":[],"target":{"kind":"member","id":text(ga,&member)},"atoms":[{"namespace":namespace,"resource":text(ga,&team)}]});
    let expected_rows = json!({"id":rows.get("id")?,"kind":"row","from":{"table":definition.table,"key":definition.key,"as":ra},"joins":[],"where":[],"table":definition.table,"key":text(ra,&definition.key),"universe":collection.get("universe")?,"atoms":[{"slot":"read","atom":{"namespace":namespace,"resource":text(ra,&rowteam)}}]});
    let mut g = grant.clone();
    let mut r = rows.clone();
    strip_null_principal(&mut g);
    strip_null_principal(&mut r);
    if g != expected_grant || r != expected_rows {
        return None;
    }
    let expected:VisibilityPlan=serde_json::from_value(json!({"version":2,"table":definition.table,"key":definition.key,"permissionTables":[gt],"sets":{"read":{"kind":"principal","table":gt,"select":team,"where":[{"column":member,"context":"member.id"}]}},"where":{"operator":"inSet","column":rowteam,"set":"read"}})).ok()?;
    if serde_json::to_value(actual).ok()? != serde_json::to_value(expected).ok()? {
        return None;
    }
    Some(CompleteProvider {
        grant_table: gt.into(),
        grant_columns: [gk.to_owned(), member.clone(), team.clone()]
            .into_iter()
            .collect(),
        manifest: serde_json::to_vec(raw).ok()?,
        plan: serde_json::to_vec(actual).ok()?,
        definition: serde_json::to_vec(definition).ok()?,
    })
}

#[cfg(test)]
pub(crate) fn fixture_manifest() -> Value {
    let text = |alias: &str, column: &str| json!({"field":{"alias":alias,"column":column},"type":"text","onNull":"omitFact"});
    json!({"version":1,"declaration":{"version":1,"atoms":{},"providers":{"teams":{"adapter":"relational-v1","version":"starter-shared-team-v1","sources":[
        {"id":"membership","kind":"grant","from":{"table":"memberships","key":"id","as":"m"},"joins":[],"where":[],"target":{"kind":"member","id":text("m","memberId")},"atoms":[{"namespace":"team","resource":text("m","team")}]},
        {"id":"tasks","kind":"row","from":{"table":"tasks","key":"id","as":"t"},"joins":[],"where":[],"table":"tasks","key":text("t","id"),"universe":"tasks","atoms":[{"slot":"read","atom":{"namespace":"team","resource":text("t","team")}}]}
    ]}},"collections":{"tasks.all":{"provider":"teams","table":"tasks","key":"id","universe":"tasks","policy":{"op":"slot","slot":"read"}}}},"dependencies":{"teams":[
        {"source":"membership","table":"memberships","key":"id","alias":"m","columns":["id","memberId","team"],"images":"oldAndNew"},
        {"source":"tasks","table":"tasks","key":"id","alias":"t","columns":["id","team"],"images":"oldAndNew"}
    ]}})
}
#[cfg(test)]
pub(crate) fn fixture_definition() -> ReplicaCollectionDefinition {
    serde_json::from_value(json!({"table":"tasks","key":"id","columns":["id","body","rank"],"orderBy":"rank","orderDirection":"asc","mode":"eager","maxRows":10000,"maxBytes":0})).unwrap()
}
#[cfg(test)]
pub(crate) fn fixture_visibility() -> VisibilityPlan {
    serde_json::from_value(json!({"version":2,"table":"tasks","key":"id","permissionTables":["memberships"],"sets":{"read":{"kind":"principal","table":"memberships","select":"team","where":[{"column":"memberId","context":"member.id"}]}},"where":{"operator":"inSet","column":"team","set":"read"}})).unwrap()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn certificate_requires_exact_complete_loaded_shared_policy_not_version_or_receipts() {
        let raw = fixture_manifest();
        let definition = fixture_definition();
        let plan = fixture_visibility();
        assert!(prove(&raw, "tasks.all", &definition, &plan).is_some());
        for kind in 0..6 {
            let mut raw = raw.clone();
            match kind {
                0 => {
                    raw["declaration"]["providers"]["teams"]["sources"][0]["atoms"][0]
                        ["principal"] = json!({"literal":"alice"})
                }
                1 => {
                    raw["declaration"]["providers"]["teams"]["sources"][0]["where"] =
                        json!([{"op":"isNull","field":{"alias":"m","column":"deletedAt"}}])
                }
                2 => {
                    raw["declaration"]["providers"]["teams"]["sources"][0]["where"] = json!([{"op":"textEq","left":{"literal":"active"},"right":{"field":{"alias":"m","column":"status"},"type":"text","onNull":"omitFact"}}])
                }
                3 => {
                    raw["declaration"]["collections"]["tasks.all"]["policy"] = json!({"op":"except","universe":"tasks","allow":{"op":"slot","slot":"read"},"remove":{"op":"slot","slot":"blocked"}})
                }
                4 => raw["dependencies"]["teams"] = json!([]),
                _ => {
                    raw["declaration"]["providers"]["teams"]["sources"][1]["table"] =
                        json!("private")
                }
            };
            assert!(prove(&raw, "tasks.all", &definition, &plan).is_none());
        }
        let mut other = definition.clone();
        other.equal_filters.insert("owner".into(), "member".into());
        assert!(prove(&raw, "tasks.all", &other, &plan).is_none());
        let mut root = serde_json::to_value(&plan).unwrap();
        root["where"] = json!({"operator":"public"});
        let root: VisibilityPlan = serde_json::from_value(root).unwrap();
        assert!(prove(&raw, "tasks.all", &definition, &root).is_none());
    }
}
