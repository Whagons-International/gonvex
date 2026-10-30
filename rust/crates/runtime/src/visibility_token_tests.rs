use crate::visibility::{row_matches, ResolvedVisibility, VisibilityPlan};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn membership_never_confuses_null_columns_with_the_string_null_token() {
    let plan: VisibilityPlan = serde_json::from_value(json!({
        "version":2,"permissionTables":["members"],"table":"tasks","key":"id",
        "sets":{"tokens":{"kind":"principal","table":"members","select":"id","where":[],"joins":[]}},
        "where":{"operator":"inSet","column":"workspaceId","set":"tokens"}
    })).unwrap();
    let principal = ResolvedVisibility {
        ephemeral_member: false,
        revision: 1,
        direct: BTreeMap::new(),
        role: "member".into(),
        permissions: Value::Null,
        sets: BTreeMap::from([("tokens".into(), BTreeSet::from(["null".into()]))]),
        fingerprint: String::new(),
        inherited: BTreeMap::new(),
        indexes: BTreeMap::new(),
    };
    assert!(!row_matches(
        &plan,
        &principal,
        &json!({"workspaceId":null})
    ));
    assert!(!row_matches(&plan, &principal, &json!({})));
    assert!(row_matches(
        &plan,
        &principal,
        &json!({"workspaceId":"null"})
    ));
}
