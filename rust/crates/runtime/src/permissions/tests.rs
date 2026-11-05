//! Source-only tests prepared for the later explicitly granted heavy window.
//! These use ONLY a newly created owned local fixture, never an app endpoint.
use super::{
    compile::Expr,
    diff,
    model::{Atom, PermissionScope},
    query::{self, NormalizedBinding},
};
use gonvex_store::{
    permission_indexes as indexes, PoolLimits, PoolRegistry, TenantRoute, TenantSession,
};

fn atom(name: &str) -> Vec<u8> {
    Atom {
        namespace: "fixture".into(),
        resource: name.into(),
        principal: None,
    }
    .canonical()
    .unwrap()
}
fn grant(source: &str, atoms: &[&str]) -> indexes::SourceChange {
    indexes::SourceChange::Grants {
        source: source.into(),
        facts: atoms
            .iter()
            .map(|a| indexes::GrantFact {
                target: indexes::Subject::Member("alice".into()),
                atom: atom(a),
            })
            .collect(),
    }
}
fn rows(extra: bool) -> indexes::SourceChange {
    let mut facts = vec![
        indexes::RowFact {
            universe: "all-tasks".into(),
            key: "t1".into(),
            atoms: vec![("allow".into(), atom("x")), ("blocked".into(), atom("z"))],
        },
        indexes::RowFact {
            universe: "all-tasks".into(),
            key: "t2".into(),
            atoms: vec![("allow".into(), atom("y"))],
        },
    ];
    if extra {
        facts.push(indexes::RowFact {
            universe: "all-tasks".into(),
            key: "t3".into(),
            atoms: vec![("allow".into(), atom("y"))],
        });
    }
    indexes::SourceChange::Rows {
        source: "rows".into(),
        facts,
    }
}
// Independent v2 reference: real row columns + materialized token sets, not
// the new row-atom SQL. Compare interpreted v2, v2 SQL and primitive SQL.
async fn oracle(
    tx: &mut gonvex_store::TenantTransaction,
    session: &TenantSession,
    binding: &NormalizedBinding,
    active: &[&str],
    extra: bool,
    pending: bool,
) -> Vec<String> {
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    let plan: crate::visibility::VisibilityPlan = serde_json::from_value(json!({
        "version":2,"table":"oracle","key":"_id","permissionTables":["memberTokens"],
        "sets":{"allowed":{"kind":"principal","table":"memberTokens","select":"token"},
                "blocked":{"kind":"principal","table":"memberTokens","select":"token"}},
        "where":{"operator":"and","children":[
            {"operator":"inSet","column":"allowToken","set":"allowed"},
            {"operator":"not","children":[{"operator":"inSet","column":"blockToken","set":"blocked"}]}]}
    })).unwrap();
    let tokens: BTreeSet<String> = active.iter().map(|s| s.to_string()).collect();
    let resolved = crate::visibility::ResolvedVisibility {
        ephemeral_member: false,
        revision: 0,
        direct: BTreeMap::from([
            ("member.id".into(), "alice".into()),
            ("account.id".into(), "account".into()),
            ("tenant.id".into(), "fixture-tenant".into()),
        ]),
        role: "member".into(),
        permissions: json!({}),
        sets: BTreeMap::from([
            ("allowed".into(), tokens.clone()),
            ("blocked".into(), tokens),
        ]),
        fingerprint: String::new(),
        inherited: BTreeMap::new(),
        indexes: BTreeMap::new(),
    };
    let mut rows = vec![
        json!({"_id":"t1","allowToken":"x","blockToken":"z"}),
        json!({"_id":"t2","allowToken":"y","blockToken":null}),
    ];
    if extra {
        rows.push(json!({"_id":"t3","allowToken":"y","blockToken":null}));
    }
    let interpreted: Vec<String> = rows
        .iter()
        .filter(|row| crate::visibility::row_matches(&plan, &resolved, row))
        .map(|row| row["_id"].as_str().unwrap().to_owned())
        .collect();
    let mut parameters = Vec::new();
    let predicate =
        crate::visibility::compile_predicate(&plan, &resolved, "r", &mut parameters).unwrap();
    let extra_sql = if extra { ",('t3','y',NULL)" } else { "" };
    let sql=format!("WITH oracle(_id,allowToken,blockToken) AS (VALUES('t1','x','z'),('t2','y',NULL){extra_sql}) SELECT _id FROM oracle r WHERE {predicate} ORDER BY _id COLLATE BINARY");
    let old_rows = crate::sql_parameters::Parameters::scalars(&parameters)
        .unwrap()
        .bind(sqlx::query(&sql))
        .fetch_all(&mut **tx.transaction())
        .await
        .unwrap();
    use sqlx::Row;
    let v2_sql: Vec<String> = old_rows.into_iter().map(|row| row.get(0)).collect();
    let prepared = query::prepare(tx, session, binding, pending).await.unwrap();
    let sql = format!(
        "SELECT row_key FROM ({}) ORDER BY row_key COLLATE BINARY",
        prepared.compiled.sql
    );
    let new_rows = query::bind(&prepared, &sql)
        .fetch_all(&mut **tx.transaction())
        .await
        .unwrap();
    let primitive: Vec<String> = new_rows.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(
        interpreted, v2_sql,
        "v2 SQL and interpreted reference disagree"
    );
    assert_eq!(
        primitive, v2_sql,
        "indexed primitive differs from v2 reference"
    );
    // Exercise the remaining existing compiler operators against v2 SQL and
    // interpreted semantics on the SAME normalized fixture. These are compiler
    // differential queries, not alternative admitted/served policy bindings.
    let allow = json!({"operator":"inSet","column":"allowToken","set":"allowed"});
    let blocked = json!({"operator":"inSet","column":"blockToken","set":"blocked"});
    let flag_x = json!({"operator":"nonEmpty","set":"flagX"});
    let flag_z = json!({"operator":"nonEmpty","set":"flagZ"});
    let atom_x = Atom {
        namespace: "fixture".into(),
        resource: "x".into(),
        principal: None,
    };
    let atom_z = Atom {
        namespace: "fixture".into(),
        resource: "z".into(),
        principal: None,
    };
    let cases = vec![
        (
            Expr::All(vec![
                Expr::SlotAny("allow".into()),
                Expr::HasAtom(atom_x.clone()),
            ]),
            json!({"operator":"and","children":[allow.clone(),flag_x.clone()]}),
        ),
        (
            Expr::Any(vec![Expr::SlotAny("allow".into()), Expr::HasAtom(atom_z)]),
            json!({"operator":"or","children":[allow.clone(),flag_z]}),
        ),
        (
            Expr::Any(vec![
                binding.expression.clone(),
                Expr::All(vec![
                    Expr::SlotAny("blocked".into()),
                    Expr::HasAtom(atom_x.clone()),
                ]),
            ]),
            json!({"operator":"or","children":[{"operator":"and","children":[allow.clone(),{"operator":"not","children":[blocked.clone()]}]},{"operator":"and","children":[blocked.clone(),flag_x.clone()]}]}),
        ),
        (
            Expr::All(vec![
                Expr::Any(vec![
                    Expr::SlotAny("allow".into()),
                    Expr::SlotAny("blocked".into()),
                ]),
                Expr::Subtract {
                    universe: binding.universe.clone(),
                    allow: Box::new(Expr::HasAtom(atom_x)),
                    remove: Box::new(Expr::SlotAny("blocked".into())),
                },
            ]),
            json!({"operator":"and","children":[{"operator":"or","children":[allow,blocked.clone()]},flag_x,{"operator":"not","children":[blocked]}]}),
        ),
    ];
    for (expression, predicate) in cases {
        let mut plan = plan.clone();
        plan.active_sets_cache = Default::default();
        plan.predicate = serde_json::from_value(predicate).unwrap();
        for name in ["flagX", "flagZ"] {
            plan.sets.insert(
                name.into(),
                serde_json::from_value(
                    json!({"kind":"principal","table":"memberTokens","select":"token"}),
                )
                .unwrap(),
            );
        }
        let mut resolved = resolved.clone();
        for (name, token) in [("flagX", "x"), ("flagZ", "z")] {
            resolved.sets.insert(
                name.into(),
                if active.contains(&token) {
                    BTreeSet::from([token.to_string()])
                } else {
                    BTreeSet::new()
                },
            );
        }
        let expected: Vec<String> = rows
            .iter()
            .filter(|row| crate::visibility::row_matches(&plan, &resolved, row))
            .map(|row| row["_id"].as_str().unwrap().to_string())
            .collect();
        let mut parameters = Vec::new();
        let predicate =
            crate::visibility::compile_predicate(&plan, &resolved, "r", &mut parameters).unwrap();
        let sql=format!("WITH oracle(_id,allowToken,blockToken) AS (VALUES('t1','x','z'),('t2','y',NULL){extra_sql}) SELECT _id FROM oracle r WHERE {predicate} ORDER BY _id COLLATE BINARY");
        let v2 = crate::sql_parameters::Parameters::scalars(&parameters)
            .unwrap()
            .bind(sqlx::query(&sql))
            .fetch_all(&mut **tx.transaction())
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect::<Vec<String>>();
        assert_eq!(
            expected, v2,
            "Boolean reference SQL differs from interpreted v2"
        );
        let compiled = super::compile::compile(&binding.universe, &expression).unwrap();
        let probe = query::Prepared {
            compiled,
            identity: prepared.identity.clone(),
            scope_id: prepared.scope_id,
            table: prepared.table.clone(),
        };
        let sql = format!(
            "SELECT row_key FROM ({}) ORDER BY row_key COLLATE BINARY",
            probe.compiled.sql
        );
        let actual = query::bind(&probe, &sql)
            .fetch_all(&mut **tx.transaction())
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect::<Vec<String>>();
        assert_eq!(
            actual, v2,
            "normalized AND/OR/HasAtom/subtraction SQL differs from v2"
        );
    }
    primitive
}

struct Environment(Option<std::ffi::OsString>);
impl Drop for Environment {
    fn drop(&mut self) {
        match self.0.take() {
            Some(value) => std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", value),
            None => std::env::remove_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL"),
        }
    }
}

#[tokio::test]
async fn normalized_sql_diff_preserves_overlap_negative_flips_and_rollback() {
    static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = ENV.lock().unwrap();
    let _environment = Environment(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    assert_eq!(
        std::fs::canonicalize(indexes::LOCAL_FIXTURE_ROOT).unwrap(),
        std::path::Path::new(indexes::LOCAL_FIXTURE_ROOT)
    );
    let directory = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let path = directory.path().join("normalized.db");
    let url = gonvex_store::database_url(&path);
    let pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE members(id TEXT PRIMARY KEY,account_id TEXT UNIQUE,status TEXT); INSERT INTO members VALUES('alice','account','active')").execute(&mut **tx.transaction()).await.unwrap();
    let instance = indexes::install_local(&mut tx).await.unwrap();
    let epoch: String =
        sqlx::query_scalar("SELECT epoch FROM _gonvex_sync_clock WHERE singleton=1")
            .fetch_one(&mut **tx.transaction())
            .await
            .unwrap();
    let expression = Expr::Subtract {
        universe: "all-tasks".into(),
        allow: Box::new(Expr::SlotAny("allow".into())),
        remove: Box::new(Expr::SlotAny("blocked".into())),
    };
    let scope = PermissionScope {
        project: "fixture-project".into(),
        database_instance: instance,
        tenant: "fixture-tenant".into(),
        epoch,
        table: "tasks".into(),
        collection: "tasks.list".into(),
        policy_version: super::compile::policy_version("all-tasks", &expression).unwrap(),
        projection_version: "keys-binary-v1".into(),
        semantics_version: "normalized-atoms-binary-v1".into(),
    };
    let scope_id = indexes::create_scope(
        &mut tx,
        &scope.canonical().unwrap(),
        &scope.database_instance,
        &scope.epoch,
        &scope.table,
    )
    .await
    .unwrap();
    indexes::register_sources(
        &mut tx,
        scope_id,
        &[
            ("grant".into(), "a".into()),
            ("grant".into(), "b".into()),
            ("grant".into(), "negative".into()),
            ("row".into(), "rows".into()),
        ],
    )
    .await
    .unwrap();
    indexes::apply(
        &mut tx,
        scope_id,
        &[
            grant("a", &["x", "y"]),
            grant("b", &["x"]),
            grant("negative", &[]),
            rows(false),
        ],
    )
    .await
    .unwrap();
    indexes::seal(&mut tx, scope_id).await.unwrap();
    tx.commit().await.unwrap();
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: scope.project.clone(),
        tenant_id: scope.tenant.clone(),
        database_url: url.clone(),
    });
    session.identity.account.id = "account".into();
    let binding = NormalizedBinding {
        scope_id,
        required_generation: 1,
        scope,
        universe: "all-tasks".into(),
        expression,
    };
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(
        query::select_keys(&mut tx, &session, &binding)
            .await
            .unwrap()
            .1,
        vec!["t1", "t2"]
    );
    tx.rollback().await.unwrap();
    // Removing source a retains x through b; rollback restores y as well.
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let delta = diff::apply_and_diff(&mut tx, &session, &binding, &[grant("a", &[])])
        .await
        .unwrap();
    assert_eq!(delta.removed, vec!["t2"]);
    assert!(delta.added.is_empty());
    tx.rollback().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    // No positive atom is added: the negative flip removes t1 while a row
    // write admits t3 in the SAME transaction.
    let delta = diff::apply_and_diff(
        &mut tx,
        &session,
        &binding,
        &[grant("negative", &["z"]), rows(true)],
    )
    .await
    .unwrap();
    assert_eq!(delta.added, vec!["t3"]);
    assert_eq!(delta.removed, vec!["t1"]);
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let delta = diff::apply_and_diff(&mut tx, &session, &binding, &[grant("negative", &[])])
        .await
        .unwrap();
    assert_eq!(delta.added, vec!["t1"]);
    assert!(delta.removed.is_empty());
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let delta = diff::apply_and_diff(&mut tx, &session, &binding, &[grant("a", &[])])
        .await
        .unwrap();
    assert_eq!(delta.removed, vec!["t2", "t3"]);
    assert!(delta.added.is_empty());
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let delta = diff::apply_and_diff(&mut tx, &session, &binding, &[grant("b", &[])])
        .await
        .unwrap();
    assert_eq!(delta.removed, vec!["t1"]);
    assert!(delta.added.is_empty());
    tx.commit().await.unwrap();
    // Deterministic generated batches exercise all direct/alternate-support,
    // row-universe and negative combinations. No timing/performance claim.
    let mut seed = 0x5916_abcd_2357_u64;
    let mut previous = std::collections::BTreeSet::<String>::new();
    for _ in 0..64 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let a = seed & 1 != 0;
        let b = seed & 2 != 0;
        let negative = seed & 4 != 0;
        let extra = seed & 8 != 0;
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        let delta = diff::apply_and_diff(
            &mut tx,
            &session,
            &binding,
            &[
                grant("a", if a { &["x", "y"] } else { &[] }),
                grant("b", if b { &["x"] } else { &[] }),
                grant("negative", if negative { &["z"] } else { &[] }),
                rows(extra),
            ],
        )
        .await
        .unwrap();
        let mut active = Vec::new();
        if a || b {
            active.push("x");
        }
        if a {
            active.push("y");
        }
        if negative {
            active.push("z");
        }
        let visible: std::collections::BTreeSet<String> =
            oracle(&mut tx, &session, &binding, &active, extra, true)
                .await
                .into_iter()
                .collect();
        assert_eq!(
            delta.added,
            visible.difference(&previous).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            delta.removed,
            previous.difference(&visible).cloned().collect::<Vec<_>>()
        );
        tx.commit().await.unwrap();
        previous = visible;
    }
    // A scoped role reaches Alice through two independent group paths.
    // Removing either membership source alone must retain the other path.
    let role = super::roles::ScopedRole {
        id: "fixture-role".into(),
        scope: super::roles::RoleScope::Group {
            namespace: "team".into(),
            id: "team-1".into(),
        },
    };
    let role_atom = Atom {
        namespace: "fixture".into(),
        resource: "x".into(),
        principal: None,
    };
    let left = role
        .assignments("left".into(), vec![indexes::Subject::Group("A".into())])
        .unwrap();
    let right = role
        .assignments("right".into(), vec![indexes::Subject::Group("B".into())])
        .unwrap();
    let role_group = match &left {
        indexes::SourceChange::Memberships { facts, .. } => facts[0].group.clone(),
        _ => unreachable!(),
    };
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    indexes::register_sources(
        &mut tx,
        scope_id,
        &[
            ("grant".into(), "role-definition".into()),
            ("membership".into(), "left".into()),
            ("membership".into(), "right".into()),
            ("membership".into(), "leaf-a".into()),
            ("membership".into(), "leaf-b".into()),
        ],
    )
    .await
    .unwrap();
    indexes::apply(
        &mut tx,
        scope_id,
        &[
            grant("a", &[]),
            grant("b", &[]),
            grant("negative", &[]),
            rows(false),
            role.definition("role-definition".into(), &[role_atom])
                .unwrap(),
            left,
            right,
            indexes::SourceChange::Memberships {
                source: "leaf-a".into(),
                facts: vec![indexes::MembershipFact {
                    group: "A".into(),
                    member: indexes::Subject::Member("alice".into()),
                }],
            },
            indexes::SourceChange::Memberships {
                source: "leaf-b".into(),
                facts: vec![indexes::MembershipFact {
                    group: "B".into(),
                    member: indexes::Subject::Member("alice".into()),
                }],
            },
        ],
    )
    .await
    .unwrap();
    indexes::seal(&mut tx, scope_id).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert_eq!(
        oracle(&mut tx, &session, &binding, &["x"], false, false).await,
        vec!["t1"]
    );
    tx.rollback().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let delta = diff::apply_and_diff(
        &mut tx,
        &session,
        &binding,
        &[role.assignments("left".into(), vec![]).unwrap()],
    )
    .await
    .unwrap();
    assert!(delta.added.is_empty() && delta.removed.is_empty());
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    let delta = diff::apply_and_diff(
        &mut tx,
        &session,
        &binding,
        &[role.assignments("right".into(), vec![]).unwrap()],
    )
    .await
    .unwrap();
    assert_eq!(delta.removed, vec!["t1"]);
    tx.commit().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    assert!(indexes::apply(
        &mut tx,
        scope_id,
        &[
            role.assignments("left".into(), vec![indexes::Subject::Group("A".into())])
                .unwrap(),
            indexes::SourceChange::Memberships {
                source: "leaf-a".into(),
                facts: vec![indexes::MembershipFact {
                    group: "A".into(),
                    member: indexes::Subject::Group(role_group)
                }]
            },
        ]
    )
    .await
    .is_err());
    assert!(
        tx.commit().await.is_err(),
        "cycles must poison the entire writer batch"
    );
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert!(
        query::select_keys(&mut tx, &session, &binding)
            .await
            .unwrap()
            .1
            .is_empty(),
        "failed cyclic batch must restore both facts and ready state"
    );
    tx.rollback().await.unwrap();
    // Merely declaring a new backfill source may not make it ready.
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    indexes::register_sources(&mut tx, scope_id, &[("grant".into(), "not-loaded".into())])
        .await
        .unwrap();
    assert!(indexes::seal(&mut tx, scope_id).await.is_err());
    assert!(tx.commit().await.is_err());
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    assert!(
        query::select_keys(&mut tx, &session, &binding)
            .await
            .unwrap()
            .1
            .is_empty(),
        "failed backfill seal must restore the old ready generation"
    );
    tx.rollback().await.unwrap();
    // Real query-path readiness/admission checks, not just metadata models.
    for assignment in [
        "phase='building'",
        "phase='failed'",
        "generation=2",
        "covered_revision=-1",
        "covered_revision=covered_revision+1",
    ] {
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        sqlx::query(&format!(
            "UPDATE _gonvex_permission_scopes SET {assignment} WHERE scope_id=?1"
        ))
        .bind(scope_id)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
        assert!(
            query::select_keys(&mut tx, &session, &binding)
                .await
                .is_err(),
            "{assignment} must reject"
        );
        tx.rollback().await.unwrap();
    }
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::query("UPDATE members SET status='revoked' WHERE id='alice'")
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    assert!(
        query::select_keys(&mut tx, &session, &binding)
            .await
            .is_err(),
        "cached session cannot override revoked member"
    );
    tx.rollback().await.unwrap();
    let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
        .await
        .unwrap();
    for (field, value) in [
        ("project", "other-project"),
        ("database", "other-instance"),
        ("policy", "different-policy"),
        ("projection", "unsupported-projection"),
    ] {
        let mut wrong = binding.clone();
        match field {
            "project" => wrong.scope.project = value.into(),
            "database" => wrong.scope.database_instance = value.into(),
            "policy" => wrong.scope.policy_version = value.into(),
            _ => wrong.scope.projection_version = value.into(),
        }
        assert!(
            query::select_keys(&mut tx, &session, &wrong).await.is_err(),
            "{field} mismatch must reject"
        );
    }
    tx.rollback().await.unwrap();
    pools.close().await;
    // Closed, checkpointed OWN fixture copy preserves epoch/UUID but has a
    // different physical inode. It must not inherit index/sharing identity.
    let copied = directory.path().join("copied.db");
    std::fs::copy(&path, &copied).unwrap();
    let copied_url = gonvex_store::database_url(&copied);
    let copied_pools = PoolRegistry::new(PoolLimits::default());
    let mut tx = gonvex_store::begin_transaction(&copied_pools, &copied_url, true)
        .await
        .unwrap();
    assert!(indexes::instance(tx.transaction()).await.is_err());
    let mut copied_session = session.clone();
    copied_session.route.database_url = copied_url;
    assert!(query::select_keys(&mut tx, &copied_session, &binding)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    copied_pools.close().await;
    drop(directory);
}
