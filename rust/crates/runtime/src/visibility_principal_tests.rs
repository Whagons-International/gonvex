//! Reduced real spots.list permission joins (app pin 7d97617cf).
use super::*;
use serde_json::json;

fn plan() -> VisibilityPlan {
    serde_json::from_value(json!({
        "version":2,"table":"spots","key":"_id",
        "permissionTables":["members","memberTeams","workspaceTeams","globalSettings","memberSpots"],
        "sets":{
            "allSpots": {"kind":"grant","table":"tasks","select":"spotId",
                "joins":[{"table":"workspaceTeams","leftColumn":"workspaceId","rightColumn":"workspaceId"},
                    {"table":"memberTeams","leftColumn":"teamId","rightColumn":"teamId"},
                    {"table":"members","leftColumn":"memberId","rightColumn":"id"}],
                "where":[{"table":"memberTeams","column":"memberId","context":"member.id"},
                    {"table":"memberTeams","column":"deletedAt","value":{"literal":null}},
                    {"table":"members","column":"see_all_spots","value":{"literal":true}}]},
            "tenantSpots": {"kind":"grant","table":"tasks","select":"spotId",
                "joins":[{"table":"workspaceTeams","leftColumn":"workspaceId","rightColumn":"workspaceId"},
                    {"table":"memberTeams","leftColumn":"teamId","rightColumn":"teamId"},
                    {"table":"members","leftColumn":"memberId","rightColumn":"id"},
                    {"table":"globalSettings","leftColumn":"status","rightColumn":"allSpotsMemberStatus"}],
                "where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]},
            "spots": {"kind":"principal","table":"memberSpots","select":"spotId",
                "where":[{"column":"memberId","context":"member.id"}]}
        },
        "where":{"operator":"or","children":[
            {"operator":"inSet","column":"_id","set":"allSpots"},
            {"operator":"inSet","column":"_id","set":"tenantSpots"},
            {"operator":"inSet","column":"_id","set":"spots"}]}
    })).unwrap()
}
fn member() -> ResolvedVisibility {
    ResolvedVisibility {
        ephemeral_member: false,
        revision: 0,
        direct: BTreeMap::from([("member.id".into(), "alice".into())]).into(),
        role: "member".into(),
        permissions: json!({}).into(),
        sets: BTreeMap::new().into(),
        fingerprint: String::new(),
        inherited: BTreeMap::new(),
        indexes: BTreeMap::new(),
    }
}
fn concerns(
    plan: &VisibilityPlan,
    table: &str,
    op: &str,
    column: &str,
    old: Value,
    new: Value,
) -> bool {
    plan.principal_change_concerns(&member(), table, op, &[column.into()], &old, &new)
}
#[test]
fn irrelevant_member_writes_do_not_reopen_spots() {
    let p = plan();
    p.validate().unwrap();
    for column in [
        "name",
        "lastSeenAt",
        "updatedAt",
        "permissions",
        "role",
        "status",
    ] {
        let bob = json!({"id":"bob","role":"member","status":"active","see_all_spots":false});
        let mut updated = bob.clone();
        updated[column] = match column {
            "permissions" => json!({"tasks.viewAll":true}),
            "role" => json!("admin"),
            "status" => json!("revoked"),
            _ => json!("changed"),
        };
        assert!(
            !concerns(&p, "members", "update", column, bob, updated),
            "{column}"
        );
    }
    for column in ["name", "lastSeenAt", "updatedAt"] {
        let alice = json!({"id":"alice","status":"active","see_all_spots":false});
        let mut updated = alice.clone();
        updated[column] = json!("changed");
        assert!(
            !concerns(&p, "members", "update", column, alice, updated),
            "{column}"
        );
    }
}
#[test]
fn visibility_changes_and_membership_moves_still_reopen() {
    let p = plan();
    for (before, after) in [(false, true), (true, false)] {
        for id in ["alice", "bob"] {
            assert_eq!(
                concerns(
                    &p,
                    "members",
                    "update",
                    "see_all_spots",
                    json!({"id":id,"see_all_spots":before}),
                    json!({"id":id,"see_all_spots":after})
                ),
                id == "alice"
            );
        }
    }
    for column in ["id", "account_id", "role", "permissions", "status"] {
        assert!(concerns(
            &p,
            "members",
            "update",
            column,
            json!({"id":"alice"}),
            json!({"id":"alice"})
        ));
    }
    for table in ["members", "memberTeams", "memberSpots"] {
        let row = if table == "members" {
            json!({"id":"alice"})
        } else {
            json!({"memberId":"alice","teamId":"t","spotId":"s"})
        };
        assert!(concerns(&p, table, "insert", "", Value::Null, row.clone()));
        assert!(concerns(&p, table, "delete", "", row, Value::Null));
    }
    for (from, to) in [("alice", "bob"), ("bob", "alice")] {
        assert!(concerns(
            &p,
            "memberTeams",
            "update",
            "memberId",
            json!({"memberId":from}),
            json!({"memberId":to})
        ));
    }
    assert!(concerns(
        &p,
        "memberTeams",
        "update",
        "teamId",
        json!({"memberId":"alice","teamId":"a"}),
        json!({"memberId":"alice","teamId":"b"})
    ));
    assert!(concerns(
        &p,
        "members",
        "update",
        "see_all_spots",
        json!({}),
        json!({})
    ));
    assert!(concerns(
        &p,
        "members",
        "update",
        "see_all_spots",
        Value::Null,
        Value::Null
    ));
    assert!(concerns(
        &p,
        "globalSettings",
        "update",
        "allSpotsMemberStatus",
        json!({}),
        json!({})
    ));
}
#[test]
#[ignore = "short module timing; run only in reserved measurement window"]
fn principal_reopen_measurement() {
    let p = plan();
    let alice = member();
    let old = json!({"id":"bob","role":"member","permissions":{}});
    let new = json!({"id":"bob","role":"member","permissions":{"tasks.viewAll":true}});
    let columns = vec!["permissions".into()];
    let n = 100_000;
    fn cpu_ns() -> u128 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // CLOCK_THREAD_CPUTIME_ID measures only this test's routing work.
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) },
            0
        );
        time.tv_sec as u128 * 1_000_000_000 + time.tv_nsec as u128
    }
    let cpu = cpu_ns();
    let start = std::time::Instant::now();
    let mut reopen = 0;
    for _ in 0..n {
        reopen += usize::from(std::hint::black_box(p.principal_change_concerns(
            std::hint::black_box(&alice),
            "members",
            "update",
            &columns,
            &old,
            &new,
        )));
    }
    eprintln!(
        "principal_reopen: checks={n} reopens={reopen} wall_ns/check={} cpu_ns/check={}",
        start.elapsed().as_nanos() / n as u128,
        (cpu_ns() - cpu) / n as u128
    );
}

#[test]
fn unprovable_joins_and_inherited_principals_remain_conservative() {
    let mut p = plan();
    // The context memberId does not prove that teamId belongs to this user.
    // Removing the memberId=id equality leaves the members occurrence unanchored.
    p.sets.get_mut("allSpots").unwrap().joins[2].left_column = "teamId".into();
    assert!(concerns(
        &p,
        "members",
        "update",
        "see_all_spots",
        json!({"id":"bob","see_all_spots":false}),
        json!({"id":"bob","see_all_spots":true})
    ));
    let mut alternatives = plan();
    alternatives.sets.insert(
        "unanchored".into(),
        serde_json::from_value(json!({
            "kind":"principal","table":"members","select":"id",
            "where":[{"column":"see_all_spots","value":{"literal":true}}]
        }))
        .unwrap(),
    );
    alternatives.predicate.children.push(
        serde_json::from_value(json!({
            "operator":"nonEmpty","set":"unanchored"
        }))
        .unwrap(),
    );
    assert!(
        concerns(
            &alternatives,
            "members",
            "update",
            "see_all_spots",
            json!({"id":"bob","see_all_spots":false}),
            json!({"id":"bob","see_all_spots":true})
        ),
        "an OR alternative without a user anchor must still resync"
    );
    let parent = plan();
    let mut child: VisibilityPlan = serde_json::from_value(json!({
        "version":2,"table":"notes","key":"_id","sets":{},
        "where":{"operator":"inherits","table":"spots","key":"_id","column":"spotId"}
    }))
    .unwrap();
    let mut resolved = member();
    resolved.inherited.insert(
        "spots".into(),
        InheritedParent {
            plan: Arc::new(parent),
            resolved: Arc::new(member()),
            index: Arc::new(std::sync::RwLock::new(ParentAclIndex::from_rows(
                0,
                std::iter::empty(),
            ))),
        },
    );
    let columns = vec!["see_all_spots".into()];
    assert!(child.principal_change_concerns(
        &resolved,
        "members",
        "update",
        &columns,
        &json!({"id":"alice","see_all_spots":false}),
        &json!({"id":"alice","see_all_spots":true})
    ));
    assert!(!child.principal_change_concerns(
        &resolved,
        "members",
        "update",
        &columns,
        &json!({"id":"bob","see_all_spots":false}),
        &json!({"id":"bob","see_all_spots":true})
    ));
    // A role branch does not turn a missing equality into a user anchor.
    child.predicate = serde_json::from_value(json!({"operator":"or","children":[
        {"operator":"role","value":"admin"},child.predicate]}))
    .unwrap();
    assert!(child.principal_change_concerns(
        &resolved,
        "members",
        "delete",
        &[],
        &json!({"id":"alice"}),
        &Value::Null
    ));
}

#[tokio::test]
async fn sql_oracle_member_flag_and_membership_transitions() {
    use sqlx::Connection;
    let mut db = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE tasks (spotId TEXT, workspaceId TEXT); CREATE TABLE workspaceTeams (workspaceId TEXT, teamId TEXT); CREATE TABLE memberTeams (memberId TEXT, teamId TEXT, deletedAt INTEGER); CREATE TABLE members (id TEXT, see_all_spots BOOLEAN, permissions TEXT); INSERT INTO tasks VALUES ('spot-a','wa'),('spot-b','wb'); INSERT INTO workspaceTeams VALUES ('wa','ta'),('wb','tb'); INSERT INTO memberTeams VALUES ('alice','ta',NULL),('bob','tb',NULL); INSERT INTO members VALUES ('alice',0,'{}'),('bob',0,'{}');").execute(&mut db).await.unwrap();
    let p = plan();
    let direct = member().direct;
    let tokens = |set: &VisibilitySet| compile_set(set, &direct).unwrap();
    let (sql, values) = tokens(&p.sets["allSpots"]);
    async fn visible(db: &mut sqlx::SqliteConnection, sql: &str, values: &[Value]) -> Vec<String> {
        let rows = Parameters::scalars(values)
            .unwrap()
            .bind(sqlx::query(sql))
            .fetch_all(db)
            .await
            .unwrap();
        rows.into_iter().map(|r| r.get::<String, _>(0)).collect()
    }
    assert!(visible(&mut db, &sql, &values).await.is_empty());
    sqlx::query("UPDATE members SET see_all_spots=1 WHERE id='bob'")
        .execute(&mut db)
        .await
        .unwrap();
    assert!(!concerns(
        &p,
        "members",
        "update",
        "see_all_spots",
        json!({"id":"bob","see_all_spots":false}),
        json!({"id":"bob","see_all_spots":true})
    ));
    assert!(visible(&mut db, &sql, &values).await.is_empty());
    sqlx::query("UPDATE members SET see_all_spots=1 WHERE id='alice'")
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(visible(&mut db, &sql, &values).await, vec!["spot-a"]);
    sqlx::query("UPDATE memberTeams SET teamId='tb' WHERE memberId='alice'")
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(visible(&mut db, &sql, &values).await, vec!["spot-b"]);
    sqlx::query("DELETE FROM memberTeams WHERE memberId='alice'")
        .execute(&mut db)
        .await
        .unwrap();
    assert!(visible(&mut db, &sql, &values).await.is_empty());
    sqlx::query("INSERT INTO memberTeams VALUES ('alice','ta',NULL)")
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(visible(&mut db, &sql, &values).await, vec!["spot-a"]);
    sqlx::query("UPDATE members SET see_all_spots=0 WHERE id='alice'")
        .execute(&mut db)
        .await
        .unwrap();
    assert!(visible(&mut db, &sql, &values).await.is_empty());
    db.close().await.unwrap();
}

#[test]
fn cached_inputs_are_shared_metadata_not_a_members_principal() {
    let p = plan();
    let old = json!({"id":"bob","see_all_spots":false});
    let new = json!({"id":"bob","see_all_spots":true});
    let columns = vec!["see_all_spots".into()];
    let alice = member();
    let mut bob = member();
    Arc::make_mut(&mut bob.direct).insert("member.id".into(), "bob".into());
    for _ in 0..2 {
        assert!(!p.principal_change_concerns(&alice, "members", "update", &columns, &old, &new));
        assert!(p.principal_change_concerns(&bob, "members", "update", &columns, &old, &new));
    }
}

#[test]
fn equality_anchors_follow_aliases_and_explicit_left_alias_only() {
    let set: VisibilitySet = serde_json::from_value(json!({
        "kind":"grant","table":"tasks","alias":"task","select":"spotId",
        "joins":[{"table":"members","alias":"owner","leftColumn":"createdBy","rightColumn":"id"},
            {"table":"members","alias":"viewer","leftAlias":"task","leftColumn":"assignedTo","rightColumn":"id"},
            {"table":"memberTeams","alias":"viewerTeams","leftColumn":"id","rightColumn":"memberId"}],
        "where":[{"table":"viewerTeams","column":"memberId","context":"member.id"}]
    })).unwrap();
    let anchors = principal_anchors(&set);
    assert!(anchors["viewer"].contains(&("id".into(), "member.id".into())));
    assert!(anchors["task"].contains(&("assignedTo".into(), "member.id".into())));
    assert!(!anchors.contains_key("owner"));
}

#[test]
fn acl_ancestry_moves_keep_the_conservative_resync_path() {
    let mut p = plan();
    p.permission_tables.push("spots".into());
    p.sets.insert(
        "descendants".into(),
        serde_json::from_value(json!({
            "kind":"principal","table":"spots","alias":"child","select":"_id",
            "joins":[{"table":"spots","alias":"parent","leftColumn":"parentId","rightColumn":"_id"},
                {"table":"memberSpots","leftColumn":"_id","rightColumn":"spotId"}],
            "where":[{"table":"memberSpots","column":"memberId","context":"member.id"}]
        }))
        .unwrap(),
    );
    p.predicate.children.push(
        serde_json::from_value(json!({"operator":"inSet","column":"_id","set":"descendants"}))
            .unwrap(),
    );
    p.validate().unwrap();
    assert!(concerns(
        &p,
        "spots",
        "update",
        "parentId",
        json!({"_id":"child","parentId":"a"}),
        json!({"_id":"child","parentId":"b"})
    ));
    assert!(concerns(
        &p,
        "spots",
        "delete",
        "",
        json!({"_id":"parent"}),
        Value::Null
    ));
    assert!(!concerns(
        &p,
        "spots",
        "update",
        "name",
        json!({"_id":"child","name":"before"}),
        json!({"_id":"child","name":"after"})
    ));
}
