//! Delivery v3 safety oracle: per-class shared row decisions (`feed_share`)
//! produce exactly the frames each connection computes alone.
//!
//! Two identical connection sets of every member of a Whagons-shaped tenant
//! receive the same committed events; set A with class sharing (decisions
//! reused between members of one visibility class), set B with every
//! connection deciding alone (the delivery v2 path). Every frame of every
//! member must be equal, through edits, inserts, deletes, spot/workspace moves
//! (rows enter and leave visible sets), grants (taskUsers), team membership
//! changes (principal changes) and presence beats. Sharing must actually
//! happen (decisions reused), or the test proves nothing.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test class_sharing_oracle`
use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::feed_share;
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind, SqlValue};
use serde_json::{json, Value};

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn class_shared_decisions_equal_each_connections_own_decisions() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let tenant_members = fixture.tenants[0].members.clone();
    let members = tenant_members.len();
    let mut shared = fixture.connect_all(0).await.unwrap();
    let mut alone = fixture.connect_all(0).await.unwrap();
    let tasks = fixture.tenants[0].tasks.clone();
    let grant_member = tenant_members[fixture.members_of_kind(0, MemberKind::GrantOnly)[0]].clone();
    let spot_member = tenant_members[fixture.members_of_kind(0, MemberKind::TeamSpots)[0]].clone();
    let other_team = tenant_members
        .iter()
        .flat_map(|member| member.teams.iter())
        .find(|team| !spot_member.teams.contains(team))
        .cloned()
        .unwrap();
    let mut events: Vec<(String, FeedEvent)> = Vec::new();
    for round in 0..24usize {
        let task = (round * 7) % tasks.len();
        let other = &tasks[(round * 31 + 5) % tasks.len()];
        let (name, event) = match round % 6 {
            0 | 3 => (
                "edit",
                fixture
                    .update_task(0, task, json!({"description": format!("sharing {round}")}))
                    .await
                    .unwrap(),
            ),
            1 => (
                "spot move",
                fixture
                    .update_task(0, task, json!({"spotId": other.spot_id.clone()}))
                    .await
                    .unwrap(),
            ),
            2 => (
                "workspace move",
                fixture
                    .update_task(0, task, json!({"workspaceId": other.workspace_id.clone()}))
                    .await
                    .unwrap(),
            ),
            4 => (
                "presence",
                fixture.beat_presence(0, round % members).await.unwrap(),
            ),
            _ => (
                "spot cleared",
                fixture
                    .update_task(0, task, json!({"spotId": null}))
                    .await
                    .unwrap(),
            ),
        };
        events.push((format!("{name} {round}"), event));
    }
    let statements: Vec<(&str, String, Vec<SqlValue>)> = vec![
        (
            "insert task",
            r#"INSERT INTO "tasks"("_id","id","workspaceId","statusId","name","createdAt","updatedAt","_creationTime") VALUES('sharing-task',999998,$1,$2,'New',1,1,1)"#.into(),
            vec![text(&tasks[1].workspace_id), text(&tasks[1].status_id)],
        ),
        (
            "assign grant-only member",
            r#"INSERT INTO "taskUsers"("_id","taskId","userId","workspaceId","createdAt","updatedAt") VALUES('sharing-assign',$1,$2,$3,1,1)"#.into(),
            vec![text(&tasks[2].id), text(&grant_member.id), text(&tasks[2].workspace_id)],
        ),
        (
            "edit assigned task",
            r#"UPDATE "tasks" SET "name"='assigned edit' WHERE "_id"=$1"#.into(),
            vec![text(&tasks[2].id)],
        ),
        (
            "unassign",
            r#"UPDATE "taskUsers" SET "deletedAt"=5 WHERE "_id"='sharing-assign'"#.into(),
            Vec::new(),
        ),
        (
            "member gains a team",
            r#"INSERT INTO "memberTeams"("_id","_creationTime","memberId","userId","teamId","roleKey","createdAt","updatedAt") VALUES('sharing-team',1,$1,$1,$2,'member',1,1)"#.into(),
            vec![text(&spot_member.id), text(&other_team)],
        ),
        (
            "edit after team change",
            r#"UPDATE "tasks" SET "name"='after team' WHERE "_id"=$1"#.into(),
            vec![text(&tasks[4].id)],
        ),
        (
            "member loses the team",
            r#"DELETE FROM "memberTeams" WHERE "_id"='sharing-team'"#.into(),
            Vec::new(),
        ),
        (
            "delete task",
            r#"DELETE FROM "tasks" WHERE "_id"=$1"#.into(),
            vec![text(&tasks[3].id)],
        ),
    ];
    for (name, statement, binds) in statements {
        events.push((
            name.to_owned(),
            fixture.commit(0, &statement, binds).await.unwrap(),
        ));
    }
    let before = feed_share::shared_decisions();
    let mut frames = 0usize;
    for (name, event) in &events {
        for member in 0..members {
            feed_share::set_sharing(Some(true));
            let with = fixture
                .runtime
                .apply_feed_event(
                    &shared[member].session.clone(),
                    &mut shared[member].subscriptions,
                    event.clone(),
                )
                .await
                .unwrap();
            feed_share::set_sharing(Some(false));
            let without = fixture
                .runtime
                .apply_feed_event(
                    &alone[member].session.clone(),
                    &mut alone[member].subscriptions,
                    event.clone(),
                )
                .await
                .unwrap();
            let with: Vec<Value> = with
                .iter()
                .map(|m| serde_json::to_value(m).unwrap())
                .collect();
            let without: Vec<Value> = without
                .iter()
                .map(|m| serde_json::to_value(m).unwrap())
                .collect();
            assert_eq!(
                with, without,
                "member {member} ({:?}), event {name}",
                tenant_members[member].kind
            );
            frames += with.len();
        }
    }
    feed_share::set_sharing(None);
    let reused = feed_share::shared_decisions() - before;
    eprintln!(
        "{} events, {frames} frames, {reused} shared decisions",
        events.len()
    );
    assert!(frames > 0);
    assert!(
        reused > 0,
        "no decision was shared: the oracle exercised nothing"
    );
}
