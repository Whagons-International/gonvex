//! Shared snapshot cache oracle: a cached base at revision R plus the
//! change-log delta R..clock equals a fresh SQL snapshot at the clock, row for
//! row (in order for ordered collections) and hash for hash (inline hashes,
//! aggregate digest, window), for every member and collection of the fixture:
//! after writes, deletes, inserts, visibility moves and principal changes,
//! with writes landing during opens, through roll-forwards, and when the
//! entry is older than the log's retention (fresh fallback, then rebuild).
//!
//!   cargo test -p gonvex-runtime --features perf-fixture --test snapshot_cache_oracle

use std::collections::BTreeMap;

use gonvex_protocol::ServerMessage;
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberKind, SqlValue, WHAGONS_SUBSCRIPTIONS,
};
use gonvex_store::TenantSession;
use serde_json::{json, Value};

struct Opened {
    keys: Vec<String>,
    rows: BTreeMap<String, Value>,
    cursor: Value,
    metadata: BTreeMap<String, Value>,
    ready_digest: String,
    window_rows: usize,
    truncated: bool,
    ordered: bool,
}

fn key_of(row: &Value, key: &str) -> String {
    match &row[key] {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn opened(result: gonvex_runtime::replica::ReplicaOpenResult) -> Opened {
    let subscription = result.subscription.expect("subscribed");
    let mut opened = Opened {
        keys: Vec::new(),
        rows: BTreeMap::new(),
        cursor: Value::Null,
        metadata: BTreeMap::new(),
        ready_digest: String::new(),
        window_rows: subscription.row_count(),
        truncated: subscription.truncated(),
        ordered: !subscription.definition.order_by.is_empty(),
    };
    for message in result.messages {
        match message {
            ServerMessage::ReplicaSnapshot {
                result,
                cursor,
                key,
                metadata,
                ..
            } => {
                for row in result {
                    let row = row.to_value();
                    let id = key_of(&row, &key);
                    opened.keys.push(id.clone());
                    opened.rows.insert(id, row);
                }
                opened.cursor = serde_json::to_value(cursor).unwrap();
                opened.metadata = metadata;
            }
            ServerMessage::ReplicaReady { digest, .. } => opened.ready_digest = digest,
            other => panic!("unexpected open message {other:?}"),
        }
    }
    opened
}

fn request(path: &str) -> gonvex_protocol::ReplicaOpenRequest {
    serde_json::from_value(json!({"id": path, "path": path, "args": {}})).unwrap()
}

/// Cached open vs fresh open. Returns false when a concurrent write put them
/// at different clocks (nothing to compare).
async fn compare(fixture: &Fixture, session: &TenantSession, path: &str, context: &str) -> bool {
    let runtime = &fixture.runtime;
    let cached = opened(runtime.open_replica(session, request(path)).await.unwrap());
    let fresh = opened(
        runtime
            .open_replica_fresh(session, request(path))
            .await
            .unwrap(),
    );
    if cached.cursor != fresh.cursor {
        return false;
    }
    let who = &session.member.id;
    let leaked = cached
        .rows
        .keys()
        .filter(|key| !fresh.rows.contains_key(*key))
        .collect::<Vec<_>>();
    let missing = fresh
        .rows
        .keys()
        .filter(|key| !cached.rows.contains_key(*key))
        .collect::<Vec<_>>();
    let stale = fresh
        .rows
        .iter()
        .filter(|(key, row)| cached.rows.get(*key).is_some_and(|held| held != *row))
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    assert!(
        leaked.is_empty() && missing.is_empty() && stale.is_empty(),
        "{context} {who} {path}: leaked {leaked:?} missing {missing:?} stale {stale:?}"
    );
    if fresh.ordered {
        assert_eq!(cached.keys, fresh.keys, "{context} {who} {path}: order");
    }
    assert_eq!(
        cached.metadata, fresh.metadata,
        "{context} {who} {path}: metadata (digests, hashes)"
    );
    assert_eq!(
        cached.ready_digest, fresh.ready_digest,
        "{context} {who} {path}: digest"
    );
    assert_eq!(
        cached.window_rows, fresh.window_rows,
        "{context} {who} {path}: window rows"
    );
    assert_eq!(
        cached.truncated, fresh.truncated,
        "{context} {who} {path}: truncated"
    );
    true
}

async fn compare_all(fixture: &Fixture, sessions: &[TenantSession], context: &str) -> usize {
    let mut compared = 0;
    for session in sessions {
        for path in WHAGONS_SUBSCRIPTIONS {
            compared += usize::from(compare(fixture, session, path, context).await);
        }
    }
    compared
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_base_plus_log_delta_equals_a_fresh_snapshot() {
    // Roll entries forward after a few log rows so the oracle covers
    // roll-forwards too (read once per process: set before the first open).
    std::env::set_var("GONVEX_SNAPSHOT_CACHE_ROLL_CHANGES", "3");
    let fixture = Fixture::build(FixtureOptions::small()).await.unwrap();
    let tenant = fixture.tenant(0);
    let sessions = (0..tenant.members.len())
        .map(|member| fixture.session(0, member))
        .collect::<Vec<_>>();
    let compared = compare_all(&fixture, &sessions, "open").await;
    assert_eq!(compared, sessions.len() * WHAGONS_SUBSCRIPTIONS.len());
    let stats = fixture.runtime.snapshot_cache_stats();
    assert!(stats.builds > 0 && stats.hits > 0, "{stats:?}");
    assert_eq!(
        stats.fallbacks, 0,
        "every open answered from the cache: {stats:?}"
    );

    let grant_member = tenant.members[fixture.members_of_kind(0, MemberKind::GrantOnly)[0]].clone();
    let spot_member = tenant.members[fixture.members_of_kind(0, MemberKind::TeamSpots)[0]].clone();
    let other_team = tenant
        .members
        .iter()
        .flat_map(|member| member.teams.iter())
        .find(|team| !spot_member.teams.contains(team))
        .cloned()
        .unwrap();
    let tasks = tenant.tasks.clone();
    let spotted = tasks
        .iter()
        .find(|task| task.spot_id.is_some())
        .unwrap()
        .clone();
    let other_workspace = tasks
        .iter()
        .map(|task| task.workspace_id.clone())
        .find(|workspace| *workspace != tasks[0].workspace_id)
        .unwrap();
    let steps: Vec<(&str, String, Vec<SqlValue>)> = vec![
        (
            "update description",
            r#"UPDATE "tasks" SET "description"='edited', "updatedAt"=2 WHERE "_id"=$1"#.into(),
            vec![text(&tasks[0].id)],
        ),
        (
            "move task to another workspace",
            r#"UPDATE "tasks" SET "workspaceId"=$2 WHERE "_id"=$1"#.into(),
            vec![text(&tasks[0].id), text(&other_workspace)],
        ),
        (
            "move spotted task out of its spot",
            r#"UPDATE "tasks" SET "spotId"=NULL WHERE "_id"=$1"#.into(),
            vec![text(&spotted.id)],
        ),
        (
            "insert task",
            r#"INSERT INTO "tasks"("_id","id","workspaceId","statusId","name","createdAt","updatedAt","_creationTime") VALUES('new-task',999999,$1,$2,'New',1,1,1)"#.into(),
            vec![text(&tasks[1].workspace_id), text(&tasks[1].status_id)],
        ),
        (
            "assign grant-only member",
            r#"INSERT INTO "taskUsers"("_id","taskId","userId","workspaceId","createdAt","updatedAt") VALUES('assign-1',$1,$2,$3,1,1)"#.into(),
            vec![text(&tasks[2].id), text(&grant_member.id), text(&tasks[2].workspace_id)],
        ),
        (
            "edit assigned task",
            r#"UPDATE "tasks" SET "name"='assigned edit' WHERE "_id"=$1"#.into(),
            vec![text(&tasks[2].id)],
        ),
        (
            "unassign grant-only member",
            r#"UPDATE "taskUsers" SET "deletedAt"=5 WHERE "_id"='assign-1'"#.into(),
            Vec::new(),
        ),
        (
            "member gains a team",
            r#"INSERT INTO "memberTeams"("_id","_creationTime","memberId","userId","teamId","roleKey","createdAt","updatedAt") VALUES('toggle-team',1,$1,$1,$2,'member',1,1)"#.into(),
            vec![text(&spot_member.id), text(&other_team)],
        ),
        (
            "edit while the member has the team",
            r#"UPDATE "tasks" SET "name"='team edit' WHERE "_id"=$1"#.into(),
            vec![text(&tasks[5].id)],
        ),
        (
            "member loses the team",
            r#"DELETE FROM "memberTeams" WHERE "_id"='toggle-team'"#.into(),
            Vec::new(),
        ),
        (
            "delete task",
            r#"DELETE FROM "tasks" WHERE "_id"=$1"#.into(),
            vec![text(&tasks[3].id)],
        ),
        (
            "soft-delete task",
            r#"UPDATE "tasks" SET "deletedAt"=9 WHERE "_id"=$1"#.into(),
            vec![text(&tasks[4].id)],
        ),
        (
            "presence beat",
            r#"UPDATE "userPresence" SET "lastSeenAt"=7, "state"='away' WHERE "userId"=$1"#.into(),
            vec![text(&grant_member.id)],
        ),
    ];
    for (name, statement, binds) in steps {
        fixture.commit(0, &statement, binds).await.unwrap();
        let compared = compare_all(&fixture, &sessions, name).await;
        assert_eq!(
            compared,
            sessions.len() * WHAGONS_SUBSCRIPTIONS.len(),
            "{name}"
        );
    }
    let stats = fixture.runtime.snapshot_cache_stats();
    eprintln!("after steps: {stats:?}");
    assert!(stats.roll_forwards > 0, "{stats:?}");
    assert_eq!(stats.fallbacks, 0, "{stats:?}");

    // Writes landing during opens: a writer commits while members open.
    let writer = {
        let tasks = tasks.clone();
        let fixture = &fixture;
        async move {
            for i in 0..40 {
                fixture
                    .update_task(
                        0,
                        (i * 37) % tasks.len(),
                        json!({"name": format!("race {i}")}),
                    )
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        }
    };
    let readers = async {
        let mut compared = 0;
        for round in 0..3 {
            compared += compare_all(&fixture, &sessions, &format!("racing round {round}")).await;
        }
        compared
    };
    let ((), compared) = tokio::join!(writer, readers);
    eprintln!("racing: {compared} comparisons at equal clocks");
    assert!(compared > 0);

    // An entry older than the log's retention falls back to a fresh build.
    let before = fixture.runtime.snapshot_cache_stats();
    fixture
        .update_task(0, 7, json!({"name": "before retention"}))
        .await
        .unwrap();
    fixture
        .commit(
            0,
            "UPDATE _gonvex_sync_clock SET retained_revision = revision WHERE singleton = 1",
            Vec::new(),
        )
        .await
        .unwrap();
    let session = &sessions[0];
    assert!(compare(&fixture, session, "tasks.recentWorkspaceTasks", "retention").await);
    let after = fixture.runtime.snapshot_cache_stats();
    assert!(
        after.fallbacks > before.fallbacks,
        "{before:?} -> {after:?}"
    );
    assert!(compare(&fixture, session, "tasks.recentWorkspaceTasks", "rebuilt").await);
    let rebuilt = fixture.runtime.snapshot_cache_stats();
    assert!(rebuilt.builds > after.builds, "{after:?} -> {rebuilt:?}");
    assert_eq!(rebuilt.fallbacks, after.fallbacks, "{rebuilt:?}");
    let compared = compare_all(&fixture, &sessions, "after retention").await;
    assert_eq!(compared, sessions.len() * WHAGONS_SUBSCRIPTIONS.len());
}
