//! Shared principal resolutions (`principal_cache`) oracle: an open that
//! reuses a member's resolution from an earlier open (same revision, or a
//! newer one after a change-log probe) returns exactly the rows, hashes and
//! digests of an open that resolves with SQL, for every member and every
//! cold-login and detail-window collection of the fixture: after data
//! writes, grant writes (assign / unassign / share), team and spot changes,
//! role and permission changes, and with writes landing during opens.
//!
//!   cargo test -p gonvex-runtime --features perf-fixture --test principal_cache_oracle

use std::collections::BTreeMap;

use gonvex_protocol::{ReplicaOpenRequest, ServerMessage};
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberKind, SqlValue, WHAGONS_COLD_LOGIN, WHAGONS_DETAIL_WINDOW,
};
use gonvex_store::TenantSession;
use serde_json::{json, Value};

struct Opened {
    rows: BTreeMap<String, Value>,
    keys: Vec<String>,
    cursor: Value,
    metadata: BTreeMap<String, Value>,
    ready_digest: String,
}

fn opened(result: gonvex_runtime::replica::ReplicaOpenResult) -> Opened {
    assert!(result.subscription.is_some(), "subscribed");
    let mut opened = Opened {
        rows: BTreeMap::new(),
        keys: Vec::new(),
        cursor: Value::Null,
        metadata: BTreeMap::new(),
        ready_digest: String::new(),
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
                    let id = match &row[&key] {
                        Value::String(text) => text.clone(),
                        other => other.to_string(),
                    };
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

fn request(path: &str, args: Value) -> ReplicaOpenRequest {
    serde_json::from_value(json!({"id": path, "path": path, "args": args})).unwrap()
}

/// The collections a member opens: cold login plus one task's detail window.
fn collections(task: &str) -> Vec<(&'static str, Value)> {
    WHAGONS_COLD_LOGIN
        .iter()
        .map(|path| (*path, json!({})))
        .chain(
            WHAGONS_DETAIL_WINDOW
                .iter()
                .map(|(path, argument)| (*path, json!({ *argument: task }))),
        )
        .collect()
}

/// Shared-resolution open vs SQL-resolution open (both bypass the snapshot
/// cache, so only the principal differs). False when a concurrent write
/// put them at different clocks.
async fn compare(
    fixture: &Fixture,
    session: &TenantSession,
    path: &str,
    args: &Value,
    context: &str,
) -> bool {
    let runtime = &fixture.runtime;
    runtime.set_principal_cache_enabled(true);
    let shared = opened(
        runtime
            .open_replica_fresh(session, request(path, args.clone()))
            .await
            .unwrap(),
    );
    runtime.set_principal_cache_enabled(false);
    let sql = opened(
        runtime
            .open_replica_fresh(session, request(path, args.clone()))
            .await
            .unwrap(),
    );
    runtime.set_principal_cache_enabled(true);
    if shared.cursor != sql.cursor {
        return false;
    }
    let who = &session.member.id;
    let leaked = shared
        .rows
        .keys()
        .filter(|key| !sql.rows.contains_key(*key))
        .collect::<Vec<_>>();
    let missing = sql
        .rows
        .keys()
        .filter(|key| !shared.rows.contains_key(*key))
        .collect::<Vec<_>>();
    assert!(
        leaked.is_empty() && missing.is_empty(),
        "{context} {who} {path}: leaked {leaked:?} missing {missing:?}"
    );
    assert_eq!(shared.rows, sql.rows, "{context} {who} {path}: rows");
    assert_eq!(shared.keys, sql.keys, "{context} {who} {path}: order");
    assert_eq!(shared.metadata, sql.metadata, "{context} {who} {path}: metadata");
    assert_eq!(
        shared.ready_digest, sql.ready_digest,
        "{context} {who} {path}: digest"
    );
    true
}

async fn compare_all(
    fixture: &Fixture,
    sessions: &[TenantSession],
    task: &str,
    context: &str,
) -> usize {
    let mut compared = 0;
    for session in sessions {
        for (path, args) in collections(task) {
            compared += usize::from(compare(fixture, session, path, &args, context).await);
        }
    }
    compared
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn shared_principals_match_sql_resolution() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let members = fixture.tenant(0).members.clone();
    let sessions = (0..members.len())
        .map(|member| fixture.session(0, member))
        .collect::<Vec<_>>();
    let tasks = fixture.tenant(0).tasks.clone();
    let grant_member = members
        .iter()
        .find(|member| member.kind == MemberKind::GrantOnly)
        .unwrap()
        .clone();
    let spot_member = members
        .iter()
        .find(|member| member.kind == MemberKind::TeamSpots)
        .unwrap()
        .clone();
    let team_member = members
        .iter()
        .find(|member| member.kind == MemberKind::TeamAllSpots)
        .unwrap()
        .clone();
    let other_team: String = {
        let route = &fixture.tenant(0).route;
        let mut transaction =
            gonvex_store::begin_transaction(fixture.pools(), &route.database_url, true)
                .await
                .unwrap();
        sqlx::query_scalar(
            r#"SELECT "_id" FROM "teams" WHERE "_id" NOT IN
               (SELECT "teamId" FROM "memberTeams" WHERE "memberId"=$1) LIMIT 1"#,
        )
        .bind(&spot_member.id)
        .fetch_one(&mut **transaction.transaction())
        .await
        .unwrap()
    };
    let detail_task = tasks[2].id.clone();

    // Baseline, then the same opens again at the same revision (hits).
    assert!(compare_all(&fixture, &sessions, &detail_task, "initial").await > 0);
    let before = fixture.runtime.principal_cache_stats();
    assert!(compare_all(&fixture, &sessions, &detail_task, "repeat").await > 0);
    let after = fixture.runtime.principal_cache_stats();
    assert!(after.hits > before.hits, "{before:?} -> {after:?}");
    assert_eq!(after.misses, before.misses, "{before:?} -> {after:?}");

    let steps: Vec<(&str, String, Vec<SqlValue>)> = vec![
        (
            "update description (tasks: only the workspaces derived set reads it)",
            r#"UPDATE "tasks" SET "description"='edited', "updatedAt"=2 WHERE "_id"=$1"#.into(),
            vec![text(&tasks[0].id)],
        ),
        (
            "assign grant-only member",
            r#"INSERT INTO "taskUsers"("_id","taskId","userId","workspaceId","createdAt","updatedAt") VALUES('assign-1',$1,$2,$3,1,1)"#.into(),
            vec![text(&detail_task), text(&grant_member.id), text(&tasks[2].workspace_id)],
        ),
        (
            "edit assigned task",
            r#"UPDATE "tasks" SET "name"='assigned edit' WHERE "_id"=$1"#.into(),
            vec![text(&detail_task)],
        ),
        (
            "note on the detail task",
            r#"INSERT INTO "taskNotes"("_id","_creationTime","taskId","userId","note") VALUES('note-1',1,$1,$2,'hi')"#.into(),
            vec![text(&detail_task), text(&grant_member.id)],
        ),
        (
            "unassign grant-only member",
            r#"UPDATE "taskUsers" SET "deletedAt"=5 WHERE "_id"='assign-1'"#.into(),
            Vec::new(),
        ),
        (
            "share task with grant-only member",
            r#"INSERT INTO "taskShares"("_id","taskId","sharedToUserId","status","permission","createdAt","updatedAt") VALUES('share-1',$1,$2,'active','view',1,1)"#.into(),
            vec![text(&tasks[3].id), text(&grant_member.id)],
        ),
        (
            "member gains a team",
            r#"INSERT INTO "memberTeams"("_id","_creationTime","memberId","userId","teamId","roleKey","createdAt","updatedAt") VALUES('toggle-team',1,$1,$1,$2,'member',1,1)"#.into(),
            vec![text(&spot_member.id), text(&other_team)],
        ),
        (
            "member loses the team",
            r#"DELETE FROM "memberTeams" WHERE "_id"='toggle-team'"#.into(),
            Vec::new(),
        ),
        (
            "member loses all spots",
            r#"UPDATE "memberSpots" SET "deletedAt"=11 WHERE "memberId"=$1"#.into(),
            vec![text(&spot_member.id)],
        ),
        (
            "member becomes admin",
            r#"UPDATE "members" SET "role"='admin', "permissions"='{"tasks.viewAll":true,"view-users":true}' WHERE "id"=$1"#.into(),
            vec![text(&team_member.id)],
        ),
        (
            "member back to its role",
            r#"UPDATE "members" SET "role"=$2, "permissions"=$3 WHERE "id"=$1"#.into(),
            vec![
                text(&team_member.id),
                text(&team_member.role),
                text(&team_member.permissions.to_string()),
            ],
        ),
        (
            "presence beat (no resolution reads userPresence)",
            r#"UPDATE "userPresence" SET "lastSeenAt"=7, "state"='away' WHERE "userId"=$1"#.into(),
            vec![text(&grant_member.id)],
        ),
    ];
    for (name, statement, binds) in steps {
        let before = fixture.runtime.principal_cache_stats();
        fixture.commit(0, &statement, binds).await.unwrap();
        let compared = compare_all(&fixture, &sessions, &detail_task, name).await;
        let after = fixture.runtime.principal_cache_stats();
        eprintln!("{name}: {compared} comparisons, {after:?}");
        assert_eq!(
            compared,
            sessions.len() * (WHAGONS_COLD_LOGIN.len() + WHAGONS_DETAIL_WINDOW.len()),
            "{name}"
        );
        // Writes to tables a resolution does not read are proven unchanged
        // by the change-log probe; principal and grant writes force SQL.
        let invalidated = after.invalidated - before.invalidated;
        if name.contains("no resolution reads") || name.contains("note on") {
            assert!(after.probed_hits > before.probed_hits, "{name}: {after:?}");
            assert_eq!(invalidated, 0, "{name}: {after:?}");
        } else if name.contains("tasks:") || name.contains("edit") {
            // `workspaces` (derived `activePendingTasks`) once per member.
            assert!(after.probed_hits > before.probed_hits, "{name}: {after:?}");
            assert!(invalidated <= sessions.len() as u64, "{name}: {after:?}");
        } else {
            assert!(invalidated > 0, "{name}: {after:?}");
        }
    }

    // Writes landing during opens.
    let writer = {
        let tasks = tasks.clone();
        let fixture = &fixture;
        let grant_member = grant_member.id.clone();
        async move {
            for i in 0..30 {
                if i % 5 == 0 {
                    fixture
                        .commit(
                            0,
                            r#"INSERT INTO "taskUsers"("_id","taskId","userId","workspaceId","createdAt","updatedAt") VALUES($1,$2,$3,$4,1,1)"#,
                            vec![
                                text(&format!("race-assign-{i}")),
                                text(&tasks[i % tasks.len()].id),
                                text(&grant_member),
                                text(&tasks[i % tasks.len()].workspace_id),
                            ],
                        )
                        .await
                        .unwrap();
                } else {
                    fixture
                        .update_task(0, (i * 37) % tasks.len(), json!({"name": format!("race {i}")}))
                        .await
                        .unwrap();
                }
                tokio::task::yield_now().await;
            }
        }
    };
    let readers = async {
        let mut compared = 0;
        for round in 0..2 {
            compared +=
                compare_all(&fixture, &sessions, &detail_task, &format!("racing {round}")).await;
        }
        compared
    };
    let ((), compared) = tokio::join!(writer, readers);
    eprintln!(
        "racing: {compared} comparisons at equal clocks, {:?}",
        fixture.runtime.principal_cache_stats()
    );
    assert!(compared > 0);
    fixture.shutdown().await;
}
