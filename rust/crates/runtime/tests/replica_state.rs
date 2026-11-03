//! Replica subscriptions hold no rows: what every client ends up holding is
//! modelled by the SDK client model and compared with a fresh SQL snapshot
//! (the oracle) after every transaction, resume and principal change, on the
//! shared Whagons fixture (small shape).
//!
//!   cargo test -p gonvex-runtime --features perf-fixture --test replica_state

#[path = "support/replica_client.rs"]
mod replica_client;

use std::collections::BTreeMap;

use gonvex_protocol::ServerMessage;
use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberKind, SqlValue, WHAGONS_SUBSCRIPTIONS,
};
use gonvex_runtime::replica::ReplicaSubscription;
use gonvex_store::TenantSession;
use replica_client::{ClientModel, DigestKind};
use serde_json::{json, Value};

const TASKS: &str = "tasks.recentWorkspaceTasks";

struct Client {
    member: usize,
    /// Old clients never apply the connection watermark: their windows only
    /// advance with a delta or ready.
    watermarks: bool,
    session: TenantSession,
    subscriptions: BTreeMap<String, ReplicaSubscription>,
    model: ClientModel,
}

impl Client {
    async fn connect(fixture: &Fixture, member: usize) -> Self {
        let mut client = Self {
            member,
            watermarks: true,
            session: fixture.session(0, member),
            subscriptions: BTreeMap::new(),
            model: ClientModel::new(Some(DigestKind::Xor)),
        };
        for path in WHAGONS_SUBSCRIPTIONS {
            client.model.register(path, path, "", json!({}));
            client.reopen(fixture, path).await;
        }
        client
    }

    async fn reopen(&mut self, fixture: &Fixture, id: &str) -> Vec<ServerMessage> {
        let path = self.model.windows[id].path.clone();
        let request = self.model.open_request(id, &path, &json!({}));
        let opened = fixture
            .runtime
            .open_replica(&self.session, request)
            .await
            .unwrap();
        match opened.subscription {
            Some(subscription) => {
                self.model.windows.get_mut(id).unwrap().entity =
                    subscription.definition.table.clone();
                self.subscriptions.insert(id.to_owned(), subscription);
            }
            None => {
                self.subscriptions.remove(id);
            }
        }
        self.model.apply_all(&opened.messages);
        opened.messages
    }

    async fn apply(&mut self, fixture: &Fixture, event: &FeedEvent) -> Vec<ServerMessage> {
        let mut messages = fixture
            .runtime
            .apply_feed_event(&self.session, &mut self.subscriptions, event.clone())
            .await
            .unwrap();
        self.model.apply_all(&messages);
        if let FeedEvent::Transaction { revision, .. } = event {
            if self.watermarks {
                self.model.watermark(*revision);
            }
        }
        for _ in 0..3 {
            let pending = self.model.pending_reopens();
            if pending.is_empty() {
                break;
            }
            for id in pending {
                messages.extend(self.reopen(fixture, &id).await);
            }
        }
        messages
    }

    /// Reopens every collection from the client's cursor (a reconnect).
    async fn resume(&mut self, fixture: &Fixture) -> Vec<ServerMessage> {
        self.subscriptions.clear();
        let mut messages = Vec::new();
        for path in WHAGONS_SUBSCRIPTIONS {
            messages.extend(self.reopen(fixture, path).await);
        }
        for id in self.model.pending_reopens() {
            messages.extend(self.reopen(fixture, &id).await);
        }
        messages
    }

    async fn oracle(&self, fixture: &Fixture, context: &str) {
        assert!(
            self.model.mismatches.is_empty(),
            "member {} {context}: digest mismatches {:?}",
            self.member,
            self.model.mismatches
        );
        for (id, window) in &self.model.windows {
            let opened = fixture
                .runtime
                .open_replica(
                    &self.session,
                    serde_json::from_value(json!({"id": id, "path": window.path, "args": {}}))
                        .unwrap(),
                )
                .await
                .unwrap();
            let Some(ServerMessage::ReplicaSnapshot { result, key, .. }) = opened.messages.first()
            else {
                panic!("{id}: no snapshot")
            };
            let expected = result
                .iter()
                .map(|row| (replica_client::key_of(row, key), Value::clone(row)))
                .collect::<BTreeMap<_, _>>();
            let leaked = window
                .rows
                .keys()
                .filter(|key| !expected.contains_key(*key))
                .collect::<Vec<_>>();
            let missing = expected
                .keys()
                .filter(|key| !window.rows.contains_key(*key))
                .collect::<Vec<_>>();
            let stale = expected
                .iter()
                .filter(|(key, row)| window.rows.get(*key).is_some_and(|held| held != *row))
                .map(|(key, _)| key)
                .collect::<Vec<_>>();
            assert!(
                leaked.is_empty() && missing.is_empty() && stale.is_empty(),
                "member {} {context} {id}: leaked {leaked:?} missing {missing:?} stale {stale:?}",
                self.member
            );
            if let Some(subscription) = self.subscriptions.get(id) {
                assert_eq!(
                    subscription.row_count(),
                    window.rows.len(),
                    "{context} {id}"
                );
            }
        }
    }
}

async fn commit(fixture: &Fixture, statement: &str, binds: Vec<SqlValue>) -> FeedEvent {
    fixture.commit(0, statement, binds).await.unwrap()
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clients_hold_exactly_the_visible_rows_through_changes_resumes_and_principal_changes() {
    let fixture = Fixture::build(FixtureOptions::small()).await.unwrap();
    let tenant = fixture.tenant(0);
    let kinds = [
        MemberKind::Admin,
        MemberKind::Manager,
        MemberKind::TeamAllSpots,
        MemberKind::TeamSpots,
        MemberKind::GrantOnly,
    ];
    let mut clients = Vec::new();
    for kind in kinds {
        for member in fixture.members_of_kind(0, kind).into_iter().take(2) {
            clients.push(Client::connect(&fixture, member).await);
        }
    }
    for client in &clients {
        client.oracle(&fixture, "open").await;
    }
    let grant_member = tenant.members[fixture.members_of_kind(0, MemberKind::GrantOnly)[0]].clone();
    let spot_member_index = fixture.members_of_kind(0, MemberKind::TeamSpots)[0];
    let spot_member = tenant.members[spot_member_index].clone();
    let other_team = tenant
        .members
        .iter()
        .flat_map(|member| member.teams.iter())
        .find(|team| !spot_member.teams.contains(team))
        .cloned()
        .unwrap();
    let tasks = tenant.tasks.clone();
    let workspaces = tasks
        .iter()
        .map(|task| task.workspace_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let spotted = tasks
        .iter()
        .find(|task| task.spot_id.is_some())
        .unwrap()
        .clone();
    let other_workspace = workspaces
        .iter()
        .find(|workspace| **workspace != tasks[0].workspace_id)
        .unwrap()
        .clone();
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
            "presence beat",
            r#"UPDATE "userPresence" SET "lastSeenAt"=7, "state"='away' WHERE "userId"=$1"#.into(),
            vec![text(&grant_member.id)],
        ),
    ];
    for (name, statement, binds) in steps {
        let event = commit(&fixture, &statement, binds).await;
        for client in &mut clients {
            client.apply(&fixture, &event).await;
        }
        for client in &clients {
            client.oracle(&fixture, name).await;
        }
    }

    // A subscription whose visible rows did not change receives nothing for
    // that revision (no ready); an old client (no watermark) keeps its older
    // cursor and still resumes correctly from it.
    let grant_index = clients
        .iter()
        .position(|client| client.member == fixture.members_of_kind(0, MemberKind::GrantOnly)[0])
        .unwrap();
    clients[grant_index].watermarks = false;
    let held = clients[grant_index].model.windows[TASKS].rows.clone();
    let hidden = tasks
        .iter()
        .skip(10)
        .find(|task| !held.contains_key(&task.id))
        .unwrap()
        .clone();
    let cursor_before = clients[grant_index].model.windows[TASKS].cursor.clone();
    let event = commit(
        &fixture,
        r#"UPDATE "tasks" SET "description"='unseen edit' WHERE "_id"=$1"#,
        vec![text(&hidden.id)],
    )
    .await;
    let mut admin_messages = 0;
    for (index, client) in clients.iter_mut().enumerate() {
        let messages = client.apply(&fixture, &event).await;
        if index == grant_index {
            assert!(
                messages.iter().all(|message| !matches!(
                    message,
                    ServerMessage::ReplicaReady { .. } | ServerMessage::ReplicaDelta { .. }
                )),
                "an unaffected subscription must receive nothing: {messages:?}"
            );
        } else if fixture.tenant(0).members[client.member].kind == MemberKind::Admin {
            admin_messages += messages
                .iter()
                .filter(|message| matches!(message, ServerMessage::ReplicaDelta { id, .. } if id == TASKS))
                .count();
        }
    }
    assert!(admin_messages > 0, "an admin sees every task");
    assert_eq!(
        clients[grant_index].model.windows[TASKS].cursor,
        cursor_before
    );
    for client in &clients {
        client.oracle(&fixture, "unseen task edit").await;
    }
    let mut unaffected = clients.remove(grant_index);
    unaffected.subscriptions.clear();
    for (statement, binds) in [
        (
            r#"UPDATE "tasks" SET "description"='unseen again' WHERE "_id"=$1"#,
            vec![text(&hidden.id)],
        ),
        (
            r#"UPDATE "tasks" SET "description"='seen while offline' WHERE "_id"=$1"#,
            vec![text(held.keys().next().unwrap_or(&hidden.id))],
        ),
    ] {
        let event = commit(&fixture, statement, binds).await;
        for client in &mut clients {
            client.apply(&fixture, &event).await;
        }
    }
    let resumed = unaffected.resume(&fixture).await;
    unaffected
        .oracle(&fixture, "resume from a cursor older than unseen revisions")
        .await;
    assert!(
        !resumed
            .iter()
            .any(|message| matches!(message, ServerMessage::ReplicaSnapshot { .. })),
        "an older cursor within retention resumes without a snapshot"
    );

    // Resume from the client's cursor after missed changes.
    let mut offline = clients.remove(0);
    let mut offline_two = clients.remove(0);
    for (statement, binds) in [
        (
            r#"UPDATE "tasks" SET "description"='while offline' WHERE "_id"=$1"#,
            vec![text(&tasks[4].id)],
        ),
        (
            r#"DELETE FROM "tasks" WHERE "_id"=$1"#,
            vec![text(&tasks[5].id)],
        ),
        (
            r#"UPDATE "tasks" SET "workspaceId"=$2 WHERE "_id"=$1"#,
            vec![text(&tasks[6].id), text(&other_workspace)],
        ),
    ] {
        let event = commit(&fixture, statement, binds).await;
        for client in &mut clients {
            client.apply(&fixture, &event).await;
        }
        for client in &clients {
            client.oracle(&fixture, statement).await;
        }
    }
    let resumed = offline.resume(&fixture).await;
    offline.oracle(&fixture, "resume within retention").await;
    assert!(
        !resumed
            .iter()
            .any(|message| matches!(message, ServerMessage::ReplicaSnapshot { .. })),
        "a cursor within retention resumes without a snapshot"
    );
    // Many source changes since the cursor resume from the log alone.
    let mut quiet = clients.remove(0);
    quiet.subscriptions.clear();
    for i in 0..200 {
        let _ = commit(
            &fixture,
            r#"UPDATE "tasks" SET "description"=$2, "updatedAt"=$3 WHERE "_id"=$1"#,
            vec![
                text(&tasks[(i * 37) % tasks.len()].id),
                text(&format!("bulk {i}")),
                SqlValue::Int(i as i64),
            ],
        )
        .await;
    }
    let resumed = quiet.resume(&fixture).await;
    quiet.oracle(&fixture, "resume after 200 changes").await;
    assert_eq!(
        quiet.model.need_hashes, 0,
        "description edits since the cursor resume without hashes"
    );
    let _ = resumed;
    // A client whose state diverged is detected by the digest and repaired.
    offline
        .model
        .windows
        .get_mut(TASKS)
        .unwrap()
        .rows
        .insert("phantom".into(), json!({"_id": "phantom"}));
    let resumed = offline.resume(&fixture).await;
    assert!(resumed
        .iter()
        .any(|message| matches!(message, ServerMessage::ReplicaNeedHashes { .. })));
    offline.oracle(&fixture, "repaired divergence").await;
    // A cursor outside retention falls back to a snapshot.
    let _ = commit(
        &fixture,
        r#"UPDATE "tasks" SET "description"='after prune' WHERE "_id"=$1"#,
        vec![text(&tasks[7].id)],
    )
    .await;
    let _ = commit(
        &fixture,
        r#"UPDATE "_gonvex_sync_clock" SET "retained_revision"="revision" WHERE singleton=1"#,
        Vec::new(),
    )
    .await;
    let resumed = offline_two.resume(&fixture).await;
    offline_two
        .oracle(&fixture, "resume outside retention")
        .await;
    assert!(resumed
        .iter()
        .any(|message| matches!(message, ServerMessage::ReplicaSnapshot { .. })));
    fixture.shutdown().await;
}
