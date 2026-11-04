//! Whagons visibility v1 → v2 parity oracle, through the real runtime.
//!
//! Two runtimes over identical Whagons-shaped tenants (the shared perf
//! fixture: real schema, real collection definitions): one declares the
//! legacy plans Whagons staging deployed (`whagons_visibility_legacy.json`),
//! the other the version 2 masks of the same tables (`whagons_module.json`).
//! Every member connects to every fixture collection in both. Then the same
//! targeted writes commit to both (grants, shares, approvals, contexts,
//! team/spot/role/flag changes, spot-tree moves, personal spaces, soft and
//! hard deletes, inserts, tenant settings). After the open and after every
//! write, for every member and collection:
//!
//! - v1 client state (legacy runtime: SQL-resolved sets, interpreted rows)
//!   equals v2 client state (compiled masks on deltas);
//! - each equals a fresh SQL snapshot of its own runtime (v1-SQL, v2-SQL);
//! - the v2 runtime answered every change row with compiled programs
//!   (`fallback_row_checks == 0`).
//!
//! It also prints the per-write visibility counters of both runtimes (SQL
//! statements, collection snapshots, principal rebuilds), which are
//! deterministic.
//!
//!   cargo test -p gonvex-runtime --features perf-fixture --test visibility_v2_parity -- --nocapture
#[path = "support/replica_client.rs"]
mod replica_client;

use std::collections::BTreeMap;

use gonvex_protocol::ServerMessage;
use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, FixtureVisibility, MemberKind, SqlValue, TenantSpec,
    WHAGONS_SUBSCRIPTIONS,
};
use gonvex_runtime::replica::ReplicaSubscription;
use gonvex_runtime::visibility_metrics::VisibilityMetricsSnapshot;
use gonvex_store::TenantSession;
use replica_client::{ClientModel, DigestKind};
use serde_json::{json, Value};

struct Client {
    member: usize,
    session: TenantSession,
    subscriptions: BTreeMap<String, ReplicaSubscription>,
    model: ClientModel,
}

impl Client {
    async fn connect(fixture: &Fixture, member: usize) -> Self {
        let mut client = Self {
            member,
            session: fixture.session(0, member),
            subscriptions: BTreeMap::new(),
            model: ClientModel::new(Some(DigestKind::Xor)),
        };
        for path in WHAGONS_SUBSCRIPTIONS {
            client.model.register(path, path, "", json!({}));
            client.reopen(fixture, path).await;
        }
        client.settle(fixture).await;
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

    async fn settle(&mut self, fixture: &Fixture) {
        for _ in 0..3 {
            let pending = self.model.pending_reopens();
            if pending.is_empty() {
                return;
            }
            for id in pending {
                self.reopen(fixture, &id).await;
            }
        }
    }

    async fn apply(&mut self, fixture: &Fixture, event: &FeedEvent) {
        let messages = fixture
            .runtime
            .apply_feed_event(&self.session, &mut self.subscriptions, event.clone())
            .await
            .unwrap();
        self.model.apply_all(&messages);
        if let FeedEvent::Transaction { revision, .. } = event {
            self.model.watermark(*revision);
        }
        self.settle(fixture).await;
    }

    /// Fresh SQL snapshot of every collection (the runtime's own oracle).
    async fn snapshot(&self, fixture: &Fixture) -> BTreeMap<String, BTreeMap<String, Value>> {
        let mut out = BTreeMap::new();
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
            out.insert(
                id.clone(),
                result
                    .iter()
                    .map(|row| (replica_client::key_of(row, key), Value::clone(row)))
                    .collect(),
            );
        }
        out
    }
}

fn diff(
    left: &BTreeMap<String, Value>,
    right: &BTreeMap<String, Value>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let only_left = left
        .keys()
        .filter(|key| !right.contains_key(*key))
        .cloned()
        .collect();
    let only_right = right
        .keys()
        .filter(|key| !left.contains_key(*key))
        .cloned()
        .collect();
    let changed = left
        .iter()
        .filter(|(key, row)| right.get(*key).is_some_and(|other| other != *row))
        .map(|(key, _)| key.clone())
        .collect();
    (only_left, only_right, changed)
}

struct Side {
    fixture: Fixture,
    clients: Vec<Client>,
}

struct Pair {
    legacy: Side,
    v2: Side,
    rows_compared: usize,
    report: Vec<(String, VisibilityMetricsSnapshot, VisibilityMetricsSnapshot)>,
}

fn delta(
    after: VisibilityMetricsSnapshot,
    before: VisibilityMetricsSnapshot,
) -> VisibilityMetricsSnapshot {
    VisibilityMetricsSnapshot {
        visibility_sql_statements: after.visibility_sql_statements
            - before.visibility_sql_statements,
        collection_snapshots: after.collection_snapshots - before.collection_snapshots,
        live_query_reruns: after.live_query_reruns - before.live_query_reruns,
        principal_rebuilds: after.principal_rebuilds - before.principal_rebuilds,
        consistency_rebuilds: after.consistency_rebuilds - before.consistency_rebuilds,
        in_memory_row_checks: after.in_memory_row_checks - before.in_memory_row_checks,
        compiled_row_checks: after.compiled_row_checks - before.compiled_row_checks,
        class_evaluations: after.class_evaluations - before.class_evaluations,
        member_evaluations: after.member_evaluations - before.member_evaluations,
        fallback_row_checks: after.fallback_row_checks - before.fallback_row_checks,
    }
}

/// One write, as a statement or a row clone, applied identically to both.
enum Write<'a> {
    Sql(&'a str, Vec<SqlValue>),
    InsertLike(&'a str, &'a str, &'a str, Vec<(&'a str, SqlValue)>),
}

impl Pair {
    async fn check(&mut self, context: &str) {
        for (legacy, v2) in self.legacy.clients.iter().zip(&self.v2.clients) {
            assert_eq!(legacy.member, v2.member);
            for (name, client) in [("legacy", legacy), ("v2", v2)] {
                assert!(
                    client.model.mismatches.is_empty(),
                    "{name} member {} {context}: digest mismatches {:?}",
                    client.member,
                    client.model.mismatches
                );
            }
            let fresh_legacy = legacy.snapshot(&self.legacy.fixture).await;
            let fresh_v2 = v2.snapshot(&self.v2.fixture).await;
            for id in WHAGONS_SUBSCRIPTIONS {
                let held_legacy = &legacy.model.windows[id].rows;
                let held_v2 = &v2.model.windows[id].rows;
                for (what, left, right) in [
                    ("v1 SQL vs v2 SQL", &fresh_legacy[id], &fresh_v2[id]),
                    ("v1 client vs v2 client", held_legacy, held_v2),
                    ("v1 client vs v1 SQL", held_legacy, &fresh_legacy[id]),
                    ("v2 client vs v2 SQL", held_v2, &fresh_v2[id]),
                ] {
                    let (only_left, only_right, changed) = diff(left, right);
                    assert!(
                        only_left.is_empty() && only_right.is_empty() && changed.is_empty(),
                        "{context}: member {} ({:?}) {id}: {what}: only left {only_left:?}, only right {only_right:?}, changed {changed:?}",
                        legacy.member,
                        self.legacy.fixture.tenant(0).members[legacy.member].kind,
                    );
                }
                self.rows_compared += fresh_v2[id].len();
            }
        }
    }

    async fn write(&mut self, name: &str, write: Write<'_>) {
        let mut metrics = Vec::new();
        for side in [&mut self.legacy, &mut self.v2] {
            let before = side.fixture.runtime.visibility_metrics().snapshot();
            let event = match &write {
                Write::Sql(statement, binds) => side
                    .fixture
                    .commit(0, statement, binds.clone())
                    .await
                    .unwrap(),
                Write::InsertLike(table, key, source, overrides) => side
                    .fixture
                    .insert_like(0, table, key, source, overrides)
                    .await
                    .unwrap(),
            };
            for client in side.clients.iter_mut() {
                client.apply(&side.fixture, &event).await;
            }
            metrics.push(delta(
                side.fixture.runtime.visibility_metrics().snapshot(),
                before,
            ));
        }
        assert_eq!(
            metrics[1].fallback_row_checks, 0,
            "{name}: v2 rows must be answered by compiled programs"
        );
        self.report.push((name.to_owned(), metrics[0], metrics[1]));
        let started = std::time::Instant::now();
        self.check(name).await;
        eprintln!(
            "parity: {name}: ok (v1 sql {}, v2 sql {}; checked in {:.1}s)",
            metrics[0].visibility_sql_statements,
            metrics[1].visibility_sql_statements,
            started.elapsed().as_secs_f64()
        );
    }
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

const NOW: i64 = 1_770_000_000_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_and_v2_plans_deliver_identical_rows_to_every_member() {
    let tasks: usize = std::env::var("PARITY_TASKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let members: usize = std::env::var("PARITY_MEMBERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(78);
    let mut sides = Vec::new();
    for visibility in [FixtureVisibility::Legacy, FixtureVisibility::V2] {
        let fixture = Fixture::build(FixtureOptions {
            tenants: vec![TenantSpec::new("big", tasks, members)],
            visibility,
            ..FixtureOptions::whagons()
        })
        .await
        .unwrap();
        let mut clients = Vec::new();
        for member in 0..members {
            clients.push(Client::connect(&fixture, member).await);
        }
        sides.push(Side { fixture, clients });
    }
    let v2 = sides.pop().unwrap();
    let legacy = sides.pop().unwrap();
    let mut pair = Pair {
        legacy,
        v2,
        rows_compared: 0,
        report: Vec::new(),
    };
    pair.check("open").await;
    eprintln!("parity: open: ok");

    let fixture = &pair.v2.fixture;
    struct Tenant {
        members: Vec<gonvex_runtime::perf_fixture::FixtureMember>,
        tasks: Vec<gonvex_runtime::perf_fixture::FixtureTask>,
    }
    let tenant = Tenant {
        members: fixture.tenant(0).members.clone(),
        tasks: fixture.tenant(0).tasks.clone(),
    };
    let kind =
        |kind: MemberKind, n: usize| tenant.members[fixture.members_of_kind(0, kind)[n]].clone();
    let admin = kind(MemberKind::Admin, 0);
    let manager = kind(MemberKind::Manager, 0);
    let all_spots = kind(MemberKind::TeamAllSpots, 0);
    let spots = kind(MemberKind::TeamSpots, 0);
    let spots_b = kind(MemberKind::TeamSpots, 1);
    let grant = kind(MemberKind::GrantOnly, 0);
    let grant_b = kind(MemberKind::GrantOnly, 1);
    let other_team = tenant
        .members
        .iter()
        .flat_map(|member| member.teams.iter())
        .find(|team| !spots.teams.contains(team))
        .unwrap()
        .clone();
    let task = |i: usize| tenant.tasks[i % tenant.tasks.len()].clone();
    let spotted: Vec<_> = tenant
        .tasks
        .iter()
        .filter(|t| t.spot_id.is_some())
        .cloned()
        .collect();
    let spot_of = |i: usize| spotted[i % spotted.len()].spot_id.clone().unwrap();
    let workspace_ids: Vec<String> = {
        let mut ids: Vec<_> = tenant
            .tasks
            .iter()
            .map(|t| t.workspace_id.clone())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };
    // A task outside `spots`'s current view (the admin sees it; the spot
    // member's snapshot does not hold it).
    let hidden_from = |member: usize| -> String {
        let client = &pair.v2.clients[member];
        tenant
            .tasks
            .iter()
            .find(|t| {
                !client.model.windows["tasks.recentWorkspaceTasks"]
                    .rows
                    .contains_key(&t.id)
            })
            .unwrap()
            .id
            .clone()
    };
    let index_of = |id: &str| tenant.members.iter().position(|m| m.id == id).unwrap();
    let hidden_grant = hidden_from(index_of(&grant.id));
    let hidden_spots = hidden_from(index_of(&spots.id));
    let t0 = task(0).id;

    // Plain data writes (no visibility change, then rows entering/leaving).
    pair.write(
        "task description",
        Write::Sql(
            r#"UPDATE "tasks" SET "description"=$1,"updatedAt"=$2 WHERE "_id"=$3"#,
            vec![text("parity edit"), NOW.into(), text(&t0)],
        ),
    )
    .await;
    pair.write(
        "task spot move",
        Write::Sql(
            r#"UPDATE "tasks" SET "spotId"=$1 WHERE "_id"=$2"#,
            vec![text(&spot_of(7)), text(&task(3).id)],
        ),
    )
    .await;
    pair.write(
        "task spot cleared",
        Write::Sql(
            r#"UPDATE "tasks" SET "spotId"=NULL WHERE "_id"=$1"#,
            vec![text(&task(4).id)],
        ),
    )
    .await;
    pair.write(
        "task spot blank",
        Write::Sql(
            r#"UPDATE "tasks" SET "spotId"='' WHERE "_id"=$1"#,
            vec![text(&task(5).id)],
        ),
    )
    .await;
    pair.write(
        "task workspace move",
        Write::Sql(
            r#"UPDATE "tasks" SET "workspaceId"=$1 WHERE "_id"=$2"#,
            vec![text(&workspace_ids[1]), text(&task(6).id)],
        ),
    )
    .await;
    pair.write(
        "task soft delete",
        Write::Sql(
            r#"UPDATE "tasks" SET "deletedAt"=$1 WHERE "_id"=$2"#,
            vec![NOW.into(), text(&task(8).id)],
        ),
    )
    .await;
    pair.write(
        "task restore",
        Write::Sql(
            r#"UPDATE "tasks" SET "deletedAt"=NULL WHERE "_id"=$1"#,
            vec![text(&task(8).id)],
        ),
    )
    .await;
    pair.write(
        "task insert",
        Write::InsertLike(
            "tasks",
            "_id",
            &task(9).id,
            vec![
                ("_id", text("parity-task-1")),
                ("id", 9_000_001i64.into()),
                ("spotId", text(&spot_of(11))),
            ],
        ),
    )
    .await;
    pair.write(
        "task hard delete",
        Write::Sql(
            r#"DELETE FROM "tasks" WHERE "_id"=$1"#,
            vec![text(&task(10).id)],
        ),
    )
    .await;
    pair.write(
        "finding created by member",
        Write::Sql(
            r#"UPDATE "tasks" SET "createdVia"='finding',"createdBy"=$1 WHERE "_id"=$2"#,
            vec![text(&grant.id), text(&hidden_grant)],
        ),
    )
    .await;
    pair.write(
        "finding reported by member",
        Write::Sql(
            r#"UPDATE "tasks" SET "createdVia"='finding',"reportedByUserId"=$1 WHERE "_id"=$2"#,
            vec![text(&grant_b.id), text(&task(12).id)],
        ),
    )
    .await;
    pair.write(
        "reported by member's team",
        Write::Sql(
            r#"UPDATE "tasks" SET "reportedByTeamId"=$1 WHERE "_id"=$2"#,
            vec![text(&spots.teams[0]), text(&hidden_spots)],
        ),
    )
    .await;

    // Grants: assignment, share (user, team), approval, workspace contexts.
    pair.write("assign", Write::Sql(r#"INSERT INTO "taskUsers"("_id","createdAt","deletedAt","role","taskId","updatedAt","userId","workspaceId") VALUES('parity-tu-1',$1,NULL,'assignee',$2,$1,$3,$4)"#, vec![NOW.into(), text(&hidden_spots), text(&spots.id), text(&workspace_ids[0])])).await;
    pair.write("assign other member", Write::Sql(r#"INSERT INTO "taskUsers"("_id","createdAt","deletedAt","role","taskId","updatedAt","userId","workspaceId") VALUES('parity-tu-2',$1,NULL,'assignee',$2,$1,$3,$4)"#, vec![NOW.into(), text(&task(13).id), text(&grant_b.id), text(&workspace_ids[0])])).await;
    pair.write(
        "unassign (soft)",
        Write::Sql(
            r#"UPDATE "taskUsers" SET "deletedAt"=$1 WHERE "_id"='parity-tu-1'"#,
            vec![NOW.into()],
        ),
    )
    .await;
    pair.write(
        "unassign (hard)",
        Write::Sql(
            r#"DELETE FROM "taskUsers" WHERE "_id"='parity-tu-2'"#,
            vec![],
        ),
    )
    .await;
    pair.write("share to user", Write::Sql(r#"INSERT INTO "taskShares"("_id","createdAt","permission","sharedByUserId","sharedToUserId","status","taskId","updatedAt") VALUES('parity-sh-1',$1,'view',$2,$3,'active',$4,$1)"#, vec![NOW.into(), text(&admin.id), text(&grant.id), text(&task(14).id)])).await;
    pair.write("share to team", Write::Sql(r#"INSERT INTO "taskShares"("_id","createdAt","permission","sharedByUserId","sharedToTeamId","status","taskId","updatedAt") VALUES('parity-sh-2',$1,'view',$2,$3,'active',$4,$1)"#, vec![NOW.into(), text(&admin.id), text(&spots.teams[0]), text(&hidden_spots)])).await;
    pair.write(
        "share revoked",
        Write::Sql(
            r#"UPDATE "taskShares" SET "revokedAt"=$1 WHERE "_id"='parity-sh-1'"#,
            vec![NOW.into()],
        ),
    )
    .await;
    pair.write(
        "share soft deleted",
        Write::Sql(
            r#"UPDATE "taskShares" SET "deletedAt"=$1 WHERE "_id"='parity-sh-2'"#,
            vec![NOW.into()],
        ),
    )
    .await;
    pair.write("approval", Write::Sql(r#"INSERT INTO "taskApprovalInstances"("_id","approverUserId","createdAt","isRequired","orderIndex","status","taskId","updatedAt") VALUES('parity-ap-1',$1,$2,1,0,'pending',$3,$2)"#, vec![text(&grant_b.id), NOW.into(), text(&task(15).id)])).await;
    pair.write(
        "approval deleted",
        Write::Sql(
            r#"UPDATE "taskApprovalInstances" SET "deletedAt"=$1 WHERE "_id"='parity-ap-1'"#,
            vec![NOW.into()],
        ),
    )
    .await;
    pair.write("context to user", Write::Sql(r#"INSERT INTO "taskWorkspaceContexts"("_id","createdAt","kind","targetUserId","taskId","updatedAt","workspaceId") VALUES('parity-cx-1',$1,'approval',$2,$3,$1,$4)"#, vec![NOW.into(), text(&grant.id), text(&task(16).id), text(&workspace_ids[2])])).await;
    pair.write("context to team", Write::Sql(r#"INSERT INTO "taskWorkspaceContexts"("_id","createdAt","kind","targetTeamId","taskId","updatedAt","workspaceId") VALUES('parity-cx-2',$1,'acknowledgment',$2,$3,$1,$4)"#, vec![NOW.into(), text(&other_team), text(&task(17).id), text(&workspace_ids[2])])).await;
    pair.write(
        "context revoked",
        Write::Sql(
            r#"UPDATE "taskWorkspaceContexts" SET "revokedAt"=$1 WHERE "_id"='parity-cx-1'"#,
            vec![NOW.into()],
        ),
    )
    .await;

    // Principal changes: teams, spots, flags, roles, permissions.
    pair.write("member joins team", Write::Sql(r#"INSERT INTO "memberTeams"("_id","_creationTime","memberId","userId","teamId","roleKey","createdAt","updatedAt") VALUES('parity-mt-1',$1,$2,$2,$3,'member',$1,$1)"#, vec![NOW.into(), text(&grant.id), text(&other_team)])).await;
    pair.write("member leaves team (soft)", Write::Sql(r#"UPDATE "memberTeams" SET "deletedAt"=$1 WHERE "memberId"=$2 AND "deletedAt" IS NULL AND "teamId"=$3"#, vec![NOW.into(), text(&all_spots.id), text(&all_spots.teams[0])])).await;
    pair.write("member gains spot", Write::Sql(r#"INSERT INTO "memberSpots"("_id","_creationTime","memberId","userId","spotId","createdAt","updatedAt") VALUES('parity-ms-1',$1,$2,$2,$3,$1,$1)"#, vec![NOW.into(), text(&spots_b.id), text(&spot_of(23))])).await;
    pair.write(
        "member loses spot",
        Write::Sql(
            r#"DELETE FROM "memberSpots" WHERE "memberId"=$1 AND "_id"<>'parity-ms-1'"#,
            vec![text(&spots.id)],
        ),
    )
    .await;
    pair.write(
        "see_all_spots on",
        Write::Sql(
            r#"UPDATE "members" SET "see_all_spots"=1 WHERE "id"=$1"#,
            vec![text(&spots_b.id)],
        ),
    )
    .await;
    pair.write(
        "see_all_spots off",
        Write::Sql(
            r#"UPDATE "members" SET "see_all_spots"=0 WHERE "id"=$1"#,
            vec![text(&manager.id)],
        ),
    )
    .await;
    pair.write(
        "permission tasks.viewAll granted",
        Write::Sql(
            r#"UPDATE "members" SET "permissions"=$1 WHERE "id"=$2"#,
            vec![text(r#"{"tasks.viewAll":true}"#), text(&grant_b.id)],
        ),
    )
    .await;
    pair.write(
        "role system_admin",
        Write::Sql(
            r#"UPDATE "members" SET "role"='system_admin' WHERE "id"=$1"#,
            vec![text(&grant.id)],
        ),
    )
    .await;
    pair.write(
        "admin loses tasks.viewAll",
        Write::Sql(
            r#"UPDATE "members" SET "permissions"='{}' WHERE "id"=$1"#,
            vec![text(&admin.id)],
        ),
    )
    .await;
    pair.write(
        "workspace unlinked from team",
        Write::Sql(
            r#"UPDATE "workspaceTeams" SET "deletedAt"=$1 WHERE "workspaceId"=$2"#,
            vec![NOW.into(), text(&workspace_ids[3])],
        ),
    )
    .await;
    pair.write("workspace linked to team", Write::Sql(r#"INSERT INTO "workspaceTeams"("_id","_creationTime","workspaceId","teamId","createdAt","updatedAt") VALUES('parity-wt-1',$1,$2,$3,$1,$1)"#, vec![NOW.into(), text(&workspace_ids[3]), text(&spots_b.teams[0])])).await;
    pair.write("spot subtree moved", Write::Sql(r#"UPDATE "spots" SET "parentId"=$1 WHERE "_id"=(SELECT "parentId" FROM "spots" WHERE "_id"=$2)"#, vec![text(&spot_of(31)), text(&spot_of(29))])).await;
    pair.write("tenant all-spots setting", Write::Sql(r#"INSERT INTO "globalSettings"("_id","_creationTime","group","key","updatedAt","value","allSpotsMemberStatus") VALUES('parity-gs-1',$1,'tasks','allSpotsMemberStatus',$1,'{}','active')"#, vec![NOW.into()])).await;
    pair.write(
        "tenant all-spots setting removed",
        Write::Sql(
            r#"DELETE FROM "globalSettings" WHERE "_id"='parity-gs-1'"#,
            vec![],
        ),
    )
    .await;

    // Personal spaces: owner, member, team, and personal contexts.
    let personal = workspace_ids[4].clone();
    pair.write(
        "workspace becomes personal",
        Write::Sql(
            r#"UPDATE "workspaces" SET "type"='PERSONAL',"createdBy"=$1 WHERE "_id"=$2"#,
            vec![text(&grant_b.id), text(&personal)],
        ),
    )
    .await;
    pair.write("personal workspace member", Write::Sql(r#"INSERT INTO "personalWorkspaceMembers"("_id","_creationTime","workspaceId","memberId","createdAt") VALUES('parity-pw-1',$1,$2,$3,$1)"#, vec![NOW.into(), text(&personal), text(&spots_b.id)])).await;
    pair.write("personal context", Write::Sql(r#"INSERT INTO "taskWorkspaceContexts"("_id","createdAt","kind","taskId","updatedAt","workspaceId") VALUES('parity-cx-3',$1,'personal',$2,$1,$3)"#, vec![NOW.into(), text(&task(19).id), text(&personal)])).await;
    pair.write(
        "task moved into personal space",
        Write::Sql(
            r#"UPDATE "tasks" SET "workspaceId"=$1 WHERE "_id"=$2"#,
            vec![text(&personal), text(&task(20).id)],
        ),
    )
    .await;
    pair.write(
        "personal workspace deleted",
        Write::Sql(
            r#"UPDATE "workspaces" SET "deletedAt"=$1 WHERE "_id"=$2"#,
            vec![NOW.into(), text(&personal)],
        ),
    )
    .await;

    // Children follow their parent: task tags, assignments of hidden tasks.
    pair.write("tag added", Write::Sql(r#"INSERT INTO "taskTags"("_creationTime","_id","tagId","taskId","userId") SELECT $1,'parity-tt-1',"tagId",$2,$3 FROM "taskTags" LIMIT 1"#, vec![NOW.into(), text(&task(21).id), text(&admin.id)])).await;
    pair.write(
        "tagged task hidden by spot move",
        Write::Sql(
            r#"UPDATE "tasks" SET "spotId"=$1,"workspaceId"=$2 WHERE "_id"=$3"#,
            vec![
                text(&spot_of(37)),
                text(&workspace_ids[5]),
                text(&task(21).id),
            ],
        ),
    )
    .await;
    pair.write(
        "tag removed",
        Write::Sql(
            r#"DELETE FROM "taskTags" WHERE "_id"='parity-tt-1'"#,
            vec![],
        ),
    )
    .await;
    pair.write(
        "presence beat",
        Write::Sql(
            r#"UPDATE "userPresence" SET "lastSeenAt"=$1 WHERE "userId"=$2"#,
            vec![NOW.into(), text(&spots.id)],
        ),
    )
    .await;
    pair.write(
        "workspace preference",
        Write::Sql(
            r#"UPDATE "workspacePreferences" SET "updatedAt"=$1 WHERE "userId"=$2"#,
            vec![NOW.into(), text(&spots.id)],
        ),
    )
    .await;

    // A burst of ordinary task edits across the fixture (rows entering and
    // leaving members' sets through spot and workspace moves).
    for i in 0..24usize {
        let target = task(100 + i * 37).id;
        let write = match i % 3 {
            0 => Write::Sql(
                r#"UPDATE "tasks" SET "spotId"=$1 WHERE "_id"=$2"#,
                vec![text(&spot_of(i * 13 + 1)), text(&target)],
            ),
            1 => Write::Sql(
                r#"UPDATE "tasks" SET "workspaceId"=$1 WHERE "_id"=$2"#,
                vec![text(&workspace_ids[i % workspace_ids.len()]), text(&target)],
            ),
            _ => Write::Sql(
                r#"UPDATE "tasks" SET "statusId"=(SELECT "_id" FROM "statuses" ORDER BY "_id" LIMIT 1 OFFSET $1) WHERE "_id"=$2"#,
                vec![((i % 8) as i64).into(), text(&target)],
            ),
        };
        pair.write(&format!("task edit {i}"), write).await;
    }

    eprintln!(
        "parity: {} members x {} collections, {} writes, {} visible rows compared",
        members,
        WHAGONS_SUBSCRIPTIONS.len(),
        pair.report.len(),
        pair.rows_compared
    );
    eprintln!("write | v1 sql | v1 snapshots | v1 principal rebuilds | v2 sql | v2 snapshots | v2 principal rebuilds | v2 compiled checks | v2 fallback");
    for (name, legacy, v2) in &pair.report {
        eprintln!(
            "{name} | {} | {} | {} | {} | {} | {} | {} | {}",
            legacy.visibility_sql_statements,
            legacy.collection_snapshots,
            legacy.principal_rebuilds,
            v2.visibility_sql_statements,
            v2.collection_snapshots,
            v2.principal_rebuilds,
            v2.compiled_row_checks,
            v2.fallback_row_checks
        );
    }
    pair.legacy.fixture.shutdown().await;
    pair.v2.fixture.shutdown().await;
}
