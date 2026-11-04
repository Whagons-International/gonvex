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

/// Both tests flip the process-wide sharing switch: run them one at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Edits, spot/workspace moves, presence beats, an insert, a grant assigned
/// and removed, a team gained and lost, and a delete, committed in order.
async fn events(fixture: &Fixture) -> Vec<(String, FeedEvent)> {
    let tenant_members = fixture.tenants[0].members.clone();
    let members = tenant_members.len();
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
    // Delivery coverage: transactions that reach inherited collections
    // (`taskTags` / `taskUsers` follow their parent task), several statements
    // per transaction as the Whagons reducers commit them.
    let log = |id: &str, task: &str| -> (String, Vec<SqlValue>) {
        (
            r#"INSERT INTO "taskLogs"("_id","_creationTime","action","taskId","userId","timestamp","uuid") VALUES($1,1,'updated',$2,$3,1.0,$1)"#.into(),
            vec![text(id), text(task), text(&tenant_members[0].id)],
        )
    };
    let tag = |id: &str, task: &str| -> (String, Vec<SqlValue>) {
        (
            r#"INSERT INTO "taskTags"("_id","_creationTime","taskId","tagId","userId") VALUES($1,1,$2,$1,$3)"#.into(),
            vec![text(id), text(task), text(&tenant_members[0].id)],
        )
    };
    let transactions: Vec<(&str, Vec<(String, Vec<SqlValue>)>)> = vec![
        ("tag a task", vec![tag("cov-tag-1", &tasks[5].id)]),
        (
            "tag two tasks",
            vec![tag("cov-tag-2", &tasks[6].id), tag("cov-tag-3", &tasks[9].id)],
        ),
        (
            "untag",
            vec![(r#"DELETE FROM "taskTags" WHERE "_id"='cov-tag-1'"#.into(), Vec::new())],
        ),
        (
            "view",
            vec![(
                r#"INSERT INTO "taskViews"("_id","_creationTime","taskId","userId","viewedAt") VALUES('cov-view',1,$1,$2,1)"#.into(),
                vec![text(&tasks[6].id), text(&tenant_members[1].id)],
            )],
        ),
        (
            "notification",
            vec![(
                r#"INSERT INTO "notifications"("_id","_creationTime","userId","title","isRead") VALUES('cov-note',1,$1,'t',0)"#.into(),
                vec![text(&tenant_members[2].id)],
            )],
        ),
        (
            "edit and log",
            vec![
                (
                    r#"UPDATE "tasks" SET "description"='coverage edit' WHERE "_id"=$1"#.into(),
                    vec![text(&tasks[6].id)],
                ),
                log("cov-log-1", &tasks[6].id),
            ],
        ),
        (
            "spot move and log",
            vec![
                (
                    r#"UPDATE "tasks" SET "spotId"=$1 WHERE "_id"=$2"#.into(),
                    vec![
                        tasks[11].spot_id.clone().map_or(SqlValue::Null, SqlValue::Text),
                        text(&tasks[9].id),
                    ],
                ),
                log("cov-log-2", &tasks[9].id),
            ],
        ),
        (
            "create task with a tag",
            vec![
                (
                    r#"INSERT INTO "tasks"("_id","id","workspaceId","statusId","name","createdAt","updatedAt","_creationTime") VALUES('cov-task',999997,$1,$2,'New',1,1,1)"#.into(),
                    vec![text(&tasks[1].workspace_id), text(&tasks[1].status_id)],
                ),
                tag("cov-tag-4", "cov-task"),
            ],
        ),
        ("tag a missing task", vec![tag("cov-tag-5", "no-such-task")]),
        (
            "member availability (not visibility)",
            vec![(
                r#"UPDATE "members" SET "availability_status"='busy', "updated_at"=99 WHERE "id"=$1"#.into(),
                vec![text(&tenant_members[3].id)],
            )],
        ),
        (
            "member sees all spots",
            vec![(
                r#"UPDATE "members" SET "see_all_spots"=1 WHERE "id"=$1"#.into(),
                vec![text(&tenant_members[4].id)],
            )],
        ),
        (
            "tag and touch the task",
            vec![
                tag("cov-tag-6", &tasks[14].id),
                (
                    r#"UPDATE "tasks" SET "updatedAt"=77 WHERE "_id"=$1"#.into(),
                    vec![text(&tasks[14].id)],
                ),
            ],
        ),
        (
            "tag and move the task",
            vec![
                tag("cov-tag-7", &tasks[15].id),
                (
                    r#"UPDATE "tasks" SET "workspaceId"=$1 WHERE "_id"=$2"#.into(),
                    vec![text(&tasks[16].workspace_id), text(&tasks[15].id)],
                ),
            ],
        ),
        (
            "workspace move of a tagged task",
            vec![(
                r#"UPDATE "tasks" SET "workspaceId"=$1 WHERE "_id"=$2"#.into(),
                vec![text(&tasks[12].workspace_id), text(&tasks[6].id)],
            )],
        ),
        (
            "delete a tagged task",
            vec![(r#"DELETE FROM "tasks" WHERE "_id"=$1"#.into(), vec![text(&tasks[9].id)])],
        ),
    ];
    for (name, statements) in transactions {
        events.push((
            name.to_owned(),
            fixture.commit_all(0, statements).await.unwrap(),
        ));
    }
    events
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn class_shared_decisions_equal_each_connections_own_decisions() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let tenant_members = fixture.tenants[0].members.clone();
    let members = tenant_members.len();
    let mut shared = fixture.connect_all(0).await.unwrap();
    let mut alone = fixture.connect_all(0).await.unwrap();
    let events = events(&fixture).await;
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

/// Delivery v4 oracle: connections served by the per-tenant fan-out
/// (`Runtime::deliver_feed_event`: the shared outcome of their shape, or the
/// full path that records it) send exactly the batch frames each connection
/// computes alone (class sharing and fan-out off, `apply_feed_event` +
/// `ReplicaBatcher::push`), for several subscription sets per member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_out_frames_equal_each_connections_own_frames() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    use gonvex_runtime::delivery::{BatchConfig, DeliveryHub, ReplicaBatcher};
    use gonvex_websocket::OutboundBatch;
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let members = fixture.tenants[0].members.len();
    let tenant_id = fixture.tenants[0].route.tenant_id.clone();
    let sets: [&[&str]; 4] = [
        &gonvex_runtime::perf_fixture::WHAGONS_SUBSCRIPTIONS,
        &["tasks.recentWorkspaceTasks"],
        &[
            "presence.list",
            "statuses.statuses",
            "tags.tags",
            "tasks.recentWorkspaceTasks",
        ],
        &["taskResources.taskTags", "taskResources.taskUsers"],
    ];
    struct Side {
        connection: gonvex_runtime::perf_fixture::MemberConnection,
        batcher: ReplicaBatcher,
        batch: OutboundBatch,
    }
    let side = |connection| {
        let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
        batcher.set_enabled(true);
        Side {
            connection,
            batcher,
            batch: OutboundBatch::default(),
        }
    };
    let mut shared = Vec::new();
    let mut alone = Vec::new();
    // Set 0 also has a task detail window open, on one of 5 tasks: such
    // connections differ only in collections most events skip, and still
    // share outcomes (the shape's key leaves skipped subscriptions out).
    let tasks = fixture.tenants[0].tasks.clone();
    for (set, paths) in sets.iter().enumerate() {
        for member in 0..members {
            for side_list in [&mut shared, &mut alone] {
                let mut connection = fixture.connect_paths(0, member, paths).await.unwrap();
                if set == 0 {
                    let task = &tasks[(member % 5) * 3].id;
                    for (path, argument) in gonvex_runtime::perf_fixture::WHAGONS_DETAIL_WINDOW {
                        let id = format!("~detail {path}");
                        let opened = fixture
                            .open_replica_args(&connection.session, &id, path, json!({ argument: task }))
                            .await
                            .unwrap();
                        if let Some(subscription) = opened.subscription {
                            connection.subscriptions.insert(id, subscription);
                        }
                    }
                }
                side_list.push(side(connection));
            }
        }
    }
    let mut events = events(&fixture).await;
    // Burst control: a lagging connection merges the transactions waiting
    // for it into one event, its own copy. Three more transactions merged;
    // every connection receives a private copy of the same range (found by
    // revision range, `feed_share`).
    let mut tail = Vec::new();
    for (index, task) in [20usize, 21, 22].into_iter().enumerate() {
        tail.push(if index == 1 {
            fixture.beat_presence(0, 1).await.unwrap()
        } else {
            fixture
                .update_task(0, task, json!({"description": format!("merged {index}")}))
                .await
                .unwrap()
        });
    }
    let merged_index = events.len();
    events.push((
        "merged range (private copies)".to_owned(),
        gonvex_runtime::change_feed::merge_transactions(&tail).unwrap(),
    ));
    let shares_before = feed_share::metrics()["foundByRange"].as_u64().unwrap();
    // Targeted wakeups: connections the tenant feed would skip for an event
    // (checked against each connection deciding alone below).
    let mut wake_skipped = 0usize;
    let mut wake_resyncs = 0usize;
    let hub_shared = DeliveryHub::default();
    let hub_alone = DeliveryHub::default();
    let (hits_before, _, data_before) = gonvex_runtime::replica::fanout_stats();
    let paths_before = gonvex_runtime::replica::fanout_path_counts();
    // Connections of sets with inherited collections (0 and 3) served by the
    // fan-out with decided rows, per event.
    let mut inherited_data = 0usize;
    let mut frames = 0usize;
    let mut data_frames = 0usize;
    {
        for (event_index, (name, event)) in events.iter().enumerate() {
            let revision = match event {
                FeedEvent::Transaction { revision, .. } => *revision,
                FeedEvent::Reset { .. } => 0,
            };
            // Shared side first. Every other event delivers to all its
            // connections concurrently, as the connection loops do: the first
            // connection of a shape claims its outcome while the others wait
            // for it (`replica_fanout::Claim`).
            feed_share::set_sharing(Some(true));
            let concurrent = event_index % 2 == 1;
            async fn deliver(
                fixture: &Fixture,
                hub: &gonvex_runtime::delivery::DeliveryHub,
                event: &FeedEvent,
                index: usize,
                a: &mut Side,
                private: bool,
            ) -> Vec<gonvex_protocol::ServerMessage> {
                let session = a.connection.session.clone();
                // A private copy (new change list `Arc`) of the same range.
                let event = if private {
                    gonvex_runtime::change_feed::merge_transactions(std::slice::from_ref(event))
                        .unwrap()
                } else {
                    event.clone()
                };
                let event = &event;
                fixture
                    .runtime
                    .deliver_feed_event(
                        &session,
                        &mut a.connection.subscriptions,
                        event.clone(),
                        &mut a.batcher,
                        &mut a.batch,
                        hub,
                        index % 2 == 1,
                    )
                    .await
                    .unwrap()
            }
            let mut rests = Vec::new();
            if concurrent {
                rests = futures_util::future::join_all(
                    shared
                        .iter_mut()
                        .enumerate()
                        .map(|(index, a)| {
                            deliver(&fixture, &hub_shared, event, index, a, event_index == merged_index)
                        }),
                )
                .await;
            } else {
                for (index, a) in shared.iter_mut().enumerate() {
                    let data_before = gonvex_runtime::replica::fanout_stats().2;
                    rests.push(
                        deliver(&fixture, &hub_shared, event, index, a, event_index == merged_index)
                            .await,
                    );
                    let set = index / members;
                    if (set == 0 || set == 3)
                        && gonvex_runtime::replica::fanout_stats().2 > data_before
                    {
                        inherited_data += 1;
                    }
                }
            }
            for a in shared.iter_mut() {
                a.batcher.watermark(
                    &mut a.batch,
                    &tenant_id,
                    &a.connection.subscriptions,
                    revision,
                );
            }
            feed_share::set_sharing(Some(false));
            for (index, (a, b)) in shared.iter_mut().zip(alone.iter_mut()).enumerate() {
                // Half the connections use native row encoding.
                let native = index % 2 == 1;
                let rest_a: Vec<Value> = rests[index]
                    .iter()
                    .map(|m| serde_json::to_value(m).unwrap())
                    .collect();
                // Decided before the connection takes the event, as the
                // tenant feed decides before broadcasting it.
                let skips = gonvex_runtime::replica::wake_skips(
                    &mut b.connection.subscriptions,
                    &b.connection.session,
                    event,
                );
                let messages = fixture
                    .runtime
                    .apply_feed_event(
                        &b.connection.session.clone(),
                        &mut b.connection.subscriptions,
                        event.clone(),
                    )
                    .await
                    .unwrap();
                if skips {
                    // A skipped connection must have had nothing to deliver:
                    // no row, ready or transaction frame. The only frame
                    // tolerated is a consistency resync the full path asks
                    // for because a shared index is already ahead of the
                    // event (this oracle commits every event first): a
                    // skipped connection decides no row against that index,
                    // so its digest stays exact without one.
                    for message in &messages {
                        assert!(
                            matches!(message, gonvex_protocol::ServerMessage::ReplicaNeedHashes { .. }),
                            "targeted wakeups skipped connection {index} for event {name}, which sends {message:?}"
                        );
                        wake_resyncs += 1;
                    }
                    wake_skipped += 1;
                }
                let mut rest_b = Vec::new();
                for message in messages {
                    if let Some(rest) = b
                        .batcher
                        .push(
                            &mut b.batch,
                            &hub_alone,
                            &tenant_id,
                            native,
                            &b.connection.subscriptions,
                            message,
                        )
                        .unwrap()
                    {
                        rest_b.push(serde_json::to_value(&rest).unwrap());
                    }
                }
                assert_eq!(
                    rest_a, rest_b,
                    "unbatched frames, connection {index}, event {name}"
                );
                b.batcher.watermark(
                    &mut b.batch,
                    &tenant_id,
                    &b.connection.subscriptions,
                    revision,
                );
                let data = b.batcher.has_items();
                assert_eq!(
                    a.batcher.has_items(),
                    data,
                    "connection {index}, event {name}"
                );
                let with = a
                    .batcher
                    .take_frame(&mut a.batch, None)
                    .map(|frame| frame.bytes.to_vec());
                let without = b
                    .batcher
                    .take_frame(&mut b.batch, None)
                    .map(|frame| frame.bytes.to_vec());
                assert_eq!(
                    with.as_deref().map(String::from_utf8_lossy),
                    without.as_deref().map(String::from_utf8_lossy),
                    "connection {index}, event {name}, native {native}"
                );
                for (x, y) in a
                    .connection
                    .subscriptions
                    .values()
                    .zip(b.connection.subscriptions.values())
                {
                    assert_eq!(
                        x.digest(),
                        y.digest(),
                        "digest, connection {index}, event {name}"
                    );
                    assert_eq!(
                        x.cursor, y.cursor,
                        "cursor, connection {index}, event {name}"
                    );
                    assert_eq!(
                        x.row_count(),
                        y.row_count(),
                        "rows, connection {index}, event {name}"
                    );
                }
                frames += 1;
                data_frames += usize::from(data);
            }
        }
    }
    feed_share::set_sharing(None);
    let (hits_after, recorded, data_after) = gonvex_runtime::replica::fanout_stats();
    let hits = hits_after - hits_before;
    let data_hits = data_after - data_before;
    eprintln!("{} events, {frames} frames ({data_frames} with data), {hits} fan-out hits ({data_hits} with decided rows), {recorded} outcomes recorded", events.len());
    let paths_after = gonvex_runtime::replica::fanout_path_counts();
    for (index, (path, _)) in gonvex_runtime::replica::FANOUT_PATHS.iter().enumerate() {
        eprintln!("  {path}: {}", paths_after[index] - paths_before[index]);
    }
    eprintln!("  inherited connections served with decided rows: {inherited_data}");
    let waits = gonvex_runtime::replica::fanout_coverage()["waits"].as_u64().unwrap();
    eprintln!("  waits for another connection's outcome: {waits}");
    assert!(waits > 0, "concurrent deliveries never waited on a claimed shape");
    let coverage = gonvex_runtime::replica::fanout_coverage();
    let by_range = feed_share::metrics()["foundByRange"].as_u64().unwrap() - shares_before;
    eprintln!(
        "  merged deliveries {} (shared {}), shares found by range {by_range}",
        coverage["merged"], coverage["mergedShared"]
    );
    assert!(by_range > 0, "private merged copies never found the range's share");
    eprintln!("  connections the tenant feed would skip (verified idle): {wake_skipped} of {frames} ({wake_resyncs} index-ahead resyncs not needed)");
    assert!(wake_skipped > 0, "targeted wakeups never skipped a connection: the check proved nothing");
    assert!(
        coverage["mergedShared"].as_u64().unwrap() > 0,
        "no merged delivery was served by the fan-out"
    );
    assert!(
        inherited_data > 0,
        "no connection holding inherited collections was served rows by the fan-out"
    );
    assert!(data_hits > 0, "no shared outcome with rows was used");
    assert!(data_frames > 0);
    assert!(
        hits > 0,
        "no connection was served by the fan-out: the oracle exercised nothing"
    );
}
