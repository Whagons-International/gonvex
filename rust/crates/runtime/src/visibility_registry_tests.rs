//! Lifecycle of the tenant-shared parent ACL indexes: one gap-free applier per
//! index, poison on reset/lag/epoch/projection change, prepared lookups.

use super::*;
use crate::change_feed::{FeedEvent, LogChange};
use crate::visibility_index::ready_at;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;

fn shape(columns: &[&str]) -> IndexShape {
    IndexShape {
        table: "tasks".into(),
        key: "id".into(),
        id_column: "id".into(),
        columns: columns.iter().map(|column| (*column).to_owned()).collect(),
    }
}

fn change(revision: u64, id: &str, row: Option<Value>) -> LogChange {
    LogChange {
        revision,
        ordinal: 1,
        origin_command_id: String::new(),
        table: "tasks".into(),
        row_id: id.into(),
        operation: if row.is_some() { "UPDATE" } else { "DELETE" }.into(),
        changed_columns: Vec::new(),
        old_value: Value::Null,
        new_value: row.unwrap_or(Value::Null),
        provenance: Default::default(),
    }
}

fn tx(epoch: &str, revision: u64, changes: Vec<LogChange>) -> FeedEvent {
    FeedEvent::Transaction {
        database_epoch: epoch.into(),
        revision,
        changes,
    }
}

fn task(id: &str, workspace: &str) -> Value {
    json!({"id": id, "workspaceId": workspace, "title": "ignored"})
}

#[tokio::test]
async fn applier_panic_with_a_poisoned_lock_wakes_existing_waiters() {
    let index = Arc::new(std::sync::RwLock::new(ParentAclIndex::maintained(
        "e1".into(),
        1,
        Vec::new(),
    )));
    let mut signal = index.read().unwrap().subscribe();
    let worker_index = index.clone();
    let worker = std::thread::spawn(move || {
        let _exit = PoisonOnExit(worker_index.clone());
        let _write = worker_index.write().unwrap();
        panic!("controlled applier failure while holding its index lock");
    });
    assert!(worker.join().is_err());
    tokio::time::timeout(
        Duration::from_secs(1),
        signal.wait_for(|state| state.poisoned),
    )
    .await
    .expect("a poisoned lock must not leave an existing waiter parked")
    .unwrap();
    assert!(!ready_at(&index, 2, "e1").await);
}

fn load(revision: u64, rows: &[(&str, &str)]) -> IndexLoad {
    IndexLoad {
        epoch: "e1".into(),
        revision,
        rows: rows
            .iter()
            .map(|(id, workspace)| {
                (
                    (*id).to_owned(),
                    (*id).to_owned(),
                    json!({"id": id, "workspaceId": workspace}),
                )
            })
            .collect(),
    }
}

fn workspace(index: &SharedIndex, key: &str) -> Option<String> {
    index
        .read()
        .unwrap()
        .get(key)
        .map(|row| row["workspaceId"].as_str().unwrap().to_owned())
}

async fn ready(index: &SharedIndex, revision: u64) -> bool {
    tokio::time::timeout(Duration::from_secs(5), ready_at(index, revision, "e1"))
        .await
        .expect("readiness must be decided by the applier, not hang")
}

async fn ensure(
    registry: &ParentAclRegistry,
    sender: &broadcast::Sender<FeedEvent>,
    shape: IndexShape,
    loaded: IndexLoad,
) -> SharedIndex {
    registry
        .ensure_index(
            "tenant\u{0}tasks\u{0}id".into(),
            shape,
            || std::future::ready(sender.subscribe()),
            |_| std::future::ready(Ok(loaded)),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn ordered_single_writer_applies_each_transaction_once() {
    let (sender, _) = broadcast::channel(16);
    let registry = ParentAclRegistry::default();
    let loads = AtomicUsize::new(0);
    let ensure_counted = || async {
        registry
            .ensure_index(
                "tenant\u{0}tasks\u{0}id".into(),
                shape(&["id", "workspaceId"]),
                || std::future::ready(sender.subscribe()),
                |_| {
                    loads.fetch_add(1, Ordering::SeqCst);
                    async {
                        tokio::task::yield_now().await;
                        Ok(load(10, &[("t1", "a")]))
                    }
                },
            )
            .await
            .unwrap()
    };
    let (first, second) = tokio::join!(ensure_counted(), ensure_counted());
    assert!(Arc::ptr_eq(&first, &second), "one index per key");
    assert_eq!(loads.load(Ordering::SeqCst), 1, "single-flight load");
    sender
        .send(tx("e1", 11, vec![change(11, "t1", Some(task("t1", "b")))]))
        .unwrap();
    // A replayed revision is a no-op, never a regression.
    sender
        .send(tx("e1", 11, vec![change(11, "t1", Some(task("t1", "z")))]))
        .unwrap();
    sender
        .send(tx("e1", 12, vec![change(12, "t2", Some(task("t2", "c")))]))
        .unwrap();
    assert!(ready(&first, 12).await);
    assert_eq!(workspace(&first, "t1").as_deref(), Some("b"));
    assert_eq!(workspace(&first, "t2").as_deref(), Some("c"));
    // Only projected columns are kept.
    assert!(first
        .read()
        .unwrap()
        .get("t1")
        .unwrap()
        .get("title")
        .is_none());
}

/// Base: only connections with a v2 subscription advanced the index, so with
/// no consumer it froze, and the first later event jumped the gap.
#[tokio::test]
async fn index_keeps_advancing_with_no_consumers_for_a_new_query() {
    let (sender, _) = broadcast::channel(16);
    let registry = ParentAclRegistry::default();
    let index = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(10, &[("t1", "a")]),
    )
    .await;
    let weak = Arc::downgrade(&index);
    drop(index); // every consumer is gone
    sender
        .send(tx(
            "e1",
            11,
            vec![change(11, "t1", Some(task("t1", "revoked")))],
        ))
        .unwrap();
    sender.send(tx("e1", 12, Vec::new())).unwrap();
    let again = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(99, &[]),
    )
    .await;
    assert!(
        weak.upgrade().is_some_and(|old| Arc::ptr_eq(&old, &again)),
        "no reload"
    );
    assert!(ready(&again, 12).await);
    assert_eq!(workspace(&again, "t1").as_deref(), Some("revoked"));
}

/// A commit after the load snapshot is queued for the applier because it
/// subscribed first. Base loaded inside the caller's snapshot and inserted
/// after the scan, so a revision applied meanwhile was lost.
#[tokio::test]
async fn subscribing_before_the_load_snapshot_closes_the_load_gap() {
    let (sender, _) = broadcast::channel(16);
    let registry = ParentAclRegistry::default();
    let index = registry
        .ensure_index(
            "tenant\u{0}tasks\u{0}id".into(),
            shape(&["id", "workspaceId"]),
            || std::future::ready(sender.subscribe()),
            |_| {
                // Delivered while the scan runs: 10 is inside the snapshot,
                // 11 committed after it.
                sender
                    .send(tx(
                        "e1",
                        10,
                        vec![change(10, "t1", Some(task("t1", "stale")))],
                    ))
                    .unwrap();
                sender
                    .send(tx("e1", 11, vec![change(11, "t1", Some(task("t1", "b")))]))
                    .unwrap();
                std::future::ready(Ok(load(10, &[("t1", "a")])))
            },
        )
        .await
        .unwrap();
    sender
        .send(tx("e1", 12, vec![change(12, "t2", Some(task("t2", "c")))]))
        .unwrap();
    assert!(ready(&index, 12).await);
    assert_eq!(
        workspace(&index, "t1").as_deref(),
        Some("b"),
        "revision 11 kept"
    );
    assert_eq!(workspace(&index, "t2").as_deref(), Some("c"));
}

/// Base: every connection removed the tenant's indexes on Reset, detaching a
/// fresh index another connection already held, which then froze.
#[tokio::test]
async fn reset_poisons_held_handles_and_ensure_rebuilds() {
    let (sender, _) = broadcast::channel(16);
    let registry = ParentAclRegistry::default();
    let held = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(10, &[("t1", "a")]),
    )
    .await;
    sender
        .send(FeedEvent::Reset {
            reason: "test".into(),
        })
        .unwrap();
    assert!(!ready(&held, 11).await, "a reset index certifies nothing");
    assert!(held.read().unwrap().is_poisoned());
    let fresh = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(20, &[("t1", "b")]),
    )
    .await;
    assert!(!Arc::ptr_eq(&held, &fresh));
    sender.send(tx("e1", 21, Vec::new())).unwrap();
    assert!(ready(&fresh, 21).await, "the rebuilt index keeps advancing");
    assert_eq!(workspace(&fresh, "t1").as_deref(), Some("b"));
}

#[tokio::test]
async fn applier_lag_poisons_the_index() {
    let (sender, _) = broadcast::channel(2);
    let registry = ParentAclRegistry::default();
    let held = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(10, &[]),
    )
    .await;
    // Current-thread runtime: the applier cannot run until this test yields.
    for revision in 11..=15 {
        sender.send(tx("e1", revision, Vec::new())).unwrap();
    }
    assert!(!ready(&held, 15).await);
    assert!(held.read().unwrap().is_poisoned());
}

#[tokio::test]
async fn epoch_change_poisons_and_foreign_epoch_is_never_ready() {
    let (sender, _) = broadcast::channel(16);
    let registry = ParentAclRegistry::default();
    let held = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(10, &[]),
    )
    .await;
    assert!(!ready_at(&held, 5, "e2").await, "an event of another epoch");
    sender.send(tx("e2", 11, Vec::new())).unwrap();
    assert!(!ready(&held, 11).await);
    assert!(held.read().unwrap().is_poisoned());
}

/// A module change that alters the projection replaces the index; reusing it
/// would evaluate new plans against rows missing their columns.
#[tokio::test]
async fn projection_change_replaces_and_poisons_the_old_index() {
    let (sender, _) = broadcast::channel(16);
    let registry = ParentAclRegistry::default();
    let old = ensure(&registry, &sender, shape(&["id"]), load(10, &[("t1", "a")])).await;
    let new = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(10, &[("t1", "a")]),
    )
    .await;
    assert!(!Arc::ptr_eq(&old, &new));
    assert!(old.read().unwrap().is_poisoned());
    assert!(!ready(&old, 10).await);
    assert!(ready(&new, 10).await);
}

/// A subscription evaluating event E waits for the applier, through every
/// inherited level, instead of reading an index that has not reached E.
#[tokio::test]
async fn readiness_waits_for_the_applier_through_inherited_parents() {
    let (sender, _) = broadcast::channel(16);
    let registry = ParentAclRegistry::default();
    let index = ensure(
        &registry,
        &sender,
        shape(&["id", "workspaceId"]),
        load(10, &[]),
    )
    .await;
    let plan: VisibilityPlan = serde_json::from_value(json!({
        "version":2,"permissionTables":[],"table":"tasks","key":"id","sets":{},
        "where":{"operator":"eqContext","column":"workspaceId","context":"member.id"}
    }))
    .unwrap();
    let mut inner = ResolvedVisibility {
        ephemeral_member: false,
        revision: 10,
        direct: BTreeMap::new(),
        role: String::new(),
        permissions: Value::Null,
        sets: BTreeMap::new(),
        fingerprint: String::new(),
        inherited: BTreeMap::new(),
        indexes: BTreeMap::new(),
    };
    let mut outer = inner.clone();
    inner.indexes.insert("derived".into(), index.clone());
    outer.inherited.insert(
        "tasks".into(),
        InheritedParent {
            plan: Arc::new(plan),
            resolved: Arc::new(inner),
            index: Arc::new(std::sync::RwLock::new(ParentAclIndex::maintained(
                "e1".into(),
                100,
                Vec::new(),
            ))),
        },
    );
    let waiter = tokio::spawn(async move { indexes_ready(&outer, 11, "e1").await });
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "the nested index is behind revision 11"
    );
    sender.send(tx("e1", 11, Vec::new())).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap());
}

/// Indexes built outside the registry (no applier) are never waited on.
#[tokio::test]
async fn unmaintained_index_behind_the_event_is_not_ready() {
    let index = std::sync::RwLock::new(ParentAclIndex::from_snapshot(5, BTreeMap::new()));
    assert!(ready_at(&index, 5, "any").await);
    assert!(!ready_at(&index, 6, "any").await);
}

/// Real SQLite, real change feed: no connection consumes the feed, yet the
/// index advances; and a snapshot holding the runtime's only admission slot
/// resolves inherited parents without asking for a second one.
#[tokio::test]
async fn real_feed_maintains_index_without_consumers_and_without_nested_admission() {
    use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
    async fn script(pools: &PoolRegistry, url: &str, sql: &str) {
        let mut transaction = gonvex_store::begin_transaction(pools, url, false)
            .await
            .unwrap();
        gonvex_store::execute_script(transaction.transaction(), sql)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }
    let directory = tempfile::tempdir().unwrap();
    // One file holds the Control Plane and the tenant tables.
    let url = gonvex_store::database_url(&directory.path().join("acl.db"));
    let pools = PoolRegistry::new(PoolLimits {
        max_total_connections: 1,
        acquire_timeout: Duration::from_secs(2),
        ..PoolLimits::default()
    });
    let control = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    script(
        &pools,
        &url,
        r#"CREATE TABLE tasks(id TEXT PRIMARY KEY, "workspaceId" TEXT, title TEXT);
           CREATE TABLE "taskNotes"(id TEXT PRIMARY KEY, "taskId" TEXT);
           CREATE TABLE "memberTeams"(id TEXT PRIMARY KEY, "memberId" TEXT, "teamId" TEXT);
           INSERT INTO tasks VALUES('t1','private','Task');"#,
    )
    .await;
    let route = TenantRoute {
        project_id: "acl".into(),
        tenant_id: "tenant".into(),
        database_url: url.clone(),
    };
    control
        .clone()
        .provision_tenant_database(route.clone(), Vec::new())
        .await
        .unwrap();
    script(
        &pools,
        &url,
        r#"INSERT INTO members(id,account_id,status,display_name) VALUES('alice','alice','active','Alice');
           INSERT INTO "memberTeams" VALUES('mt','alice','team');"#,
    )
    .await;
    let mut plans = BTreeMap::new();
    for plan in [
        json!({"version":2,"table":"tasks","key":"id","permissionTables":["memberTeams"],"sets":{
            "teams":{"kind":"principal","table":"memberTeams","select":"teamId","joins":[],
                "where":[{"table":"memberTeams","column":"memberId","context":"member.id"}]}},
            "where":{"operator":"inSet","column":"workspaceId","set":"teams"}}),
        json!({"version":2,"table":"taskNotes","key":"id","permissionTables":["memberTeams"],"sets":{},
            "where":{"operator":"inherits","column":"taskId","table":"tasks","key":"id"}}),
    ] {
        let plan: VisibilityPlan = serde_json::from_value(plan).unwrap();
        plan.validate().unwrap();
        plans.insert(plan.table.clone(), plan);
    }
    let mut session = TenantSession::anonymous(route.clone());
    session.identity.account.id = "alice".into();
    session.member.id = "alice".into();
    session.member.account_id = "alice".into();
    session.member.status = "active".into();
    session.member.role = "member".into();
    let feeds = crate::change_feed::ChangeFeedHub::new(pools.clone());
    let registry = ParentAclRegistry::default();
    let prepared = registry
        .ensure(&feeds, &control, &session, &plans, &[&plans["taskNotes"]])
        .await
        .unwrap();
    let index = prepared.get("tasks", "id").unwrap();
    assert_eq!(workspace(&index, "t1").as_deref(), Some("private"));

    // No consumer of any kind reads the feed from here on.
    script(
        &pools,
        &url,
        "UPDATE tasks SET \"workspaceId\"='team' WHERE id='t1'",
    )
    .await;
    let clock = crate::change_feed::read_clock(&pools, &route)
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(15),
        ready_at(&index, clock.revision, &clock.epoch)
    )
    .await
    .expect("the applier must reach the committed revision from the real feed"));
    assert_eq!(workspace(&index, "t1").as_deref(), Some("team"));

    // The snapshot holds the only admission slot; resolving parents must not
    // need another one.
    let mut transaction = control
        .begin_tenant_transaction(&route, true)
        .await
        .unwrap();
    let resolved = tokio::time::timeout(
        Duration::from_secs(10),
        resolve_with_parents(
            &mut transaction,
            &session,
            &plans["taskNotes"],
            clock.revision,
            &plans,
            &prepared,
            &crate::visibility_metrics::VisibilityMetrics::default(),
        ),
    )
    .await
    .expect("prepared lookups never wait for admission")
    .unwrap();
    assert!(Arc::ptr_eq(&resolved.inherited["tasks"].index, &index));
    assert!(indexes_ready(&resolved, clock.revision, &clock.epoch).await);
    // Control: loading an index inside the held snapshot is exactly the
    // nested admission the prepared API avoids; it cannot be admitted.
    let nested = registry
        .ensure_index(
            "nested-control".into(),
            IndexShape {
                table: "memberTeams".into(),
                key: "memberId".into(),
                id_column: "id".into(),
                columns: BTreeSet::from(["memberId".into()]),
            },
            || feeds.subscribe(&route),
            |shape| load_index(&control, &session, shape),
        )
        .await;
    assert!(
        matches!(
            nested,
            Err(VisibilityError::Database(DatabaseError::AdmissionTimeout))
        ),
        "a nested load must be refused by admission, got {:?}",
        nested.err()
    );
    transaction.commit().await.unwrap();

    feeds.shutdown();
    pools.close().await;
}
