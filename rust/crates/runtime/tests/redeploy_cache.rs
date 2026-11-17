//! An app deploy that changes no collection keeps the shared snapshot cache:
//! entries are keyed by what their rows depend on (definition, visibility
//! plan, table schema), not by module generation. Before, a deploy (a new
//! generation) missed every key, so every connected client's reopen after
//! `system.reload` rebuilt every collection's entry (staging 2026-11-04
//! 21:29: ~140-170 collections per client).
//!
//!   cargo test -p gonvex-runtime --features perf-fixture --test redeploy_cache

use gonvex_protocol::{ReplicaCursor, ReplicaOpenRequest, ServerMessage};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind, TenantSpec};
use gonvex_store::TenantSession;
use serde_json::json;

const TASKS: &str = "tasks.recentWorkspaceTasks";

fn request(resume: Option<&(ReplicaCursor, String)>) -> ReplicaOpenRequest {
    let mut request = json!({"id": TASKS, "path": TASKS, "args": {}});
    if let Some((cursor, digest)) = resume {
        request["cursor"] = serde_json::to_value(cursor).unwrap();
        request["digest"] = json!(digest);
    }
    serde_json::from_value(request).unwrap()
}

/// Rows read by per-member SQL scans and by cache fills.
fn rows_read() -> (u64, u64) {
    let mut scans = 0;
    let mut fills = 0;
    for (name, calls, ..) in gonvex_runtime::raw_sites::snapshot() {
        match name {
            "scan_row_raw" | "scan_row_parsed" => scans += calls,
            "cache_row_raw" | "cache_row_parsed" => fills += calls,
            _ => {}
        }
    }
    (scans, fills)
}

/// (ready cursor + digest, snapshot rows if any, needHashes, rows read).
async fn open(
    fixture: &Fixture,
    session: &TenantSession,
    resume: Option<&(ReplicaCursor, String)>,
) -> ((ReplicaCursor, String), Option<usize>, bool, (u64, u64)) {
    let before = rows_read();
    let opened = fixture
        .runtime
        .open_replica(session, request(resume))
        .await
        .unwrap();
    let after = rows_read();
    let mut ready = None;
    let mut rows = None;
    let mut need_hashes = false;
    for message in opened.messages {
        match message {
            ServerMessage::ReplicaReady { cursor, digest, .. } => ready = Some((cursor, digest)),
            ServerMessage::ReplicaSnapshot { result, .. } => rows = Some(result.len()),
            ServerMessage::ReplicaNeedHashes { .. } => need_hashes = true,
            _ => {}
        }
    }
    (
        ready.expect("ready"),
        rows,
        need_hashes,
        (after.0 - before.0, after.1 - before.1),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deploy_that_changes_no_collection_keeps_cache_entries_and_log_only_resumes() {
    let mut options = FixtureOptions::small();
    options.tenants = vec![TenantSpec::new("big", 2_500, 8)];
    let fixture = Fixture::build(options).await.unwrap();
    let admin = fixture.session(0, fixture.members_of_kind(0, MemberKind::Admin)[0]);

    let (token, rows, _, read) = open(&fixture, &admin, None).await;
    assert_eq!(rows, Some(2_500));
    assert_eq!(read, (0, 2_500), "the cold open fills the shared entry");
    let builds = fixture.runtime.snapshot_cache_stats().builds;

    fixture.redeploy(2).await.unwrap();
    fixture
        .update_task(0, 3, json!({"name": "after the deploy"}))
        .await
        .unwrap();

    // The client's reopen after `system.reload`: its stored cursor + digest.
    let (resumed, rows, need_hashes, read) = open(&fixture, &admin, Some(&token)).await;
    assert!(!need_hashes);
    assert_eq!(rows, None, "resumed without a snapshot");
    assert_eq!(read, (0, 0), "no member scan and no cache refill");
    assert_eq!(
        fixture.runtime.snapshot_cache_stats().builds,
        builds,
        "the deploy kept the entry"
    );
    let fresh = fixture
        .runtime
        .open_replica_fresh(&admin, request(None))
        .await
        .unwrap()
        .messages
        .into_iter()
        .find_map(|message| match message {
            ServerMessage::ReplicaReady { digest, .. } => Some(digest),
            _ => None,
        })
        .unwrap();
    assert_eq!(resumed.1, fresh, "digest equals a fresh snapshot's");

    // A member opening cold after the deploy is served from the same entry.
    let other = fixture.session(0, fixture.members_of_kind(0, MemberKind::TeamAllSpots)[0]);
    let (_, rows, _, read) = open(&fixture, &other, None).await;
    assert!(rows.is_some());
    assert_eq!(read, (0, 0), "cold open after the deploy: a cache hit");
    fixture.shutdown().await;
}
