//! A queued metadata resume must resolve permissions after admission, not
//! authorize from the anchor captured before a concurrent writer commit.
#[path = "support/replica_client.rs"]
mod replica_client;

use gonvex_protocol::{ReplicaCursor, ServerMessage};
use gonvex_runtime::offload::SnapshotAdmission;
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind, SqlValue};
use gonvex_runtime::replica::ReplicaOpenResult;
use replica_client::{ClientModel, DigestKind};
use serde_json::json;
use std::collections::BTreeSet;
use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::Duration;

const PATH: &str = "tasks.recentWorkspaceTasks";

fn client() -> ClientModel {
    let mut client = ClientModel::new(Some(DigestKind::Xor));
    client.register(PATH, PATH, "tasks", json!({}));
    client
}

fn whole_list_calls() -> u64 {
    gonvex_runtime::raw_sites::snapshot()
        .iter()
        .filter(|s| {
            matches!(
                s.0,
                "cache_view" | "cache_row_raw" | "scan_row_raw" | "scan_row_parsed"
            )
        })
        .map(|s| s.1)
        .sum()
}

fn ready(opened: &ReplicaOpenResult) -> (&ReplicaCursor, &str) {
    opened
        .messages
        .iter()
        .find_map(|m| match m {
            ServerMessage::ReplicaReady { cursor, digest, .. } => Some((cursor, digest.as_str())),
            _ => None,
        })
        .expect("authoritative open must finish ready")
}

fn assert_same_open(actual: &ReplicaOpenResult, oracle: &ReplicaOpenResult) {
    assert_eq!(actual.messages, oracle.messages, "wire messages");
    match (&actual.subscription, &oracle.subscription) {
        (None, None) => {}
        (Some(actual), Some(oracle)) => {
            assert_eq!(actual.id, oracle.id);
            assert_eq!(actual.path, oracle.path);
            assert_eq!(actual.args, oracle.args);
            assert_eq!(actual.visibility_scope, oracle.visibility_scope);
            assert_eq!(actual.cursor, oracle.cursor);
            assert_eq!(actual.resolved.revision, oracle.resolved.revision);
            assert_eq!(actual.resolved.fingerprint, oracle.resolved.fingerprint);
            assert_eq!(actual.resolved.role, oracle.resolved.role);
            assert_eq!(actual.resolved.permissions, oracle.resolved.permissions);
            assert_eq!(actual.resolved.direct, oracle.resolved.direct);
            assert_eq!(actual.resolved.sets, oracle.resolved.sets);
        }
        _ => panic!("subscription presence differs from authoritative oracle"),
    }
}

#[derive(Clone, Copy, Debug)]
enum Revoke {
    SeeAllSpots,
    TeamMembership,
    MemberRoleAndPermissions,
}

impl Revoke {
    fn member_kind(self) -> MemberKind {
        match self {
            Self::SeeAllSpots | Self::TeamMembership => MemberKind::TeamAllSpots,
            Self::MemberRoleAndPermissions => MemberKind::Admin,
        }
    }

    fn statement(self) -> &'static str {
        match self {
            Self::SeeAllSpots => "UPDATE members SET see_all_spots=0 WHERE id=$1",
            Self::TeamMembership => {
                "UPDATE memberTeams SET deletedAt=1 WHERE memberId=$1 AND deletedAt IS NULL"
            }
            Self::MemberRoleAndPermissions => {
                "UPDATE members SET role='member', permissions='{}' WHERE id=$1"
            }
        }
    }
}

async fn revoke_before_admitted_read(case: Revoke) {
    let fixture = Fixture::build(FixtureOptions::small()).await.unwrap();
    let member = fixture.members_of_kind(0, case.member_kind())[0];
    let session = fixture.session(0, member);
    // Keep this admitted session unchanged across the write: changing its
    // locator would hide the race by making the candidate a simple key miss.
    let mut client = client();
    let anchored = fixture
        .runtime
        .open_replica(&session, client.open_request(PATH, PATH, &json!({})))
        .await
        .unwrap();
    assert!(anchored.subscription.is_some());
    client.apply_all(&anchored.messages);
    assert!(client.mismatches.is_empty());
    let before_rows = client.windows[PATH].rows.clone();
    assert!(!before_rows.is_empty(), "{case:?}: needs authorized rows");
    let before_revision = ready(&anchored).0.revision;
    assert!(fixture.runtime.resume_validation_stats().0 > 0);

    // Establish that this precise request can use the metadata path before
    // the writer runs. The new regression must not pass only because every
    // open was already taking the authoritative fallback.
    let before_calls = whole_list_calls();
    let warm = fixture
        .runtime
        .open_replica(&session, client.open_request(PATH, PATH, &json!({})))
        .await
        .unwrap();
    assert_eq!(whole_list_calls(), before_calls, "{case:?}: anchor hit");
    assert_eq!(ready(&warm).0.revision, before_revision);
    client.apply_all(&warm.messages);

    let admission = SnapshotAdmission::global();
    let available = admission.available_permits();
    assert_eq!(admission.waiting(), (0, 0));
    assert_eq!(available, admission.capacity());
    // Teach the dummy hold key its empty size before holding every count
    // slot, so the test does not exhaust the byte budget while taking slots.
    let seed = admission
        .admit("permission-race-hold".into(), "permission-race-hold")
        .await
        .unwrap();
    seed.observe_json_bytes(0);
    drop(seed);
    let mut held = Vec::new();
    while admission.available_permits() > 0 {
        held.push(
            admission
                .admit("permission-race-hold".into(), "permission-race-hold")
                .await
                .unwrap(),
        );
    }

    let resume_request = client.open_request(PATH, PATH, &json!({}));
    let mut queued = Box::pin(
        fixture
            .runtime
            .open_replica(&session, resume_request.clone()),
    );
    // Poll until the real admission queue contains this future. No spawned
    // task can advance it while the writer commits below. The timeout is
    // only a failure deadline; permit/queue state determines the ordering.
    tokio::time::timeout(
        Duration::from_secs(10),
        poll_fn(|cx| {
            assert!(queued.as_mut().poll(cx).is_pending(), "slots are held");
            let (short, long) = admission.waiting();
            if short + long == 1 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("resume must reach admission before the writer starts");
    assert_eq!(whole_list_calls(), before_calls);
    assert_eq!(admission.available_permits(), 0);

    // A distinct writer commits while the resume is alive and provably
    // waiting with no read snapshot. No timing sleeps or feed-applier wait
    // are needed: resolving at the later SQL clock is the required guard.
    fixture
        .commit(
            0,
            case.statement(),
            vec![SqlValue::Text(session.member.id.clone())],
        )
        .await
        .unwrap();
    let committed_revision = fixture.revision(0).await.unwrap();
    assert!(committed_revision > before_revision, "{case:?}: real write");
    assert_eq!(admission.available_permits(), 0);
    let (short, long) = admission.waiting();
    assert_eq!(short + long, 1);

    drop(held);
    let resumed = queued.await.unwrap();
    let oracle_resume = fixture
        .runtime
        .open_replica_fresh(&session, resume_request)
        .await
        .unwrap();
    assert_same_open(&resumed, &oracle_resume);
    assert!(resumed.subscription.is_none(), "{case:?}: no stale binding");
    assert!(matches!(
        resumed.messages.as_slice(),
        [ServerMessage::ReplicaNeedHashes { .. }]
    ));
    // needHashes carries no cursor. Admission ordering plus the unchanged
    // committed clock brackets the only possible actual read revision;
    // reconciliation below checks that revision explicitly on the wire and
    // on the installed subscription, not just the aggregate hash.
    assert_eq!(fixture.revision(0).await.unwrap(), committed_revision);
    client.apply_all(&resumed.messages);

    let reconcile_request = client.open_request(PATH, PATH, &json!({}));
    assert!(reconcile_request.full_integrity);
    assert!(!reconcile_request.hashes.is_empty());
    let reconciled = fixture
        .runtime
        .open_replica(&session, reconcile_request.clone())
        .await
        .unwrap();
    let oracle_reconciled = fixture
        .runtime
        .open_replica_fresh(&session, reconcile_request)
        .await
        .unwrap();
    assert_same_open(&reconciled, &oracle_reconciled);
    assert_eq!(ready(&reconciled).0.revision, committed_revision);
    assert_eq!(
        reconciled.subscription.as_ref().unwrap().resolved.revision,
        committed_revision
    );
    client.apply_all(&reconciled.messages);

    let mut oracle_client = self::client();
    let current = fixture
        .runtime
        .open_replica_fresh(&session, oracle_client.open_request(PATH, PATH, &json!({})))
        .await
        .unwrap();
    assert_eq!(ready(&current).0.revision, committed_revision);
    oracle_client.apply_all(&current.messages);
    let expected = &oracle_client.windows[PATH].rows;
    let revoked: BTreeSet<_> = before_rows
        .keys()
        .filter(|id| !expected.contains_key(*id))
        .cloned()
        .collect();
    assert!(
        !revoked.is_empty(),
        "{case:?}: fixture must really revoke rows"
    );
    assert_eq!(
        &client.windows[PATH].rows, expected,
        "{case:?}: no leaks/misses"
    );
    assert!(revoked
        .iter()
        .all(|id| !client.windows[PATH].rows.contains_key(id)));
    assert!(client.mismatches.is_empty(), "{case:?}: integrity");
    assert!(oracle_client.mismatches.is_empty());
    assert_eq!(ready(&reconciled), ready(&current));
    assert_eq!(fixture.revision(0).await.unwrap(), committed_revision);
    assert_eq!(admission.waiting(), (0, 0));
    assert_eq!(admission.available_permits(), available);
    eprintln!(
        "{case:?}: queued before admission; writer {before_revision}->{committed_revision}; {} revoked rows; authoritative messages/subscription/rows agree",
        revoked.len()
    );
    fixture.shutdown().await;
}

// One test per binary serializes these global-admission/raw-site probes.
// A separate test target keeps the other resume test's counters and permit
// cancellation probes from running concurrently with these barriers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_metadata_resume_rechecks_committed_permission_revocations() {
    for case in [
        Revoke::SeeAllSpots,
        Revoke::TeamMembership,
        Revoke::MemberRoleAndPermissions,
    ] {
        revoke_before_admitted_read(case).await;
    }
}
