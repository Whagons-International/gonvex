//! A resume with a valid cursor and digest reads the change log only: no
//! full-set query (no per-member SQL scan, counted by `raw_sites`), also on a
//! collection that narrow members made sparse (perf/resume-fix: the sparse
//! bypass of perf/cache-heap was per key, so one window of narrow opens sent
//! every member's resume back to the full SQL scan for 10 minutes).
//!
//!   cargo test -p gonvex-runtime --features perf-fixture --test resume_log_only

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

struct Opened {
    ready: (ReplicaCursor, String),
    snapshot_rows: Option<usize>,
    need_hashes: bool,
    /// (per-member SQL rows, cache-fill rows) the open read.
    read: (u64, u64),
}

async fn open(
    fixture: &Fixture,
    session: &TenantSession,
    resume: Option<&(ReplicaCursor, String)>,
) -> Opened {
    let before = rows_read();
    let opened = fixture
        .runtime
        .open_replica(session, request(resume))
        .await
        .unwrap();
    let after = rows_read();
    let mut result = Opened {
        ready: (
            ReplicaCursor {
                epoch: String::new(),
                revision: 0,
            },
            String::new(),
        ),
        snapshot_rows: None,
        need_hashes: false,
        read: (after.0 - before.0, after.1 - before.1),
    };
    for message in opened.messages {
        match message {
            ServerMessage::ReplicaReady { cursor, digest, .. } => result.ready = (cursor, digest),
            ServerMessage::ReplicaSnapshot { result: rows, .. } => {
                result.snapshot_rows = Some(rows.len())
            }
            ServerMessage::ReplicaNeedHashes { .. } => result.need_hashes = true,
            _ => {}
        }
    }
    result
}

/// The fresh SQL path's digest at the current clock (the oracle).
async fn fresh_digest(fixture: &Fixture, session: &TenantSession) -> String {
    let opened = fixture
        .runtime
        .open_replica_fresh(session, request(None))
        .await
        .unwrap();
    opened
        .messages
        .into_iter()
        .find_map(|message| match message {
            ServerMessage::ReplicaReady { digest, .. } => Some(digest),
            _ => None,
        })
        .unwrap()
}

async fn assert_log_only(
    fixture: &Fixture,
    session: &TenantSession,
    token: &(ReplicaCursor, String),
    context: &str,
) -> (ReplicaCursor, String) {
    let resumed = open(fixture, session, Some(token)).await;
    assert!(!resumed.need_hashes, "{context}: needHashes");
    assert_eq!(resumed.snapshot_rows, None, "{context}: resumed with a snapshot");
    assert_eq!(
        resumed.read,
        (0, 0),
        "{context}: a valid-cursor resume read (per-member SQL rows, cache-fill rows)"
    );
    assert_eq!(
        resumed.ready.1,
        fresh_digest(fixture, session).await,
        "{context}: digest"
    );
    resumed.ready
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_cursor_resumes_read_no_full_set_even_on_a_sparse_key() {
    let mut options = FixtureOptions::small();
    // Large enough to count as a sparse entry (GONVEX_SNAPSHOT_CACHE_SPARSE_MIN_ROWS
    // 1000), and enough members for several narrow (GrantOnly) ones.
    options.tenants = vec![TenantSpec::new("big", 2_500, 24)];
    let fixture = Fixture::build(options).await.unwrap();
    let admin = fixture.session(0, fixture.members_of_kind(0, MemberKind::Admin)[0]);
    let broad = fixture.session(0, fixture.members_of_kind(0, MemberKind::TeamAllSpots)[0]);
    let narrow = fixture.members_of_kind(0, MemberKind::GrantOnly);
    assert!(narrow.len() >= 4, "{} narrow members", narrow.len());

    // A cold open builds the shared entry once.
    let first = open(&fixture, &admin, None).await;
    assert_eq!(first.snapshot_rows, Some(2_500));
    assert_eq!(first.read.0, 0, "a cold open through the cache scans no member rows");
    assert_eq!(first.read.1, 2_500, "the shared entry is filled once");

    // Resume, nothing changed: log only.
    let token = assert_log_only(&fixture, &admin, &first.ready, "no changes").await;
    // Resume after writes: log only, with a delta.
    for task in 0..5 {
        fixture
            .update_task(0, task * 17, json!({"name": format!("edit {task}")}))
            .await
            .unwrap();
    }
    let token = assert_log_only(&fixture, &admin, &token, "after 5 writes").await;

    // Mixed population (the regression): narrow members open the same key.
    // Each narrow member's own later opens read SQL through the index, but
    // the broad member's resume stays log-only.
    for member in narrow.iter().take(4) {
        let session = fixture.session(0, *member);
        open(&fixture, &session, None).await;
    }
    let narrow_session = fixture.session(0, narrow[0]);
    let skips = fixture.runtime.snapshot_cache_sparse_counts().1;
    let narrow_open = open(&fixture, &narrow_session, None).await;
    assert!(
        narrow_open.read.0 > 0 && narrow_open.read.1 == 0,
        "a narrow member reads SQL: {:?}",
        narrow_open.read
    );
    assert!(fixture.runtime.snapshot_cache_sparse_counts().1 > skips);
    fixture
        .update_task(0, 3, json!({"name": "after narrow opens"}))
        .await
        .unwrap();
    let token = assert_log_only(&fixture, &admin, &token, "broad member after narrow opens").await;

    // An all-narrow window drops the entry and makes the key sparse.
    fixture.runtime.clear_snapshot_cache();
    for member in narrow.iter().take(4) {
        let session = fixture.session(0, *member);
        open(&fixture, &session, None).await;
    }
    let (drops, _) = fixture.runtime.snapshot_cache_sparse_counts();
    assert!(drops >= 1, "the narrow-only opens made the key sparse");
    // A broad member the key does not know yet: its first open reads SQL
    // and marks it broad, which revives the key. Its resume refills the
    // entry once (a shared read, no member scan); the next one reads only
    // the log.
    let unknown = open(&fixture, &broad, None).await;
    assert!(unknown.read.0 > 0, "an unknown member on a sparse key reads SQL");
    let refill = open(&fixture, &broad, Some(&unknown.ready)).await;
    assert!(!refill.need_hashes);
    assert_eq!(refill.snapshot_rows, None);
    assert_eq!(refill.read.0, 0, "the revived key: no member scan");
    assert_log_only(&fixture, &broad, &refill.ready, "revived key").await;
    // The admin (forgotten by the clear) behaves the same way.
    let admin_open = open(&fixture, &admin, Some(&token)).await;
    assert!(!admin_open.need_hashes);
    assert_log_only(&fixture, &admin, &admin_open.ready, "admin on the revived key").await;
}
