//! Exact server metadata permits log-only resume; client digests never create proof.
use gonvex_protocol::{ReplicaCursor, ReplicaOpenRequest, ServerMessage};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind, SqlValue};
use gonvex_runtime::replica::ReplicaOpenResult;
use gonvex_store::TenantSession;
use serde_json::{json, Value};

const PATH: &str = "tasks.recentWorkspaceTasks";
fn request(path: &str, args: Value, token: Option<&(ReplicaCursor, String)>) -> ReplicaOpenRequest {
    let mut r = json!({"id":path,"path":path,"args":args});
    if let Some((cursor, digest)) = token {
        r["cursor"] = json!(cursor);
        r["digest"] = json!(digest);
    }
    serde_json::from_value(r).unwrap()
}
fn token(opened: &ReplicaOpenResult) -> (ReplicaCursor, String) {
    opened
        .messages
        .iter()
        .find_map(|m| match m {
            ServerMessage::ReplicaReady { cursor, digest, .. } => {
                Some((cursor.clone(), digest.clone()))
            }
            _ => None,
        })
        .expect("ready")
}
fn evaluated() -> u64 {
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
async fn open(
    f: &Fixture,
    s: &TenantSession,
    t: Option<&(ReplicaCursor, String)>,
) -> ReplicaOpenResult {
    f.runtime
        .open_replica(s, request(PATH, json!({}), t))
        .await
        .unwrap()
}
async fn assert_digest(f: &Fixture, s: &TenantSession, opened: &ReplicaOpenResult) {
    let oracle = f
        .runtime
        .open_replica_fresh(s, request(PATH, json!({}), None))
        .await
        .unwrap();
    assert_eq!(token(opened), token(&oracle));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validated_resumes_preserve_integrity_and_scope_and_cancellation() {
    let fixture = Fixture::build(FixtureOptions::small()).await.unwrap();
    let admin = fixture.session(0, fixture.members_of_kind(0, MemberKind::Admin)[0]);
    let cold = open(&fixture, &admin, None).await;
    let initial = token(&cold);
    let before = evaluated();
    let unchanged = open(&fixture, &admin, Some(&initial)).await;
    assert_eq!(
        evaluated(),
        before,
        "no full-list visibility/cache view on anchored nochange resume"
    );
    assert_digest(&fixture, &admin, &unchanged).await;
    fixture.runtime.clear_snapshot_cache();
    let before = evaluated();
    let no_base = open(&fixture, &admin, Some(&initial)).await;
    assert_eq!(
        evaluated(),
        before,
        "anchor does not rebuild an evicted shared base"
    );
    assert_digest(&fixture, &admin, &no_base).await;

    // Altered client digests still go through the authoritative integrity path.
    let bad = (initial.0.clone(), format!("xor1:{}", "0".repeat(64)));
    let before = evaluated();
    let rejected = open(&fixture, &admin, Some(&bad)).await;
    assert!(rejected.subscription.is_none());
    assert!(evaluated() > before);
    let before = evaluated();
    let good = open(&fixture, &admin, Some(&initial)).await;
    assert_eq!(
        evaluated(),
        before,
        "an invalid digest did not poison proof"
    );
    assert_digest(&fixture, &admin, &good).await;

    // Another user/tenant/args cannot use this member's server checkpoint.
    let member = fixture.session(0, fixture.members_of_kind(0, MemberKind::TeamAllSpots)[0]);
    let before = evaluated();
    let other = open(&fixture, &member, Some(&initial)).await;
    assert!(evaluated() > before, "cross-user proof must miss");
    if other.subscription.is_some() {
        assert_digest(&fixture, &member, &other).await;
    }
    let other_tenant = fixture.session(1, 0);
    let cross = open(&fixture, &other_tenant, Some(&initial)).await;
    assert_digest(&fixture, &other_tenant, &cross).await;
    let before = evaluated();
    let args = fixture
        .runtime
        .open_replica(
            &admin,
            request(PATH, json!({"ignored":"different"}), Some(&initial)),
        )
        .await
        .unwrap();
    assert!(
        evaluated() > before,
        "even ignored args have separate proof"
    );
    assert_digest(&fixture, &admin, &args).await;

    // Write catch-up touches changed row images and preserves the SQL digest.
    let anchored = open(&fixture, &admin, None).await;
    let old = token(&anchored);
    fixture
        .update_task(0, 0, json!({"name":"resume delta edit"}))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let before = evaluated();
    let changed = open(&fixture, &admin, Some(&old)).await;
    assert_eq!(
        evaluated(),
        before,
        "stable-index source write should evaluate only changed rows"
    );
    assert_digest(&fixture, &admin, &changed).await;
    assert!(changed
        .messages
        .iter()
        .any(|m| matches!(m, ServerMessage::ReplicaDelta { .. })));

    // A client watermark can be newer than its last server checkpoint: replay
    // the prefix, validate its claimed digest, then deliver only the suffix.
    let a = token(&changed);
    fixture
        .update_task(0, 1, json!({"name":"prefix edit"}))
        .await
        .unwrap();
    let prefix = fixture
        .runtime
        .open_replica_fresh(&admin, request(PATH, json!({}), None))
        .await
        .unwrap();
    let advanced = token(&prefix);
    fixture
        .update_task(0, 2, json!({"name":"suffix edit"}))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let before = evaluated();
    let replay = open(&fixture, &admin, Some(&advanced)).await;
    assert_eq!(evaluated(), before);
    assert_digest(&fixture, &admin, &replay).await;
    assert!(token(&replay).0.revision > a.0.revision);

    // Insert and delete catch-up: reuse a real row's complete column set.
    let before_insert = token(&replay);
    let pool = fixture
        .pools()
        .reader(&admin.route.database_url)
        .await
        .unwrap();
    let mut connection = pool.acquire().await.unwrap();
    let columns = gonvex_store::rows::table_columns(&mut connection, "tasks")
        .await
        .unwrap();
    drop(connection);
    let names = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>();
    let values = columns
        .iter()
        .map(|c| {
            if c.name == "_id" {
                "'resume-insert'".to_owned()
            } else {
                format!("\"{}\"", c.name)
            }
        })
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO tasks ({}) SELECT {} FROM tasks WHERE _id=$1",
        names.join(","),
        values.join(",")
    );
    fixture
        .commit(
            0,
            &sql,
            vec![SqlValue::Text(fixture.tenant(0).tasks[0].id.clone())],
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let before = evaluated();
    let inserted = open(&fixture, &admin, Some(&before_insert)).await;
    assert_eq!(evaluated(), before);
    assert_digest(&fixture, &admin, &inserted).await;
    let inserted_token = token(&inserted);
    fixture
        .commit(0, "DELETE FROM tasks WHERE _id='resume-insert'", vec![])
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let before = evaluated();
    let removed = open(&fixture, &admin, Some(&inserted_token)).await;
    assert_eq!(evaluated(), before);
    assert_digest(&fixture, &admin, &removed).await;

    let wrong_path = fixture
        .runtime
        .open_replica(
            &admin,
            request(
                "tasks.get",
                json!({"id":fixture.tenant(0).tasks[0].id}),
                Some(&token(&removed)),
            ),
        )
        .await
        .unwrap();
    assert!(
        wrong_path.subscription.is_none(),
        "another collection's digest is not proof"
    );

    // Cancellation before acquiring the short read cannot install new proof or
    // leak admission. All held build slots and the pending future are dropped.
    let saved = token(&removed);
    let admission = gonvex_runtime::offload::SnapshotAdmission::global();
    let available = admission.available_permits();
    let mut held = Vec::new();
    while admission.available_permits() > 0 {
        held.push(admission.admit("test-hold".into(), "hold").await.unwrap());
    }
    {
        let mut pending = Box::pin(
            fixture
                .runtime
                .open_replica(&admin, request(PATH, json!({}), Some(&saved))),
        );
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
    }
    drop(held);
    assert_eq!(
        admission.available_permits(),
        available,
        "cancelled read returned admission"
    );
    let before = evaluated();
    let recovered = open(&fixture, &admin, Some(&saved)).await;
    assert_eq!(evaluated(), before);
    assert_digest(&fixture, &admin, &recovered).await;

    // New module generation, retained-history loss and real principal changes
    // all fall back to authoritative validation instead of trusting an anchor.
    fixture.redeploy(3).await.unwrap();
    let before = evaluated();
    let deployed = open(&fixture, &admin, Some(&saved)).await;
    assert!(evaluated() > before);
    if deployed.subscription.is_some() {
        assert_digest(&fixture, &admin, &deployed).await;
    }
    let fresh = open(&fixture, &admin, None).await;
    let stale = token(&fresh);
    fixture
        .update_task(0, 3, json!({"name":"after prune"}))
        .await
        .unwrap();
    fixture
        .commit(
            0,
            "UPDATE _gonvex_sync_clock SET retained_revision=revision WHERE singleton=1",
            vec![],
        )
        .await
        .unwrap();
    let pruned = open(&fixture, &admin, Some(&stale)).await;
    assert!(pruned
        .messages
        .iter()
        .any(|m| matches!(m, ServerMessage::ReplicaSnapshot { .. })));
    assert_digest(&fixture, &admin, &pruned).await;
    for _ in 0..2 {
        let ordinary = open(&fixture, &member, None).await;
        let ordinary_token = token(&ordinary);
        fixture.commit(0,"UPDATE members SET see_all_spots=CASE WHEN see_all_spots=1 THEN 0 ELSE 1 END WHERE id=$1",vec![SqlValue::Text(member.member.id.clone())]).await.unwrap();
        let before = evaluated();
        let principal = open(&fixture, &member, Some(&ordinary_token)).await;
        assert!(evaluated() > before || principal.subscription.is_none());
        if principal.subscription.is_some() {
            assert_digest(&fixture, &member, &principal).await;
        }
    }
    fixture.shutdown().await;
}
