use std::collections::BTreeMap;
use std::time::Instant;

use chrono::{TimeZone, Utc};
use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry};
use serde_json::{json, Value};
use sqlx::Row;

use super::*;

const RAW: &str = "TypeScript module host returned execution_failed: module 3f2a9c1e-0b7d-4e55-9a41-2c6f0d8e7b11 function taskSubtasks.create: Error: Workspace not found\n    at assertWorkspaceVisible$4 (file:///gonvex/3f2a9c1e/reducers.js:112:105318)\n    at create (file:///gonvex/3f2a9c1e/reducers.js:140:2201)";

fn entry(path: &str, raw: &str) -> RuntimeErrorEntry {
    RuntimeErrorEntry {
        project: "project".to_owned(),
        tenant: "tenant-a".to_owned(),
        account: "account-a".to_owned(),
        account_email: "a@example.com".to_owned(),
        account_name: "Ada".to_owned(),
        member_id: "member-a".to_owned(),
        member_name: "Ada L".to_owned(),
        kind: "reducer",
        path: path.to_owned(),
        execution_id: uuid::Uuid::new_v4().to_string(),
        command_id: Some("command-1".to_owned()),
        raw_error: raw.to_owned(),
        class: "rejected",
        level: "error",
        client_release: "5.4.90+288d9aa".to_owned(),
        client_environment: "production".to_owned(),
        connection_id: Some("connection-1".to_owned()),
        channel: None,
        device: Some(json!({"platform":"iOS","deviceType":"mobile","browserName":"Safari"})),
        args_shape: args_shape(&json!({"workspaceId":"w-1","title":"x","tags":[1,2]}), 0),
        args_bytes: 42,
        duration_ms: 3.5,
        occurred_at: Utc::now(),
    }
}

async fn control() -> (tempfile::TempDir, ControlPlane) {
    let directory = tempfile::tempdir().unwrap();
    let url = gonvex_store::database_url(&directory.path().join("control.db"));
    let control = ControlPlane::connect(
        &url,
        PoolRegistry::new(PoolLimits::default()),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    (directory, control)
}

async fn persist(control: &ControlPlane, entries: Vec<RuntimeErrorEntry>) {
    let mut window = Coalescer::default();
    for entry in entries {
        window.push(entry);
    }
    persist_window(control, window, &BTreeMap::new(), "test")
        .await
        .unwrap();
}

async fn scalar(control: &ControlPlane, sql: &str) -> i64 {
    let mut transaction = control.begin_control_transaction(true).await.unwrap();
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(&mut **transaction.transaction())
        .await
        .unwrap()
}

async fn group(control: &ControlPlane, fingerprint: &str) -> sqlx::sqlite::SqliteRow {
    let mut transaction = control.begin_control_transaction(true).await.unwrap();
    sqlx::query("SELECT * FROM gonvex_error_groups WHERE fingerprint=$1")
        .bind(fingerprint)
        .fetch_one(&mut **transaction.transaction())
        .await
        .unwrap()
}

async fn execute(control: &ControlPlane, sql: &str) {
    let mut transaction = control.begin_control_transaction(false).await.unwrap();
    sqlx::raw_sql(sql)
        .execute(&mut **transaction.transaction())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

#[test]
fn event_carries_the_clean_message_and_keeps_the_raw_text_as_stack() {
    let mut window = Coalescer::default();
    window.push(entry("taskSubtasks.create", RAW));
    let (_, group) = window.into_groups().next().unwrap();
    let event = runtime_event(&group, "g3:abcdef012345", "sha", "Acme Ops");
    assert_eq!(event["message"], "Workspace not found");
    assert_eq!(event["stack"], RAW);
    assert_eq!(event["name"], "reducerError");
    assert_eq!(event["culprit"], "reducer taskSubtasks.create");
    assert_eq!(event["environment"], "server");
    assert_eq!(event["level"], "error");
    assert_eq!(event["release"], "5.4.90+288d9aa");
    assert_eq!(
        event["account"],
        json!({"id":"account-a","email":"a@example.com","name":"Ada"})
    );
    assert_eq!(event["tenantName"], "Acme Ops");
    assert_eq!(event["device"]["platform"], "iOS");
    assert_eq!(
        event["context"]["member"],
        json!({"id":"member-a","name":"Ada L"})
    );
    assert_eq!(event["context"]["durationMs"], 3.5);
    assert_eq!(
        event["context"]["args"],
        json!({"bytes":42,"shape":{"workspaceId":"string(3)","title":"string(1)",
               "tags":{"array":2,"items":"number"}}})
    );
    assert_eq!(event["tenant"], "tenant-a");
    assert_eq!(
        event["tags"],
        json!({"source":"runtime","kind":"reducer","path":"taskSubtasks.create",
               "class":"rejected","moduleVersion":"g3:abcdef012345","runtimeVersion":"sha",
               "clientEnvironment":"production","platform":"iOS","deviceType":"mobile",
               "browser":"Safari"})
    );
    assert_eq!(event["context"]["commandId"], "command-1");
    assert_eq!(event["context"]["occurrences"], 1);
}

#[test]
fn fingerprint_groups_per_function_and_normalized_message() {
    let fingerprint = |path: &str, message: &str| runtime_fingerprint(&entry(path, message));
    assert_eq!(
        fingerprint("tasks.get", "Task 123 not found"),
        fingerprint("tasks.get", "Task 98765 not found")
    );
    assert_eq!(
        fingerprint(
            "tasks.get",
            "row 3f2a9c1e-0b7d-4e55-9a41-2c6f0d8e7b11 is locked"
        ),
        fingerprint(
            "tasks.get",
            "row 00000000-1111-2222-3333-444444444444 is locked"
        )
    );
    assert_eq!(
        fingerprint("tasks.get", "hash deadbeef1234 mismatch"),
        fingerprint("tasks.get", "hash 0123abcd9876 mismatch")
    );
    // The same throw from another module build (ids and stack frames differ).
    assert_eq!(
        fingerprint("taskSubtasks.create", RAW),
        fingerprint(
            "taskSubtasks.create",
            "TypeScript module host returned execution_failed: module 9d1b function taskSubtasks.create: Error: Workspace not found\n    at x (file:///gonvex/9d1b/reducers.js:1:2)"
        )
    );
    assert_ne!(
        fingerprint("tasks.get", "Task 1 not found"),
        fingerprint("tasks.list", "Task 1 not found")
    );
    assert_ne!(
        fingerprint("tasks.get", "Task 1 not found"),
        fingerprint("tasks.get", "Workspace 1 not found")
    );
    assert_eq!(normalize_message("Task 42 not found"), "Task <n> not found");
    assert_eq!(normalize_message("id k57abc9dz bad v2"), "id <id> bad v<n>");
}

#[test]
fn every_failure_is_captured_and_protocol_states_are_warnings() {
    let rejected = ExecutionError::InvalidArguments {
        path: "p".to_owned(),
        message: "m".to_owned(),
    };
    assert_eq!(capture_class(&rejected), ("rejected", "error"));
    assert_eq!(
        capture_class(&ExecutionError::ModuleMissing("p".to_owned())),
        ("transient", "error")
    );
    assert_eq!(
        capture_class(&ExecutionError::ClientUpdateRequired {
            expected: 1,
            active: 2
        }),
        ("update_required", "warning")
    );
    assert_eq!(
        capture_class(&ExecutionError::Database(
            gonvex_store::DatabaseError::InvalidSession
        )),
        ("unauthenticated", "warning")
    );
    // Warnings group apart from errors with the same text.
    let mut warning = entry("tasks.get", "boom");
    warning.level = "warning";
    assert_ne!(
        runtime_fingerprint(&warning),
        runtime_fingerprint(&entry("tasks.get", "boom"))
    );
    // Secret-looking argument keys never reach the stored shape.
    let mut window = Coalescer::default();
    let mut secret = entry("auth.login", "bad");
    secret.args_shape = args_shape(&json!({"email":"x","password":"hunter2"}), 0);
    window.push(secret);
    let (_, group) = window.into_groups().next().unwrap();
    let event = runtime_event(&group, "", "", "");
    assert_eq!(
        event["context"]["args"]["shape"],
        json!({"email":"string(1)"})
    );
}

#[test]
fn full_queue_drops_without_blocking() {
    let capture = RuntimeErrorCapture::with_capacity(4);
    let started = Instant::now();
    let accepted = (0..1_000)
        .filter(|_| capture.enqueue(entry("hot.loop", "boom")))
        .count();
    assert_eq!(accepted, 4);
    assert_eq!(capture.dropped(), 996);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn releases_compare_numerically_before_build_metadata() {
    assert!(release_is_older("5.4.80", "5.4.90+288d9aa"));
    assert!(release_is_older("5.4.9+aaa", "5.4.10+bbb"));
    assert!(release_is_older("v5.3", "5.4.0"));
    assert!(!release_is_older("5.4.90+abc", "5.4.90+def"));
    assert!(!release_is_older("5.4.91", "5.4.90"));
    // Unparseable releases are treated as newer.
    assert!(!release_is_older("288d9aa", "5.4.90"));
    assert!(!release_is_older("5.4.80", "development"));
    assert!(!release_is_older("", "5.4.90"));
    assert_eq!(
        newest_release(["5.4.9", "5.4.10+x", "abc"].into_iter()).as_deref(),
        Some("5.4.10+x")
    );
}

#[tokio::test]
async fn ordinary_failures_are_stored_one_event_each() {
    let (_directory, control) = control().await;
    let failures: Vec<_> = (0..3)
        .map(|index| entry("tasks.get", &format!("Task {index} not found")))
        .collect();
    let fingerprint = runtime_fingerprint(&failures[0]);
    persist(&control, failures).await;
    assert_eq!(
        scalar(&control, "SELECT count(*) FROM gonvex_error_events").await,
        3
    );
    assert_eq!(
        group(&control, &fingerprint)
            .await
            .get::<i64, _>("event_count"),
        3
    );
}

#[tokio::test]
async fn storm_coalescing_keeps_counts_exact() {
    let (_directory, control) = control().await;
    let mut entries = Vec::new();
    for index in 0..1_000 {
        let mut failure = entry("hot.loop", &format!("Task {index} not found"));
        failure.tenant = format!("tenant-{}", index % 2);
        failure.client_release = if index % 4 == 0 { "5.4.89" } else { "5.4.90" }.to_owned();
        entries.push(failure);
    }
    let fingerprint = runtime_fingerprint(&entries[0]);
    persist(&control, entries).await;
    // The first occurrences keep their own events; the storm's tail is
    // folded into the last one.
    assert_eq!(
        scalar(&control, "SELECT count(*) FROM gonvex_error_events").await,
        STORM_EVENTS_PER_FLUSH as i64
    );
    assert_eq!(
        scalar(
            &control,
            "SELECT sum(json_extract(payload,'$.count')) FROM gonvex_error_events"
        )
        .await,
        1_000
    );
    let row = group(&control, &fingerprint).await;
    assert_eq!(row.get::<i64, _>("event_count"), 1_000);
    let Json(tenants) = row.get::<Json<Value>, _>("tenants");
    assert_eq!(tenants, json!({"tenant-0":500,"tenant-1":500}));
    let Json(releases) = row.get::<Json<Value>, _>("releases");
    assert_eq!(releases, json!({"5.4.89":250,"5.4.90":750}));
    let Json(environments) = row.get::<Json<Value>, _>("environments");
    assert_eq!(environments, json!({"server":1000}));
    let Json(latest) = row.get::<Json<Value>, _>("latest_event");
    assert_eq!(latest["count"], 1_000 - STORM_EVENTS_PER_FLUSH as i64 + 1);
    assert_eq!(
        latest["context"]["executionIds"].as_array().unwrap().len(),
        20
    );

    // The next write adds to the same group.
    persist(
        &control,
        (0..3)
            .map(|_| entry("hot.loop", "Task 5 not found"))
            .collect(),
    )
    .await;
    assert_eq!(
        scalar(&control, "SELECT count(*) FROM gonvex_error_events").await,
        STORM_EVENTS_PER_FLUSH as i64 + 3
    );
    assert_eq!(
        group(&control, &fingerprint)
            .await
            .get::<i64, _>("event_count"),
        1_003
    );
}

#[tokio::test]
async fn resolved_group_ignores_older_releases_and_reopens_on_newer_ones() {
    let (_directory, control) = control().await;
    let failure = entry("tasks.get", "Task 1 not found");
    let fingerprint = runtime_fingerprint(&failure);
    persist(&control, vec![failure]).await;

    // Resolve with the default release: the newest one seen on the group.
    let mut transaction = control.begin_control_transaction(false).await.unwrap();
    let row = update_error_group(
        transaction.transaction(),
        "project",
        &fingerprint,
        "resolved",
        "",
        None,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(row.get::<String, _>("resolved_release"), "5.4.90+288d9aa");

    // Explicit release.
    let mut transaction = control.begin_control_transaction(false).await.unwrap();
    update_error_group(
        transaction.transaction(),
        "project",
        &fingerprint,
        "resolved",
        "",
        None,
        Some("5.4.91"),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    // A stale client on an older build: counted, group stays resolved.
    let mut stale = entry("tasks.get", "Task 2 not found");
    stale.client_release = "5.4.90+288d9aa".to_owned();
    persist(&control, vec![stale]).await;
    let row = group(&control, &fingerprint).await;
    assert_eq!(row.get::<String, _>("status"), "resolved");
    assert!(!row.get::<bool, _>("regression"));
    assert_eq!(row.get::<i64, _>("event_count"), 2);

    // The fixed release (or newer) failing again reopens it.
    let mut current = entry("tasks.get", "Task 3 not found");
    current.client_release = "5.4.91+abc".to_owned();
    persist(&control, vec![current]).await;
    let row = group(&control, &fingerprint).await;
    assert_eq!(row.get::<String, _>("status"), "unresolved");
    assert!(row.get::<bool, _>("regression"));
    assert_eq!(row.get::<String, _>("resolved_release"), "");
    assert_eq!(row.get::<i64, _>("event_count"), 3);

    // A failure with no release (cron, HTTP) also reopens a resolved group.
    let mut transaction = control.begin_control_transaction(false).await.unwrap();
    update_error_group(
        transaction.transaction(),
        "project",
        &fingerprint,
        "resolved",
        "",
        None,
        Some("5.4.91"),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let mut unversioned = entry("tasks.get", "Task 4 not found");
    unversioned.client_release = String::new();
    persist(&control, vec![unversioned]).await;
    assert_eq!(
        group(&control, &fingerprint)
            .await
            .get::<String, _>("status"),
        "unresolved"
    );
}

#[tokio::test]
async fn retention_applies_each_rule() {
    let (_directory, control) = control().await;
    let now = Utc.with_ymd_and_hms(2026, 11, 25, 12, 0, 0).unwrap();
    let now_ms = now.timestamp_millis();
    let day = DAY_MS;
    // Groups: busy (unresolved, 120 recent events + 5 expired), quiet
    // (unresolved, last seen 15 days ago), old (resolved, last seen 91 days
    // ago), kept (ignored, last seen 89 days ago).
    let mut sql = String::new();
    for (fingerprint, status, last_seen) in [
        ("busy", "unresolved", now_ms - day),
        ("quiet", "unresolved", now_ms - 15 * day),
        ("old", "resolved", now_ms - 91 * day),
        ("kept", "ignored", now_ms - 89 * day),
    ] {
        sql.push_str(&format!(
            "INSERT INTO gonvex_error_groups(project_id,fingerprint,title,first_seen,last_seen,event_count,latest_event,status)
             VALUES('p','{fingerprint}','t',{last_seen},{last_seen},1,'{{}}','{status}');"
        ));
    }
    let mut insert_event = |id: String, fingerprint: &str, at: i64| {
        sql.push_str(&format!(
            "INSERT INTO gonvex_error_events(project_id,event_id,fingerprint,occurred_at,payload)
             VALUES('p','{id}','{fingerprint}',{at},'{{}}');"
        ));
    };
    for index in 0..120 {
        insert_event(format!("busy-{index}"), "busy", now_ms - day - index);
    }
    for index in 0..5 {
        insert_event(format!("expired-{index}"), "busy", now_ms - 31 * day);
    }
    insert_event("quiet-1".to_owned(), "quiet", now_ms - 15 * day);
    insert_event("old-1".to_owned(), "old", now_ms - 91 * day);
    insert_event("kept-1".to_owned(), "kept", now_ms - 29 * day);
    execute(&control, &sql).await;

    let report = run_error_retention(&control, now_ms, &RetentionPolicy::default())
        .await
        .unwrap();
    assert_eq!(report.expired_events, 6, "{report:?}"); // 5 busy + old-1
    assert_eq!(report.capped_events, 20, "{report:?}");
    assert_eq!(report.auto_resolved_groups, 1, "{report:?}");
    assert_eq!(report.deleted_groups, 1, "{report:?}");
    // The newest 100 busy events survive.
    assert_eq!(
        scalar(
            &control,
            "SELECT count(*) FROM gonvex_error_events WHERE fingerprint='busy'"
        )
        .await,
        100
    );
    assert_eq!(
        scalar(
            &control,
            "SELECT count(*) FROM gonvex_error_events WHERE event_id IN ('busy-0','busy-99')"
        )
        .await,
        2
    );
    assert_eq!(
        group(&control, "quiet").await.get::<String, _>("status"),
        "resolved"
    );
    assert_eq!(
        group(&control, "busy").await.get::<String, _>("status"),
        "unresolved"
    );
    assert_eq!(
        group(&control, "kept").await.get::<String, _>("status"),
        "ignored"
    );
    assert_eq!(
        scalar(
            &control,
            "SELECT count(*) FROM gonvex_error_groups WHERE fingerprint='old'"
        )
        .await,
        0
    );
    // A second pass has nothing left to do.
    let again = run_error_retention(&control, now_ms, &RetentionPolicy::default())
        .await
        .unwrap();
    assert_eq!(again, RetentionReport::default());
}

#[tokio::test]
async fn prune_removes_old_releases_and_groups_seen_only_in_them() {
    let (_directory, control) = control().await;
    let mut old_only = entry("old.only", "gone");
    old_only.client_release = "5.4.70+a".to_owned();
    let old_fingerprint = runtime_fingerprint(&old_only);
    let mut mixed_old = entry("mixed", "boom");
    mixed_old.client_release = "5.4.79".to_owned();
    let mixed_fingerprint = runtime_fingerprint(&mixed_old);
    let mut mixed_new = entry("mixed", "boom");
    mixed_new.client_release = "5.4.80".to_owned();
    let mut unversioned = entry("server.only", "cron failed");
    unversioned.client_release = String::new();
    let mut mixed_old_again = mixed_old.clone();
    mixed_old_again.execution_id = uuid::Uuid::new_v4().to_string();
    persist(&control, vec![old_only, mixed_old, mixed_old_again]).await;
    persist(&control, vec![mixed_new, unversioned]).await;

    let report = prune_errors_before_release(&control, "project", "5.4.80")
        .await
        .unwrap();
    assert_eq!(report.deleted_groups, 1, "{report:?}");
    assert_eq!(report.updated_groups, 1, "{report:?}");
    // old.only's event and mixed's two 5.4.79 events.
    assert_eq!(report.deleted_events, 3, "{report:?}");
    assert_eq!(
        scalar(
            &control,
            &format!(
                "SELECT count(*) FROM gonvex_error_groups WHERE fingerprint='{old_fingerprint}'"
            )
        )
        .await,
        0
    );
    let mixed = group(&control, &mixed_fingerprint).await;
    assert_eq!(mixed.get::<i64, _>("event_count"), 1);
    let Json(releases) = mixed.get::<Json<Value>, _>("releases");
    assert_eq!(releases, json!({"5.4.80":1}));
    assert_eq!(
        scalar(&control, "SELECT count(*) FROM gonvex_error_groups").await,
        2
    );
    assert_eq!(
        scalar(&control, "SELECT count(*) FROM gonvex_error_events").await,
        2
    );
}
