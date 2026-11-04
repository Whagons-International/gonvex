//! Cost of the Whagons legacy (v1) visibility plans vs their version 2
//! masks, on the shared fixture (9,000 tasks, 78 members x 10 collections).
//!
//! For each write kind, every member's subscriptions apply the committed
//! transaction (`apply_feed_event`, the work of the websocket loop's
//! `delivery.visibility` profiler stage; no encode). Reported per write:
//! `delivery.visibility` per connection (p50/p99/max) and in total,
//! visibility SQL statements, collection snapshots (rebuilds), principal
//! rebuilds, row checks (compiled / fallback), and all of it per collection.
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench visibility_v2
//!   BENCH_FILTER=legacy/ BENCH_ITERS=10 ... (one mode or write)
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::perf_fixture::harness::{BenchConfig, CountingAlloc, Report};
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, FixtureVisibility, MemberConnection, MemberKind, SqlValue,
    WHAGONS_SUBSCRIPTIONS,
};
use gonvex_runtime::visibility_metrics::VisibilityMetricsSnapshot;
use serde_json::{json, Value};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const BIG: usize = 0;

fn main() {
    let workers = std::env::var("BENCH_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

/// One write delivered to every connection, as the websocket loop's
/// `delivery.visibility` stage measures it: per connection, the replica
/// `apply_feed_event` (the fixture opens no live queries). Each subscription
/// is applied on its own so time, visibility SQL and rebuilds can be
/// attributed to the collection (and so the table and plan) that caused them.
#[derive(Default)]
struct Attributed {
    /// `delivery.visibility` per connection (sum over its subscriptions).
    per_connection_ns: Vec<u64>,
    /// path -> (ns, visibility SQL, collection snapshots, principal rebuilds)
    per_collection: BTreeMap<String, (u64, u64, u64, u64)>,
    messages: usize,
    totals: VisibilityMetricsSnapshot,
    /// Collections the runtime dropped (reset / rebuild) that the client
    /// then reopens cold, and the time of those reopens (the `snapshot.*`
    /// stages on staging, not `delivery.visibility`).
    reopens: usize,
    reopen_ns: u64,
    /// Per connection: its apply plus its reopens. On the staging runtime
    /// (f3ef8ae1) a legacy dependency change re-ran the collection snapshot
    /// inline in `apply_feed_event`, so this is what its
    /// `delivery.visibility` stage measured.
    per_connection_inline_ns: Vec<u64>,
    /// path -> (reopens, ns)
    reopens_per_collection: BTreeMap<String, (u64, u64)>,
}

fn diff(a: VisibilityMetricsSnapshot, b: VisibilityMetricsSnapshot) -> VisibilityMetricsSnapshot {
    VisibilityMetricsSnapshot {
        visibility_sql_statements: a.visibility_sql_statements - b.visibility_sql_statements,
        collection_snapshots: a.collection_snapshots - b.collection_snapshots,
        live_query_reruns: a.live_query_reruns - b.live_query_reruns,
        principal_rebuilds: a.principal_rebuilds - b.principal_rebuilds,
        consistency_rebuilds: a.consistency_rebuilds - b.consistency_rebuilds,
        in_memory_row_checks: a.in_memory_row_checks - b.in_memory_row_checks,
        compiled_row_checks: a.compiled_row_checks - b.compiled_row_checks,
        class_evaluations: a.class_evaluations - b.class_evaluations,
        member_evaluations: a.member_evaluations - b.member_evaluations,
        fallback_row_checks: a.fallback_row_checks - b.fallback_row_checks,
    }
}

async fn deliver_attributed(
    fixture: &Fixture,
    connections: &mut [MemberConnection],
    event: &FeedEvent,
) -> Attributed {
    let metrics = fixture.runtime.visibility_metrics();
    let start = metrics.snapshot();
    let mut out = Attributed::default();
    for connection in connections.iter_mut() {
        let mut connection_ns = 0u64;
        let paths: Vec<String> = connection.subscriptions.keys().cloned().collect();
        for path in paths {
            let subscription = connection.subscriptions.remove(&path).unwrap();
            let mut one = BTreeMap::from([(path.clone(), subscription)]);
            let before = metrics.snapshot();
            let started = std::time::Instant::now();
            let messages = fixture
                .runtime
                .apply_feed_event(&connection.session, &mut one, event.clone())
                .await
                .unwrap();
            let ns = started.elapsed().as_nanos() as u64;
            let delta = diff(metrics.snapshot(), before);
            connection_ns += ns;
            out.messages += messages.len();
            let entry = out.per_collection.entry(path.clone()).or_default();
            entry.0 += ns;
            entry.1 += delta.visibility_sql_statements;
            entry.2 += delta.collection_snapshots;
            entry.3 += delta.principal_rebuilds;
            connection.subscriptions.extend(one);
        }
        out.per_connection_ns.push(connection_ns);
    }
    out.totals = diff(metrics.snapshot(), start);
    for (index, connection) in connections.iter_mut().enumerate() {
        let mut connection_ns = 0;
        for path in WHAGONS_SUBSCRIPTIONS {
            if connection.subscriptions.contains_key(path) {
                continue;
            }
            let started = std::time::Instant::now();
            let opened = fixture
                .open_replica(&connection.session, path, path)
                .await
                .unwrap();
            let ns = started.elapsed().as_nanos() as u64;
            if let Some(subscription) = opened.subscription {
                connection
                    .subscriptions
                    .insert(path.to_owned(), subscription);
            }
            out.reopens += 1;
            connection_ns += ns;
            let entry = out
                .reopens_per_collection
                .entry(path.to_owned())
                .or_default();
            entry.0 += 1;
            entry.1 += ns;
        }
        out.reopen_ns += connection_ns;
        out.per_connection_inline_ns
            .push(out.per_connection_ns[index] + connection_ns);
    }
    out
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn extra(extra: &mut BTreeMap<String, Value>, deliveries: &[Attributed], members: usize) {
    let per =
        |f: &dyn Fn(&Attributed) -> u64| median(deliveries.iter().map(|d| f(d) as f64).collect());
    let total = per(&|d| d.per_connection_ns.iter().sum());
    extra.insert(
        "deliveryVisibilityTotalNsMedian".into(),
        json!(total.round()),
    );
    extra.insert(
        "deliveryVisibilityNsPerConnection".into(),
        json!((total / members as f64).round()),
    );
    let mut all: Vec<f64> = deliveries
        .iter()
        .flat_map(|d| d.per_connection_ns.iter().map(|ns| *ns as f64))
        .collect();
    all.sort_by(|a, b| a.partial_cmp(b).unwrap());
    extra.insert(
        "deliveryVisibilityP50Ns".into(),
        json!(percentile(&all, 0.5).round()),
    );
    extra.insert(
        "deliveryVisibilityP99Ns".into(),
        json!(percentile(&all, 0.99).round()),
    );
    extra.insert(
        "deliveryVisibilityMaxNs".into(),
        json!(all.last().copied().unwrap_or(0.0).round()),
    );
    extra.insert("messages".into(), json!(per(&|d| d.messages as u64)));
    extra.insert("reopens".into(), json!(per(&|d| d.reopens as u64)));
    extra.insert(
        "reopenNsMedian".into(),
        json!(per(&|d| d.reopen_ns).round()),
    );
    let mut inline: Vec<f64> = deliveries
        .iter()
        .flat_map(|d| d.per_connection_inline_ns.iter().map(|ns| *ns as f64))
        .collect();
    inline.sort_by(|a, b| a.partial_cmp(b).unwrap());
    extra.insert(
        "inlineP50Ns".into(),
        json!(percentile(&inline, 0.5).round()),
    );
    extra.insert(
        "inlineP99Ns".into(),
        json!(percentile(&inline, 0.99).round()),
    );
    extra.insert(
        "inlineMaxNs".into(),
        json!(inline.last().copied().unwrap_or(0.0).round()),
    );
    extra.insert(
        "inlineTotalNsMedian".into(),
        json!(per(&|d| d.per_connection_inline_ns.iter().sum()).round()),
    );
    let mut reopened = serde_json::Map::new();
    if let Some(last) = deliveries.last() {
        for (path, (count, ns)) in &last.reopens_per_collection {
            reopened.insert(path.clone(), json!({"reopens": count, "ns": ns}));
        }
    }
    extra.insert("reopensPerCollection".into(), Value::Object(reopened));
    extra.insert(
        "visibilitySql".into(),
        json!(per(&|d| d.totals.visibility_sql_statements)),
    );
    extra.insert(
        "collectionSnapshots".into(),
        json!(per(&|d| d.totals.collection_snapshots)),
    );
    extra.insert(
        "principalRebuilds".into(),
        json!(per(&|d| d.totals.principal_rebuilds)),
    );
    extra.insert(
        "compiledRowChecks".into(),
        json!(per(&|d| d.totals.compiled_row_checks)),
    );
    extra.insert(
        "fallbackRowChecks".into(),
        json!(per(&|d| d.totals.fallback_row_checks)),
    );
    // Per collection: median ns per write, and SQL / rebuilds of the last write.
    let mut collections = serde_json::Map::new();
    if let Some(last) = deliveries.last() {
        for (path, (_, sql, snapshots, rebuilds)) in &last.per_collection {
            let ns = median(
                deliveries
                    .iter()
                    .map(|d| d.per_collection[path].0 as f64)
                    .collect(),
            );
            collections.insert(path.clone(), json!({"ns": ns.round(), "sql": sql, "snapshots": snapshots, "principalRebuilds": rebuilds}));
        }
    }
    extra.insert("perCollection".into(), Value::Object(collections));
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

async fn run() {
    let mut report = Report::new();
    let iterations: usize = std::env::var("BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);
    let warmup: usize = std::env::var("BENCH_WARMUP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    for (mode, visibility) in [
        ("legacy", FixtureVisibility::Legacy),
        ("v2", FixtureVisibility::V2),
    ] {
        if !report.enabled(&format!("visibility_v2/{mode}/")) && !report.enabled(mode) {
            continue;
        }
        let started = std::time::Instant::now();
        let fixture = Fixture::build(FixtureOptions {
            visibility,
            ..FixtureOptions::whagons()
        })
        .await
        .unwrap();
        let connect = std::time::Instant::now();
        let connections = RefCell::new(fixture.connect_all(BIG).await.unwrap());
        let connect_ms = connect.elapsed().as_secs_f64() * 1e3;
        let members = connections.borrow().len();
        eprintln!(
            "{mode}: fixture + connect in {:.1}s (connect {connect_ms:.0} ms)",
            started.elapsed().as_secs_f64()
        );
        let mut open = BTreeMap::new();
        open.insert("connectAllMs".into(), json!(connect_ms.round()));
        open.insert(
            "snapshotRows".into(),
            json!(connections
                .borrow()
                .iter()
                .map(|c| c.snapshot_rows)
                .sum::<usize>()),
        );
        report.record(
            &format!("visibility_v2/{mode}/connect_all/{members}_members"),
            open,
        );

        let tenant = fixture.tenant(BIG);
        let tasks = tenant.tasks.clone();
        let grant_only = fixture.members_of_kind(BIG, MemberKind::GrantOnly);
        let team_members = fixture.members_of_kind(BIG, MemberKind::TeamSpots);
        let teams: Vec<String> = {
            let mut teams: Vec<_> = tenant
                .members
                .iter()
                .flat_map(|m| m.teams.clone())
                .collect();
            teams.sort();
            teams.dedup();
            teams
        };
        let member_ids: Vec<String> = tenant.members.iter().map(|m| m.id.clone()).collect();
        let spots: Vec<String> = tasks.iter().filter_map(|t| t.spot_id.clone()).collect();
        let workspace = tasks[0].workspace_id.clone();
        let counter = Cell::new(0usize);
        let fresh = || {
            counter.set(counter.get() + 1);
            counter.get()
        };

        type Make<'a> = Box<dyn Fn(usize) -> (String, Vec<SqlValue>) + 'a>;
        let writes: Vec<(&str, Make)> = vec![
            (
                "task_update_description",
                Box::new(|i| {
                    (
                        r#"UPDATE "tasks" SET "description"=$1,"updatedAt"=$2 WHERE "_id"=$3"#
                            .into(),
                        vec![
                            text(&format!("edit {i} {}", "x".repeat(380))),
                            (1_770_000_000_000i64 + i as i64).into(),
                            text(&tasks[(i * 7) % tasks.len()].id),
                        ],
                    )
                }),
            ),
            (
                "task_spot_move",
                Box::new(|i| {
                    (
                        r#"UPDATE "tasks" SET "spotId"=$1 WHERE "_id"=$2"#.into(),
                        vec![
                            text(&spots[(i * 13 + 3) % spots.len()]),
                            text(&tasks[(i * 11 + 1) % tasks.len()].id),
                        ],
                    )
                }),
            ),
            // Committed by `insert_like` in the setup below.
            ("task_insert", Box::new(|_| unreachable!())),
            (
                "task_assign",
                Box::new(|i| {
                    (
                r#"INSERT INTO "taskUsers"("_id","createdAt","role","taskId","updatedAt","userId","workspaceId") VALUES($1,$2,'assignee',$3,$2,$4,$5)"#.into(),
                vec![text(&format!("bench-tu-{}", fresh())), 1_770_000_000_000i64.into(), text(&tasks[(i * 17 + 5) % tasks.len()].id), text(&member_ids[grant_only[i % grant_only.len()]]), text(&workspace)],
            )
                }),
            ),
            (
                "task_share_user",
                Box::new(|i| {
                    (
                r#"INSERT INTO "taskShares"("_id","createdAt","permission","sharedToUserId","status","taskId","updatedAt") VALUES($1,$2,'view',$3,'active',$4,$2)"#.into(),
                vec![text(&format!("bench-sh-{}", fresh())), 1_770_000_000_000i64.into(), text(&member_ids[grant_only[(i + 1) % grant_only.len()]]), text(&tasks[(i * 19 + 7) % tasks.len()].id)],
            )
                }),
            ),
            (
                "approval_insert",
                Box::new(|i| {
                    (
                r#"INSERT INTO "taskApprovalInstances"("_id","approverUserId","createdAt","isRequired","orderIndex","status","taskId","updatedAt") VALUES($1,$2,$3,1,0,'pending',$4,$3)"#.into(),
                vec![text(&format!("bench-ap-{}", fresh())), text(&member_ids[team_members[i % team_members.len()]]), 1_770_000_000_000i64.into(), text(&tasks[(i * 23 + 9) % tasks.len()].id)],
            )
                }),
            ),
            (
                "member_team_join",
                Box::new(|i| {
                    (
                r#"INSERT INTO "memberTeams"("_id","_creationTime","memberId","userId","teamId","roleKey","createdAt","updatedAt") VALUES($1,$2,$3,$3,$4,'member',$2,$2)"#.into(),
                vec![text(&format!("bench-mt-{}", fresh())), 1_770_000_000_000i64.into(), text(&member_ids[grant_only[i % grant_only.len()]]), text(&teams[i % teams.len()])],
            )
                }),
            ),
            (
                "presence_beat",
                Box::new(|i| {
                    (
                r#"UPDATE "userPresence" SET "lastSeenAt"=$1,"state"='active' WHERE "userId"=$2"#.into(),
                vec![(1_770_000_000_000i64 + i as i64).into(), text(&member_ids[i % member_ids.len()])],
            )
                }),
            ),
        ];
        for (write, make) in &writes {
            let name = format!("visibility_v2/{mode}/{write}/{members}_members");
            let stats = RefCell::new(Vec::new());
            let make = &make;
            let (fixture_ref, tasks_ref) = (&fixture, &tasks);
            let result = report
                .run(
                    &name,
                    BenchConfig::new(warmup, iterations),
                    members as f64,
                    "subscriber",
                    |i| async move {
                        if *write == "task_insert" {
                            let source = &tasks_ref[(i * 29 + 11) % tasks_ref.len()].id;
                            let id = format!("bench-task-{mode}-{i}");
                            return fixture_ref
                                .insert_like(
                                    BIG,
                                    "tasks",
                                    "_id",
                                    source,
                                    &[
                                        ("_id", text(&id)),
                                        ("id", SqlValue::Int(10_000_000 + i as i64)),
                                    ],
                                )
                                .await
                                .unwrap();
                        }
                        let (statement, binds) = make(i);
                        fixture_ref.commit(BIG, &statement, binds).await.unwrap()
                    },
                    |_, event: FeedEvent| {
                        let (connections, stats) = (&connections, &stats);
                        async move {
                            let mut connections = connections.borrow_mut();
                            let delivery =
                                deliver_attributed(fixture_ref, &mut connections, &event).await;
                            stats.borrow_mut().push(delivery);
                        }
                    },
                )
                .await;
            if let Some(result) = result {
                extra(&mut result.extra, &stats.take(), members);
            }
        }
        drop(connections);
        fixture.shutdown().await;
    }
    report.finish();
}
