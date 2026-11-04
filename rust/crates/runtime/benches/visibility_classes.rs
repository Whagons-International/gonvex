//! Visibility classes (workstream 7): interpreted vs compiled checks.
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench visibility_classes
//!
//! Env: BENCH_FILTER, BENCH_ITERS, BENCH_JSON_OUT (see perf-fixture.md);
//! VIS_MEMBERS (default 1950) and VIS_TASKS (default 9000) for the large tenant.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use gonvex_runtime::change_feed::{FeedEvent, LogChange};
use gonvex_runtime::perf_fixture::harness::{retained, BenchConfig, CountingAlloc, Report};
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberConnection, MemberKind, TenantSpec,
};
use gonvex_runtime::visibility_class::{EvalMemo, RowView};
use serde_json::json;

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const TASKS: &str = "tasks.recentWorkspaceTasks";

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

fn task_change(event: &FeedEvent) -> LogChange {
    match event {
        FeedEvent::Transaction { changes, .. } => changes
            .iter()
            .find(|change| change.table == "tasks")
            .cloned()
            .expect("task change"),
        _ => panic!("expected a transaction"),
    }
}

async fn sample_changes(fixture: &Fixture, tenant: usize, count: usize) -> Vec<LogChange> {
    let tasks = fixture.tenant(tenant).tasks.len();
    let mut changes = Vec::new();
    for i in 0..count {
        // Spread over the task table: spotless, blank-spot and spotted tasks.
        let task = (i * 7919) % tasks;
        let event = fixture
            .update_task(
                tenant,
                task,
                json!({"description": format!("visibility sample {i}")}),
            )
            .await
            .unwrap();
        changes.push(task_change(&event));
    }
    changes
}

async fn bench_tenant(report: &mut Report, label: &str, fixture: &Fixture, tenant: usize) {
    let members = fixture.tenant(tenant).members.len();
    let started = std::time::Instant::now();
    let (connections, opened) = retained(async {
        let mut connections = Vec::with_capacity(members);
        for member in 0..members {
            connections.push(
                fixture
                    .connect_paths(tenant, member, &[TASKS])
                    .await
                    .unwrap(),
            );
        }
        connections
    })
    .await;
    let mut connections: Vec<MemberConnection> = connections;
    eprintln!(
        "{label}: {members} members opened {TASKS} in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let changes = sample_changes(fixture, tenant, 32).await;

    // Correctness on the sampled changes: compiled == interpreted per member.
    for change in &changes {
        let interpreted = Fixture::evaluate_visibility(&connections, TASKS, change);
        let (compiled, _) = Fixture::evaluate_visibility_compiled(&mut connections, TASKS, change);
        assert_eq!(interpreted, compiled, "{label}: admitted counts differ");
    }

    // ---- per change, every member: interpreted vs compiled -------------
    let mut index = 0;
    let mut admitted = 0;
    let name = format!("visibility/{label}/task_update/{members}_members/interpreted");
    if let Some(result) = report.run_sync(
        &name,
        BenchConfig::new(5, 100),
        members as f64,
        "subscriber",
        |_| {
            index += 1;
            admitted = std::hint::black_box(Fixture::evaluate_visibility(
                &connections,
                TASKS,
                &changes[index % changes.len()],
            ));
        },
    ) {
        result
            .extra
            .insert("checksPerChange".into(), json!(members));
    }
    let mut stats = Default::default();
    let name = format!("visibility/{label}/task_update/{members}_members/compiled");
    if let Some(result) = report.run_sync(
        &name,
        BenchConfig::new(5, 100),
        members as f64,
        "subscriber",
        |_| {
            index += 1;
            let (count, run) = Fixture::evaluate_visibility_compiled(
                &mut connections,
                TASKS,
                &changes[index % changes.len()],
            );
            admitted = std::hint::black_box(count);
            stats = run;
        },
    ) {
        result.extra.insert(
            "classEvaluationsPerChange".into(),
            json!(stats.class_evaluations),
        );
        result.extra.insert(
            "memberEvaluationsPerChange".into(),
            json!(stats.member_evaluations),
        );
        result.extra.insert(
            "compiledChecksPerChange".into(),
            json!(stats.compiled_checks),
        );
        result.extra.insert(
            "fallbackChecksPerChange".into(),
            json!(stats.fallback_checks),
        );
    }

    // ---- classes, memory -------------------------------------------------
    let handles = Fixture::visibility_handles(&mut connections, TASKS);
    let compiled: Vec<_> = handles.iter().flatten().cloned().collect();
    let Some(first) = compiled.first() else {
        eprintln!("{label}: tasks plan falls back; no class statistics");
        return;
    };
    let scope = first.scope().clone();
    let mut per_class: BTreeMap<u64, usize> = BTreeMap::new();
    let mut per_kind: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
    for (connection, handle) in connections.iter().zip(&handles) {
        if let Some(handle) = handle {
            *per_class.entry(handle.class().id()).or_default() += 1;
            let kind = fixture.tenant(tenant).members[connection.member].kind;
            per_kind
                .entry(format!("{kind:?}"))
                .or_default()
                .insert(handle.class().id());
        }
    }
    let stats = scope.stats();
    let mut sizes: Vec<usize> = per_class.values().copied().collect();
    sizes.sort_unstable_by(|a, b| b.cmp(a));
    let member_bytes: usize = compiled.iter().map(|member| member.heap_bytes()).sum();
    let resolved_bytes: usize = connections
        .iter()
        .map(|connection| Fixture::resolved_heap_bytes(connection, TASKS))
        .sum();
    let mut extra = BTreeMap::new();
    extra.insert("members".into(), json!(members));
    extra.insert("classes".into(), json!(stats.classes));
    extra.insert("gatedClasses".into(), json!(stats.gated_classes));
    extra.insert(
        "largestClasses".into(),
        json!(sizes.iter().take(8).collect::<Vec<_>>()),
    );
    extra.insert(
        "singletonClasses".into(),
        json!(sizes.iter().filter(|size| **size == 1).count()),
    );
    extra.insert(
        "classesPerKind".into(),
        json!(per_kind
            .iter()
            .map(|(kind, ids)| (kind.clone(), ids.len()))
            .collect::<BTreeMap<_, _>>()),
    );
    extra.insert("classBytes".into(), json!(stats.class_bytes));
    extra.insert(
        "bytesPerClass".into(),
        json!(stats.class_bytes / stats.classes.max(1)),
    );
    extra.insert("memberBindingBytes".into(), json!(member_bytes));
    extra.insert(
        "bytesPerMemberBinding".into(),
        json!(member_bytes / compiled.len().max(1)),
    );
    extra.insert("internedTokens".into(), json!(stats.tokens));
    extra.insert("internerBytes".into(), json!(stats.interner_bytes));
    extra.insert(
        "resolvedVisibilityBytesPerMember".into(),
        json!(resolved_bytes / members.max(1)),
    );
    extra.insert("openLiveBytes".into(), json!(opened.live_bytes));
    report.record(&format!("visibility/{label}/classes"), extra);

    // ---- token -> class routing (workstream 5 API) ------------------------
    for member in &compiled {
        scope.register(member);
    }
    let mut visits = 0usize;
    let mut admitted_routed = 0usize;
    let name = format!("visibility/{label}/task_update/{members}_members/candidates");
    if let Some(result) = report.run_sync(&name, BenchConfig::new(5, 200), 1.0, "change", |_| {
        index += 1;
        let change = &changes[index % changes.len()];
        let old = scope.decode(RowView::Value(&change.old_value));
        let new = scope.decode(RowView::Value(&change.new_value));
        let candidates = scope.candidates(Some(&old), Some(&new));
        visits = candidates.classes.len() + candidates.members.len();
        let hit: BTreeSet<*const _> = candidates.members.iter().map(Arc::as_ptr).collect();
        let mut admitted = 0;
        let mut defaults: BTreeMap<u64, bool> = BTreeMap::new();
        for class in &candidates.classes {
            defaults.insert(class.id(), class.visible_default(&new));
        }
        for member in &compiled {
            admitted += if hit.contains(&Arc::as_ptr(member)) {
                member.visible_row(&new)
            } else {
                defaults.get(&member.class().id()).copied().unwrap_or(false)
            } as usize;
        }
        admitted_routed = std::hint::black_box(admitted);
    }) {
        result
            .extra
            .insert("visitedClassesAndMembers".into(), json!(visits));
        result
            .extra
            .insert("admitted".into(), json!(admitted_routed));
        result.extra.insert(
            "note".into(),
            json!("includes the O(members) admission tally; visits is the routing work"),
        );
    }
    // Routing visit counts over all sampled changes.
    let mut total_visits = 0usize;
    for change in &changes {
        let old = scope.decode(RowView::Value(&change.old_value));
        let new = scope.decode(RowView::Value(&change.new_value));
        let candidates = scope.candidates(Some(&old), Some(&new));
        total_visits += candidates.classes.len() + candidates.members.len();
        // Soundness: no member that sees the row is outside the candidates.
        for member in &compiled {
            let routed = candidates.members.iter().any(|m| Arc::ptr_eq(m, member))
                || candidates
                    .classes
                    .iter()
                    .any(|c| Arc::ptr_eq(c, member.class()));
            assert!(
                routed || !member.visible_row(&new),
                "{label}: routing missed a member"
            );
        }
    }
    let mut extra = BTreeMap::new();
    extra.insert(
        "meanVisitsPerChange".into(),
        json!(total_visits as f64 / changes.len() as f64),
    );
    extra.insert("members".into(), json!(members));
    extra.insert("classes".into(), json!(stats.classes));
    report.record(&format!("visibility/{label}/routing"), extra);

    // Decoding: from a parsed Value vs raw JSON bytes.
    let raw = serde_json::to_vec(&changes[0].new_value).unwrap();
    let value = &changes[0].new_value;
    report.run_sync(
        &format!("visibility/{label}/decode/value"),
        BenchConfig::new(100, 5000),
        1.0,
        "row",
        |_| {
            std::hint::black_box(scope.decode(RowView::Value(value)));
        },
    );
    if let Some(result) = report.run_sync(
        &format!("visibility/{label}/decode/raw_json"),
        BenchConfig::new(100, 5000),
        1.0,
        "row",
        |_| {
            std::hint::black_box(scope.decode(RowView::Json(&raw)));
        },
    ) {
        result.extra.insert("rowBytes".into(), json!(raw.len()));
    }
    // One member check without the memo (decode included).
    let member = compiled
        .iter()
        .zip(&connections)
        .find(|(_, connection)| {
            fixture.tenant(tenant).members[connection.member].kind == MemberKind::TeamSpots
        })
        .map(|(member, _)| member.clone())
        .unwrap_or_else(|| first.clone());
    report.run_sync(
        &format!("visibility/{label}/single_check/TeamSpots"),
        BenchConfig::new(100, 5000),
        1.0,
        "check",
        |_| {
            std::hint::black_box(member.visible(RowView::Value(value)));
        },
    );
    let mut memo = EvalMemo::default();
    std::hint::black_box(member.visible_memo(&mut memo, value));
    drop(connections);
}

async fn run() {
    let mut report = Report::new();
    let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
    bench_tenant(&mut report, "fixture", &fixture, 0).await;
    fixture.shutdown().await;

    let members = std::env::var("VIS_MEMBERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1950);
    let tasks = std::env::var("VIS_TASKS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(9000);
    if report.enabled(&format!("visibility/large/task_update/{members}_members"))
        || report.enabled("visibility/large/")
    {
        let started = std::time::Instant::now();
        let fixture = Fixture::build(FixtureOptions {
            tenants: vec![TenantSpec::new("large", tasks, members)],
            ..FixtureOptions::whagons()
        })
        .await
        .unwrap();
        eprintln!(
            "large fixture ready in {:.1}s",
            started.elapsed().as_secs_f64()
        );
        bench_tenant(&mut report, "large", &fixture, 0).await;
        fixture.shutdown().await;
    }
    report.finish();
}
