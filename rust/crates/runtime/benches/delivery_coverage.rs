//! Delivery coverage micro-driver: the whole per-change feed path of every
//! connection of the 78-member Whagons fixture tenant, each holding the 10
//! collections a lab user holds (`WHAGONS_SUBSCRIPTIONS`, including the
//! inherited `taskUsers` / `taskTags`), for the kinds of transactions the
//! lab workload commits. Reports per kind the CPU per connection and how the
//! connections were served (`replica::fanout::Path`: shared fan-out or their
//! own full path, and why).
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench delivery_coverage
//!
//! Env: BENCH_FILTER, BENCH_ITERS (default 20), BENCH_JSON_OUT (see
//! perf-fixture.md); DC_MEMBERS / DC_TASKS: use one tenant of that size
//! instead of the Whagons fixture's large tenant.
//!
//! Kinds (one transaction each, written as the Whagons reducers write them):
//! - `task_edit`: a task field edit (description) plus its `taskLogs` row;
//! - `task_view`: a `taskViews` row (opening a task's detail window);
//! - `note`: a `taskNotes` row plus its `taskLogs` row;
//! - `notification`: a `notifications` row for one member (scheduled push);
//! - `tag`: a `taskTags` row (addTag);
//! - `assign`: a `taskUsers` row for a member (assignUser) plus its log;
//! - `spot_move`: a task's `spotId` changes (visibility moves) plus its log;
//! - `presence`: a `userPresence` beat row.
//!
//! `v4/coverage/mix` weights the kinds by the lab action mix (README
//! "Action mix", production): edits 35, views 15, notes 8, notifications 10,
//! assignments 5, tags 3, spot moves 2, presence 2.
use std::collections::BTreeMap;
use std::time::Instant;

use gonvex_runtime::change_feed::FeedEvent;
use gonvex_runtime::delivery::{BatchConfig, DeliveryHub, ReplicaBatcher};
use gonvex_runtime::perf_fixture::harness::{AllocSnapshot, CountingAlloc, Report};
use gonvex_runtime::perf_fixture::{
    Fixture, FixtureOptions, MemberConnection, SqlValue, TenantSpec,
};
use gonvex_runtime::replica::{fanout_path_counts, FANOUT_PATHS};
use gonvex_websocket::OutboundBatch;
use serde_json::json;

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

const BIG: usize = 0;
const WINDOW: u8 = 12;
const THRESHOLD: usize = 1024;

const KINDS: [(&str, f64); 8] = [
    ("task_edit", 35.0),
    ("task_view", 15.0),
    ("note", 8.0),
    ("notification", 10.0),
    ("assign", 5.0),
    ("tag", 3.0),
    ("spot_move", 2.0),
    ("presence", 2.0),
];

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

fn revision(event: &FeedEvent) -> u64 {
    match event {
        FeedEvent::Transaction { revision, .. } => *revision,
        FeedEvent::Reset { .. } => 0,
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Adds the time since `.0` to `.1` when dropped.
struct Elapsed<'a>(Instant, &'a mut u64);

impl Drop for Elapsed<'_> {
    fn drop(&mut self) {
        *self.1 += self.0.elapsed().as_nanos() as u64;
    }
}

/// (count, sum ns) of a profiler stage.
fn stage(stage: gonvex_profiler::Stage) -> (u64, u64) {
    gonvex_profiler::snapshot()
        .into_iter()
        .filter(|snapshot| snapshot.stage == stage)
        .fold((0, 0), |(count, sum), snapshot| (count + snapshot.count, sum + snapshot.sum))
}

struct Conn {
    batcher: ReplicaBatcher,
    batch: OutboundBatch,
}

fn text(value: impl Into<String>) -> SqlValue {
    SqlValue::Text(value.into())
}

fn log_row(id: String, task: &str, member: &str) -> (String, Vec<SqlValue>) {
    (
        r#"INSERT INTO "taskLogs" ("_id","_creationTime","action","taskId","userId","timestamp","uuid") VALUES ($1,$2,'updated',$3,$4,$5,$1)"#.to_owned(),
        vec![text(id), SqlValue::Int(now_ms()), text(task), text(member), SqlValue::Real(now_ms() as f64)],
    )
}

/// One transaction of `kind`, number `n`.
async fn commit(fixture: &Fixture, kind: &str, n: usize, spots: &[String]) -> FeedEvent {
    let tenant = &fixture.tenants[BIG];
    let task = &tenant.tasks[(n * 7919) % tenant.tasks.len()];
    let member = &tenant.members[n % tenant.members.len()].id;
    let other = &tenant.members[(n * 31 + 5) % tenant.members.len()].id;
    let id = format!("dc-{kind}-{n}-{}", now_ms());
    let statements: Vec<(String, Vec<SqlValue>)> = match kind {
        "task_edit" => vec![
            (
                r#"UPDATE "tasks" SET "description"=$1, "updatedAt"=$2 WHERE "_id"=$3"#.to_owned(),
                vec![
                    text(format!("coverage edit {n} {}", "x".repeat(380))),
                    SqlValue::Int(now_ms()),
                    text(task.id.as_str()),
                ],
            ),
            log_row(id, &task.id, member),
        ],
        "task_view" => vec![(
            r#"INSERT INTO "taskViews" ("_id","_creationTime","taskId","userId","viewedAt") VALUES ($1,$2,$3,$4,$2)"#.to_owned(),
            vec![text(id), SqlValue::Int(now_ms()), text(task.id.as_str()), text(member.as_str())],
        )],
        "note" => vec![
            (
                r#"INSERT INTO "taskNotes" ("_id","_creationTime","taskId","userId","source","note") VALUES ($1,$2,$3,$4,'web','Synthetic local comment')"#.to_owned(),
                vec![text(id.clone()), SqlValue::Int(now_ms()), text(task.id.as_str()), text(member.as_str())],
            ),
            log_row(format!("{id}-log"), &task.id, member),
        ],
        "notification" => vec![(
            r#"INSERT INTO "notifications" ("_id","_creationTime","userId","title","message","type","isRead") VALUES ($1,$2,$3,'Reminder','A scheduled reminder','reminder',0)"#.to_owned(),
            vec![text(id), SqlValue::Int(now_ms()), text(other.as_str())],
        )],
        "tag" => vec![(
            r#"INSERT INTO "taskTags" ("_id","_creationTime","taskId","tagId","userId") VALUES ($1,$2,$3,$1,$4)"#.to_owned(),
            vec![text(id), SqlValue::Int(now_ms()), text(task.id.as_str()), text(member.as_str())],
        )],
        "assign" => vec![
            (
                r#"INSERT INTO "taskUsers" ("_id","createdAt","updatedAt","taskId","userId","workspaceId") VALUES ($1,$2,$2,$3,$4,$5)"#.to_owned(),
                vec![
                    text(id.clone()),
                    SqlValue::Int(now_ms()),
                    text(task.id.as_str()),
                    text(format!("dc-assignee-{n}")),
                    text(task.workspace_id.as_str()),
                ],
            ),
            log_row(format!("{id}-log"), &task.id, member),
        ],
        "spot_move" => vec![
            (
                r#"UPDATE "tasks" SET "spotId"=$1, "updatedAt"=$2 WHERE "_id"=$3"#.to_owned(),
                vec![
                    text(spots[n % spots.len()].as_str()),
                    SqlValue::Int(now_ms()),
                    text(task.id.as_str()),
                ],
            ),
            log_row(id, &task.id, member),
        ],
        "presence" => vec![(
            r#"UPDATE "userPresence" SET "lastSeenAt"=$1, "state"='active' WHERE "userId"=$2"#.to_owned(),
            vec![SqlValue::Int(now_ms()), text(member.as_str())],
        )],
        other => panic!("unknown kind {other}"),
    };
    fixture.commit_all(BIG, statements).await.unwrap()
}

/// One connection's whole feed step for `event` (shared outcome or full
/// path), the watermark, and the batch frame when it holds rows.
async fn feed_one(
    fixture: &Fixture,
    hub: &DeliveryHub,
    tenant_id: &str,
    connection: &mut MemberConnection,
    conn: &mut Conn,
    event: &FeedEvent,
    frame_ns: &mut u64,
) -> bool {
    let rest = fixture
        .runtime
        .deliver_feed_event(
            &connection.session,
            &mut connection.subscriptions,
            event.clone(),
            &mut conn.batcher,
            &mut conn.batch,
            hub,
            false,
        )
        .await
        .unwrap();
    assert!(rest.is_empty(), "unexpected unbatched frames {rest:?}");
    let started = Instant::now();
    let _frame = Elapsed(started, frame_ns);
    conn.batcher.watermark(
        &mut conn.batch,
        tenant_id,
        &connection.subscriptions,
        revision(event),
    );
    if conn.batcher.has_items() {
        conn.batcher
            .take_frame(&mut conn.batch, Some((WINDOW, THRESHOLD)))
            .is_some()
    } else {
        // Row-less: waits for the idle delay (not counted).
        let _ = conn.batcher.take_frame(&mut conn.batch, None);
        false
    }
}

async fn run() {
    let mut report = Report::new();
    gonvex_profiler::set_enabled(true);
    let options = match (std::env::var("DC_MEMBERS").ok(), std::env::var("DC_TASKS").ok()) {
        (None, None) => FixtureOptions::whagons(),
        _ => FixtureOptions {
            tenants: vec![TenantSpec::new(
                "large",
                env_usize("DC_TASKS", 9000),
                env_usize("DC_MEMBERS", 500),
            )],
            ..FixtureOptions::whagons()
        },
    };
    let fixture = Fixture::build(options).await.unwrap();
    let tenant_id = fixture.tenants[BIG].route.tenant_id.clone();
    let members = fixture.tenants[BIG].members.len();
    let mut spots: Vec<String> = fixture.tenants[BIG]
        .tasks
        .iter()
        .filter_map(|task| task.spot_id.clone())
        .collect();
    spots.sort();
    spots.dedup();
    let mut connections = fixture.connect_all(BIG).await.unwrap();
    let hub = DeliveryHub::default();
    let mut conns: Vec<Conn> = (0..members)
        .map(|_| {
            let mut batcher = ReplicaBatcher::with_config(BatchConfig::from_env());
            batcher.set_enabled(true);
            Conn {
                batcher,
                batch: OutboundBatch::default(),
            }
        })
        .collect();
    let iterations = env_usize("BENCH_ITERS", 20);
    let warmup = 3;
    let mut n = 0usize;
    let mut mix_ns = 0.0;
    let mut mix_weight = 0.0;
    let mut mix_paths = vec![0f64; FANOUT_PATHS.len()];
    for (kind, weight) in KINDS {
        let name = format!("v4/coverage/{members}_members/{kind}");
        if !report.enabled(&name) && !report.enabled("v4/coverage/mix") {
            continue;
        }
        let mut samples = Vec::new();
        let mut allocs = AllocSnapshot::default();
        let mut receivers = 0u64;
        let mut paths = vec![0u64; FANOUT_PATHS.len()];
        let mut frame_total = 0u64;
        let mut stages_before = ((0u64, 0u64), (0u64, 0u64));
        for i in 0..warmup + iterations {
            n += 1;
            let event = commit(&fixture, kind, n, &spots).await;
            if i == warmup {
                stages_before = (
                    stage(gonvex_profiler::Stage::DeliveryShared),
                    stage(gonvex_profiler::Stage::DeliveryIndividual),
                );
            }
            let before_paths = fanout_path_counts();
            let mut frame_ns = 0u64;
            let before = AllocSnapshot::now();
            let started = Instant::now();
            let mut received = 0;
            for (connection, conn) in connections.iter_mut().zip(conns.iter_mut()) {
                if feed_one(&fixture, &hub, &tenant_id, connection, conn, &event, &mut frame_ns).await {
                    received += 1;
                }
            }
            let elapsed = started.elapsed();
            let delta = AllocSnapshot::now().since(before);
            let after_paths = fanout_path_counts();
            if i >= warmup {
                samples.push(elapsed.as_nanos() as f64);
                allocs.allocations += delta.allocations;
                allocs.allocated_bytes += delta.allocated_bytes;
                receivers += received;
                frame_total += frame_ns;
                for (index, path) in paths.iter_mut().enumerate() {
                    *path += after_paths[index] - before_paths[index];
                }
            }
        }
        let (shared_after, full_after) = (
            stage(gonvex_profiler::Stage::DeliveryShared),
            stage(gonvex_profiler::Stage::DeliveryIndividual),
        );
        let (shared_count, shared_ns) = (
            shared_after.0 - stages_before.0 .0,
            shared_after.1 - stages_before.0 .1,
        );
        let (full_count, full_ns) = (
            full_after.0 - stages_before.1 .0,
            full_after.1 - stages_before.1 .1,
        );
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let iterations_f = iterations as f64;
        let median = samples[samples.len() / 2];
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let mut extra = BTreeMap::new();
        extra.insert("medianNs".into(), json!(median));
        extra.insert("meanNs".into(), json!(mean));
        extra.insert("p90Ns".into(), json!(samples[samples.len() * 9 / 10]));
        extra.insert("nsPerConnection".into(), json!(median / members as f64));
        extra.insert(
            "allocationsPerChange".into(),
            json!(allocs.allocations as f64 / iterations_f),
        );
        extra.insert(
            "bytesAllocatedPerChange".into(),
            json!(allocs.allocated_bytes as f64 / iterations_f),
        );
        extra.insert(
            "receiversPerChange".into(),
            json!(receivers as f64 / iterations_f),
        );
        let split: BTreeMap<String, f64> = FANOUT_PATHS
            .iter()
            .zip(&paths)
            .filter(|(_, count)| **count > 0)
            .map(|((path, _), count)| (path.to_string(), *count as f64 / iterations_f))
            .collect();
        let shared = paths[0] + paths[1];
        let total: u64 = paths.iter().sum();
        extra.insert("pathsPerChange".into(), json!(split));
        extra.insert(
            "frameNsPerChange".into(),
            json!(frame_total as f64 / iterations_f),
        );
        extra.insert(
            "sharedNsPerConnection".into(),
            json!(shared_ns as f64 / shared_count.max(1) as f64),
        );
        extra.insert(
            "fullNsPerConnection".into(),
            json!(full_ns as f64 / full_count.max(1) as f64),
        );
        extra.insert(
            "sharedNsPerChange".into(),
            json!(shared_ns as f64 / iterations_f),
        );
        extra.insert("fullNsPerChange".into(), json!(full_ns as f64 / iterations_f));
        extra.insert(
            "sharedFraction".into(),
            json!(if total == 0 { 0.0 } else { shared as f64 / total as f64 }),
        );
        eprintln!("{name}: {extra:?}");
        mix_ns += weight * mean;
        mix_weight += weight;
        for (index, path) in paths.iter().enumerate() {
            mix_paths[index] += weight * *path as f64 / iterations_f;
        }
        if report.enabled(&name) {
            report.record(&name, extra);
        }
    }
    let name = "v4/coverage/mix";
    if report.enabled(name) && mix_weight > 0.0 {
        let mut extra = BTreeMap::new();
        let per_change = mix_ns / mix_weight;
        extra.insert("meanNsPerChange".into(), json!(per_change));
        extra.insert("nsPerConnection".into(), json!(per_change / members as f64));
        let total: f64 = mix_paths.iter().sum();
        extra.insert(
            "sharedFraction".into(),
            json!(if total == 0.0 { 0.0 } else { (mix_paths[0] + mix_paths[1]) / total }),
        );
        let split: BTreeMap<String, f64> = FANOUT_PATHS
            .iter()
            .zip(&mix_paths)
            .filter(|(_, count)| **count > 0.0)
            .map(|((path, _), count)| (path.to_string(), *count / total))
            .collect();
        extra.insert("pathShare".into(), json!(split));
        extra.insert("weights".into(), json!(KINDS.iter().map(|(k, w)| (k.to_string(), *w)).collect::<BTreeMap<_, _>>()));
        eprintln!("{name}: {extra:?}");
        report.record(name, extra);
    }
    report.finish();
}
