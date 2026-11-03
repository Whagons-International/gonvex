//! Replica subscription state (workstream 5): retained bytes per subscriber,
//! snapshot open time and peak heap, feed apply cost per change, resume and
//! principal-change cost, on the shared Whagons fixture.
//!
//!   cargo bench -p gonvex-runtime --features perf-fixture --bench replica_state
//!
//! Env: BENCH_FILTER=<substring>, BENCH_ITERS=<n> (default per section).
//! Uses only public protocol/runtime entry points so it runs unchanged on the
//! base branch for before/after numbers.

#[path = "../tests/support/replica_client.rs"]
mod replica_client;

use std::alloc::{GlobalAlloc, Layout};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use gonvex_protocol::ServerMessage;
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions, MemberKind, SqlValue, WHAGONS_SUBSCRIPTIONS};
use gonvex_runtime::replica::ReplicaSubscription;
use gonvex_store::TenantSession;
use replica_client::{ClientModel, DigestKind};
use serde_json::{json, Value};

struct PeakAlloc;
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);
static ALLOCS: AtomicU64 = AtomicU64::new(0);

fn grow(size: i64) {
    let live = LIVE.fetch_add(size, Relaxed) + size;
    PEAK.fetch_max(live, Relaxed);
}

unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        grow(layout.size() as i64);
        mimalloc::MiMalloc.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        grow(layout.size() as i64);
        mimalloc::MiMalloc.alloc_zeroed(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as i64, Relaxed);
        mimalloc::MiMalloc.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        grow(new_size as i64 - layout.size() as i64);
        mimalloc::MiMalloc.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: PeakAlloc = PeakAlloc;

const BIG: usize = 0;
const TASKS: &str = "tasks.recentWorkspaceTasks";

/// Measures one async op: wall time, allocations, peak heap above the
/// starting live bytes, and live bytes left behind.
struct Sample {
    nanos: f64,
    allocations: u64,
    peak: i64,
    retained: i64,
}

async fn measure<T, F: std::future::Future<Output = T>>(op: F) -> (T, Sample) {
    let live = LIVE.load(Relaxed);
    PEAK.store(live, Relaxed);
    let allocations = ALLOCS.load(Relaxed);
    let started = Instant::now();
    let value = op.await;
    let nanos = started.elapsed().as_nanos() as f64;
    let sample = Sample {
        nanos,
        allocations: ALLOCS.load(Relaxed) - allocations,
        peak: PEAK.load(Relaxed) - live,
        retained: LIVE.load(Relaxed) - live,
    };
    (value, sample)
}

struct Row {
    name: String,
    values: BTreeMap<String, Value>,
}

#[derive(Default)]
struct Out {
    rows: Vec<Row>,
}

impl Out {
    fn record(&mut self, name: &str, values: Value) {
        let Value::Object(values) = values else { unreachable!() };
        eprintln!("{name}: {}", Value::Object(values.clone()));
        self.rows.push(Row {
            name: name.into(),
            values: values.into_iter().collect(),
        });
    }
    fn summarize(&mut self, name: &str, samples: &[Sample], units: f64, extra: Value) {
        let mut nanos = samples.iter().map(|s| s.nanos).collect::<Vec<_>>();
        nanos.sort_by(f64::total_cmp);
        let median = nanos[nanos.len() / 2];
        let mut values = json!({
            "iterations": samples.len(),
            "median_ms": median / 1e6,
            "per_unit_us": median / units / 1e3,
            "allocations_per_op": samples.iter().map(|s| s.allocations).sum::<u64>() as f64 / samples.len() as f64,
            "peak_heap_bytes_max": samples.iter().map(|s| s.peak).max().unwrap_or(0),
            "retained_bytes_median": {
                let mut r = samples.iter().map(|s| s.retained).collect::<Vec<_>>();
                r.sort();
                r[r.len() / 2]
            },
        });
        if let (Value::Object(values), Value::Object(extra)) = (&mut values, extra) {
            values.extend(extra);
        }
        self.record(name, values);
    }
}

fn enabled(name: &str) -> bool {
    std::env::var("BENCH_FILTER")
        .ok()
        .filter(|filter| !filter.is_empty())
        .is_none_or(|filter| name.contains(&filter))
}

fn iterations(default: usize) -> usize {
    std::env::var("BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn wire_bytes(messages: &[ServerMessage]) -> usize {
    messages
        .iter()
        .map(|message| serde_json::to_string(message).unwrap().len())
        .sum()
}

/// One member connection driven through the client model: opens, feed
/// application, and the reopens the model asks for (needHashes / reset).
struct ModelConnection {
    session: TenantSession,
    subscriptions: BTreeMap<String, ReplicaSubscription>,
    model: ClientModel,
}

impl ModelConnection {
    async fn open(fixture: &Fixture, session: TenantSession, paths: &[&str]) -> Self {
        let mut connection = Self {
            session,
            subscriptions: BTreeMap::new(),
            model: ClientModel::new(Some(DigestKind::Xor)),
        };
        for path in paths {
            connection.model.register(path, path, "", json!({}));
            connection.reopen(fixture, path).await;
        }
        connection
    }

    async fn reopen(&mut self, fixture: &Fixture, id: &str) -> (usize, usize) {
        let path = self.model.windows[id].path.clone();
        let request = self.model.open_request(id, &path, &json!({}));
        let request_bytes = serde_json::to_string(&request).unwrap().len();
        let opened = fixture
            .runtime
            .open_replica_negotiated(&self.session, request, false)
            .await
            .unwrap();
        if let Some(subscription) = opened.subscription {
            self.model.windows.get_mut(id).unwrap().entity = subscription.definition.table.clone();
            self.subscriptions.insert(id.to_owned(), subscription);
        } else {
            self.subscriptions.remove(id);
        }
        self.model.apply_all(&opened.messages);
        (request_bytes, wire_bytes(&opened.messages))
    }

    /// Applies `event` and settles every reopen it causes. Returns
    /// (client->server bytes, server->client bytes).
    async fn settle(
        &mut self,
        fixture: &Fixture,
        event: gonvex_runtime::change_feed::FeedEvent,
    ) -> (usize, usize) {
        let messages = fixture
            .runtime
            .apply_feed_event(&self.session, &mut self.subscriptions, event)
            .await
            .unwrap();
        let mut up = 0;
        let mut down = wire_bytes(&messages);
        self.model.apply_all(&messages);
        for _ in 0..4 {
            let pending = self.model.pending_reopens();
            if pending.is_empty() {
                break;
            }
            for id in pending {
                let (request, response) = self.reopen(fixture, &id).await;
                up += request;
                down += response;
            }
        }
        (up, down)
    }

    /// Compares every window with a fresh cold snapshot (the SQL oracle).
    async fn oracle(&self, fixture: &Fixture) -> Result<(), String> {
        for (id, window) in &self.model.windows {
            let opened = fixture
                .runtime
                .open_replica_negotiated(
                    &self.session,
                    serde_json::from_value(json!({"id": id, "path": window.path, "args": {}}))
                        .unwrap(),
                    false,
                )
                .await
                .unwrap();
            let Some(ServerMessage::ReplicaSnapshot { result, key, .. }) = opened.messages.first()
            else {
                return Err(format!("{id}: no snapshot"));
            };
            let expected = result
                .iter()
                .map(|row| (replica_client::key_of(row, key), Value::clone(row)))
                .collect::<BTreeMap<_, _>>();
            if expected != window.rows {
                let missing = expected.keys().filter(|k| !window.rows.contains_key(*k)).count();
                let leaked = window.rows.keys().filter(|k| !expected.contains_key(*k)).count();
                let stale = expected
                    .iter()
                    .filter(|(k, v)| window.rows.get(*k).is_some_and(|row| row != *v))
                    .count();
                return Err(format!("{id}: missing {missing} leaked {leaked} stale {stale}"));
            }
        }
        if !self.model.mismatches.is_empty() {
            return Err(format!("digest mismatches: {:?}", self.model.mismatches));
        }
        Ok(())
    }
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

async fn run() {
    let mut out = Out::default();
    let started = Instant::now();
    let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
    eprintln!("fixture ready in {:.1}s", started.elapsed().as_secs_f64());
    let members = fixture.tenant(BIG).members.len();
    let team_member = fixture.members_of_kind(BIG, MemberKind::TeamAllSpots)[0];

    // ---- Retained subscription state ------------------------------------
    let (connections, sample) = measure(fixture.connect_all(BIG)).await;
    let mut connections = connections.unwrap();
    let subscriptions = members * WHAGONS_SUBSCRIPTIONS.len();
    out.record(
        "retained/all_members_10_subscriptions",
        json!({
            "members": members,
            "subscriptions": subscriptions,
            "retained_bytes": sample.retained,
            "bytes_per_member": sample.retained / members as i64,
            "bytes_per_subscription": sample.retained / subscriptions as i64,
            "projected_1950_members_bytes": sample.retained / members as i64 * 1950,
            "open_all_ms": sample.nanos / 1e6,
            "peak_heap_bytes": sample.peak,
        }),
    );
    {
        let session = fixture.session(BIG, team_member);
        let (connection, sample) =
            measure(ModelConnection::open(&fixture, session, &[TASKS])).await;
        out.record(
            "retained/one_tasks_subscription/TeamAllSpots",
            json!({
                "rows": connection.model.windows[TASKS].rows.len(),
                "retained_bytes_incl_test_client": sample.retained,
            }),
        );
        let subscription_alone = {
            let live = LIVE.load(Relaxed);
            drop(connection);
            live - LIVE.load(Relaxed)
        };
        eprintln!("(one tasks connection incl. client model: {subscription_alone} bytes)");
    }

    // ---- Snapshot open ---------------------------------------------------
    for kind in [
        MemberKind::Admin,
        MemberKind::TeamAllSpots,
        MemberKind::TeamSpots,
        MemberKind::GrantOnly,
    ] {
        let name = format!("snapshot_open/{TASKS}/{kind:?}");
        if !enabled(&name) {
            continue;
        }
        let member = fixture.members_of_kind(BIG, kind)[0];
        let session = fixture.session(BIG, member);
        let mut samples = Vec::new();
        let mut rows = 0;
        let mut bytes = 0;
        for i in 0..iterations(10) + 2 {
            let (opened, sample) = measure(fixture.open_replica(&session, TASKS, TASKS)).await;
            let opened = opened.unwrap();
            if let Some(ServerMessage::ReplicaSnapshot { result, .. }) = opened.messages.first() {
                rows = result.len();
            }
            bytes = wire_bytes(&opened.messages);
            drop(opened);
            if i >= 2 {
                samples.push(sample);
            }
        }
        out.summarize(&name, &samples, 1.0, json!({"rows": rows, "wire_bytes": bytes}));
    }

    // ---- Feed apply: one task update to every member's 10 collections ----
    let spotless: Vec<usize> = fixture
        .tenant(BIG)
        .tasks
        .iter()
        .enumerate()
        .filter(|(_, task)| task.spot_id.is_none())
        .map(|(i, _)| i)
        .collect();
    for (label, presence) in [("task_update", false), ("presence_beat", true)] {
        let name = format!("apply_feed/{label}/{members}_members");
        if !enabled(&name) {
            continue;
        }
        let mut samples = Vec::new();
        let mut messages = 0;
        let mut bytes = 0;
        for i in 0..iterations(30) + 3 {
            let event = if presence {
                fixture.beat_presence(BIG, i % members).await.unwrap()
            } else {
                let task = spotless[i % spotless.len()];
                fixture
                    .update_task(BIG, task, json!({"description": format!("fan-out edit {i} {}", "x".repeat(380))}))
                    .await
                    .unwrap()
            };
            let (result, sample) = measure(async {
                let mut all = Vec::new();
                for connection in connections.iter_mut() {
                    all.push(
                        fixture
                            .runtime
                            .apply_feed_event(&connection.session, &mut connection.subscriptions, event.clone())
                            .await
                            .unwrap(),
                    );
                }
                all
            })
            .await;
            messages = result.iter().map(Vec::len).sum::<usize>();
            bytes = result.iter().map(|m| wire_bytes(m)).sum::<usize>();
            drop(result);
            if i >= 3 {
                samples.push(sample);
            }
        }
        out.summarize(
            &name,
            &samples,
            members as f64,
            json!({"unit": "subscriber connection", "messages": messages, "wire_bytes": bytes}),
        );
    }
    drop(connections);

    // ---- Resume from a client cursor --------------------------------------
    for changes in [0usize, 10, 200] {
        let name = format!("resume/{TASKS}/TeamAllSpots/after_{changes}_changes");
        if !enabled(&name) {
            continue;
        }
        let session = fixture.session(BIG, team_member);
        let mut samples = Vec::new();
        let mut up = 0;
        let mut down = 0;
        let mut oracle = Ok(());
        for i in 0..iterations(8) + 1 {
            let mut connection = ModelConnection::open(&fixture, session.clone(), &[TASKS]).await;
            connection.subscriptions.clear();
            for c in 0..changes {
                let task = spotless[(i * 7919 + c * 31) % spotless.len()];
                fixture
                    .update_task(BIG, task, json!({"description": format!("resume edit {i} {c}")}))
                    .await
                    .unwrap();
            }
            let ((request, response), sample) = measure(connection.reopen(&fixture, TASKS)).await;
            up = request;
            down = response;
            if i == 0 {
                oracle = connection.oracle(&fixture).await;
            } else {
                samples.push(sample);
            }
        }
        out.summarize(
            &name,
            &samples,
            1.0,
            json!({"request_bytes": up, "response_bytes": down, "oracle": format!("{oracle:?}")}),
        );
    }

    // ---- Principal change: a member gains / loses a team -------------------
    let name = "principal_change/team_toggle/TeamSpots/10_subscriptions";
    if enabled(name) {
        let member = fixture.members_of_kind(BIG, MemberKind::TeamSpots)[0];
        let fixture_member = fixture.tenant(BIG).members[member].clone();
        let other_team = fixture
            .tenant(BIG)
            .members
            .iter()
            .flat_map(|m| m.teams.iter())
            .find(|team| !fixture_member.teams.contains(team))
            .cloned()
            .expect("a team the member is not in");
        let session = fixture.session(BIG, member);
        let mut connection =
            ModelConnection::open(&fixture, session, &WHAGONS_SUBSCRIPTIONS).await;
        let mut samples = Vec::new();
        let mut gained = Vec::new();
        let mut lost = Vec::new();
        let mut wire = (0, 0);
        for i in 0..iterations(6) * 2 {
            let event = if i % 2 == 0 {
                fixture
                    .commit(
                        BIG,
                        r#"INSERT INTO "memberTeams"("_id","_creationTime","memberId","userId","teamId","roleKey","createdAt","updatedAt") VALUES('bench-team-toggle',1,$1,$1,$2,'member',1,1)"#,
                        vec![SqlValue::Text(fixture_member.id.clone()), SqlValue::Text(other_team.clone())],
                    )
                    .await
                    .unwrap()
            } else {
                fixture
                    .commit(BIG, r#"DELETE FROM "memberTeams" WHERE "_id"='bench-team-toggle'"#, Vec::new())
                    .await
                    .unwrap()
            };
            let before = connection.model.windows[TASKS].rows.len();
            let (bytes, sample) = measure(connection.settle(&fixture, event)).await;
            let after = connection.model.windows[TASKS].rows.len();
            if i % 2 == 0 { gained.push(after as i64 - before as i64) } else { lost.push(before as i64 - after as i64) }
            wire = bytes;
            samples.push(sample);
        }
        let oracle = connection.oracle(&fixture).await;
        out.summarize(
            name,
            &samples,
            1.0,
            json!({
                "task_rows_gained": gained.first(),
                "task_rows_lost": lost.first(),
                "client_to_server_bytes": wire.0,
                "server_to_client_bytes": wire.1,
                "need_hashes": connection.model.need_hashes,
                "resets": connection.model.resets,
                "oracle": format!("{oracle:?}"),
            }),
        );
    }

    println!("\n| benchmark | values |\n|---|---|");
    for row in &out.rows {
        println!("| {} | {} |", row.name, serde_json::to_string(&row.values).unwrap());
    }
    fixture.shutdown().await;
}
