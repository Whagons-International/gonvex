//! Presence cost model: old Reducer heartbeat path vs presence v2.
//!
//! `cargo run --release -p gonvex-runtime --example presence_bench`
//!
//! Old path (V8 excluded): the `presence.beat` Reducer's two `ctx.db` host
//! calls (SELECT the Member's row, UPDATE lastSeenAt/state) through the real
//! store writer with change capture and a `synchronous=FULL` commit, followed
//! by the per-subscriber encode of the resulting replica frame for every
//! subscriber of `presence.list` (the whole tenant). New path: the in-memory
//! hub (`gonvex_runtime::presence`).
//!
//! Environment: `PRESENCE_BENCH_USERS` (default 1950), `PRESENCE_BENCH_BEATS`
//! (writer samples, default 2000), `PRESENCE_BENCH_SECONDS` (simulated time,
//! default 1800).
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gonvex_protocol::PresenceState;
use gonvex_runtime::presence::{PresenceConfig, PresenceHub, PresenceSlot, ScopeKey};
use gonvex_store::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
use serde_json::json;

struct Counting;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

struct Measured {
    mean_ns: f64,
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
    allocs_per_op: f64,
}

fn summarize(mut samples: Vec<f64>, allocations: u64) -> Measured {
    let n = samples.len().max(1) as f64;
    let mean = samples.iter().sum::<f64>() / n;
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Measured {
        mean_ns: mean,
        p50_ns: quantile(&samples, 0.5),
        p95_ns: quantile(&samples, 0.95),
        p99_ns: quantile(&samples, 0.99),
        allocs_per_op: allocations as f64 / n,
    }
}

fn print(label: &str, m: &Measured) {
    println!(
        "{label:<58} mean {:>11.0} ns  p50 {:>11.0}  p95 {:>11.0}  p99 {:>11.0}  allocs/op {:>9.1}",
        m.mean_ns, m.p50_ns, m.p95_ns, m.p99_ns, m.allocs_per_op
    );
}

/// The old `presence.beat` through the real tenant writer: admission, the
/// Reducer's SELECT + UPDATE host calls, change capture and the FULL commit.
async fn old_writer(users: usize, beats: usize) -> (Measured, Measured) {
    let data = tempfile::tempdir().unwrap();
    let control_url = gonvex_store::database_url(&data.path().join("control.db"));
    let tenant_url = gonvex_store::database_url(&data.path().join("tenant.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let control = ControlPlane::connect(&control_url, pools.clone(), BTreeMap::new())
        .await
        .unwrap();
    control
        .clone()
        .provision_tenant_database(
            TenantRoute {
                project_id: "project".into(),
                tenant_id: "tenant".into(),
                database_url: tenant_url.clone(),
            },
            Vec::new(),
        )
        .await
        .unwrap();
    let mut setup = gonvex_store::begin_transaction(&pools, &tenant_url, false)
        .await
        .unwrap();
    let columns: Vec<gonvex_store::rows::ColumnInfo> = Vec::new();
    gonvex_store::execute_script(
        &mut **setup.transaction(),
        &format!(
            "CREATE TABLE \"userPresence\" (_id TEXT PRIMARY KEY, _creationTime INTEGER NOT NULL, \
             \"userId\" TEXT NOT NULL, \"lastSeenAt\" INTEGER NOT NULL, state TEXT NOT NULL);\n\
             CREATE INDEX user_presence_user ON \"userPresence\"(\"userId\");\n{}",
            gonvex_store::feed::capture_triggers_sql("userPresence", "_id", &columns)
        ),
    )
    .await
    .unwrap();
    for user in 0..users {
        sqlx::query("INSERT INTO \"userPresence\" VALUES($1,1,$2,1,'active')")
            .bind(format!("presence-{user}"))
            .bind(format!("member-{user}"))
            .execute(&mut **setup.transaction())
            .await
            .unwrap();
    }
    setup.commit().await.unwrap();

    let mut hold = Vec::with_capacity(beats);
    let mut end_to_end = Vec::with_capacity(beats);
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for beat in 0..beats {
        let member = format!("member-{}", beat % users);
        let started = Instant::now();
        let mut transaction = gonvex_store::begin_transaction(&pools, &tenant_url, false)
            .await
            .unwrap();
        let held = Instant::now();
        // Host call 1: readDataRows(userPresence where userId = member limit 1)
        let id: String =
            sqlx::query_scalar("SELECT _id FROM \"userPresence\" WHERE \"userId\"=$1 LIMIT 1")
                .bind(&member)
                .fetch_one(&mut **transaction.transaction())
                .await
                .unwrap();
        // Host call 2: ctx.db.update(userPresence, id, {lastSeenAt, state})
        sqlx::query("UPDATE \"userPresence\" SET \"lastSeenAt\"=$2, state='active' WHERE _id=$1")
            .bind(&id)
            .bind(1_000 + beat as i64)
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        transaction.commit_with_revision().await.unwrap();
        hold.push(held.elapsed().as_nanos() as f64);
        end_to_end.push(started.elapsed().as_nanos() as f64);
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    drop(control);
    pools.close().await;
    (
        summarize(hold, allocations),
        summarize(end_to_end, allocations),
    )
}

/// Per-subscriber encode of the replica frame one beat produces. The runtime
/// encodes per connection; this is a lower bound (no visibility, no socket).
fn old_fanout(users: usize, beats: usize) -> Measured {
    let row = json!({
        "_id":"presence-17","_creationTime":1_730_000_000_000_i64,"userId":"member-17",
        "lastSeenAt":1_730_000_030_000_i64,"state":"active"
    });
    let mut samples = Vec::with_capacity(beats);
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let mut bytes = 0usize;
    for beat in 0..beats {
        let started = Instant::now();
        for subscriber in 0..users {
            let frame = json!({
                "type":"replica.transaction","id":format!("sub-{subscriber}"),
                "cursor":{"revision":beat},
                "changes":[{"entity":"userPresence","id":"presence-17","operation":"update",
                            "newValue":row.clone(),"changedColumns":["lastSeenAt"]}],
            });
            bytes += serde_json::to_string(&frame).unwrap().len();
        }
        samples.push(started.elapsed().as_nanos() as f64);
    }
    std::hint::black_box(bytes);
    summarize(samples, ALLOCATIONS.load(Ordering::Relaxed) - before)
}

fn hub_with_subscribers(users: usize) -> (PresenceHub, ScopeKey, Vec<(String, Arc<PresenceSlot>)>) {
    let hub = PresenceHub::new(PresenceConfig::default());
    let scope = ScopeKey::new("project", "tenant");
    let now = Instant::now();
    let mut slots = Vec::with_capacity(users);
    for user in 0..users {
        let connection = format!("connection-{user}");
        hub.set_state(
            &connection,
            &scope,
            &format!("member-{user}"),
            PresenceState::Active,
            false,
            now,
        );
        let (slot, _) = hub.subscribe(&connection, &scope, "presence", now);
        slots.push((connection, slot));
    }
    hub.tick(now, 0);
    for (connection, slot) in &slots {
        hub.drain(connection, slot);
    }
    (hub, scope, slots)
}

fn new_paths(users: usize, ops: usize) {
    let (hub, scope, slots) = hub_with_subscribers(users);
    let now = Instant::now();

    // Old clients' short-circuited beat (connection already present: no-op).
    let mut samples = Vec::with_capacity(ops);
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for op in 0..ops {
        let user = op % users;
        let connection = &slots[user].0;
        let started = Instant::now();
        hub.legacy_beat(connection, &scope, &format!("member-{user}"), now);
        samples.push(started.elapsed().as_nanos() as f64);
    }
    print(
        "new: legacy beat short-circuit (hub only, per beat)",
        &summarize(samples, ALLOCATIONS.load(Ordering::Relaxed) - before),
    );

    // presence.set transition (in-memory update, no publish yet).
    let mut current = vec![PresenceState::Active; users];
    let flip = |state: PresenceState| {
        if state == PresenceState::Idle {
            PresenceState::Active
        } else {
            PresenceState::Idle
        }
    };
    let mut samples = Vec::with_capacity(ops);
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for op in 0..ops {
        let user = op % users;
        let state = flip(current[user]);
        current[user] = state;
        let started = Instant::now();
        hub.set_state(
            &slots[user].0,
            &scope,
            &format!("member-{user}"),
            state,
            false,
            now,
        );
        samples.push(started.elapsed().as_nanos() as f64);
    }
    print(
        "new: presence.set (per transition, before tick)",
        &summarize(samples, ALLOCATIONS.load(Ordering::Relaxed) - before),
    );
    hub.tick(now, 1);
    for (connection, slot) in &slots {
        hub.drain(connection, slot);
    }

    // One tick publishing k transitions to every subscriber, plus drains.
    for k in [1usize, 5, 20] {
        let rounds = 50;
        let mut tick_samples = Vec::with_capacity(rounds);
        let mut drain_samples = Vec::with_capacity(rounds);
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        let mut drain_allocs = 0;
        for round in 0..rounds {
            for i in 0..k {
                let user = (round * k + i) % users;
                let state = flip(current[user]);
                current[user] = state;
                hub.set_state(
                    &slots[user].0,
                    &scope,
                    &format!("member-{user}"),
                    state,
                    false,
                    now,
                );
            }
            let started = Instant::now();
            let stats = hub.tick(now, 2 + round as i64);
            tick_samples.push(started.elapsed().as_nanos() as f64);
            assert_eq!(stats.frames, users);
            let drain_before = ALLOCATIONS.load(Ordering::Relaxed);
            let started = Instant::now();
            for (connection, slot) in &slots {
                std::hint::black_box(hub.drain(connection, slot));
            }
            drain_samples.push(started.elapsed().as_nanos() as f64);
            drain_allocs += ALLOCATIONS.load(Ordering::Relaxed) - drain_before;
        }
        let total = ALLOCATIONS.load(Ordering::Relaxed) - before;
        print(
            &format!("new: tick, {k} transition(s) -> {users} subscribers"),
            &summarize(tick_samples, total - drain_allocs),
        );
        print(
            &format!("new: drain all {users} slots ({k} transition(s))"),
            &summarize(drain_samples, drain_allocs),
        );
    }
}

/// Simulated tenant: every user beats every 30 s (old: presence + support)
/// and alternates idle/active with exponential gaps averaging 101 s.
fn simulate(users: usize, seconds: usize, tick: Duration) {
    let config = PresenceConfig {
        tick,
        ..PresenceConfig::default()
    };
    let hub = PresenceHub::new(config);
    let scope = ScopeKey::new("project", "tenant");
    let start = Instant::now();
    let mut slots = Vec::with_capacity(users);
    let mut next_transition = Vec::with_capacity(users);
    let mut idle = vec![false; users];
    let mut seed = 0x9e3779b97f4a7c15_u64;
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed >> 11) as f64 / (1u64 << 53) as f64
    };
    for user in 0..users {
        let connection = format!("connection-{user}");
        hub.set_state(
            &connection,
            &scope,
            &format!("member-{user}"),
            PresenceState::Active,
            false,
            start,
        );
        let (slot, _) = hub.subscribe(&connection, &scope, "presence", start);
        slots.push((connection, slot));
        next_transition.push(-random().max(1e-6).ln() * 101.0);
    }
    hub.tick(start, 0);
    for (connection, slot) in &slots {
        hub.drain(connection, slot);
    }
    let tick_s = tick.as_secs_f64();
    let steps = (seconds as f64 / tick_s) as usize;
    let (mut transitions, mut frames, mut changes, mut bytes) = (0usize, 0usize, 0usize, 0usize);
    let mut hub_ns = 0u128;
    for step in 1..=steps {
        let now_s = step as f64 * tick_s;
        let started = Instant::now();
        for user in 0..users {
            while next_transition[user] <= now_s {
                idle[user] = !idle[user];
                let state = if idle[user] {
                    PresenceState::Idle
                } else {
                    PresenceState::Active
                };
                hub.set_state(
                    &slots[user].0,
                    &scope,
                    &format!("member-{user}"),
                    state,
                    false,
                    start,
                );
                transitions += 1;
                next_transition[user] += -random().max(1e-6).ln() * 101.0;
            }
        }
        let stats = hub.tick(start, step as i64);
        changes += stats.changes;
        for (connection, slot) in &slots {
            if let Some(frame) = hub.drain(connection, slot) {
                frames += 1;
                bytes += frame.len();
            }
        }
        hub_ns += started.elapsed().as_nanos();
    }
    let seconds = seconds as f64;
    let beats_per_s = users as f64 / 30.0;
    let transitions_per_s = transitions as f64 / seconds;
    println!(
        "simulated {users}-user tenant, {seconds:.0} s, tick {} ms:",
        tick.as_millis()
    );
    println!(
        "  old: presence beats {beats_per_s:.1}/s + support heartbeats {beats_per_s:.1}/s + transitions {transitions_per_s:.1}/s \
         = {:.1} durable reducer commits/s; presence row deliveries {:.0}/s ({:.0} beat + {:.0} transition)",
        2.0 * beats_per_s + transitions_per_s,
        (beats_per_s + transitions_per_s) * users as f64,
        beats_per_s * users as f64,
        transitions_per_s * users as f64,
    );
    println!(
        "  new: durable writes 0/s; presence frames {:.0}/s carrying {:.0} member changes/s ({:.1} distinct transitions/s); {:.0} B/s; hub CPU {:.3} ms per simulated second",
        frames as f64 / seconds,
        changes as f64 * users as f64 / seconds,
        changes as f64 / seconds,
        bytes as f64 / seconds,
        hub_ns as f64 / 1e6 / seconds,
    );
}

fn main() {
    let users = env("PRESENCE_BENCH_USERS", 1950);
    let beats = env("PRESENCE_BENCH_BEATS", 2000);
    let seconds = env("PRESENCE_BENCH_SECONDS", 1800);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    println!("users per tenant {users}; writer samples {beats}");
    let (hold, total) = runtime.block_on(old_writer(users, beats));
    print(
        "old: writer hold per beat (SELECT+UPDATE+capture+FULL commit)",
        &hold,
    );
    print("old: begin..commit incl. admission per beat", &total);
    let fanout = old_fanout(users, 200.min(beats));
    print(
        &format!("old: encode replica frame for {users} subscribers per beat"),
        &fanout,
    );
    new_paths(users, 200_000);
    let live_before = LIVE_BYTES.load(Ordering::Relaxed);
    let (hub, _, slots) = hub_with_subscribers(users);
    let live_after = LIVE_BYTES.load(Ordering::Relaxed);
    println!(
        "new: resident hub state for {users} online members + subscribers: {} bytes ({:.0} B/user, excluding the shared 1-slot Arc per connection already counted)",
        live_after.saturating_sub(live_before),
        live_after.saturating_sub(live_before) as f64 / users as f64,
    );
    drop((hub, slots));
    simulate(users, seconds, Duration::from_millis(1_000));
    simulate(users, seconds, Duration::from_millis(250));
}
