use super::*;
use serde_json::json;
struct CountingAlloc;
static CAPTURE_ALL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static ALL_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ALL_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn count_all(size: usize) {
    use std::sync::atomic::Ordering::Relaxed;
    if CAPTURE_ALL.load(Relaxed) {
        ALL_COUNT.fetch_add(1, Relaxed);
        ALL_BYTES.fetch_add(size as u64, Relaxed);
    }
}
pub(super) fn capture_all(start: bool) -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    CAPTURE_ALL.store(false, Relaxed);
    let value = (ALL_COUNT.swap(0, Relaxed), ALL_BYTES.swap(0, Relaxed));
    CAPTURE_ALL.store(start, Relaxed);
    value
}

thread_local! { static ALLOCS: std::cell::Cell<Option<(u64,u64)>> = const { std::cell::Cell::new(None) }; }
unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        count_all(layout.size());
        let _ = ALLOCS.try_with(|c| {
            if let Some((n, b)) = c.get() {
                c.set(Some((n + 1, b + layout.size() as u64)));
            }
        });
        unsafe { std::alloc::GlobalAlloc::alloc(&std::alloc::System, layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::GlobalAlloc::dealloc(&std::alloc::System, ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        count_all(size);
        let _ = ALLOCS.try_with(|c| {
            if let Some((n, b)) = c.get() {
                c.set(Some((n + 1, b + size as u64)));
            }
        });
        unsafe { std::alloc::GlobalAlloc::realloc(&std::alloc::System, ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;
fn cpu_ns() -> u128 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) },
        0
    );
    ts.tv_sec as u128 * 1_000_000_000 + ts.tv_nsec as u128
}
fn rss_bytes() -> Option<u64> {
    let text = fs::read_to_string("/proc/self/status").ok()?;
    text.lines()
        .find(|s| s.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()
        .map(|n| n * 1024)
}
fn authority() -> Authority {
    Authority {
        identity: Identity {
            canonical_set:
                b"exact project/db/tenant/epoch/policy/projection/team/global/user atoms".to_vec(),
            source_authority: b"complete synthetic source authority".to_vec(),
            module: "module-sha".into(),
            permission_generation: 1,
            permission_serial: 1,
            encoding: ENCODING.into(),
        },
        epoch: "epoch".into(),
        revision: 10,
        retained_revision: 0,
        index_ready: true,
        dependency_complete: true,
        active_account: true,
        active_member: true,
        supported_shared_collection: true,
        now_ms: 100,
        valid_until_ms: 10_000,
    }
}
fn store() -> (tempfile::TempDir, Arc<Store>) {
    let root = std::env::var_os("STARTER_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/mnt/data/work/gonvex-starter-files/tests"));
    fs::create_dir_all(&root).unwrap();
    let dir = tempfile::tempdir_in(root).unwrap();
    let store = Store::open(
        dir.path(),
        Limits {
            piece_bytes: 1024,
            ..Limits::default()
        },
    )
    .unwrap();
    (dir, store)
}
fn build(store: &Arc<Store>, a: &Authority) -> Manifest {
    assert!(store.enqueue_after_miss(a));
    let mut b = store.next().unwrap();
    b.begin(a).unwrap();
    for id in 0..40 {
        b.row(&json!({"id": id, "text": "synthetic repeated description".repeat(5)}))
            .unwrap();
    }
    b.publish(|| Ok(a.clone())).unwrap()
}
#[test]
fn immutable_pieces_dedup_and_all_authority_boundaries_fail_closed() {
    let (_dir, store) = store();
    let a = authority();
    let m = build(&store, &a);
    let first = fs::read_dir(store.root.join("pieces")).unwrap().count();
    build(&store, &a);
    assert_eq!(
        first,
        fs::read_dir(store.root.join("pieces")).unwrap().count()
    );
    let mut mutations = vec![];
    for field in 0..14 {
        let mut x = a.clone();
        match field {
            0 => x.identity.canonical_set.push(0),
            1 => x.identity.module.push('x'),
            2 => x.identity.permission_generation += 1,
            3 => x.epoch.push('x'),
            4 => x.retained_revision = m.revision + 1,
            5 => x.now_ms = m.expires_ms,
            6 => x.active_member = false,
            7 => x.active_account = false,
            8 => x.supported_shared_collection = false,
            9 => x.identity.permission_serial += 1,
            10 => x.identity.encoding.push_str("-different"),
            11 => x.identity.source_authority.push(0),
            12 => x.index_ready = false,
            _ => x.dependency_complete = false,
        };
        mutations.push(x);
    }
    for x in mutations {
        assert!(store
            .read(
                &a.identity,
                || Ok(x.clone()),
                |_, _| panic!("unauthorized bytes")
            )
            .is_err());
    }
    let mut delivered = 0;
    store
        .read(
            &a.identity,
            || Ok(a.clone()),
            |p, b| {
                assert_eq!(p.bytes as usize, b.len());
                delivered += 1;
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(delivered, m.pieces.len());
}
#[test]
fn cancellation_revocation_and_crash_cannot_publish_partial_manifest() {
    let (dir, store) = store();
    let a = authority();
    assert!(store.enqueue_after_miss(&a));
    assert!(!store.enqueue_after_miss(&a));
    let mut b = store.next().unwrap();
    b.begin(&a).unwrap();
    b.row(&json!({"id":"cancel"})).unwrap();
    b.cancellation().store(true, Ordering::Release);
    assert!(b.publish(|| Ok(a.clone())).is_err());
    assert!(store.load(&a.identity).is_err());
    assert!(store.enqueue_after_miss(&a));
    let mut b = store.next().unwrap();
    b.begin(&a).unwrap();
    b.row(&json!({"id":"revoke"})).unwrap();
    let mut revoked = a.clone();
    revoked.active_member = false;
    let mut checks = 0;
    assert!(b
        .publish(|| {
            checks += 1;
            Ok(if checks == 1 {
                a.clone()
            } else {
                revoked.clone()
            })
        })
        .is_err());
    assert_eq!(
        checks, 2,
        "revoke must be observed after pieces were durably staged"
    );
    assert!(store.load(&a.identity).is_err());
    drop(store);
    let reopened = Store::open(dir.path(), Limits::default()).unwrap();
    assert!(reopened.load(&a.identity).is_err());
    assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    assert_eq!(fs::read_dir(dir.path().join("pieces")).unwrap().count(), 0);
}
#[test]
fn corrupt_later_piece_delivers_nothing_and_rebuild_repairs() {
    let (_dir, store) = store();
    let a = authority();
    let m = build(&store, &a);
    fs::write(
        store
            .root
            .join("pieces")
            .join(&m.pieces.last().unwrap().hash),
        b"corrupt",
    )
    .unwrap();
    assert!(store
        .read(
            &a.identity,
            || Ok(a.clone()),
            |_, _| panic!("partial delivery")
        )
        .is_err());
    build(&store, &a);
    store
        .read(&a.identity, || Ok(a.clone()), |_, _| Ok(()))
        .unwrap();
    store.cleanup(a.valid_until_ms).unwrap();
    assert_eq!(fs::read_dir(store.root.join("pieces")).unwrap().count(), 0);
}
#[test]
fn disk_queue_and_rebuild_thresholds_are_bounded() {
    let (_dir, store) = store();
    let a = authority();
    let m = build(&store, &a);
    assert!(!store.should_rebuild(&m, &a, 0, 0));
    assert!(store.should_rebuild(&m, &a, store.limits.rebuild_rows + 1, 0));
    assert!(store.should_rebuild(&m, &a, 0, store.limits.rebuild_bytes + 1));
    for n in 0..store.limits.queued {
        let mut x = a.clone();
        x.identity.module = n.to_string();
        assert!(store.enqueue_after_miss(&x));
    }
    assert!(!store.enqueue_after_miss(&a));
}

fn decode(p: &Piece, bytes: &[u8]) -> Vec<u8> {
    let raw = if p.compressed {
        let mut framed = bytes.to_vec();
        framed.extend_from_slice(&[0, 0, 255, 255]);
        let mut out = vec![0; p.raw_bytes as usize + 1];
        let mut decoder = flate2::Decompress::new(false);
        decoder
            .decompress(&framed, &mut out, flate2::FlushDecompress::Sync)
            .unwrap();
        assert_eq!(decoder.total_out(), p.raw_bytes);
        out.truncate(p.raw_bytes as usize);
        out
    } else {
        bytes.to_vec()
    };
    assert_eq!(hash(&raw), p.raw_hash);
    raw
}

/// Independent SQL oracle: overlapping positive grants minus a negative branch,
/// followed by a delta. Bootstrap + delta must equal a fresh SQL selection.
#[tokio::test]
async fn sql_oracle_bootstrap_and_catchup_preserve_exact_rows() {
    use sqlx::Connection;
    let mut db = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE tasks(id INTEGER PRIMARY KEY, team TEXT, blocked INTEGER); INSERT INTO tasks VALUES(1,'a',0),(2,'b',0),(3,'a',1),(4,'c',0); CREATE TABLE grants(team TEXT); INSERT INTO grants VALUES('a'),('a'),('b');").execute(&mut db).await.unwrap();
    let rows: Vec<String>=sqlx::query_scalar("SELECT json_object('id',id,'team',team) FROM tasks WHERE team IN (SELECT team FROM grants) AND NOT blocked ORDER BY id").fetch_all(&mut db).await.unwrap();
    let (_dir, store) = store();
    let a = authority();
    assert!(store.enqueue_after_miss(&a));
    let mut b = store.next().unwrap();
    b.begin(&a).unwrap();
    for r in rows {
        b.row(&serde_json::from_str::<serde_json::Value>(&r).unwrap())
            .unwrap();
    }
    b.publish(|| Ok(a.clone())).unwrap();
    let mut hydrated = std::collections::BTreeMap::new();
    store
        .read(
            &a.identity,
            || Ok(a.clone()),
            |p, bytes| {
                for r in decode(p, bytes)
                    .split(|b| *b == b'\n')
                    .filter(|r| !r.is_empty())
                {
                    let value: serde_json::Value = serde_json::from_slice(r)?;
                    hydrated.insert(value["id"].as_i64().unwrap(), value);
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(hydrated.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
    sqlx::query("UPDATE tasks SET team='c' WHERE id=2")
        .execute(&mut db)
        .await
        .unwrap();
    hydrated.remove(&2); // Authoritative delta deletes the row leaving the set.
    let expected: Vec<String>=sqlx::query_scalar("SELECT json_object('id',id,'team',team) FROM tasks WHERE team IN (SELECT team FROM grants) AND NOT blocked ORDER BY id").fetch_all(&mut db).await.unwrap();
    assert_eq!(
        hydrated.values().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
            .collect::<Vec<_>>()
    );
}

/// Repeatable module-only baseline + candidate, no network capacity inference.
/// Run explicitly under lane cgroup; emits qualified JSON, not an E2E result.
#[tokio::test]
#[ignore]
async fn benchmark_cold_sql_vs_starter_read_decompress_catchup() {
    use sqlx::Connection;
    let mut db = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE tasks(id INTEGER PRIMARY KEY, description TEXT, revision INTEGER); WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<9000) INSERT INTO tasks SELECT x,replace(hex(zeroblob(200)),'0','x'),1 FROM n;").execute(&mut db).await.unwrap();
    let (dir, small_store) = store();
    drop(small_store);
    let store = Store::open(dir.path(), Limits::default()).unwrap();
    let a = authority();
    let select = "SELECT json_object('id',id,'description',description) FROM tasks ORDER BY id";
    let mut baseline = Vec::new();
    let mut candidate = Vec::new();
    let mut baseline_cpu = Vec::new();
    let mut candidate_cpu = Vec::new();
    let mut baseline_allocs = Vec::new();
    let mut candidate_allocs = Vec::new();
    let rss_before = rss_bytes();
    // Baseline first, before starter construction.
    for _ in 0..7 {
        let begin = std::time::Instant::now();
        let cpu_begin = cpu_ns();
        ALLOCS.with(|c| c.set(Some((0, 0))));
        let rows: Vec<String> = sqlx::query_scalar(select).fetch_all(&mut db).await.unwrap();
        let encoded = serde_json::to_vec(
            &rows
                .iter()
                .map(|r| gonvex_protocol::JsonRow::from_json(r.clone()).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let _ = gonvex_websocket::bench_compress_text(&encoded, 12).unwrap();
        baseline.push(begin.elapsed().as_nanos());
        baseline_cpu.push(cpu_ns() - cpu_begin);
        baseline_allocs.push(ALLOCS.with(|c| c.replace(None).unwrap()));
    }
    let begin = std::time::Instant::now();
    assert!(store.enqueue_after_miss(&a));
    let mut build = store.next().unwrap();
    build.begin(&a).unwrap();
    {
        use futures_util::TryStreamExt;
        let mut stream = sqlx::query_scalar::<_, String>(select).fetch(&mut db);
        while let Some(row) = stream.try_next().await.unwrap() {
            build
                .row(&gonvex_protocol::JsonRow::from_json(row).unwrap())
                .unwrap();
        }
    }
    let m = build.publish(|| Ok(a.clone())).unwrap();
    let build_ns = begin.elapsed().as_nanos();
    sqlx::query("UPDATE tasks SET revision=2 WHERE id<=10")
        .execute(&mut db)
        .await
        .unwrap();
    for _ in 0..7 {
        let begin = std::time::Instant::now();
        let cpu_begin = cpu_ns();
        ALLOCS.with(|c| c.set(Some((0, 0))));
        let mut rows = 0;
        store
            .read(
                &a.identity,
                || Ok(a.clone()),
                |p, b| {
                    let raw = decode(p, b);
                    rows += raw.iter().filter(|c| **c == b'\n').count();
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(rows, 9000);
        let delta: Vec<String> = sqlx::query_scalar(
            "SELECT json_object('id',id,'description',description) FROM tasks WHERE revision>1",
        )
        .fetch_all(&mut db)
        .await
        .unwrap();
        assert_eq!(delta.len(), 10);
        candidate.push(begin.elapsed().as_nanos());
        candidate_cpu.push(cpu_ns() - cpu_begin);
        candidate_allocs.push(ALLOCS.with(|c| c.replace(None).unwrap()));
    }
    println!(
        "STARTER_BENCH {}",
        json!({"rows":9000,"description_bytes":400,"repetitions":7,"baseline_ns_per_op":baseline,"starter_read_decompress_10row_catchup_ns_per_op":candidate,"build_ns":build_ns,"raw_bytes":m.pieces.iter().map(|p|p.raw_bytes).sum::<u64>(),"compressed_bytes":m.pieces.iter().map(|p|p.bytes).sum::<u64>(),"disk_bytes":store.disk_bytes().unwrap(),"baseline_process_cpu_ns":baseline_cpu,"starter_process_cpu_ns":candidate_cpu,"baseline_alloc_count_requested_bytes":baseline_allocs,"starter_alloc_count_requested_bytes":candidate_allocs,"resident_bytes_before":rss_before,"resident_bytes_after":rss_bytes(),"qualifiers":"SQLite in-memory synthetic module; warm OS file cache; baseline builds validated raw JSON row array and compresses, candidate validates twice then decompresses pieces and queries 10 rows; caller-thread allocation requests only, excludes SQLite worker; CPU includes process workers; resident bytes are whole test process, not per operation; no client hydration/network; concurrent host noisy; not 1vCPU capacity"})
    );
}
