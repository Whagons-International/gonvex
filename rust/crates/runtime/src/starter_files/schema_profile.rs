//! Explicit test-only source-schema attribution. No rows, SQL or identities kept.
//! Static per-connection contexts survive async cancellation; CLOSE releases the
//! slot. No progress handler, query rewrite, extra SELECT or cached authority.
use super::*;
use std::ffi::{c_int, c_uint, c_void};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed},
    Mutex,
};
const N: usize = 8;
const NAMES: [&str; N] = [
    "physical_admission",
    "ddl",
    "columns",
    "key_shape",
    "rust_validation",
    "authority_serialization",
    "identity_key",
    "key_index_metadata",
];
static ENABLED: AtomicBool = AtomicBool::new(false);
struct Metrics {
    calls: AtomicU64,
    wall: AtomicU64,
    cpu: AtomicU64,
    queries: AtomicU64,
    internal: AtomicU64,
    internal_vm: AtomicU64,
    worker_cpu: AtomicU64,
    worker_wall: AtomicU64,
    rows: AtomicU64,
    vm: AtomicU64,
    visits: AtomicU64,
    sorts: AtomicU64,
}
impl Metrics {
    const fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
            wall: AtomicU64::new(0),
            cpu: AtomicU64::new(0),
            queries: AtomicU64::new(0),
            internal: AtomicU64::new(0),
            internal_vm: AtomicU64::new(0),
            worker_cpu: AtomicU64::new(0),
            worker_wall: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            vm: AtomicU64::new(0),
            visits: AtomicU64::new(0),
            sorts: AtomicU64::new(0),
        }
    }
}
static METRICS: [Metrics; N] = [const { Metrics::new() }; N];
static ERRORS: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy)]
struct Frame {
    ptr: usize,
    leaf: usize,
    cpu: u64,
    wall: u64,
    vm: i64,
    visits: i64,
    sorts: i64,
    rows: u64,
    root: bool,
}
const EMPTY: Frame = Frame {
    ptr: 0,
    leaf: 0,
    cpu: 0,
    wall: 0,
    vm: 0,
    visits: 0,
    sorts: 0,
    rows: 0,
    root: false,
};
struct Stack {
    frames: [Frame; 16],
}
struct Slot {
    db: AtomicUsize,
    leaf: AtomicUsize,
    stack: Mutex<Stack>,
}
impl Slot {
    const fn new() -> Self {
        Self {
            db: AtomicUsize::new(0),
            leaf: AtomicUsize::new(0),
            stack: Mutex::new(Stack {
                frames: [EMPTY; 16],
            }),
        }
    }
}
static SLOTS: [Slot; 64] = [const { Slot::new() }; 64];
unsafe extern "C" {
    fn sqlite3_trace_v2(
        db: *mut c_void,
        mask: c_uint,
        callback: Option<
            unsafe extern "C" fn(c_uint, *mut c_void, *mut c_void, *mut c_void) -> c_int,
        >,
        ctx: *mut c_void,
    ) -> c_int;
    fn sqlite3_stmt_status(stmt: *mut c_void, op: c_int, reset: c_int) -> c_int;
}
fn clock(kind: libc::clockid_t) -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(kind, &mut t) } != 0 {
        return 0;
    }
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}
unsafe extern "C" fn event(
    kind: c_uint,
    ctx: *mut c_void,
    stmt: *mut c_void,
    _value: *mut c_void,
) -> c_int {
    // ctx always points into static SLOTS, even when the initiating future drops.
    let slot = &*(ctx as *const Slot);
    if kind == 8 {
        slot.leaf.store(0, Relaxed);
        if let Ok(mut s) = slot.stack.lock() {
            s.frames.fill(EMPTY);
        }
        slot.db.store(0, Relaxed);
        return 0;
    }
    if kind == 1 && slot.leaf.load(Relaxed) == 0 {
        return 0;
    }
    let Ok(mut s) = slot.stack.lock() else {
        ERRORS.fetch_add(1, Relaxed);
        return 0;
    };
    if stmt.is_null() {
        ERRORS.fetch_add(1, Relaxed);
        return 0;
    }
    if kind == 1 {
        let leaf = slot.leaf.load(Relaxed);
        if leaf == 0 {
            return 0;
        }
        // Virtual-table PRAGMAs can finish their outer statement before the
        // internal statement profiles. Track active statements, not a LIFO stack.
        if s.frames.iter().any(|f| f.ptr == stmt as usize) {
            return 0;
        }
        let Some(i) = s.frames.iter().position(|f| f.ptr == 0) else {
            ERRORS.fetch_add(1, Relaxed);
            return 0;
        };
        let root = !s.frames.iter().any(|f| f.ptr != 0 && f.root);
        s.frames[i] = Frame {
            ptr: stmt as usize,
            root,
            leaf: leaf - 1,
            cpu: clock(libc::CLOCK_THREAD_CPUTIME_ID),
            wall: clock(libc::CLOCK_MONOTONIC),
            vm: sqlite3_stmt_status(stmt, 4, 0) as i64,
            visits: sqlite3_stmt_status(stmt, 1, 0) as i64,
            sorts: sqlite3_stmt_status(stmt, 2, 0) as i64,
            ..EMPTY
        };
    } else if kind == 4 {
        if let Some(i) = (0..s.frames.len())
            .rev()
            .find(|i| s.frames[*i].ptr == stmt as usize)
        {
            s.frames[i].rows += 1;
        }
    } else if kind == 2 {
        let Some(i) = (0..s.frames.len())
            .rev()
            .find(|i| s.frames[*i].ptr == stmt as usize)
        else {
            return 0;
        };
        let f = s.frames[i];
        s.frames[i] = EMPTY;
        let vm = (sqlite3_stmt_status(stmt, 4, 0) as i64 - f.vm).max(0) as u64;
        let visits = (sqlite3_stmt_status(stmt, 1, 0) as i64 - f.visits).max(0) as u64;
        let sorts = (sqlite3_stmt_status(stmt, 2, 0) as i64 - f.sorts).max(0) as u64;
        let m = &METRICS[f.leaf];
        if !f.root {
            m.internal.fetch_add(1, Relaxed);
            m.internal_vm.fetch_add(vm, Relaxed);
        } else {
            m.queries.fetch_add(1, Relaxed);
            m.worker_cpu.fetch_add(
                clock(libc::CLOCK_THREAD_CPUTIME_ID).saturating_sub(f.cpu),
                Relaxed,
            );
            m.worker_wall
                .fetch_add(clock(libc::CLOCK_MONOTONIC).saturating_sub(f.wall), Relaxed);
            m.rows.fetch_add(f.rows, Relaxed);
            m.vm.fetch_add(vm, Relaxed);
            m.visits.fetch_add(visits, Relaxed);
            m.sorts.fetch_add(sorts, Relaxed);
        }
    }
    0
}
pub(crate) struct Probe {
    slot: &'static Slot,
}
impl Probe {
    pub(crate) async fn install(
        tx: &mut TenantTransaction,
    ) -> Result<Option<Self>, LocalSnapshotError> {
        if !ENABLED.load(Relaxed) {
            return Ok(None);
        }
        let mut handle = tx.transaction().lock_handle().await?;
        let db = handle.as_raw_handle().as_ptr().cast::<c_void>();
        let key = db as usize;
        let slot = SLOTS
            .iter()
            .find(|s| s.db.load(Relaxed) == key)
            .or_else(|| {
                SLOTS
                    .iter()
                    .find(|s| s.db.compare_exchange(0, key, Relaxed, Relaxed).is_ok())
            })
            .ok_or(LocalSnapshotError::Rejected(
                "schema profile connection slots exhausted",
            ))?;
        let rc = unsafe {
            sqlite3_trace_v2(
                db,
                1 | 2 | 4 | 8,
                Some(event),
                slot as *const Slot as *mut c_void,
            )
        };
        if rc != 0 {
            return Err(LocalSnapshotError::Rejected(
                "schema trace installation failed",
            ));
        }
        Ok(Some(Self { slot }))
    }
    pub(crate) fn leaf(&self, index: usize) -> Leaf {
        self.slot.leaf.store(index + 1, Relaxed);
        Leaf::new(index, Some(self.slot))
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.slot.leaf.store(0, Relaxed);
    }
}
pub(crate) struct Leaf {
    index: usize,
    slot: Option<&'static Slot>,
    wall: std::time::Instant,
    cpu: u64,
    active: bool,
}
impl Leaf {
    fn new(index: usize, slot: Option<&'static Slot>) -> Self {
        Self {
            index,
            slot,
            wall: std::time::Instant::now(),
            cpu: clock(libc::CLOCK_PROCESS_CPUTIME_ID),
            active: true,
        }
    }
}
impl Drop for Leaf {
    fn drop(&mut self) {
        if self.active {
            if let Some(s) = self.slot {
                s.leaf.store(0, Relaxed);
            }
            let m = &METRICS[self.index];
            m.calls.fetch_add(1, Relaxed);
            m.wall
                .fetch_add(self.wall.elapsed().as_nanos() as u64, Relaxed);
            m.cpu.fetch_add(
                clock(libc::CLOCK_PROCESS_CPUTIME_ID).saturating_sub(self.cpu),
                Relaxed,
            );
        }
    }
}
pub(crate) fn pure(index: usize) -> Option<Leaf> {
    ENABLED.load(Relaxed).then(|| Leaf::new(index, None))
}
pub(crate) fn reset() {
    ENABLED.store(false, Relaxed);
    ERRORS.store(0, Relaxed);
    for m in &METRICS {
        for x in [
            &m.calls,
            &m.wall,
            &m.cpu,
            &m.queries,
            &m.internal,
            &m.internal_vm,
            &m.worker_cpu,
            &m.worker_wall,
            &m.rows,
            &m.vm,
            &m.visits,
            &m.sorts,
        ] {
            x.store(0, Relaxed);
        }
    }
    ENABLED.store(
        std::env::var_os("STARTER_SCHEMA_PROFILE").is_some(),
        Relaxed,
    );
}
pub(crate) fn snapshot() -> serde_json::Value {
    serde_json::json!({"errors":ERRORS.load(Relaxed),"qualification":"Test-only SQLite trace on exact connection. Worker thread CPU/monotonic spans begin at STMT (prepare excluded); fullscan_steps is SQLite stmt_status, not inferred row count. Leaf process CPU includes all Rust threads; residual bounds caller/prepare/FS/handoff work, not exclusive IPC. Source SQLx successful leaf submits one Execute command per top-level query per inspected sqlx0.8.6 source. Static contexts survive cancellation and close safely; no progress-handler replacement.","leaves":METRICS.iter().enumerate().map(|(i,m)|serde_json::json!({"leaf":NAMES[i],"calls":m.calls.load(Relaxed),"wallNs":m.wall.load(Relaxed),"processCpuNs":m.cpu.load(Relaxed),"topLevelQueries":m.queries.load(Relaxed),"internalStatements":m.internal.load(Relaxed),"internalVmSteps":m.internal_vm.load(Relaxed),"workerCpuNs":m.worker_cpu.load(Relaxed),"workerWallNs":m.worker_wall.load(Relaxed),"outputRows":m.rows.load(Relaxed),"vmSteps":m.vm.load(Relaxed),"fullscanSteps":m.visits.load(Relaxed),"sorts":m.sorts.load(Relaxed)})).collect::<Vec<_>>()})
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Env(Option<std::ffi::OsString>);
    impl Drop for Env {
        fn drop(&mut self) {
            if let Some(v) = self.0.take() {
                std::env::set_var("STARTER_SCHEMA_PROFILE", v)
            } else {
                std::env::remove_var("STARTER_SCHEMA_PROFILE")
            }
            reset();
        }
    }
    #[tokio::test]
    async fn source_schema_profile_tracks_nested_pragma_and_survives_dropped_scope() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let env = Env(std::env::var_os("STARTER_SCHEMA_PROFILE"));
        std::env::set_var("STARTER_SCHEMA_PROFILE", "1");
        reset();
        std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
        let d = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
        let url = gonvex_store::database_url(&d.path().join("trace.db"));
        let pools = gonvex_store::PoolRegistry::new(Default::default());
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t(id TEXT PRIMARY KEY,body TEXT)")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        let p = Probe::install(&mut tx).await.unwrap().unwrap();
        {
            let _leaf = p.leaf(2);
            let rows = gonvex_store::rows::table_columns(tx.transaction(), "t")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        }
        {
            let _leaf = p.leaf(3);
            let value: i64 = sqlx::query_scalar("SELECT count(*) FROM t")
                .fetch_one(&mut **tx.transaction())
                .await
                .unwrap();
            assert_eq!(value, 0);
        }
        let before = snapshot();
        assert_eq!(before["errors"], 0);
        assert_eq!(before["leaves"][2]["topLevelQueries"], 1);
        assert_eq!(before["leaves"][2]["internalStatements"], 1);
        drop(p);
        let value: i64 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&mut **tx.transaction())
            .await
            .unwrap();
        assert_eq!(value, 1);
        assert_eq!(
            snapshot()["leaves"][3]["topLevelQueries"],
            before["leaves"][3]["topLevelQueries"],
            "dropped guard continued tracing unrelated work"
        );
        tx.rollback().await.unwrap();
        drop(pools);
        drop(env);
    }
    #[tokio::test]
    async fn source_schema_profile_keeps_original_ddl_key_and_error_ordering() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let old = std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL");
        std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
        let env = Env(std::env::var_os("STARTER_SCHEMA_PROFILE"));
        std::env::set_var("STARTER_SCHEMA_PROFILE", "1");
        reset();
        std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
        let d = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
        let url = gonvex_store::database_url(&d.path().join("metadata.db"));
        let pools = gonvex_store::PoolRegistry::new(Default::default());
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TABLE t(id TEXT PRIMARY KEY,body TEXT); INSERT INTO t VALUES('ok','value'); CREATE VIEW v AS SELECT * FROM t; CREATE TABLE c(id TEXT PRIMARY KEY COLLATE NOCASE,body TEXT)").execute(&mut **tx.transaction()).await.unwrap();
        let definition:ReplicaCollectionDefinition=serde_json::from_value(serde_json::json!({"table":"t","key":"id","columns":["id","body"],"orderBy":"id","orderDirection":"asc","mode":"eager","maxRows":10})).unwrap();
        let expected: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name='t'")
                .fetch_one(&mut **tx.transaction())
                .await
                .unwrap();
        assert_eq!(source_schema(&mut tx, &definition).await.unwrap(), expected);
        for invalid in ["NULL", "X'01'", "''", "char(0)"] {
            sqlx::query(&format!("UPDATE t SET id={invalid}"))
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
            assert!(source_schema(&mut tx, &definition).await.is_err());
            sqlx::query("UPDATE t SET id='ok'")
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
        }
        for table in ["missing", "v", "c"] {
            let mut bad = definition.clone();
            bad.table = table.into();
            reset();
            assert!(source_schema(&mut tx, &bad).await.is_err());
            assert_eq!(
                snapshot()["leaves"][3]["topLevelQueries"],
                0,
                "key scan ran before invalid DDL was rejected"
            );
        }
        sqlx::query("ALTER TABLE t ADD COLUMN extra TEXT")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        let changed = source_schema(&mut tx, &definition).await.unwrap();
        assert_ne!(changed, expected);
        let fresh: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name='t'")
                .fetch_one(&mut **tx.transaction())
                .await
                .unwrap();
        assert_eq!(changed, fresh);
        assert_eq!(snapshot()["errors"], 0);
        tx.rollback().await.unwrap();
        if let Some(v) = old {
            std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", v)
        } else {
            std::env::remove_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL")
        }
        drop(env);
    }
}
