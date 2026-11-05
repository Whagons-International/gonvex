//! Opt-in OWN synthetic SQLite fixture index. No authority memo or write constraint.
//! Every admission still executes the exact original invalid-key predicate in its
//! current transaction. A verified SQLite partial index makes that query cheap.
use super::*;
use sqlx::Row;
use std::path::Path;
const FLAG: &str = "GONVEX_STARTER_KEY_INDEX_LOCAL";
const VERSION: &str = "gonvex-starter-invalid-key-v1";
pub(super) fn enabled() -> bool {
    std::env::var(FLAG).as_deref() == Ok("1")
}
pub(super) fn predicate(key: &str) -> Result<String, LocalSnapshotError> {
    let k = quote(key)?;
    Ok(format!(
        "typeof({k}) <> 'text' OR {k}='' OR instr({k},char(0))<>0"
    ))
}
#[derive(serde::Serialize)]
pub(super) struct Verified {
    version: &'static str,
    device: u64,
    inode: u64,
    table: String,
    key: String,
    pub name: String,
    ddl: String,
    root_page: i64,
}
fn expected(path: &Path, d: &ReplicaCollectionDefinition) -> Result<Verified, LocalSnapshotError> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(path).map_err(|e| gonvex_store::DatabaseError::Io(e.to_string()))?;
    let identity = serde_json::to_vec(&(
        VERSION,
        m.dev(),
        m.ino(),
        &d.table,
        &d.key,
        predicate(&d.key)?,
    ))
    .expect("private index identity");
    let name = format!(
        "_gonvex_starter_invalid_key_v1_{}",
        &hex_digest(&identity)[..32]
    );
    let ddl = format!(
        "CREATE INDEX {} ON {}({}) WHERE {}",
        quote(&name)?,
        quote(&d.table)?,
        quote(&d.key)?,
        predicate(&d.key)?
    );
    Ok(Verified {
        version: VERSION,
        device: m.dev(),
        inode: m.ino(),
        table: d.table.clone(),
        key: d.key.clone(),
        name,
        ddl,
        root_page: 0,
    })
}
/// Exact same-connection metadata, never a cached cookie/receipt/readiness flag.
/// Bound output: a rowid index has exactly key + auxiliary rowid (two rows).
/// Unsupported layouts/collisions never authorize an INDEXED BY query.
pub(super) async fn inspect(
    tx: &mut TenantTransaction,
    d: &ReplicaCollectionDefinition,
    path: &Path,
) -> Result<Option<Verified>, LocalSnapshotError> {
    if !enabled() {
        return Ok(None);
    }
    verify_physical(tx, path).await?;
    let mut proof = expected(path, d)?;
    let rows = sqlx::query("SELECT i.sql,i.tbl_name,i.rootpage,l.\"unique\" AS is_unique,l.origin,l.partial,x.seqno,x.cid,x.name,x.\"desc\" AS is_desc,x.coll,x.\"key\" AS is_key FROM main.sqlite_schema i JOIN pragma_index_list(?2,'main') l ON l.name=i.name CROSS JOIN pragma_index_xinfo(?1,'main') x WHERE i.type='index' AND i.name=?1 AND i.tbl_name=?2 ORDER BY x.seqno LIMIT 3")
        .bind(&proof.name).bind(&d.table).fetch_all(&mut **tx.transaction()).await?;
    if rows.len() != 2 {
        return Ok(None);
    }
    for (i, r) in rows.iter().enumerate() {
        if r.try_get::<String, _>("sql")? != proof.ddl
            || r.try_get::<String, _>("tbl_name")? != d.table
            || r.try_get::<i64, _>("rootpage")? <= 0
            || r.try_get::<i64, _>("is_unique")? != 0
            || r.try_get::<String, _>("origin")? != "c"
            || r.try_get::<i64, _>("partial")? != 1
            || r.try_get::<i64, _>("seqno")? != i as i64
            || r.try_get::<i64, _>("is_desc")? != 0
            || r.try_get::<String, _>("coll")? != "BINARY"
            || r.try_get::<i64, _>("is_key")? != if i == 0 { 1 } else { 0 }
            || (i == 0
                && (r.try_get::<Option<String>, _>("name")?.as_deref() != Some(d.key.as_str())
                    || r.try_get::<i64, _>("cid")? < 0))
            || (i == 1
                && (r.try_get::<Option<String>, _>("name")?.is_some()
                    || r.try_get::<i64, _>("cid")? != -1))
        {
            return Ok(None);
        }
    }
    proof.root_page = rows[0].try_get("rootpage")?;
    Ok(Some(proof))
}
/// Explicit writer registration only, before source backfill/seal; never called
/// by source_schema, offer, piece, commit, a normal open or provisioning.
pub(super) async fn install(
    tx: &mut TenantTransaction,
    d: &ReplicaCollectionDefinition,
) -> Result<(), LocalSnapshotError> {
    if !enabled() {
        return Ok(());
    }
    if tx.read_only() {
        return Err(LocalSnapshotError::Rejected(
            "key index requires explicit fixture writer",
        ));
    }
    let path = indexes::require_local(tx.transaction()).await?;
    verify_physical(tx, &path).await?;
    let p = expected(&path, d)?;
    let existing: Option<String> =
        sqlx::query_scalar("SELECT sql FROM main.sqlite_schema WHERE name=?1")
            .bind(&p.name)
            .fetch_optional(&mut **tx.transaction())
            .await?;
    match existing {
        None => {
            sqlx::query(&p.ddl).execute(&mut **tx.transaction()).await?;
        }
        Some(ddl) if ddl == p.ddl => {}
        Some(_) => {
            return Err(LocalSnapshotError::Rejected(
                "private invalid-key index name collision",
            ))
        }
    }
    if inspect(tx, d, &path).await?.is_none() {
        return Err(LocalSnapshotError::Rejected(
            "private invalid-key index layout unsupported",
        ));
    }
    Ok(())
}
pub(super) fn query(
    d: &ReplicaCollectionDefinition,
    index: Option<&Verified>,
) -> Result<String, LocalSnapshotError> {
    let hint = if let Some(p) = index {
        format!(" INDEXED BY {}", quote(&p.name)?)
    } else if enabled() {
        " NOT INDEXED".into()
    } else {
        String::new()
    };
    Ok(format!(
        "SELECT EXISTS(SELECT 1 FROM {}{hint} WHERE {})",
        quote(&d.table)?,
        predicate(&d.key)?
    ))
}
pub(super) fn identity(schema: String, index: Option<&Verified>) -> String {
    if let Some(p) = index {
        serde_json::to_string(&(VERSION, schema, p)).expect("private source identity")
    } else {
        schema
    }
}
pub(super) async fn capture_cookie(
    tx: &mut TenantTransaction,
    d: &ReplicaCollectionDefinition,
) -> Result<Option<i64>, LocalSnapshotError> {
    if !enabled() {
        return Ok(None);
    }
    let path = indexes::require_local(tx.transaction()).await?;
    if inspect(tx, d, &path).await?.is_some() {
        Ok(Some(schema_cookie(tx).await?))
    } else {
        Ok(None)
    }
}
// Direct PRAGMA forms cannot be shadowed by ordinary tables named
// pragma_database_list/pragma_schema_version. Both read THIS connection/TX.
async fn verify_physical(
    tx: &mut TenantTransaction,
    expected: &Path,
) -> Result<(), LocalSnapshotError> {
    let r = sqlx::query("PRAGMA main.database_list")
        .fetch_one(&mut **tx.transaction())
        .await?;
    let name: String = r.try_get("name")?;
    let seq: i64 = r.try_get("seq")?;
    let file: String = r.try_get("file")?;
    let actual =
        std::fs::canonicalize(file).map_err(|e| gonvex_store::DatabaseError::Io(e.to_string()))?;
    if seq != 0 || name != "main" || actual != expected {
        return Err(LocalSnapshotError::Rejected(
            "invalid-key index physical connection mismatch",
        ));
    }
    Ok(())
}
async fn schema_cookie(tx: &mut TenantTransaction) -> Result<i64, LocalSnapshotError> {
    Ok(sqlx::query_scalar("PRAGMA main.schema_version")
        .fetch_one(&mut **tx.transaction())
        .await?)
}
/// A lifetime guard, NOT a shape-validity token: the invalid predicate still
/// runs every time. Pin AFTER registration's last DDL, avoiding recursive scope
/// identities as installing native source watches itself changes schema_version.
pub(super) async fn verify_cookie(
    tx: &mut TenantTransaction,
    expected: Option<i64>,
) -> Result<(), LocalSnapshotError> {
    if let Some(expected) = expected {
        let actual = schema_cookie(tx).await?;
        if actual != expected {
            return Err(LocalSnapshotError::Rejected(
                "private invalid-key index registration DDL changed",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;
    pub(crate) struct Env(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Env {
        pub(crate) fn set() -> Self {
            let old = [FLAG, "GONVEX_PERMISSION_PRIMITIVES_LOCAL"]
                .into_iter()
                .map(|k| {
                    let v = std::env::var_os(k);
                    std::env::set_var(k, "1");
                    (k, v)
                })
                .collect();
            Self(old)
        }
    }
    impl Drop for Env {
        fn drop(&mut self) {
            for (k, v) in self.0.drain(..) {
                if let Some(v) = v {
                    std::env::set_var(k, v)
                } else {
                    std::env::remove_var(k)
                }
            }
        }
    }
    fn definition() -> ReplicaCollectionDefinition {
        serde_json::from_value(serde_json::json!({"table":"tasks","key":"id","columns":["id","body"],"orderBy":"id","orderDirection":"asc","mode":"eager","maxRows":100})).unwrap()
    }
    async fn setup() -> (
        tempfile::TempDir,
        gonvex_store::PoolRegistry,
        String,
        TenantTransaction,
    ) {
        std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
        let d = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
        let url = gonvex_store::database_url(&d.path().join("keys.db"));
        let pools = gonvex_store::PoolRegistry::new(Default::default());
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE tasks(id TEXT PRIMARY KEY,body TEXT,scope TEXT)")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        (d, pools, url, tx)
    }
    async fn compare(tx: &mut TenantTransaction, d: &ReplicaCollectionDefinition) -> i64 {
        let path = indexes::require_local(tx.transaction()).await.unwrap();
        let p = inspect(tx, d, &path).await.unwrap();
        let old = format!(
            "SELECT EXISTS(SELECT 1 FROM {} NOT INDEXED WHERE {})",
            quote(&d.table).unwrap(),
            predicate(&d.key).unwrap()
        );
        let expected: Result<i64, _> = sqlx::query_scalar(&old)
            .fetch_one(&mut **tx.transaction())
            .await;
        let actual: Result<i64, _> = sqlx::query_scalar(&query(d, p.as_ref()).unwrap())
            .fetch_one(&mut **tx.transaction())
            .await;
        match (expected, actual) {
            (Ok(a), Ok(b)) => {
                assert_eq!(a, b);
                a
            }
            (Err(a), Err(b)) => {
                assert_eq!(a.to_string(), b.to_string());
                -1
            }
            other => panic!("predicate error asymmetry {other:?}"),
        }
    }
    #[tokio::test]
    async fn invalid_key_index_exact_predicate_all_native_types_hidden_rows_and_rollback() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let _env = Env::set();
        let (_d, _pools, _url, mut tx) = setup().await;
        let d = definition();
        // Existing invalid rows are indexed, not rejected or repaired by DDL.
        sqlx::query("INSERT INTO tasks VALUES(NULL,'hidden','not-current-principal')")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        install(&mut tx, &d).await.unwrap();
        assert_eq!(compare(&mut tx, &d).await, 1);
        sqlx::query("DELETE FROM tasks")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert_eq!(compare(&mut tx, &d).await, 0);
        for expr in [
            "'good'",
            "NULL",
            "X'FF'",
            "''",
            "'a'||char(0)||'b'",
            "123",
            "1.5",
            "'é'",
            "'é'",
            "CAST(X'80FF' AS TEXT)",
            "CAST(X'8000FF' AS TEXT)",
        ] {
            sqlx::query(&format!(
                "INSERT INTO tasks VALUES({expr},'hidden','not-current-principal')"
            ))
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
            let expected = compare(&mut tx, &d).await;
            // Integer/real affinity becomes TEXT in both paths. Malformed TEXT
            // has precisely the original SQLite predicate's behavior.
            assert_eq!(
                expected,
                if [
                    "NULL",
                    "X'FF'",
                    "''",
                    "'a'||char(0)||'b'",
                    "CAST(X'8000FF' AS TEXT)"
                ]
                .contains(&expr)
                {
                    1
                } else {
                    0
                }
            );
            sqlx::query("UPDATE tasks SET id='xy' WHERE rowid=(SELECT max(rowid) FROM tasks)")
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
            assert_eq!(compare(&mut tx, &d).await, 0);
            sqlx::query("UPDATE tasks SET id=char(0)||'y',scope='retargeted' WHERE id='xy'")
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
            assert_eq!(compare(&mut tx, &d).await, 1);
            sqlx::query("UPDATE tasks SET id='xy' WHERE scope='retargeted'")
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
            assert_eq!(compare(&mut tx, &d).await, 0);
            sqlx::query("DELETE FROM tasks")
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
        }
        sqlx::raw_sql("SAVEPOINT invalid; INSERT INTO tasks VALUES(X'01','hidden','outside');")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert_eq!(compare(&mut tx, &d).await, 1);
        sqlx::raw_sql("ROLLBACK TO invalid; RELEASE invalid;")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert_eq!(compare(&mut tx, &d).await, 0);
        let path = indexes::require_local(tx.transaction()).await.unwrap();
        let proof = inspect(&mut tx, &d, &path).await.unwrap().unwrap();
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM tasks INDEXED BY {} WHERE {}",
            quote(&proof.name).unwrap(),
            predicate("id").unwrap()
        ))
        .fetch_one(&mut **tx.transaction())
        .await
        .unwrap();
        assert_eq!(count, 0);
        tx.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn invalid_key_index_missing_weaker_recreated_layout_and_schema_are_not_authority() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let _env = Env::set();
        let (_d, _pools, _url, mut tx) = setup().await;
        let d = definition();
        install(&mut tx, &d).await.unwrap();
        let path = indexes::require_local(tx.transaction()).await.unwrap();
        let p = inspect(&mut tx, &d, &path).await.unwrap().unwrap();
        let cookie_before = schema_cookie(&mut tx).await.unwrap();
        let identity_before = source_schema(&mut tx, &d).await.unwrap();
        sqlx::query(&format!("DROP INDEX {}", quote(&p.name).unwrap()))
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert!(inspect(&mut tx, &d, &path).await.unwrap().is_none());
        assert!(verify_cookie(&mut tx, Some(cookie_before)).await.is_err());
        assert_ne!(source_schema(&mut tx, &d).await.unwrap(), identity_before);
        sqlx::query(&format!(
            "CREATE INDEX {} ON tasks(id) WHERE id IS NULL",
            quote(&p.name).unwrap()
        ))
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
        assert!(inspect(&mut tx, &d, &path).await.unwrap().is_none());
        assert!(install(&mut tx, &d).await.is_err());
        sqlx::query("INSERT INTO tasks VALUES(X'01','hidden','outside')")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert_eq!(compare(&mut tx, &d).await, 1);
        assert!(source_schema(&mut tx, &d).await.is_err());
        sqlx::query("DELETE FROM tasks")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        sqlx::query(&format!("DROP INDEX {}", quote(&p.name).unwrap()))
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        sqlx::query(&p.ddl)
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert!(inspect(&mut tx, &d, &path).await.unwrap().is_some());
        assert!(
            verify_cookie(&mut tx, Some(cookie_before)).await.is_err(),
            "same-name, same-DDL recreation retained a stale registration"
        );
        let now = schema_cookie(&mut tx).await.unwrap();
        sqlx::raw_sql(
            "SAVEPOINT ddl; ALTER TABLE tasks ADD COLUMN extra TEXT; ROLLBACK TO ddl; RELEASE ddl;",
        )
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
        verify_cookie(&mut tx, Some(now)).await.unwrap();
        sqlx::query("ALTER TABLE tasks ADD COLUMN extra TEXT")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert!(verify_cookie(&mut tx, Some(now)).await.is_err());
        assert_ne!(source_schema(&mut tx, &d).await.unwrap(), identity_before);
        let mut bad = d.clone();
        bad.key = "body".into();
        assert!(source_schema(&mut tx, &bad).await.is_err());
        bad.table = "missing".into();
        assert_eq!(compare(&mut tx, &bad).await, -1);
        tx.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn invalid_key_index_native_wal_readers_rollback_copy_and_quoted_names() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let _env = Env::set();
        let (dir, pools, url, mut tx) = setup().await;
        let d = definition();
        install(&mut tx, &d).await.unwrap();
        tx.commit().await.unwrap();
        let mut reader = gonvex_store::begin_transaction(&pools, &url, true)
            .await
            .unwrap();
        assert_eq!(compare(&mut reader, &d).await, 0);
        let mut writer = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks VALUES(X'00','outside','hidden')")
            .execute(&mut **writer.transaction())
            .await
            .unwrap();
        assert_eq!(compare(&mut writer, &d).await, 1);
        writer.commit().await.unwrap();
        assert_eq!(
            compare(&mut reader, &d).await,
            0,
            "reader lost its consistent pre-write snapshot"
        );
        reader.commit().await.unwrap();
        let mut reader = gonvex_store::begin_transaction(&pools, &url, true)
            .await
            .unwrap();
        assert_eq!(compare(&mut reader, &d).await, 1);
        reader.commit().await.unwrap();
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        // A native SQLite backup has consistent index contents, but the private
        // descriptor's physical inode does not grant index use in the copy.
        sqlx::query("VACUUM main INTO ?1")
            .bind(dir.path().join("copy.db").to_str().unwrap())
            .execute(&mut **tx.transaction())
            .await
            .unwrap_err(); // prohibited in TX, never weaken the store boundary
        tx.rollback().await.unwrap();
        let copy = dir.path().join("copy.db");
        {
            let mut c = sqlx::SqliteConnection::connect(&url).await.unwrap();
            sqlx::query("VACUUM main INTO ?1")
                .bind(copy.to_str().unwrap())
                .execute(&mut c)
                .await
                .unwrap();
        }
        let copy_url = gonvex_store::database_url(&copy);
        let mut tx = gonvex_store::begin_transaction(&pools, &copy_url, false)
            .await
            .unwrap();
        let path = indexes::require_local(tx.transaction()).await.unwrap();
        assert!(inspect(&mut tx, &d, &path).await.unwrap().is_none());
        assert_eq!(compare(&mut tx, &d).await, 1);
        sqlx::query("CREATE TABLE \"select\"(\"from\" TEXT PRIMARY KEY,body TEXT)")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        let mut named = d.clone();
        named.table = "select".into();
        named.key = "from".into();
        named.columns = vec!["from".into(), "body".into()];
        named.order_by = "from".into();
        install(&mut tx, &named).await.unwrap();
        assert_eq!(compare(&mut tx, &named).await, 0);
        tx.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn invalid_key_index_native_metadata_cannot_be_shadowed_by_table_receipts() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let _env = Env::set();
        let (dir, _pools, _url, mut tx) = setup().await;
        let d = definition();
        install(&mut tx, &d).await.unwrap();
        let before = schema_cookie(&mut tx).await.unwrap();
        sqlx::query("CREATE TABLE pragma_schema_version(schema_version INTEGER)")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        sqlx::query("INSERT INTO pragma_schema_version VALUES(?1)")
            .bind(before)
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        assert!(
            verify_cookie(&mut tx, Some(before)).await.is_err(),
            "table receipt replaced native schema state"
        );
        sqlx::query("CREATE TABLE pragma_database_list(name TEXT,file TEXT)")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        sqlx::query("INSERT INTO pragma_database_list VALUES('main',?1)")
            .bind(dir.path().to_str().unwrap())
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        let fake = dir.path();
        assert!(
            verify_physical(&mut tx, fake).await.is_err(),
            "table receipt replaced native physical connection"
        );
        tx.rollback().await.unwrap();
    }
    fn cpu() -> u64 {
        let mut t = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut t) },
            0
        );
        t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
    }
    async fn stats(tx: &mut TenantTransaction, name: &str) -> serde_json::Value {
        let r=sqlx::query("SELECT count(*) AS pages,coalesce(sum(pgsize),0) AS bytes,coalesce(sum(ncell),0) AS cells FROM dbstat WHERE name=?1").bind(name).fetch_one(&mut **tx.transaction()).await.unwrap();
        serde_json::json!({"pages":r.get::<i64,_>("pages"),"bytes":r.get::<i64,_>("bytes"),"cells":r.get::<i64,_>("cells")})
    }
    #[tokio::test]
    #[ignore = "explicit optimized isolated synthetic kernel, not capacity"]
    async fn invalid_key_index_kernel_benchmark() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let _env = Env::set();
        let (_dir, _pools, _url, mut tx) = setup().await;
        let d = definition();
        sqlx::query("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<9002) INSERT INTO tasks SELECT printf('synthetic-%05d',x),replace(hex(zeroblob(200)),'0','d'),'t1' FROM n").execute(&mut **tx.transaction()).await.unwrap();
        let before_table = stats(&mut tx, "tasks").await;
        let start = std::time::Instant::now();
        let c = cpu();
        install(&mut tx, &d).await.unwrap();
        let build = serde_json::json!({"cpuNs":cpu()-c,"wallNs":start.elapsed().as_nanos()});
        let path = indexes::require_local(tx.transaction()).await.unwrap();
        let proof = inspect(&mut tx, &d, &path).await.unwrap().unwrap();
        let empty = stats(&mut tx, &proof.name).await;
        let old = format!(
            "SELECT EXISTS(SELECT 1 FROM tasks INDEXED BY sqlite_autoindex_tasks_1 WHERE {})",
            predicate("id").unwrap()
        );
        let new = query(&d, Some(&proof)).unwrap();
        let mut plans = Vec::new();
        for sql in [&old, &new] {
            plans.push(
                sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
                    .fetch_all(&mut **tx.transaction())
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| r.get::<String, _>(3))
                    .collect::<Vec<_>>(),
            );
        }
        let profile_before = std::env::var_os("STARTER_SCHEMA_PROFILE");
        std::env::set_var("STARTER_SCHEMA_PROFILE", "1");
        let mut pairs = Vec::new();
        for pair in 0..12 {
            for indexed in if pair % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                schema_profile::reset();
                let probe = schema_profile::Probe::install(&mut tx)
                    .await
                    .unwrap()
                    .unwrap();
                let start = std::time::Instant::now();
                let c = cpu();
                for _ in 0..20 {
                    let _leaf = probe.leaf(3);
                    let v: i64 = sqlx::query_scalar(if indexed { &new } else { &old })
                        .fetch_one(&mut **tx.transaction())
                        .await
                        .unwrap();
                    assert_eq!(v, 0);
                }
                let result = serde_json::json!({"pair":pair,"indexed":indexed,"cpuNs":cpu()-c,"wallNs":start.elapsed().as_nanos(),"profile":schema_profile::snapshot()});
                pairs.push(result);
                drop(probe);
            }
        }
        if let Some(v) = profile_before {
            std::env::set_var("STARTER_SCHEMA_PROFILE", v)
        } else {
            std::env::remove_var("STARTER_SCHEMA_PROFILE")
        };
        schema_profile::reset();
        let mut writes = Vec::new();
        for pair in 0..6 {
            for indexed in if pair % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                sqlx::query("SAVEPOINT write_cost")
                    .execute(&mut **tx.transaction())
                    .await
                    .unwrap();
                if !indexed {
                    sqlx::query(&format!("DROP INDEX {}", quote(&proof.name).unwrap()))
                        .execute(&mut **tx.transaction())
                        .await
                        .unwrap();
                }
                for (kind, sql) in [
                    ("unrelated", "UPDATE tasks SET body=body"),
                    ("valid_key_retarget", "UPDATE tasks SET id=id||'-new'"),
                    ("invalid_blob_key", "UPDATE tasks SET id=CAST(id AS BLOB)"),
                    ("repair_text_key", "UPDATE tasks SET id=CAST(id AS TEXT)"),
                ] {
                    let start = std::time::Instant::now();
                    let c = cpu();
                    let result = sqlx::query(sql)
                        .execute(&mut **tx.transaction())
                        .await
                        .unwrap();
                    let cpu_ns = cpu() - c;
                    let wall = start.elapsed().as_nanos();
                    let index_stats = if indexed {
                        Some(stats(&mut tx, &proof.name).await)
                    } else {
                        None
                    };
                    writes.push(serde_json::json!({"pair":pair,"indexed":indexed,"kind":kind,"rows":result.rows_affected(),"cpuNs":cpu_ns,"wallNs":wall,"index":index_stats,"table":stats(&mut tx,"tasks").await}));
                    assert_eq!(
                        compare(&mut tx, &d).await,
                        if kind == "invalid_blob_key" { 1 } else { 0 }
                    );
                }
                sqlx::raw_sql("ROLLBACK TO write_cost; RELEASE write_cost;")
                    .execute(&mut **tx.transaction())
                    .await
                    .unwrap();
            }
        }
        // One invalid BLOB sorts after every valid TEXT key in the old PK
        // control: measure the negative (reject) result, not just empty index.
        sqlx::query("INSERT INTO tasks VALUES(X'00','invalid','outside')")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        let invalid_size = stats(&mut tx, &proof.name).await;
        let old_env = std::env::var_os("STARTER_SCHEMA_PROFILE");
        std::env::set_var("STARTER_SCHEMA_PROFILE", "1");
        let mut invalid_pairs = Vec::new();
        for pair in 0..12 {
            for indexed in if pair % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                schema_profile::reset();
                let probe = schema_profile::Probe::install(&mut tx)
                    .await
                    .unwrap()
                    .unwrap();
                let start = std::time::Instant::now();
                let c = cpu();
                for _ in 0..20 {
                    let _leaf = probe.leaf(3);
                    let v: i64 = sqlx::query_scalar(if indexed { &new } else { &old })
                        .fetch_one(&mut **tx.transaction())
                        .await
                        .unwrap();
                    assert_eq!(v, 1);
                }
                invalid_pairs.push(serde_json::json!({"pair":pair,"indexed":indexed,"cpuNs":cpu()-c,"wallNs":start.elapsed().as_nanos(),"profile":schema_profile::snapshot()}));
                drop(probe);
            }
        }
        if let Some(v) = old_env {
            std::env::set_var("STARTER_SCHEMA_PROFILE", v)
        } else {
            std::env::remove_var("STARTER_SCHEMA_PROFILE")
        };
        schema_profile::reset();
        let output = serde_json::json!({"build":build,"emptyIndex":empty,"table":before_table,"plans":plans,"lookupPairs":pairs,"invalidLookupPairs":invalid_pairs,"invalidIndex":invalid_size,"invalidPhysicalRows":9004,"writes":writes,"qualification":"9003 rows/400-byte body; original full covering-PK predicate forced for matched paired control after adding native partial index; no expression rewrite. 20 semantic checks per pair, native trace counters actual. Bulk 9003-row writes alternate with/without native partial index under rollback savepoints, no permission/capture triggers in this isolated kernel. dbstat is allocated logical SQLite page bytes (includes uncommitted write pages), not fsync/WAL write amplification. Module benchmark separately includes all authority/normal-v2/SDK work."});
        std::fs::write(
            std::env::var("STARTER_KEY_INDEX_KERNEL").unwrap(),
            serde_json::to_vec_pretty(&output).unwrap(),
        )
        .unwrap();
        tx.rollback().await.unwrap();
    }
    #[tokio::test]
    #[ignore = "explicit committed native SQLite write/WAL cost; own synthetic files"]
    async fn invalid_key_index_committed_write_benchmark() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let _env = Env::set();
        let mut reports = Vec::new();
        for indexed in [false, true] {
            let (dir, pools, url, mut tx) = setup().await;
            let d = definition();
            sqlx::query("PRAGMA wal_autocheckpoint=0")
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
            sqlx::query("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<9002) INSERT INTO tasks SELECT printf('synthetic-%05d',x),replace(hex(zeroblob(200)),'0','d'),'t1' FROM n").execute(&mut **tx.transaction()).await.unwrap();
            if indexed {
                install(&mut tx, &d).await.unwrap();
            }
            tx.commit().await.unwrap();
            let mut c = sqlx::SqliteConnection::connect(&url).await.unwrap();
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(&mut c)
                .await
                .unwrap();
            drop(c);
            let db = dir.path().join("keys.db");
            let wal = dir.path().join("keys.db-wal");
            let initial_db = std::fs::metadata(&db).unwrap().len();
            for (kind, sql) in [
                ("unrelated", "UPDATE tasks SET body=body"),
                ("valid_key_retarget", "UPDATE tasks SET id=id||'-new'"),
                ("invalid_blob_key", "UPDATE tasks SET id=CAST(id AS BLOB)"),
                ("repair_text_key", "UPDATE tasks SET id=CAST(id AS TEXT)"),
            ] {
                let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
                    .await
                    .unwrap();
                sqlx::query("PRAGMA wal_autocheckpoint=0")
                    .execute(&mut **tx.transaction())
                    .await
                    .unwrap();
                let before = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
                let start = std::time::Instant::now();
                let c = cpu();
                let count = sqlx::query(sql)
                    .execute(&mut **tx.transaction())
                    .await
                    .unwrap()
                    .rows_affected();
                tx.commit().await.unwrap();
                let cpu_ns = cpu() - c;
                let wall_ns = start.elapsed().as_nanos();
                let after = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
                assert!(
                    after >= before,
                    "WAL checkpoint invalidated growth measurement"
                );
                let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
                    .await
                    .unwrap();
                let path = indexes::require_local(tx.transaction()).await.unwrap();
                let p = inspect(&mut tx, &d, &path).await.unwrap();
                let index_stats = if let Some(p) = p {
                    Some(stats(&mut tx, &p.name).await)
                } else {
                    None
                };
                assert_eq!(
                    compare(&mut tx, &d).await,
                    if kind == "invalid_blob_key" { 1 } else { 0 }
                );
                tx.commit().await.unwrap();
                reports.push(serde_json::json!({"indexed":indexed,"kind":kind,"rows":count,"cpuNsIncludingCommit":cpu_ns,"wallNsIncludingCommit":wall_ns,"initialDbBytes":initial_db,"walBytesAppended":after-before,"index":index_stats}));
            }
        }
        std::fs::write(std::env::var("STARTER_KEY_INDEX_WRITES").unwrap(),serde_json::to_vec_pretty(&serde_json::json!({"reports":reports,"qualification":"Own 9003-row synthetic databases; single native statement plus actual commit. Initial WAL truncated, per-writer autocheckpoint disabled only for these fixtures; file growth measures actual WAL bytes appended, not fsync latency/capacity. No permission/capture triggers; kernel repeated CPU pairs and full module reads measured separately. No rows/paths retained."})).unwrap()).unwrap();
    }
}
