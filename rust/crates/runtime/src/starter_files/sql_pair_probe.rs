//! Test-only execution comparison. Never authorizes a manifest or emits rows.
use super::*;
use sha2::{Digest, Sha256};
fn cpu_ns() -> u128 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut t) },
        0
    );
    t.tv_sec as u128 * 1_000_000_000 + t.tv_nsec as u128
}
pub(super) async fn compare(
    tx: &mut TenantTransaction,
    prepared: &query::Prepared,
    sql: &str,
    values: &[crate::sql_parameters::Parameter],
    path: &str,
) -> Result<(), LocalSnapshotError> {
    const JOIN:&str="FROM _gonvex_permission_row_atoms ra JOIN _gonvex_permission_set_atoms pa ON pa.scope_id=ra.scope_id AND pa.atom_id=ra.atom_id AND pa.generation=ra.generation WHERE ra.row_key IN (SELECT row_key FROM universe) AND ra.scope_id=?1 AND ra.table_id=?2 AND ra.generation=?3 AND pa.set_id=?4 AND ra.slot=?5";
    const SEMI:&str="FROM _gonvex_permission_row_atoms ra WHERE ra.row_key IN (SELECT row_key FROM universe) AND ra.scope_id=?1 AND ra.table_id=?2 AND ra.generation=?3 AND ra.slot=?5 AND ra.atom_id IN (SELECT pa.atom_id FROM _gonvex_permission_set_atoms pa WHERE pa.scope_id=?1 AND pa.generation=?3 AND pa.set_id=?4)";
    assert!(
        sql.contains(JOIN) || sql.contains(SEMI),
        "paired probe supports this exact single-slot fixture only"
    );
    let old = sql.replace(SEMI, JOIN);
    let new = sql.replace(JOIN, SEMI);
    assert_ne!(old, new);
    let mut results = Vec::new();
    let mut expected = None;
    for repeat in 0..12 {
        let variants = if repeat % 2 == 0 {
            [("join", &old), ("semi", &new)]
        } else {
            [("semi", &new), ("join", &old)]
        };
        for (variant, statement) in variants {
            let steps = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let count = steps.clone();
            tx.transaction()
                .lock_handle()
                .await?
                .set_progress_handler(1000, move || {
                    count.fetch_add(1000, std::sync::atomic::Ordering::Relaxed);
                    true
                });
            let mut q = query::bind(prepared, statement);
            for v in values.iter().cloned() {
                q = v.bind(q);
            }
            let start = cpu_ns();
            let wall = std::time::Instant::now();
            let mut stream = q.fetch(&mut **tx.transaction());
            let mut hash = Sha256::new();
            let mut rows = 0u64;
            let mut bytes = 0u64;
            let outcome = async {
                while let Some(row) = stream.try_next().await? {
                    let text: String = row.try_get(0)?;
                    rows += 1;
                    bytes += text.len() as u64;
                    hash.update((text.len() as u64).to_be_bytes());
                    hash.update(text.as_bytes());
                }
                Ok::<_, sqlx::Error>(())
            }
            .await;
            let wall = wall.elapsed().as_nanos();
            let cpu = cpu_ns() - start;
            drop(stream);
            tx.transaction()
                .lock_handle()
                .await?
                .remove_progress_handler();
            outcome?;
            let digest = format!("{:x}", hash.finalize());
            let identity = (rows, bytes, digest.clone());
            if let Some(ref e) = expected {
                assert_eq!(
                    &identity, e,
                    "query execution changed ordered projected bytes"
                );
            } else {
                expected = Some(identity);
            }
            results.push(serde_json::json!({"repeat":repeat,"variant":variant,"cpuNs":cpu,"wallNs":wall,"outputRows":rows,"outputBytes":bytes,"orderedOutputSha256":digest,"vmStepsSampled":steps.load(std::sync::atomic::Ordering::Relaxed)}));
        }
    }
    std::fs::write(path,serde_json::to_vec_pretty(&serde_json::json!({"results":results,"vmResolutionInstructions":1000,"qualification":"Separate SQLx actual-query experiment in SAME TX/scope/gen/principal, typed binds and full projection/order/limit; alternating order, streaming hash/no table copy. Includes SQLx rows+hash; excludes primary/production timing; no authority memo."})).unwrap()).unwrap();
    Ok(())
}
