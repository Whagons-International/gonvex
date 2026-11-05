//! Fixed, test-only counters. No row data, SQL text or identity is retained.
#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum Site {
    AuthoritySchema,
    AuthoritySources,
    AuthorityGuards,
    AuthorityPrepare,
    AuthorityStamp,
    AdmissionModule,
    AdmissionNative,
    OfferSql,
    OfferRead,
    OfferFormat,
    PieceRead,
    CommitResolve,
    CommitSql,
    CommitDurability,
    IdentitySerialize,
    RootShapeScans,
    SqlRows,
    FormatRows,
    EncodedReadBytes,
    IdentityBytes,
    OfferVmSteps,
}
const N: usize = Site::OfferVmSteps as usize + 1;
const NAMES: [&str; N] = [
    "authority.schema",
    "authority.sources",
    "authority.guards",
    "authority.prepare",
    "authority.stamp",
    "admission.module",
    "admission.native",
    "offer.sql",
    "offer.read",
    "offer.format",
    "piece.read",
    "commit.resolve",
    "commit.sql",
    "commit.durability",
    "identity.serialize",
    "root_shape.scans",
    "sql.rows",
    "format.rows",
    "encoded.read_bytes",
    "identity.serialized_bytes",
    "offer.vm_steps",
];
#[cfg(all(test, feature = "perf-fixture"))]
mod implementation {
    use super::*;
    use std::{
        sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        time::Instant,
    };
    static ACTIVE: AtomicBool = AtomicBool::new(false);
    static COUNT: [AtomicU64; N] = [const { AtomicU64::new(0) }; N];
    static NS: [AtomicU64; N] = [const { AtomicU64::new(0) }; N];
    pub(crate) struct Scope {
        site: Site,
        start: Option<Instant>,
    }
    impl Scope {
        pub(crate) fn new(site: Site) -> Self {
            Self {
                site,
                start: ACTIVE.load(Relaxed).then(Instant::now),
            }
        }
    }
    impl Drop for Scope {
        fn drop(&mut self) {
            if let Some(t) = self.start {
                COUNT[self.site as usize].fetch_add(1, Relaxed);
                NS[self.site as usize].fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
            }
        }
    }
    pub(crate) fn add(site: Site, n: u64) {
        if ACTIVE.load(Relaxed) {
            COUNT[site as usize].fetch_add(n, Relaxed);
        }
    }
    pub(crate) fn reset(active: bool) {
        ACTIVE.store(false, Relaxed);
        for i in 0..N {
            COUNT[i].store(0, Relaxed);
            NS[i].store(0, Relaxed);
        }
        ACTIVE.store(active, Relaxed);
    }
    pub(crate) fn snapshot() -> Vec<serde_json::Value> {
        (0..N)
            .filter_map(|i| {
                let count = COUNT[i].load(Relaxed);
                (count > 0).then(
                    || serde_json::json!({"site":NAMES[i],"count":count,"ns":NS[i].load(Relaxed)}),
                )
            })
            .collect()
    }
}
#[cfg(all(test, feature = "perf-fixture"))]
pub(crate) use implementation::*;
#[cfg(not(all(test, feature = "perf-fixture")))]
pub(crate) struct Scope;
#[cfg(not(all(test, feature = "perf-fixture")))]
impl Scope {
    #[inline(always)]
    pub(crate) fn new(_: Site) -> Self {
        Self
    }
}
#[cfg(not(all(test, feature = "perf-fixture")))]
#[inline(always)]
pub(crate) fn add(_: Site, _: u64) {}
