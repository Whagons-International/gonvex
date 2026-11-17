//! Per-call-site counters for the snapshot / cache / change-log row paths
//! (perf/snapshot-raw): calls, rows handled, allocations made on the calling
//! thread and time inside each site. Only with the `perf-fixture` feature
//! (the counting allocator lives there); otherwise every scope is a no-op.
//!
//! Scopes nest: a site's numbers include the sites it calls.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Site {
    /// `collect_window`: one SQL-scanned row on the raw path.
    ScanRowRaw,
    /// `collect_window`: one SQL-scanned row parsed into a `Value`.
    ScanRowParsed,
    /// Cache fill (`build_entry`): one row kept as raw text.
    CacheRowRaw,
    /// Cache fill: one row parsed into a `Value` (projection, eval text).
    CacheRowParsed,
    /// A cached row built from a change-log image (`CachedRow::from_image`).
    LogImageRow,
    /// `resume_changes`: one change-log row decoded.
    LogRowDecode,
    /// `collapse_changes` (calls; rows = changes).
    Collapse,
    /// `snapshot_cache::view` (whole call; rows = rows returned).
    CacheView,
    /// `CacheEntry::with_acl` decoding rows for a class scope (rows = decoded).
    AclDecode,
    /// Aggregate resume: projecting and hashing log images.
    ResumeImage,
    /// Reconcile resume: a sent row parsed into a `Value`.
    ReconcileRow,
    /// Interpreted (version 1) visibility on a cached row: parsed per row.
    LegacyEvalRow,
    /// Feed: `apply_rows_with` per subscription (full path).
    FeedApplyRows,
}

pub const SITES: [(&str, Site); 13] = [
    ("scan_row_raw", Site::ScanRowRaw),
    ("scan_row_parsed", Site::ScanRowParsed),
    ("cache_row_raw", Site::CacheRowRaw),
    ("cache_row_parsed", Site::CacheRowParsed),
    ("log_image_row", Site::LogImageRow),
    ("log_row_decode", Site::LogRowDecode),
    ("collapse", Site::Collapse),
    ("cache_view", Site::CacheView),
    ("acl_decode", Site::AclDecode),
    ("resume_image", Site::ResumeImage),
    ("reconcile_row", Site::ReconcileRow),
    ("legacy_eval_row", Site::LegacyEvalRow),
    ("feed_apply_rows", Site::FeedApplyRows),
];

#[cfg(feature = "perf-fixture")]
mod imp {
    use super::{Site, SITES};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    const N: usize = SITES.len();
    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: AtomicU64 = AtomicU64::new(0);
    static CALLS: [AtomicU64; N] = [ZERO; N];
    static ROWS: [AtomicU64; N] = [ZERO; N];
    static ALLOCS: [AtomicU64; N] = [ZERO; N];
    static NANOS: [AtomicU64; N] = [ZERO; N];

    pub struct Scope {
        site: Site,
        allocations: u64,
        started: std::time::Instant,
    }

    impl Scope {
        #[inline]
        pub fn new(site: Site) -> Self {
            Self {
                site,
                allocations: crate::perf_fixture::harness::thread_allocations(),
                started: std::time::Instant::now(),
            }
        }

        /// Re-attributes the scope (decided after it started).
        #[inline]
        pub fn set(&mut self, site: Site) {
            self.site = site;
        }
    }

    impl Drop for Scope {
        fn drop(&mut self) {
            let index = self.site as usize;
            CALLS[index].fetch_add(1, Relaxed);
            NANOS[index].fetch_add(self.started.elapsed().as_nanos() as u64, Relaxed);
            ALLOCS[index].fetch_add(
                crate::perf_fixture::harness::thread_allocations() - self.allocations,
                Relaxed,
            );
        }
    }

    #[inline]
    pub fn rows(site: Site, rows: usize) {
        ROWS[site as usize].fetch_add(rows as u64, Relaxed);
    }

    /// (name, calls, rows, allocations, nanoseconds) of every site.
    pub fn snapshot() -> Vec<(&'static str, u64, u64, u64, u64)> {
        SITES
            .iter()
            .map(|(name, site)| {
                let index = *site as usize;
                (
                    *name,
                    CALLS[index].load(Relaxed),
                    ROWS[index].load(Relaxed),
                    ALLOCS[index].load(Relaxed),
                    NANOS[index].load(Relaxed),
                )
            })
            .collect()
    }

    pub fn reset() {
        for counters in [&CALLS, &ROWS, &ALLOCS, &NANOS] {
            for counter in counters.iter() {
                counter.store(0, Relaxed);
            }
        }
    }
}

#[cfg(not(feature = "perf-fixture"))]
mod imp {
    use super::Site;

    pub struct Scope;

    impl Scope {
        #[inline(always)]
        pub fn new(_: Site) -> Self {
            Self
        }

        #[inline(always)]
        pub fn set(&mut self, _: Site) {}
    }

    #[inline(always)]
    pub fn rows(_: Site, _: usize) {}
}

pub use imp::*;

/// `let _site = raw_site!(Name);` counts the enclosing block as one call.
macro_rules! raw_site {
    ($site:ident) => {
        crate::raw_sites::Scope::new(crate::raw_sites::Site::$site)
    };
}
pub(crate) use raw_site;
