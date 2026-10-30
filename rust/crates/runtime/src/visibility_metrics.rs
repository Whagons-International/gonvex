//! Statement and work budgets owned by one Runtime, never process-global.
//!
//! Read snapshots after the measured operations finish. Individual counters
//! are atomic; a snapshot during concurrent work is not a transaction boundary.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct VisibilityMetrics {
    visibility_sql_statements: AtomicU64,
    collection_snapshots: AtomicU64,
    live_query_reruns: AtomicU64,
    principal_rebuilds: AtomicU64,
    in_memory_row_checks: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VisibilityMetricsSnapshot {
    pub visibility_sql_statements: u64,
    pub collection_snapshots: u64,
    pub live_query_reruns: u64,
    pub principal_rebuilds: u64,
    pub in_memory_row_checks: u64,
}

impl VisibilityMetrics {
    pub fn record_visibility_sql_statement(&self) {
        self.visibility_sql_statements
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_collection_snapshot(&self) {
        self.collection_snapshots.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_live_query_rerun(&self) {
        self.live_query_reruns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_principal_rebuild(&self) {
        self.principal_rebuilds.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_in_memory_row_check(&self) {
        self.in_memory_row_checks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> VisibilityMetricsSnapshot {
        VisibilityMetricsSnapshot {
            visibility_sql_statements: self.visibility_sql_statements.load(Ordering::Relaxed),
            collection_snapshots: self.collection_snapshots.load(Ordering::Relaxed),
            live_query_reruns: self.live_query_reruns.load(Ordering::Relaxed),
            principal_rebuilds: self.principal_rebuilds.load(Ordering::Relaxed),
            in_memory_row_checks: self.in_memory_row_checks.load(Ordering::Relaxed),
        }
    }

    /// Test setup only. Call with no in-flight operations on this runtime.
    /// Keep the same instance so every owner continues recording into it.
    pub fn reset_for_test(&self) {
        self.visibility_sql_statements.store(0, Ordering::Relaxed);
        self.collection_snapshots.store(0, Ordering::Relaxed);
        self.live_query_reruns.store(0, Ordering::Relaxed);
        self.principal_rebuilds.store(0, Ordering::Relaxed);
        self.in_memory_row_checks.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_runtime_local_and_reset_preserves_the_instance() {
        let first = VisibilityMetrics::default();
        let second = VisibilityMetrics::default();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let metrics = &first;
                scope.spawn(move || {
                    for _ in 0..100 {
                        metrics.record_visibility_sql_statement();
                        metrics.record_collection_snapshot();
                        metrics.record_live_query_rerun();
                        metrics.record_principal_rebuild();
                        metrics.record_in_memory_row_check();
                    }
                });
            }
        });
        assert_eq!(
            first.snapshot(),
            VisibilityMetricsSnapshot {
                visibility_sql_statements: 400,
                collection_snapshots: 400,
                live_query_reruns: 400,
                principal_rebuilds: 400,
                in_memory_row_checks: 400,
            }
        );
        assert_eq!(second.snapshot(), VisibilityMetricsSnapshot::default());
        first.reset_for_test();
        assert_eq!(first.snapshot(), VisibilityMetricsSnapshot::default());
        first.record_visibility_sql_statement();
        assert_eq!(first.snapshot().visibility_sql_statements, 1);
    }
}

