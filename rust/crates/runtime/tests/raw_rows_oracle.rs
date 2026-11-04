//! Delivery v4: row hashes computed from SQLite's JSON text (`canonical_row`)
//! are byte-identical to the `Value` path (`stable_json` of the parsed row)
//! for every row of every table of the Whagons-shaped fixture tenants,
//! read with the snapshot encoding over all declared columns.
//!
//! `cargo test -p gonvex-runtime --features perf-fixture --test raw_rows_oracle`
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_row_hashes_equal_value_hashes_for_every_fixture_row() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let mut total = 0;
    for tenant in 0..fixture.tenants.len() {
        let (rows, fast, mismatched, first) = fixture.raw_hash_check(tenant).await.unwrap();
        eprintln!("tenant {tenant}: {rows} rows, {fast} on the fast path, {mismatched} mismatched");
        assert_eq!(mismatched, 0, "first mismatch: {first:?}");
        total += rows;
    }
    assert!(total > 1000);
}

/// `replicaOmitNulls`: every fixture row without its null columns, restored
/// with the collection's column list as the SDK does, is the same text and
/// hashes identically (raw and `Value` paths) to the full row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omitted_null_rows_restore_to_identical_text_and_hashes() {
    let cache = tempfile::tempdir().unwrap();
    let mut options = FixtureOptions::small();
    options.cache_dir = Some(cache.path().to_path_buf());
    let fixture = Fixture::build(options).await.unwrap();
    let (mut total, mut with_nulls) = (0, 0);
    for tenant in 0..fixture.tenants.len() {
        let (rows, nulls, mismatched, full, stripped, first) =
            fixture.omit_nulls_check(tenant).await.unwrap();
        eprintln!(
            "tenant {tenant}: {rows} rows, {nulls} with null columns, {full} -> {stripped} bytes, {mismatched} mismatched"
        );
        assert_eq!(mismatched, 0, "first mismatch: {first:?}");
        total += rows;
        with_nulls += nulls;
    }
    assert!(total > 1000 && with_nulls > 100);
}
