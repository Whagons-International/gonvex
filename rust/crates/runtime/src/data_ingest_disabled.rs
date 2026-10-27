use std::path::Path;

pub fn ingest(
    _bytes: &[u8],
    _filename: &str,
    _target: &Path,
) -> Result<Vec<serde_json::Value>, String> {
    Err("DuckDB analysis is unavailable in this build; enable the duckdb Cargo feature".to_owned())
}
