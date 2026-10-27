use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;

#[cfg(feature = "duckdb")]
#[test]
fn seccomp_allows_v8_execution_but_keeps_duckdb_external_access_closed() {
    let response = run_worker("await duckdb.register('rows', [{value: 2}, {value: 3}]); const total = await duckdb.query('select sum(value) as total from rows'); let externalDenied = false; try { await duckdb.query(\"select * from read_csv_auto('/etc/passwd')\"); } catch { externalDenied = true; } return {total: total.rows[0].total, externalDenied};", true);
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(
        response["result"],
        json!({"total": 5, "externalDenied": true})
    );
}

#[test]
fn plain_typescript_and_workspace_files_do_not_require_duckdb() {
    let response = run_worker("const value: number = 7; await files.writeText('answer.txt', String(value)); return { value: await files.readText('answer.txt'), duckdbType: typeof duckdb };", false);
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(
        response["result"],
        json!({"value": "7", "duckdbType": "undefined"})
    );
}

#[cfg(not(feature = "duckdb"))]
#[test]
fn unavailable_duckdb_returns_a_structured_error() {
    let response = run_worker("throw new Error('must not execute');", true);
    assert_eq!(response["ok"], false, "{response}");
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("unavailable in this build"));
}

fn run_worker(code: &str, duckdb: bool) -> serde_json::Value {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "gonvex-sandbox-process-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(root.join("files")).unwrap();
    fs::create_dir_all(root.join("imports")).unwrap();
    let request = json!({
        "version": 1,
        "root": root,
        "allowUnconfined": true,
        "code": code,
        "duckdb": duckdb,
        "imports": [],
        "maxHeapBytes": 67_108_864,
        "maxFileBytes": 1_048_576,
        "maxWorkspaceBytes": 16_777_216,
        "maxOutputBytes": 1_048_576,
        "maxRows": 100,
        "duckdbMemoryBytes": 67_108_864,
        "timeoutMs": 10_000,
        "workerUid": 65_534,
        "workerGid": 65_534
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_gonvex-sandbox-worker"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    fs::remove_dir_all(root).unwrap();
    response
}
