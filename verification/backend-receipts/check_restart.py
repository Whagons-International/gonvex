#!/usr/bin/env python3
"""Crash the process that owns the SQLite tenant file across a witnessed receipt boundary.

SQLite runs inside the runtime process, so the crash boundary is the runtime
process itself: the native fixture witnesses a committed or still-open
transaction, this driver SIGKILLs it, then a fresh fixture process reopens the
same owned data directory (WAL recovery) and replays the receipt.

Fixture contract (rust/crates/runtime/src/tests/receipt_guarantee_tests.rs):
  GONVEX_TEST_DATA_DIR             owned data directory that survives the kill
  GONVEX_RECEIPT_CRASH_MODE        committed | uncommitted
  GONVEX_RECEIPT_CRASH_PHASE       crash | recover
  GONVEX_RECEIPT_CRASH_COORDINATION path prefix; crash phase writes
                                   <prefix>.ready = {"mode", "database"} and
                                   then blocks until killed
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
TEST = "tests::receipt_guarantee_tests::receipt_recovery_after_process_crash"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=30, **kwargs).strip()


def run_case(binary, host, output, mode, damage=False):
    label = mode + ("-missing-effect-control" if damage else "")
    coordination = output / label
    data_dir = Path(tempfile.mkdtemp(prefix="gonvex-receipt-crash-"))
    process = None
    base = dict(os.environ, GONVEX_RECEIPT_TEST_REQUIRE_NATIVE="1", GONVEX_TEST_MODULE_HOST_BINARY=str(host),
                GONVEX_TEST_DATA_DIR=str(data_dir), GONVEX_SQLITE_SYNCHRONOUS="full",
                GONVEX_RECEIPT_CRASH_MODE=mode, GONVEX_RECEIPT_CRASH_COORDINATION=str(coordination))
    try:
        with (output / (label + "-crash.log")).open("w") as stream:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=dict(base, GONVEX_RECEIPT_CRASH_PHASE="crash"),
                                       stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 60
            ready = Path(str(coordination) + ".ready")
            while not ready.is_file():
                require(process.poll() is None, "native fixture exited before witnessing the commit boundary")
                require(time.monotonic() < deadline, "native fixture did not witness the commit boundary")
                time.sleep(.05)
            witness = json.loads(ready.read_text())
            database = Path(witness["database"]).resolve()
            require(witness["mode"] == mode and database.is_relative_to(data_dir.resolve()) and database.is_file(),
                    "invalid native boundary witness")
            os.killpg(process.pid, signal.SIGKILL)
            code = process.wait(timeout=10)
            require(code == -signal.SIGKILL, "fixture was not killed at the boundary")
            process = None
        if damage:
            require(mode == "committed", "the evidence control requires a committed receipt")
            with sqlite3.connect(database) as connection:
                connection.execute("DELETE FROM receipt_records")
        log = output / (label + ".log")
        with log.open("w") as stream:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=dict(base, GONVEX_RECEIPT_CRASH_PHASE="recover"),
                                       stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=110)
            process = None
        result = log.read_text()
        if damage:
            require(code != 0 and "crash recovery changed committed business count" in result,
                    "missing-effect evidence control did not fail its intended assertion")
        else:
            require(code == 0 and "1 passed; 0 failed; 0 ignored;" in result, "native receipt crash recovery failed")
        return {"mode": mode, "storage": "sqlite", "synchronous": "full", "killSignal": "SIGKILL",
                "missingEffectControl": damage,
                "result": "expected assertion failure" if damage else "passed"}
    finally:
        if process and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        shutil.rmtree(data_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-metadata", required=True, type=Path)
    parser.add_argument("--module-host", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    artifacts = []
    for line in args.build_metadata.read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("reason") == "compiler-artifact" and row.get("target", {}).get("name") == "gonvex_runtime" and row.get("executable") and row.get("profile", {}).get("test"):
            artifacts.append(Path(row["executable"]).resolve())
    require(len(set(artifacts)) == 1, "one exact runtime test artifact required")
    binary = artifacts[0]
    host = args.module_host.resolve()
    require(binary.is_file() and host.is_file(), "native binaries missing")
    environment = dict(os.environ)
    environment.pop("GONVEX_RECEIPT_CRASH_COORDINATION", None)
    missing = subprocess.run([str(binary), TEST, "--ignored", "--exact", "--nocapture"], env=environment,
                             text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    (output / "missing-coordinator.log").write_text(missing.stdout)
    require(missing.returncode != 0 and "requires its process-crash coordinator" in missing.stdout,
            "missing crash coordinator did not fail explicitly")
    results = [run_case(binary, host, output, mode) for mode in ["committed", "uncommitted"]]
    results.append(run_case(binary, host, output, "committed", damage=True))
    paths = ["rust/Cargo.lock", "rust/crates/runtime/src/tests/receipt_guarantee_tests.rs", "rust/crates/runtime/src/execution.rs",
             "rust/crates/runtime/src/host_calls.rs", "rust/crates/runtime/src/replica_open_queue.rs", "rust/crates/store/src/lib.rs"]
    def digest(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    receipt = {"driverSha256": digest(Path(__file__)), "runtimeBinarySha256": digest(binary), "moduleHostSha256": digest(host),
               "sources": {path: digest(ROOT / path) for path in paths}, "results": results, "missingCoordinatorRejected": True,
               "boundary": "Real Linux V8/runtime process SIGKILL while it owns the SQLite tenant file (WAL, synchronous=full) in a retained data directory. Two witnessed transaction boundaries; a fresh process reopens the files, changes transport ID and repeats replay. Does not prove SDK transport, automatic reconnection, tenant HTTP/WS admission, native device storage or machine power-loss durability."}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print("Both runtime-process crash boundaries passed; missing-effect and missing-coordinator controls rejected")


if __name__ == "__main__":
    main()
