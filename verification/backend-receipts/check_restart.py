#!/usr/bin/env python3
"""Crash only an owned PostgreSQL container across a witnessed receipt boundary."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
TEST = "tests::receipt_guarantee_tests::receipt_recovery_after_postgres_process_crash"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=30, **kwargs).strip()


def postgres_ready(container):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        result = subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        if result.returncode == 0:
            return
        time.sleep(.1)
    raise AssertionError("owned PostgreSQL did not recover")


def run_case(binary, host, output, mode, damage=False):
    label = mode + ("-missing-effect-control" if damage else "")
    coordination = output / label
    log = output / (label + ".log")
    container = None
    process = None
    with log.open("w") as stream:
        try:
            with socket.socket() as reserved:
                reserved.bind(("127.0.0.1", 0))
                selected_port = reserved.getsockname()[1]
            container = command(["docker", "create", "--name", "gonvex-receipt-crash-" + uuid.uuid4().hex[:12],
                                 "--memory", "512m", "--cpus", "1", "--pids-limit", "128", "--shm-size", "64m",
                                 "-p", f"127.0.0.1:{selected_port}:5432", "-e", "POSTGRES_PASSWORD=fixture", "-e", "POSTGRES_DB=postgres", "postgres:16-alpine"])
            command(["docker", "start", container])
            postgres_ready(container)
            image = command(["docker", "inspect", container, "--format", "{{.Image}}"])
            settings = command(["docker", "exec", container, "psql", "-h", "127.0.0.1", "-U", "postgres", "-Atc",
                                "SELECT name || '=' || setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit') ORDER BY name"]).splitlines()
            require(settings == ["fsync=on", "full_page_writes=on", "synchronous_commit=on"], "durable PostgreSQL settings required")
            port = command(["docker", "port", container, "5432/tcp"]).rsplit(":", 1)[1]
            environment = dict(os.environ, GONVEX_RECEIPT_TEST_REQUIRE_NATIVE="1", GONVEX_TEST_MODULE_HOST_BINARY=str(host),
                               GONVEX_TEST_POSTGRES_URL=f"postgres://postgres:fixture@127.0.0.1:{port}/postgres?sslmode=disable",
                               GONVEX_RECEIPT_CRASH_MODE=mode, GONVEX_RECEIPT_CRASH_COORDINATION=str(coordination))
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 60
            ready = Path(str(coordination) + ".ready")
            while not ready.is_file():
                require(process.poll() is None, "native fixture exited before witnessing the commit boundary")
                require(time.monotonic() < deadline, "native fixture did not witness the commit boundary")
                time.sleep(.05)
            witness = json.loads(ready.read_text())
            require(witness["mode"] == mode and re.fullmatch(r"receipt_guarantee_[a-f0-9]{32}", witness["schema"]), "invalid native boundary witness")
            command(["docker", "kill", "--signal", "KILL", container])
            command(["docker", "wait", container])
            command(["docker", "start", container])
            postgres_ready(container)
            require(command(["docker", "port", container, "5432/tcp"]).rsplit(":", 1)[1] == port,
                    "owned PostgreSQL endpoint changed across restart")
            recovery = subprocess.run(["docker", "logs", container], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=15).stdout
            (output / (label + "-postgres.log")).write_text(recovery)
            require("database system was interrupted" in recovery or "database system was not properly shut down" in recovery,
                    "PostgreSQL did not report crash recovery")
            if damage:
                require(mode == "committed", "the evidence control requires a committed receipt")
                command(["docker", "exec", container, "psql", "-h", "127.0.0.1", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
                         f'SET search_path TO "{witness["schema"]}"; DELETE FROM receipt_records'])
            Path(str(coordination) + ".resume").write_text("recovered\n")
            code = process.wait(timeout=110)
            stream.flush()
            result = log.read_text()
            if damage:
                require(code != 0 and "crash recovery changed committed business count" in result,
                        "missing-effect evidence control did not fail its intended assertion")
            else:
                require(code == 0 and "1 passed; 0 failed; 0 ignored;" in result, "native receipt crash recovery failed")
            return {"mode": mode, "postgresImage": image, "postgresSettings": settings,
                    "crashRecoveryObserved": True, "missingEffectControl": damage,
                    "result": "expected assertion failure" if damage else "passed"}
        finally:
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
            if container:
                subprocess.run(["docker", "rm", "--force", "--volumes", container], check=True, stdout=subprocess.DEVNULL, timeout=20)


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
             "rust/crates/runtime/src/host_calls.rs", "rust/crates/runtime/src/replica_open_queue.rs", "rust/crates/postgres/src/lib.rs"]
    def digest(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    receipt = {"driverSha256": digest(Path(__file__)), "runtimeBinarySha256": digest(binary), "moduleHostSha256": digest(host),
               "sources": {path: digest(ROOT / path) for path in paths}, "results": results, "missingCoordinatorRejected": True,
               "boundary": "Real Linux V8/runtime/PostgreSQL process crash on an owned container with retained PGDATA. Two witnessed transaction boundaries, changed transport ID and repeat replay. Explicitly reconnects runtime DB pools before retry. Does not prove SDK transport, automatic reconnection, tenant HTTP/WS admission, native device storage or machine power-loss durability."}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print("Both PostgreSQL crash boundaries passed; missing-effect and missing-coordinator controls rejected")


if __name__ == "__main__":
    main()
