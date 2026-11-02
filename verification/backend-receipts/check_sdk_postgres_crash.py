#!/usr/bin/env python3
"""Recover a real SDK journal through live runtime pools after owned PostgreSQL SIGKILL."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import time
import uuid

from check_restart import command, postgres_ready, require
from check_sdk import digest, ROOT, TEST


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-metadata", type=Path, required=True)
    parser.add_argument("--module-host", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--missing-effect-control", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    artifacts = set()
    for line in args.build_metadata.read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("reason") == "compiler-artifact" and row.get("target", {}).get("name") == "gonvex_runtime" and row.get("executable") and row.get("profile", {}).get("test"):
            artifacts.add(Path(row["executable"]).resolve())
    require(len(artifacts) == 1, "one exact runtime artifact required")
    binary, host = artifacts.pop(), args.module_host.resolve()
    require(binary.is_file() and host.is_file(), "native artifacts missing")
    native_hashes = {"runtime": digest(binary), "moduleHost": digest(host)}
    source_paths = [
        "rust/crates/runtime/src/tests/receipt_guarantee_tests.rs", "rust/crates/runtime/src/execution.rs",
        "rust/crates/runtime/src/replica_open_queue.rs", "rust/crates/postgres/src/lib.rs",
        "packages/client/src/index.ts", "packages/client/src/outbox.ts", "packages/client/src/local-replica.ts",
        "verification/backend-receipts/sdk_composition.mjs", "verification/backend-receipts/check_sdk_postgres_crash.py",
    ]
    source_paths += [str(path.relative_to(ROOT)) for path in (ROOT / "verification/backend-receipts").glob("*.mjs")
                     if str(path.relative_to(ROOT)) not in source_paths]
    sources = {name: digest(ROOT / name) for name in source_paths}
    packages = ["client", "expo-sqlite", "protocol", "local-runtime", "module-sdk"]
    built = {str(path.relative_to(ROOT)): digest(path) for package in packages for path in ROOT.glob(f"packages/{package}/dist/**/*.js")}
    typescript = {str(path.relative_to(ROOT)): digest(path) for package in packages for path in ROOT.glob(f"packages/{package}/src/**/*.ts")
                  if not path.name.endswith(".test.ts")}
    require(built and typescript, "built SDK and TypeScript source inputs required")
    coordination = output / "postgres-crash"
    process = container = None
    try:
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        container = command(["docker", "create", "--name", "gonvex-sdk-pg-crash-" + uuid.uuid4().hex[:12],
                             "--memory", "512m", "--cpus", "1", "--pids-limit", "128", "--shm-size", "64m",
                             "-p", f"127.0.0.1:{port}:5432", "-e", "POSTGRES_PASSWORD=fixture", "-e", "POSTGRES_DB=postgres", "postgres:16-alpine"])
        command(["docker", "start", container])
        postgres_ready(container)
        image = command(["docker", "inspect", container, "--format", "{{.Image}}"])
        def sql(statement):
            return command(["docker", "exec", container, "psql", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-U", "postgres", "-Atc", statement])
        settings = sql("SELECT name || '=' || setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit') ORDER BY name").splitlines()
        require(settings == ["fsync=on", "full_page_writes=on", "synchronous_commit=on"], "durable PostgreSQL settings required")
        environment = dict(os.environ)
        for name in ["GONVEX_SDK_RECEIPT_DAMAGE", "GONVEX_SDK_RECEIPT_CLIENT_MODULE", "GONVEX_SDK_PROCESS_DAMAGE", "GONVEX_SDK_ROLLBACK_DAMAGE", "GONVEX_SDK_UPGRADE_DAMAGE", "GONVEX_SDK_CONCURRENCY_DAMAGE", "GONVEX_SDK_OWNER_ORDER"]:
            environment.pop(name, None)
        environment.update(GONVEX_RECEIPT_TEST_REQUIRE_NATIVE="1", GONVEX_TEST_MODULE_HOST_BINARY=str(host),
                           GONVEX_TEST_POSTGRES_URL=f"postgres://postgres:fixture@127.0.0.1:{port}/postgres?sslmode=disable",
                           GONVEX_SDK_RECEIPT_SCRIPT=str(ROOT / "verification/backend-receipts/sdk_composition.mjs"),
                           GONVEX_SDK_RECEIPT_OUTPUT=str(output / "sdk"), GONVEX_SDK_RECEIPT_HOLD_REPLICA="1",
                           GONVEX_SDK_PG_CRASH_COORDINATION=str(coordination))
        with (output / "native.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 60
            ready = Path(str(coordination) + ".ready")
            while not ready.is_file():
                require(process.poll() is None, "SDK exited before PostgreSQL crash boundary")
                require(time.monotonic() < deadline, "SDK did not witness PostgreSQL crash boundary")
                time.sleep(.02)
            boundary = json.loads(ready.read_text())
            require(boundary["mode"] == "lost_reply" and boundary["rowId"] == "sdk-lost_reply"
                    and boundary["receivedResultDropped"] and boundary["oldSDKClosed"] and boundary["committedRevision"] > 0,
                    "actual lost-result SDK boundary required")
            require(re.fullmatch(r"[a-f0-9-]{36}", boundary["key"]), "invalid fixture intent key")
            require(len(boundary["retained"]) == 1 and boundary["retained"][0]["idempotencyKey"] == boundary["key"], "durable original intent missing")
            schema = sql("SELECT nspname FROM pg_namespace WHERE nspname ~ '^receipt_guarantee_[a-f0-9]{32}$'")
            require(re.fullmatch(r"receipt_guarantee_[a-f0-9]{32}", schema), "one owned tenant schema required")
            def witness():
                return json.loads(sql(f'''SELECT json_build_object(
                  'effects',(SELECT count(*) FROM "{schema}".receipt_records WHERE id='sdk-lost_reply' AND value=7),
                  'receipts',(SELECT count(*) FROM "{schema}"._gonvex_reducer_idempotency
                    WHERE subject='account' AND idempotency_key='{boundary["key"]}' AND path='write' AND result IS NOT NULL))'''))
            before = witness()
            require(before == {"effects": 1, "receipts": 1}, "committed business/receipt SQL witness required before PostgreSQL crash")
            original_pid = process.pid
            command(["docker", "kill", "--signal", "KILL", container])
            exit_code = int(command(["docker", "wait", container]))
            require(exit_code == 137, "owned PostgreSQL must actually exit from SIGKILL")
            command(["docker", "start", container])
            postgres_ready(container)
            if args.missing_effect_control:
                sql(f'''BEGIN; SET LOCAL search_path TO "{schema}", public;
                        DELETE FROM receipt_records WHERE id='sdk-lost_reply'; COMMIT;''')
            after = witness()
            # This named failure is an evidence-integrity control, not a simulated
            # PostgreSQL durability failure or a replacement for SDK row checks.
            require(after == before, "PostgreSQL crash recovery lost committed business/receipt witness")
            require(process.poll() is None and process.pid == original_pid, "runtime process was replaced during PostgreSQL recovery")
            restart = {"killSignal": "SIGKILL", "exitCode": exit_code, "before": before, "after": after}
            temporary = Path(str(coordination) + ".restarted.tmp")
            temporary.write_text(json.dumps(restart) + "\n")
            temporary.replace(Path(str(coordination) + ".restarted"))
            code = process.wait(timeout=150)
        native_log = (output / "native.log").read_text()
        require(code == 0 and "1 passed; 0 failed; 0 ignored;" in native_log, "SDK recovery across PostgreSQL SIGKILL failed")
        recovered = json.loads((output / "sdk/lost_reply.json").read_text())
        require(recovered["postgresCrash"] == restart and len(recovered["calls"]) == 1
                and recovered["receiptQueries"] and recovered["retirements"], "SDK did not reconcile original committed intent after PostgreSQL crash")
        require(recovered["finalCursor"]["revision"] >= boundary["committedRevision"], "durable recovered replica trails the committed result")
        require(all(digest(ROOT / name) == value for name, value in sources.items()), "source changed during composed crash run")
        require(all(digest(ROOT / name) == value for name, value in (built | typescript).items()), "SDK inputs changed during composed crash run")
        require(digest(binary) == native_hashes["runtime"] and digest(host) == native_hashes["moduleHost"], "native artifacts changed during composed crash run")
        receipt = {"nativeArtifacts": native_hashes, "sources": sources, "postgresImage": image,
                   "builtJavaScript": built, "typescriptSources": typescript,
                   "postgresSettings": settings, "runtimePidUnchanged": True, "explicitPoolReset": False,
                   "postgresCrash": restart, "recovered": recovered,
                   "boundary": "Actual owned PostgreSQL SIGKILL with retained data and unchanged runtime process/pools. SDK lost the real committed reply, closed, reopened the same SQLite files and reconciled without ordinary resend. Covers SQLx pool recovery, not automatic same-SDK socket reconnection, uncommitted crash, native Expo/Hermes/iOS or machine power loss."}
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print("SDK durable retirement across PostgreSQL SIGKILL passed without runtime restart or explicit pool reset")
    finally:
        if process and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        if container:
            command(["docker", "rm", "--force", "--volumes", container])


if __name__ == "__main__":
    main()
