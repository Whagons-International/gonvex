#!/usr/bin/env python3
"""Own a native backend and require the real SDK/SQLite composition schedules."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import tempfile

from check_restart import require

ROOT = Path(__file__).resolve().parents[2]
TEST = "tests::receipt_guarantee_tests::authenticated_sdk_receipt_replica_composition"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-metadata", type=Path, required=True)
    parser.add_argument("--module-host", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
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
    binary, host = artifacts[0], args.module_host.resolve()
    require(binary.is_file() and host.is_file(), "native binaries missing")
    # Establish artifact identity before execution. Reading each full artifact
    # also makes ELF paging part of setup rather than a cold-start latency test.
    native_hashes = {"runtime": digest(binary), "moduleHost": digest(host)}
    (output / "native-artifacts.json").write_text(json.dumps(native_hashes, indent=2) + "\n")
    environment = dict(os.environ)
    environment.pop("GONVEX_SDK_RECEIPT_SCRIPT", None)
    environment.pop("GONVEX_SDK_RECEIPT_DAMAGE", None)
    environment.pop("GONVEX_SDK_RECEIPT_CLIENT_MODULE", None)
    environment.pop("GONVEX_SDK_PROCESS_DAMAGE", None)
    environment.pop("GONVEX_SDK_ROLLBACK_DAMAGE", None)
    environment.pop("GONVEX_SDK_UPGRADE_DAMAGE", None)
    environment.pop("GONVEX_SDK_CONCURRENCY_DAMAGE", None)
    environment.pop("GONVEX_SDK_OWNER_ORDER", None)
    missing = subprocess.run([str(binary), TEST, "--ignored", "--exact", "--nocapture"],
                             env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    (output / "missing-coordinator.log").write_text(missing.stdout)
    require(missing.returncode != 0 and "requires its coordinator" in missing.stdout,
            "missing SDK coordinator did not fail explicitly")
    data_dir = None
    process = None
    try:
        # The runtime owns its SQLite files; an owned temporary data directory
        # replaces the former database container.
        data_dir = Path(tempfile.mkdtemp(prefix="gonvex-sdk-receipt-"))
        settings = {"engine": "sqlite", "synchronous": "full"}
        environment.update(GONVEX_RECEIPT_TEST_REQUIRE_NATIVE="1", GONVEX_TEST_MODULE_HOST_BINARY=str(host),
                           GONVEX_TEST_DATA_DIR=str(data_dir), GONVEX_SQLITE_SYNCHRONOUS="full",
                           GONVEX_SDK_RECEIPT_SCRIPT=str(ROOT / "verification/backend-receipts/sdk_composition.mjs"),
                           GONVEX_SDK_RECEIPT_OUTPUT=str(output / "sdk"), GONVEX_SDK_RECEIPT_HOLD_REPLICA="1")
        with (output / "native.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        native_log = (output / "native.log").read_text()
        require(code == 0 and "1 passed; 0 failed; 0 ignored;" in native_log, "authenticated SDK/backend/SQLite composition failed")
        cases = []
        for mode in ["success", "lost_reply", "write_failure", "receipt_write_failure"]:
            result = json.loads((output / "sdk" / (mode + ".json")).read_text())
            require(result["mode"] == mode and result["retirements"], "SDK case did not produce its retirement witness")
            require(result["dropped"] == (mode == "lost_reply") and result["failedWrite"] == (mode == "write_failure"),
                    "SDK fault boundary was not exercised")
            require(result["failedAcceptanceWrite"] == (mode == "receipt_write_failure"), "SDK acceptance storage boundary was not exercised")
            cases.append(result)
        require(cases[0]["commitHeld"] and cases[0]["heldWitness"]["outcomePending"],
                "positive SDK did not witness the uncommitted replica boundary")
        process_crashes = json.loads((output / "sdk/process-crashes/receipt.json").read_text())
        browser_upgrade = json.loads((output / "sdk/browser-upgrade/receipt.json").read_text())
        require(browser_upgrade["backend"] == {"business": 0, "receipts": 0}, "real rollback witness missing")
        recovery = browser_upgrade["lostReplyRecovery"]
        require(recovery["killSignal"] == "SIGKILL" and recovery["recoveredOrdinaryCalls"] == 0,
                "renamed intent requires actual browser kill and receipt-only recovery")
        require(recovery["interrupted"]["droppedResult"]["committedRevision"] > 0,
                "browser interruption did not follow a committed renamed call")
        original = browser_upgrade["upgraded"]["entries"][0]
        reopened = recovery["reopened"]["entries"][0]
        require(all(reopened.get(field) == original.get(field)
                    for field in ["id", "scope", "idempotencyKey", "createdAt", "receiptPath", "path", "args"]),
                "browser crash changed migrated intent identity")
        require(original["receiptPath"] == "abort" and original["path"] == "write"
                and recovery["lookups"] and all(call["args"]["receiptPath"] == "abort" for call in recovery["lookups"]),
                "browser recovery omitted the historical receipt namespace")
        require(len(process_crashes["cases"]) == 2 and all(case["killSignal"] == "SIGKILL" for case in process_crashes["cases"]),
                "both SDK process crash boundaries are mandatory")
        # Remove a durable replica row immediately before the same retirement
        # oracle. This must fail that oracle, not a setup or timeout check.
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "missing-replica-control"),
                           GONVEX_SDK_RECEIPT_DAMAGE="remove-replica-row")
        with (output / "missing-replica-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        control_log = (output / "missing-replica-control.log").read_text()
        require(code != 0 and "intent retirement passed durable replica data" in control_log,
                "missing replica evidence control did not fail its intended assertion")
        # Compile a single production-source mutation into a separate artifact;
        # neither source files nor the candidate's built package are rewritten.
        with (output / "mutant-build.log").open("w") as log:
            subprocess.run(["node", str(ROOT / "verification/backend-receipts/build_sdk_mutant.mjs"),
                            "--output", str(output / "sdk-mutant")], cwd=ROOT,
                           check=True, stdout=log, stderr=subprocess.STDOUT, timeout=60)
        environment.pop("GONVEX_SDK_RECEIPT_DAMAGE", None)
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "barrier-source-control"),
                           GONVEX_SDK_RECEIPT_CLIENT_MODULE=str(output / "sdk-mutant/client.mjs"))
        with (output / "barrier-source-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        mutant_log = (output / "barrier-source-control.log").read_text()
        require(code != 0 and ("intent retirement passed uncommitted replica data" in mutant_log
                              or "intent retirement passed durable replica data" in mutant_log),
                "SDK source barrier mutant did not fail its intended retirement assertion")
        with (output / "acceptance-mutant-build.log").open("w") as log:
            subprocess.run(["node", str(ROOT / "verification/backend-receipts/build_sdk_mutant.mjs"),
                            "--output", str(output / "acceptance-mutant"), "--mutation", "acceptance"], cwd=ROOT,
                           check=True, stdout=log, stderr=subprocess.STDOUT, timeout=60)
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "acceptance-source-control"),
                           GONVEX_SDK_RECEIPT_CLIENT_MODULE=str(output / "acceptance-mutant/client.mjs"))
        with (output / "acceptance-source-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "received acceptance was not durably recorded" in (output / "acceptance-source-control.log").read_text(),
                "SDK source acceptance mutant did not fail its intended journal assertion")
        with (output / "recovery-mutant-build.log").open("w") as log:
            subprocess.run(["node", str(ROOT / "verification/backend-receipts/build_sdk_mutant.mjs"),
                            "--output", str(output / "recovery-mutant"), "--mutation", "recovery"], cwd=ROOT,
                           check=True, stdout=log, stderr=subprocess.STDOUT, timeout=60)
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "recovery-source-control"),
                           GONVEX_SDK_RECEIPT_CLIENT_MODULE=str(output / "recovery-mutant/client.mjs"))
        with (output / "recovery-source-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "accepted intent became sendable again" in (output / "recovery-source-control.log").read_text(),
                "SDK source recovery mutant did not fail its intended resend assertion")
        environment.pop("GONVEX_SDK_RECEIPT_CLIENT_MODULE", None)
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "process-missing-row-control"),
                           GONVEX_SDK_PROCESS_DAMAGE="missing-row")
        with (output / "process-missing-row-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "process recovery retirement passed durable replica data" in (output / "process-missing-row-control.log").read_text(),
                "SDK process recovery evidence control did not fail its intended assertion")
        environment.pop("GONVEX_SDK_PROCESS_DAMAGE", None)
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "rollback-effect-control"),
                           GONVEX_SDK_ROLLBACK_DAMAGE="insert-effect")
        with (output / "rollback-effect-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "rollback left committed business effect" in (output / "rollback-effect-control.log").read_text(),
                "SDK rollback evidence control did not fail its intended assertion")
        rollback_failure = json.loads((output / "rollback-effect-control/browser-upgrade/failure.json").read_text())
        cached_state = rollback_failure["state"].get("lastCompletedState", {})
        require(rollback_failure["state"].get("pageClosed") is True
                and len(cached_state.get("entries", [])) == 1 and cached_state.get("snapshots"),
                "closed-page rollback failure lost its cached durable state")
        environment.pop("GONVEX_SDK_ROLLBACK_DAMAGE", None)
        # Omit only the namespace sent by production deliverOutboxBatch. Keep
        # the durable migrated entry and the fresh browser's lookup intact.
        # The backend must reject the historical-path lookup of the incorrectly
        # claimed receipt. A fixture timeout is not evidence for this control.
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "upgrade-receipt-source-control"),
                           GONVEX_SDK_UPGRADE_DAMAGE="omit-receipt-path")
        with (output / "upgrade-receipt-source-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "renamed receipt namespace was lost before restart recovery"
                in (output / "upgrade-receipt-source-control.log").read_text(),
                "SDK upgrade source control did not fail its intended receipt namespace assertion")
        upgrade_failure = json.loads((output / "upgrade-receipt-source-control/browser-upgrade/failure.json").read_text())
        mutation = upgrade_failure["sourceMutation"]
        require(mutation["active"] and mutation["mutations"] == 2 and mutation["method"] == "deliverOutboxBatch"
                and mutation["originalIndexSha256"] == digest(ROOT / "packages/client/src/index.ts")
                and mutation["originalIndexSha256"] != mutation["mutatedIndexSha256"],
                "upgrade control did not compile the intended production source mutation")
        interrupted = json.loads((output / "upgrade-receipt-source-control/browser-upgrade/postcommit-interrupted.json").read_text())
        retained = interrupted["entries"][0]
        require(retained["receiptPath"] == "abort" and interrupted["droppedResult"]["committedRevision"] > 0,
                "upgrade control corrupted the durable historical namespace or omitted the actual commit")
        deliveries = [delivery for call in interrupted["calls"] for delivery in
                      (call["calls"] if call["type"] == "reducer.callMany" else [call] if call["type"] == "reducer.call" else [])]
        require(len(deliveries) == 1 and deliveries[0]["path"] == "write" and not deliveries[0].get("receiptPath"),
                "upgrade source mutation did not reach the reducer wire call")
        failed_state = upgrade_failure["state"]["lastCompletedStorageRead"]
        lookups = [call for call in failed_state["calls"] if call["type"] == "query.call"
                   and call["path"] == "__gonvex.reducerReceipt" and call["args"].get("receiptPath") == "abort"]
        require(lookups and any(event["type"] == "query.error" and any(call["id"] == event["id"] for call in lookups)
                               and "refusing to replay" in event["error"] for event in failed_state["events"]),
                "upgrade control lacks an actual backend receipt namespace rejection")
        environment.pop("GONVEX_SDK_UPGRADE_DAMAGE", None)
        with (output / "owner-mutant-build.log").open("w") as log:
            subprocess.run(["node", str(ROOT / "verification/backend-receipts/build_sdk_mutant.mjs"),
                            "--output", str(output / "owner-mutant"), "--mutation", "owner"], cwd=ROOT,
                           check=True, stdout=log, stderr=subprocess.STDOUT, timeout=60)
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "owner-source-control"),
                           GONVEX_SDK_RECEIPT_CLIENT_MODULE=str(output / "owner-mutant/client.mjs"))
        with (output / "owner-source-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "old accepted callback removed replacement owner prediction" in (output / "owner-source-control.log").read_text(),
                "SDK owner source control did not fail its intended publication assertion")
        owner_switch = json.loads((output / "sdk/owner-switch/receipt.json").read_text())
        require([event["owner"] for event in owner_switch["auth"]] == ["account", "bob", "account"], "real account switch witness missing")
        tenant_switch = json.loads((output / "sdk/tenant-switch/receipt.json").read_text())
        require([event["owner"] for event in tenant_switch["auth"]] == ["tenant", "tenant-b", "tenant"], "real tenant switch witness missing")
        require(all(event["account"] == "account" for event in tenant_switch["auth"]), "tenant switch changed account identity")
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "tenant-source-control"),
                           GONVEX_SDK_OWNER_ORDER="tenant-first")
        with (output / "tenant-source-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "old accepted callback removed replacement owner prediction" in (output / "tenant-source-control.log").read_text(),
                "SDK tenant source control did not fail its intended publication assertion")
        tenant_failure = json.loads((output / "tenant-source-control/tenant-switch/failure.json").read_text())
        require([event["owner"] for event in tenant_failure["auth"]] == ["tenant", "tenant-b"], "tenant negative control did not rotate tenants")
        environment.pop("GONVEX_SDK_OWNER_ORDER", None)
        environment.pop("GONVEX_SDK_RECEIPT_CLIENT_MODULE", None)
        environment.update(GONVEX_SDK_RECEIPT_OUTPUT=str(output / "concurrency-witness-control"),
                           GONVEX_SDK_CONCURRENCY_DAMAGE="omit-overlap")
        with (output / "concurrency-witness-control.log").open("w") as log:
            process = subprocess.Popen([str(binary), TEST, "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=150)
        require(code != 0 and "missing witnessed receipt overlap" in (output / "concurrency-witness-control.log").read_text(),
                "SDK concurrency evidence control did not fail its intended assertion")
        concurrency = json.loads((output / "sdk/concurrency/receipt.json").read_text())
        # Runtime-writer gate (commit/rollback) and native receipt-INSERT gate
        # (native-commit/native-rollback), in the coordinator's order
        # (SDK_CONCURRENCY_MODES in receipt_guarantee_tests.rs).
        require([case["mode"] for case in concurrency["cases"]] == ["commit", "rollback", "native-commit", "native-rollback"],
                "every runtime-writer and native concurrent ownership outcome required")
        paths = ["verification/backend-receipts/check_sdk.py", "verification/backend-receipts/check_restart.py",
                 "verification/backend-receipts/build_sdk_mutant.mjs", ".github/workflows/runtime-regression.yml",
                 "verification/backend-receipts/sdk_composition.mjs", "rust/crates/runtime/src/tests/receipt_guarantee_tests.rs",
                 "verification/backend-receipts/sdk_process_crash.mjs", "verification/backend-receipts/sdk_owner_switch.mjs", "verification/backend-receipts/sdk_concurrent.mjs",
                 "verification/backend-receipts/sdk_browser_upgrade.mjs", "verification/backend-receipts/browser_rollback_upgrade_entry.mjs",
                 "rust/crates/runtime/src/lib.rs", "rust/crates/runtime/src/execution.rs", "rust/crates/runtime/src/error_class.rs", "rust/crates/runtime/src/host_calls.rs",
                 "rust/crates/runtime/src/replica.rs", "rust/crates/runtime/src/replica_open_queue.rs", "rust/crates/runtime/src/visibility.rs",
                 "rust/crates/runtime/src/change_feed.rs", "rust/crates/runtime/src/snapshot_delivery.rs",
                 "rust/crates/runtime/src/modules.rs", "rust/crates/store/src/lib.rs", "rust/Cargo.lock",
                 "pnpm-lock.yaml", "packages/client/package.json", "packages/expo-sqlite/package.json"]
        # Bind all actual built package inputs used by the native Node process.
        packages = ["client", "expo-sqlite", "protocol", "local-runtime", "module-sdk"]
        built = sorted(path for package in packages for path in ROOT.glob(f"packages/{package}/dist/**/*.js"))
        typescript = sorted(path for package in packages for path in ROOT.glob(f"packages/{package}/src/**/*.ts") if not path.name.endswith(".test.ts"))
        require(digest(binary) == native_hashes["runtime"] and digest(host) == native_hashes["moduleHost"],
                "native binaries changed during the SDK composition")
        receipt = {"runtimeBinarySha256": native_hashes["runtime"], "moduleHostSha256": native_hashes["moduleHost"],
                   "nativeBinariesPrehashed": True,
                   "sources": {path: digest(ROOT / path) for path in paths},
                   "builtJavaScript": {str(path.relative_to(ROOT)): digest(path) for path in built},
                   "typescriptSources": {str(path.relative_to(ROOT)): digest(path) for path in typescript},
                   "storageSettings": settings, "missingCoordinatorRejected": True,
                   "missingReplicaControlRejected": True,
                   "sourceBarrierControlRejected": True,
                   "sourceAcceptanceControlRejected": True,
                   "sourceAcceptanceMutation": json.loads((output / "acceptance-mutant/receipt.json").read_text()),
                   "sourceRecoveryControlRejected": True,
                   "sourceRecoveryMutation": json.loads((output / "recovery-mutant/receipt.json").read_text()),
                   "processCrashes": process_crashes,
                   "processMissingRowControlRejected": True,
                   "browserUpgrade": browser_upgrade,
                   "upgradeReceiptNamespaceControlRejected": True, "upgradeReceiptSourceMutation": mutation,
                   "rollbackEffectControlRejected": True, "ownerSourceControlRejected": True, "ownerSwitch": owner_switch,
                   "tenantSourceControlRejected": True, "tenantSwitch": tenant_switch,
                   "concurrency": concurrency, "concurrencyWitnessControlRejected": True,
                   "ownerSourceMutation": json.loads((output / "owner-mutant/receipt.json").read_text()),
                   "sourceBarrierMutation": json.loads((output / "sdk-mutant/receipt.json").read_text()),
                   "cases": cases,
                   "boundary": "Production authenticated WebSocket admission, real SDK/runtime/V8/SQLite storage, independent file-backed Node SQLite journals/replicas, actual SDK process kills, account rotation and witnessed competing claims. Chromium runs the production legacy IndexedDB browser contract upgrade after a server rollback witness. Native Expo/Hermes, other browser engines/adapters, automatic same-instance reconnection and power-loss durability remain outside this fixture."}
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print("Authenticated SDK/backend/storage composition, process kills, browser rollback/upgrade recovery, account and tenant switches, witnessed concurrency and all eleven negative controls passed")
    finally:
        if process:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        if data_dir:
            shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
