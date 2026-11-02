#!/usr/bin/env python3
"""Exercise the shared historical fixture with real SQLite and owned process kills.

This is a Node SQLite driver check. It never claims Expo/Hermes execution.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

from contract import TERMINAL, corrupt_result, historical_result, historical_transition, require

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "verification/native-sqlite/historical-node.mjs"
PHASES = ["historical-seed", "historical-replica-held", "historical-journal-held", "historical-recover", "historical-reopen"]
require([name for name in TERMINAL if name.startswith("historical-")] == PHASES, "required historical native phases changed")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run_case(directory, adapter=None):
    directory.mkdir()
    run_id = uuid.uuid4().hex
    previous, stops = {}, []
    env = dict(os.environ)
    env.pop("GONVEX_HISTORICAL_ADAPTER", None)
    if adapter:
        env["GONVEX_HISTORICAL_ADAPTER"] = str(adapter)
    for phase in PHASES + ["journal-corrupt-seed", "journal-corrupt-recover"]:
        child = None
        with (directory / (phase + ".log")).open("w") as log:
            try:
                child = subprocess.Popen(["node", str(RUNNER), "--directory", str(directory),
                                          "--phase", phase, "--run-id", run_id], cwd=ROOT,
                                         env=env, stdout=log, stderr=subprocess.STDOUT)
                end = time.monotonic() + 20
                event_path = directory / (phase + ".json")
                while not event_path.is_file():
                    require(child.poll() is None, f"historical process exited before {phase}; see {log.name}")
                    require(time.monotonic() < end, f"historical process missed {phase}")
                    time.sleep(.01)
                result = json.loads(event_path.read_text())
                require(result["type"] == TERMINAL[phase], "wrong historical boundary")
                require(result["runtime"]["node"].startswith("v") and len(result["runtime"]["sqlite"]) == 1, "real Node SQLite version witness required")
                require(result["runtime"]["textBindings"] == ("nul-truncation-fault-model" if env.get("GONVEX_HISTORICAL_TRUNCATE_BINDINGS") == "1" else "native-Node"), "wrong Node binding mode")
                if phase.endswith("-held"):
                    require(child.poll() is None, "held historical transaction exited before kill")
                    child.kill()
                    require(child.wait(timeout=5) == -signal.SIGKILL, "historical process was not killed with SIGKILL")
                    stops.append({"phase": phase, "pid": child.pid, "signal": "SIGKILL"})
                else:
                    require(child.wait(timeout=5) == 0, "historical child failed after its boundary")
                if phase.startswith("historical-"):
                    historical_result(result, phase)
                    historical_transition(previous, phase, result)
                else:
                    corrupt_result(result, phase)
                    if phase == "journal-corrupt-recover":
                        require(result["rows"] == previous["journal-corrupt-seed"]["rows"], "corrupt accepted journal bytes changed across process restart")
                previous[phase] = result
            finally:
                if child and child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
    require([stop["phase"] for stop in stops] == PHASES[1:3], "both native transaction kills are required")
    return {"phases": previous, "processStops": stops}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    paths = ["verification/native-sqlite/historical.mjs", "verification/native-sqlite/historical-node.mjs",
             "verification/native-sqlite/check_historical_node.py", "verification/native-sqlite/contract.py", "verification/native-sqlite/corrupt-journal.mjs",
             "verification/native-sqlite/proof-entry.js", "verification/native-sqlite/prepare.mjs", "verification/native-sqlite/run.py",
             ".github/workflows/runtime-regression.yml", ".github/workflows/native-sqlite-verification.yml", "pnpm-lock.yaml"]
    paths += [str(path.relative_to(ROOT)) for package in ["client", "expo-sqlite", "protocol", "local-runtime", "module-sdk"]
              for pattern in ["dist/**/*.js", "src/**/*.ts"] for path in (ROOT / "packages" / package).glob(pattern)
              if not path.name.endswith(".test.ts")]
    sources = {name: digest(ROOT / name) for name in paths}
    positive = run_case(output / "positive")
    mutant = output / "mutant"
    mutant.mkdir()
    with (mutant / "build.log").open("w") as log:
        subprocess.run(["node", str(RUNNER), "--directory", str(mutant), "--build-mutant", "yes"],
                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=30)
    mutation = json.loads((mutant / "mutation.json").read_text())
    require(mutation["mutations"] == 1 and mutation["method"] == "importLegacy"
            and mutation["originalSha256"] == sources["packages/expo-sqlite/src/outbox.ts"]
            and mutation["mutatedSha256"] != mutation["originalSha256"], "wrong historical acceptance source control")
    try:
        run_case(output / "accepted-state-source-control", mutant / "adapter-mutant.mjs")
    except AssertionError as error:
        require(str(error) == "historical import changed accepted intent identity or state", "historical source control failed at an unrelated boundary: " + str(error))
        (output / "expected-control-error.txt").write_text(str(error) + "\n")
    else:
        raise AssertionError("historical accepted-state source control escaped")
    for name, expected in sources.items():
        require(digest(ROOT / name) == expected, "historical input changed: " + name)
    receipt = {"textBindings": "nul-truncation-fault-model" if os.environ.get("GONVEX_HISTORICAL_TRUNCATE_BINDINGS") == "1" else "native-Node", "sources": sources, "positive": positive, "sourceAcceptedStateControlRejected": True, "mutation": mutation,
               "boundary": "Actual Node SQLite, real SDK adapters and StoreReducerOutbox. Shared historical schedule also wired into required Expo/Android fixture. Two owned Node processes SIGKILLed during native SQL transactions. This receipt does not prove Expo/Hermes, iOS, physical-device or power-loss behavior."}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print("Historical SQLite schemas, two transaction process kills, accepted-state recovery, corrupt-byte restart refusal and compiled acceptance control passed")


if __name__ == "__main__":
    main()
