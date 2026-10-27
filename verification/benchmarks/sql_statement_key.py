#!/usr/bin/env python3
"""Prepare, then separately measure the actual old/new SQL-key serializers on Linux."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[2]
BASELINE = "8bae698880d6a7e63472596b1a22c5dc1ced0ebc"
SOURCE = "rust/crates/runtime/src/sql_statement_key.rs"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["prepare", "measure"])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rustc", default="rustc")
    args = parser.parse_args()
    out = args.output.resolve()
    harness = Path(__file__).with_suffix(".rs")
    current = ROOT / SOURCE
    metadata = out / "provenance.json"
    if args.mode == "prepare":
        out.mkdir(parents=True, exist_ok=False)
        baseline = subprocess.check_output(["git", "show", BASELINE + ":" + SOURCE], cwd=ROOT)
        (out / "before.rs").write_bytes(baseline)
        (out / "after.rs").write_bytes(current.read_bytes())
        (out / "paired.rs").write_bytes(harness.read_bytes())
        env = dict(os.environ, GONVEX_SQL_KEY_BEFORE=str(out / "before.rs"),
                   GONVEX_SQL_KEY_AFTER=str(out / "after.rs"))
        compiler = subprocess.check_output([args.rustc, "--version"], text=True).strip()
        subprocess.run([args.rustc, "--edition=2021", "--test", str(out / "after.rs"),
                        "-o", str(out / "native-tests")], env=env, check=True)
        subprocess.run([str(out / "native-tests")], check=True)
        subprocess.run([args.rustc, "--edition=2021", "-O", str(out / "paired.rs"),
                        "-o", str(out / "paired")], env=env, check=True)
        provenance = {
            "baseline": BASELINE, "source": SOURCE, "compiler": compiler,
            "files": {name: digest(out / name) for name in
                      ["before.rs", "after.rs", "paired.rs", "paired", "native-tests"]},
            "runnerSha256": digest(Path(__file__)),
        }
        metadata.write_text(json.dumps(provenance, indent=2) + "\n")
        print("Prepared native tests and benchmark. Measure separately under the desired CPU/memory caps.")
        return

    provenance = json.loads(metadata.read_text())

    def verify_inputs():
        assert provenance["baseline"] == BASELINE and provenance["source"] == SOURCE
        assert provenance["runnerSha256"] == digest(Path(__file__))
        assert provenance["files"]["after.rs"] == digest(current), "current Rust source changed"
        assert provenance["files"]["paired.rs"] == digest(harness), "benchmark source changed"
        for name, expected in provenance["files"].items():
            assert digest(out / name) == expected, name

    verify_inputs()
    raw = subprocess.check_output([str(out / "paired")], text=True, timeout=90)
    verify_inputs()
    rows = list(csv.DictReader(io.StringIO(raw)))
    assert len(rows) == 176, len(rows)
    summary = []
    for operation, case in sorted({(r["operation"], r["case"]) for r in rows}):
        pairs = []
        for round_number in range(8):
            pair = {r["implementation"]: r for r in rows if r["operation"] == operation
                    and r["case"] == case and int(r["round"]) == round_number}
            assert set(pair) == {"original", "candidate"}
            ns = {k: int(v["cpu_ns"]) / int(v["iterations"]) for k, v in pair.items()}
            pairs.append({"round": round_number, **ns,
                          "differenceNs": ns["candidate"] - ns["original"]})
        summary.append({"operation": operation, "case": case,
                        "originalMedianCpuNs": statistics.median(p["original"] for p in pairs),
                        "candidateMedianCpuNs": statistics.median(p["candidate"] for p in pairs),
                        "pairedMedianDifferenceNs": statistics.median(p["differenceNs"] for p in pairs),
                        "pairs": pairs})
    result = {"success": True, "provenance": provenance, "summary": summary,
              "scope": "Native serializer CPU time, eight alternating paired rounds. No workload weighting, allocation-count, statistical-equivalence, SQLx, database, or whole-runtime claim."}
    with (out / "samples.csv").open("x") as stream:
        stream.write(raw)
    with (out / "result.json").open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps([{k: v for k, v in entry.items() if k != "pairs"} for entry in summary]))


if __name__ == "__main__":
    main()
