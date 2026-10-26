#!/usr/bin/env python3
"""Compare production cursor decisions using the actual cursor fields."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "verification/verus"))
from proof_runner import item


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--baseline", default="3ad2ec077bd5c6b82d4f76a7a5174ede93fd42b9")
    args = parser.parse_args()
    cpu = max(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    if cpu is not None:
        os.sched_setaffinity(0, {cpu})
    out = args.build_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    source = ROOT / "rust/crates/runtime/src/replica_cursor.rs"
    initial = source.read_bytes()
    receipt = {"baseline": args.baseline, "success": False, "commands": [],
               "scope": "Actual production cursor decision bodies and unchanged ReplicaCursor fields, excluding Serde derives. Prebuilt cursor strings, including Unicode and long equal prefixes. CPU only, not retention storage, serialization or server throughput.",
               "sampling": "Nine paired rounds in separate binaries, alternating order after warm-up, 10 million checks per sample. Other workstation jobs are not paused.",
               "cpuAffinity": cpu,
               "scriptSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}

    def command(name, argv):
        start = time.monotonic()
        result = subprocess.run([str(v) for v in argv], cwd=ROOT, capture_output=True, text=True, timeout=300)
        (out / (name + ".log")).write_text(result.stdout + result.stderr)
        receipt["commands"].append({"name": name, "exit": result.returncode, "seconds": time.monotonic() - start})
        if result.returncode:
            raise RuntimeError(name + " failed; inspect its log")
        return result.stdout

    types = ROOT / "rust/crates/protocol/src/lib.rs"
    type_bytes = types.read_bytes()
    declaration = item(types, "pub struct ReplicaCursor {")
    harness = r'''
use std::{hint::black_box, time::Instant};
fn run(cases: &[(Option<ReplicaCursor>, ReplicaCursor, u64)]) -> f64 {
    let start = Instant::now();
    for i in 0..10000000_usize {
        let (client, current, retained) = black_box(&cases[i % cases.len()]);
        black_box(cursor_resumable(black_box(client.as_ref()), black_box(current), black_box(*retained)));
    }
    start.elapsed().as_secs_f64() * 1e9 / 10000000.0
}
fn main() {
    let mut cases = Vec::new();
    for epoch in ["epoch".to_owned(), "".to_owned(), "é🌎".to_owned(), "long-".repeat(200)] {
        for (client_epoch, revision, retained) in [
            (epoch.clone(), 5, 5), (epoch.clone(), 10, 5),
            (epoch.clone(), 4, 5), (epoch.clone(), 11, 5),
            (format!("{epoch}x"), 10, 5), (epoch.clone(), 10, 11),
            (epoch.clone(), u64::MAX, u64::MAX),
        ] {
            let current = ReplicaCursor { epoch: epoch.clone(), revision: if revision == u64::MAX { u64::MAX } else { 10 } };
            let expected = client_epoch == current.epoch && retained <= revision && revision <= current.revision;
            let client = Some(ReplicaCursor { epoch: client_epoch, revision });
            assert_eq!(cursor_resumable(client.as_ref(), &current, retained), expected);
            cases.push((client, current, retained));
        }
    }
    let current = ReplicaCursor {epoch: "e".to_owned(), revision: 10};
    assert!(!cursor_resumable(None, &current, 0));
    cases.push((None, current, 0));
    black_box(run(&cases));
    println!("{}", run(&cases));
}
'''
    try:
        gate = Path.home() / ".local/bin/build-headroom"
        if gate.exists():
            command("headroom", [gate, out])
        previous = command("baseline-source", ["git", "show", args.baseline + ":rust/crates/runtime/src/replica.rs"])
        before = out / "before.rs"
        before.write_text(previous)
        for name, path in [("baseline", before), ("current", source)]:
            signature = "fn cursor_resumable(" if name == "baseline" else "pub(crate) fn cursor_resumable("
            code = declaration + "\n" + item(path, signature) + "\n" + harness
            target = out / (name + ".rs")
            target.write_text(code)
            compile_gate = shutil.which("coach-compile")
            prefix = [compile_gate, "--"] if compile_gate else []
            if gate.exists():
                command("headroom-" + name, [gate, out])
            command("compile-" + name, prefix + ["rustup", "run", "1.94.0", "rustc", "--edition=2021", "-O", target, "-o", out / name])
        samples = {"baseline": [], "current": []}
        for round in range(9):
            for name in (["baseline", "current"] if round % 2 == 0 else ["current", "baseline"]):
                samples[name].append(float(command(f"sample-{round}-{name}", [out / name])))
        if source.read_bytes() != initial or types.read_bytes() != type_bytes or hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != receipt["scriptSha256"]:
            raise RuntimeError("benchmark inputs changed during measurement")
        receipt.update(samplesNanoseconds=samples,
                       medianNanoseconds={name: statistics.median(values) for name, values in samples.items()},
                       baselineSourceSha256=hashlib.sha256(previous.encode()).hexdigest(),
                       currentSourceSha256=hashlib.sha256(initial).hexdigest(),
                       typeSourceSha256=hashlib.sha256(type_bytes).hexdigest(), success=True)
    finally:
        (out / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt["medianNanoseconds"]))


if __name__ == "__main__":
    main()
