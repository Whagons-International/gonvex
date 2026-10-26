#!/usr/bin/env python3
"""Compare the old ACK transition with the extracted mutable production policy."""
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
    source = ROOT / "rust/crates/runtime/src/snapshot_ack.rs"
    initial = source.read_bytes()
    receipt = {"baseline": args.baseline, "success": False, "commands": [],
               "scope": "Old production current-snapshot ACK branch versus extracted mutable production function. Baseline substitutes its error string with unit and early Ok(None) with Ok(false), then returns Ok(true) at the former queue-release boundary. Only ACK progress CPU, excluding frame queues, routing, storage and transport.",
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

    declaration = item(source, "pub(crate) struct AckProgress {")
    harness = r'''
use std::{hint::black_box, time::Instant};
fn run() -> f64 {
    let cases = [
        (None, None, 0), (Some(0), None, 0),
        (Some(10), None, 5), (Some(10), Some(5), 5),
        (Some(10), Some(5), 4), (Some(10), Some(5), 6),
        (Some(10), None, 11), (Some(10), Some(20), 11),
        (Some(u64::MAX), Some(u64::MAX - 1), u64::MAX),
    ];
    let started = Instant::now();
    for i in 0..10000000_usize {
        let (sent, acked, sequence) = black_box(cases[i % cases.len()]);
        let mut progress = AckProgress { sent, acked };
        let _ = black_box(record_ack(black_box(&mut progress), black_box(sequence)));
        black_box(progress.acked);
    }
    started.elapsed().as_secs_f64() * 1e9 / 10000000.0
}
fn main() { black_box(run()); println!("{}", run()); }
'''
    try:
        gate = Path.home() / ".local/bin/build-headroom"
        if gate.exists():
            command("headroom", [gate, out])
        previous = command("baseline-source", ["git", "show", args.baseline + ":rust/crates/runtime/src/snapshot_delivery.rs"])
        before = out / "before.rs"
        before.write_text(previous)
        for name, path in [("baseline", before), ("current", source)]:
            if name == "baseline":
                begin = previous.index("        if pending.sent.is_none_or(")
                end = previous.index("        while pending", begin)
                body = previous[begin:end].replace("pending.", "progress.")
                marker = 'return Err("snapshot acknowledgement exceeds sent sequence");'
                if body.count(marker) != 1 or body.count("return Ok(None);") != 1:
                    raise RuntimeError("baseline transition changed; review extraction")
                body = body.replace(marker, "return Err(());").replace("return Ok(None);", "return Ok(false);")
                function = "fn record_ack(progress: &mut AckProgress, sequence: u64) -> Result<bool, ()> {\n" + body + "    Ok(true)\n}"
            else:
                function = item(source, "pub(crate) fn record_ack(")
            code = declaration + "\n" + function + "\n" + harness
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
        if source.read_bytes() != initial or hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != receipt["scriptSha256"]:
            raise RuntimeError("benchmark inputs changed during measurement")
        receipt.update(samplesNanoseconds=samples,
                       medianNanoseconds={name: statistics.median(values) for name, values in samples.items()},
                       baselineSourceSha256=hashlib.sha256(previous.encode()).hexdigest(),
                       currentSourceSha256=hashlib.sha256(initial).hexdigest(), success=True)
    finally:
        (out / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt["medianNanoseconds"]))


if __name__ == "__main__":
    main()
