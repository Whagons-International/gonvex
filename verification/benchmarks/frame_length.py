#!/usr/bin/env python3
"""Measure copied production frame-length adapters, excluding transport I/O."""
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
    source = ROOT / "rust/crates/module-host/src/framing.rs"
    initial = source.read_bytes()
    receipt = {"baseline": args.baseline, "success": False, "commands": [],
               "scope": "Copied production checked_frame_length bodies and error variant fields; only Display/From derive attributes are omitted. Successful length checks only. Not allocation, transport, async scheduling, or server throughput.",
               "sampling": "Nine paired rounds in separate binaries, alternating order after warm-up, 100 million checks per sample. Other workstation jobs are not paused.",
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

    harness = r'''
use std::{hint::black_box, time::Instant};
fn run() -> f64 {
    let start = Instant::now();
    for i in 0..100000000_usize {
        let size = black_box((i % 4096) + 1);
        black_box(checked_frame_length(size, black_box(65536)).unwrap());
    }
    start.elapsed().as_secs_f64() * 1e9 / 100000000.0
}
fn main() { black_box(run()); println!("{}", run()); }
'''
    try:
        gate = Path.home() / ".local/bin/build-headroom"
        if gate.exists():
            command("headroom", [gate, out])
        previous = command("baseline-source", ["git", "show", args.baseline + ":rust/crates/module-host/src/framing.rs"])
        before = out / "before.rs"
        before.write_text(previous)
        for name, path in [("baseline", before), ("current", source)]:
            error = item(path, "pub enum FrameError {")
            error = re.sub(r"^\s*#\[error[^\n]*\]\n", "", error, flags=re.M).replace("#[from] ", "")
            if "#[" in error:
                raise RuntimeError("error fields changed; review benchmark extraction")
            core = item(path, "fn frame_length(") if name == "current" else ""
            code = "use std::io;\n#[derive(Debug)]\n" + error + "\n" + core + "\n" + item(path, "fn checked_frame_length(") + "\n" + harness
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
