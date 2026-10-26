#!/usr/bin/env python3
"""Compare the real shared capability API with the pinned V8 policy bodies."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "verification/verus"))
from proof_runner import item

BASELINE = "3ad2ec077bd5c6b82d4f76a7a5174ede93fd42b9"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--baseline", default=BASELINE)
    args = parser.parse_args()
    out = args.build_dir.resolve()
    crate = out / "crate"
    crate.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(out / "target"), CARGO_BUILD_JOBS="1", CARGO_NET_OFFLINE="true")
    receipt = {"baseline": args.baseline, "commands": [], "success": False,
               "scope": "Exact pinned V8 policy bodies compiled against unchanged ABI types versus the actual shared crate API. CPU only, not V8 dispatch, server throughput, or workspace compilation time.",
               "sampling": "All three kinds and 1024 grants checked for compatibility before timing. Nine paired rounds in separate binaries with alternating order and warm-up. Workstation jobs are not paused.",
               "scriptSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    inputs = [ROOT / "rust/crates/module-runtime/src/capabilities.rs",
              ROOT / "rust/crates/module-runtime/src/lib.rs",
              ROOT / "verification/verus/proof_runner.py", Path(__file__)]
    input_hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}

    def command(name, argv):
        start = time.monotonic()
        result = subprocess.run([str(v) for v in argv], cwd=ROOT, env=env,
                                capture_output=True, text=True, timeout=300)
        (out / (name + ".log")).write_text(result.stdout + result.stderr)
        receipt["commands"].append({"name": name, "exit": result.returncode, "seconds": time.monotonic() - start})
        if result.returncode:
            raise RuntimeError(name + " failed; inspect its log")
        return result.stdout

    try:
        gate = Path.home() / ".local/bin/build-headroom"
        if gate.exists():
            command("headroom", [gate, out])
        source = command("baseline-source", ["git", "show", args.baseline + ":rust/crates/module-runtime-v8/src/dispatch.rs"])
        path = out / "baseline_source.rs"
        path.write_text(source)
        baseline = "mod baseline { use gonvex_module_runtime::{Capabilities, FunctionKind};\n" + item(path, "fn structural_capabilities(") + "\n" + item(path, "pub(crate) fn effective_capabilities(") + "\n}\n"
        fields = ["db_read", "db_write", "action_outbox", "action_tools", "scheduler", "network", "storage", "sandbox", "secrets", "functions"]
        harness = "use gonvex_module_runtime::{Capabilities, FunctionKind};\nfn grant(mask: u16) -> Capabilities { Capabilities {\n"
        harness += "".join(f"{field}: mask & {1 << index} != 0,\n" for index, field in enumerate(fields))
        harness += r'''} }
use std::{hint::black_box, time::Instant};
fn main() {
    let kinds = [FunctionKind::Query, FunctionKind::Reducer, FunctionKind::Action];
    let grants: Vec<_> = (0..1024).map(grant).collect();
    for kind in &kinds {
        for granted in &grants {
            assert_eq!(baseline::effective_capabilities(kind, granted),
                       gonvex_module_runtime::effective_capabilities(kind, granted));
        }
    }
    let run = || {
        let started = Instant::now();
        for i in 0..10000000_usize {
            black_box(filter(black_box(&kinds[i % 3]), black_box(&grants[i % 1024])));
        }
        started.elapsed().as_secs_f64() * 1e9 / 10000000.0
    };
    black_box(run());
    println!("{}", run());
}
'''
        manifest = "[package]\nname = 'gonvex-capability-benchmark'\nversion = '0.0.0'\nedition = '2021'\n[dependencies]\ngonvex-module-runtime = { path = " + json.dumps(str(ROOT / "rust/crates/module-runtime")) + " }\n"
        for name, function in [("baseline", "baseline::effective_capabilities"), ("current", "gonvex_module_runtime::effective_capabilities")]:
            (crate / (name + ".rs")).write_text(baseline + f"use {function} as filter;\n" + harness)
            manifest += f"[[bin]]\nname = '{name}'\npath = '{name}.rs'\n"
        (crate / "Cargo.toml").write_text(manifest)
        cargo = ["rustup", "run", "1.94.0", "cargo"]
        command("lock", cargo + ["generate-lockfile", "--offline", "--manifest-path", crate / "Cargo.toml"])
        compile_gate = shutil.which("coach-compile")
        prefix = [compile_gate, "--"] if compile_gate else []
        command("compile", prefix + cargo + ["build", "--release", "--locked", "--manifest-path", crate / "Cargo.toml"])
        samples = {"baseline": [], "current": []}
        for round in range(9):
            for name in (["baseline", "current"] if round % 2 == 0 else ["current", "baseline"]):
                samples[name].append(float(command(f"sample-{round}-{name}", [out / "target/release" / name])))
        for path, expected in input_hashes.items():
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise RuntimeError("benchmark input changed while running: " + str(path))
        receipt.update(samplesNanoseconds=samples,
                       medianNanoseconds={name: statistics.median(values) for name, values in samples.items()},
                       baselineSourceSha256=hashlib.sha256(source.encode()).hexdigest(),
                       policySourceSha256=hashlib.sha256((ROOT / "rust/crates/module-runtime/src/capabilities.rs").read_bytes()).hexdigest(),
                       typeSourceSha256=hashlib.sha256((ROOT / "rust/crates/module-runtime/src/lib.rs").read_bytes()).hexdigest(),
                       harnessSha256=hashlib.sha256(harness.encode()).hexdigest(),
                       lockSha256=hashlib.sha256((crate / "Cargo.lock").read_bytes()).hexdigest(), success=True)
    finally:
        (out / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt["medianNanoseconds"]))


if __name__ == "__main__":
    main()
