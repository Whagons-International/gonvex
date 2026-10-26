#!/usr/bin/env python3
"""Compare the shared SQL scanner with a pinned Rust staging scanner."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
BASELINE = "0832985abbf13e43cb777459c215d754333d4294"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--baseline", default=BASELINE)
    args = parser.parse_args()
    out = args.build_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    receipt = {
        "baseline": args.baseline,
        "started": time.time(),
        "scope": "SQL scanner CPU cost only. No database, server throughput, memory, or whole-workspace build comparison.",
        "sampling": "Nine paired rounds in separate binaries with identical harnesses, alternating process order. Each case warms up before timing. Other machine work is not paused.",
        "commands": [],
        "scriptSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }

    def command(name, argv):
        start = time.monotonic()
        result = subprocess.run([str(x) for x in argv], cwd=ROOT,
                                capture_output=True, text=True, timeout=300)
        (out / (name + ".log")).write_text(result.stdout + result.stderr)
        receipt["commands"].append({"name": name, "exit": result.returncode,
                                    "seconds": time.monotonic() - start})
        if result.returncode:
            raise RuntimeError(name + " failed; inspect its log")
        return result.stdout

    try:
        gate = Path.home() / ".local/bin/build-headroom"
        if gate.exists():
            command("headroom", [gate, out])
        previous = command("baseline-source", ["git", "show", args.baseline +
                           ":rust/crates/runtime/src/sql_statement_lexer.rs"])
        current = (ROOT / "rust/crates/postgres/src/sql_lexer.rs").read_text()
        receipt["baselineSourceSha256"] = hashlib.sha256(previous.encode()).hexdigest()
        receipt["currentSourceSha256"] = hashlib.sha256(current.encode()).hexdigest()
        harness = r'''
use std::{hint::black_box, time::Instant};
fn run(sql: &str, count: u32) -> u128 {
    let start = Instant::now();
    for _ in 0..count {
        black_box(lexer::analyze(black_box(sql)).unwrap());
    }
    start.elapsed().as_nanos() / u128::from(count)
}
fn main() {
    let cases = [
        ("short", "SELECT * FROM tasks WHERE id=$1".to_owned(), 2000000),
        ("join", "SELECT t.id,t.title FROM tasks t JOIN members m ON m.id=t.member_id WHERE m.account_id=$1 AND t.status=$2 ORDER BY t.id LIMIT 50".to_owned(), 1000000),
        ("large_literal", format!("SELECT '{}' AS n", "x".repeat(100000)), 10000),
    ];
    print!("{{");
    for (index, (name, sql, count)) in cases.iter().enumerate() {
        black_box(run(sql, *count));
        if index > 0 { print!(","); }
        print!("\"{name}\":{}", run(sql, *count));
    }
    println!("}}");
}
'''
        receipt["harnessSha256"] = hashlib.sha256(harness.encode()).hexdigest()
        compile_gate = Path.home() / ".local/bin/coach-compile"
        prefix = [compile_gate, "--"] if compile_gate.exists() else []
        for name, source in [("baseline", previous), ("current", current)]:
            code = "#[allow(dead_code)] mod lexer {\n" + source.split("#[cfg(test)]")[0] + "\n}\n" + harness
            (out / (name + ".rs")).write_text(code)
            command("compile-" + name, prefix + ["rustup", "run", "1.94.0", "rustc", "--edition=2021",
                                               "-O", out / (name + ".rs"), "-o", out / name])
        samples = {}
        for round in range(9):
            order = ["baseline", "current"] if round % 2 == 0 else ["current", "baseline"]
            for name in order:
                result = json.loads(command(f"sample-{round}-{name}", [out / name]))
                for case, nanos in result.items():
                    samples.setdefault(case, {"baseline": [], "current": []})[name].append(nanos)
        receipt["samplesNanoseconds"] = samples
        receipt["medianNanoseconds"] = {
            case: {variant: statistics.median(data) for variant, data in variants.items()}
            for case, variants in samples.items()
        }
        receipt["success"] = True
    except BaseException as error:
        receipt.update(success=False, failure=str(error))
        raise
    finally:
        receipt["finished"] = time.time()
        (out / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
