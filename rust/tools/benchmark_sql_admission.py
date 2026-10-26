#!/usr/bin/env python3
"""Benchmark Rust SQL admission only; no database or Bend toolchain is needed."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
BASELINE = 'f28b6cd18a1a9ee5ae13b2c35e8735023c02cc8a'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, required=True)
    parser.add_argument('--baseline', default=BASELINE)
    args = parser.parse_args()
    out = args.build_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    receipt = {
        'baseline': args.baseline,
        'started': time.time(),
        'scope': 'Rust admission CPU cost on three valid SQL samples; not database or server throughput.',
        'sampling': 'Nine sequential samples per strategy in fixed order; other machine work is not paused.',
        'scriptSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'commands': [],
        'sourceItems': {},
    }

    def command(name, argv):
        start = time.monotonic()
        process = subprocess.run([str(v) for v in argv], cwd=ROOT, capture_output=True, text=True, timeout=300)
        (out / (name + '.log')).write_text(process.stdout + process.stderr)
        receipt['commands'].append({'name': name, 'exit': process.returncode, 'seconds': time.monotonic() - start})
        if process.returncode:
            raise RuntimeError(name + ' failed; inspect its log')
        return process.stdout

    def item(source, name):
        return source[source.index('fn ' + name + '('):].split('\n}\n', 1)[0] + '\n}\n'

    try:
        gate = Path.home() / '.local/bin/build-headroom'
        if gate.exists():
            command('headroom', [gate, out])
        old = command('baseline-source', ['git', 'show', args.baseline + ':rust/crates/runtime/src/host_calls.rs'])
        current = (ROOT / 'rust/crates/runtime/src/host_calls.rs').read_text()
        lexer = (ROOT / 'rust/crates/postgres/src/sql_lexer.rs').read_text().split('#[cfg(test)]')[0]
        policy = '\n'.join(line for line in (ROOT / 'rust/crates/runtime/src/sql_admission.rs').read_text().split('#[cfg(test)]')[0].splitlines() if not line.startswith('//!'))
        code = 'mod sql_statement_lexer {\n' + lexer + '\n}\n' + policy + '\n'
        for name, replacement in [('require_single_statement', 'baseline_single'), ('require_read_statement', 'baseline_read')]:
            body = item(old, name)
            receipt['sourceItems']['baseline_' + name] = hashlib.sha256(body.encode()).hexdigest()
            code += body.replace('fn ' + name + '(', 'fn ' + replacement + '(', 1) + '\n'
        for name in ['require_single_statement', 'require_read_head']:
            body = item(current, name)
            receipt['sourceItems']['current_' + name] = hashlib.sha256(body.encode()).hexdigest()
            code += body + '\n'
        code += r'''
use std::{hint::black_box, time::Instant};
fn run(variant: usize, sql: &str, count: u32) -> u128 {
    let start = Instant::now();
    for _ in 0..count {
        let sql = black_box(sql);
        if variant == 0 {
            baseline_single(sql).unwrap();
            baseline_read(sql).unwrap();
        } else {
            let result = require_single_statement(sql).unwrap();
            require_read_head(result.first_word.unwrap_or_default()).unwrap();
        }
    }
    start.elapsed().as_nanos() / u128::from(count)
}
fn main() {
    let cases = [
        ("short", "SELECT * FROM tasks WHERE id=$1".to_owned(), 200000),
        ("join", "SELECT t.id,t.title FROM tasks t JOIN members m ON m.id=t.member_id WHERE m.account_id=$1 AND t.status=$2 ORDER BY t.id LIMIT 50".to_owned(), 100000),
        ("large_literal", format!("SELECT '{}' AS n", "x".repeat(100000)), 1000),
    ];
    print!("{{");
    for (index, (name, sql, count)) in cases.iter().enumerate() {
        if index > 0 { print!(","); }
        print!("\"{name}\":{{");
        for variant in 0..2 {
            if variant > 0 { print!(","); }
            let label = ["baseline", "current"][variant];
            print!("\"{label}\":[");
            for n in 0..9 {
                if n > 0 { print!(","); }
                print!("{}", run(variant, sql, *count));
            }
            print!("]");
        }
        print!("}}");
    }
    println!("}}");
}
'''
        receipt['harnessSha256'] = hashlib.sha256(code.encode()).hexdigest()
        (out / 'bench.rs').write_text(code)
        compile_gate = Path.home() / '.local/bin/coach-compile'
        prefix = [compile_gate, '--'] if compile_gate.exists() else []
        command('compile', prefix + ['rustup', 'run', '1.94.0', 'rustc', '--edition=2021', '-O', out / 'bench.rs', '-o', out / 'bench'])
        data = json.loads(command('benchmark', [out / 'bench']))
        receipt['samplesNanoseconds'] = data
        receipt['medianNanoseconds'] = {case: {variant: statistics.median(samples) for variant, samples in variants.items()} for case, variants in data.items()}
        receipt['lexerSha256'] = hashlib.sha256((ROOT / 'rust/crates/postgres/src/sql_lexer.rs').read_bytes()).hexdigest()
        receipt['success'] = True
    except BaseException as error:
        receipt.update(success=False, failure=str(error))
        raise
    finally:
        receipt['finished'] = time.time()
        (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')


if __name__ == '__main__':
    main()
