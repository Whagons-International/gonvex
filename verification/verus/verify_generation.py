#!/usr/bin/env python3
"""Verify the production allocation body, with deliberate failing controls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'rust/crates/server/src/generation.rs'
VERSION = '0.2026.10.18.7d82994'
CONTRACT = '''-> (result: Option<u64>)
    ensures
        match requested {
            Some(value) => result == if value > issued && value > active {
                Some(value)
            } else {
                None
            },
            None => match result {
                Some(value) => value > issued && value > active
                    && value as int == (if issued > active { issued as int } else { active as int }) + 1,
                None => issued == u64::MAX || active == u64::MAX,
            },
        },
{'''


def production_function():
    text = SOURCE.read_text()
    start = text.index('pub(crate) fn next_generation(')
    end = text.index('\n#[cfg(test)]', start)
    function = text[start:end].strip()
    if re.search(r'\b(assume|admit|axiom|external_body|external_fn_specification)\b', function):
        raise RuntimeError('unexpected trust escape in production body')
    marker = '-> Option<u64> {'
    if function.count(marker) != 1:
        raise RuntimeError('production signature changed; review the proof adapter')
    # Only the return binder and contract are inserted. The executable body is
    # copied byte for byte. No separately maintained executable model is used.
    return function, function.replace(marker, CONTRACT, 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verus', default=os.environ.get('VERUS_BIN') or shutil.which('verus'))
    args = parser.parse_args()
    if not args.verus:
        parser.error('set VERUS_BIN or pass --verus with the pinned Verus binary')
    args.output.mkdir(parents=True, exist_ok=True)
    version = subprocess.check_output([args.verus, '--version'], text=True).strip()
    if VERSION not in version:
        raise RuntimeError(f'expected Verus {VERSION}, got {version}')
    original, function = production_function()
    controls = [
        ('accept_equal_generation', 'if value > floor {', 'if value >= floor {'),
        ('reuse_generation', 'Some(floor + 1)', 'Some(floor)'),
        ('unchecked_exhaustion', 'if floor == u64::MAX {', 'if false {'),
    ]
    variants = [('production', function, True)]
    for name, before, after in controls:
        if function.count(before) != 1:
            raise RuntimeError('negative control no longer matches production: ' + name)
        variants.append((name, function.replace(before, after, 1), False))
    receipt = {
        'verus': version,
        'productionSource': str(SOURCE.relative_to(ROOT)),
        'sourceSha256': hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        'functionSha256': hashlib.sha256(original.encode()).hexdigest(),
        'scope': 'Allocation arithmetic for every u64 input and Option<u64> request. Registry locks, unload persistence, and native effects are outside this proof.',
        'checks': [],
    }
    for name, code, expected in variants:
        path = args.output / (name + '.rs')
        path.write_text('use vstd::prelude::*;\nverus! {\n' + code + '\n}\n')
        start = time.monotonic()
        process = subprocess.run([args.verus, '--edition=2021', '--crate-type=lib', str(path)], capture_output=True, text=True)
        output = process.stdout + process.stderr
        (args.output / (name + '.log')).write_text(output)
        if expected:
            passed = process.returncode == 0 and re.search(r'[1-9][0-9]* verified, 0 errors', output) is not None
        else:
            passed = process.returncode != 0 and re.search(r'verification results.*[1-9][0-9]* errors', output) is not None
        receipt['checks'].append({'name': name, 'expectedToVerify': expected, 'exit': process.returncode, 'seconds': time.monotonic() - start, 'summary': re.findall(r'verification results.*', output), 'passed': passed})
        receipt['success'] = all(check['passed'] for check in receipt['checks'])
        (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        if not passed:
            raise RuntimeError(name + ' had an unexpected verification result; inspect its log')
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
