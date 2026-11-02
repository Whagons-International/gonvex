#!/usr/bin/env python3
"""Finite lifetime/capacity protocol with meaningful broken-policy traces."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jar', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    tool = json.loads((HERE / 'toolchain.json').read_text())
    assert digest(args.jar) == tool['sha256'], 'TLC checksum mismatch'
    args.output.mkdir(parents=True, exist_ok=False)
    cases = []
    for name, capacity, policy, violation in [
        ('candidate-1', 1, 'correct', None), ('candidate-2', 2, 'correct', None),
        ('release-before-disposal', 1, 'early-retire', 'RetirementRetainsCapacity'),
        ('never-release-after-disposal', 1, 'leak-retire', 'StoppedOwnsNoPermit'),
        ('double-return-at-disposal', 1, 'double-stop', 'Accounting'),
        ('successful-reuse-witness', 1, 'correct', 'NoSecondCall'),
    ]:
        run = args.output / name
        run.mkdir()
        shutil.copyfile(HERE / 'PermitRetirement.tla', run / 'PermitRetirement.tla')
        invariants = 'TypeOK Accounting LeaseOwnsPermit RetirementRetainsCapacity StoppedOwnsNoPermit LiveCapacity'
        if violation == 'NoSecondCall':
            invariants += ' NoSecondCall'
        (run / 'PermitRetirement.cfg').write_text(
            f'INIT Init\nNEXT Next\nCONSTANTS Capacity = {capacity}\n Policy = "{policy}"\nINVARIANTS {invariants}\n')
        result = subprocess.run(['java', '-XX:+UseSerialGC', '-Xms32m', '-Xmx384m', '-cp', str(args.jar.resolve()),
                                 'tlc2.TLC', '-workers', '1', '-metadir', str((run / 'states').resolve()),
                                 '-deadlock', 'PermitRetirement.tla'], cwd=run, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, timeout=90)
        (run / 'tlc.log').write_text(result.stdout)
        if violation:
            assert result.returncode == 12, result.stdout
            assert f'Invariant {violation} is violated' in result.stdout, result.stdout
            assert 'The behavior up to this point is:' in result.stdout, result.stdout
        else:
            assert result.returncode == 0, result.stdout
            assert 'Model checking completed. No error has been found.' in result.stdout, result.stdout
        counts = re.search(r'([\d,]+) states generated, ([\d,]+) distinct states found', result.stdout)
        assert counts, result.stdout
        cases.append({'name': name, 'capacity': capacity, 'policy': policy, 'expectedViolation': violation,
                      'generatedStates': int(counts[1].replace(',', '')), 'distinctStates': int(counts[2].replace(',', ''))})
    receipt = {'modelSha256': digest(HERE / 'PermitRetirement.tla'), 'checkerSha256': digest(Path(__file__)),
               'toolchain': tool, 'cases': cases,
               'boundary': 'Finite protocol for capacities 1 and 2, Capacity+1 worker identities, and saturated completed-call witness. Models startup, healthy reuse, cancellation, native disposal and disposal preceding retirement. Not source refinement, Rust/Tokio semantics, native interruption liveness, idle-worker failure or panic/poison proof.'}
    (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
