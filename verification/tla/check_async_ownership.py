#!/usr/bin/env python3
"""Bounded await/activation schedules, with named negative controls."""
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
    parser.add_argument('--jar', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    tool = json.loads((HERE / 'toolchain.json').read_text())
    assert digest(args.jar) == tool['sha256'], 'TLC checksum mismatch'
    args.output.mkdir(parents=True, exist_ok=False)
    java = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, text=True).strip()
    cases = []
    for name, maximum, policy, violation in [
        ('candidate-2', 2, 'correct', None), ('candidate-3', 3, 'correct', None),
        ('omit-read', 2, 'omit-read', 'AdmissionSafety'),
        ('omit-write', 2, 'omit-write', 'PublicationSafety'),
        ('omit-rebase', 2, 'omit-rebase', 'PublicationSafety'),
        ('scope-only', 2, 'scope-only', 'AdmissionSafety'),
        ('reroute-journal', 2, 'reroute-journal', 'JournalIsolation'),
        ('positive-publication-witness', 2, 'correct', 'NoSuccessfulPublication'),
    ]:
        run = args.output / name
        run.mkdir()
        shutil.copyfile(HERE / 'AsyncOwnership.tla', run / 'AsyncOwnership.tla')
        invariants = 'TypeOK AdmissionSafety PublicationSafety JournalIsolation'
        if violation == 'NoSuccessfulPublication':
            invariants += ' NoSuccessfulPublication'
        (run / 'AsyncOwnership.cfg').write_text(
            f'INIT Init\nNEXT Next\nCONSTANTS MaxGeneration = {maximum}\n Policy = "{policy}"\nINVARIANTS {invariants}\n')
        result = subprocess.run(['java', '-XX:+UseSerialGC', '-Xms32m', '-Xmx384m', '-cp', str(args.jar.resolve()),
                                 'tlc2.TLC', '-workers', '1', '-metadir', str((run / 'states').resolve()),
                                 '-config', 'AsyncOwnership.cfg', 'AsyncOwnership.tla'],
                                cwd=run, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=90)
        (run / 'tlc.log').write_text(result.stdout)
        if violation:
            assert result.returncode == 12, f'{name}: expected safety trace, got {result.returncode}: {result.stdout}'
            assert f'Invariant {violation} is violated' in result.stdout, result.stdout
            assert 'The behavior up to this point is:' in result.stdout, result.stdout
        else:
            assert result.returncode == 0, result.stdout
            assert 'Model checking completed. No error has been found.' in result.stdout, result.stdout
        counts = re.search(r'([\d,]+) states generated, ([\d,]+) distinct states found', result.stdout)
        assert counts, result.stdout
        cases.append({'name': name, 'maxGeneration': maximum, 'policy': policy, 'expectedViolation': violation,
                      'generatedStates': int(counts[1].replace(',', '')), 'distinctStates': int(counts[2].replace(',', ''))})
    receipt = {'modelSha256': digest(HERE / 'AsyncOwnership.tla'), 'checkerSha256': digest(Path(__file__)),
               'toolchain': tool, 'javaVersion': java, 'cases': cases,
               'boundary': 'Finite model of one captured operation, two owners and bounded fresh activations. Each completion/publication action is one synchronous JS segment. Not source refinement, generation allocation proof, multi-operation interference, storage atomicity, authentication admission or liveness.'}
    (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
