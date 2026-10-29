#!/usr/bin/env python3
"""Explore an explicitly bounded protocol model; never claim a code proof."""
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
    tool = json.loads((HERE / 'toolchain.json').read_text())
    digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
    assert digest(args.jar) == tool['sha256'], 'TLC toolchain checksum mismatch'
    args.output.mkdir(parents=True, exist_ok=True)
    java = subprocess.run(['java', '-version'], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True).stdout.strip()
    rows = []
    for name, revisions, generations, fence, owner, retirement, violation in [
        ('candidate-3-2', 3, 2, True, True, True, None),
        ('candidate-4-2', 4, 2, True, True, True, None),
        ('missing-data-fence', 3, 2, False, True, True, 'CertificateSafety'),
        ('unowned-recovery', 3, 2, True, False, True, 'OldFailureIsolation'),
        ('early-intent-retirement', 3, 2, True, True, False, 'RetirementSafety'),
    ]:
        run = args.output / name
        run.mkdir()
        shutil.copyfile(HERE / 'ReplicaDurability.tla', run / 'ReplicaDurability.tla')
        config = f'''INIT Init
NEXT Next
CONSTANTS
  MaxRevision = {revisions}
  MaxGeneration = {generations}
  FenceDataFailure = {str(fence).upper()}
  OwnFailures = {str(owner).upper()}
  BarrierRetirement = {str(retirement).upper()}
INVARIANTS TypeOK CertificateSafety OldFailureIsolation RetirementSafety
'''
        (run / 'ReplicaDurability.cfg').write_text(config)
        result = subprocess.run(['java', '-XX:+UseSerialGC', '-Xms32m', '-Xmx384m', '-cp', str(args.jar.resolve()),
                                 'tlc2.TLC', '-workers', '1', '-metadir', str((run / 'states').resolve()),
                                 '-config', 'ReplicaDurability.cfg', 'ReplicaDurability.tla'],
                                cwd=run, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        (run / 'tlc.log').write_text(result.stdout)
        if violation:
            assert result.returncode == 12, f'{name}: expected TLC safety violation, got {result.returncode}: {result.stdout}'
            assert f'Invariant {violation} is violated' in result.stdout, result.stdout
            assert 'The behavior up to this point is:' in result.stdout, result.stdout
        else:
            assert result.returncode == 0, result.stdout
            assert 'Model checking completed. No error has been found.' in result.stdout, result.stdout
        counts = re.search(r'([\d,]+) states generated, ([\d,]+) distinct states found', result.stdout)
        assert counts, result.stdout
        rows.append(dict(name=name, maxRevision=revisions, maxGeneration=generations,
                         generatedStates=int(counts[1].replace(',', '')), distinctStates=int(counts[2].replace(',', '')),
                         expectedViolation=violation, exitCode=result.returncode))
    receipt = dict(modelSha256=digest(HERE / 'ReplicaDurability.tla'), checkerSha256=digest(Path(__file__)),
                   toolchain=tool, javaVersion=java, cases=rows, boundary='Exhaustive finite model safety exploration at declared bounds, not an unbounded or source implementation proof. Assumes ordered data/watermarks, atomic writes, replay from the certified checkpoint and a pending command receipt for data position one. No liveness claim.')
    (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))

if __name__ == '__main__':
    main()
