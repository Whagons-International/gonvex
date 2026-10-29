#!/usr/bin/env python3
"""Bounded receipt/transaction protocol checking, not a source-code proof."""
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
    args.output.mkdir(parents=True, exist_ok=True)
    cases = []
    for name, clock, exclusive, atomic, head, new_transport, pruning, violation in [
        ('candidate-clock-2', 2, True, True, True, True, True, None),
        ('candidate-clock-3', 3, True, True, True, True, True, None),
        ('new-transport-loses-barrier', 2, True, True, False, True, False, 'ReplyHasBarrier'),
        ('pruned-feed-loses-barrier', 2, True, True, False, False, True, 'ReplyHasBarrier'),
        ('visible-uncommitted-receipt', 2, True, False, True, True, True, 'ReceiptImpliesEffect'),
        ('nonexclusive-claims', 2, False, True, True, True, True, 'AtMostOnce'),
    ]:
        run = args.output / name
        run.mkdir()
        shutil.copyfile(HERE / 'ReducerReceipts.tla', run / 'ReducerReceipts.tla')
        (run / 'ReducerReceipts.cfg').write_text(f'''INIT Init
NEXT Next
CONSTANTS
  MaxClock = {clock}
  ExclusiveClaims = {str(exclusive).upper()}
  AtomicReceipt = {str(atomic).upper()}
  ReplayHead = {str(head).upper()}
  AllowNewTransport = {str(new_transport).upper()}
  AllowPruning = {str(pruning).upper()}
INVARIANTS TypeOK AtMostOnce ReceiptImpliesEffect ReplyHasBarrier CachedResult
''')
        result = subprocess.run(['java', '-XX:+UseSerialGC', '-Xms32m', '-Xmx384m', '-cp', str(args.jar.resolve()),
                                 'tlc2.TLC', '-workers', '1', '-metadir', str((run / 'states').resolve()),
                                 '-config', 'ReducerReceipts.cfg', 'ReducerReceipts.tla'], cwd=run,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
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
        cases.append(dict(name=name, maxClock=clock, actors=2, generatedStates=int(counts[1].replace(',', '')),
                          distinctStates=int(counts[2].replace(',', '')), allowNewTransport=new_transport, allowPruning=pruning, expectedViolation=violation, exitCode=result.returncode))
    receipt = dict(modelSha256=digest(HERE / 'ReducerReceipts.tla'), checkerSha256=digest(Path(__file__)),
                   toolchain=tool, cases=cases, boundary='Finite protocol safety model: one receipt key/namespace, two transport identities, declared clock bounds. Atomic PostgreSQL visibility, exclusive claim behavior and monotonic clock are assumptions connected to native tests. No implementation refinement proof, permanent retention, power-loss or liveness claim.')
    (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print(json.dumps(receipt))

if __name__ == '__main__':
    main()
