#!/usr/bin/env python3
"""Check actual deque release/admission bodies and deliberate typed mutations."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

HERE = Path(__file__).resolve().parent
VERSION = '0.2026.10.18.7d82994'
QUEUE_IMPORT = 'use std::collections::VecDeque;'
RELEASE_SIGNATURE = 'pub(crate) fn release_acked_prefix(frames: &mut VecDeque<(u64, usize)>, sequence: u64) {'
ADMISSION_SIGNATURE = '''pub(crate) fn admit_frame(
    count: usize,
    bytes: usize,
    frame_bytes: usize,
    frame_limit: usize,
    byte_limit: usize,
) -> Option<(usize, usize)> {'''

RELEASE_CONTRACT = '''
    ensures
        final(frames)@.len() <= old(frames)@.len(),
        final(frames)@ == old(frames)@.skip(old(frames)@.len() - final(frames)@.len()),
        forall|i: int| 0 <= i < old(frames)@.len() - final(frames)@.len() ==>
            old(frames)@[i].0 <= sequence,
        final(frames)@.len() > 0 ==> final(frames)@[0].0 > sequence,
        old(frames)@.len() == old(frames)@.take(old(frames)@.len() - final(frames)@.len()).len()
            + final(frames)@.len(),
        byte_sum(old(frames)@) ==
            byte_sum(old(frames)@.take(old(frames)@.len() - final(frames)@.len()))
            + byte_sum(final(frames)@),
'''
LOOP_CONTRACT = '''
        invariant
            frames@.len() <= before.len(),
            forall|i: int| 0 <= i < frames@.len() ==>
                frames@[i] == before[before.len() - frames@.len() + i],
            forall|i: int| 0 <= i < before.len() - frames@.len() ==>
                before[i].0 <= sequence,
        ensures frames@.len() == 0 || frames@[0].0 > sequence,
        decreases frames@.len(),
'''
RELEASE_POST = '''
    proof {
        let cut = before.len() - frames@.len();
        assert(frames@ =~= before.skip(cut));
        byte_sum_split(before, cut);
    }
'''
ADMISSION_CONTRACT = '''-> (result: Option<(usize, usize)>)
    ensures
        result.is_some() == (count < frame_limit && bytes as int + frame_bytes as int <= byte_limit),
        match result {
            None => true,
            Some((next_count, next_bytes)) =>
                next_count as int == count as int + 1
                && next_bytes as int == bytes as int + frame_bytes as int
                && next_count <= frame_limit && next_bytes <= byte_limit,
        },
{'''
LAWS = '''
fn replay_cannot_release_twice(frames: &mut VecDeque<(u64, usize)>, sequence: u64, replay: u64)
    requires replay <= sequence,
{
    release_acked_prefix(frames, sequence);
    let ghost once = frames@;
    release_acked_prefix(frames, replay);
    proof {
        if once.len() > 0 && frames@.len() < once.len() {
            assert(once[0].0 <= replay);
            assert(false);
        }
        assert(frames@ == once);
        assert(byte_sum(frames@) == byte_sum(once));
    }
}

// Composition of the same production mutation on active and retired queues.
// This does not model or prove the caller's map lookup/routing.
fn two_queue_credit_conservation(
    active: &mut VecDeque<(u64, usize)>,
    retired: &mut VecDeque<(u64, usize)>,
    active_ack: u64,
    retired_ack: u64,
) {
    let ghost old_active = active@;
    let ghost old_retired = retired@;
    release_acked_prefix(active, active_ack);
    release_acked_prefix(retired, retired_ack);
    let ghost removed_active = old_active.take(old_active.len() - active@.len());
    let ghost removed_retired = old_retired.take(old_retired.len() - retired@.len());
    assert(old_active.len() + old_retired.len() ==
        removed_active.len() + removed_retired.len() + active@.len() + retired@.len());
    assert(byte_sum(old_active) + byte_sum(old_retired) ==
        byte_sum(removed_active) + byte_sum(removed_retired) + byte_sum(active@) + byte_sum(retired@));
}
'''

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def replace_once(source, before, after):
    if source.count(before) != 1:
        raise RuntimeError('source marker changed: ' + before)
    return source.replace(before, after, 1)

def item(source, signature):
    if source.count(signature) != 1:
        raise RuntimeError('expected exactly one signature: ' + signature)
    start = source.index(signature)
    end = source.index('\n}', start) + 2
    code = source[start:end]
    if re.search(r'\b(assume|admit|axiom|external_body|external_fn_specification|unsafe)\b', code):
        raise RuntimeError('unexpected trusted escape')
    return code

def annotate(release, admission, specs):
    # Every insertion is ghost/spec syntax. The loop and executable body are
    # copied from production, including indexing and the real pop_front call.
    release = replace_once(release, RELEASE_SIGNATURE,
        RELEASE_SIGNATURE[:-1] + RELEASE_CONTRACT + '{\n    let ghost before = frames@;')
    release = replace_once(release, '    while frames.len() > 0 {',
        '    while frames.len() > 0' + LOOP_CONTRACT + '    {')
    release = release[:-2] + RELEASE_POST + '\n}'
    admission = replace_once(admission, '-> Option<(usize, usize)> {', ADMISSION_CONTRACT)
    return ('use vstd::prelude::*;\n' + QUEUE_IMPORT + '\nverus! {\n'
            + specs + '\n' + release + '\n' + admission + '\n' + LAWS + '\n}\n')

def check_call_sites(source):
    source = source.split('#[cfg(test)]', 1)[0]
    for expected in ['release_acked_prefix(frames, sequence);',
                     'release_acked_prefix(&mut pending.in_flight, sequence);',
                     'let Some((next_count, next_bytes)) = admit_frame(',
                     'count = next_count;', 'bytes = next_bytes;']:
        if source.count(expected) != 1:
            raise RuntimeError('delivery call site changed: ' + expected)
    if 'frames.pop_front()' in source or 'in_flight.pop_front()' in source:
        raise RuntimeError('unshared release mutation in delivery')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=HERE.parents[1])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verus', default=os.environ.get('VERUS_BIN') or shutil.which('verus'))
    parser.add_argument('--only', help='run only the named check for local iteration')
    args = parser.parse_args()
    if not args.verus:
        parser.error('pass --verus with the pinned verifier')
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'receipt.json').unlink(missing_ok=True)
    paths = [args.root / 'rust/crates/runtime/src/snapshot_ack.rs',
             args.root / 'rust/crates/runtime/src/snapshot_delivery.rs',
             HERE / 'credit_specs.rs', Path(__file__)]
    hashes = {str(path.resolve()): digest(path) for path in paths}
    source, delivery, specs = [path.read_text() for path in paths[:3]]
    if source.count(QUEUE_IMPORT) != 1:
        raise RuntimeError('production queue type import changed')
    check_call_sites(delivery)
    release = item(source, RELEASE_SIGNATURE)
    admission = item(source, ADMISSION_SIGNATURE)
    if re.search(r'\b(assume|admit|axiom|external_body|external_fn_specification|unsafe)\b', specs):
        raise RuntimeError('unexpected trusted escape in specification')
    version = subprocess.check_output([args.verus, '--version'], text=True).strip()
    if VERSION not in version:
        raise RuntimeError('wrong Verus version: ' + version)
    controls = [
        ('production', release, admission, True),
        ('retain_equal_ack', replace_once(release, 'sent > sequence', 'sent >= sequence'), admission, False),
        ('release_unacked', replace_once(release, 'sent > sequence', 'sent > sequence && false'), admission, False),
        ('pop_wrong_end', replace_once(release, 'frames.pop_front()', 'frames.pop_back()'), admission, False),
        ('release_only_one', replace_once(release, 'frames.pop_front();', 'frames.pop_front();\n        break;'), admission, False),
        ('corrupt_retained_bytes', replace_once(release, '        if sent > sequence {\n            break;',
            '        if sent > sequence {\n            frames.pop_front();\n            frames.push_front((sent, 0));\n            break;'), admission, False),
        ('admit_extra_frame', release, replace_once(admission, 'count >= frame_limit', 'count > frame_limit'), False),
        ('admit_extra_bytes', release, replace_once(admission, 'frame_bytes > byte_limit - bytes', 'frame_bytes > byte_limit'), False),
        ('lose_frame_credit', release, replace_once(admission, 'count + 1', 'count'), False),
        ('lose_byte_credit', release, replace_once(admission, 'bytes + frame_bytes', 'bytes'), False),
    ]
    if args.only:
        controls = [control for control in controls if control[0] == args.only]
        if not controls:
            parser.error('unknown check name')
    receipt = {'success': False, 'verus': version, 'sourcesSha256': hashes,
               'releaseExecutableSha256': hashlib.sha256(release.encode()).hexdigest(),
               'admissionExecutableSha256': hashlib.sha256(admission.encode()).hexdigest(),
               'expectedChecks': len(controls), 'partial': args.only is not None, 'checks': [],
               'boundary': 'Actual extracted VecDeque<(u64, usize)> mutation and admission arithmetic; ghost sums and two-queue composition. Routing, aggregate sums, locks, transport and protocol remain unproved.'}
    for name, mutated_release, mutated_admission, expected in controls:
        code = annotate(mutated_release, mutated_admission, specs)
        path = args.output / (name + '.rs')
        path.write_text(code)
        command = [args.verus, '--edition=2021', '--crate-type=lib', '--num-threads', '1', str(path)]
        started = time.monotonic()
        # Refuse to count parse/type/translation errors as a detected false law.
        typed = subprocess.run(command[:-1] + ['--no-verify', str(path)],
                               capture_output=True, text=True, timeout=40)
        (args.output / (name + '.typecheck.log')).write_text(typed.stdout + typed.stderr)
        if typed.returncode != 0:
            raise RuntimeError(name + ' does not typecheck')
        result = subprocess.run(command, capture_output=True, text=True, timeout=45)
        log = result.stdout + result.stderr
        (args.output / (name + '.log')).write_text(log)
        summary = re.findall(r'verification results.*', log)
        passed = (result.returncode == 0 and re.search(r'[1-9][0-9]* verified, 0 errors', log) is not None
                  if expected else result.returncode != 0 and re.search(r'verification results.*[1-9][0-9]* errors', log) is not None)
        receipt['checks'].append({'name': name, 'expectedToVerify': expected,
            'typecheckExit': typed.returncode, 'exit': result.returncode, 'passed': passed,
            'seconds': time.monotonic() - started, 'harnessSha256': digest(path), 'summary': summary})
        (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        if not passed:
            raise RuntimeError(name + ' had an unexpected result')
    for path, hashed in hashes.items():
        if digest(Path(path)) != hashed:
            raise RuntimeError('input changed during verification: ' + path)
    receipt['success'] = True
    (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print('All ' + str(len(controls)) + ' requested proof outcomes passed.')

if __name__ == '__main__':
    main()
