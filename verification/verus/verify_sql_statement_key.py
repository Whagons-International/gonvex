#!/usr/bin/env python3
"""Extract actual SQL append bodies; verify bytes/identity and reject mutations.

Run under the local resource wrapper on arch3. Does not compile, use a database,
benchmark, edit production, or import SQLx. CI uses the existing pinned Verus job.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

VERSION = '0.2026.10.18.7d82994'
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FORBIDDEN = r'\b(assume|admit|axiom|external_body|external_fn_specification|assume_specification|unsafe|inline_air)\b'
DECLS = {
    'decimal': 'fn append_decimal(key: &mut String, oid: u32) {',
    'execution': 'fn append_execution(key: &mut String, sql: &str, oids: &[u32]) {',
    'description': 'fn append_description(key: &mut String, sql: &str) {',
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def once(text, before, after):
    if text.count(before) != 1:
        raise RuntimeError('extraction/mutation target changed: ' + before)
    return text.replace(before, after, 1)


def extract(text, declaration):
    if text.count(declaration) != 1:
        raise RuntimeError('production signature changed: ' + declaration)
    start = text.index(declaration)
    code = text[start:text.index('\n}', start) + 2]
    if re.search(FORBIDDEN, code):
        raise RuntimeError('unexpected trust escape')
    return code


def annotate(bodies):
    # Every transformation is an insertion, and its exact inverse is checked.
    out = dict(bodies)
    inserts = {
        'decimal': [(') {', ''')
    ensures final(key)@ == old(key)@ + decimal(oid as nat)
{
    let ghost initial = key@;
    proof {
        reveal_with_fuel(decimal, 11);
        assert(decimal(oid as nat).len() <= 10);
    }'''),
            ('while remain > 0 {', '''while remain > 0
        invariant 0 <= index < 10,
            decimal(oid as nat).len() <= 10,
            decimal(oid as nat) ==
                (if remain == 0 { Seq::<char>::empty() } else { decimal(remain as nat) })
                    + buffer@.subrange(index as int, 10),
        decreases remain
    {
        proof {
            assert(decimal(remain as nat).len() >= 1) by {
                if remain >= 10 { reveal_with_fuel(decimal, 2); }
            }
            assert(index > 0);
        }
        let ghost previous = buffer@;
        let ghost old_index = index;
        let ghost old_remain = remain;'''),
            ('        remain /= 10;', '''        proof {
            assert(buffer@ =~= previous.update(index as int, digit((old_remain % 10) as int)));
            assert(buffer@.subrange(index as int, 10) =~=
                seq![digit((old_remain % 10) as int)] + previous.subrange(old_index as int, 10));
            assert(decimal(old_remain as nat) ==
                (if old_remain / 10 == 0 { Seq::<char>::empty() }
                 else { decimal((old_remain / 10) as nat) })
                    + seq![digit((old_remain % 10) as int)]);
        }
        remain /= 10;'''),
            ('    while index < 10 {', '''    let ghost start = index;
    while index < 10
        invariant start <= index <= 10,
            decimal(oid as nat) == buffer@.subrange(start as int, 10),
            key@ == initial + buffer@.subrange(start as int, index as int),
        decreases 10 - index
    {'''),
            ('        index += 1;', '''        proof {
            assert(buffer@.subrange(start as int, index as int + 1) =~=
                buffer@.subrange(start as int, index as int) + seq![buffer@[index as int]]);
        }
        index += 1;''')],
        'execution': [(') {', ''')
    ensures final(key)@ == old(key)@ + execution(sql@, oids@)
{
    let ghost initial = key@;'''),
            ('while index < oids.len() {', '''while index < oids.len()
        invariant index <= oids.len(),
            key@ == initial + "/* gonvex.parameter-types.v1:"@ + tokens(oids@.subrange(0, index as int)),
        decreases oids.len() - index
    {
        proof {
            tokens_push(oids@.subrange(0, index as int), oids@[index as int]);
            assert(oids@.subrange(0, index as int + 1) =~=
                oids@.subrange(0, index as int).push(oids@[index as int]));
        }'''),
            ('    key.push_str(" */\\n");', '''    proof { assert(oids@.subrange(0, oids.len() as int) =~= oids@); }
    key.push_str(" */\\n");''')],
        'description': [(') {', ''')
    ensures final(key)@ == old(key)@ + description_spec(sql@)
{''')],
    }
    for name, changes in inserts.items():
        before, after = changes[0]
        header = bodies[name].splitlines()[0]
        changes[0] = (header, header[:-3] + after)
        for before, after in changes:
            out[name] = once(out[name], before, after)
        inverse = out[name]
        for before, after in reversed(changes):
            inverse = once(inverse, after, before)
        if inverse != bodies[name]:
            raise RuntimeError('annotation modified executable body')
    return out


def build(bodies, specs, laws):
    annotated = annotate(bodies)
    return ('use vstd::prelude::*;\nverus! {\n' + specs + '\n'
            + '\n'.join(annotated.values()) + '\n' + laws + '\n}\n')


MUTATIONS = [
    ('ignore_sql', 'execution', '    key.push_str(sql);', ''),
    ('omit_first_oid', 'execution', 'append_decimal(key, oids[index]);',
        'if index != 0 { append_decimal(key, oids[index]); }'),
    ('ignore_oids', 'execution', 'append_decimal(key, oids[index]);', 'append_decimal(key, 0);'),
    ('reverse_oids', 'execution', 'oids[index]', 'oids[oids.len() - 1 - index]'),
    ('missing_comma', 'execution', "    key.push(',');", ''),
    ('empty_zero', 'decimal', "        key.push(buffer[index]);",
        "        if oid != 0 { key.push(buffer[index]); }"),
    ('wrong_digit', 'decimal', "buffer[index] = (b'0' + (oid % 10) as u8) as char;",
        "buffer[index] = (b'1' + (oid % 10) as u8) as char;"),
    ('missing_most_significant', 'decimal', 'let mut remain = oid / 10;', 'let mut remain = 0u32;'),
    ('wrong_ten_boundary', 'decimal', 'let mut remain = oid / 10;',
        'let mut remain = if oid > 10 { oid / 10 } else { 0u32 };'),
    ('overlapping_prefix', 'execution', '/* gonvex.parameter-types.v1:', '/* gonvex.description.v1 */\\n'),
    # Keep the annotation anchor; change the actual body after annotation for these two.
    ('missing_space', 'annotated', '    key.push_str(" */\\n");', '    key.push_str("*/\\n");'),
    ('missing_newline', 'annotated', '    key.push_str(" */\\n");', '    key.push_str(" */");'),
    ('description_no_prefix', 'description', '/* gonvex.description.v1 */\\n', ''),
    ('description_no_sql', 'description', '    key.push_str(sql);', ''),
]


def delimiter_witness(bodies, specs, broken):
    # Use exact contracts for the mutated output, so the final identity law,
    # rather than the canonical-output contract, is the failing assertion.
    small = specs[:specs.index('proof fn decimal_facts')] + extract(specs, 'proof fn tokens_push(')
    aseq = "seq!['1', '2', '3']" if broken else "seq!['1', ',', '2', '3', ',']"
    bseq = "seq!['1', '2', '3']" if broken else "seq!['1', '2', ',', '3', ',']"
    witness = """fn delimiter_identity_witness() {
    let a = [1u32, 23u32];
    let b = [12u32, 3u32];
    let mut x = String::new();
    let mut y = String::new();
    append_execution(&mut x, "", &a);
    append_execution(&mut y, "", &b);
    proof { reveal_with_fuel(tokens, 4); reveal_with_fuel(decimal, 4); }
    assert(a@ =~= seq![1u32, 23u32]);
    assert(b@ =~= seq![12u32, 3u32]);
    assert(tokens(a@) =~= ASEQ);
    assert(tokens(b@) =~= BSEQ);
    let ghost offset = "/* gonvex.parameter-types.v1:"@.len();
    assert(x@[offset as int + 1] == tokens(a@)[1]);
    assert(y@[offset as int + 1] == tokens(b@)[1]);
    assert(x@ != y@);
}
""".replace('ASEQ', aseq).replace('BSEQ', bseq)
    harness = build(bodies, small, witness)
    if broken:
        if harness.count("    key.push(',');") != 1:
            raise RuntimeError('comma collision mutation target changed')
        harness = harness.replace(" + seq![',']", '').replace("    key.push(',');", '')
    return harness


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--source', type=Path, default=ROOT / 'rust/crates/runtime/src/sql_statement_key.rs')
    p.add_argument('--verus', default=os.environ.get('VERUS_BIN') or shutil.which('verus'))
    p.add_argument('--redactor', default=shutil.which('coach-redact'))
    args = p.parse_args()
    if not args.verus:
        p.error('pass the pinned --verus')
    if args.output.exists():
        p.error('fresh output directory required')
    args.output.mkdir(parents=True)
    inputs = [args.source, Path(__file__), HERE / 'sql_statement_key_laws.rs', HERE / 'sql_statement_key_body_laws.rs']
    hashes = {str(path.resolve()): sha(path) for path in inputs}
    source = args.source.read_text()
    bodies = {name: extract(source, decl) for name, decl in DECLS.items()}
    specs = inputs[2].read_text()
    laws = inputs[3].read_text()
    if re.search(FORBIDDEN, specs + laws):
        raise RuntimeError('unexpected proof trust escape')
    # Allocation and packaging are explicitly outside the theorem but fail closed on drift.
    wrapper = extract(source, "pub(crate) fn statement<'a>(")
    if wrapper != """pub(crate) fn statement<'a>(sql: &'a str, oids: &[u32]) -> Cow<'a, str> {
    let mut key = String::with_capacity(sql.len() + 40 + oids.len() * 4);
    append_execution(&mut key, sql, oids);
    Cow::Owned(key)
}""":
        raise RuntimeError('review unverified allocation/Cow boundary')
    description_wrapper = extract(source, 'pub(crate) fn description(')
    if description_wrapper != """pub(crate) fn description(sql: &str) -> String {
    let mut key = String::with_capacity("/* gonvex.description.v1 */\\n".len() + sql.len());
    append_description(&mut key, sql);
    key
}""":
        raise RuntimeError('review unverified description allocation boundary')

    def command(argv):
        # Local outputs, including tool identity and failures, cross the redactor first.
        if args.redactor:
            argv = [args.redactor, 'run', '--', *argv]
        result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, timeout=240)
        return result.returncode, result.stdout

    version_exit, version = command([args.verus, '--version'])
    if version_exit or VERSION not in version:
        raise RuntimeError('wrong Verus pin: ' + version)
    (args.output / 'version.log').write_text(version)
    variants = [('production_candidate', build(bodies, specs, laws), True)]
    for name, target, before, after in MUTATIONS:
        if target == 'annotated':
            harness = once(build(bodies, specs, laws), before, after)
        else:
            changed = dict(bodies)
            changed[target] = once(changed[target], before, after)
            harness = build(changed, specs, laws)
        variants.append((name, harness, False))
    variants.extend([
        ('comma_identity_witness', delimiter_witness(bodies, specs, False), True),
        ('comma_collision_control', delimiter_witness(bodies, specs, True), False),
    ])
    receipt = {
        'success': False, 'verus': version.strip(), 'sourceHashes': hashes,
        'extractedBodyHashes': {name: hashlib.sha256(code.encode()).hexdigest() for name, code in bodies.items()},
        'expectedChecks': len(variants), 'checks': [],
        'scope': 'Actual extracted append bodies: canonical unsigned decimal, exact UTF-8 bytes, full SQL plus ordered u32 OID identity, description identity, namespace separation.',
        'trustBoundaries': ['Unverified allocation/reservation/Cow wrappers', 'Pinned vstd String/slice/UTF-8 contracts, Verus, SMT, compiler', 'SQLx produces/type_info/codec relation and same-owned-plan caller discipline', 'Postgres/cache semantics, all other callers', 'Old Rust Display compatibility and runtime performance require parent native checks'],
    }
    for name, harness, expected in variants:
        path = args.output / (name + '.rs')
        path.write_text(harness)
        cmd = [args.verus, '--edition=2021', '--crate-type=lib', '--no-cheating', '--num-threads', '1', str(path)]
        tc_exit, tc = command([*cmd, '--no-verify'])
        (args.output / (name + '.typecheck.log')).write_text(tc)
        if tc_exit:
            raise RuntimeError(name + ' is not a well-typed control')
        code, log = command(cmd)
        (args.output / (name + '.log')).write_text(log)
        summaries = re.findall(r'verification results.*', log)
        counts = re.search(r'verification results.*?([0-9]+) verified, ([0-9]+) errors', log)
        if expected:
            passed = code == 0 and counts is not None and int(counts[1]) > 0 and counts[2] == '0'
        else:
            # Reject syntax, unsupported APIs, safety failures and solver timeouts.
            errors = re.findall(r'^error: (.*)', log, re.M)
            accepted = (('assertion failed',) if name == 'comma_collision_control' else
                        ('postcondition not satisfied', 'invariant not satisfied at end of loop', 'invariant not satisfied before loop'))
            passed = (code != 0 and counts is not None and int(counts[2]) > 0
                      and any(e.startswith(accepted) for e in errors)
                      and all(e.startswith(accepted) or e.startswith('aborting due to') for e in errors)
                      and not re.search(r'timed out|resource limit|unsupported|precondition not satisfied', log))
        if name == 'comma_collision_control':
            passed = passed and 'assert(x@ != y@)' in log and int(counts[2]) == 1
        check = {'name': name, 'expectedToVerify': expected, 'typecheckExit': tc_exit,
                 'exit': code, 'passed': passed, 'harnessSha256': sha(path), 'summary': summaries}
        receipt['checks'].append(check)
        (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print(name + ': ' + ', '.join(summaries), flush=True)
        if not passed:
            raise RuntimeError(name + ' had an unexpected result; inspect redacted log')
    for path in inputs:
        if sha(path) != hashes[str(path.resolve())]:
            raise RuntimeError('input changed during verification: ' + str(path))
    receipt['success'] = True
    (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print('Actual append bodies verified; ' + str(sum(not expected for _, _, expected in variants)) + ' well-typed broken variants rejected.')


if __name__ == '__main__':
    main()
