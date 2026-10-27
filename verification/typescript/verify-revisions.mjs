#!/usr/bin/env node
// Source-linked verification of a deliberately small TypeScript subset.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath, pathToFileURL} from 'node:url';

// This is an existing direct, pinned dependency of the CLI, not a runtime SDK dependency.
const require = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url));
const ts = require('typescript-api');
assert.equal(ts.version, '6.0.3');
const ROOT = fileURLToPath(new URL('../../', import.meta.url));
const SOURCE = resolve(ROOT, 'packages/client/src/revision-policy.ts');
const FP = '(_ FloatingPoint 11 53)';
const zero = '((_ to_fp 11 53) RNE 0.0)';
const max = '((_ to_fp 11 53) RNE 9007199254740991.0)';
const signatures = {
  isReplicaRevision: ['revision'], isTransactionRevision: ['revision'],
  canReplaceReplicaField: ['incoming', 'current'],
  canResurrectReplicaRow: ['incoming', 'deleted'],
  canDeleteReplicaRow: ['incoming', 'newest'],
};

/** Reject every construct outside single-return, numeric-parameter Boolean functions.
 * In particular, no ignored statements, calls, imports, defaults, getters or mutations. */
export function extract(source) {
  const file = ts.createSourceFile('revision-policy.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0, 'TypeScript parse error');
  const functions = new Map();
  for (const declaration of file.statements) {
    assert(ts.isFunctionDeclaration(declaration), 'only function declarations are supported');
    assert(declaration.name && declaration.body && !declaration.asteriskToken && !declaration.typeParameters);
    const name = declaration.name.text;
    assert(Object.hasOwn(signatures, name) && !functions.has(name), 'unexpected or duplicate function');
    assert.deepEqual(declaration.modifiers?.map(m => m.kind), [ts.SyntaxKind.ExportKeyword]);
    assert.equal(declaration.type?.kind, ts.SyntaxKind.BooleanKeyword);
    assert.deepEqual(declaration.parameters.map(p => {
      assert(ts.isIdentifier(p.name) && !p.initializer && !p.questionToken && !p.dotDotDotToken && !p.modifiers);
      assert.equal(p.type?.kind, ts.SyntaxKind.NumberKeyword);
      return p.name.text;
    }), signatures[name]);
    assert.equal(declaration.body.statements.length, 1, 'exactly one return required');
    const statement = declaration.body.statements[0];
    assert(ts.isReturnStatement(statement) && statement.expression);
    const parameters = new Set(signatures[name]);
    const expr = node => {
      if (ts.isParenthesizedExpression(node)) return expr(node.expression);
      if (ts.isPrefixUnaryExpression(node)) {
        assert.equal(node.operator, ts.SyntaxKind.ExclamationToken, 'only Boolean negation is supported');
        const value = expr(node.operand);
        assert.equal(value.sort, 'boolean');
        return {sort: 'boolean', smt: `(not ${value.smt})`};
      }
      if (ts.isIdentifier(node)) {
        assert(parameters.has(node.text), 'unknown identifier');
        return {sort: 'number', smt: node.text};
      }
      if (ts.isNumericLiteral(node)) {
        // Only zero is currently needed. Fail closed rather than round unknown literals.
        assert.equal(node.getText(file), '0');
        return {sort: 'number', smt: zero};
      }
      if (node.kind === ts.SyntaxKind.TrueKeyword || node.kind === ts.SyntaxKind.FalseKeyword)
        return {sort: 'boolean', smt: node.kind === ts.SyntaxKind.TrueKeyword ? 'true' : 'false'};
      if (ts.isBinaryExpression(node)) {
        const left = expr(node.left), right = expr(node.right);
        const operators = new Map([
          [ts.SyntaxKind.GreaterThanEqualsToken, ['number', 'fp.geq']],
          [ts.SyntaxKind.GreaterThanToken, ['number', 'fp.gt']],
          [ts.SyntaxKind.LessThanEqualsToken, ['number', 'fp.leq']],
          [ts.SyntaxKind.LessThanToken, ['number', 'fp.lt']],
          [ts.SyntaxKind.AmpersandAmpersandToken, ['boolean', 'and']],
          [ts.SyntaxKind.BarBarToken, ['boolean', 'or']],
        ]);
        const operation = operators.get(node.operatorToken.kind);
        assert(operation, 'unsupported binary operation');
        assert.equal(left.sort, operation[0]); assert.equal(right.sort, operation[0]);
        return {sort: 'boolean', smt: `(${operation[1]} ${left.smt} ${right.smt})`};
      }
      if (ts.isCallExpression(node)) {
        assert(!node.questionDotToken && !node.typeArguments && node.arguments.length === 1);
        const callee = node.expression;
        assert(ts.isPropertyAccessExpression(callee) && !callee.questionDotToken
          && ts.isIdentifier(callee.expression) && callee.expression.text === 'Number'
          && callee.name.text === 'isSafeInteger', 'unsupported call');
        const argument = expr(node.arguments[0]);
        assert.equal(argument.sort, 'number');
        return {sort: 'boolean', smt: `(safe ${argument.smt})`};
      }
      throw new Error(`unsupported syntax ${ts.SyntaxKind[node.kind]}`);
    };
    const result = expr(statement.expression);
    assert.equal(result.sort, 'boolean');
    functions.set(name, `(define-fun ${name} (${signatures[name].map(p => `(${p} ${FP})`).join(' ')}) Bool ${result.smt})`);
  }
  assert.deepEqual([...functions.keys()].sort(), Object.keys(signatures).sort(), 'missing policy function');
  return [...functions.values()].join('\n');
}

// ECMAScript Number is IEEE-754 binary64. No ideal-real approximation is used.
// Number.isSafeInteger is a trusted builtin model, checked against Node below.
const prelude = `(set-option :timeout 10000)
(define-fun finite ((v ${FP})) Bool (and (not (fp.isNaN v)) (not (fp.isInfinite v))))
(define-fun integral ((v ${FP})) Bool (and (finite v) (fp.eq v (fp.roundToIntegral RTZ v))))
(define-fun safe ((v ${FP})) Bool (and (integral v) (fp.leq (fp.abs v) ${max})))
(declare-const x ${FP}) (declare-const y ${FP}) (declare-const z ${FP})`;

const laws = {
  'snapshot-exact-domain': `(= (isReplicaRevision x) (and (safe x) (fp.geq x ${zero})))`,
  'transaction-exact-domain': `(= (isTransactionRevision x) (and (safe x) (fp.gt x ${zero})))`,
  'transaction-is-snapshot-revision': '(=> (isTransactionRevision x) (isReplicaRevision x))',
  'zero-is-snapshot-only': `(and (isReplicaRevision ${zero}) (not (isTransactionRevision ${zero})))`,
  'negative-zero-is-snapshot-only': '(and (isReplicaRevision (_ -zero 11 53)) (not (isTransactionRevision (_ -zero 11 53))))',
  'reject-nan': '(=> (fp.isNaN x) (and (not (isReplicaRevision x)) (not (isTransactionRevision x))))',
  'reject-infinity': '(=> (fp.isInfinite x) (and (not (isReplicaRevision x)) (not (isTransactionRevision x))))',
  'reject-fractions': '(=> (not (integral x)) (and (not (isReplicaRevision x)) (not (isTransactionRevision x))))',
  'reject-negative': `(=> (fp.lt x ${zero}) (and (not (isReplicaRevision x)) (not (isTransactionRevision x))))`,
  'reject-unsafe': `(=> (fp.gt (fp.abs x) ${max}) (and (not (isReplicaRevision x)) (not (isTransactionRevision x))))`,
  'accept-max-safe': `(and (isReplicaRevision ${max}) (isTransactionRevision ${max}))`,
  'field-exact-rejection': '(= (canReplaceReplicaField x y) (not (fp.lt x y)))',
  'resurrection-exact-rejection': '(= (canResurrectReplicaRow x y) (not (fp.leq x y)))',
  'deletion-exact-rejection': '(= (canDeleteReplicaRow x y) (not (fp.lt x y)))',
  'finite-field-order': '(=> (and (finite x) (finite y)) (= (canReplaceReplicaField x y) (fp.geq x y)))',
  'finite-resurrection-order': '(=> (and (finite x) (finite y)) (= (canResurrectReplicaRow x y) (fp.gt x y)))',
  'finite-deletion-order': '(=> (and (finite x) (finite y)) (= (canDeleteReplicaRow x y) (fp.geq x y)))',
  'recover-legacy-nan-field': '(=> (and (isReplicaRevision x) (fp.isNaN y)) (canReplaceReplicaField x y))',
  'recover-legacy-nan-tombstone': '(=> (and (isReplicaRevision x) (fp.isNaN y)) (canResurrectReplicaRow x y))',
  'recover-legacy-nan-deletion': '(=> (and (isReplicaRevision x) (fp.isNaN y)) (canDeleteReplicaRow x y))',
  'field-tie-accepted': '(=> (safe x) (canReplaceReplicaField x x))',
  'deletion-wins-tie': '(=> (safe x) (and (canDeleteReplicaRow x x) (not (canResurrectReplicaRow x x))))',
  'older-field-rejected': '(=> (fp.lt x y) (not (canReplaceReplicaField x y)))',
  'older-deletion-rejected': '(=> (fp.lt x y) (not (canDeleteReplicaRow x y)))',
  'newer-row-resurrects': '(=> (fp.gt x y) (canResurrectReplicaRow x y))',
  'field-order-transitive': '(=> (and (finite x) (finite y) (finite z) (canReplaceReplicaField x y) (canReplaceReplicaField y z)) (canReplaceReplicaField x z))',
};

function solve(z3, input, path) {
  writeFileSync(path, input);
  const result = spawnSync(z3, ['-smt2', path], {encoding: 'utf8', timeout: 15000, maxBuffer: 1024 * 1024});
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const lines = result.stdout.trim().split('\n');
  assert(lines.every(line => line === 'sat' || line === 'unsat'), `unexpected solver result: ${result.stdout}`);
  return lines;
}
const hash = value => createHash('sha256').update(value).digest('hex');
export function verify({z3, output}) {
  mkdirSync(output, {recursive: true});
  const version = spawnSync(z3, ['--version'], {encoding: 'utf8'});
  assert.equal(version.status, 0);
  assert.match(version.stdout, /^Z3 version 4\.16\.0 /, 'use the pinned Verus release solver');
  const source = readFileSync(SOURCE, 'utf8');
  const candidate = extract(source);
  // Each check searches for a counterexample to its law. SAT/unknown/error fails.
  const checks = Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n');
  const actual = solve(z3, `${prelude}\n${candidate}\n${checks}\n`, resolve(output, 'candidate.smt2'));
  assert.equal(actual.length, Object.keys(laws).length);
  assert(actual.every(result => result === 'unsat'), 'policy law has a counterexample');
  // Differential sanity check of the trusted builtin model on edge values and
  // reproducible arbitrary binary64 bit patterns. This is testing, not a proof
  // that the ECMAScript engine implements its specification.
  const values = [0, -0, 1, -1, Number.MIN_VALUE, -Number.MIN_VALUE, Number.MAX_VALUE,
    Number.MAX_SAFE_INTEGER, -Number.MAX_SAFE_INTEGER, 2 ** 53, -(2 ** 53),
    NaN, Infinity, -Infinity, 0.5, -0.5, 1.5, -1.5];
  const buffer = Buffer.alloc(8);
  let bits = 0x123456789abcdef0n;
  for (let i = 0; i < 256; i++) {
    bits = BigInt.asUintN(64, bits * 6364136223846793005n + 1442695040888963407n);
    buffer.writeBigUInt64BE(bits); values.push(buffer.readDoubleBE());
  }
  const vectors = values.map(value => {
    buffer.writeDoubleBE(value);
    return `(= (safe ((_ to_fp 11 53) #x${buffer.toString('hex')})) ${Number.isSafeInteger(value)})`;
  });
  assert.deepEqual(solve(z3, `${prelude}\n(assert (not (and ${vectors.join(' ')})))\n(check-sat)\n`,
    resolve(output, 'builtin-vectors.smt2')), ['unsat']);
  // Well-typed controls must reach the solver and exhibit a real counterexample.
  const controls = [
    ['snapshot-missing-safe', 'Number.isSafeInteger(revision) && revision >= 0', 'revision >= 0'],
    ['transaction-missing-safe', 'Number.isSafeInteger(revision) && revision > 0', 'revision > 0'],
    ['snapshot-excludes-zero', 'revision >= 0', 'revision > 0'],
    ['transaction-allows-zero', 'revision > 0', 'revision >= 0'],
    ['snapshot-allows-negative', 'Number.isSafeInteger(revision) && revision >= 0', 'Number.isSafeInteger(revision)'],
    ['field-excludes-tie', '!(incoming < current)', 'incoming > current'],
    ['field-reversed', '!(incoming < current)', 'incoming <= current'],
    ['resurrects-on-tie', '!(incoming <= deleted)', 'incoming >= deleted'],
    ['resurrection-reversed', '!(incoming <= deleted)', 'incoming < deleted'],
    ['deletion-excludes-tie', '!(incoming < newest)', 'incoming > newest'],
    ['deletion-reversed', '!(incoming < newest)', 'incoming <= newest'],
    ['freezes-nan-fields', '!(incoming < current)', 'incoming >= current'],
    ['freezes-nan-tombstones', '!(incoming <= deleted)', 'incoming > deleted'],
    ['freezes-nan-deletion', '!(incoming < newest)', 'incoming >= newest'],
  ];
  const rejected = [];
  for (const [name, before, after] of controls) {
    assert.equal(source.split(before).length, 2, `unique mutation required: ${name}`);
    const mutated = source.replace(before, after);
    const body = extract(mutated); // Parsing and sort checks must succeed.
    const results = solve(z3, `${prelude}\n${body}\n${checks}\n`, resolve(output, `${name}.smt2`));
    assert.equal(results.length, actual.length);
    assert(results.includes('sat'), `broken variant escaped: ${name}`);
    writeFileSync(resolve(output, `${name}.ts`), mutated);
    rejected.push(name);
  }
  const typecheck = ts.createProgram([SOURCE, ...rejected.map(name => resolve(output, `${name}.ts`))], {
    strict: true, noEmit: true, skipLibCheck: true, types: [], target: ts.ScriptTarget.ES2022,
  });
  const diagnostics = ts.getPreEmitDiagnostics(typecheck);
  assert.equal(diagnostics.length, 0, diagnostics.map(d => ts.flattenDiagnosticMessageText(d.messageText, '\n')).join('\n'));
  const receipt = {source: 'packages/client/src/revision-policy.ts', sourceSha256: hash(source),
    verifierSha256: hash(readFileSync(fileURLToPath(import.meta.url))), parser: ts.version,
    solver: version.stdout.trim(), solverSha256: hash(readFileSync(z3)),
    laws: Object.keys(laws), negativeControls: rejected, builtinTestVectors: values.length,
    boundary: 'Restricted AST translation + binary64 SMT model; not storage, callers, async behavior or full ECMAScript.'};
  writeFileSync(resolve(output, 'source.ts'), source);
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log(`${actual.length} laws verified; ${rejected.length} well-typed broken controls rejected`);
  return receipt;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2);
  assert.equal(args.length, 4, 'usage: node verify-revisions.mjs --z3 /absolute/z3 --output /proof/directory');
  assert.equal(args[0], '--z3'); assert.equal(args[2], '--output');
  verify({z3: resolve(args[1]), output: resolve(args[3])});
}
