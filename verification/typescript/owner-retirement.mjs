import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const compact = (node, file) => node.getText(file).replace(/\s/g, '');
const originalId = 'entry.id';
const mappedId = 'this.optimisticOutboxEntryIds.get(entry.idempotencyKey)';
// Translate only this helper's closed grammar. Unknown statements or expressions
// fail, including side effects in a guard or an await before volatile cleanup.
export function extractRetirement(source) {
  const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'GonvexClient');
  assert.equal(classes.length, 1);
  const methods = classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name?.getText(file) === 'retireOutboxEntry');
  assert.equal(methods.length, 1);
  const method = methods[0];
  assert.deepEqual(method.modifiers?.map(m => m.kind), [ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.AsyncKeyword]);
  assert(!method.typeParameters && !method.asteriskToken && !method.questionToken);
  assert.deepEqual(method.parameters.map(n => compact(n, file)), [
    'entry:Pick<ReducerOutboxEntry,"id"|"idempotencyKey">', 'current:()=>boolean',
  ]);
  const body = method.body?.statements;
  assert.equal(body?.length, 3);
  // Acceptance bookkeeping is removed only after ACK succeeds, and always
  // uses the captured originating ID. No lookup through a new owner is allowed.
  assert.equal(compact(body[2], file), 'this.knownAcceptedEntries.delete(entry.id);');
  assert(ts.isIfStatement(body[0]) && !body[0].elseStatement && ts.isBlock(body[0].thenStatement));
  assert.deepEqual(body[0].thenStatement.statements.map(n => compact(n, file)), [
    'this.optimisticOutboxEntryIds.delete(entry.idempotencyKey);',
    'this.optimisticReducerIds.delete(entry.idempotencyKey);',
  ]);
  function condition(node) {
    if (ts.isParenthesizedExpression(node)) return condition(node.expression);
    const text = compact(node, file);
    if (text === 'current()') return 'current';
    if (node.kind === ts.SyntaxKind.TrueKeyword) return 'true';
    if (node.kind === ts.SyntaxKind.FalseKeyword) return 'false';
    if (ts.isPrefixUnaryExpression(node) && node.operator === ts.SyntaxKind.ExclamationToken) return `(not ${condition(node.operand)})`;
    if (ts.isBinaryExpression(node)) {
      const op = node.operatorToken.kind;
      if (op === ts.SyntaxKind.AmpersandAmpersandToken || op === ts.SyntaxKind.BarBarToken)
        return `(${op === ts.SyntaxKind.AmpersandAmpersandToken ? 'and' : 'or'} ${condition(node.left)} ${condition(node.right)})`;
      if (op === ts.SyntaxKind.EqualsEqualsEqualsToken && compact(node.left, file) === mappedId && compact(node.right, file) === originalId)
        return '(and mapPresent (= mappedId originalId))';
    }
    throw Error('unsupported retirement guard');
  }
  assert(ts.isExpressionStatement(body[1]) && ts.isAwaitExpression(body[1].expression));
  const call = body[1].expression.expression;
  assert(ts.isCallExpression(call) && compact(call.expression, file) === 'this.reducerOutbox.ack' && call.arguments.length === 1 && !call.typeArguments);
  const target = compact(call.arguments[0], file);
  assert([originalId, `${mappedId}??${originalId}`].includes(target), 'unsupported retirement target');
  return {
    clears: condition(body[0].expression),
    target: target === originalId ? 'originalId' : '(ite (and mapPresent (not clears)) mappedId originalId)',
    source: method.getText(file),
  };
}
const laws = {
  'old-owner-cannot-clear-live-state': '(=> (not current) (not clears))',
  'different-entry-cannot-clear-live-state': '(=> (or (not mapPresent) (not (= mappedId originalId))) (not clears))',
  'current-matching-entry-can-clear': '(=> (and current mapPresent (= mappedId originalId)) clears)',
  'retirement-target-is-originating-entry': '(= target originalId)',
};
const hash = x => createHash('sha256').update(x).digest('hex');
export function verifyRetirement({ z3, output }) {
  mkdirSync(output, { recursive: true });
  const source = readFileSync(new URL('../../packages/client/src/index.ts', import.meta.url), 'utf8');
  function solve(candidate, name) {
    const extracted = extractRetirement(candidate);
    const smt = `(declare-sort EntryId 0)\n(declare-const originalId EntryId)\n(declare-const mappedId EntryId)\n(declare-const mapPresent Bool)\n(declare-const current Bool)\n`
      + `(define-fun clears () Bool ${extracted.clears})\n(define-fun target () EntryId ${extracted.target})\n`
      + Object.values(laws).map(l => `(push) (assert (not ${l})) (check-sat) (pop)`).join('\n') + '\n';
    const path = resolve(output, `${name}.smt2`);
    writeFileSync(path, smt); writeFileSync(resolve(output, `${name}.ts`), extracted.source);
    const result = spawnSync(z3, ['-smt2', path], { encoding: 'utf8', timeout: 15000 });
    assert.ifError(result.error); assert.equal(result.status, 0, result.stderr);
    const answers = result.stdout.trim().split('\n');
    assert.equal(answers.length, Object.keys(laws).length);
    assert(answers.every(a => ['sat', 'unsat'].includes(a)), result.stdout);
    return Object.keys(laws).filter((_, i) => answers[i] === 'sat');
  }
  assert.deepEqual(solve(source, 'candidate'), []);
  const mutations = [
    ['ignore-owner', 'current() && this.optimisticOutboxEntryIds.get(entry.idempotencyKey) === entry.id', 'this.optimisticOutboxEntryIds.get(entry.idempotencyKey) === entry.id'],
    ['ignore-entry', 'current() && this.optimisticOutboxEntryIds.get(entry.idempotencyKey) === entry.id', 'current()'],
    ['never-clear', 'current() && this.optimisticOutboxEntryIds.get(entry.idempotencyKey) === entry.id', 'false'],
    ['reroute-retirement', 'await this.reducerOutbox.ack(entry.id);', 'await this.reducerOutbox.ack(this.optimisticOutboxEntryIds.get(entry.idempotencyKey) ?? entry.id);'],
  ];
  const negativeControls = mutations.map(([name, before, after]) => {
    const method = extractRetirement(source).source;
    assert.equal(method.split(before).length, 2);
    const mutant = source.replace(method, method.replace(before, after));
    const violatedLaws = solve(mutant, name);
    assert(violatedLaws.length > 0, name);
    return { name, violatedLaws };
  });
  const receipt = { sourceSha256: hash(source), verifierSha256: hash(readFileSync(new URL(import.meta.url))), laws: Object.keys(laws), negativeControls,
    boundary: 'Closed grammar of retireOutboxEntry only. Models supplied current() result and optional non-NaN journal-ID equality with uninterpreted IDs. Assumes current() is a pure predicate, ordinary built-in Map/Set effects and outbox ack deletes its supplied ID. Does not prove caller entry provenance, current() closure freshness, storage behavior, callbacks, or full async placement.' };
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log('4 retirement effect laws verified; 4 broken helpers rejected');
  return receipt;
}
