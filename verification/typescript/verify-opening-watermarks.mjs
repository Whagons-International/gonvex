import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { verifyWatermarkPublication } from './watermark-publication.mjs';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const sourcePath = fileURLToPath(new URL('../../packages/client/src/index.ts', import.meta.url));
const compact = (node, file) => node.getText(file).replace(/\s/g, '');
const inputs = [
  ['scope', 'Scope'], ['currentScope', 'Scope'], ['generation', 'Activation'], ['currentGeneration', 'Activation'],
  ['closed', 'Bool'], ['connected', 'Bool'], ['opening', 'Bool'], ['registered', 'Ref'], ['subscription', 'Ref'],
  ['listeners', 'Int'], ['ready', 'Bool'], ['epoch', 'Epoch'], ['readyEpoch', 'Epoch'],
  ['readyRevision', 'Int'], ['revision', 'Int'],
];
const atoms = new Map([
  ['deferred.scope', ['Scope', 'scope']], ['this.replicaScope', ['Scope', 'currentScope']],
  ['deferred.generation', ['Activation', 'generation']], ['this.socketGeneration', ['Activation', 'currentGeneration']],
  ['this.manuallyClosed', ['Bool', 'closed']], ['this.isWebSocketConnected', ['Bool', 'connected']],
  ['this.replicaSubscriptions.get(subscription.key)', ['Ref', 'registered']], ['subscription', ['Ref', 'subscription']],
  ['subscription.listeners.size', ['Int', 'listeners']], ['ready', ['Bool', 'ready']],
  ['epoch', ['Epoch', 'epoch']], ['undefined', ['Epoch', 'noEpoch']], ['ready.epoch', ['Epoch', 'readyEpoch']],
  ['ready.revision', ['Int', 'readyRevision']], ['deferred.revision', ['Int', 'revision']],
]);

/** Translate the actual three guards and blocker loop, refusing unreviewed control flow. */
export function extractOpeningWatermarks(source) {
  const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'GonvexClient');
  assert.equal(classes.length, 1);
  const methods = name => classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name.getText(file) === name);
  const drain = methods('drainOpeningReplicaWatermark'); assert.equal(drain.length, 1);
  const method = drain[0];
  assert.deepEqual(method.modifiers?.map(n => n.kind), [ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.AsyncKeyword]);
  assert(!method.asteriskToken && !method.questionToken && !method.typeParameters && !method.type && method.body);
  assert.equal(method.parameters.length, 0);
  const statements = method.body.statements; assert.equal(statements.length, 7, 'watermark drain control flow changed');
  assert.equal(compact(statements[0], file), 'constdeferred=this.openingReplicaWatermark;');
  assert.equal(compact(statements[1], file), 'if(!deferred)return;');
  const invalid = statements[2], opening = statements[3], loop = statements[4];
  assert(ts.isIfStatement(invalid) && !invalid.elseStatement);
  assert.equal(compact(invalid.thenStatement, file), '{this.openingReplicaWatermark=undefined;return;}');
  assert(ts.isIfStatement(opening) && !opening.elseStatement);
  assert.equal(compact(opening.thenStatement, file), 'return;');
  // This exact aggregate binds the Boolean opening input to every live subscription.
  assert.equal(compact(opening.expression, file), '[...this.replicaSubscriptions.values()].some(subscription=>subscription.listeners.size>0&&(subscription.opening||!subscription.isUpToDate))');
  assert(ts.isForOfStatement(loop) && !loop.awaitModifier);
  assert.equal(compact(loop.initializer, file), 'const[subscription,epoch]');
  assert.equal(compact(loop.expression, file), 'deferred.blockers');
  assert(ts.isBlock(loop.statement));
  const body = loop.statement.statements; assert.equal(body.length, 3);
  const skip = body[0], block = body[2];
  assert(ts.isIfStatement(skip) && !skip.elseStatement && ts.isContinueStatement(skip.thenStatement));
  assert.equal(compact(body[1], file), 'constready=subscription.readyCursor;');
  assert(ts.isIfStatement(block) && !block.elseStatement);
  assert.equal(compact(block.thenStatement, file), 'return;');
  assert.equal(compact(statements[5], file), 'this.openingReplicaWatermark=undefined;');
  assert.equal(compact(statements[6], file), 'awaitthis.persistReplicaWatermark(deferred.revision);');

  function expression(node) {
    if (ts.isParenthesizedExpression(node)) return expression(node.expression);
    const key = compact(node, file);
    if (atoms.has(key)) { const [sort, smt] = atoms.get(key); return { sort, smt }; }
    if (node.kind === ts.SyntaxKind.TrueKeyword || node.kind === ts.SyntaxKind.FalseKeyword)
      return { sort: 'Bool', smt: key };
    if (ts.isNumericLiteral(node)) { assert.equal(node.text, '0'); return { sort: 'Int', smt: '0' }; }
    if (ts.isPrefixUnaryExpression(node)) {
      assert.equal(node.operator, ts.SyntaxKind.ExclamationToken);
      const value = expression(node.operand); assert.equal(value.sort, 'Bool');
      return { sort: 'Bool', smt: `(not ${value.smt})` };
    }
    if (ts.isConditionalExpression(node)) {
      const condition = expression(node.condition), yes = expression(node.whenTrue), no = expression(node.whenFalse);
      assert.equal(condition.sort, 'Bool'); assert.equal(yes.sort, no.sort);
      return { sort: yes.sort, smt: `(ite ${condition.smt} ${yes.smt} ${no.smt})` };
    }
    assert(ts.isBinaryExpression(node), `unsupported watermark expression ${key}`);
    const a = expression(node.left), b = expression(node.right); assert.equal(a.sort, b.sort);
    const op = node.operatorToken.kind;
    if ([ts.SyntaxKind.BarBarToken, ts.SyntaxKind.AmpersandAmpersandToken].includes(op)) {
      assert.equal(a.sort, 'Bool');
      return { sort: 'Bool', smt: `(${op === ts.SyntaxKind.BarBarToken ? 'or' : 'and'} ${a.smt} ${b.smt})` };
    }
    if ([ts.SyntaxKind.EqualsEqualsEqualsToken, ts.SyntaxKind.ExclamationEqualsEqualsToken].includes(op)) {
      const equality = `(= ${a.smt} ${b.smt})`;
      return { sort: 'Bool', smt: op === ts.SyntaxKind.EqualsEqualsEqualsToken ? equality : `(not ${equality})` };
    }
    assert.equal(a.sort, 'Int');
    assert.equal(op, ts.SyntaxKind.LessThanToken);
    return { sort: 'Bool', smt: `(< ${a.smt} ${b.smt})` };
  }
  const invalidGuard = expression(invalid.expression), skipGuard = expression(skip.expression), blockGuard = expression(block.expression);
  for (const value of [invalidGuard, skipGuard, blockGuard]) assert.equal(value.sort, 'Bool');

  // Source correspondence for the lifecycle boundary that previously retained B's epoch.
  const reset = methods('resetReplicaScopeState'); assert.equal(reset.length, 1);
  const loops = reset[0].body.statements.filter(n => ts.isForOfStatement(n)
    && compact(n.expression, file) === 'this.replicaSubscriptions.values()');
  assert.equal(loops.length, 1); const resetLoop = loops[0];
  assert.equal(compact(resetLoop.initializer, file), 'constsubscription');
  assert(ts.isBlock(resetLoop.statement));
  const clears = resetLoop.statement.statements.filter(n => compact(n, file) === 'subscription.readyCursor=undefined;');
  assert.equal(clears.length, 1, 'scope reset must unconditionally clear old readiness certificates');
  // No later direct write in this method may restore a certificate.
  const writes = [];
  function visit(node) {
    if (ts.isBinaryExpression(node) && compact(node.left, file) === 'subscription.readyCursor') writes.push(compact(node, file));
    ts.forEachChild(node, visit);
  }
  visit(reset[0]); assert.deepEqual(writes, ['subscription.readyCursor=undefined']);
  const parameters = inputs.map(([name, sort]) => `(${name} ${sort})`).join(' ');
  return {
    definition: `(define-fun canDrainOne (${parameters}) Bool (and (not ${invalidGuard.smt}) (not opening) (or ${skipGuard.smt} (not ${blockGuard.smt}))))`,
    reset: 'Every directly iterated replica subscription unconditionally clears readyCursor at scope reset.',
    drainSource: method.getText(file), resetSource: resetLoop.getText(file),
  };
}

const call = `(canDrainOne ${inputs.map(([name]) => name).join(' ')})`;
const active = '(and (= registered subscription) (> listeners 0))';
const validOwner = '(and (= scope currentScope) (= generation currentGeneration) (not closed) connected)';
const adequate = '(and ready (or (= epoch noEpoch) (= readyEpoch epoch)) (>= readyRevision revision))';
const laws = {
  'old-scope-cannot-drain': `(=> (not (= scope currentScope)) (not ${call}))`,
  'old-activation-cannot-drain': `(=> (not (= generation currentGeneration)) (not ${call}))`,
  'closed-cannot-drain': `(=> closed (not ${call}))`,
  'disconnected-cannot-drain': `(=> (not connected) (not ${call}))`,
  'opening-cannot-drain': `(=> opening (not ${call}))`,
  'missing-readiness-blocks': `(=> (and ${active} (not ready)) (not ${call}))`,
  'wrong-epoch-blocks': `(=> (and ${active} (not (= epoch noEpoch)) (not (= readyEpoch epoch))) (not ${call}))`,
  'insufficient-revision-blocks': `(=> (and ${active} (< readyRevision revision)) (not ${call}))`,
  'adequate-current-blocker-progresses': `(=> (and ${validOwner} (not opening) ${active} ${adequate}) ${call})`,
  'removed-blocker-does-not-prevent-progress': `(=> (and ${validOwner} (not opening) (not (= registered subscription))) ${call})`,
  'unsubscribed-blocker-does-not-prevent-progress': `(=> (and ${validOwner} (not opening) (= listeners 0)) ${call})`,
};
const prelude = ['Scope', 'Activation', 'Ref', 'Epoch'].map(s => `(declare-sort ${s} 0)`).join('\n')
  + '\n(declare-const noEpoch Epoch)\n' + inputs.map(([name, sort]) => `(declare-const ${name} ${sort})`).join('\n');
const hash = data => createHash('sha256').update(data).digest('hex');
export const watermarkMutations = [
  ['ignore-scope', 'deferred.scope !== this.replicaScope', 'false'],
  ['ignore-activation', 'deferred.generation !== this.socketGeneration', 'false'],
  ['ignore-close', 'this.manuallyClosed || !this.isWebSocketConnected', 'false || !this.isWebSocketConnected'],
  ['ignore-connection', 'this.manuallyClosed || !this.isWebSocketConnected', 'this.manuallyClosed || false'],
  ['ignore-ready', '!ready || (epoch !== undefined && ready.epoch !== epoch) || ready.revision < deferred.revision',
    'ready ? (epoch !== undefined && ready.epoch !== epoch) || ready.revision < deferred.revision : false'],
  ['ignore-epoch', '(epoch !== undefined && ready.epoch !== epoch)', 'false'],
  ['ignore-revision', 'ready.revision < deferred.revision', 'false'],
  ['deny-all-blockers', '!ready || (epoch !== undefined', 'true || (epoch !== undefined'],
  ['retain-removed-blocker', 'this.replicaSubscriptions.get(subscription.key) !== subscription || subscription.listeners.size === 0', 'false || subscription.listeners.size === 0'],
];

export function verifyOpeningWatermarks({ z3, output }) {
  mkdirSync(output, { recursive: true });
  const version = spawnSync(z3, ['--version'], { encoding: 'utf8' });
  assert.equal(version.status, 0); assert.match(version.stdout, /^Z3 version 4\.16\.0 /);
  const source = readFileSync(sourcePath, 'utf8');
  const extracted = extractOpeningWatermarks(source);
  function solve(candidate, name) {
    const path = resolve(output, `${name}.smt2`);
    const queries = Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n');
    writeFileSync(path, `${prelude}\n${extractOpeningWatermarks(candidate).definition}\n${queries}\n`);
    const result = spawnSync(z3, ['-smt2', path], { encoding: 'utf8', timeout: 15000 });
    assert.ifError(result.error); assert.equal(result.status, 0, result.stderr || result.stdout);
    const answers = result.stdout.trim().split('\n');
    assert.equal(answers.length, Object.keys(laws).length);
    assert(answers.every(a => ['sat', 'unsat'].includes(a)), result.stdout);
    return answers;
  }
  assert(solve(source, 'candidate').every(a => a === 'unsat'));
  const negativeControls = [];
  for (const [name, before, after] of watermarkMutations) {
    // Change only the extracted method: the same guard can occur in other methods.
    assert.equal(extracted.drainSource.split(before).length, 2, name);
    const candidate = source.replace(extracted.drainSource, extracted.drainSource.replace(before, after));
    const answers = solve(candidate, name);
    const violatedLaws = Object.keys(laws).filter((_, i) => answers[i] === 'sat');
    assert(violatedLaws.length, `broken watermark guard escaped: ${name}`);
    negativeControls.push({ name, violatedLaws });
  }
  const retained = source.replace('subscription.readyCursor = undefined;', '');
  assert.notEqual(retained, source);
  assert.throws(() => extractOpeningWatermarks(retained), /scope reset must unconditionally clear/);
  const publication = verifyWatermarkPublication({ z3, output: resolve(output, 'publication') });
  const receipt = {
    publication,
    source: 'packages/client/src/index.ts', sourceSha256: hash(source), verifierSha256: hash(readFileSync(fileURLToPath(import.meta.url))),
    solver: version.stdout.trim(), solverSha256: hash(readFileSync(z3)), parser: ts.version,
    laws: Object.keys(laws), negativeControls, retainedOldReadinessRejected: true, extracted,
    boundary: 'Actual drain guards translated over primitive Booleans, integer revisions and uninterpreted owner/epoch/reference identities. An arbitrary blocker is checked; the unchanged source loop applies that guard to every blocker before persistence. Opening aggregate and reset certificate clearing are structural AST checks. Assumes nonnegative listener counts, ordinary typed fields, no reflective/aliased writes or reentrant property access, and no synchronous owner change between guards and the persistence call. Does not prove blocker capture, asynchronous persistence atomicity, snapshot integrity, storage, the event loop or the complete recovery protocol.',
  };
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log(`${Object.keys(laws).length} opening-watermark laws verified; ${negativeControls.length} broken source guards and retained readiness rejected`);
  return receipt;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2); assert.equal(args.length, 4); assert.equal(args[0], '--z3'); assert.equal(args[2], '--output');
  verifyOpeningWatermarks({ z3: args[1], output: args[3] });
}
