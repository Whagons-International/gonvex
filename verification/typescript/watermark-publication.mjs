import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const compact = (node, file) => node.getText(file).replace(/\s/g, '');
const inputs = [
  ['scope', 'Scope'], ['currentScope', 'Scope'], ['generation', 'Activation'],
  ['currentGeneration', 'Activation'], ['activation', 'Activation'], ['currentActivation', 'Activation'],
  ['closed', 'Bool'], ['connected', 'Bool'],
];
const atoms = new Map([
  ['scope', ['Scope', 'scope']], ['this.replicaScope', ['Scope', 'currentScope']],
  ['generation', ['Activation', 'generation']], ['this.socketGeneration', ['Activation', 'currentGeneration']],
  ['activation', ['Activation', 'activation']], ['this.receiptReconciliationGeneration', ['Activation', 'currentActivation']],
  ['this.manuallyClosed', ['Bool', 'closed']], ['this.isWebSocketConnected', ['Bool', 'connected']],
]);

/** Structural bridge for the readiness certificate written after persistence.
 * The early certificate path is synchronous. The only await is the captured
 * scope's window write, followed by this full owner/routing guard. */
export function extractReadyPublication(source) {
  const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  const printer = ts.createPrinter({ removeComments: true });
  const compact = node => printer.printNode(ts.EmitHint.Unspecified, node, file).replace(/\s/g, '');
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'GonvexClient');
  assert.equal(classes.length, 1);
  const methods = name => classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name.getText(file) === name);
  const found = methods('acceptReplicaReady'); assert.equal(found.length, 1);
  const method = found[0];
  assert.deepEqual(method.modifiers?.map(n => n.kind), [ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.AsyncKeyword]);
  assert(!method.asteriskToken && !method.questionToken && !method.typeParameters && !method.type);
  assert.deepEqual(method.parameters.map(n => compact(n, file)), [
    'subscription:ReplicaSubscription', 'message:ReplicaReadyMessage', 'hashes:Readonly<Record<string,string>>',
    'hashesWereStored:boolean', 'scope=this.replicaScope',
  ]);
  const body = method.body.statements; assert.equal(body.length, 14, 'ready publication control flow changed');
  assert.deepEqual(body.slice(0, 9).map(n => compact(n, file)), [
    'constpublicationId=subscription.id;', 'constgeneration=subscription.verificationGeneration;',
    'constsocketGeneration=this.socketGeneration;', 'this.clearReplicaRetry(subscription,true);',
    'subscription.forceFullIntegrity=false;', 'subscription.isUpToDate=true;', 'subscription.opening=false;',
    'raiseReplicaCursorFloor(subscription,message.cursor);', 'constwindow=this.replica.windowProtocolState(subscription.key);',
  ]);
  const window = body[9]; assert(ts.isIfStatement(window) && !window.elseStatement && ts.isBlock(window.thenStatement));
  assert.equal(compact(window.expression, file), 'window');
  const inner = window.thenStatement.statements; assert.equal(inner.length, 7);
  assert.deepEqual(inner.slice(0, 5).map(n => compact(n, file)), [
    'constcompleteness=message.truncated===true?"partial":"complete";',
    'constmode=message.mode??window.mode;', 'consttruncated=message.truncated??window.truncated;',
    'constreadyAlreadyPersisted=hashesWereStored&&window.source==="server"&&window.cursor?.epoch===message.cursor.epoch&&window.cursor.revision===message.cursor.revision&&window.completeness===completeness&&window.mode===mode&&window.truncated===truncated;',
    'constcursorOnlyCheckpoint=this.serverCapabilities.replicaWatermark===1&&hashesWereStored&&window.source==="server"&&window.cursor?.epoch===message.cursor.epoch&&window.cursor.revision<message.cursor.revision&&window.completeness===completeness&&window.mode===mode&&window.truncated===truncated;',
  ]);
  assert.equal(compact(inner[5], file), 'if(readyAlreadyPersisted||cursorOnlyCheckpoint){subscription.readyCursor={...message.cursor};this.replica.setFreshness("current");this.emitReplicaMessage(subscription,message,scope);return;}');
  assert.equal(compact(inner[6], file), 'awaitthis.replica.replaceWindow({...window,scope,rows:this.replica.committedWindowRows(subscription.key),source:"server",cursor:message.cursor,completeness,mode,truncated,hashes,});');
  const guard = body[10];
  assert.equal(compact(guard, file), 'if(!this.ownsReplicaSubscription(subscription)||subscription.id!==publicationId||generation!==subscription.verificationGeneration||socketGeneration!==this.socketGeneration||scope!==this.replicaScope||subscription.scope!==scope||!this.isWebSocketConnected)return;', 'ready certificate needs its post-await owner guard');
  assert.deepEqual(body.slice(11).map(n => compact(n, file)), [
    'subscription.readyCursor={...message.cursor};', 'this.replica.setFreshness("current");',
    'this.emitReplicaMessage(subscription,message,scope);',
  ]);
  const owns = methods('ownsReplicaSubscription'); assert.equal(owns.length, 1);
  assert.equal(compact(owns[0].body, file), '{return!this.manuallyClosed&&this.replicaSubscriptions.get(subscription.key)===subscription;}');
  return { source: method.getText(file), guard: guard.getText(file),
    boundary: 'Closed AST correspondence for captured subscription routing, verification/socket identity and scope after the sole window-write await. Structural check, not another solver theorem. Trusts synchronous helper/built-in behavior and the separate identity-allocation checks.' };
}

/** Check the closed publication path and translate its actual post-await guard. */
export function extractWatermarkPublication(source) {
  const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'GonvexClient');
  assert.equal(classes.length, 1);
  const methods = name => classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name.getText(file) === name);
  const found = methods('persistReplicaWatermark'); assert.equal(found.length, 1);
  const method = found[0];
  assert.deepEqual(method.modifiers?.map(n => n.kind), [ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.AsyncKeyword]);
  assert(!method.asteriskToken && !method.questionToken && !method.typeParameters && !method.type && method.body);
  assert.deepEqual(method.parameters.map(n => compact(n, file)), ['revision:number']);
  const body = method.body.statements;
  assert.equal(body.length, 10, 'watermark publication control flow changed');
  assert.deepEqual(body.slice(0, 4).map(n => compact(n, file)), [
    'constscope=this.replicaScope;', 'constgeneration=this.socketGeneration;',
    'constactivation=this.receiptReconciliationGeneration;', 'consteligibleSignatures:string[]=[];',
  ], 'capture the original owner before storage can yield');
  assert.equal(compact(body[4], file), 'for(constsubscriptionofthis.replicaSubscriptions.values()){if(!subscription.isUpToDate||subscription.opening||!this.replica.windowCanAdvance(subscription.key,revision))continue;eligibleSignatures.push(subscription.key);}');
  assert.equal(compact(body[5], file), 'awaitthis.replica.advanceWatermark(revision,eligibleSignatures,scope);', 'persist the captured scope before publication');
  const guard = body[6];
  assert(ts.isIfStatement(guard) && !guard.elseStatement && ts.isReturnStatement(guard.thenStatement)
    && !guard.thenStatement.expression, 'post-await owner guard must precede every publication');
  assert.equal(compact(body[7], file), 'this.processedReplicaWatermarkRevision=Math.max(this.processedReplicaWatermarkRevision,revision,);');
  assert.equal(compact(body[8], file), 'for(constpendingofthis.pendingCalls.values()){if((pending.kind!=="reducer"&&pending.kind!=="action")||pending.committedRevision===undefined||pending.committedRevision>this.processedReplicaWatermarkRevision)continue;constcomplete=pending.completeAfterReplicaWatermark;pending.completeAfterReplicaWatermark=undefined;complete?.();}');
  assert.equal(compact(body[9], file), 'awaitthis.retireReconciledReceipts();');
  const reset = methods('resetReplicaScopeState'); assert.equal(reset.length, 1);
  assert.equal(compact(reset[0].body.statements[0], file), 'this.receiptReconciliationGeneration=Symbol();');
  const writes = [];
  function visit(node) {
    if (ts.isBinaryExpression(node) && compact(node.left, file) === 'this.receiptReconciliationGeneration') {
      assert.equal(node.operatorToken.kind, ts.SyntaxKind.EqualsToken);
      assert.equal(compact(node.right, file), 'Symbol()', 'receipt activations must allocate fresh identities');
      writes.push(node.getText(file));
    }
    if (ts.isPrefixUnaryExpression(node) || ts.isPostfixUnaryExpression(node))
      assert.notEqual(compact(node.operand, file), 'this.receiptReconciliationGeneration');
    ts.forEachChild(node, visit);
  }
  visit(classes[0]); assert.equal(writes.length, 3);
  const fields = classes[0].members.filter(n => ts.isPropertyDeclaration(n) && n.name.getText(file) === 'receiptReconciliationGeneration');
  assert.equal(fields.length, 1); assert.equal(compact(fields[0].initializer, file), 'Symbol()');
  function expression(node) {
    if (ts.isParenthesizedExpression(node)) return expression(node.expression);
    const key = compact(node, file);
    if (atoms.has(key)) { const [sort, smt] = atoms.get(key); return { sort, smt }; }
    if (node.kind === ts.SyntaxKind.TrueKeyword || node.kind === ts.SyntaxKind.FalseKeyword) return { sort: 'Bool', smt: key };
    if (ts.isPrefixUnaryExpression(node)) {
      assert.equal(node.operator, ts.SyntaxKind.ExclamationToken);
      const value = expression(node.operand); assert.equal(value.sort, 'Bool');
      return { sort: 'Bool', smt: `(not ${value.smt})` };
    }
    assert(ts.isBinaryExpression(node), `unsupported publication expression ${key}`);
    const a = expression(node.left), b = expression(node.right); assert.equal(a.sort, b.sort);
    const op = node.operatorToken.kind;
    if ([ts.SyntaxKind.BarBarToken, ts.SyntaxKind.AmpersandAmpersandToken].includes(op)) {
      assert.equal(a.sort, 'Bool');
      return { sort: 'Bool', smt: `(${op === ts.SyntaxKind.BarBarToken ? 'or' : 'and'} ${a.smt} ${b.smt})` };
    }
    assert([ts.SyntaxKind.EqualsEqualsEqualsToken, ts.SyntaxKind.ExclamationEqualsEqualsToken].includes(op));
    const equality = `(= ${a.smt} ${b.smt})`;
    return { sort: 'Bool', smt: op === ts.SyntaxKind.EqualsEqualsEqualsToken ? equality : `(not ${equality})` };
  }
  const invalid = expression(guard.expression); assert.equal(invalid.sort, 'Bool');
  return {
    definition: `(define-fun canPublish (${inputs.map(([name, sort]) => `(${name} ${sort})`).join(' ')}) Bool (not ${invalid.smt}))`,
    source: method.getText(file), guard: guard.getText(file), freshActivationWrites: writes.length,
    readyPublication: extractReadyPublication(source),
  };
}

const call = `(canPublish ${inputs.map(([name]) => name).join(' ')})`;
const laws = {
  'old-scope-cannot-publish': `(=> (not (= scope currentScope)) (not ${call}))`,
  'old-socket-cannot-publish': `(=> (not (= generation currentGeneration)) (not ${call}))`,
  'old-activation-cannot-publish': `(=> (not (= activation currentActivation)) (not ${call}))`,
  'closed-cannot-publish': `(=> closed (not ${call}))`,
  'disconnected-cannot-publish': `(=> (not connected) (not ${call}))`,
  'current-live-owner-can-publish': `(=> (and (= scope currentScope) (= generation currentGeneration) (= activation currentActivation) (not closed) connected) ${call})`,
  'return-to-scope-still-needs-activation': '(=> (not (= activation currentActivation)) (not (canPublish scope scope generation generation activation currentActivation false true)))',
};
export const publicationMutations = [
  ['ignore-scope', 'scope !== this.replicaScope', 'false'],
  ['ignore-socket', 'generation !== this.socketGeneration', 'false'],
  ['ignore-activation', 'activation !== this.receiptReconciliationGeneration', 'false'],
  ['ignore-close', 'this.manuallyClosed || !this.isWebSocketConnected', 'false || !this.isWebSocketConnected'],
  ['ignore-connection', 'this.manuallyClosed || !this.isWebSocketConnected', 'this.manuallyClosed || false'],
  ['deny-current-owner', 'this.manuallyClosed || !this.isWebSocketConnected', 'true'],
];
const hash = data => createHash('sha256').update(data).digest('hex');
export function verifyWatermarkPublication({ z3, output }) {
  mkdirSync(output, { recursive: true });
  const source = readFileSync(new URL('../../packages/client/src/index.ts', import.meta.url), 'utf8');
  const extracted = extractWatermarkPublication(source);
  function solve(candidate, name) {
    const value = extractWatermarkPublication(candidate);
    const query = ['(declare-sort Scope 0)', '(declare-sort Activation 0)', ...inputs.map(([n, s]) => `(declare-const ${n} ${s})`), value.definition,
      ...Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`)].join('\n');
    const path = resolve(output, name + '.smt2'); writeFileSync(path, query + '\n');
    const result = spawnSync(z3, ['-smt2', path], { encoding: 'utf8', timeout: 15000 });
    assert.ifError(result.error); assert.equal(result.status, 0, result.stderr);
    const answers = result.stdout.trim().split('\n');
    assert.equal(answers.length, Object.keys(laws).length);
    assert(answers.every(answer => ['sat', 'unsat'].includes(answer)), result.stdout);
    return Object.keys(laws).filter((_, index) => answers[index] === 'sat');
  }
  assert.deepEqual(solve(source, 'candidate'), []);
  const negativeControls = publicationMutations.map(([name, before, after]) => {
    assert.equal(extracted.source.split(before).length, 2);
    const candidate = source.replace(extracted.source, extracted.source.replace(before, after));
    const violatedLaws = solve(candidate, name); assert(violatedLaws.length, name);
    return { name, violatedLaws };
  });
  const receipt = { sourceSha256: hash(source), verifierSha256: hash(readFileSync(new URL(import.meta.url))),
    parser: ts.version, laws: Object.keys(laws), negativeControls, extracted,
    boundary: 'Actual post-await guard and closed persistReplicaWatermark path. Checks owner capture, await ordering, scoped storage call, fresh receipt activation writes and all publication/settlement effects behind the guard. Primitive equality and ordinary private fields, Symbol allocation and synchronous built-ins are trusted. Storage commits, guards in called methods, reentrant observers, arbitrary aliasing/reflection and full async protocol refinement remain outside this proof.' };
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log('7 post-await watermark ownership laws verified; 6 broken source guards rejected');
  return receipt;
}
