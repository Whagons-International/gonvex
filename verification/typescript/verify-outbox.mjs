// Source-linked proof of the finite delivery/recovery transition system.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
import {readFileSync, mkdirSync, writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const SOURCE = fileURLToPath(new URL('../../packages/client/src/outbox-policy.ts', import.meta.url));
const domains = {ReducerOutboxState: ['pending', 'inflight', 'committed', 'failed', 'rejected'], DeliveryAction: ['start', 'recover']};
const sort = name => name === 'ReducerOutboxState' ? 'State' : 'Action';
const symbol = name => 'v_' + name;
export function extract(source) {
  const file = ts.createSourceFile('outbox-policy.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  assert.equal(file.statements.length, 3, 'only two closed types and one function supported');
  for (const [index, [name, values]] of Object.entries(domains).entries()) {
    const node = file.statements[index];
    assert(ts.isTypeAliasDeclaration(node) && node.name.text === name && !node.typeParameters);
    assert.deepEqual(node.modifiers?.map(m => m.kind), [ts.SyntaxKind.ExportKeyword]);
    assert(ts.isUnionTypeNode(node.type));
    assert.deepEqual(node.type.types.map(t => {
      assert(ts.isLiteralTypeNode(t) && ts.isStringLiteral(t.literal)); return t.literal.text;
    }), values, 'closed state/action domain changed; review the specification');
  }
  const fn = file.statements[2];
  assert(ts.isFunctionDeclaration(fn) && fn.name?.text === 'deliveryState' && fn.body && !fn.asteriskToken && !fn.typeParameters);
  assert.deepEqual(fn.modifiers?.map(m => m.kind), [ts.SyntaxKind.ExportKeyword]);
  assert.equal(fn.parameters.length, 2);
  const params = new Map();
  for (const [index, [name, type]] of [['state', 'ReducerOutboxState'], ['action', 'DeliveryAction']].entries()) {
    const p = fn.parameters[index];
    assert(ts.isIdentifier(p.name) && p.name.text === name && !p.initializer && !p.questionToken && !p.dotDotDotToken && !p.modifiers);
    assert(ts.isTypeReferenceNode(p.type) && !p.type.typeArguments && p.type.typeName.getText(file) === type);
    params.set(name, sort(type));
  }
  assert(ts.isTypeReferenceNode(fn.type) && !fn.type.typeArguments && fn.type.typeName.getText(file) === 'ReducerOutboxState');
  assert.equal(fn.body.statements.length, 1, 'exactly one return required');
  const statement = fn.body.statements[0]; assert(ts.isReturnStatement(statement) && statement.expression);
  const expr = node => {
    if (ts.isParenthesizedExpression(node)) return expr(node.expression);
    if (ts.isIdentifier(node)) { assert(params.has(node.text)); return {sort: params.get(node.text), smt: node.text}; }
    if (ts.isStringLiteral(node)) {
      const domain = Object.keys(domains).find(key => domains[key].includes(node.text)); assert(domain, 'unknown literal');
      return {sort: sort(domain), smt: symbol(node.text)};
    }
    if (ts.isBinaryExpression(node)) {
      assert([ts.SyntaxKind.EqualsEqualsEqualsToken, ts.SyntaxKind.ExclamationEqualsEqualsToken].includes(node.operatorToken.kind), 'unsupported comparison');
      const left = expr(node.left), right = expr(node.right); assert.equal(left.sort, right.sort);
      const eq = `(= ${left.smt} ${right.smt})`;
      return {sort: 'Bool', smt: node.operatorToken.kind === ts.SyntaxKind.EqualsEqualsEqualsToken ? eq : `(not ${eq})`};
    }
    if (ts.isConditionalExpression(node)) {
      const condition = expr(node.condition), yes = expr(node.whenTrue), no = expr(node.whenFalse);
      assert.equal(condition.sort, 'Bool'); assert.equal(yes.sort, no.sort);
      return {sort: yes.sort, smt: `(ite ${condition.smt} ${yes.smt} ${no.smt})`};
    }
    throw Error(`unsupported syntax ${ts.SyntaxKind[node.kind]}`);
  };
  const result = expr(statement.expression); assert.equal(result.sort, 'State');
  return `(define-fun next ((state State) (action Action)) State ${result.smt})`;
}
const prelude = Object.entries(domains).map(([name, values]) => `(declare-datatypes () ((${sort(name)} ${values.map(symbol).join(' ')})))`).join('\n') + '\n(declare-const s State) (declare-const a Action)';
// These contracts state admission/recovery independently of the source expression.
const laws = {
  'start-admits-exactly-pending': '(= (and (= (next s v_start) v_inflight) (not (= s v_inflight))) (= s v_pending))',
  'pending-starts': '(= (next v_pending v_start) v_inflight)',
  'inflight-start-is-idempotent': '(= (next v_inflight v_start) v_inflight)',
  'recovery-only-changes-inflight': '(=> (not (= s v_inflight)) (= (next s v_recover) s))',
  'inflight-recovers': '(= (next v_inflight v_recover) v_pending)',
  'committed-invariant-step': '(=> (= s v_committed) (= (next s a) v_committed))',
  'failed-invariant-step': '(=> (= s v_failed) (= (next s a) v_failed))',
  'rejected-invariant-step': '(=> (= s v_rejected) (= (next s a) v_rejected))',
  'start-idempotent': '(= (next (next s v_start) v_start) (next s v_start))',
  'recovery-idempotent': '(= (next (next s v_recover) v_recover) (next s v_recover))',
  'active-stays-active': '(=> (or (= s v_pending) (= s v_inflight)) (or (= (next s a) v_pending) (= (next s a) v_inflight)))',
};
const checks = Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n');
function solve(z3, body, file) {
  writeFileSync(file, `(set-option :timeout 10000)\n${prelude}\n${body}\n${checks}\n`);
  const r = spawnSync(z3, ['-smt2', file], {encoding: 'utf8', timeout: 15000});
  assert.ifError(r.error); assert.equal(r.status, 0, r.stderr || r.stdout);
  const results = r.stdout.trim().split('\n'); assert.equal(results.length, Object.keys(laws).length);
  assert(results.every(value => ['sat', 'unsat'].includes(value)), r.stdout); return results;
}
const hash = value => createHash('sha256').update(value).digest('hex');
export function verify({z3, output}) {
  mkdirSync(output, {recursive: true});
  const version = spawnSync(z3, ['--version'], {encoding: 'utf8'});
  assert.equal(version.status, 0); assert.match(version.stdout, /^Z3 version 4\.16\.0 /);
  const source = readFileSync(SOURCE, 'utf8');
  assert(solve(z3, extract(source), resolve(output, 'candidate.smt2')).every(r => r === 'unsat'));
  const mutants = [
    ['resend-committed', 'state === "pending"', 'state !== "inflight"'],
    ['recover-committed', 'state === "inflight"', 'state !== "pending"'],
    ['start-does-nothing', '? "inflight" : state', '? "pending" : state'],
    ['recovery-does-nothing', '? "pending" : state', '? "inflight" : state'],
    ['wrong-action', 'action === "start"', 'action === "recover"'],
    ['erase-parked-on-start', '? "inflight" : state', '? "inflight" : "pending"'],
  ];
  const files = [];
  for (const [name, before, after] of mutants) {
    assert.equal(source.split(before).length, 2, name);
    const candidate = source.replace(before, after), file = resolve(output, name + '.ts');
    writeFileSync(file, candidate); files.push(file);
    assert(solve(z3, extract(candidate), resolve(output, name + '.smt2')).includes('sat'), `broken transition escaped: ${name}`);
  }
  const program = ts.createProgram([SOURCE, ...files], {strict: true, noEmit: true, skipLibCheck: true, types: [], target: ts.ScriptTarget.ES2022});
  const errors = ts.getPreEmitDiagnostics(program); assert.equal(errors.length, 0, errors.map(d => ts.flattenDiagnosticMessageText(d.messageText, '\n')).join('\n'));
  const receipt = {source: 'packages/client/src/outbox-policy.ts', sourceSha256: hash(source), verifierSha256: hash(readFileSync(fileURLToPath(import.meta.url))), parser: ts.version, solver: version.stdout.trim(), solverSha256: hash(readFileSync(z3)), laws: Object.keys(laws), negativeControls: mutants.map(m => m[0]), boundary: 'Closed five-state/two-action source transition; Invariant step plus a protected initial state supports arbitrary finite delivery/recovery sequences. Does not prove storage atomicity, integration, retries, commit, deletion, metadata or async delivery.'};
  writeFileSync(resolve(output, 'source.ts'), source); writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2)+'\n');
  console.log(`${Object.keys(laws).length} delivery laws verified; ${mutants.length} typed broken transitions rejected`); return receipt;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2); assert.equal(args.length, 4); assert.equal(args[0], '--z3'); assert.equal(args[2], '--output');
  verify({z3: resolve(args[1]), output: resolve(args[3])});
}
