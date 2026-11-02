import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const compact = (node, file) => node.getText(file).replace(/\s/g, '');
const original = { state: { sort: 'State', smt: 's' }, waiting: { sort: 'Bool', smt: 'waiting' } };
function choose(condition, yes, no) {
  assert.equal(condition.sort, 'Bool');
  return Object.fromEntries(['state', 'waiting'].map(key => [key,
    { sort: yes[key].sort, smt: `(ite ${condition.smt} ${yes[key].smt} ${no[key].smt})` }]));
}

/** Closed translation of the actual pure record changes, including no-op returns. */
export function extractSettlement(source) {
  const file = ts.createSourceFile('outbox.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const functions = {}, definitions = [];
  for (const name of ['markAcceptedChange', 'markCommittedChange']) {
    const matches = file.statements.filter(n => ts.isFunctionDeclaration(n) && n.name?.text === name);
    assert.equal(matches.length, 1);
    const fn = matches[0];
    assert(!fn.modifiers && !fn.typeParameters && !fn.asteriskToken);
    assert.deepEqual(fn.parameters.map(n => compact(n, file)), ['entry:ReducerOutboxEntry']);
    assert.equal(compact(fn.type, file), 'ReducerOutboxEntry|undefined');
    const environment = new Map([['entry.state', original.state], ['entry.awaitingReplica', original.waiting]]);
    function expression(node) {
      if (ts.isParenthesizedExpression(node)) return expression(node.expression);
      const text = compact(node, file);
      if (environment.has(text)) return environment.get(text);
      if (['true', 'false', 'undefined'].includes(text)) return { sort: 'Bool', smt: text === 'true' ? 'true' : 'false' };
      if (ts.isStringLiteral(node)) {
        assert(['pending', 'inflight', 'committed', 'failed', 'rejected'].includes(node.text));
        return { sort: 'State', smt: 'v_' + node.text };
      }
      if (ts.isPrefixUnaryExpression(node)) {
        assert.equal(node.operator, ts.SyntaxKind.ExclamationToken);
        const operand = expression(node.operand); assert.equal(operand.sort, 'Bool');
        return { sort: 'Bool', smt: `(not ${operand.smt})` };
      }
      if (ts.isCallExpression(node)) {
        assert.equal(compact(node.expression, file), 'deliveryState');
        assert(!node.typeArguments && node.arguments.length === 2);
        assert.equal(compact(node.arguments[1], file), '"commit"');
        const state = expression(node.arguments[0]); assert.equal(state.sort, 'State');
        return { sort: 'State', smt: `(next ${state.smt} v_commit)` };
      }
      if (ts.isBinaryExpression(node)) {
        const left = expression(node.left), right = expression(node.right); assert.equal(left.sort, right.sort);
        if (node.operatorToken.kind === ts.SyntaxKind.AmpersandAmpersandToken) {
          assert.equal(left.sort, 'Bool'); return { sort: 'Bool', smt: `(and ${left.smt} ${right.smt})` };
        }
        assert.equal(node.operatorToken.kind, ts.SyntaxKind.EqualsEqualsEqualsToken);
        return { sort: 'Bool', smt: `(= ${left.smt} ${right.smt})` };
      }
      throw Error('unsupported settlement expression: ' + text);
    }
    function result(node) {
      if (ts.isIdentifier(node) && node.text === 'undefined') return original;
      if (ts.isConditionalExpression(node)) return choose(expression(node.condition), result(node.whenTrue), result(node.whenFalse));
      assert(ts.isObjectLiteralExpression(node));
      const properties = node.properties;
      assert(ts.isSpreadAssignment(properties[0]) && compact(properties[0].expression, file) === 'entry');
      const value = { ...original }, assigned = new Set();
      for (const property of properties.slice(1)) {
        assert(ts.isPropertyAssignment(property) || ts.isShorthandPropertyAssignment(property));
        const key = compact(property.name, file);
        assert(['knownUnsent', 'state', 'awaitingReplica'].includes(key) && !assigned.has(key)); assigned.add(key);
        const next = expression(ts.isShorthandPropertyAssignment(property) ? property.name : property.initializer);
        if (key === 'knownUnsent') { assert.deepEqual(next, { sort: 'Bool', smt: 'false' }); continue; }
        const field = key === 'state' ? 'state' : 'waiting';
        assert.equal(next.sort, original[field].sort); value[field] = next;
      }
      return value;
    }
    function statements(nodes) {
      assert(nodes.length > 0);
      const [first, ...rest] = nodes;
      if (ts.isVariableStatement(first)) {
        assert.equal(first.declarationList.flags, ts.NodeFlags.Const);
        assert.equal(first.declarationList.declarations.length, 1);
        const variable = first.declarationList.declarations[0];
        assert(ts.isIdentifier(variable.name) && variable.name.text === 'state' && !variable.type);
        assert(!environment.has('state'), 'redeclared settlement state');
        environment.set('state', expression(variable.initializer)); return statements(rest);
      }
      if (ts.isIfStatement(first)) {
        assert(!first.elseStatement && ts.isReturnStatement(first.thenStatement));
        return choose(expression(first.expression), result(first.thenStatement.expression), statements(rest));
      }
      assert(ts.isReturnStatement(first) && rest.length === 0); return result(first.expression);
    }
    const value = statements([...fn.body.statements]);
    functions[name] = fn.getText(file);
    for (const key of ['state', 'waiting']) definitions.push(`(define-fun ${name}_${key} ((s State) (waiting Bool)) ${value[key].sort} ${value[key].smt})`);
  }
  return { definitions: definitions.join('\n'), functions };
}

export function verifySettlement({ z3, output, policy }) {
  mkdirSync(output, { recursive: true });
  const source = readFileSync(new URL('../../packages/client/src/outbox.ts', import.meta.url), 'utf8');
  const accepted = '(markAcceptedChange_state s waiting)', pending = '(markAcceptedChange_waiting s waiting)';
  const laws = {
    'received-acceptance-is-committed': `(= ${accepted} v_committed)`,
    'fresh-acceptance-needs-replica': `(=> (not (= s v_committed)) ${pending})`,
    'duplicate-acceptance-preserves-barrier': `(=> (= s v_committed) (= ${pending} waiting))`,
    'completed-acceptance-stays-completed': '(not (markAcceptedChange_waiting v_committed false))',
    'replica-completion-is-committed': '(= (markCommittedChange_state s waiting) v_committed)',
    'replica-completion-clears-pending': '(not (markCommittedChange_waiting s waiting))',
    'accepted-record-cannot-start-delivery': `(= (next ${accepted} v_start) v_committed)`,
  };
  const prelude = '(declare-datatypes () ((State v_pending v_inflight v_committed v_failed v_rejected)))\n'
    + '(declare-datatypes () ((Action v_start v_recover v_commit v_reject v_retry v_fail-retry v_fail-park)))\n'
    + '(declare-const s State)\n(declare-const waiting Bool)\n';
  function solve(candidate, name) {
    const extracted = extractSettlement(candidate), path = resolve(output, name + '.smt2');
    writeFileSync(path, prelude + policy + '\n' + extracted.definitions + '\n'
      + Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n'));
    const result = spawnSync(z3, ['-smt2', path], { encoding: 'utf8', timeout: 15000 });
    assert.ifError(result.error); assert.equal(result.status, 0, result.stderr || result.stdout);
    const answers = result.stdout.trim().split('\n'); assert.equal(answers.length, Object.keys(laws).length);
    assert(answers.every(answer => ['sat', 'unsat'].includes(answer)));
    return { extracted, violated: Object.keys(laws).filter((_, index) => answers[index] === 'sat') };
  }
  const candidate = solve(source, 'candidate'); assert.deepEqual(candidate.violated, []);
  writeFileSync(resolve(output, 'source.ts'), Object.values(candidate.extracted.functions).join('\n\n') + '\n');
  const mutations = [
    ['forget-pending-data', 'markAcceptedChange', 'awaitingReplica: true', 'awaitingReplica: undefined'],
    ['reopen-completed-barrier', 'markAcceptedChange', 'if (entry.state === "committed")', 'if (false)'],
    ['leave-pending-after-completion', 'markCommittedChange', 'awaitingReplica: undefined', 'awaitingReplica: true'],
    ['skip-pending-completion', 'markCommittedChange', 'state === entry.state && !entry.awaitingReplica', 'state === entry.state'],
  ];
  const negativeControls = [];
  for (const [name, functionName, before, after] of mutations) {
    const method = candidate.extracted.functions[functionName]; assert.equal(method.split(before).length, 2);
    const mutated = source.replace(method, method.replace(before, after)), result = solve(mutated, name);
    assert(result.violated.length > 0, name); negativeControls.push({ name, violatedLaws: result.violated });
    const file = resolve(output, name + '.ts');
    writeFileSync(file, 'type ReducerOutboxState="pending"|"inflight"|"committed"|"failed"|"rejected";\n'
      + 'type ReducerOutboxEntry={state:ReducerOutboxState;awaitingReplica?:true;knownUnsent?:boolean};\n'
      + 'declare function deliveryState(state:ReducerOutboxState,action:"commit"):ReducerOutboxState;\n'
      + Object.values(result.extracted.functions).join('\n'));
    const program = ts.createProgram([file], { strict: true, noEmit: true, skipLibCheck: true, types: [], target: ts.ScriptTarget.ES2022 });
    assert.equal(ts.getPreEmitDiagnostics(program).length, 0, 'ill-typed settlement mutant');
  }
  const hash = value => createHash('sha256').update(value).digest('hex');
  const receipt = { source: 'packages/client/src/outbox.ts', sourceSha256: hash(source), verifierSha256: hash(readFileSync(new URL(import.meta.url))),
    laws: Object.keys(laws), negativeControls,
    boundary: 'Closed AST translation of two actual pure record changes, composed with source-derived delivery policy. Optional true/undefined marker modeled by Bool on plain records. Assumes ordinary object spread. Does not prove callback placement, record provenance, persistence, SQL, event-loop scheduling or full async refinement.' };
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log('7 receipt settlement laws verified; 4 typed broken helpers rejected');
  return receipt;
}
