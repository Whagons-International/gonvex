import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { extractGenerations, extractAuthGenerations } from './owner-generations.mjs';
import { extractStreamGenerations } from './stream-generations.mjs';
import { verifyRetirement } from './owner-retirement.mjs';
import { verifyIntentPublication } from './intent-publication.mjs';
import { verifyQueryPublication } from './query-publication.mjs';
import { verifyControlWatch } from './control-watch-publication.mjs';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const sourcePath = fileURLToPath(new URL('../../packages/client/src/ownership-policy.ts', import.meta.url));
const parameters = [
  ['capturedScope', 'Scope', ts.SyntaxKind.StringKeyword], ['currentScope', 'Scope', ts.SyntaxKind.StringKeyword],
  ['capturedGeneration', 'Activation', ts.SyntaxKind.SymbolKeyword], ['currentGeneration', 'Activation', ts.SyntaxKind.SymbolKeyword],
  ['closed', 'Bool', ts.SyntaxKind.BooleanKeyword],
];
const sorts = new Map(parameters.map(([name, sort]) => [name, sort]));
export function extract(source) {
  const file = ts.createSourceFile('ownership-policy.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  assert.equal(file.statements.length, 1, 'only the ownership function is supported');
  const fn = file.statements[0];
  assert(ts.isFunctionDeclaration(fn) && fn.name?.text === 'ownsCurrentScope' && fn.body && !fn.asteriskToken && !fn.typeParameters);
  assert.deepEqual(fn.modifiers?.map(m => m.kind), [ts.SyntaxKind.ExportKeyword]);
  assert.equal(fn.type?.kind, ts.SyntaxKind.BooleanKeyword);
  assert.equal(fn.parameters.length, parameters.length);
  for (const [index, [name, , type]] of parameters.entries()) {
    const p = fn.parameters[index];
    assert(ts.isIdentifier(p.name) && p.name.text === name && p.type?.kind === type);
    assert(!p.initializer && !p.questionToken && !p.dotDotDotToken && !p.modifiers);
  }
  assert.equal(fn.body.statements.length, 1, 'exactly one return required');
  const statement = fn.body.statements[0];
  assert(ts.isReturnStatement(statement) && statement.expression);
  const expr = node => {
    if (ts.isParenthesizedExpression(node)) return expr(node.expression);
    if (ts.isIdentifier(node)) { assert(sorts.has(node.text), 'unknown identifier'); return { sort: sorts.get(node.text), smt: node.text }; }
    if (node.kind === ts.SyntaxKind.TrueKeyword || node.kind === ts.SyntaxKind.FalseKeyword)
      return { sort: 'Bool', smt: node.kind === ts.SyntaxKind.TrueKeyword ? 'true' : 'false' };
    if (ts.isPrefixUnaryExpression(node)) {
      assert.equal(node.operator, ts.SyntaxKind.ExclamationToken);
      const value = expr(node.operand); assert.equal(value.sort, 'Bool');
      return { sort: 'Bool', smt: `(not ${value.smt})` };
    }
    if (ts.isBinaryExpression(node)) {
      const left = expr(node.left), right = expr(node.right);
      assert.equal(left.sort, right.sort);
      const op = node.operatorToken.kind;
      if (op === ts.SyntaxKind.AmpersandAmpersandToken || op === ts.SyntaxKind.BarBarToken) {
        assert.equal(left.sort, 'Bool');
        return { sort: 'Bool', smt: `(${op === ts.SyntaxKind.AmpersandAmpersandToken ? 'and' : 'or'} ${left.smt} ${right.smt})` };
      }
      assert([ts.SyntaxKind.EqualsEqualsEqualsToken, ts.SyntaxKind.ExclamationEqualsEqualsToken].includes(op), 'only strict comparisons supported');
      const equality = `(= ${left.smt} ${right.smt})`;
      return { sort: 'Bool', smt: op === ts.SyntaxKind.EqualsEqualsEqualsToken ? equality : `(not ${equality})` };
    }
    throw Error(`unsupported syntax ${ts.SyntaxKind[node.kind]}`);
  };
  const value = expr(statement.expression); assert.equal(value.sort, 'Bool');
  return `(define-fun owns (${parameters.map(([name, sort]) => `(${name} ${sort})`).join(' ')}) Bool ${value.smt})`;
}

const prelude = '(declare-sort Scope 0)\n(declare-sort Activation 0)\n'
  + parameters.map(([name, sort]) => `(declare-const ${name} ${sort})`).join('\n');
const allowed = '(owns capturedScope currentScope capturedGeneration currentGeneration closed)';
const laws = {
  'closed-cannot-publish': `(=> closed (not ${allowed}))`,
  'different-scope-cannot-publish': `(=> (not (= capturedScope currentScope)) (not ${allowed}))`,
  'different-activation-cannot-publish': `(=> (not (= capturedGeneration currentGeneration)) (not ${allowed}))`,
  'current-open-owner-can-publish': `(=> (and (not closed) (= capturedScope currentScope) (= capturedGeneration currentGeneration)) ${allowed})`,
  'return-to-same-scope-still-requires-activation': '(=> (not (= capturedGeneration currentGeneration)) (not (owns capturedScope capturedScope capturedGeneration currentGeneration false)))',
};
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
function solve(z3, source, output) {
  const queries = Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n');
  writeFileSync(output, `(set-option :timeout 10000)\n${prelude}\n${extract(source)}\n${queries}\n`);
  const result = spawnSync(z3, ['-smt2', output], { encoding: 'utf8', timeout: 15000 });
  assert.ifError(result.error); assert.equal(result.status, 0, result.stderr || result.stdout);
  const answers = result.stdout.trim().split('\n');
  assert.equal(answers.length, Object.keys(laws).length);
  assert(answers.every(answer => ['sat', 'unsat'].includes(answer)), result.stdout);
  return answers;
}
export function verify({ z3, output }) {
  mkdirSync(output, { recursive: true });
  const version = spawnSync(z3, ['--version'], { encoding: 'utf8' });
  assert.equal(version.status, 0); assert.match(version.stdout, /^Z3 version 4\.16\.0 /);
  const source = readFileSync(sourcePath, 'utf8');
  const clientPath = fileURLToPath(new URL('../../packages/client/src/index.ts', import.meta.url));
  const clientSource = readFileSync(clientPath, 'utf8');
  const allocations = extractGenerations(clientSource);
  const authAllocations = extractAuthGenerations(clientSource);
  const streamAllocations = extractStreamGenerations(clientSource);
  assert(solve(z3, source, resolve(output, 'candidate.smt2')).every(answer => answer === 'unsat'));
  const mutations = [
    ['ignore-close', '!closed', 'true'],
    ['ignore-scope', 'capturedScope === currentScope && ', ''],
    ['ignore-generation', ' && capturedGeneration === currentGeneration', ''],
    ['wrong-generation', 'capturedGeneration === currentGeneration', 'capturedGeneration !== currentGeneration'],
    ['allow-any-open-owner', '!closed &&', '!closed ||'],
    ['deny-every-owner', '!closed && capturedScope === currentScope && capturedGeneration === currentGeneration', 'false'],
  ];
  const files = [], negativeControls = [];
  for (const [name, before, after] of mutations) {
    assert.equal(source.split(before).length, 2, name);
    const candidate = source.replace(before, after), path = resolve(output, name + '.ts');
    writeFileSync(path, candidate); files.push(path);
    const answers = solve(z3, candidate, resolve(output, name + '.smt2'));
    const violatedLaws = Object.keys(laws).filter((_, index) => answers[index] === 'sat');
    assert(violatedLaws.length > 0, `broken ownership guard escaped: ${name}`);
    negativeControls.push({ name, violatedLaws });
  }
  const program = ts.createProgram([sourcePath, ...files], { strict: true, noEmit: true, skipLibCheck: true, types: [], target: ts.ScriptTarget.ES2022 });
  const errors = ts.getPreEmitDiagnostics(program);
  assert.equal(errors.length, 0, errors.map(d => ts.flattenDiagnosticMessageText(d.messageText, '\n')).join('\n'));
  const retirement = verifyRetirement({ z3, output: resolve(output, 'retirement') });
  const receipt = { retirement, intentPublication: verifyIntentPublication(clientSource), queryPublication: verifyQueryPublication(clientSource), controlWatch: verifyControlWatch(clientSource), source: 'packages/client/src/ownership-policy.ts', sourceSha256: hash(source), verifierSha256: hash(readFileSync(fileURLToPath(import.meta.url))), parser: ts.version, solver: version.stdout.trim(), solverSha256: hash(readFileSync(z3)), laws: Object.keys(laws), negativeControls,
    allocationSource: 'packages/client/src/index.ts', allocationSourceSha256: hash(clientSource),
    allocationVerifierSha256: hash(readFileSync(new URL('./owner-generations.mjs', import.meta.url))), allocations, authAllocations, streamAllocations,
    streamAllocationVerifierSha256: hash(readFileSync(new URL('./stream-generations.mjs', import.meta.url))),
    boundary: 'Primitive string/Symbol equality and Boolean input, modeled by uninterpreted equality sorts. AST checks initialization plus all four outbox and seven auth invalidation/fetch writes use Symbol(). Relies on standard unmodified Symbol allocation and ordinary private-field access; not a verified JS compiler, async guard placement proof, storage atomicity or backend authorization proof.' };
  writeFileSync(resolve(output, 'source.ts'), source);
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log(`${Object.keys(laws).length} ownership laws verified; ${mutations.length} typed broken guards rejected`);
  return receipt;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2); assert.equal(args.length, 4); assert.equal(args[0], '--z3'); assert.equal(args[2], '--output');
  verify({ z3: resolve(args[1]), output: resolve(args[3]) });
}
