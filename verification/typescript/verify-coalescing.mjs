import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const sourceUrl = new URL('../../packages/client/src/coalescing-policy.ts', import.meta.url);
export function extract(source) {
  const file = ts.createSourceFile('coalescing-policy.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  assert.equal(file.statements.length, 1);
  const fn = file.statements[0];
  assert(ts.isFunctionDeclaration(fn) && fn.name?.text === 'coalescingAdmission' && fn.body && !fn.asteriskToken && !fn.typeParameters);
  assert.deepEqual(fn.modifiers?.map(m => m.kind), [ts.SyntaxKind.ExportKeyword]);
  assert.equal(fn.type?.kind, ts.SyntaxKind.BooleanKeyword);
  assert.equal(fn.parameters.length, 2);
  for (const [i, [name, type]] of [['state', ts.SyntaxKind.StringKeyword], ['knownUnsent', ts.SyntaxKind.UnknownKeyword]].entries()) {
    const p = fn.parameters[i];
    assert(ts.isIdentifier(p.name) && p.name.text === name && p.type?.kind === type);
    assert(!p.initializer && !p.questionToken && !p.dotDotDotToken && !p.modifiers);
  }
  assert.equal(fn.body.statements.length, 1);
  const statement = fn.body.statements[0];
  assert(ts.isReturnStatement(statement) && statement.expression);
  function expr(node) {
    if (ts.isParenthesizedExpression(node)) return expr(node.expression);
    if (node.kind === ts.SyntaxKind.TrueKeyword) return 'true';
    if (node.kind === ts.SyntaxKind.FalseKeyword) return 'false';
    if (ts.isPrefixUnaryExpression(node) && node.operator === ts.SyntaxKind.ExclamationToken) return `(not ${expr(node.operand)})`;
    if (ts.isBinaryExpression(node)) {
      const op = node.operatorToken.kind;
      if ([ts.SyntaxKind.AmpersandAmpersandToken, ts.SyntaxKind.BarBarToken].includes(op))
        return `(${op === ts.SyntaxKind.AmpersandAmpersandToken ? 'and' : 'or'} ${expr(node.left)} ${expr(node.right)})`;
      assert([ts.SyntaxKind.EqualsEqualsEqualsToken, ts.SyntaxKind.ExclamationEqualsEqualsToken].includes(op), 'only strict equality');
      assert(ts.isIdentifier(node.left));
      let equality;
      if (node.left.text === 'state') {
        assert(ts.isStringLiteral(node.right) && /^[a-z]+$/.test(node.right.text));
        equality = `(= state "${node.right.text}")`;
      } else {
        assert.equal(node.left.text, 'knownUnsent');
        // Only this exact comparison is represented. All other JS values,
        // including false, undefined, strings and objects, inhabit the false class.
        assert.equal(node.right.kind, ts.SyntaxKind.TrueKeyword);
        equality = 'exactlyTrue';
      }
      return op === ts.SyntaxKind.EqualsEqualsEqualsToken ? equality : `(not ${equality})`;
    }
    throw Error(`unsupported coalescing expression ${ts.SyntaxKind[node.kind]}`);
  }
  return `(define-fun admits ((state String) (exactlyTrue Bool)) Bool ${expr(statement.expression)})`;
}
const laws = {
  'unknown-or-sent-cannot-coalesce': '(=> (not exactlyTrue) (not (admits state exactlyTrue)))',
  'only-pending-can-coalesce': '(=> (not (= state "pending")) (not (admits state exactlyTrue)))',
  'fresh-pending-can-coalesce': '(admits "pending" true)',
};
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
export function verify({ z3, output }) {
  mkdirSync(output, { recursive: true });
  const version = spawnSync(z3, ['--version'], { encoding: 'utf8' });
  assert.equal(version.status, 0); assert.match(version.stdout, /^Z3 version 4\.16\.0 /);
  const source = readFileSync(sourceUrl, 'utf8');
  function solve(candidate, name) {
    const path = resolve(output, `${name}.smt2`);
    const queries = Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n');
    writeFileSync(path, `(set-option :timeout 10000)\n(declare-const state String)\n(declare-const exactlyTrue Bool)\n${extract(candidate)}\n${queries}\n`);
    const result = spawnSync(z3, ['-smt2', path], { encoding: 'utf8', timeout: 15000 });
    assert.ifError(result.error); assert.equal(result.status, 0, result.stderr || result.stdout);
    const answers = result.stdout.trim().split('\n');
    assert.equal(answers.length, Object.keys(laws).length);
    assert(answers.every(answer => ['sat', 'unsat'].includes(answer)), result.stdout);
    return Object.keys(laws).filter((_, i) => answers[i] === 'sat');
  }
  assert.deepEqual(solve(source, 'candidate'), []);
  const mutations = [
    ['ignore-provenance', ' && knownUnsent === true', ''],
    ['allow-committed', 'state === "pending"', 'state === "committed"'],
    ['wrong-provenance', 'knownUnsent === true', 'knownUnsent !== true'],
    ['either-condition', ' && ', ' || '],
    ['deny-all', 'state === "pending" && knownUnsent === true', 'false'],
  ];
  const files = [], negativeControls = mutations.map(([name, before, after]) => {
    assert.equal(source.split(before).length, 2, name);
    const candidate = source.replace(before, after), path = resolve(output, name + '.ts');
    writeFileSync(path, candidate); files.push(path);
    const violatedLaws = solve(candidate, name);
    assert(violatedLaws.length > 0, name);
    return { name, violatedLaws };
  });
  const program = ts.createProgram([fileURLToPath(sourceUrl), ...files], { strict: true, noEmit: true, skipLibCheck: true, types: [], target: ts.ScriptTarget.ES2022 });
  const errors = ts.getPreEmitDiagnostics(program);
  assert.equal(errors.length, 0, errors.map(d => ts.flattenDiagnosticMessageText(d.messageText, '\n')).join('\n'));
  const receipt = { source: 'packages/client/src/coalescing-policy.ts', sourceSha256: hash(source), verifierSha256: hash(readFileSync(fileURLToPath(import.meta.url))), parser: ts.version,
    solver: version.stdout.trim(), solverSha256: hash(readFileSync(z3)), laws: Object.keys(laws), negativeControls,
    boundary: 'Closed pure helper grammar only. JS primitive string equality is represented by SMT string equality. The unknown input is partitioned by strict equality to true; no other unknown operations are accepted. Does not prove provenance production, call-site dominance, transactions, delivery, or preservation through migrations. Adapter and async schedules require separate tests.' };
  writeFileSync(resolve(output, 'source.ts'), source);
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log('3 coalescing admission laws verified; 5 typed broken helpers rejected');
  return receipt;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2); assert.equal(args.length, 4); assert.equal(args[0], '--z3'); assert.equal(args[2], '--output');
  verify({ z3: resolve(args[1]), output: resolve(args[3]) });
}
