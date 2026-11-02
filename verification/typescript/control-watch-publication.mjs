import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { controlWatchControls } from './control-watch-controls.mjs';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const printer = ts.createPrinter({ removeComments: true });
const hash = value => createHash('sha256').update(value).digest('hex');
const contract = readFileSync(new URL('./control-watch-contract.ts.txt', import.meta.url), 'utf8');
function parse(source, name) {
  const file = ts.createSourceFile(name + '.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(node => ts.isClassDeclaration(node) && node.name?.text === name);
  assert.equal(classes.length, 1);
  const methods = classes[0].members.filter(node => ts.isMethodDeclaration(node) && node.name?.getText(file) === 'watchControlQuery');
  assert.equal(methods.length, 1);
  return { file, cls: classes[0], method: methods[0] };
}
export function extractControlWatch(source) {
  const actual = parse(source, 'GonvexClient'), expected = parse(contract, 'ControlWatchContract');
  const compact = ({ method, file }) => printer.printNode(ts.EmitHint.Unspecified, method, file);
  assert.equal(compact(actual), compact(expected), 'Control Query watch contract changed');
  const fresh = node => assert(ts.isCallExpression(node) && ts.isIdentifier(node.expression)
    && node.expression.text === 'Symbol' && node.arguments.length === 0 && !node.typeArguments && !node.questionDotToken,
  'session scope invalidation must allocate Symbol()');
  const declarations = actual.cls.members.filter(node => ts.isPropertyDeclaration(node) && node.name.getText(actual.file) === 'sessionScopeGeneration');
  assert.equal(declarations.length, 1); fresh(declarations[0].initializer);
  const resets = actual.cls.members.filter(node => ts.isMethodDeclaration(node) && node.name.getText(actual.file) === 'resetReplicaScopeState');
  assert.equal(resets.length, 1);
  assert.equal(printer.printNode(ts.EmitHint.Unspecified, resets[0].body.statements[0], actual.file).replace(/\s/g, ''), 'this.receiptReconciliationGeneration=Symbol();');
  const first = resets[0].body.statements[1];
  assert(ts.isExpressionStatement(first) && ts.isBinaryExpression(first.expression));
  assert.equal(first.expression.left.getText(actual.file), 'this.sessionScopeGeneration');
  assert.equal(first.expression.operatorToken.kind, ts.SyntaxKind.EqualsToken); fresh(first.expression.right);
  const writes = [];
  const visit = node => {
    if (ts.isElementAccessExpression(node) && node.argumentExpression?.getText(actual.file).includes('sessionScopeGeneration'))
      assert.fail('computed session scope identity access is unsupported');
    if (ts.isPropertyAccessExpression(node) && node.name.text === 'sessionScopeGeneration') {
      assert.equal(node.expression.kind, ts.SyntaxKind.ThisKeyword);
      const parent = node.parent;
      if (ts.isBinaryExpression(parent) && parent.left === node
        && parent.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && parent.operatorToken.kind <= ts.SyntaxKind.LastAssignment) {
        assert.equal(parent.operatorToken.kind, ts.SyntaxKind.EqualsToken); fresh(parent.right);
        let site = node;
        while (site.parent !== actual.cls) { assert(site.parent); site = site.parent; }
        assert(ts.isMethodDeclaration(site)); writes.push(site.name.getText(actual.file));
      }
      if ((ts.isPrefixUnaryExpression(parent) || ts.isPostfixUnaryExpression(parent))
        && [ts.SyntaxKind.PlusPlusToken, ts.SyntaxKind.MinusMinusToken].includes(parent.operator)) assert.fail('numeric session scope identities are unsupported');
    }
    ts.forEachChild(node, visit);
  };
  visit(actual.cls); assert.deepEqual(writes, ['resetReplicaScopeState']);
  return { method: actual.method.getText(actual.file), sessionScopeAllocations: ['initialization', ...writes],
    boundary: 'Closed AST correspondence for the Control Query watch and the two session-scope Symbol allocations. The cached snapshot uses the source-proved scope/activation predicate independently of transport reconnect. Data and initial notifications use the separately checked full publication owner before every listener. Scope-clear notifications capture the installed session owner in a later microtask and survive independent receipt or socket changes. This is structural source correspondence, not a scheduler, backend authorization or native storage theorem.' };
}
export function verifyControlWatch(source) {
  const extracted = extractControlWatch(source);
  const variants = controlWatchControls(source, extracted.method).slice(1);
  for (const variant of variants) assert.throws(() => extractControlWatch(variant.candidate), undefined, variant.name);
  const initialization = '  private sessionScopeGeneration = Symbol();';
  assert.equal(source.split(initialization).length, 2);
  assert.throws(() => extractControlWatch(source.replace(initialization, '  private sessionScopeGeneration = 0;')));
  const allocation = '    this.sessionScopeGeneration = Symbol();';
  assert.equal(source.split(allocation).length, 2);
  assert.throws(() => extractControlWatch(source.replace(allocation, '    if (false) this.sessionScopeGeneration = Symbol();')));
  return { ...extracted, sourceSha256: hash(source), verifierSha256: hash(readFileSync(new URL(import.meta.url))),
    contractSha256: hash(contract), controlsSha256: hash(readFileSync(new URL('./control-watch-controls.mjs', import.meta.url))),
    structuralControlsRejected: [...variants.map(variant => variant.name), 'numeric-session-scope-initialization', 'conditional-session-scope-invalidation'] };
}
