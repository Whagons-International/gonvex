// Check the direct stream activation allocations and metadata-copy boundaries.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const names = new Set(['socketGeneration', 'revisionSocketGeneration', 'authenticatedSocketGeneration', 'verificationGeneration', 'resetGeneration']);
export function extractStreamGenerations(source) {
  const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'GonvexClient');
  assert.equal(classes.length, 1); const cls = classes[0];
  const fresh = node => assert(ts.isCallExpression(node) && ts.isIdentifier(node.expression)
    && node.expression.text === 'Symbol' && node.arguments.length === 0 && !node.typeArguments && !node.questionDotToken,
  'stream invalidation must allocate fresh Symbol()');
  const current = node => ts.isPropertyAccessExpression(node) && node.expression.kind === ts.SyntaxKind.ThisKeyword && node.name.text === 'socketGeneration';
  const declaration = cls.members.filter(n => ts.isPropertyDeclaration(n) && n.name.getText(file) === 'socketGeneration');
  assert.equal(declaration.length, 1); fresh(declaration[0].initializer);
  const allocations = [], copies = [];
  function site(node) {
    while (node.parent !== cls) { assert(node.parent); node = node.parent; }
    assert(ts.isMethodDeclaration(node) || ts.isConstructorDeclaration(node));
    return ts.isConstructorDeclaration(node) ? 'constructor' : node.name.getText(file);
  }
  function visit(node) {
    if (ts.isElementAccessExpression(node) && [...names].some(name => node.argumentExpression?.getText(file).includes(name)))
      assert.fail('computed stream generation access is unsupported');
    if ((ts.isPropertyAssignment(node) || ts.isShorthandPropertyAssignment(node)) && names.has(node.name.getText(file))) {
      assert(ts.isPropertyAssignment(node));
      const name = node.name.getText(file);
      if (name === 'verificationGeneration') { fresh(node.initializer); allocations.push({ property: name, site: site(node) }); }
      else { assert.equal(name, 'socketGeneration'); assert(current(node.initializer)); copies.push({ property: name, site: site(node) }); }
    }
    if (ts.isPropertyAccessExpression(node) && names.has(node.name.text)) {
      const parent = node.parent, name = node.name.text;
      if (ts.isBinaryExpression(parent) && parent.left === node
        && parent.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && parent.operatorToken.kind <= ts.SyntaxKind.LastAssignment) {
        assert.equal(parent.operatorToken.kind, ts.SyntaxKind.EqualsToken, 'numeric stream counters are unsupported');
        if (name === 'verificationGeneration' || name === 'resetGeneration'
          || (name === 'socketGeneration' && node.expression.kind === ts.SyntaxKind.ThisKeyword)) {
          if (name !== 'socketGeneration') assert(ts.isIdentifier(node.expression) && ['subscription', 'latest'].includes(node.expression.text));
          fresh(parent.right); allocations.push({ property: name, site: site(node) });
        } else {
          assert(current(parent.right) || (ts.isIdentifier(parent.right) && parent.right.text === 'undefined'), 'metadata must copy the current stream token or clear it');
          copies.push({ property: name, site: site(node) });
        }
      }
      if ((ts.isPrefixUnaryExpression(parent) || ts.isPostfixUnaryExpression(parent))
        && [ts.SyntaxKind.PlusPlusToken, ts.SyntaxKind.MinusMinusToken].includes(parent.operator))
        assert.fail('numeric stream counters are unsupported');
    }
    ts.forEachChild(node, visit);
  }
  visit(cls);
  const expected = [
    ['socketGeneration', 'connect'], ['verificationGeneration', 'subscribeReplicaTransport'],
    ['verificationGeneration', 'close'], ['verificationGeneration', 'markReplicaSubscriptionsOutOfDate'],
    ['verificationGeneration', 'unsubscribeReplicaListener'], ['verificationGeneration', 'resumeReplicaSubscriptions'],
    ['verificationGeneration', 'resubscribeQueries'], ['verificationGeneration', 'resetReplicaScopeState'],
    ['resetGeneration', 'handleReplicaMessage'],
  ];
  const key = row => row.property + ':' + row.site;
  assert.deepEqual(allocations.map(key).sort(), expected.map(([property, site]) => key({ property, site })).sort(), 'stream allocation sites changed; review their lifecycle');
  return { initialization: declaration[0].initializer.getText(file), allocations, metadataCopies: copies,
    boundary: 'Direct AST allocations and metadata copies only. Assumes ordinary typed field access and no aliased/reflective writes, unmodified Symbol allocation and standard JavaScript equality. Not an async placement, arbitrary aliasing, socket, snapshot or authentication protocol proof.' };
}
