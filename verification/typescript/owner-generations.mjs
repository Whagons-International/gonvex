// Validate the actual allocation sites consumed by async owner guards.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
function extractAllocations(source, property, expected) {
  const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'GonvexClient');
  assert.equal(classes.length, 1);
  const cls = classes[0], writes = [];
  const fresh = expression => {
    assert(ts.isCallExpression(expression) && ts.isIdentifier(expression.expression)
      && expression.expression.text === 'Symbol' && expression.arguments.length === 0
      && !expression.typeArguments && !expression.questionDotToken, 'activation must use fresh Symbol()');
  };
  const declarations = cls.members.filter(n => ts.isPropertyDeclaration(n) && n.name.getText(file) === property);
  assert.equal(declarations.length, 1);
  const declaration = declarations[0];
  assert.deepEqual(declaration.modifiers?.map(m => m.kind), [ts.SyntaxKind.PrivateKeyword]);
  assert(!declaration.type && !declaration.questionToken && !declaration.exclamationToken);
  fresh(declaration.initializer);
  function site(node) {
    while (node.parent !== cls) { assert(node.parent, 'write outside client class'); node = node.parent; }
    assert(ts.isMethodDeclaration(node) || ts.isConstructorDeclaration(node));
    return ts.isConstructorDeclaration(node) ? 'constructor' : node.name.getText(file);
  }
  function visit(node) {
    if (ts.isElementAccessExpression(node) && node.argumentExpression?.getText(file).includes(property))
      assert.fail('computed generation access is unsupported');
    if ((ts.isPropertyAssignment(node) || ts.isShorthandPropertyAssignment(node)) && node.name.getText(file).includes(property))
      assert.fail('indirect generation assignment is unsupported');
    if (ts.isPropertyAccessExpression(node) && node.name.text === property) {
      assert.equal(node.expression.kind, ts.SyntaxKind.ThisKeyword, 'generation aliases are unsupported');
      const parent = node.parent;
      if (ts.isBinaryExpression(parent) && parent.left === node) {
        const assignment = parent.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && parent.operatorToken.kind <= ts.SyntaxKind.LastAssignment;
        if (assignment) {
          assert.equal(parent.operatorToken.kind, ts.SyntaxKind.EqualsToken, 'generation counters are unsupported');
          fresh(parent.right);
          writes.push({ site: site(node), expression: parent.getText(file) });
        }
      }
      if ((ts.isPrefixUnaryExpression(parent) || ts.isPostfixUnaryExpression(parent))
        && [ts.SyntaxKind.PlusPlusToken, ts.SyntaxKind.MinusMinusToken].includes(parent.operator))
        assert.fail('generation counters are unsupported');
    }
    ts.forEachChild(node, visit);
  }
  visit(cls);
  assert.deepEqual(writes.map(row => row.site).sort(), [...expected].sort(), 'activation invalidation sites changed; review the contract');
  return { initialization: declaration.initializer.getText(file), writes };
}

export function extractGenerations(source) {
  return extractAllocations(source, 'outboxScopeGeneration', ['constructor', 'close', 'activateOutboxScope', 'quarantineReplicaScope']);
}
export function extractAuthGenerations(source) {
  return extractAllocations(source, 'authFetchGeneration', ['setAuth', 'setAuth', 'authenticate', 'connect', 'close', 'fetchAndSendAuth', 'refreshRejectedAuth']);
}
