import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { binary64Prelude, extract as extractRevisionPolicy } from './verify-revisions.mjs';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const maximum = '9007199254740991';
const compact = (node, file) => node.getText(file).replace(/\s/g, '');

/** Bind the separately proved numeric domain to the actual startup publication boundary. */
export function extractStartupSequenceGate(source) {
  const file = ts.createSourceFile('local-replica.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const imports = file.statements.filter(n => ts.isImportDeclaration(n) && n.moduleSpecifier.text === './revision-policy.js');
  assert.equal(imports.length, 1);
  const binding = imports[0].importClause; assert(binding && !binding.isTypeOnly && ts.isNamedImports(binding.namedBindings));
  for (const name of ['isReplicaRevision', 'isTransactionRevision']) {
    const predicate = binding.namedBindings.elements.filter(n => n.name.text === name);
    assert.equal(predicate.length, 1); assert(!predicate[0].isTypeOnly && !predicate[0].propertyName);
  }
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'LocalReplica');
  assert.equal(classes.length, 1);
  const methods = classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name.getText(file) === 'activateScope');
  assert.equal(methods.length, 1); const method = methods[0];
  assert(!method.modifiers && !method.asteriskToken && !method.typeParameters && method.body);
  assert.deepEqual(method.parameters.map(n => compact(n, file)), ['scope:ReplicaScope=defaultReplicaScope', 'forceReload=false', 'restoreFromStorage=true']);
  function declaration(statement, name) {
    if (!ts.isVariableStatement(statement)) return undefined;
    return statement.declarationList.declarations.find(n => ts.isIdentifier(n.name) && n.name.text === name);
  }
  const outer = method.body.statements;
  assert.equal(compact(outer[0], file), 'if(this.disposed)returnPromise.reject(newError("Localreplicaisclosed"));', 'closed replicas cannot reuse an activation cache');
  const index = outer.findIndex(n => declaration(n, 'activation'));
  assert(index >= 0);
  const clears = outer.map((n, i) => compact(n, file) === 'this.scopeLoaded=false;' ? i : -1).filter(i => i >= 0);
  assert.equal(clears.length, 1); assert(clears[0] < index, 'old scope must become unreadable before startup is queued');
  const generationIndex = outer.findIndex(n => declaration(n, 'generation'));
  assert(generationIndex >= 0 && generationIndex < clears[0], 'activation identity must be captured before invalidation callbacks');
  assert.equal(compact(outer[generationIndex], file), 'constgeneration=++this.scopeActivationGeneration;');
  assert.deepEqual(outer.slice(generationIndex + 1, generationIndex + 3).map(n => compact(n, file)), [
    'this.scopeActivation=undefined;', 'this.scopeActivationName=undefined;',
  ], 'a new generation must retire the old cached handle before callbacks');
  const beforeQueue = outer.slice(clears[0] + 1, index);
  assert.equal(beforeQueue.length, 3, 'invalidation, numeric admission and reentrancy fence must precede queuing');
  assert.equal(compact(beforeQueue[0], file), 'if(wasLoaded){this.invalidateEntityVersions();this.notify(true);}');
  const allocationGuard = beforeQueue[1];
  assert(ts.isIfStatement(allocationGuard) && !allocationGuard.elseStatement);
  assert.equal(compact(allocationGuard.thenStatement, file), 'returnPromise.reject(newError("Replicascopeactivationsequenceexhausted."));');
  assert.equal(compact(beforeQueue[2], file), 'if(generation!==this.scopeActivationGeneration)returnPromise.resolve();', 'a newer synchronous activation must keep its handle');
  function allocationExpression(node) {
    if (ts.isParenthesizedExpression(node)) return allocationExpression(node.expression);
    if (node.kind === ts.SyntaxKind.TrueKeyword) return 'true';
    if (node.kind === ts.SyntaxKind.FalseKeyword) return 'false';
    if (ts.isPrefixUnaryExpression(node)) {
      assert.equal(node.operator, ts.SyntaxKind.ExclamationToken);
      return `(not ${allocationExpression(node.operand)})`;
    }
    assert.equal(compact(node, file), 'isTransactionRevision(generation)', 'activation admission must use the verified positive numeric domain');
    return '(isTransactionRevision g)';
  }
  const allocationRejection = allocationExpression(allocationGuard.expression);
  const activation = declaration(outer[index], 'activation');
  assert(outer[index].declarationList.flags & ts.NodeFlags.Const);
  assert(ts.isCallExpression(activation.initializer) && compact(activation.initializer.expression, file) === 'this.enqueueApplication');
  assert.equal(activation.initializer.arguments.length, 1);
  const callback = activation.initializer.arguments[0];
  assert(ts.isArrowFunction(callback) && callback.parameters.length === 0 && ts.isBlock(callback.body));
  assert.deepEqual(callback.modifiers?.map(n => n.kind), [ts.SyntaxKind.AsyncKeyword]);
  const statements = callback.body.statements;
  const candidate = statements.findIndex(n => declaration(n, 'storageSequence'));
  assert.equal(candidate, 3, 'startup read/owner fence must precede numeric admission');
  assert.equal(compact(statements[0], file), 'awaitthis.persistence;');
  assert(declaration(statements[1], 'snapshot') && statements[1].declarationList.flags & ts.NodeFlags.Const);
  assert.equal(compact(statements[2], file), 'if(generation!==this.scopeActivationGeneration)return;');
  assert.equal(compact(statements[candidate], file), 'conststorageSequence=snapshot?.storageSequence??0;');
  const guard = statements[candidate + 1];
  assert(ts.isIfStatement(guard) && !guard.elseStatement);
  assert.equal(compact(guard.expression, file), '!isReplicaRevision(storageSequence)', 'startup must validate the captured sequence');
  assert(ts.isThrowStatement(guard.thenStatement));
  assert.equal(compact(guard.thenStatement, file), 'thrownewError("Invalidreplicastoragesequence.");');
  assert.deepEqual(statements.slice(candidate + 2, candidate + 5).map(n => compact(n, file)), [
    'this.scopeValue=nextScope;', 'this.storageSequence=storageSequence;', 'this.scopeLoaded=true;',
  ], 'only the validated candidate can become readable');
  const tail = outer.slice(-5).map(n => compact(n, file));
  assert.deepEqual(tail, [
    'this.scopeActivation=activation;', 'this.scopeActivationName=nextScope;',
    'constclearActivation=()=>{if(this.scopeActivation!==activation)return;this.scopeActivation=undefined;this.scopeActivationName=undefined;};',
    'voidactivation.then(clearActivation,clearActivation);', 'returnactivation;',
  ], 'only the owning settled activation may clear the in-flight handle');
  const writes = [];
  function visit(node) {
    if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name))
      assert(!['isReplicaRevision', 'isTransactionRevision'].includes(node.name.text), 'numeric predicates cannot be shadowed');
    if (ts.isBinaryExpression(node) && node.operatorToken.kind >= ts.SyntaxKind.FirstAssignment
      && node.operatorToken.kind <= ts.SyntaxKind.LastAssignment
      && ['this.storageSequence', 'this.scopeValue', 'this.scopeLoaded'].includes(compact(node.left, file)))
      writes.push(compact(node, file));
    ts.forEachChild(node, visit);
  }
  visit(method);
  assert.deepEqual(writes, ['this.scopeLoaded=false', 'this.scopeValue=nextScope', 'this.storageSequence=storageSequence', 'this.scopeLoaded=true']);
  const fields = classes[0].members.filter(n => ts.isPropertyDeclaration(n) && n.name.getText(file) === 'scopeActivationGeneration');
  assert.equal(fields.length, 1); assert.equal(compact(fields[0], file), 'privatescopeActivationGeneration=0;');
  const increments = [];
  function visitAllocations(node) {
    if (ts.isElementAccessExpression(node) && node.argumentExpression?.getText(file).includes('scopeActivationGeneration'))
      assert.fail('computed activation generation access is unsupported');
    if ((ts.isPropertyAssignment(node) || ts.isShorthandPropertyAssignment(node)) && node.name.getText(file).includes('scopeActivationGeneration'))
      assert.fail('indirect activation generation assignment is unsupported');
    if (ts.isPropertyAccessExpression(node) && node.name.text === 'scopeActivationGeneration') {
      assert.equal(node.expression.kind, ts.SyntaxKind.ThisKeyword, 'activation counter aliases are unsupported');
      const parent = node.parent;
      if (ts.isBinaryExpression(parent) && parent.left === node && parent.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && parent.operatorToken.kind <= ts.SyntaxKind.LastAssignment)
        assert.fail('activation counter reset or compound assignment is unsupported');
      if (ts.isPrefixUnaryExpression(parent) || ts.isPostfixUnaryExpression(parent)) {
        assert.equal(parent.operator, ts.SyntaxKind.PlusPlusToken);
        let site = node;
        while (site.parent !== classes[0]) { assert(site.parent); site = site.parent; }
        assert(ts.isMethodDeclaration(site));
        increments.push(`${site.name.getText(file)}:${ts.isPrefixUnaryExpression(parent) ? 'prefix' : 'postfix'}`);
      }
    }
    ts.forEachChild(node, visitAllocations);
  }
  visitAllocations(classes[0]);
  assert.deepEqual(increments.sort(), ['activateScope:prefix', 'dispose:postfix']);
  const disposal = classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name.getText(file) === 'dispose');
  assert.equal(disposal.length, 1);
  assert.deepEqual(disposal[0].body.statements.slice(0, 5).map(n => compact(n, file)), [
    'this.disposed=true;', 'this.scopeLoaded=false;', 'this.scopeActivationGeneration++;',
    'this.scopeActivation=undefined;', 'this.scopeActivationName=undefined;',
  ], 'disposal must invalidate identity and cached handles before cleanup');
  return { candidate: compact(statements[candidate], file), predicate: 'isReplicaRevision',
    allocationRejection, activationCounter: { initialization: '0', increments, clearsOldHandleBeforeCallbacks: true, refusesClosedBeforeCoalescing: true },
    publication: statements.slice(candidate + 1, candidate + 5).map(n => n.getText(file)),
    terminalActivation: outer.slice(-5).map(n => n.getText(file)),
    boundary: 'Structural AST correspondence for startup capture, owner fences, numeric predicate calls, publication and identity-guarded terminal cleanup. Activation increments are checked at both actual sites; admission is before queuing, after invalidation callbacks and a fresh owner check. Startup numeric domain comes from the separately required revision-policy binary64 proof. Assumes ordinary typed fields, no reflective aliases/reentrant getters and the existing application queue; not complete hydration, persistence or owner protocol verification.' };
}

function verifyActivationNumbers({ source, policy, z3, output }) {
  const fp = '(_ FloatingPoint 11 53)', number = value => `((_ to_fp 11 53) RNE ${value}.0)`;
  const zero = number(0), one = number(1), max = number(maximum), beforeMax = number('9007199254740990'), over = number('9007199254740992');
  const laws = {
    'admitted-positive-safe': '(=> (allocationAdmitted g) (isTransactionRevision g))',
    'valid-identity-progresses': '(=> (isTransactionRevision g) (allocationAdmitted g))',
    'initial-identity-progresses': `(allocationAdmitted (allocated ${zero}))`,
    'last-safe-identity-progresses': `(and (fp.eq (allocated ${beforeMax}) ${max}) (allocationAdmitted (allocated ${beforeMax})))`,
    'exhausted-identity-refused': `(not (allocationAdmitted (allocated ${max})))`,
    'admitted-step-is-distinct': '(=> (and (isReplicaRevision x) (allocationAdmitted (allocated x))) (fp.gt (allocated x) x))',
    'exhaustion-cannot-reenter-domain': `(=> (and (not (fp.isNaN x)) (fp.geq x ${over})) (not (allocationAdmitted (allocated x))))`,
    'rejected-step-invalidates-safe-prior': '(=> (and (isReplicaRevision x) (not (allocationAdmitted (allocated x)))) (not (fp.eq (allocated x) x)))',
  };
  mkdirSync(output, { recursive: true });
  function solve(candidate, name) {
    const allocation = extractStartupSequenceGate(candidate);
    const definitions = `${binary64Prelude}\n${extractRevisionPolicy(policy)}\n(declare-const g ${fp})\n`
      + `(define-fun allocated ((p ${fp})) ${fp} (fp.add RNE p ${one}))\n`
      + `(define-fun allocationAdmitted ((g ${fp})) Bool (not ${allocation.allocationRejection}))\n`;
    const answers = [];
    for (const [lawName, law] of Object.entries(laws)) {
      const path = resolve(output, `${name}-${lawName}.smt2`);
      writeFileSync(path, `${definitions}(assert (not ${law}))\n(check-sat)\n`);
      const result = spawnSync(z3, ['-smt2', path], { encoding: 'utf8', timeout: 15000 });
      assert.ifError(result.error); assert.equal(result.status, 0, result.stderr || result.stdout);
      const answer = result.stdout.trim(); assert(['sat', 'unsat'].includes(answer), `${lawName}: ${answer}`);
      answers.push(answer);
    }
    return answers;
  }
  assert(solve(source, 'candidate').every(a => a === 'unsat'), 'activation number law has a counterexample');
  const negativeControls = [];
  for (const [name, replacement] of [['ignore-domain', 'false'], ['deny-all-identities', 'true']]) {
    const before = '!isTransactionRevision(generation)';
    assert.equal(source.split(before).length, 2);
    const answers = solve(source.replace(before, replacement), name);
    const violatedLaws = Object.keys(laws).filter((_, i) => answers[i] === 'sat');
    assert(violatedLaws.length, `broken activation admission escaped: ${name}`);
    negativeControls.push({ name, violatedLaws });
  }
  return { laws: Object.keys(laws), negativeControls, arithmetic: 'IEEE-754 binary64 fp.add RNE, extracted positive-safe numeric policy and actual checked prefix increment.',
    numericVerifierDependencySha256: hash(readFileSync(new URL('./verify-revisions.mjs', import.meta.url))) };
}
export function extractSequenceIntegrity(source) {
  const file = ts.createSourceFile('indexeddb-sequence.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  assert(file.statements.every(n => ts.isTypeAliasDeclaration(n) || ts.isFunctionDeclaration(n)
    || (ts.isImportDeclaration(n) && n.importClause?.isTypeOnly)), 'unexpected runtime binding in sequence validator');
  const declarations = file.statements.filter(ts.isFunctionDeclaration);
  assert.deepEqual(declarations.map(n => n.name?.text), ['replicaSequenceEncoding', 'replicaSequenceCovers', 'assertReplicaSequence', 'readReplicaSequence']);
  const fn = name => declarations.find(n => n.name.text === name);
  const encoding = fn('replicaSequenceEncoding');
  assert.equal(encoding.body.statements.length, 1);
  assert.equal(compact(encoding.body.statements[0], file), "returnrecord===undefined?sequence===0:typeofrecord.value==='string'&&record.value===String(sequence);");
  const read = fn('readReplicaSequence');
  assert.deepEqual(read.body.statements.map(n => compact(n, file)), [
    "constprior=awaitdatabase.meta.get([scope,'sequence']);", 'constsequence=Number(prior?.value??0);',
    'awaitassertReplicaSequence(database,scope,prior,sequence);', 'returnsequence;',
  ]);
  const validation = fn('assertReplicaSequence').body.statements;
  assert.equal(validation.length, 6);
  for (const statement of [validation[0], validation[5]]) {
    assert(ts.isIfStatement(statement) && !statement.elseStatement);
    const block = ts.isBlock(statement.thenStatement) ? statement.thenStatement.statements : [statement.thenStatement];
    assert.equal(block.length, 1);
    assert.equal(compact(block[0], file), "thrownewError('Invalidreplicastoragesequence.');");
  }
  assert.equal(compact(validation[0].expression, file), '!replicaSequenceEncoding(prior,sequence)');
  assert.equal(compact(validation[1], file), "const[entity,window,reset]=awaitPromise.all([database.entities.where('[scope+sequence]').between([scope,-Infinity],[scope,[]],true,false).reverse().limit(1).keys(),database.windows.where('[scope+sequence]').between([scope,-Infinity],[scope,[]],true,false).reverse().limit(1).keys(),database.meta.get([scope,'resetSequence']),]);");
  assert.equal(compact(validation[2], file), 'constresetSequence=Number(reset?.value??0);');
  assert.equal(compact(validation[3], file), 'constentitySequence=(entity[0]as[string,number]|undefined)?.[1]??0;');
  assert.equal(compact(validation[4], file), 'constwindowSequence=(window[0]as[string,number]|undefined)?.[1]??0;');
  assert.equal(compact(validation[5].expression, file), '!replicaSequenceEncoding(reset,resetSequence)||!replicaSequenceCovers(sequence,entitySequence,windowSequence,resetSequence)');
  const policy = fn('replicaSequenceCovers');
  assert.deepEqual(policy.parameters.map(n => compact(n, file)), ['sequence:number', 'entity:number', 'window:number', 'reset:number']);
  assert.equal(compact(policy.type, file), 'boolean');
  assert.equal(policy.body.statements.length, 1);
  assert(ts.isReturnStatement(policy.body.statements[0]) && policy.body.statements[0].expression);
  const names = ['sequence', 'entity', 'window', 'reset'];
  function expression(node) {
    if (ts.isParenthesizedExpression(node)) return expression(node.expression);
    const text = compact(node, file);
    if (names.includes(text)) return { sort: 'Real', smt: text };
    if (text === '0') return { sort: 'Real', smt: '0' };
    if (['true', 'false'].includes(text)) return { sort: 'Bool', smt: text };
    for (const name of names) if (text === `Number.isSafeInteger(${name})`) return { sort: 'Bool', smt: `(safe ${name} ${name}Finite)` };
    assert(ts.isBinaryExpression(node), 'unsupported integrity policy expression: ' + text);
    const a = expression(node.left), b = expression(node.right); assert.equal(a.sort, b.sort);
    if (node.operatorToken.kind === ts.SyntaxKind.AmpersandAmpersandToken) {
      assert.equal(a.sort, 'Bool'); return { sort: 'Bool', smt: `(and ${a.smt} ${b.smt})` };
    }
    assert.equal(a.sort, 'Real');
    const op = node.operatorToken.kind === ts.SyntaxKind.GreaterThanEqualsToken ? '>='
      : node.operatorToken.kind === ts.SyntaxKind.LessThanEqualsToken ? '<=' : undefined;
    assert(op, 'unsupported integrity comparison: ' + text);
    return { sort: 'Bool', smt: `(${op} ${a.smt} ${b.smt})` };
  }
  const result = expression(policy.body.statements[0].expression); assert.equal(result.sort, 'Bool');
  return { policy: policy.getText(file), encoding: encoding.getText(file),
    definition: `(define-fun covers (${names.map(name => `(${name} Real) (${name}Finite Bool)`).join(' ')}) Bool ${result.smt})`,
    sourceBridge: { scopedIndexReads: 2, keyOnly: true, indexKeysPerTable: 1, resetRead: true, canonicalEncodingBeforePublication: true, capturedReadValidatedBeforeReturn: true } };
}

export function extractSequenceCallSites(source, className) {
  const file = ts.createSourceFile('storage.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const cls = file.statements.find(n => ts.isClassDeclaration(n) && n.name?.text === className); assert(cls);
  const imports = file.statements.filter(n => ts.isImportDeclaration(n) && n.moduleSpecifier.text === './indexeddb-sequence.js');
  assert.equal(imports.length, 1);
  const binding = imports[0].importClause;
  assert(binding && !binding.isTypeOnly && ts.isNamedImports(binding.namedBindings));
  assert.deepEqual(binding.namedBindings.elements.map(n => { assert(!n.propertyName && !n.isTypeOnly); return n.name.text; }).sort(), ['assertReplicaSequence', 'readReplicaSequence']);
  const readers = { readChanges: 'constsequence=awaitreadReplicaSequence(this.database,scope);',
    load: 'conststorageSequence=awaitreadReplicaSequence(this.database,normalizedScope);',
    loadWorkingSet: 'conststorageSequence=awaitreadReplicaSequence(this.database,scope);',
    loadEntityRows: 'awaitreadReplicaSequence(this.database,scope);',
    loadWindowRows: 'awaitreadReplicaSequence(this.database,scope);',
    withReadView: 'awaitreadReplicaSequence(this.database,normalizeScope(scope));' };
  const writers = ['commit', 'advanceWatermark', 'removeWindow', 'replaceSnapshot', 'clear'];
  const sites = [];
  for (const name of [...Object.keys(readers), ...writers]) {
    const methods = cls.members.filter(n => ts.isMethodDeclaration(n) && n.name.getText(file) === name); assert.equal(methods.length, 1);
    const calls = [];
    function visit(node) {
      if (ts.isCallExpression(node) && compact(node.expression, file) === 'this.database.transaction') calls.push(node);
      ts.forEachChild(node, visit);
    }
    visit(methods[0]); assert.equal(calls.length, 1, 'one transaction required for ' + name);
    const call = calls[0], callback = call.arguments.at(-1);
    assert(ts.isArrowFunction(callback) && ts.isBlock(callback.body));
    const tables = call.arguments.slice(1, -1).map(n => compact(n, file));
    for (const table of ['entities', 'windows', 'meta']) assert(tables.includes('this.database.' + table), `${name} omitted ${table} from the validating transaction`);
    const first = compact(callback.body.statements[0], file);
    if (readers[name]) assert.equal(first, readers[name], 'read validation must precede data in ' + name);
    else assert.equal(first, name === 'commit' ? 'constsequence=awaitthis.nextSequence(scope);' : 'constsequence=awaitthis.nextSequence(normalizedScope);');
    if (['load', 'loadWorkingSet'].includes(name)) {
      const publications = [];
      function inspect(node) {
        if (ts.isReturnStatement(node) && node.expression && ts.isObjectLiteralExpression(node.expression)) {
          for (const property of node.expression.properties) if (property.name?.getText(file) === 'storageSequence') publications.push(property);
        }
        ts.forEachChild(node, inspect);
      }
      inspect(callback.body); assert.equal(publications.length, 1);
      assert(ts.isShorthandPropertyAssignment(publications[0]), name + ' must publish the validated capture');
    }
    if (name === 'readChanges') {
      const statement = callback.body.statements[2]; assert(ts.isVariableStatement(statement));
      const declarations = statement.declarationList.declarations;
      assert.equal(declarations.length, 1); assert.equal(compact(declarations[0].name, file), 'changes');
      const value = declarations[0].initializer; assert(value && ts.isObjectLiteralExpression(value));
      const properties = value.properties.filter(property => property.name?.getText(file) === 'sequence');
      assert.equal(properties.length, 1); assert(ts.isShorthandPropertyAssignment(properties[0]), 'change reader must publish the validated capture');
    }
    sites.push({ method: name, mode: compact(call.arguments[0], file), tables, validatedFirst: true });
  }
  return sites;
}

function verifyCounterIntegrity({ source, z3, output }) {
  mkdirSync(output, { recursive: true });
  const original = extractSequenceIntegrity(source), names = ['sequence', 'entity', 'window', 'reset'];
  const domain = name => `(and (safe ${name} ${name}Finite) (<= 0 ${name}))`;
  const call = '(covers sequence sequenceFinite entity entityFinite window windowFinite reset resetFinite)';
  const valid = `(and ${names.map(domain).join(' ')} (<= entity sequence) (<= window sequence) (<= reset sequence))`;
  const laws = {
    'counter-domain-required': `(=> ${call} ${domain('sequence')})`,
    'record-domains-required': `(=> ${call} (and ${names.slice(1).map(domain).join(' ')}))`,
    'entity-position-covered': `(=> ${call} (<= entity sequence))`,
    'window-position-covered': `(=> ${call} (<= window sequence))`,
    'reset-position-covered': `(=> ${call} (<= reset sequence))`,
    'valid-checkpoint-progresses': `(=> ${valid} ${call})`,
    'last-safe-checkpoint-readable': `(=> (and ${valid} (= sequence ${maximum})) ${call})`,
    'legacy-zero-checkpoint-readable': `(=> (and ${names.map(name => `(and ${name}Finite (= ${name} 0))`).join(' ')}) ${call})`,
  };
  const prelude = `(define-fun safe ((value Real) (finite Bool)) Bool (and finite (is_int value) (<= (- ${maximum}) value) (<= value ${maximum})))\n`
    + names.map(name => `(declare-const ${name} Real)\n(declare-const ${name}Finite Bool)`).join('\n');
  function solve(candidate, name) {
    const query = resolve(output, name + '.smt2');
    writeFileSync(query, `${prelude}\n${extractSequenceIntegrity(candidate).definition}\n` + Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n') + '\n');
    const result = spawnSync(z3, ['-smt2', query], { encoding: 'utf8', timeout: 15000 });
    assert.ifError(result.error); assert.equal(result.status, 0, result.stderr || result.stdout);
    const answers = result.stdout.trim().split('\n'); assert.equal(answers.length, Object.keys(laws).length);
    assert(answers.every(answer => ['sat', 'unsat'].includes(answer))); return answers;
  }
  assert(solve(source, 'candidate').every(answer => answer === 'unsat'));
  const mutations = [
    ...names.map(name => [`ignore-${name}-domain`, `Number.isSafeInteger(${name})`, 'true']),
    ...names.slice(1).map(name => [`ignore-${name}-coverage`, `${name} <= sequence`, 'true']),
    ...names.slice(1).map(name => [`ignore-${name}-negative`, `${name} >= 0`, 'true']),
    ['deny-all-progress', original.policy.slice(original.policy.indexOf('return '), original.policy.lastIndexOf(';')), 'return false'],
  ];
  const negativeControls = [];
  for (const [name, before, after] of mutations) {
    assert.equal(original.policy.split(before).length, 2);
    const candidate = source.replace(original.policy, original.policy.replace(before, after));
    const answers = solve(candidate, name), violatedLaws = Object.keys(laws).filter((_, i) => answers[i] === 'sat');
    assert(violatedLaws.length, 'integrity mutation escaped: ' + name); negativeControls.push({ name, violatedLaws });
  }
  return { source: 'packages/client/src/indexeddb-sequence.ts', sourceSha256: hash(source), laws: Object.keys(laws), negativeControls,
    extracted: original, boundary: 'Actual safe-position coverage predicate, canonical encoding/read bridge and checked adapter transaction entry points. Standard Number/String builtins and native scoped IndexedDB index ordering/transaction isolation are trusted. Latest indexed positions detect counter regression; this does not authenticate every stored field or detect coordinated corruption of both metadata and rows.' };
}

export function extractReplicaSequence(source, className) {
  const file = ts.createSourceFile('storage.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === className);
  assert.equal(classes.length, 1);
  const methods = classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name.getText(file) === 'nextSequence');
  assert.equal(methods.length, 1); const method = methods[0];
  assert.deepEqual(method.modifiers?.map(n => n.kind), [ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.AsyncKeyword]);
  assert(!method.asteriskToken && !method.typeParameters && !method.questionToken && method.body);
  assert.deepEqual(method.parameters.map(n => compact(n, file)), ['scope:string']);
  assert.equal(compact(method.type, file), 'Promise<number>');
  const statements = method.body.statements;
  assert.equal(statements.length, 7, 'counter transition shape changed');
  assert.equal(compact(statements[0], file), "constprior=awaitthis.database.meta.get([scope,'sequence']);");
  assert.equal(compact(statements[1], file), 'constpreviousSequence=Number(prior?.value??0);');
  assert.equal(compact(statements[2], file), 'awaitassertReplicaSequence(this.database,scope,prior,previousSequence);');
  const imports = file.statements.filter(n => ts.isImportDeclaration(n) && n.moduleSpecifier.text === './indexeddb-sequence.js');
  assert.equal(imports.length, 1);
  assert(imports[0].importClause && !imports[0].importClause.isTypeOnly && ts.isNamedImports(imports[0].importClause.namedBindings));
  assert(imports[0].importClause.namedBindings.elements.some(n => n.name.text === 'assertReplicaSequence' && !n.propertyName && !n.isTypeOnly));
  const guard = statements[3]; assert(ts.isIfStatement(guard) && !guard.elseStatement && ts.isBlock(guard.thenStatement));
  assert.equal(guard.thenStatement.statements.length, 1);
  const failure = guard.thenStatement.statements[0]; assert(ts.isThrowStatement(failure));
  assert(ts.isNewExpression(failure.expression) && ts.isIdentifier(failure.expression.expression) && failure.expression.expression.text === 'Error');
  assert.equal(failure.expression.arguments?.length, 1); assert(ts.isStringLiteral(failure.expression.arguments[0]));
  assert.equal(compact(statements[5], file), "awaitthis.database.meta.put({scope,key:'sequence',value:String(sequence)});");
  assert.equal(compact(statements[6], file), 'returnsequence;');
  const declaration = statements[4]; assert(ts.isVariableStatement(declaration));
  assert.equal(declaration.declarationList.declarations.length, 1);
  const value = declaration.declarationList.declarations[0]; assert.equal(compact(value.name, file), 'sequence');
  assert(!value.type && value.initializer);
  function expression(node) {
    if (ts.isParenthesizedExpression(node)) return expression(node.expression);
    const text = compact(node, file);
    if (text === 'previousSequence') return { sort: 'Real', smt: 'previous' };
    if (text === 'Number.MAX_SAFE_INTEGER') return { sort: 'Real', smt: maximum };
    if (text === 'Number.isSafeInteger(previousSequence)') return { sort: 'Bool', smt: '(safe previous finite)' };
    if (['true', 'false'].includes(text)) return { sort: 'Bool', smt: text };
    if (ts.isNumericLiteral(node)) { assert(['0', '1', '2'].includes(node.text)); return { sort: 'Real', smt: node.text }; }
    if (ts.isPrefixUnaryExpression(node)) {
      assert.equal(node.operator, ts.SyntaxKind.ExclamationToken);
      const value = expression(node.operand); assert.equal(value.sort, 'Bool');
      return { sort: 'Bool', smt: `(not ${value.smt})` };
    }
    assert(ts.isBinaryExpression(node), `unsupported counter expression ${text}`);
    const a = expression(node.left), b = expression(node.right); assert.equal(a.sort, b.sort);
    const op = node.operatorToken.kind;
    if ([ts.SyntaxKind.BarBarToken, ts.SyntaxKind.AmpersandAmpersandToken].includes(op)) {
      assert.equal(a.sort, 'Bool'); return { sort: 'Bool', smt: `(${op === ts.SyntaxKind.BarBarToken ? 'or' : 'and'} ${a.smt} ${b.smt})` };
    }
    if (op === ts.SyntaxKind.EqualsEqualsEqualsToken) return { sort: 'Bool', smt: `(= ${a.smt} ${b.smt})` };
    assert.equal(a.sort, 'Real');
    if (op === ts.SyntaxKind.LessThanToken) return { sort: 'Bool', smt: `(< ${a.smt} ${b.smt})` };
    assert.equal(op, ts.SyntaxKind.PlusToken);
    return { sort: 'Real', smt: `(+ ${a.smt} ${b.smt})` };
  }
  const condition = expression(guard.expression), next = expression(value.initializer);
  assert.equal(condition.sort, 'Bool'); assert.equal(next.sort, 'Real');
  return { method: method.getText(file), condition: guard.expression.getText(file),
    definition: `(define-fun admitted ((previous Real) (finite Bool)) Bool (not ${condition.smt}))\n(define-fun next ((previous Real)) Real ${next.smt})` };
}

const prelude = `(define-fun safe ((value Real) (finite Bool)) Bool (and finite (is_int value) (<= (- ${maximum}) value) (<= value ${maximum})))\n(declare-const previous Real)\n(declare-const finite Bool)`;
const valid = `(and (safe previous finite) (<= 0 previous) (< previous ${maximum}))`;
const laws = {
  'invalid-number-blocked': '(=> (not (safe previous finite)) (not (admitted previous finite)))',
  'negative-counter-blocked': '(=> (< previous 0) (not (admitted previous finite)))',
  'exhausted-counter-blocked': `(=> (= previous ${maximum}) (not (admitted previous finite)))`,
  'valid-counter-progresses': `(=> ${valid} (admitted previous finite))`,
  'next-is-safe': `(=> (admitted previous finite) (safe (next previous) true))`,
  'strictly-increases': '(=> (admitted previous finite) (> (next previous) previous))',
  'one-position-per-commit': '(=> (admitted previous finite) (= (next previous) (+ previous 1)))',
  'last-safe-position-progresses': `(=> (and finite (= previous (- ${maximum} 1))) (and (admitted previous finite) (= (next previous) ${maximum})))`,
};
export const sequenceMutations = [
  ['ignore-safe-number', '!Number.isSafeInteger(previousSequence)', 'false'],
  ['ignore-negative', 'previousSequence < 0', 'false'],
  ['ignore-exhaustion', 'previousSequence === Number.MAX_SAFE_INTEGER', 'false'],
  ['deny-all-writes', '!Number.isSafeInteger(previousSequence)', 'true'],
  ['reuse-position', 'previousSequence+1', 'previousSequence+0'],
  ['skip-position', 'previousSequence+1', 'previousSequence+2'],
];
const hash = data => createHash('sha256').update(data).digest('hex');
export function verifyReplicaSequences({ z3, output }) {
  mkdirSync(output, { recursive: true });
  const version = spawnSync(z3, ['--version'], { encoding: 'utf8' });
  assert.equal(version.status, 0); assert.match(version.stdout, /^Z3 version 4\.16\.0 /);
  const adapters = [];
  for (const [file, className] of [['indexeddb-replica.ts', 'IndexedDBLocalReplicaStorage'], ['indexeddb-engine.ts', 'IndexedDBEngineReplicaStorage']]) {
    const path = new URL('../../packages/client/src/' + file, import.meta.url), source = readFileSync(path, 'utf8');
    const original = extractReplicaSequence(source, className), negativeControls = [];
    function solve(candidate, name) {
      const query = resolve(output, `${file}-${name}.smt2`);
      writeFileSync(query, `${prelude}\n${extractReplicaSequence(candidate, className).definition}\n` + Object.values(laws).map(law => `(push) (assert (not ${law})) (check-sat) (pop)`).join('\n') + '\n');
      const result = spawnSync(z3, ['-smt2', query], { encoding: 'utf8', timeout: 15000 });
      assert.ifError(result.error); assert.equal(result.status, 0, result.stderr || result.stdout);
      const answers = result.stdout.trim().split('\n'); assert.equal(answers.length, Object.keys(laws).length);
      assert(answers.every(a => ['sat', 'unsat'].includes(a)), result.stdout);
      return answers;
    }
    assert(solve(source, 'candidate').every(a => a === 'unsat'));
    for (const [name, before, after] of sequenceMutations) {
      assert.equal(original.method.split(before).length, 2, name);
      const candidate = source.replace(original.method, original.method.replace(before, after));
      const answers = solve(candidate, name), violatedLaws = Object.keys(laws).filter((_, i) => answers[i] === 'sat');
      assert(violatedLaws.length, `broken counter transition escaped: ${name}`);
      negativeControls.push({ name, violatedLaws });
    }
    adapters.push({ source: 'packages/client/src/' + file, sourceSha256: hash(source), className, extracted: original,
      counterIntegrityCallSites: extractSequenceCallSites(source, className), negativeControls });
  }
  const startupPath = new URL('../../packages/client/src/local-replica.ts', import.meta.url);
  const startupSource = readFileSync(startupPath, 'utf8');
  const policySource = readFileSync(new URL('../../packages/client/src/revision-policy.ts', import.meta.url), 'utf8');
  const startup = { source: 'packages/client/src/local-replica.ts', sourceSha256: hash(startupSource),
    policy: 'packages/client/src/revision-policy.ts', policySha256: hash(policySource),
    extracted: extractStartupSequenceGate(startupSource),
    activationNumbers: verifyActivationNumbers({ source: startupSource, policy: policySource, z3, output: resolve(output, 'activation-numbers') }) };
  const counterIntegrity = verifyCounterIntegrity({ source: readFileSync(new URL('../../packages/client/src/indexeddb-sequence.ts', import.meta.url), 'utf8'),
    z3, output: resolve(output, 'counter-integrity') });
  const receipt = { adapters, startup, counterIntegrity, laws: Object.keys(laws), verifierSha256: hash(readFileSync(fileURLToPath(import.meta.url))), parser: ts.version,
    solver: version.stdout.trim(), solverSha256: hash(readFileSync(z3)),
    boundary: 'Actual private nextSequence arithmetic guard and increment after persisted-integrity validation, with checked read/validation/guard/write/return order and public read/write transaction bridges. Numeric laws assume validation succeeds and standard Number/isSafeInteger, MAX_SAFE_INTEGER, exact addition of admitted safe integers, ordinary fields and transaction serialization. Counter integrity has a separately extracted predicate and source bridge. Does not authenticate arbitrary stored fields, native storage atomicity, peer synchronization or whole replica recovery.' };
  writeFileSync(resolve(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log(`${Object.keys(laws).length} sequence laws verified for each IndexedDB adapter; ${sequenceMutations.length} broken transitions rejected per adapter`);
  console.log(`${counterIntegrity.laws.length} persisted counter integrity laws verified; ${counterIntegrity.negativeControls.length} broken source policies rejected`);
  return receipt;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2); assert.equal(args.length, 4); assert.equal(args[0], '--z3'); assert.equal(args[2], '--output');
  verifyReplicaSequences({ z3: args[1], output: args[3] });
}
