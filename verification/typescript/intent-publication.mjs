import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const hash = value => createHash('sha256').update(value).digest('hex');
const printer = ts.createPrinter({ removeComments: true });
// Closed AST correspondence to the separately verified ownsCurrentScope policy.
// This checks await placement and the complete effect block, not a new theorem
// about JavaScript scheduling, storage or arbitrary observer callbacks.
export function extractIntentPublication(source) {
  const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === 'GonvexClient');
  assert.equal(classes.length, 1);
  const methods = classes[0].members.filter(n => ts.isMethodDeclaration(n) && n.name?.getText(file) === 'refreshIntents');
  assert.equal(methods.length, 1);
  const actual = printer.printNode(ts.EmitHint.Unspecified, methods[0], file).replace(/\s/g, '');
  const expected = `private refreshIntents() {
    if (this.intentListeners.size === 0 || this.manuallyClosed) return;
    this.intentsRefreshDirty = true;
    if (this.intentsRefreshRunning) return;
    this.intentsRefreshRunning = true;
    void (async () => {
      await new Promise(resolve => setTimeout(resolve, 0));
      try {
        while (this.intentsRefreshDirty && !this.manuallyClosed) {
          this.intentsRefreshDirty = false;
          await this.outboxReady.catch(() => undefined);
          if (this.manuallyClosed) break;
          const scope = this.outboxScope;
          const generation = this.outboxScopeGeneration;
          const entries = await this.reducerOutbox.list(scope).catch(() => undefined);
          if (!entries) continue;
          if (!ownsCurrentScope(scope, this.outboxScope, generation,
            this.outboxScopeGeneration, this.manuallyClosed)) {
            this.intentsRefreshDirty ||= !this.manuallyClosed;
            continue;
          }
          this.publishIntents(entries.map(outboxIntentFromEntry));
        }
      } finally {
        this.intentsRefreshRunning = false;
      }
    })();
  }`.replace(/\s/g, '');
  assert.equal(actual, expected, 'intent publication capture, await or effects changed');
  return { method: methods[0].getText(file),
    boundary: 'Closed structural AST check. Captures scope and activation before the awaited journal read, checks the verified ownership predicate afterward and publishes without another await. Relies on ordinary private fields, native promises and the existing synchronous observer contract. Not a storage or full async refinement proof.' };
}
export function verifyIntentPublication(source) {
  const extracted = extractIntentPublication(source);
  const controls = [
    ['capture-current-after-read', 'const generation = this.outboxScopeGeneration;', ''],
    ['ignore-activation', 'scope, this.outboxScope, generation,', 'scope, this.outboxScope, this.outboxScopeGeneration,'],
    ['ignore-close', 'this.outboxScopeGeneration, this.manuallyClosed))', 'this.outboxScopeGeneration, false))'],
    ['skip-replacement-refresh', 'this.intentsRefreshDirty ||= !this.manuallyClosed;', 'this.intentsRefreshDirty = false;'],
    ['await-after-guard', 'this.publishIntents(entries.map(outboxIntentFromEntry));', 'await Promise.resolve(); this.publishIntents(entries.map(outboxIntentFromEntry));'],
    ['ignore-close-during-readiness', 'if (this.manuallyClosed) break;', ''],
  ];
  for (const [name, before, after] of controls) {
    assert.equal(extracted.method.split(before).length, 2, name);
    const changed = source.replace(extracted.method, extracted.method.replace(before, after));
    assert.throws(() => extractIntentPublication(changed), undefined, name);
  }
  return { ...extracted, sourceSha256: hash(source), verifierSha256: hash(readFileSync(new URL(import.meta.url))),
    structuralControlsRejected: controls.map(([name]) => name) };
}
