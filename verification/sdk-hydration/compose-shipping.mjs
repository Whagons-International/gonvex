import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import cp from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { applyDeferredOwnerOverlay } from './deferred-owner-overlay.mjs';
const require = createRequire(import.meta.url);
const ts = require(process.env.GONVEX_TYPESCRIPT_COMPILER);
const directory = path.dirname(fileURLToPath(import.meta.url));
const inventory = JSON.parse(fs.readFileSync(path.join(directory, 'shipping-source-provenance.json')));
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
const baseline = fs.realpathSync(inventory.package.readOnlyInstalledPath);
const output = process.argv[2];
if (!output || !path.resolve(output).startsWith('/mnt/data/work/') || fs.existsSync(output)) throw Error('new isolated /mnt/data/work output required');
for (const [name, expected] of Object.entries(inventory.package.files)) {
  if (sha(fs.readFileSync(path.join(baseline, name))) !== expected) throw Error(`baseline inventory drift: ${name}`);
}
if (sha(fs.readFileSync(inventory.existingPatch.path)) !== inventory.existingPatch.sha256) throw Error('shipping patch drift');
const core = cp.execFileSync('git', ['show', `${inventory.coreSource.commit}:packages/client/src/index.ts`], { encoding: 'utf8' });
if (sha(core) !== inventory.coreSource.sha256) throw Error('core source drift');
function members(source, filename) {
  const ast = ts.createSourceFile(filename, source, ts.ScriptTarget.Latest, true);
  if (ast.parseDiagnostics.length) throw Error(`parse diagnostics: ${filename}`);
  const klass = ast.statements.find(n => ts.isClassDeclaration(n) && ['GonvexClient', 'Patch'].includes(n.name?.text));
  if (!klass) throw Error('SDK class missing');
  const result = new Map(klass.members.map(m => [m.name?.getText(ast) ?? 'constructor', { start: m.getStart(ast), end: m.getEnd(), text: m.getText(ast) }]));
  if (filename !== 'index.ts' && result.size !== klass.members.length) throw Error('duplicate members would escape audit');
  return result;
}
const coreMembers = members(core, 'index.ts');
const methods = ['sendReplicaOpen', 'flushReplicaOpens', 'setReplicaReady'];
const fields = ['hydratedReplicaReady', 'replicaOpenHydrations', 'replicaOpenOwners'];
const snippets = [...methods, ...fields].map(name => coreMembers.get(name).text).join('\n');
const emitted = ts.transpileModule(`class Patch {\n${snippets}\n}`, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } });
const compiled = members(emitted.outputText, 'patch.js');
const original = fs.readFileSync(path.join(baseline, 'dist/index.js'), 'utf8');
const old = members(original, 'shipping.js');
const edits = methods.map(name => ({ ...old.get(name), text: compiled.get(name).text }));
const replaceOne = (source, before, after) => { if (source.split(before).length !== 2) throw Error(`anchor drift: ${before}`); return source.replace(before, after); };
edits.push({ ...old.get('close'), text: replaceOne(old.get('close').text, 'this.pendingReplicaOpens.clear();', 'this.pendingReplicaOpens.clear();\n        this.replicaOpenHydrations.clear();') });
edits.push({ ...old.get('unsubscribeReplicaListener'), text: replaceOne(old.get('unsubscribeReplicaListener').text, 'this.pendingReplicaOpens.delete(latest);', 'this.pendingReplicaOpens.delete(latest);\n            this.replicaOpenHydrations.delete(latest);') });
edits.push({ start: old.get('replicaReady').end, end: old.get('replicaReady').end, text: '\n    ' + fields.map(name => compiled.get(name).text).join('\n    ') });
let source = original;
for (const edit of edits.sort((a,b) => b.start-a.start)) source = source.slice(0,edit.start)+edit.text+source.slice(edit.end);
const overlay = applyDeferredOwnerOverlay(source);
source = overlay.source.replace(/\n\/\/# sourceMappingURL=index.js.map\s*$/, '\n');
const resultMembers = members(source, 'composed.js');
const imports = text => ts.createSourceFile('index.js',text,ts.ScriptTarget.Latest,true).statements
  .filter(n=>ts.isImportDeclaration(n)||ts.isExportDeclaration(n)).map(n=>n.getText());
if (JSON.stringify(imports(original)) !== JSON.stringify(imports(source))) throw Error('runtime import/export graph changed');
const allowed = new Set([...methods, 'close', 'unsubscribeReplicaListener', 'subscribeReplicaTransport', 'releaseDeferredReplicaOpens', 'activateReplicaDirective', 'resetReplicaScopeState']);
const memberAudit = [...old].map(([name, before]) => {
  const after = resultMembers.get(name); if (!after) throw Error(`removed member: ${name}`);
  const changed = sha(before.text) !== sha(after.text);
  if (changed && !allowed.has(name)) throw Error(`unapproved member change: ${name}`);
  return { name, changed, beforeSha256: sha(before.text), afterSha256: sha(after.text) };
});
fs.cpSync(baseline, output, { recursive: true, dereference: true });
fs.writeFileSync(path.join(output, 'dist/index.js'), source);
fs.unlinkSync(path.join(output, 'dist/index.js.map'));
// The existing shipping dependency graph is reused read-only. No base SDK swap.
fs.symlinkSync(path.resolve(baseline, '../..'), path.join(output, 'node_modules'));
const outputFiles = Object.fromEntries(Object.keys(inventory.package.files).filter(n => n !== 'dist/index.js.map').map(n => [n, sha(fs.readFileSync(path.join(output,n)))]));
for (const [name, expected] of Object.entries(inventory.package.files)) if (!['dist/index.js','dist/index.js.map'].includes(name) && outputFiles[name] !== expected) throw Error(`unapproved file change: ${name}`);
const provenance = {
  experimentalIdentity: `gonvex-sqlite1-hydration-experimental-${path.basename(output)}`,
  notPublished: true, baselinePackageVersion: inventory.package.version,
  baselineClientJsSha256: inventory.package.files['dist/index.js'], existingPatchSha256: inventory.existingPatch.sha256,
  coreSourceCommit: inventory.coreSource.commit, coreSourceSha256: sha(core),
  compositionSourceCommit: cp.execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),
  transformFiles: Object.fromEntries(['compose-shipping.mjs','deferred-owner-overlay.mjs'].map(n=>[n,sha(fs.readFileSync(path.join(directory,n)))])),
  compilerVersion: ts.version, composedClientJsSha256: sha(source), outputPackageTreeHash: sha(JSON.stringify(outputFiles)),
  baselineFiles: inventory.package.files, outputFiles, removedFiles: ['dist/index.js.map'],
  changedMembers: memberAudit.filter(m=>m.changed), unchangedMembers: memberAudit.filter(m=>!m.changed),
  addedMembers: [...resultMembers].filter(([name])=>!old.has(name)).map(([name,m])=>({name,sha256:sha(m.text)})),
  overlay: overlay.provenance, dependenciesReadOnlyNodeModules: path.resolve(baseline,'../..'),
  unchangedImportExportDeclarations: imports(original),
  artifactTests: 'pending', observerCapture: 'not authorized/not performed',
};
fs.writeFileSync(path.join(output,'composition-provenance.json'),JSON.stringify(provenance,null,2)+'\n');
fs.writeFileSync(path.join(path.dirname(output),path.basename(output)+'.index.diff'),cp.spawnSync('git',['diff','--no-index','--',path.join(baseline,'dist/index.js'),path.join(output,'dist/index.js')],{encoding:'utf8'}).stdout);
console.log(JSON.stringify({ output, experimentalIdentity: provenance.experimentalIdentity, jsSha256: sha(source), treeHash: provenance.outputPackageTreeHash, changedMembers: provenance.changedMembers.map(m=>m.name), unchangedMembers: provenance.unchangedMembers.length }));
