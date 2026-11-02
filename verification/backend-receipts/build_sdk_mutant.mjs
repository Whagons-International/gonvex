import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { globSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
assert.equal(process.argv[2], '--output');
const output = resolve(process.argv[3]);
const mutation = process.argv[4] === '--mutation' ? process.argv[5] : 'barrier';
assert(['barrier', 'acceptance', 'recovery', 'owner'].includes(mutation), 'unknown SDK source mutation');
await mkdir(output, { recursive: false });
const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
assert(bundler, 'frozen workspace esbuild required');
const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const receipt = { mutation: mutation === 'barrier' ? 'disable reducer durable-watermark wait' : mutation === 'acceptance' ? 'omit received acceptance persistence' : mutation === 'owner' ? 'allow a stale accepted completion to acknowledge a new owner' : 'skip receipt probe before recovered intent delivery', mutations: 0, inputs: {} };
const result = await build({ entryPoints: [join(root, 'packages/client/src/index.ts')],
  bundle: true, platform: 'node', format: 'esm', outfile: join(output, 'client.mjs'), metafile: true,
  plugins: [{ name: 'remove-reducer-watermark-barrier', setup(builder) {
    builder.onLoad({ filter: /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
      const source = await readFile(path, 'utf8');
      const start = mutation === 'recovery' ? 'private async probePreviouslySentIntents(' : 'if (kind === "reducer" && message.type === "reducer.result") {';
      const end = mutation === 'recovery' ? 'private async reconcileAcceptedReceipts(' : 'if (kind === "reducer" && message.type === "reducer.error") {';
      assert.equal(source.split(start).length, 2, 'reducer settlement region changed');
      const begin = source.indexOf(start), finish = source.indexOf(end, begin);
      assert(finish > begin, 'reducer settlement region missing');
      const region = source.slice(begin, finish);
      const guard = mutation === 'owner' ? 'if (this.pendingCalls.get(id) !== pending) return;\n              try {' : mutation === 'barrier' ? '&& committedRevision > this.processedReplicaWatermarkRevision' : mutation === 'acceptance' ? 'const acceptance = onAccepted?.() ?? Promise.resolve();' : 'if (this.serverCapabilities.reducerReceiptLookup !== 1 || this.serverCapabilities.replicaWatermark !== 1) return entries;';
      assert.equal(region.split(guard).length, 2, 'reducer watermark guard changed');
      const replacement = mutation === 'owner' ? 'try {' : mutation === 'barrier' ? '&& false' : mutation === 'acceptance' ? 'const acceptance = Promise.resolve();' : 'if (true) return entries;';
      const mutated = source.slice(0, begin) + region.replace(guard, replacement) + source.slice(finish);
      receipt.mutations++;
      receipt.originalIndexSha256 = digest(source);
      receipt.mutatedIndexSha256 = digest(mutated);
      return { contents: mutated, loader: 'ts' };
    });
  } }],
});
assert.equal(receipt.mutations, 1, 'exactly one production reducer barrier must change');
for (const input of Object.keys(result.metafile.inputs).sort()) {
  const path = resolve(input), label = relative(root, path);
  if (label.startsWith('packages/') && !label.includes('/node_modules/')) receipt.inputs[label] = digest(await readFile(path));
}
receipt.bundleSha256 = digest(await readFile(join(output, 'client.mjs')));
receipt.bundlerSha256 = digest(await readFile(resolve(root, bundler)));
await writeFile(join(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
console.log('Built one source-linked reducer-barrier mutant without changing the checkout');
